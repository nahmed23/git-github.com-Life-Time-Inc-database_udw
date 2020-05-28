CREATE PROC [dbo].[proc_p_mms_membership_type] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @task_description varchar(500)

-- Do this as a single transaction to make sure that exactly one record for a business key is active
--   Re-activate the previously active PIT record.  This is the record with dv_load_end_date_time = current record dv_load_date_time
--   Delete the active PIT record

declare @wf_name varchar(100)
set @wf_name = 'wf_dv_mms_membership_type'

if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db) as
select distinct dv_batch_id
  from dbo.dv_job_status_history
 where job_name = @wf_name
   and dv_batch_id > (select max(dv_batch_id) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
 union
select @current_dv_batch_id

if object_id('tempdb..#delete') is not null drop table #delete
create table dbo.#delete with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select bk_hash,
       dv_load_date_time
  from dbo.p_mms_membership_type
  join #batch_id
    on p_mms_membership_type.dv_batch_id = #batch_id.dv_batch_id
 group by bk_hash,
       dv_load_date_time

begin tran

delete from dbo.p_mms_membership_type
 where dv_batch_id in (select dv_batch_id from #batch_id)

update p_mms_membership_type
   set dv_load_end_date_time = 'dec 31, 9999'
  from #delete
 where p_mms_membership_type.bk_hash = #delete.bk_hash
   and p_mms_membership_type.dv_load_end_date_time = #delete.dv_load_date_time

commit tran

if object_id('tempdb..#max_batch_id') is not null drop table #max_batch_id
create table dbo.#max_batch_id with(distribution=round_robin, location=user_db) as
select isnull(max(dv_batch_id),-2) dv_batch_id
  from dbo.p_mms_membership_type

-- For each satellite table populate a temp table with the satellite data that is not in the PIT table.
-- Rank each business key by dv_load_date_time
if object_id('tempdb..#l_mms_membership_type') is not null drop table #l_mms_membership_type
create table dbo.#l_mms_membership_type with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select l_mms_membership_type.l_mms_membership_type_id,
       l_mms_membership_type.bk_hash,
       l_mms_membership_type.membership_type_id,
       l_mms_membership_type.dv_load_date_time,
       l_mms_membership_type.dv_batch_id,
       dense_rank() over (partition by l_mms_membership_type.bk_hash order by l_mms_membership_type.dv_load_date_time) ranking
  from dbo.l_mms_membership_type
  join #max_batch_id
    on l_mms_membership_type.dv_batch_id > #max_batch_id.dv_batch_id

if object_id('tempdb..#s_mms_membership_type') is not null drop table #s_mms_membership_type
create table dbo.#s_mms_membership_type with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_mms_membership_type.s_mms_membership_type_id,
       s_mms_membership_type.bk_hash,
       s_mms_membership_type.membership_type_id,
       s_mms_membership_type.dv_load_date_time,
       s_mms_membership_type.dv_batch_id,
       dense_rank() over (partition by s_mms_membership_type.bk_hash order by s_mms_membership_type.dv_load_date_time) ranking
  from dbo.s_mms_membership_type
  join #max_batch_id
    on s_mms_membership_type.dv_batch_id > #max_batch_id.dv_batch_id

-- For each satellite rank table from above populate a temp table with a calculated dv_load_end_date.
-- The dv_load_end_date is the dv_load_date_time from the record with the next sequential rank.  If there is no next record then use Dec 31, 9999.
if object_id('tempdb..#l_mms_membership_type_end') is not null drop table #l_mms_membership_type_end
create table dbo.#l_mms_membership_type_end with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select l_mms_membership_type_1.l_mms_membership_type_id,
       l_mms_membership_type_1.bk_hash,
       l_mms_membership_type_1.dv_load_date_time,
       l_mms_membership_type_1.dv_batch_id,
       isnull(l_mms_membership_type_2.dv_load_date_time, 'Dec 31, 9999') dv_load_end_date_time
  from #l_mms_membership_type l_mms_membership_type_1
  left join #l_mms_membership_type l_mms_membership_type_2
    on l_mms_membership_type_1.bk_hash = l_mms_membership_type_2.bk_hash
   and l_mms_membership_type_1.ranking + 1 = l_mms_membership_type_2.ranking

if object_id('tempdb..#s_mms_membership_type_end') is not null drop table #s_mms_membership_type_end
create table dbo.#s_mms_membership_type_end with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_mms_membership_type_1.s_mms_membership_type_id,
       s_mms_membership_type_1.bk_hash,
       s_mms_membership_type_1.dv_load_date_time,
       s_mms_membership_type_1.dv_batch_id,
       isnull(s_mms_membership_type_2.dv_load_date_time, 'Dec 31, 9999') dv_load_end_date_time
  from #s_mms_membership_type s_mms_membership_type_1
  left join #s_mms_membership_type s_mms_membership_type_2
    on s_mms_membership_type_1.bk_hash = s_mms_membership_type_2.bk_hash
   and s_mms_membership_type_1.ranking + 1 = s_mms_membership_type_2.ranking

-- Populate temp table #u with the union of the satellite rank tables from above to find the distinct set of business keys and dv_load_date_time
if object_id('tempdb..#u') is not null drop table #u
create table dbo.#u with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select bk_hash,
       membership_type_id,
       dv_load_date_time,
       dv_batch_id
  from #l_mms_membership_type
union
select bk_hash,
       membership_type_id,
       dv_load_date_time,
       dv_batch_id
  from #s_mms_membership_type

-- Take the min(dv_batch_id) to cover records being loaded in separate batch_ids with the same source inserted date time
if object_id('tempdb..#mu') is not null drop table #mu
create table dbo.#mu with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select bk_hash,
       membership_type_id,
       dv_load_date_time,
       min(dv_batch_id) dv_batch_id
  from #u
 group by bk_hash,
          membership_type_id,
          dv_load_date_time

-- Populate temp table #pr with the rank of the union result (#u) by dv_load_date_time
if object_id('tempdb..#pr') is not null drop table #pr
create table dbo.#pr with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select bk_hash,
       membership_type_id,
       dv_load_date_time,
       dv_batch_id,
       dense_rank() over (partition by bk_hash order by dv_load_date_time) ranking
  from #mu

-- Populate temp table #du with the distinct business keys from #u
if object_id('tempdb..#du') is not null drop table #du
create table dbo.#du with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select distinct bk_hash
  from #mu

-- Populate temp table #p_active with the active PIT record (if any) associated with the business keys from above (#du)
if object_id('tempdb..#p_active') is not null drop table #p_active
create table dbo.#p_active with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_mms_membership_type.p_mms_membership_type_id,
       p_mms_membership_type.bk_hash,
       p_mms_membership_type.l_mms_membership_type_id,
       p_mms_membership_type.s_mms_membership_type_id,
       p_mms_membership_type.dv_load_end_date_time
  from dbo.p_mms_membership_type
  join #du
    on p_mms_membership_type.bk_hash = #du.bk_hash
 where p_mms_membership_type.dv_load_end_date_time = 'Dec 31, 9999'

-- Populate temp table #p_new with the new PIT records.  Also include the rank and the active PIT record id (if any) to be used
-- when setting the dv_load_end_date_time on the existing active record.
-- If the record is the first in a series set the dv_first_in_key_series flag.
if object_id('tempdb..#p_new') is not null drop table #p_new
create table dbo.#p_new with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select pr1.bk_hash,
       pr1.dv_load_date_time,
       pr1.ranking,
       pr1.membership_type_id,
       isnull(pr2.dv_load_date_time, 'dec 31, 9999') dv_load_end_date_time,
       isnull(l_mms_membership_type_end.l_mms_membership_type_id, isnull(p_active.l_mms_membership_type_id,-998)) l_mms_membership_type_id,
       isnull(s_mms_membership_type_end.s_mms_membership_type_id, isnull(p_active.s_mms_membership_type_id,-998)) s_mms_membership_type_id,
       p_active.p_mms_membership_type_id,
       case when p_active.p_mms_membership_type_id is null and pr1.ranking = 1 then 1 else 0 end dv_first_in_key_series
  from #pr pr1
  left join #pr pr2
    on pr1.bk_hash = pr2.bk_hash
   and pr1.ranking + 1 = pr2.ranking
  left join #p_active p_active
    on pr1.bk_hash = p_active.bk_hash
  left join #l_mms_membership_type_end l_mms_membership_type_end
    on pr1.bk_hash = l_mms_membership_type_end.bk_hash
   and pr1.dv_load_date_time >= l_mms_membership_type_end.dv_load_date_time
   and isnull(pr2.dv_load_date_time,'dec 31, 9999') <= l_mms_membership_type_end.dv_load_end_date_time
  left join #s_mms_membership_type_end s_mms_membership_type_end
    on pr1.bk_hash = s_mms_membership_type_end.bk_hash
   and pr1.dv_load_date_time >= s_mms_membership_type_end.dv_load_date_time
   and isnull(pr2.dv_load_date_time,'dec 31, 9999') <= s_mms_membership_type_end.dv_load_end_date_time

--stack values for greatest() calculation: satellite_date_time and dv_batch_id
if object_id('tempdb..#greatest') is not null drop table #greatest
create table dbo.#greatest with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select bk_hash,
       ranking,
       max(satellite_date_time) max_satellite_date_time,
       max(dv_batch_id) max_dv_batch_id
  from (
select #p_new.bk_hash,
       #p_new.ranking,
       null satellite_date_time,
       l_mms_membership_type.dv_batch_id
 from #p_new
 join dbo.l_mms_membership_type
   on #p_new.l_mms_membership_type_id = l_mms_membership_type.l_mms_membership_type_id
union all
select #p_new.bk_hash,
       #p_new.ranking,
       s_mms_membership_type.inserted_date_time satellite_date_time,
       s_mms_membership_type.dv_batch_id
 from #p_new
 join dbo.s_mms_membership_type
   on #p_new.s_mms_membership_type_id = s_mms_membership_type.s_mms_membership_type_id
 where #p_new.dv_first_in_key_series = 1
union all
select #p_new.bk_hash,
       #p_new.ranking,
       s_mms_membership_type.updated_date_time satellite_date_time,
       s_mms_membership_type.dv_batch_id
 from #p_new
 join dbo.s_mms_membership_type
   on #p_new.s_mms_membership_type_id = s_mms_membership_type.s_mms_membership_type_id
 where #p_new.dv_first_in_key_series <> 1

  ) x
 group by bk_hash,ranking

if object_id('tempdb..#inserts') is not null drop table #inserts
create table dbo.#inserts with(distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #p_new.p_mms_membership_type_id,
       #p_new.bk_hash,
       #p_new.membership_type_id,
       #p_new.l_mms_membership_type_id,
       #p_new.s_mms_membership_type_id,
       #greatest.max_satellite_date_time greatest_satellite_date_time,
       #p_new.dv_load_date_time,
       #p_new.dv_load_end_date_time,
       #greatest.max_dv_batch_id dv_batch_id,
       #p_new.dv_first_in_key_series,
       #p_new.ranking
  from #p_new
  join #greatest
    on #p_new.ranking = #greatest.ranking
   and #p_new.bk_hash = #greatest.bk_hash

-- Do this as a single transaction to make sure that exactly one record for a business key is active
--   Change dv_load_end_date_time from dec 31, 9999 on the existing PIT table records to the earliest dv_load_date_time in the new records
--   Insert the new PIT table records

declare @start int, @end int, @user varchar(50)
set @user = suser_sname()
declare @insert_date_time datetime = getdate()
begin tran
    update dbo.p_mms_membership_type
       set dv_load_end_date_time = p_new.dv_load_date_time,
           dv_next_greatest_satellite_date_time = p_new.greatest_satellite_date_time,
           dv_updated_date_time = @insert_date_time,
           dv_update_user = @user
      from #inserts p_new
     where p_mms_membership_type.p_mms_membership_type_id = p_new.p_mms_membership_type_id
       and p_mms_membership_type.bk_hash = p_new.bk_hash
       and p_new.ranking = 1

    insert into dbo.p_mms_membership_type(
        bk_hash,
        membership_type_id,
        l_mms_membership_type_id,
        s_mms_membership_type_id,
        dv_greatest_satellite_date_time,
        dv_next_greatest_satellite_date_time,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id,
        dv_first_in_key_series)
    select p1.bk_hash,
           p1.membership_type_id,
           p1.l_mms_membership_type_id,
           p1.s_mms_membership_type_id,
           p1.greatest_satellite_date_time,
           p2.greatest_satellite_date_time,
           @insert_date_time,
           suser_sname(),
           p1.dv_load_date_time,
           p1.dv_load_end_date_time,
           p1.dv_batch_id,
           p1.dv_first_in_key_series
      from #inserts p1
      left join #inserts p2
        on p1.bk_hash = p2.bk_hash
       and p1.ranking + 1 = p2.ranking
commit tran


end
