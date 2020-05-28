CREATE PROC [dbo].[proc_d_mms_guest_count] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_guest_count)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_guest_count_insert') is not null drop table #p_mms_guest_count_insert
create table dbo.#p_mms_guest_count_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_guest_count.p_mms_guest_count_id,
       p_mms_guest_count.bk_hash
  from dbo.p_mms_guest_count
 where p_mms_guest_count.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_guest_count.dv_batch_id > @max_dv_batch_id
        or p_mms_guest_count.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_guest_count.bk_hash,
       p_mms_guest_count.bk_hash d_mms_guest_count_key,
       p_mms_guest_count.guest_count_id guest_count_id,
       l_mms_guest_count.club_id club_id,
       case when l_mms_guest_count.club_id is null then '-998' else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_guest_count.club_id as varchar(500)),'z#@$k%&P'))),2) end dim_club_key,
       convert(varchar,s_mms_guest_count.guest_count_date, 112) fact_guest_count_dim_date_key,
       s_mms_guest_count.guest_count_date guest_count_date,
       s_mms_guest_count.inserted_date_time inserted_date_time,
       case when p_mms_guest_count.bk_hash in('-997', '-998', '-999') then p_mms_guest_count.bk_hash
           when s_mms_guest_count.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_guest_count.inserted_date_time, 112)    end inserted_dim_date_key,
       s_mms_guest_count.member_child_count member_child_count,
       s_mms_guest_count.member_count member_count,
       s_mms_guest_count.non_member_child_count non_member_child_count,
       s_mms_guest_count.non_member_count non_member_count,
       isnull(h_mms_guest_count.dv_deleted,0) dv_deleted,
       p_mms_guest_count.p_mms_guest_count_id,
       p_mms_guest_count.dv_batch_id,
       p_mms_guest_count.dv_load_date_time,
       p_mms_guest_count.dv_load_end_date_time
  from dbo.h_mms_guest_count
  join dbo.p_mms_guest_count
    on h_mms_guest_count.bk_hash = p_mms_guest_count.bk_hash
  join #p_mms_guest_count_insert
    on p_mms_guest_count.bk_hash = #p_mms_guest_count_insert.bk_hash
   and p_mms_guest_count.p_mms_guest_count_id = #p_mms_guest_count_insert.p_mms_guest_count_id
  join dbo.l_mms_guest_count
    on p_mms_guest_count.bk_hash = l_mms_guest_count.bk_hash
   and p_mms_guest_count.l_mms_guest_count_id = l_mms_guest_count.l_mms_guest_count_id
  join dbo.s_mms_guest_count
    on p_mms_guest_count.bk_hash = s_mms_guest_count.bk_hash
   and p_mms_guest_count.s_mms_guest_count_id = s_mms_guest_count.s_mms_guest_count_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_guest_count
   where d_mms_guest_count.bk_hash in (select bk_hash from #p_mms_guest_count_insert)

  insert dbo.d_mms_guest_count(
             bk_hash,
             d_mms_guest_count_key,
             guest_count_id,
             club_id,
             dim_club_key,
             fact_guest_count_dim_date_key,
             guest_count_date,
             inserted_date_time,
             inserted_dim_date_key,
             member_child_count,
             member_count,
             non_member_child_count,
             non_member_count,
             deleted_flag,
             p_mms_guest_count_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_mms_guest_count_key,
         guest_count_id,
         club_id,
         dim_club_key,
         fact_guest_count_dim_date_key,
         guest_count_date,
         inserted_date_time,
         inserted_dim_date_key,
         member_child_count,
         member_count,
         non_member_child_count,
         non_member_count,
         dv_deleted,
         p_mms_guest_count_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_guest_count)
--Done!
end
