CREATE PROC [dbo].[proc_d_exerp_person_type_log] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_person_type_log)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_person_type_log_insert') is not null drop table #p_exerp_person_type_log_insert
create table dbo.#p_exerp_person_type_log_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_person_type_log.p_exerp_person_type_log_id,
       p_exerp_person_type_log.bk_hash
  from dbo.p_exerp_person_type_log
 where p_exerp_person_type_log.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_person_type_log.dv_batch_id > @max_dv_batch_id
        or p_exerp_person_type_log.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_person_type_log.bk_hash,
       p_exerp_person_type_log.person_type_log_id person_type_log_id,
       l_exerp_person_type_log.center_id center_id,
       case when p_exerp_person_type_log.bk_hash in('-997', '-998', '-999') then p_exerp_person_type_log.bk_hash
           when l_exerp_person_type_log.center_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_person_type_log.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_center_bk_hash,
       case when p_exerp_person_type_log.bk_hash in('-997', '-998', '-999') then p_exerp_person_type_log.bk_hash
           when l_exerp_person_type_log.person_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_person_type_log.person_id as varchar(4000)),'z#@$k%&P'))),2)   end d_exerp_person_bk_hash,
       case when p_exerp_person_type_log.bk_hash in('-997', '-998', '-999') then p_exerp_person_type_log.bk_hash 
              when ((l_exerp_person_type_log.person_id is null) OR (l_exerp_person_type_log.person_id LIKE '%e%') or (l_exerp_person_type_log.person_id LIKE '%OLDe%') or (len(l_exerp_person_type_log.person_id) > 9) or (d_exerp_person.person_type = 'STAFF' and l_exerp_person_type_log.person_id not LIKE '%e%') or (d_exerp_person.person_type = 'STAFF') or (isnumeric(l_exerp_person_type_log.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_person_type_log.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       s_exerp_person_type_log.ets ets,
       case when p_exerp_person_type_log.bk_hash in('-997', '-998', '-999') then p_exerp_person_type_log.bk_hash
           when s_exerp_person_type_log.from_datetime is null then '-998'
        else convert(varchar, s_exerp_person_type_log.from_datetime, 112)    end from_dim_date_key,
       case when p_exerp_person_type_log.bk_hash in ('-997','-998','-999') then p_exerp_person_type_log.bk_hash
       when s_exerp_person_type_log.from_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_person_type_log.from_datetime,114), 1, 5),':','') end from_dim_time_key,
       l_exerp_person_type_log.person_id person_id,
       s_exerp_person_type_log.person_type person_type,
       isnull(h_exerp_person_type_log.dv_deleted,0) dv_deleted,
       p_exerp_person_type_log.p_exerp_person_type_log_id,
       p_exerp_person_type_log.dv_batch_id,
       p_exerp_person_type_log.dv_load_date_time,
       p_exerp_person_type_log.dv_load_end_date_time
  from dbo.h_exerp_person_type_log
  join dbo.p_exerp_person_type_log
    on h_exerp_person_type_log.bk_hash = p_exerp_person_type_log.bk_hash
  join #p_exerp_person_type_log_insert
    on p_exerp_person_type_log.bk_hash = #p_exerp_person_type_log_insert.bk_hash
   and p_exerp_person_type_log.p_exerp_person_type_log_id = #p_exerp_person_type_log_insert.p_exerp_person_type_log_id
  join dbo.l_exerp_person_type_log
    on p_exerp_person_type_log.bk_hash = l_exerp_person_type_log.bk_hash
   and p_exerp_person_type_log.l_exerp_person_type_log_id = l_exerp_person_type_log.l_exerp_person_type_log_id
  join dbo.s_exerp_person_type_log
    on p_exerp_person_type_log.bk_hash = s_exerp_person_type_log.bk_hash
   and p_exerp_person_type_log.s_exerp_person_type_log_id = s_exerp_person_type_log.s_exerp_person_type_log_id
 left join 	d_exerp_person		on l_exerp_person_type_log.person_id = d_exerp_person.person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_person_type_log
   where d_exerp_person_type_log.bk_hash in (select bk_hash from #p_exerp_person_type_log_insert)

  insert dbo.d_exerp_person_type_log(
             bk_hash,
             person_type_log_id,
             center_id,
             d_exerp_center_bk_hash,
             d_exerp_person_bk_hash,
             dim_mms_member_key,
             ets,
             from_dim_date_key,
             from_dim_time_key,
             person_id,
             person_type,
             deleted_flag,
             p_exerp_person_type_log_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         person_type_log_id,
         center_id,
         d_exerp_center_bk_hash,
         d_exerp_person_bk_hash,
         dim_mms_member_key,
         ets,
         from_dim_date_key,
         from_dim_time_key,
         person_id,
         person_type,
         dv_deleted,
         p_exerp_person_type_log_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_person_type_log)
--Done!
end
