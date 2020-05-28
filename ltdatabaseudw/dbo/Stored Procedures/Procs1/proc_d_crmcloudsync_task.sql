CREATE PROC [dbo].[proc_d_crmcloudsync_task] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_task)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_task_insert') is not null drop table #p_crmcloudsync_task_insert
create table dbo.#p_crmcloudsync_task_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_task.p_crmcloudsync_task_id,
       p_crmcloudsync_task.bk_hash
  from dbo.p_crmcloudsync_task
 where p_crmcloudsync_task.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_task.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_task.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_task.bk_hash,
       p_crmcloudsync_task.bk_hash fact_crm_task_key,
       p_crmcloudsync_task.activity_id activity_id,
       case when p_crmcloudsync_task.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_task.bk_hash
       when s_crmcloudsync_task.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_task.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_task.bk_hash in ('-997','-998','-999') then p_crmcloudsync_task.bk_hash
       when s_crmcloudsync_task.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_task.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_task.created_on created_on,
       case when p_crmcloudsync_task.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_task.bk_hash
    when l_crmcloudsync_task.owner_id is null then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_task.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       isnull(s_crmcloudsync_task.ltf_task_type_name,'') ltf_task_type_name,
       case when p_crmcloudsync_task.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_task.bk_hash
     when l_crmcloudsync_task.regarding_object_id is null then '-998'
      else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_task.regarding_object_id as varchar(36)),'z#@$k%&P'))),2) end regarding_object_dim_crm_system_user_key,
       isnull(s_crmcloudsync_task.regarding_object_type_code,'') regarding_object_type_code,
       s_crmcloudsync_task.scheduled_start scheduled_start,
       case when p_crmcloudsync_task.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_task.bk_hash
        when s_crmcloudsync_task.scheduled_start is null then '-998'   when  convert(varchar, s_crmcloudsync_task.scheduled_start, 112) > '20991231' then '99991231'
        when convert(varchar, s_crmcloudsync_task.scheduled_start, 112)< '19000101' then '19000101'
         else convert(varchar, s_crmcloudsync_task.scheduled_start, 112)    end scheduled_start_dim_date_key,
       case when p_crmcloudsync_task.bk_hash in ('-997','-998','-999') then p_crmcloudsync_task.bk_hash
       when s_crmcloudsync_task.scheduled_start is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_task.scheduled_start,114), 1, 5),':','') end scheduled_start_dim_time_key,
       s_crmcloudsync_task.status_code status_code,
       isnull(s_crmcloudsync_task.status_code_name,'') status_code_name,
       isnull(h_crmcloudsync_task.dv_deleted,0) dv_deleted,
       p_crmcloudsync_task.p_crmcloudsync_task_id,
       p_crmcloudsync_task.dv_batch_id,
       p_crmcloudsync_task.dv_load_date_time,
       p_crmcloudsync_task.dv_load_end_date_time
  from dbo.h_crmcloudsync_task
  join dbo.p_crmcloudsync_task
    on h_crmcloudsync_task.bk_hash = p_crmcloudsync_task.bk_hash
  join #p_crmcloudsync_task_insert
    on p_crmcloudsync_task.bk_hash = #p_crmcloudsync_task_insert.bk_hash
   and p_crmcloudsync_task.p_crmcloudsync_task_id = #p_crmcloudsync_task_insert.p_crmcloudsync_task_id
  join dbo.l_crmcloudsync_task
    on p_crmcloudsync_task.bk_hash = l_crmcloudsync_task.bk_hash
   and p_crmcloudsync_task.l_crmcloudsync_task_id = l_crmcloudsync_task.l_crmcloudsync_task_id
  join dbo.s_crmcloudsync_task
    on p_crmcloudsync_task.bk_hash = s_crmcloudsync_task.bk_hash
   and p_crmcloudsync_task.s_crmcloudsync_task_id = s_crmcloudsync_task.s_crmcloudsync_task_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_task
   where d_crmcloudsync_task.bk_hash in (select bk_hash from #p_crmcloudsync_task_insert)

  insert dbo.d_crmcloudsync_task(
             bk_hash,
             fact_crm_task_key,
             activity_id,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             dim_crm_owner_key,
             ltf_task_type_name,
             regarding_object_dim_crm_system_user_key,
             regarding_object_type_code,
             scheduled_start,
             scheduled_start_dim_date_key,
             scheduled_start_dim_time_key,
             status_code,
             status_code_name,
             deleted_flag,
             p_crmcloudsync_task_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_crm_task_key,
         activity_id,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         dim_crm_owner_key,
         ltf_task_type_name,
         regarding_object_dim_crm_system_user_key,
         regarding_object_type_code,
         scheduled_start,
         scheduled_start_dim_date_key,
         scheduled_start_dim_time_key,
         status_code,
         status_code_name,
         dv_deleted,
         p_crmcloudsync_task_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_task)
--Done!
end
