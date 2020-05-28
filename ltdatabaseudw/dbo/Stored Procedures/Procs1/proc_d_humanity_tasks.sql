CREATE PROC [dbo].[proc_d_humanity_tasks] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_humanity_tasks)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_humanity_tasks_insert') is not null drop table #p_humanity_tasks_insert
create table dbo.#p_humanity_tasks_insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_tasks.p_humanity_tasks_id,
       p_humanity_tasks.bk_hash
  from dbo.p_humanity_tasks
 where p_humanity_tasks.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_humanity_tasks.dv_batch_id > @max_dv_batch_id
        or p_humanity_tasks.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_tasks.bk_hash,
       p_humanity_tasks.bk_hash d_humanity_tasks_key,
       l_humanity_tasks.task_id task_id,
       l_humanity_tasks.shift_id shift_id,
       l_humanity_tasks.company_id company_id,
       l_humanity_tasks.task_name task_name,
       l_humanity_tasks.created_at created_at,
       l_humanity_tasks.created_by created_by,
       l_humanity_tasks.deleted deleted,
       l_humanity_tasks.load_dttm load_dttm,
       cast(substring(s_humanity_tasks.ltf_file_name,charindex('.csv',(s_humanity_tasks.ltf_file_name))-8,8) as date) file_arrive_date,
       s_humanity_tasks.ltf_file_name ltf_file_name,
       isnull(h_humanity_tasks.dv_deleted,0) dv_deleted,
       p_humanity_tasks.p_humanity_tasks_id,
       p_humanity_tasks.dv_batch_id,
       p_humanity_tasks.dv_load_date_time,
       p_humanity_tasks.dv_load_end_date_time
  from dbo.h_humanity_tasks
  join dbo.p_humanity_tasks
    on h_humanity_tasks.bk_hash = p_humanity_tasks.bk_hash
  join #p_humanity_tasks_insert
    on p_humanity_tasks.bk_hash = #p_humanity_tasks_insert.bk_hash
   and p_humanity_tasks.p_humanity_tasks_id = #p_humanity_tasks_insert.p_humanity_tasks_id
  join dbo.l_humanity_tasks
    on p_humanity_tasks.bk_hash = l_humanity_tasks.bk_hash
   and p_humanity_tasks.l_humanity_tasks_id = l_humanity_tasks.l_humanity_tasks_id
  join dbo.s_humanity_tasks
    on p_humanity_tasks.bk_hash = s_humanity_tasks.bk_hash
   and p_humanity_tasks.s_humanity_tasks_id = s_humanity_tasks.s_humanity_tasks_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_humanity_tasks
   where d_humanity_tasks.bk_hash in (select bk_hash from #p_humanity_tasks_insert)

  insert dbo.d_humanity_tasks(
             bk_hash,
             d_humanity_tasks_key,
             task_id,
             shift_id,
             company_id,
             task_name,
             created_at,
             created_by,
             deleted,
             load_dttm,
             file_arrive_date,
             ltf_file_name,
             deleted_flag,
             p_humanity_tasks_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_humanity_tasks_key,
         task_id,
         shift_id,
         company_id,
         task_name,
         created_at,
         created_by,
         deleted,
         load_dttm,
         file_arrive_date,
         ltf_file_name,
         dv_deleted,
         p_humanity_tasks_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_humanity_tasks)
--Done!
end
