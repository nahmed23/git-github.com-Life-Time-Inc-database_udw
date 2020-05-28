CREATE PROC [dbo].[proc_util_admin_job_status_history_jobs_copy] @job_group [varchar](255) AS
begin

  set xact_abort on
  set nocount on
/*
-- Count the records in dv_job_status for the input @job_group
  declare @count int
  set @count = (select count(*) from dbo.dv_job_status where job_group = @job_group)

-- Get the next sequence number for dv_job_status_history using proc_util_sequence_number_get_next
  declare @starting_sequence_id bigint
  
  exec dbo.proc_util_sequence_number_get_next
       @table_name = 'dv_job_status_history',
       @id_count = @count,
       @start_id = @starting_sequence_id out
*/
-- Copy the records from dv_job_status for the input @job_group to the dv_job_status_history table
  insert dbo.dv_job_status_history(
           --dv_job_status_history_id,
           dv_job_status_id,
           job_name,
           job_start_date_time,
           job_end_date_time,
           job_status,
           begin_extract_date_time,
           utc_begin_extract_date_time,
           next_begin_extract_date_time,
           next_utc_begin_extract_date_time,
           source_name,
           job_group,
           dv_inserted_date_time,
           dv_insert_user,
           dv_updated_date_time,
           dv_update_user,
           dv_batch_id,
           enabled_flag,
           retry_flag,
           job_priority,
           informatica_folder_name,
           dispatcher_id,
           dispatcher_ignore_flag)
  select --@starting_sequence_id + row_number() over (order by dv_job_status_id) row_num,
         dv_job_status_id,
         job_name,
         job_start_date_time,
         job_end_date_time,
         job_status,
         begin_extract_date_time,
         utc_begin_extract_date_time,
         next_begin_extract_date_time,
         next_utc_begin_extract_date_time,
         source_name,
         job_group,
         dv_inserted_date_time,
         dv_insert_user,
         dv_updated_date_time,
         dv_update_user,
         dv_batch_id,
         enabled_flag,
         retry_flag,
         job_priority,
         informatica_folder_name,
         dispatcher_id,
         dispatcher_ignore_flag
    from dbo.dv_job_status
   where job_group = @job_group

end
