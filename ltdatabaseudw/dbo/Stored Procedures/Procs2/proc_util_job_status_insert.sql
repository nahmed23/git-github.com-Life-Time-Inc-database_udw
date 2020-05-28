CREATE PROC [dbo].[proc_util_job_status_insert] @job_name [varchar](256),@begin_extract_date_time [datetime],@source_name [varchar](256),@job_group [varchar](256),@informatica_folder_name [varchar](256) AS
begin

set xact_abort on
set nocount on

declare @start_id bigint
declare @dv_inserted_date_time datetime
declare @dv_insert_user varchar(100)

set @dv_inserted_date_time = getdate()
set @dv_insert_user = suser_sname()

declare @prev_dv_job_status_id bigint = (select dv_job_status_id from dv_job_status where job_name = @job_name and job_group = @job_group)
declare @source_priority bigint = (select isnull(max(job_priority),1) from dv_job_status where source_name = @source_name)

--delete from dbo.dv_job_status
-- where job_name = @job_name
--   and job_group = @job_group

--delete from dbo.dv_job_status_history
-- where job_name = @job_name
--   and job_group = @job_group

if @prev_dv_job_status_id is null
begin

insert into dbo.dv_job_status
           (--[dv_job_status_id]
           --,
           [job_name]
           ,[job_status]
           ,[begin_extract_date_time]
           ,[utc_begin_extract_date_time]
           ,[next_begin_extract_date_time]
           ,[next_utc_begin_extract_date_time]
           ,[source_name]
           ,[job_group]
           ,[dv_inserted_date_time]
           ,[dv_insert_user]
           ,[dv_batch_id]
           ,[enabled_flag]
           ,[retry_flag]
           ,[job_priority]
           ,[informatica_folder_name]
           ,[dispatcher_ignore_flag])
     values
           (--@start_id
           --,
           @job_name
           ,'Complete'
           ,@begin_extract_date_time
           ,@begin_extract_date_time
           ,@begin_extract_date_time
           ,@begin_extract_date_time
           ,@source_name
           ,@job_group
           ,@dv_inserted_date_time
           ,@dv_insert_user
           ,-1
           ,0
           ,1
           ,@source_priority
           ,@informatica_folder_name
           ,0)

end

--declare @new_dv_job_status_id bigint = (select dv_job_status_id from dv_job_status where job_name = @job_name and job_group = @job_group)

--update dv_job_dependency set dv_job_status_id = @new_dv_job_status_id where dv_job_status_id = @prev_dv_job_status_id
--update dv_job_dependency set dependent_on_dv_job_status_id = @new_dv_job_status_id where dependent_on_dv_job_status_id = @prev_dv_job_status_id

end

