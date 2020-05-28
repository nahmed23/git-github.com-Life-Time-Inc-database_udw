CREATE PROC [dbo].[proc_util_task_status_insert] @task [varchar](500),@task_description [varchar](500),@dv_batch_id [bigint] AS

begin

set nocount on
set xact_abort on

declare @start_id bigint
declare @task_date_time datetime

/*
exec dbo.proc_util_sequence_number_get_next @table_name = 'dv_task_status', @id_count = 1, @start_id = @start_id out

set @task_date_time = getdate()

insert into dbo.dv_task_status (
       dv_task_status_id,
       task, 
       task_description,
       task_date_time,
       dv_batch_id)
select @start_id,
       @task,
       @task_description,
       @task_date_time,
       @dv_batch_id
*/

end




