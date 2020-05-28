CREATE PROC [dbo].[proc_util_job_dependency_insert] @job_name [varchar](256),@dependent_on_job_name [varchar](256),@job_group [varchar](256) AS
begin

set xact_abort on
set nocount on

declare @start_id bigint
declare @dv_job_status_id bigint = (select dv_job_status_id from dv_job_status where job_name = @job_name and job_group = @job_group)
declare @dependent_on_dv_job_status_id bigint = (select dv_job_status_id from dv_job_status where job_name = @dependent_on_job_name and job_group = @job_group)

--exec dbo.proc_util_sequence_number_get_next @table_name = 'dv_job_dependency', @id_count = 1, @start_id = @start_id out

delete from dbo.dv_job_dependency
 where dv_job_status_id = @dv_job_status_id
   and dependent_on_dv_job_status_id = @dependent_on_dv_job_status_id

insert into dbo.dv_job_dependency(
              --dv_job_dependency_id,
              dv_job_status_id,
              dependent_on_dv_job_status_id,
              dv_inserted_date_time,
              dv_insert_user)
select --@start_id,
       @dv_job_status_id,
       @dependent_on_dv_job_status_id,
       getdate(),
       suser_sname()

end
