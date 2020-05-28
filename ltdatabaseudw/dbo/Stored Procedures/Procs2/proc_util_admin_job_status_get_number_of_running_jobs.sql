CREATE PROC [dbo].[proc_util_admin_job_status_get_number_of_running_jobs] @job_group [varchar](256),@number_of_running_jobs [int] OUT AS
begin

  set xact_abort on
  set nocount on

  -- note: even though the dispatcher does not mange dv_job_status records with dispatcher_ignore_flag = 1 they still are counted as a job when determining the number of incomplete jobs
  --       but the number calculated below will not include any of those jobs that are still running and being managed elsewhere
  set @number_of_running_jobs =
   (select count(*)
      from dbo.dv_job_status
     where job_group = @job_group
       and job_status != 'Not Started'
       and job_status != 'Complete'
       and job_status != 'Failed and No Retry'
       and job_status != 'Skipped'
       and job_name not like 'wf_dv_master_%'
       and job_name not like 'wf[_]dv[_]%[_]master[_]begin'
       and enabled_flag = 1
       and dispatcher_ignore_flag = 0)

end
