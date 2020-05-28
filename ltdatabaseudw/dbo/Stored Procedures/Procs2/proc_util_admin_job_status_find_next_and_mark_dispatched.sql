CREATE PROC [dbo].[proc_util_admin_job_status_find_next_and_mark_dispatched] @job_group [varchar](256),@dispatcher_id [int],@job_name [varchar](256) OUT,@informatica_folder_name [varchar](256) OUT,@retry_flag [bit] OUT,@number_of_incomplete_jobs [int] OUT AS
begin

  set xact_abort on
  set nocount on

  declare @dv_job_status_id bigint = null
  
  -- There is a slight chance that a previous call to this procedure marked a record as 'Dispatched', but returned a failure to the caller.
  -- Check to see if there is already a dispatched record with the unique dispatcher_id.  If found then return that record.
  set @dv_job_status_id = 
   (select dv_job_status_id
      from dv_job_status
     where job_group = @job_group
       and dispatcher_id = @dispatcher_id)

  if @dv_job_status_id is null
    begin
      -- For each job in the @job_group that is Not Started find the number of incomplete dependencies
      -- Note that a job that is marked 'Failed' or 'Skipped' will cause a dependent job to never start
      --   and therefore prevent the whole set of jobs from completing
      if object_id('tempdb..#dv_job_status_with_number_of_incomplete_dependencies') is not null drop table #dv_job_status_with_number_of_incomplete_dependencies
      create table dbo.#dv_job_status_with_number_of_incomplete_dependencies with(distribution=round_robin, location=user_db, heap) as
      select dv_job_status.dv_job_status_id,
             sum(case when dependent_dv_job_status.dv_job_status_id is null then 0
                      when dependent_dv_job_status.job_status = 'Complete' then 0
                      else 1
                  end) number_of_incomplete_dependencies
        from dbo.dv_job_status
        left join dbo.dv_job_dependency
          on dv_job_status.dv_job_status_id = dv_job_dependency.dv_job_status_id
        left join dbo.dv_job_status dependent_dv_job_status
          on dv_job_dependency.dependent_on_dv_job_status_id = dependent_dv_job_status.dv_job_status_id
       where dv_job_status.job_status = 'Not Started'
         and dv_job_status.job_group = @job_group
         and dv_job_status.enabled_flag = 1
         and dv_job_status.dispatcher_ignore_flag = 0
       group by dv_job_status.dv_job_status_id

      -- Get the next job id to be run (chose the one with the highest priority that is enabled and has all dependencies completed)
      set @dv_job_status_id = 
       (select top 1 dv_job_status.dv_job_status_id
          from dbo.dv_job_status
          join #dv_job_status_with_number_of_incomplete_dependencies
            on dv_job_status.dv_job_status_id = #dv_job_status_with_number_of_incomplete_dependencies.dv_job_status_id
         where #dv_job_status_with_number_of_incomplete_dependencies.number_of_incomplete_dependencies = 0
           and dv_job_status.enabled_flag = 1
           and dv_job_status.dispatcher_ignore_flag = 0
         order by dv_job_status.job_priority desc,
                  job_name asc)

      -- Mark the job as Dispatched
      declare @suser_sname varchar(100) = suser_sname()
      update dv_job_status
         set job_status = 'Dispatched',
             dispatcher_id = @dispatcher_id,
             dv_updated_date_time = getdate(),
             dv_update_user = @suser_sname
       where dv_job_status_id = @dv_job_status_id
    end

  set @job_name = (select job_name from dbo.dv_job_status where dv_job_status_id = @dv_job_status_id)
  set @informatica_folder_name = (select informatica_folder_name from dbo.dv_job_status where dv_job_status_id = @dv_job_status_id)
  set @retry_flag  = (select retry_flag from dbo.dv_job_status where dv_job_status_id = @dv_job_status_id)
  -- note: even though the dispatcher does not mange dv_job_status records with dispatcher_ignore_flag = 1 they still need to be counted as a job when determining the number of incomplete jobs.
  --       these jobs are managed elsewhere, but still need to prevent the master from completing until they are marked complete
  set @number_of_incomplete_jobs =
   (select count(*)
      from dbo.dv_job_status
     where job_group = @job_group
       and job_status != 'Complete'
       and job_status != 'Failed and No Retry'
       and job_status != 'Skipped'
       and job_name not like 'wf_dv_master_%'
       and job_name not like 'wf[_]dv[_]%[_]master[_]begin'
       and enabled_flag = 1)

end
