CREATE VIEW [sandbox_ebi].[v_JobStatusEnabled]
AS with RefreshCTE AS (
	select RefreshDateTime = dateadd(hour, -5, getutcdate())
)
select 
	  dv_job_status_id
	, job_name
	, job_start_date_time
	, job_end_date_time
	--, case when job_end_date_time >= '2020-03-12 06:20:00' then null else job_end_date_time end job_end_date_time
	, job_status
	--, case 
	--		when job_end_date_time > '2020-03-12 06:25:00' then 'Not Started'
	--		when job_end_date_time >= '2020-03-12 06:20:00'  then 'In Progress'
	--		else job_status 
	--  end job_status
	, begin_extract_date_time
	, next_begin_extract_date_time
	, source_name
	, job_group = 
			case 
				when job_group = 'dv_main_azure' then 'UDW Main'
				when job_group = 'dv_crm_azure' then 'CRM Main'
				when job_group = 'dv_exerp_azure' then 'EXERP Main'
				when job_group = 'dv_mdm_azure' then 'MDM Main'
				when job_group = 'dv_etips_opt_in_employees_azure' then 'eTips Main'
				else job_group
			end
	, job_priority
	, dv_batch_id
	, _InProgressCount = case when job_status = 'In Progress' then 1 else 0 end
			--case 
			--	when job_end_date_time > '2020-03-12 06:25:00' then 0
			--	when job_end_date_time >= '2020-03-12 06:20:00'  then 1
			--	else 0 
			--end 
	, _NotStartedCount = case when job_status = 'Not Started' then 1 else 0 end
			--case 
			--	when job_end_date_time > '2020-03-12 06:25:00' then 1
			--	when job_end_date_time >= '2020-03-12 06:20:00'  then 0
			--	else 0 
			--end 
	, _CompleteCount = case when job_status = 'Complete' then 1 else 0 end
			--case 
			--	when job_end_date_time > '2020-03-12 06:25:00' then 0
			--	when job_end_date_time >= '2020-03-12 06:20:00'  then 0
			--	else 1
			--end 
	, _SkippedFailerCount = case when job_status not in ( 'In Progress', 'Not Started', 'Complete' )then 1 else 0 end
	, RefreshDateTime = (select RefreshDateTime from RefreshCTE)
	, isRemaining = case when job_status not in ('In Progress', 'Complete') then 1 else 0 end
			--case 
			--	when job_end_date_time > '2020-03-12 06:25:00' then 1 --'Not Started'
			--	when job_status in ('Skipped','Fail') then 1
			--	else 0
			--end
from dbo.dv_job_status
where enabled_flag = 1
	and job_group not in ('obsolete');