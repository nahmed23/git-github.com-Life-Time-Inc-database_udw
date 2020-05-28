CREATE VIEW [marketing].[v_completion_exacttarget] AS with et (job_name, job_end_date_time, job_status) as
(
    select job_name,
           job_end_date_time,
           job_status
    from dv_job_status
    where job_name like '%exacttarget%'
    and enabled_flag =1
) 
select case when sum(case when job_status <> 'Complete' then 1 else 0 end) = 0 then 'Completed' else 'In Progress' end as Status,
       case when sum(case when job_status <> 'Complete' then 1 else 0 end) = 0 then max(job_end_date_time) else null end as job_end_date_time
from et;