CREATE VIEW [marketing].[v_spa_completion] AS with spabiz (job_name) as
(
    select job_name
    from dv_job_status
    where job_name like '%spabiz%'
    and enabled_flag =1
    and job_status <> 'complete'
) 
select case when count(distinct job_name) = 0 then 'Completed' else 'In Progress' end as Status
from spabiz;