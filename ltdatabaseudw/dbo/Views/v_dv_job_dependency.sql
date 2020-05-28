CREATE VIEW [v_dv_job_dependency] AS select dv_job_status.job_name,
       dv_job_status.dv_job_status_id,
       depend.job_name is_dependent_on,
       depend.dv_job_status_id is_dependent_on_id
from dv_job_dependency
join dv_job_status on dv_job_dependency.dv_job_status_id = dv_job_status.dv_job_status_id
join dv_job_status depend on dv_job_dependency.dependent_on_dv_job_status_id = depend.dv_job_status_id;