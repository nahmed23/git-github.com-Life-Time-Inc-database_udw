CREATE VIEW [marketing].[v_dim_employee_job_title]
AS select dim_employee_job_title.business_title business_title,
       dim_employee_job_title.dim_employee_key dim_employee_key,
       dim_employee_job_title.family family,
       dim_employee_job_title.is_primary_flag is_primary_flag,
       dim_employee_job_title.job_code job_code,
       dim_employee_job_title.level level,
       dim_employee_job_title.marketing_title marketing_title,
       dim_employee_job_title.profile profile,
       dim_employee_job_title.sub_family sub_family,
       dim_employee_job_title.workday_region_id workday_region_id
  from dbo.dim_employee_job_title;