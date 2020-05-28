CREATE VIEW [marketing].[v_dim_employee_job_title_history]
AS select dim_employee_job_title_history.effective_date_time effective_date_time,
       dim_employee_job_title_history.expiration_date_time expiration_date_time,
       dim_employee_job_title_history.business_titles business_titles,
       dim_employee_job_title_history.cf_employment_status cf_employment_status,
       dim_employee_job_title_history.dim_employee_key dim_employee_key,
       dim_employee_job_title_history.employee_id employee_id,
       dim_employee_job_title_history.is_primary_flag is_primary_flag,
       dim_employee_job_title_history.job_codes job_codes,
       dim_employee_job_title_history.job_profiles job_profiles,
       dim_employee_job_title_history.marketing_titles marketing_titles
  from dbo.dim_employee_job_title_history;