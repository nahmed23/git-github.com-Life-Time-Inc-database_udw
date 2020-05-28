CREATE VIEW [dbo].[v_dim_employee_job_title] AS select dim_employee_key,
       level,
       family,
       sub_family,
       profile,
       business_title,
       marketing_title,
       job_code
from dim_employee_job_title;