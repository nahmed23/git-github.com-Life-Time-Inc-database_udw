CREATE VIEW [sandbox_ebi].[v_employee]
AS SELECT
	employee.employee_id,
       employee.dim_employee_key,
	   employee.employee_name, --PPI
	   employee.hire_date,
       employee.termination_date,
	   employee_title.business_title,
	   employee_title.family,
	   employee_title.sub_family,
	   employee_title.job_code,
	   employee_role.role_name,
	   homeclub.club_id,
	   homeclub.club_name,
	   homeclub.club_type,
	   homeclub.region,
	   homeclub.area,
	   homeclub.workday_region,
	   homeclub.current_operations_status
--	   (SELECT MIN(calendar_date) FROM [marketing].[v_dim_date] WHERE Year = Year(getdate()-1)- 1) AS first_day_of_prior_year
FROM dbo.dim_employee employee
 LEFT JOIN reporting.v_location homeclub
   ON employee.dim_club_key = homeclub.dim_club_key
JOIN dbo.dim_employee_job_title employee_title
ON employee_title.dim_employee_key=employee.dim_employee_key
JOIN dbo.d_mms_employee_role role_bridge
ON role_bridge.dim_employee_key=employee.dim_employee_key
JOIN [dbo].[dim_employee_role] employee_role
ON employee_role.dim_employee_role_key=role_bridge.dim_employee_role_key

WHERE employee.employee_id >=0;