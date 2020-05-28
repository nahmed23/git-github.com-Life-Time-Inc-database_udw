CREATE VIEW [marketing].[v_dim_fitmetrix_instructor] AS select d_fitmetrix_api_instructor.dim_fitmetrix_instructor_key dim_fitmetrix_instructor_key,
       d_fitmetrix_api_instructor.instructor_id instructor_id,
       d_fitmetrix_api_instructor.dim_employee_key dim_employee_key,
       d_fitmetrix_api_instructor.dim_fitmetrix_location_key dim_fitmetrix_location_key,
       d_fitmetrix_api_instructor.email email,
       d_fitmetrix_api_instructor.gender gender,
       d_fitmetrix_api_instructor.name name
  from dbo.d_fitmetrix_api_instructor;