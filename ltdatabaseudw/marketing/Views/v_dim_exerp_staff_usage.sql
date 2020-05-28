CREATE VIEW [marketing].[v_dim_exerp_staff_usage]
AS select dim_exerp_staff_usage.booking_id booking_id,
       dim_exerp_staff_usage.dim_club_key dim_club_key,
       dim_exerp_staff_usage.dim_employee_key dim_employee_key,
       dim_exerp_staff_usage.dim_exerp_booking_key dim_exerp_booking_key,
       dim_exerp_staff_usage.dim_exerp_staff_usage_key dim_exerp_staff_usage_key,
       dim_exerp_staff_usage.staff_usage_salary staff_usage_salary,
       dim_exerp_staff_usage.staff_usage_state staff_usage_state,
       dim_exerp_staff_usage.start_dim_date_key start_dim_date_key,
       dim_exerp_staff_usage.start_dim_time_key start_dim_time_key,
       dim_exerp_staff_usage.stop_dim_date_key stop_dim_date_key,
       dim_exerp_staff_usage.stop_dim_time_key stop_dim_time_key,
       dim_exerp_staff_usage.sub_for_dim_employee_key sub_for_dim_employee_key,
       dim_exerp_staff_usage.substitute_of_dim_employee_key substitute_of_dim_employee_key
  from dbo.dim_exerp_staff_usage;