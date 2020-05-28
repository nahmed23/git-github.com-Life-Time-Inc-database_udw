CREATE VIEW [marketing].[v_dim_boss_resource_availability] AS select dim_boss_resource_availability.capacity capacity,
       dim_boss_resource_availability.d_boss_asi_resource_id d_boss_asi_resource_id,
       dim_boss_resource_availability.dim_boss_resource_availability_key dim_boss_resource_availability_key,
       dim_boss_resource_availability.dim_club_key dim_club_key,
       dim_boss_resource_availability.dim_employee_key dim_employee_key,
       dim_boss_resource_availability.employee_id employee_id,
       dim_boss_resource_availability.end_dim_date_key end_dim_date_key,
       dim_boss_resource_availability.end_dim_time_key end_dim_time_key,
       dim_boss_resource_availability.resource resource,
       dim_boss_resource_availability.resource_type resource_type,
       dim_boss_resource_availability.start_dim_date_key start_dim_date_key,
       dim_boss_resource_availability.start_dim_time_key start_dim_time_key,
       dim_boss_resource_availability.status status
  from dbo.dim_boss_resource_availability;