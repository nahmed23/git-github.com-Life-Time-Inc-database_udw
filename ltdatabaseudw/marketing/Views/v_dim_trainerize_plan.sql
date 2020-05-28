CREATE VIEW [marketing].[v_dim_trainerize_plan]
AS select dim_trainerize_plan.created_dim_date_key created_dim_date_key,
       dim_trainerize_plan.dim_employee_key dim_employee_key,
       dim_trainerize_plan.dim_mms_member_key dim_mms_member_key,
       dim_trainerize_plan.dim_trainerize_plan_key dim_trainerize_plan_key,
       dim_trainerize_plan.dim_trainerize_program_key dim_trainerize_program_key,
       dim_trainerize_plan.duration duration,
       dim_trainerize_plan.duration_type duration_type,
       dim_trainerize_plan.end_dim_date_key end_dim_date_key,
       dim_trainerize_plan.end_dim_time_key end_dim_time_key,
       dim_trainerize_plan.plan_id plan_id,
       dim_trainerize_plan.plan_name plan_name,
       dim_trainerize_plan.source_id source_id,
       dim_trainerize_plan.source_type source_type,
       dim_trainerize_plan.start_dim_date_key start_dim_date_key,
       dim_trainerize_plan.start_dim_time_key start_dim_time_key,
       dim_trainerize_plan.updated_dim_date_key updated_dim_date_key
  from dbo.dim_trainerize_plan;