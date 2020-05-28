CREATE VIEW [marketing].[v_fact_trainerize_program_plan_item]
AS select fact_trainerize_program_plan_item.completed_flag completed_flag,
       fact_trainerize_program_plan_item.created_dim_date_key created_dim_date_key,
       fact_trainerize_program_plan_item.dim_mms_member_key dim_mms_member_key,
       fact_trainerize_program_plan_item.dim_trainerize_plan_key dim_trainerize_plan_key,
       fact_trainerize_program_plan_item.dim_trainerize_program_key dim_trainerize_program_key,
       fact_trainerize_program_plan_item.fact_trainerize_program_plan_item_key fact_trainerize_program_plan_item_key,
       fact_trainerize_program_plan_item.item_description item_description,
       fact_trainerize_program_plan_item.item_dim_date_key item_dim_date_key,
       fact_trainerize_program_plan_item.item_dim_time_key item_dim_time_key,
       fact_trainerize_program_plan_item.item_name item_name,
       fact_trainerize_program_plan_item.item_type item_type,
       fact_trainerize_program_plan_item.plan_dim_employee_key plan_dim_employee_key,
       fact_trainerize_program_plan_item.plan_item_id plan_item_id,
       fact_trainerize_program_plan_item.program_dim_employee_key program_dim_employee_key,
       fact_trainerize_program_plan_item.source_id source_id,
       fact_trainerize_program_plan_item.source_type source_type,
       fact_trainerize_program_plan_item.updated_dim_date_key updated_dim_date_key
  from dbo.fact_trainerize_program_plan_item;