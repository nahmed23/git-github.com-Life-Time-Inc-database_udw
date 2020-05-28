CREATE VIEW [marketing].[v_dim_trainerize_program]
AS select dim_trainerize_program.created_dim_date_key created_dim_date_key,
       dim_trainerize_program.dim_employee_key dim_employee_key,
       dim_trainerize_program.dim_mms_member_key dim_mms_member_key,
       dim_trainerize_program.dim_trainerize_program_key dim_trainerize_program_key,
       dim_trainerize_program.end_dim_date_key end_dim_date_key,
       dim_trainerize_program.end_dim_time_key end_dim_time_key,
       dim_trainerize_program.program_id program_id,
       dim_trainerize_program.program_name program_name,
       dim_trainerize_program.source_id source_id,
       dim_trainerize_program.source_type source_type,
       dim_trainerize_program.start_dim_date_key start_dim_date_key,
       dim_trainerize_program.start_dim_time_key start_dim_time_key,
       dim_trainerize_program.status status,
       dim_trainerize_program.updated_dim_date_key updated_dim_date_key
  from dbo.dim_trainerize_program;