CREATE VIEW [marketing].[v_fact_boss_participation] AS select fact_boss_participation.dim_boss_reservation_key dim_boss_reservation_key,
       fact_boss_participation.fact_boss_participation_key fact_boss_participation_key,
       fact_boss_participation.instructor_type instructor_type,
       fact_boss_participation.mod_count mod_count,
       fact_boss_participation.number_of_participants number_of_participants,
       fact_boss_participation.participation_dim_date_key participation_dim_date_key,
       fact_boss_participation.participation_id participation_id,
       fact_boss_participation.primary_dim_employee_key primary_dim_employee_key,
       fact_boss_participation.secondary_dim_employee_key secondary_dim_employee_key
  from dbo.fact_boss_participation;