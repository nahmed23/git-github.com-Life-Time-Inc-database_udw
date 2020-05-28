CREATE VIEW [marketing].[v_dim_boss_reservation_meeting_date] AS select wrk_boss_reservation_meeting_date.dim_boss_reservation_key dim_boss_reservation_key,
       wrk_boss_reservation_meeting_date.dim_boss_reservation_meeting_dim_date_key dim_boss_reservation_meeting_dim_date_key,
       wrk_boss_reservation_meeting_date.end_dim_date_key end_dim_date_key,
       wrk_boss_reservation_meeting_date.instructor_type instructor_type,
       wrk_boss_reservation_meeting_date.meeting_dim_date_key meeting_dim_date_key,
       wrk_boss_reservation_meeting_date.primary_dim_employee_key primary_dim_employee_key,
       wrk_boss_reservation_meeting_date.reservation_id reservation_id,
       wrk_boss_reservation_meeting_date.secondary_dim_employee_key secondary_dim_employee_key,
       wrk_boss_reservation_meeting_date.start_dim_date_key start_dim_date_key
  from dbo.wrk_boss_reservation_meeting_date;