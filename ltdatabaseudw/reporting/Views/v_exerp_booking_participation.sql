CREATE VIEW [reporting].[v_exerp_booking_participation]
AS SELECT
	booking.booking_id
      ,booking.booking_name
      ,booking.booking_state
      ,part.participation_state
       ,booking_date.calendar_date as 'booking_start_date'
	  ,booking.start_dim_date_key
	  ,start_time.display_12_hour_time as 'booking_start_time'
--      ,booking.start_dim_time_key 
	  ,stop_time.display_12_hour_time as 'booking_stop_time'
--      ,booking.stop_dim_time_key
      ,booking.class_capacity
      , act.department
      , act.activity_group_name
      , ag.activity_group_id
	  ,ag.external_id as 'appointment_type'
		 , mem.member_ID
      --, mem.customer_name_last_first --PPI might want to add later on
      , e.employee_ID
      --, e.employee_name_last_first  --PPI might want to add later
	  ,c.club_name
	  ,c.workday_region
	  ,c.club_id
	  ,c.club_code
	  ,c.current_operations_status
	  ,c.dim_club_key
	  ,show_up_date.calendar_date as 'show_up_date'
	  ,show_up_time.display_12_hour_time as 'show_up_time'
	  

 

  FROM dbo.fact_exerp_participation part 
  JOIN dbo.dim_exerp_booking booking
  ON booking.dim_exerp_booking_key = part.dim_exerp_booking_key
  left join dbo.dim_exerp_activity act on act.dim_exerp_activity_key = booking.dim_exerp_activity_key
  left join reporting.v_location c on c.dim_club_key = booking.dim_club_key
  left join dbo.d_exerp_activity_group ag on ag.dim_exerp_activity_group_key = act.dim_exerp_activity_group_key
  left join reporting.v_member mem on mem.dim_mms_member_key = part.dim_mms_member_key 
  LEFt join dbo.dim_exerp_staff_usage staff_usage on staff_usage.dim_exerp_booking_key = booking.dim_exerp_booking_key
  LEFT join dbo.dim_employee e on e.dim_employee_key = staff_usage.dim_employee_key
  JOIN dbo.dim_time start_time  ON booking.start_dim_time_key=start_time.dim_time_key
  JOIN dbo.dim_time stop_time  ON booking.stop_dim_time_key=stop_time.dim_time_key
  JOIN dbo.dim_date booking_date on booking_date.dim_date_key=booking.start_dim_date_key
  JOIN dbo.dim_date show_up_date on show_up_date.dim_date_key=part.show_up_dim_date_key
  JOIN dbo.dim_time show_up_time on show_up_time.dim_time_key=part.show_up_dim_time_key;