CREATE VIEW [reporting].[v_exerp_reservation]
AS SELECT
       mbr.booking_name
      ,mbr.[class_capacity]
      ,mbr.[color]
      ,mbr.[comment]
      ,mbr.[description]
      ,mbr.[dim_club_key]
      ,mbr.[dim_exerp_activity_key]
      ,mbr.[dim_exerp_booking_recurrence_key]
      ,mbr.[main_booking_id]
      ,mbr.[recurrence]
      ,mbr.[recurrence_end_dim_date_key]
      ,mbr.[recurrence_end_dim_time_key]
      ,mbr.[recurrence_start_dim_date_key]
      ,mbr.[recurrence_start_dim_time_key]
      ,mbr.[recurrence_type] 
      ,NULL as format_id /*Do we have the Equivalent of Product_Format in Exerp ?*/
      ,(stop_dim_time.minutes_after_midnight - st_dim_time.minutes_after_midnight)  as length_in_minutes
      ,NULL as limit/*Do we have the Equivalent of LIMIT in Exerp ? Is that MAX_CAPACITY_OVERRIDE ?*/
      ,mb.booking_state as reservation_status
      ,NULL as reservation_type /*BOSS had reservation Type C/A/L - Do we have an Equivalent in Exerp ?*/
  from
  marketing.v_dim_exerp_booking mb 
  INNER JOIN marketing.v_dim_exerp_booking_recurrence mbr on mbr.dim_exerp_booking_recurrence_key = mb.dim_exerp_booking_key
  LEFT JOIN marketing.v_dim_time st_dim_time on mbr.recurrence_start_dim_time_key = st_Dim_time.dim_time_key
  LEFT JOIN marketing.v_dim_time stop_dim_time on mbr.recurrence_end_dim_time_key = stop_dim_time.dim_time_key
 -- LEFT JOIN marketing.v_dim_exerp_staff_usage su on mb.dim_exerp_booking_key = su.dim_exerp_booking_key and su.staff_usage_state ='ACTIVE'
  where mb.dim_exerp_booking_key = mb.dim_exerp_booking_recurrence_key;