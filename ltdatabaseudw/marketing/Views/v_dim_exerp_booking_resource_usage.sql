CREATE VIEW [marketing].[v_dim_exerp_booking_resource_usage]
AS select dim_exerp_booking_resource_usage.booking_id booking_id,
       dim_exerp_booking_resource_usage.booking_resource_usage_state booking_resource_usage_state,
       dim_exerp_booking_resource_usage.booking_start_dim_date_key booking_start_dim_date_key,
       dim_exerp_booking_resource_usage.booking_start_dim_time_key booking_start_dim_time_key,
       dim_exerp_booking_resource_usage.booking_stop_dim_date_key booking_stop_dim_date_key,
       dim_exerp_booking_resource_usage.booking_stop_dim_time_key booking_stop_dim_time_key,
       dim_exerp_booking_resource_usage.dim_club_key dim_club_key,
       dim_exerp_booking_resource_usage.dim_exerp_booking_key dim_exerp_booking_key,
       dim_exerp_booking_resource_usage.dim_exerp_booking_resource_usage_key dim_exerp_booking_resource_usage_key,
       dim_exerp_booking_resource_usage.resource_access_group_id resource_access_group_id,
       dim_exerp_booking_resource_usage.resource_access_group_name resource_access_group_name,
       dim_exerp_booking_resource_usage.resource_comment resource_comment,
       dim_exerp_booking_resource_usage.resource_external_id resource_external_id,
       dim_exerp_booking_resource_usage.resource_id resource_id,
       dim_exerp_booking_resource_usage.resource_name resource_name,
       dim_exerp_booking_resource_usage.resource_type resource_type,
       dim_exerp_booking_resource_usage.show_calendar_flag show_calendar_flag
  from dbo.dim_exerp_booking_resource_usage;