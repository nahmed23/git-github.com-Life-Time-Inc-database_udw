CREATE VIEW [marketing].[v_dim_exerp_subscription_change_log]
AS select d_exerp_subscription_change_log.subscription_change_log_id subscription_change_log_id,
       d_exerp_subscription_change_log.center_id center_id,
       d_exerp_subscription_change_log.dim_club_key dim_club_key,
       d_exerp_subscription_change_log.dim_employee_key dim_employee_key,
       d_exerp_subscription_change_log.dim_exerp_subscription_key dim_exerp_subscription_key,
       d_exerp_subscription_change_log.from_date_time from_date_time,
       d_exerp_subscription_change_log.from_dim_date_key from_dim_date_key,
       d_exerp_subscription_change_log.from_dim_time_key from_dim_time_key,
       d_exerp_subscription_change_log.subscription_change_log_type subscription_change_log_type,
       d_exerp_subscription_change_log.subscription_change_log_value subscription_change_log_value,
       d_exerp_subscription_change_log.subscription_id subscription_id
  from dbo.d_exerp_subscription_change_log;