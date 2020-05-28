CREATE VIEW [marketing].[v_dim_exerp_subscription_state_log]
AS select d_exerp_subscription_state_log.dim_exerp_subscription_state_log_key dim_exerp_subscription_state_log_key,
       d_exerp_subscription_state_log.subscription_state_log_id subscription_state_log_id,
       d_exerp_subscription_state_log.dim_club_key dim_club_key,
       d_exerp_subscription_state_log.dim_exerp_subscription_key dim_exerp_subscription_key,
       d_exerp_subscription_state_log.entry_start_dim_date_key entry_start_dim_date_key,
       d_exerp_subscription_state_log.entry_start_dim_time_key entry_start_dim_time_key,
       d_exerp_subscription_state_log.ets ets,
       d_exerp_subscription_state_log.sub_state sub_state,
       d_exerp_subscription_state_log.subscription_state_log_state subscription_state_log_state
  from dbo.d_exerp_subscription_state_log;