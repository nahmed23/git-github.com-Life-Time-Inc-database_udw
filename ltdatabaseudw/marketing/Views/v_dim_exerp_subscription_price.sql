CREATE VIEW [marketing].[v_dim_exerp_subscription_price]
AS select d_exerp_subscription_price.dim_exerp_subscription_price_key dim_exerp_subscription_price_key,
       d_exerp_subscription_price.subscription_price_id subscription_price_id,
       d_exerp_subscription_price.cancel_dim_date_key cancel_dim_date_key,
       d_exerp_subscription_price.cancelled_flag cancelled_flag,
       d_exerp_subscription_price.dim_club_key dim_club_key,
       d_exerp_subscription_price.dim_exerp_subscription_key dim_exerp_subscription_key,
       d_exerp_subscription_price.entry_dim_date_key entry_dim_date_key,
       d_exerp_subscription_price.entry_dim_time_key entry_dim_time_key,
       d_exerp_subscription_price.ets ets,
       d_exerp_subscription_price.from_dim_date_key from_dim_date_key,
       d_exerp_subscription_price.price price,
       d_exerp_subscription_price.subscription_price_type subscription_price_type,
       d_exerp_subscription_price.to_dim_date_key to_dim_date_key
  from dbo.d_exerp_subscription_price;