﻿CREATE VIEW [marketing].[v_dim_exerp_subscription_period]
AS select dim_exerp_subscription_period.billing_dim_date_key billing_dim_date_key,
       dim_exerp_subscription_period.dim_club_key dim_club_key,
       dim_exerp_subscription_period.dim_exerp_product_key dim_exerp_product_key,
       dim_exerp_subscription_period.dim_exerp_subscription_key dim_exerp_subscription_key,
       dim_exerp_subscription_period.dim_exerp_subscription_period_key dim_exerp_subscription_period_key,
       dim_exerp_subscription_period.dim_mms_member_key dim_mms_member_key,
       dim_exerp_subscription_period.fact_exerp_transaction_log_key fact_exerp_transaction_log_key,
       dim_exerp_subscription_period.from_dim_date_key from_dim_date_key,
       dim_exerp_subscription_period.lt_bucks_amount lt_bucks_amount,
       dim_exerp_subscription_period.net_amount net_amount,
       dim_exerp_subscription_period.number_of_bookings number_of_bookings,
       dim_exerp_subscription_period.price_per_booking price_per_booking,
       dim_exerp_subscription_period.price_per_booking_less_lt_bucks price_per_booking_less_lt_bucks,
       dim_exerp_subscription_period.refund_amount refund_amount,
       dim_exerp_subscription_period.refund_period_flag refund_period_flag,
       dim_exerp_subscription_period.refunded_dim_exerp_subscription_period_key refunded_dim_exerp_subscription_period_key,
       dim_exerp_subscription_period.subscription_period_id subscription_period_id,
       dim_exerp_subscription_period.subscription_period_state subscription_period_state,
       dim_exerp_subscription_period.subscription_period_type subscription_period_type,
       dim_exerp_subscription_period.to_dim_date_key to_dim_date_key
  from dbo.dim_exerp_subscription_period;