CREATE VIEW [marketing].[v_fact_mms_membership_balance_history]
AS select fact_mms_membership_balance_history.dim_mms_membership_key dim_mms_membership_key,
       fact_mms_membership_balance_history.membership_id membership_id,
       fact_mms_membership_balance_history.effective_date_time effective_date_time,
       fact_mms_membership_balance_history.expiration_date_time expiration_date_time,
       fact_mms_membership_balance_history.committed_balance_products committed_balance_products,
       fact_mms_membership_balance_history.current_balance_products current_balance_products,
       fact_mms_membership_balance_history.end_of_day_committed_balance end_of_day_committed_balance,
       fact_mms_membership_balance_history.end_of_day_current_balance end_of_day_current_balance,
       fact_mms_membership_balance_history.end_of_day_statement_balance end_of_day_statement_balance,
       fact_mms_membership_balance_history.membership_balance_id membership_balance_id,
       fact_mms_membership_balance_history.original_currency_code original_currency_code,
       fact_mms_membership_balance_history.processing_complete_flag processing_complete_flag,
       fact_mms_membership_balance_history.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
       fact_mms_membership_balance_history.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key
  from dbo.fact_mms_membership_balance_history;