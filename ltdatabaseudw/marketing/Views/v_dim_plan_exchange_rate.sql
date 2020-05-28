CREATE VIEW [marketing].[v_dim_plan_exchange_rate] AS select dim_plan_exchange_rate.dim_plan_exchange_rate_key dim_plan_exchange_rate_key,
       dim_plan_exchange_rate.from_currency_code from_currency_code,
       dim_plan_exchange_rate.to_currency_code to_currency_code,
       dim_plan_exchange_rate.plan_rate plan_rate
  from dbo.dim_plan_exchange_rate;