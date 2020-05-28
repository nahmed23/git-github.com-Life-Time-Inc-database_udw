CREATE VIEW [marketing].[v_fact_revenue_goal]
AS select fact_revenue_goal.dim_club_key dim_club_key,
       fact_revenue_goal.dim_reporting_hierarchy_key dim_reporting_hierarchy_key,
       fact_revenue_goal.goal_dollar_amount goal_dollar_amount,
       fact_revenue_goal.goal_effective_dim_date_key goal_effective_dim_date_key,
       fact_revenue_goal.local_currency_monthly_average_dim_exchange_rate_key local_currency_monthly_average_dim_exchange_rate_key,
       fact_revenue_goal.original_currency_code original_currency_code,
       fact_revenue_goal.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key
  from dbo.fact_revenue_goal;