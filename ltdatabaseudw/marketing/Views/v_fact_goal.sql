CREATE VIEW [marketing].[v_fact_goal]
AS select fact_goal.club_code club_code,
       fact_goal.club_id club_id,
       fact_goal.description description,
       fact_goal.dim_club_key dim_club_key,
       fact_goal.dim_goal_line_item_key dim_goal_line_item_key,
       fact_goal.goal_dollar_amount goal_dollar_amount,
       fact_goal.goal_effective_dim_date_key goal_effective_dim_date_key,
       fact_goal.goal_quantity goal_quantity,
       fact_goal.local_currency_monthly_average_dim_exchange_rate_key local_currency_monthly_average_dim_exchange_rate_key,
       fact_goal.original_currency_code original_currency_code,
       fact_goal.percentage percentage,
       fact_goal.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key
  from dbo.fact_goal;