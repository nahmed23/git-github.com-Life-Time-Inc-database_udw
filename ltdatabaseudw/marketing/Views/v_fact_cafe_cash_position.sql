CREATE VIEW [marketing].[v_fact_cafe_cash_position]
AS select fact_cafe_cash_position.accountable_cash accountable_cash,
       fact_cafe_cash_position.cash_drop_amount cash_drop_amount,
       fact_cafe_cash_position.cashier_dim_cafe_employee_key cashier_dim_cafe_employee_key,
       fact_cafe_cash_position.dim_cafe_business_day_dates_key dim_cafe_business_day_dates_key,
       fact_cafe_cash_position.dim_cafe_meal_period_key dim_cafe_meal_period_key,
       fact_cafe_cash_position.dim_cafe_profit_center_key dim_cafe_profit_center_key,
       fact_cafe_cash_position.fact_cafe_cash_position_key fact_cafe_cash_position_key,
       fact_cafe_cash_position.loan_amount loan_amount,
       fact_cafe_cash_position.net_cash_tender_amount net_cash_tender_amount,
       fact_cafe_cash_position.over_short_amount over_short_amount,
       fact_cafe_cash_position.paid_tips paid_tips,
       fact_cafe_cash_position.withdrawal_amount withdrawal_amount
  from dbo.fact_cafe_cash_position;