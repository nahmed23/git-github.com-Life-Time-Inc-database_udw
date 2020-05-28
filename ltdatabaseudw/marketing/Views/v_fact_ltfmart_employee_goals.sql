CREATE VIEW [marketing].[v_fact_ltfmart_employee_goals] AS select fact_ltfmart_employee_goals.fact_ltfmart_employee_goals_key fact_ltfmart_employee_goals_key,
       fact_ltfmart_employee_goals.employee_goals_id employee_goals_id,
       fact_ltfmart_employee_goals.fact_employee_goals_club_key fact_employee_goals_club_key,
       fact_ltfmart_employee_goals.fact_employee_goals_employee_key fact_employee_goals_employee_key,
       fact_ltfmart_employee_goals.goal_dollar_amount goal_dollar_amount,
       fact_ltfmart_employee_goals.goal_first_of_month_date goal_first_of_month_date,
       fact_ltfmart_employee_goals.goal_first_of_month_dim_date_key goal_first_of_month_dim_date_key,
       fact_ltfmart_employee_goals.goal_name goal_name,
       fact_ltfmart_employee_goals.goal_quantity goal_quantity
  from dbo.fact_ltfmart_employee_goals;