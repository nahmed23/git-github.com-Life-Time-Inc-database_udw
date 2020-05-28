CREATE VIEW [marketing].[v_fact_cafe_employee_check_cumulative]
AS select d_ig_it_trn_emp_check_cum_BD.fact_cafe_employee_check_cumlative_key fact_cafe_employee_check_cumlative_key,
       d_ig_it_trn_emp_check_cum_BD.bus_day_id bus_day_id,
       d_ig_it_trn_emp_check_cum_BD.check_type_id check_type_id,
       d_ig_it_trn_emp_check_cum_BD.meal_period_id meal_period_id,
       d_ig_it_trn_emp_check_cum_BD.profit_center_id profit_center_id,
       d_ig_it_trn_emp_check_cum_BD.server_emp_id server_emp_id,
       d_ig_it_trn_emp_check_cum_BD.void_type_id void_type_id,
       d_ig_it_trn_emp_check_cum_BD.dim_cafe_business_day_dates_key dim_cafe_business_day_dates_key,
       d_ig_it_trn_emp_check_cum_BD.dim_cafe_check_type_key dim_cafe_check_type_key,
       d_ig_it_trn_emp_check_cum_BD.dim_cafe_meal_period_key dim_cafe_meal_period_key,
       d_ig_it_trn_emp_check_cum_BD.dim_cafe_profit_center_key dim_cafe_profit_center_key,
       d_ig_it_trn_emp_check_cum_BD.number_checks number_checks,
       d_ig_it_trn_emp_check_cum_BD.number_covers number_covers,
       d_ig_it_trn_emp_check_cum_BD.server_dim_employee_key server_dim_employee_key
  from dbo.d_ig_it_trn_emp_check_cum_BD;