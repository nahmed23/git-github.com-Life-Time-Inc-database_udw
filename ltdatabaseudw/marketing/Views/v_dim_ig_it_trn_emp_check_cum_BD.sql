CREATE VIEW [marketing].[v_dim_ig_it_trn_emp_check_cum_BD]
AS select d_ig_it_trn_emp_check_cum_BD.dim_ig_it_trn_emp_check_cum_BD_key dim_ig_it_trn_emp_check_cum_BD_key,
       d_ig_it_trn_emp_check_cum_BD.bus_day_id bus_day_id,
       d_ig_it_trn_emp_check_cum_BD.check_type_id check_type_id,
       d_ig_it_trn_emp_check_cum_BD.meal_period_id meal_period_id,
       d_ig_it_trn_emp_check_cum_BD.profit_center_id profit_center_id,
       d_ig_it_trn_emp_check_cum_BD.server_emp_id server_emp_id,
       d_ig_it_trn_emp_check_cum_BD.void_type_id void_type_id,
       d_ig_it_trn_emp_check_cum_BD.number_checks number_checks,
       d_ig_it_trn_emp_check_cum_BD.number_covers number_covers
  from dbo.d_ig_it_trn_emp_check_cum_BD;