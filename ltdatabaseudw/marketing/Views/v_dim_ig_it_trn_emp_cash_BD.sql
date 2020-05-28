CREATE VIEW [marketing].[v_dim_ig_it_trn_emp_cash_BD]
AS select d_ig_it_trn_emp_cash_BD.dim_ig_it_trn_emp_cash_BD_key dim_ig_it_trn_emp_cash_BD_key,
       d_ig_it_trn_emp_cash_BD.bus_day_id bus_day_id,
       d_ig_it_trn_emp_cash_BD.cashier_emp_id cashier_emp_id,
       d_ig_it_trn_emp_cash_BD.meal_period_id meal_period_id,
       d_ig_it_trn_emp_cash_BD.profit_center_id profit_center_id,
       d_ig_it_trn_emp_cash_BD.tender_id tender_id,
       d_ig_it_trn_emp_cash_BD.cash_drop_amount cash_drop_amount,
       d_ig_it_trn_emp_cash_BD.loan_amount loan_amount,
       d_ig_it_trn_emp_cash_BD.paid_out_amount paid_out_amount,
       d_ig_it_trn_emp_cash_BD.withdrawal_amount withdrawal_amount
  from dbo.d_ig_it_trn_emp_cash_BD;