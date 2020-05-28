CREATE VIEW [marketing].[v_dim_ig_it_trn_emp_shift_cash_BD]
AS select d_ig_it_trn_emp_shift_cash_BD.dim_ig_it_trn_emp_shift_cash_BD_key dim_ig_it_trn_emp_shift_cash_BD_key,
       d_ig_it_trn_emp_shift_cash_BD.bus_day_id bus_day_id,
       d_ig_it_trn_emp_shift_cash_BD.cash_shift_id cash_shift_id,
       d_ig_it_trn_emp_shift_cash_BD.emp_id emp_id,
       d_ig_it_trn_emp_shift_cash_BD.tender_id tender_id,
       d_ig_it_trn_emp_shift_cash_BD.breakage_amount breakage_amount,
       d_ig_it_trn_emp_shift_cash_BD.cash_drop_amount cash_drop_amount,
       d_ig_it_trn_emp_shift_cash_BD.change_amount change_amount,
       d_ig_it_trn_emp_shift_cash_BD.loan_amount loan_amount,
       d_ig_it_trn_emp_shift_cash_BD.number_tendered_checks number_tendered_checks,
       d_ig_it_trn_emp_shift_cash_BD.paid_out_amount paid_out_amount,
       d_ig_it_trn_emp_shift_cash_BD.received_current_amount received_current_amount,
       d_ig_it_trn_emp_shift_cash_BD.tender_amount tender_amount,
       d_ig_it_trn_emp_shift_cash_BD.tender_quantity tender_quantity,
       d_ig_it_trn_emp_shift_cash_BD.withdrawal_amount withdrawal_amount
  from dbo.d_ig_it_trn_emp_shift_cash_BD;