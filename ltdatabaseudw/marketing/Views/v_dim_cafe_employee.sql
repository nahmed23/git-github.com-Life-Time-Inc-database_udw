CREATE VIEW [marketing].[v_dim_cafe_employee]
AS select d_ig_it_cfg_emp_master.dim_cafe_employee_key dim_cafe_employee_key,
       d_ig_it_cfg_emp_master.emp_id emp_id,
       d_ig_it_cfg_emp_master.emp_card_number emp_card_number,
       d_ig_it_cfg_emp_master.emp_first_name emp_first_name,
       d_ig_it_cfg_emp_master.emp_hire_date emp_hire_date,
       d_ig_it_cfg_emp_master.emp_hire_dim_date_key emp_hire_dim_date_key,
       d_ig_it_cfg_emp_master.emp_hire_dim_time_key emp_hire_dim_time_key,
       d_ig_it_cfg_emp_master.emp_last_name emp_last_name,
       d_ig_it_cfg_emp_master.emp_pos_name emp_pos_name,
       d_ig_it_cfg_emp_master.emp_terminate_date emp_terminate_date,
       d_ig_it_cfg_emp_master.emp_terminate_dim_date_key emp_terminate_dim_date_key,
       d_ig_it_cfg_emp_master.emp_terminate_dim_time_key emp_terminate_dim_time_key,
       d_ig_it_cfg_emp_master.store_id store_id,
       d_ig_it_cfg_emp_master.supervisor_dim_cafe_employee_key supervisor_dim_cafe_employee_key,
       d_ig_it_cfg_emp_master.supervisor_emp_id supervisor_emp_id
  from dbo.d_ig_it_cfg_emp_master;