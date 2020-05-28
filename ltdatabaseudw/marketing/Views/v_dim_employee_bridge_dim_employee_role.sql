CREATE VIEW [marketing].[v_dim_employee_bridge_dim_employee_role] AS select d_mms_employee_role.employee_role_id employee_role_id,
       d_mms_employee_role.assistant_department_head_sales_for_net_units_flag assistant_department_head_sales_for_net_units_flag,
       d_mms_employee_role.department_head_sales_for_net_units_flag department_head_sales_for_net_units_flag,
       d_mms_employee_role.dim_employee_key dim_employee_key,
       d_mms_employee_role.dim_employee_role_key dim_employee_role_key,
       d_mms_employee_role.employee_id employee_id,
       d_mms_employee_role.primary_employee_role_flag primary_employee_role_flag,
       d_mms_employee_role.sales_group_flag sales_group_flag,
       d_mms_employee_role.sales_manager_flag sales_manager_flag
  from d_mms_employee_role
 where isnull(d_mms_employee_role.deleted_flag,0) = 0;