CREATE VIEW [marketing].[v_dim_employee_role] AS select dim_employee_role.val_employee_role_id val_employee_role_id,
       dim_employee_role.commissionable_flag commissionable_flag,
       dim_employee_role.dim_employee_role_key dim_employee_role_key,
       dim_employee_role.mms_department_name mms_department_name,
       dim_employee_role.role_name role_name
  from dbo.dim_employee_role;