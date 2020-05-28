CREATE VIEW [marketing].[v_dim_mms_department] AS select d_mms_department.dim_mms_department_key dim_mms_department_key,
       d_mms_department.department_id department_id,
       d_mms_department.description description,
       d_mms_department.name name,
       d_mms_department.sort_order sort_order
  from dbo.d_mms_department;