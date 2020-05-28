CREATE VIEW [marketing].[v_fact_mms_membership_audit] AS select d_mms_membership_audit.fact_mms_membership_audit_key fact_mms_membership_audit_key,
       d_mms_membership_audit.membership_audit_id membership_audit_id,
       d_mms_membership_audit.modified_dim_date_key modified_dim_date_key,
       d_mms_membership_audit.modified_dim_employee_key modified_dim_employee_key,
       d_mms_membership_audit.modified_dim_time_key modified_dim_time_key,
       d_mms_membership_audit.new_value new_value,
       d_mms_membership_audit.old_value old_value,
       d_mms_membership_audit.source_column_name source_column_name,
       d_mms_membership_audit.source_row_key source_row_key,
       d_mms_membership_audit.update_flag update_flag
  from dbo.d_mms_membership_audit;