CREATE VIEW [marketing].[v_dim_nmo_hub_task_audit]
AS select d_nmo_hub_task_audit.hub_task_audit_id hub_task_audit_id,
       d_nmo_hub_task_audit.created_date created_date,
       d_nmo_hub_task_audit.created_dim_date_key created_dim_date_key,
       d_nmo_hub_task_audit.created_dim_time_key created_dim_time_key,
       d_nmo_hub_task_audit.dim_nmo_hub_task_key dim_nmo_hub_task_key,
       d_nmo_hub_task_audit.field field,
       d_nmo_hub_task_audit.modified_name modified_name,
       d_nmo_hub_task_audit.modified_party_id modified_party_id,
       d_nmo_hub_task_audit.new_value new_value,
       d_nmo_hub_task_audit.old_value old_value,
       d_nmo_hub_task_audit.operation operation,
       d_nmo_hub_task_audit.updated_date updated_date,
       d_nmo_hub_task_audit.updated_dim_date_key updated_dim_date_key,
       d_nmo_hub_task_audit.updated_dim_time_key updated_dim_time_key
  from dbo.d_nmo_hub_task_audit;