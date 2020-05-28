CREATE VIEW [marketing].[v_dim_nmo_hub_task_type]
AS select d_nmo_hub_task_type.hub_task_type_id hub_task_type_id,
       d_nmo_hub_task_type.created_dim_date_key created_dim_date_key,
       d_nmo_hub_task_type.created_dim_time_key created_dim_time_key,
       d_nmo_hub_task_type.description description,
       d_nmo_hub_task_type.title title,
       d_nmo_hub_task_type.updated_dim_date_key updated_dim_date_key,
       d_nmo_hub_task_type.updated_dim_time_key updated_dim_time_key
  from dbo.d_nmo_hub_task_type;