CREATE VIEW [marketing].[v_dim_nmo_hub_task_department]
AS select d_nmo_hub_task_department.hub_task_department_id hub_task_department_id,
       d_nmo_hub_task_department.activation_date activation_date,
       d_nmo_hub_task_department.activation_dim_date_key activation_dim_date_key,
       d_nmo_hub_task_department.activation_dim_time_key activation_dim_time_key,
       d_nmo_hub_task_department.created_date created_date,
       d_nmo_hub_task_department.created_dim_date_key created_dim_date_key,
       d_nmo_hub_task_department.created_dim_time_key created_dim_time_key,
       d_nmo_hub_task_department.expiration_date expiration_date,
       d_nmo_hub_task_department.expiration_dim_date_key expiration_dim_date_key,
       d_nmo_hub_task_department.expiration_dim_time_key expiration_dim_time_key,
       d_nmo_hub_task_department.title title,
       d_nmo_hub_task_department.updated_date updated_date,
       d_nmo_hub_task_department.updated_dim_date_key updated_dim_date_key,
       d_nmo_hub_task_department.updated_dim_time_key updated_dim_time_key
  from dbo.d_nmo_hub_task_department;