CREATE VIEW [marketing].[v_dim_nmo_hub_task_meta]
AS select d_nmo_hub_task_meta.hub_task_meta_id hub_task_meta_id,
       d_nmo_hub_task_meta.created_date created_date,
       d_nmo_hub_task_meta.created_dim_date_key created_dim_date_key,
       d_nmo_hub_task_meta.created_dim_time_key created_dim_time_key,
       d_nmo_hub_task_meta.dim_nmo_hub_task_key dim_nmo_hub_task_key,
       d_nmo_hub_task_meta.meta_description meta_description,
       d_nmo_hub_task_meta.meta_key meta_key,
       d_nmo_hub_task_meta.updated_date updated_date,
       d_nmo_hub_task_meta.updated_dim_date_key updated_dim_date_key,
       d_nmo_hub_task_meta.updated_dim_time_key updated_dim_time_key
  from dbo.d_nmo_hub_task_meta;