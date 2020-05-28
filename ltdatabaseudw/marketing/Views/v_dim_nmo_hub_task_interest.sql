CREATE VIEW [marketing].[v_dim_nmo_hub_task_interest]
AS select d_nmo_hub_task_interest.hub_task_interest_id hub_task_interest_id,
       d_nmo_hub_task_interest.activation_date activation_date,
       d_nmo_hub_task_interest.activation_dim_date_key activation_dim_date_key,
       d_nmo_hub_task_interest.activation_dim_time_key activation_dim_time_key,
       d_nmo_hub_task_interest.created_date created_date,
       d_nmo_hub_task_interest.created_dim_date_key created_dim_date_key,
       d_nmo_hub_task_interest.created_dim_time_key created_dim_time_key,
       d_nmo_hub_task_interest.dim_nmo_hub_task_department_key dim_nmo_hub_task_department_key,
       d_nmo_hub_task_interest.expiration_date expiration_date,
       d_nmo_hub_task_interest.expiration_dim_date_key expiration_dim_date_key,
       d_nmo_hub_task_interest.expiration_dim_time_key expiration_dim_time_key,
       d_nmo_hub_task_interest.title title,
       d_nmo_hub_task_interest.updated_date updated_date,
       d_nmo_hub_task_interest.updated_dim_date_key updated_dim_date_key,
       d_nmo_hub_task_interest.updated_dim_time_key updated_dim_time_key
  from dbo.d_nmo_hub_task_interest;