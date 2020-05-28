CREATE VIEW [marketing].[v_dim_nmo_hub_task_notes]
AS select d_nmo_hub_task_notes.hub_task_notes_id hub_task_notes_id,
       d_nmo_hub_task_notes.created_date created_date,
       d_nmo_hub_task_notes.created_dim_date_key created_dim_date_key,
       d_nmo_hub_task_notes.created_dim_time_key created_dim_time_key,
       d_nmo_hub_task_notes.creator_name creator_name,
       d_nmo_hub_task_notes.creator_party_id creator_party_id,
       d_nmo_hub_task_notes.description description,
       d_nmo_hub_task_notes.dim_nmo_hub_task_key dim_nmo_hub_task_key,
       d_nmo_hub_task_notes.title title,
       d_nmo_hub_task_notes.updated_date updated_date,
       d_nmo_hub_task_notes.updated_dim_date_key updated_dim_date_key,
       d_nmo_hub_task_notes.updated_dim_time_key updated_dim_time_key
  from dbo.d_nmo_hub_task_notes;