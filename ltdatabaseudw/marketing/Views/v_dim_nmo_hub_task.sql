﻿CREATE VIEW [marketing].[v_dim_nmo_hub_task]
AS select d_nmo_hub_task.dim_nmo_hub_task_key dim_nmo_hub_task_key,
       d_nmo_hub_task.hub_task_id hub_task_id,
       d_nmo_hub_task.assignee_name assignee_name,
       d_nmo_hub_task.assignee_party_id assignee_party_id,
       d_nmo_hub_task.created_date created_date,
       d_nmo_hub_task.created_dim_date_key created_dim_date_key,
       d_nmo_hub_task.created_dim_time_key created_dim_time_key,
       d_nmo_hub_task.creator_name creator_name,
       d_nmo_hub_task.creator_party_id creator_party_id,
       d_nmo_hub_task.dim_club_key dim_club_key,
       d_nmo_hub_task.dim_nmo_hub_task_department_key dim_nmo_hub_task_department_key,
       d_nmo_hub_task.dim_nmo_hub_task_status_key dim_nmo_hub_task_status_key,
       d_nmo_hub_task.dim_nmo_hub_task_type_key dim_nmo_hub_task_type_key,
       d_nmo_hub_task.due_date due_date,
       d_nmo_hub_task.due_dim_date_key due_dim_date_key,
       d_nmo_hub_task.due_dim_time_key due_dim_time_key,
       d_nmo_hub_task.party_id party_id,
       d_nmo_hub_task.priority priority,
       d_nmo_hub_task.resolution_date resolution_date,
       d_nmo_hub_task.resolution_dim_date_key resolution_dim_date_key,
       d_nmo_hub_task.resolution_dim_time_key resolution_dim_time_key,
       d_nmo_hub_task.title title,
       d_nmo_hub_task.updated_date updated_date,
       d_nmo_hub_task.updated_dim_date_key updated_dim_date_key,
       d_nmo_hub_task.updated_dim_time_key updated_dim_time_key
  from dbo.d_nmo_hub_task;