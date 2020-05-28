CREATE VIEW [marketing].[v_dim_fitmetrix_activity] AS select d_fitmetrix_api_activities.dim_fitmetrix_activity_key dim_fitmetrix_activity_key,
       d_fitmetrix_api_activities.activity_id activity_id,
       d_fitmetrix_api_activities.activity_added_dim_date_key activity_added_dim_date_key,
       d_fitmetrix_api_activities.activity_name activity_name,
       d_fitmetrix_api_activities.activity_type_id activity_type_id,
       d_fitmetrix_api_activities.external_id external_id,
       d_fitmetrix_api_activities.is_deleted_flag is_deleted_flag,
       d_fitmetrix_api_activities.position position
  from dbo.d_fitmetrix_api_activities;