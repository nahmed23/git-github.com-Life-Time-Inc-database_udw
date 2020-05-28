CREATE VIEW [marketing].[v_dim_mms_resource_usage]
AS select d_mms_resource_usage.resource_usage_id resource_usage_id,
       d_mms_resource_usage.d_mms_ltf_key_owner_bk_hash d_mms_ltf_key_owner_bk_hash,
       d_mms_resource_usage.d_mms_ltf_resource_bk_hash d_mms_ltf_resource_bk_hash,
       d_mms_resource_usage.inserted_date_time inserted_date_time,
       d_mms_resource_usage.inserted_dim_date_key inserted_dim_date_key,
       d_mms_resource_usage.inserted_dim_time_key inserted_dim_time_key,
       d_mms_resource_usage.ltf_key_owner_id ltf_key_owner_id,
       d_mms_resource_usage.ltf_resource_id ltf_resource_id,
       d_mms_resource_usage.party_id party_id,
       d_mms_resource_usage.ref_val_resource_usage_source_type_id ref_val_resource_usage_source_type_id,
       d_mms_resource_usage.updated_date_time updated_date_time,
       d_mms_resource_usage.updated_dim_date_key updated_dim_date_key,
       d_mms_resource_usage.updated_dim_time_key updated_dim_time_key,
       d_mms_resource_usage.usage_date_time usage_date_time,
       d_mms_resource_usage.usage_date_time_zone usage_date_time_zone,
       d_mms_resource_usage.usage_dim_date_key usage_dim_date_key,
       d_mms_resource_usage.usage_dim_time_key usage_dim_time_key
  from dbo.d_mms_resource_usage;