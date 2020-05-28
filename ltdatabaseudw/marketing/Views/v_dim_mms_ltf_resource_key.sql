CREATE VIEW [marketing].[v_dim_mms_ltf_resource_key]
AS select d_mms_ltf_resource_key.ltf_resource_key_id ltf_resource_key_id,
       d_mms_ltf_resource_key.d_ltf_key_bk_hash d_ltf_key_bk_hash,
       d_mms_ltf_resource_key.d_ltf_resource_bk_hash d_ltf_resource_bk_hash,
       d_mms_ltf_resource_key.inserted_date_time inserted_date_time,
       d_mms_ltf_resource_key.inserted_dim_date_key inserted_dim_date_key,
       d_mms_ltf_resource_key.inserted_dim_time_key inserted_dim_time_key,
       d_mms_ltf_resource_key.ltf_key_id ltf_key_id,
       d_mms_ltf_resource_key.ltf_resource_id ltf_resource_id,
       d_mms_ltf_resource_key.updated_date_time updated_date_time,
       d_mms_ltf_resource_key.updated_dim_date_key updated_dim_date_key,
       d_mms_ltf_resource_key.updated_dim_time_key updated_dim_time_key
  from dbo.d_mms_ltf_resource_key;