CREATE VIEW [marketing].[v_dim_mms_ltf_resource]
AS select d_mms_ltf_resource.ltf_resource_id ltf_resource_id,
       d_mms_ltf_resource.inserted_date_time inserted_date_time,
       d_mms_ltf_resource.inserted_dim_date_key inserted_dim_date_key,
       d_mms_ltf_resource.inserted_dim_time_key inserted_dim_time_key,
       d_mms_ltf_resource.ltf_resource_identifier ltf_resource_identifier,
       d_mms_ltf_resource.ltf_resource_name ltf_resource_name,
       d_mms_ltf_resource.updated_date_time updated_date_time,
       d_mms_ltf_resource.updated_dim_date_key updated_dim_date_key,
       d_mms_ltf_resource.updated_dim_time_key updated_dim_time_key,
       d_mms_ltf_resource.val_resource_type_id val_resource_type_id
  from dbo.d_mms_ltf_resource;