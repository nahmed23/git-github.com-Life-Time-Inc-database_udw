CREATE VIEW [marketing].[v_dim_mms_membership_type_attribute]
AS select d_mms_membership_type_attribute.membership_type_attribute_id membership_type_attribute_id,
       d_mms_membership_type_attribute.dim_mms_membership_type_key dim_mms_membership_type_key,
       d_mms_membership_type_attribute.inserted_dim_date_key inserted_dim_date_key,
       d_mms_membership_type_attribute.inserted_dim_time_key inserted_dim_time_key,
       d_mms_membership_type_attribute.membership_type_id membership_type_id,
       d_mms_membership_type_attribute.updated_dim_date_key updated_dim_date_key,
       d_mms_membership_type_attribute.updated_dim_time_key updated_dim_time_key,
       d_mms_membership_type_attribute.val_membership_type_attribute_id val_membership_type_attribute_id,
       d_mms_membership_type_attribute.val_membership_type_attribute_key val_membership_type_attribute_key
  from dbo.d_mms_membership_type_attribute;