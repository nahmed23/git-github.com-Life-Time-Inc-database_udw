CREATE VIEW [marketing].[v_dim_mms_unique_membership_attribute]
AS select d_mms_membership_attribute.dim_mms_membership_attribute_key dim_mms_membership_attribute_key,
       d_mms_membership_attribute.membership_attribute_id membership_attribute_id,
       d_mms_membership_attribute.dim_mms_membership_key dim_mms_membership_key,
       d_mms_membership_attribute.effective_from_date_time effective_from_date_time,
       d_mms_membership_attribute.effective_thru_date_time effective_thru_date_time,
       d_mms_membership_attribute.membership_attribute_value membership_attribute_value,
       d_mms_membership_attribute.val_membership_attribute_type_id val_membership_attribute_type_id
  from dbo.d_mms_membership_attribute;