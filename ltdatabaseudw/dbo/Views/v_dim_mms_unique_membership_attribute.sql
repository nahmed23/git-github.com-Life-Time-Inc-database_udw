CREATE VIEW [dbo].[v_dim_mms_unique_membership_attribute]
AS select dim_mms_membership_attribute.d_mms_membership_attribute_id,
       dim_mms_membership_attribute.membership_attribute_id,
       dim_mms_membership_attribute.dim_mms_membership_key,
       dim_mms_membership_attribute.dim_mms_membership_attribute_key,
       dim_mms_membership_attribute.effective_from_date_time,
       dim_mms_membership_attribute.effective_thru_date_time, 
       dim_mms_membership_attribute.membership_attribute_value,
       dim_mms_membership_attribute.val_membership_attribute_type_id 
  from dbo.d_mms_membership_attribute dim_mms_membership_attribute
  join (select max(membership_attribute_id) membership_attribute_id
         from dbo.d_mms_membership_attribute
        group by dim_mms_membership_key, val_membership_attribute_type_id) max_membership_attribute
    on dim_mms_membership_attribute.membership_attribute_id = max_membership_attribute.membership_attribute_id;