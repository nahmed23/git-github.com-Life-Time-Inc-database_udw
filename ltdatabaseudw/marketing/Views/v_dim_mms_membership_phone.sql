CREATE VIEW [marketing].[v_dim_mms_membership_phone]
AS select d_mms_membership_phone.dim_mms_membership_phone_key dim_mms_membership_phone_key,
       d_mms_membership_phone.membership_phone_id membership_phone_id,
       d_mms_membership_phone.area_code area_code,
       d_mms_membership_phone.dim_mms_membership_key dim_mms_membership_key,
       d_mms_membership_phone.number number,
       d_mms_membership_phone.phone_type_dim_description_key phone_type_dim_description_key
  from dbo.d_mms_membership_phone;