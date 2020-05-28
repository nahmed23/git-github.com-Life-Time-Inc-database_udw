CREATE VIEW [marketing].[v_dim_mms_merchant_number] AS select dim_mms_merchant_number.auto_reconcile_flag auto_reconcile_flag,
       dim_mms_merchant_number.business_area_dim_description_key business_area_dim_description_key,
       dim_mms_merchant_number.club_id club_id,
       dim_mms_merchant_number.club_merchant_number_id club_merchant_number_id,
       dim_mms_merchant_number.currency_code currency_code,
       dim_mms_merchant_number.dim_club_key dim_club_key,
       dim_mms_merchant_number.dim_mms_merchant_number_key dim_mms_merchant_number_key,
       dim_mms_merchant_number.merchant_location_number merchant_location_number,
       dim_mms_merchant_number.merchant_number merchant_number,
       dim_mms_merchant_number.val_business_area_id val_business_area_id
  from dbo.dim_mms_merchant_number;