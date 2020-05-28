CREATE VIEW [marketing].[v_dim_mms_membership_product_tier_history]
AS select d_mms_membership_product_tier_history.dim_membership_bridge_dim_mms_product_tier_key dim_membership_bridge_dim_mms_product_tier_key,
       d_mms_membership_product_tier_history.membership_product_tier_id membership_product_tier_id,
       d_mms_membership_product_tier_history.effective_date_time effective_date_time,
       d_mms_membership_product_tier_history.expiration_date_time expiration_date_time,
       d_mms_membership_product_tier_history.dim_mms_membership_key dim_mms_membership_key,
       d_mms_membership_product_tier_history.dim_mms_product_tier_key dim_mms_product_tier_key
  from dbo.d_mms_membership_product_tier_history;