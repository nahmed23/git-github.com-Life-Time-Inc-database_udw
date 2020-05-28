CREATE VIEW [marketing].[v_dim_mms_product_tier] AS select d_mms_product_tier.dim_product_tier_key dim_product_tier_key,
       d_mms_product_tier.product_tier_id product_tier_id,
       d_mms_product_tier.description description,
       d_mms_product_tier.dim_mms_product_key dim_mms_product_key,
       d_mms_product_tier.display_text display_text,
       d_mms_product_tier.val_product_tier_type_id val_product_tier_type_id
  from dbo.d_mms_product_tier;