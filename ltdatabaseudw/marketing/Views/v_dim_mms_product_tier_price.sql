CREATE VIEW [marketing].[v_dim_mms_product_tier_price]
AS select d_mms_product_tier_price.dim_mms_product_tier_price_key dim_mms_product_tier_price_key,
       d_mms_product_tier_price.product_tier_price_id product_tier_price_id,
       d_mms_product_tier_price.card_level_dim_description_key card_level_dim_description_key,
       d_mms_product_tier_price.dim_mms_product_tier_key dim_mms_product_tier_key,
       d_mms_product_tier_price.inserted_date_time inserted_date_time,
       d_mms_product_tier_price.inserted_dim_date_key inserted_dim_date_key,
       d_mms_product_tier_price.inserted_dim_time_key inserted_dim_time_key,
       d_mms_product_tier_price.membership_type_group_dim_description_key membership_type_group_dim_description_key,
       d_mms_product_tier_price.price price,
       d_mms_product_tier_price.product_tier_id product_tier_id,
       d_mms_product_tier_price.updated_date_time updated_date_time,
       d_mms_product_tier_price.updated_dim_date_key updated_dim_date_key,
       d_mms_product_tier_price.updated_dim_time_key updated_dim_time_key,
       d_mms_product_tier_price.val_card_level_id val_card_level_id,
       d_mms_product_tier_price.val_membership_type_group_id val_membership_type_group_id
  from dbo.d_mms_product_tier_price;