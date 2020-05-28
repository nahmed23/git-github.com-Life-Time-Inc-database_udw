CREATE VIEW [marketing].[v_dim_mms_product_tier_price_history]
AS select d_mms_product_tier_price_history.dim_mms_product_tier_price_key dim_mms_product_tier_price_key,
       d_mms_product_tier_price_history.product_tier_price_id product_tier_price_id,
       d_mms_product_tier_price_history.effective_date_time effective_date_time,
       d_mms_product_tier_price_history.expiration_date_time expiration_date_time,
       d_mms_product_tier_price_history.card_level_dim_description_key card_level_dim_description_key,
       d_mms_product_tier_price_history.dim_mms_product_tier_key dim_mms_product_tier_key,
       d_mms_product_tier_price_history.inserted_date_time inserted_date_time,
       d_mms_product_tier_price_history.inserted_dim_date_key inserted_dim_date_key,
       d_mms_product_tier_price_history.inserted_dim_time_key inserted_dim_time_key,
       d_mms_product_tier_price_history.membership_type_group_dim_description_key membership_type_group_dim_description_key,
       d_mms_product_tier_price_history.price price,
       d_mms_product_tier_price_history.product_tier_id product_tier_id,
       d_mms_product_tier_price_history.updated_date_time updated_date_time,
       d_mms_product_tier_price_history.updated_dim_date_key updated_dim_date_key,
       d_mms_product_tier_price_history.updated_dim_time_key updated_dim_time_key,
       d_mms_product_tier_price_history.val_card_level_id val_card_level_id,
       d_mms_product_tier_price_history.val_membership_type_group_id val_membership_type_group_id
  from dbo.d_mms_product_tier_price_history;