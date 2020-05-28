CREATE VIEW [marketing].[v_dim_mms_card_level_price_range]
AS select d_mms_card_level_price_range.card_level_price_range_id card_level_price_range_id,
       d_mms_card_level_price_range.card_level_dim_description_key card_level_dim_description_key,
       d_mms_card_level_price_range.dim_mms_product_key dim_mms_product_key,
       d_mms_card_level_price_range.ending_price ending_price,
       d_mms_card_level_price_range.inserted_date_time inserted_date_time,
       d_mms_card_level_price_range.inserted_dim_date_key inserted_dim_date_key,
       d_mms_card_level_price_range.inserted_dim_time_key inserted_dim_time_key,
       d_mms_card_level_price_range.product_id product_id,
       d_mms_card_level_price_range.starting_price starting_price,
       d_mms_card_level_price_range.updated_date_time updated_date_time,
       d_mms_card_level_price_range.updated_dim_date_key updated_dim_date_key,
       d_mms_card_level_price_range.updated_dim_time_key updated_dim_time_key,
       d_mms_card_level_price_range.val_card_level_id val_card_level_id
  from dbo.d_mms_card_level_price_range;