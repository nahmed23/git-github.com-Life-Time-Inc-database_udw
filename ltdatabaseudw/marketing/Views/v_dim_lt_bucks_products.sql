CREATE VIEW [marketing].[v_dim_lt_bucks_products] AS select d_lt_bucks_products.dim_products_key dim_products_key,
       d_lt_bucks_products.product_id product_id,
       d_lt_bucks_products.created_date_time created_date_time,
       d_lt_bucks_products.date_updated date_updated,
       d_lt_bucks_products.last_modified_timestamp last_modified_timestamp,
       d_lt_bucks_products.price price,
       d_lt_bucks_products.product_active_flag product_active_flag,
       d_lt_bucks_products.product_description product_description,
       d_lt_bucks_products.product_is_soft_deleted_flag product_is_soft_deleted_flag,
       d_lt_bucks_products.product_name product_name,
       d_lt_bucks_products.product_per product_per,
       d_lt_bucks_products.sku sku
  from dbo.d_lt_bucks_products;