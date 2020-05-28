CREATE VIEW [marketing].[v_fact_lt_bucks_cart_details] AS select d_lt_bucks_cart_details.fact_lt_bucks_cart_details_key fact_lt_bucks_cart_details_key,
       d_lt_bucks_cart_details.cdetail_id cdetail_id,
       d_lt_bucks_cart_details.cart_id cart_id,
       d_lt_bucks_cart_details.delivery_date_time delivery_date_time,
       d_lt_bucks_cart_details.dim_club_key dim_club_key,
       d_lt_bucks_cart_details.dim_lt_bucks_product_options_key dim_lt_bucks_product_options_key,
       d_lt_bucks_cart_details.fact_lt_bucks_shopping_cart_key fact_lt_bucks_shopping_cart_key,
       d_lt_bucks_cart_details.fact_mms_package_key fact_mms_package_key,
       d_lt_bucks_cart_details.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
       d_lt_bucks_cart_details.transaction_expiration_date_time transaction_expiration_date_time
  from dbo.d_lt_bucks_cart_details;