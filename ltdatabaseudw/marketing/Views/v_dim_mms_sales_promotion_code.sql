CREATE VIEW [marketing].[v_dim_mms_sales_promotion_code]
AS select d_mms_sales_promotion_code.sales_promotion_code_id sales_promotion_code_id,
       d_mms_sales_promotion_code.dim_mms_member_key dim_mms_member_key,
       d_mms_sales_promotion_code.dim_mms_sales_promotion_key dim_mms_sales_promotion_key,
       d_mms_sales_promotion_code.display_ui_flag display_ui_flag,
       d_mms_sales_promotion_code.expiration_dim_date_key expiration_dim_date_key,
       d_mms_sales_promotion_code.expiration_dim_time_key expiration_dim_time_key,
       d_mms_sales_promotion_code.inserted_dim_date_key inserted_dim_date_key,
       d_mms_sales_promotion_code.inserted_dim_time_key inserted_dim_time_key,
       d_mms_sales_promotion_code.member_id member_id,
       d_mms_sales_promotion_code.notify_email_address notify_email_address,
       d_mms_sales_promotion_code.number_of_code_recipients number_of_code_recipients,
       d_mms_sales_promotion_code.promotion_code promotion_code,
       d_mms_sales_promotion_code.sales_promotion_code_usage_limit sales_promotion_code_usage_limit,
       d_mms_sales_promotion_code.sales_promotion_id sales_promotion_id,
       d_mms_sales_promotion_code.updated_dim_date_key updated_dim_date_key,
       d_mms_sales_promotion_code.updated_dim_time_key updated_dim_time_key
  from dbo.d_mms_sales_promotion_code;