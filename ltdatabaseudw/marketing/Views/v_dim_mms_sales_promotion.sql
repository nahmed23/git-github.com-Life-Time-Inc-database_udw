CREATE VIEW [marketing].[v_dim_mms_sales_promotion]
AS select d_mms_sales_promotion.dim_mms_sales_promotion_key dim_mms_sales_promotion_key,
       d_mms_sales_promotion.sales_promotion_id sales_promotion_id,
       d_mms_sales_promotion.effective_from_date_time effective_from_date_time,
       d_mms_sales_promotion.effective_thru_date_time effective_thru_date_time,
       d_mms_sales_promotion.exclude_from_attrition_reporting_flag exclude_from_attrition_reporting_flag,
       d_mms_sales_promotion.exclude_my_health_check_flag exclude_my_health_check_flag,
       d_mms_sales_promotion.sales_promotion_display_text sales_promotion_display_text,
       d_mms_sales_promotion.sales_promotion_receipt_text sales_promotion_receipt_text,
       d_mms_sales_promotion.val_revenue_reporting_category_id val_revenue_reporting_category_id,
       d_mms_sales_promotion.val_sales_promotion_type_id val_sales_promotion_type_id,
       d_mms_sales_promotion.val_sales_reporting_category_id val_sales_reporting_category_id
  from dbo.d_mms_sales_promotion;