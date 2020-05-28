CREATE VIEW [marketing].[v_fact_cafe_allocated_transaction_item]
AS select fact_cafe_sales_transaction_item.item_sales_dollar_amount_excluding_tax allocated_amount,
       fact_cafe_sales_transaction_item.allocated_month_starting_dim_date_key allocated_month_starting_dim_date_key,
       fact_cafe_sales_transaction_item.item_quantity allocated_quantity,
       fact_cafe_sales_transaction_item.check_seq check_seq,
       fact_cafe_sales_transaction_item.order_commissionable_dim_employee_key commissioned_sales_dim_employee_key,
       fact_cafe_sales_transaction_item.dim_cafe_product_key dim_cafe_product_key,
       fact_cafe_sales_transaction_item.dim_club_key dim_club_key,
       dim_cafe_product.dim_reporting_hierarchy_key dim_reporting_hierarchy_key,
       fact_cafe_sales_transaction_item.item_discount_amount discount_amount,
       fact_cafe_sales_transaction_item.fact_cafe_sales_transaction_item_key fact_cafe_sales_transaction_item_key,
       fact_cafe_sales_transaction_item.order_hdr_id order_hdr_id,
       fact_cafe_sales_transaction_item.original_currency_code original_currency_code,
       fact_cafe_sales_transaction_item.posted_business_end_dim_date_key posted_business_end_dim_date_key,
       case when fact_cafe_sales_transaction_item.order_refund_flag = 'Y' or fact_cafe_sales_transaction_item.item_refund_flag = 'Y' then 'Y' else 'N' end refund_flag,
       fact_cafe_sales_transaction_item.item_sales_dollar_amount_excluding_tax transaction_amount,
       fact_cafe_sales_transaction_item.order_close_dim_date_key transaction_close_dim_date_key,
       fact_cafe_sales_transaction_item.order_close_dim_time_key transaction_close_dim_time_key,
       fact_cafe_sales_transaction_item.item_quantity transaction_quantity,
       fact_cafe_sales_transaction_item.usd_dim_plan_exchange_rate usd_dim_plan_exchange_rate_key,
       fact_cafe_sales_transaction_item.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key
  from fact_cafe_sales_transaction_item
  join marketing.v_dim_cafe_product_history dim_cafe_product 
    on fact_cafe_sales_transaction_item.dim_cafe_product_key = dim_cafe_product.dim_cafe_product_key
   and dim_cafe_product.effective_date_time < fact_cafe_sales_transaction_item.allocated_recalculate_through_datetime
   and dim_cafe_product.expiration_date_time >= fact_cafe_sales_transaction_item.allocated_recalculate_through_datetime
   and dim_cafe_product.dim_cafe_product_key not in ('-999','-998','-997')
 where dim_cafe_product.reporting_product_group <> ''
   and (fact_cafe_sales_transaction_item.order_refund_flag = 'Y'
        or fact_cafe_sales_transaction_item.order_void_flag = 'N')
   and fact_cafe_sales_transaction_item.item_voided_flag = 'N';