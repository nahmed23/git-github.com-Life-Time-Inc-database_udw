CREATE VIEW [marketing].[v_fact_magento_allocated_transaction_item]
AS select fact_magento_transaction_item.allocated_amount allocated_amount,
       fact_magento_transaction_item.allocated_month_starting_dim_date_key allocated_month_starting_dim_date_key,
       fact_magento_transaction_item.transaction_quantity allocated_quantity,
       fact_magento_transaction_item.dim_employee_key commissioned_sales_dim_employee_key,
       fact_magento_transaction_item.allocated_dim_club_key dim_club_key,
       fact_magento_transaction_item.dim_magento_product_key dim_magento_product_key,
       fact_magento_transaction_item.dim_mms_member_key dim_mms_member_key,
       fact_magento_transaction_item.dim_mms_membership_key dim_mms_membership_key,
       dim_magento_product.dim_reporting_hierarchy_key dim_reporting_hierarchy_key,
       fact_magento_transaction_item.transaction_discount_amount discount_amount,
       null fact_magento_invoice_item_key,
       null fact_magento_refund_item_key,
       fact_magento_transaction_item.fact_magento_transaction_item_key fact_magento_transaction_item_key,
       fact_magento_transaction_item.transaction_dim_date_key invoice_dim_date_key,
       fact_magento_transaction_item.order_id order_id,
       fact_magento_transaction_item.order_item_id order_item_id,
       fact_magento_transaction_item.order_number order_number,
       fact_magento_transaction_item.original_currency_code original_currency_code,
       fact_magento_transaction_item.product_cost product_cost,
       fact_magento_transaction_item.refund_flag refund_flag,
       case when fact_magento_transaction_item.refund_flag = 'Y' then'N' else 'Y' end sale_flag,
       fact_magento_transaction_item.shipping_amount shipping_and_handling_amount,
       fact_magento_transaction_item.transaction_amount transaction_amount,
       fact_magento_transaction_item.transaction_quantity transaction_quantity,
       fact_magento_transaction_item.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
       fact_magento_transaction_item.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key
  from marketing.v_fact_magento_transaction_item fact_magento_transaction_item
  join marketing.v_dim_magento_product_history dim_magento_product 
    on fact_magento_transaction_item.dim_magento_product_key = dim_magento_product.dim_magento_product_key
   and dim_magento_product.effective_date_time < fact_magento_transaction_item.allocated_recalculate_through_datetime
   and dim_magento_product.expiration_date_time >= fact_magento_transaction_item.allocated_recalculate_through_datetime
   and dim_magento_product.dim_magento_product_key not in ('-999','-998','-997')
 where dim_magento_product.reporting_product_group <> '';