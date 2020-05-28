CREATE VIEW [marketing].[v_fact_hybris_allocated_transaction_item]
AS select fact_hybris_transaction_item.transaction_amount allocated_amount,
       fact_hybris_transaction_item.allocated_month_starting_dim_date_key allocated_month_starting_dim_date_key,
       fact_hybris_transaction_item.transaction_quantity allocated_quantity,
       fact_hybris_transaction_item.auto_ship_flag autoship_flag,
       fact_hybris_transaction_item.sales_dim_employee_key commissioned_sales_dim_employee_key,
       fact_hybris_transaction_item.allocated_dim_club_key dim_club_key,
       fact_hybris_transaction_item.dim_hybris_product_key dim_hybris_product_key,
       fact_hybris_transaction_item.dim_mms_member_key dim_mms_member_key,
       fact_hybris_transaction_item.dim_mms_membership_key dim_mms_membership_key,
       dim_hybris_product.dim_reporting_hierarchy_key dim_reporting_hierarchy_key,
       fact_hybris_transaction_item.discount_amount discount_amount,
       fact_hybris_transaction_item.entry_number entry_number,
       fact_hybris_transaction_item.fact_hybris_transaction_item_key fact_hybris_transaction_item_key,
       fact_hybris_transaction_item.order_code order_code,
       fact_hybris_transaction_item.original_currency_code original_currency_code,
       fact_hybris_transaction_item.sales_dim_employee_key primary_sales_dim_employee_key,
       dim_hybris_product.product_cost product_cost,
       fact_hybris_transaction_item.refund_flag refund_flag,
       case when fact_hybris_transaction_item.refund_flag = 'Y' then'N' else 'Y' end sale_flag,
       fact_hybris_transaction_item.settlement_dim_date_key settlement_dim_date_key,
	   fact_hybris_transaction_item.settlement_dim_time_key settlement_dim_time_key,
       fact_hybris_transaction_item.shipping_and_handling_amount shipping_and_handling_amount,
       fact_hybris_transaction_item.transaction_amount_gross transaction_amount,
       fact_hybris_transaction_item.transaction_quantity transaction_quantity,
       fact_hybris_transaction_item.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
       fact_hybris_transaction_item.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key
  from fact_hybris_transaction_item
  join marketing.v_dim_hybris_product_history dim_hybris_product 
    on fact_hybris_transaction_item.dim_hybris_product_key = dim_hybris_product.dim_hybris_product_key
   and dim_hybris_product.effective_date_time < fact_hybris_transaction_item.allocated_recalculate_through_datetime
   and dim_hybris_product.expiration_date_time >= fact_hybris_transaction_item.allocated_recalculate_through_datetime
   and dim_hybris_product.dim_hybris_product_key not in ('-999','-998','-997')
 where dim_hybris_product.reporting_product_group <> '';