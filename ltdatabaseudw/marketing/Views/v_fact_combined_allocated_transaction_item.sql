CREATE VIEW [marketing].[v_fact_combined_allocated_transaction_item]
AS select 'Hybris' sales_source,
       fact_hybris_allocated_transaction_item.fact_hybris_transaction_item_key source_fact_table_key,
       fact_hybris_allocated_transaction_item.dim_club_key allocated_dim_club_key,
       fact_hybris_allocated_transaction_item.dim_hybris_product_key dim_product_key,
       fact_hybris_allocated_transaction_item.dim_mms_member_key,
       fact_hybris_allocated_transaction_item.dim_mms_membership_key,
       fact_hybris_allocated_transaction_item.primary_sales_dim_employee_key,
       fact_hybris_allocated_transaction_item.dim_reporting_hierarchy_key,
       fact_hybris_allocated_transaction_item.settlement_dim_date_key transaction_dim_date_key,
	   fact_hybris_allocated_transaction_item.settlement_dim_time_key transaction_dim_time_key, /*--added---	   */
       fact_hybris_allocated_transaction_item.allocated_month_starting_dim_date_key,
       fact_hybris_allocated_transaction_item.allocated_quantity,
       fact_hybris_allocated_transaction_item.allocated_amount,
       fact_hybris_allocated_transaction_item.transaction_quantity,
       fact_hybris_allocated_transaction_item.transaction_amount,
       fact_hybris_allocated_transaction_item.original_currency_code,
       cast(NULL as varchar(4000)) as payment_types,
       fact_hybris_allocated_transaction_item.discount_amount,
       cast('-998' as varchar(32)) as dim_mms_transaction_reason_key,
       case when fact_hybris_allocated_transaction_item.refund_flag = 'Y' then 'Refund'
            when fact_hybris_allocated_transaction_item.sale_flag = 'Y' then 'Sale'
            else 'Unknown'
        end transaction_type,
       cast('-998' as varchar(32)) as sales_channel_dim_description_key,
       cast(fact_hybris_allocated_transaction_item.order_code as varchar(255)) transaction_id,
       fact_hybris_allocated_transaction_item.entry_number line_number,
       fact_hybris_allocated_transaction_item.autoship_flag,
       0 shipping_and_handling_amount,
       fact_hybris_allocated_transaction_item.product_cost
from marketing.v_fact_hybris_allocated_transaction_item fact_hybris_allocated_transaction_item
union all
select 'Cafe' sales_source,
       fact_cafe_allocated_transaction_item.fact_cafe_sales_transaction_item_key,
       fact_cafe_allocated_transaction_item.dim_club_key allocated_dim_club_key,
       fact_cafe_allocated_transaction_item.dim_cafe_product_key dim_product_key,
       cast('-998' as varchar(32)) dim_mms_member_key,
       cast('-998' as varchar(32)) dim_mms_membership_key,
       fact_cafe_allocated_transaction_item.commissioned_sales_dim_employee_key primary_sales_dim_employee_key,
       fact_cafe_allocated_transaction_item.dim_reporting_hierarchy_key,
       fact_cafe_allocated_transaction_item.transaction_close_dim_date_key transaction_dim_date_key,
	   fact_cafe_allocated_transaction_item.transaction_close_dim_time_key transaction_dim_time_key, /*--added---	  */
       fact_cafe_allocated_transaction_item.allocated_month_starting_dim_date_key,
       fact_cafe_allocated_transaction_item.allocated_quantity,
       fact_cafe_allocated_transaction_item.allocated_amount,
       fact_cafe_allocated_transaction_item.transaction_quantity,
       fact_cafe_allocated_transaction_item.transaction_amount,
       fact_cafe_allocated_transaction_item.original_currency_code,
       cast(NULL as varchar(4000)) as payment_types,
       fact_cafe_allocated_transaction_item.discount_amount,
       cast('-998' as varchar(32)) as dim_mms_transaction_reason_key,
       case when fact_cafe_allocated_transaction_item.refund_flag = 'Y' then 'Refund' else 'Sale' end transaction_type,
       cast('-998' as varchar(32)) as sales_channel_dim_description_key,
       cast(fact_cafe_allocated_transaction_item.order_hdr_id as varchar(255)) transaction_id,
       fact_cafe_allocated_transaction_item.check_seq line_number,
       null autoship_flag,
       0 shipping_and_handling_amount,
       null product_cost
from marketing.v_fact_cafe_allocated_transaction_item fact_cafe_allocated_transaction_item
union all
select 'MMS' sales_source,
       fact_mms_allocated_transaction_item.fact_mms_allocated_transaction_item_key,
       fact_mms_allocated_transaction_item.dim_club_key allocated_dim_club_key,
       fact_mms_allocated_transaction_item.dim_mms_product_key dim_product_key,
       fact_mms_allocated_transaction_item.dim_mms_member_key,
       d_mms_member.dim_mms_membership_key,
       fact_mms_allocated_transaction_item.primary_sales_dim_employee_key,
       fact_mms_allocated_transaction_item.dim_reporting_hierarchy_key,
       fact_mms_allocated_transaction_item.transaction_post_dim_date_key transaction_dim_date_key,
	   fact_mms_allocated_transaction_item.transaction_post_dim_time_key transaction_dim_time_key, /*--added---	 */
       fact_mms_allocated_transaction_item.allocated_month_starting_dim_date_key,
       fact_mms_allocated_transaction_item.allocated_quantity,
       fact_mms_allocated_transaction_item.allocated_amount,
       fact_mms_allocated_transaction_item.transaction_quantity,
       fact_mms_allocated_transaction_item.transaction_amount,
       fact_mms_allocated_transaction_item.original_currency_code,
       cast(NULL as varchar(4000)) as payment_types,
       fact_mms_allocated_transaction_item.transaction_discount_dollar_amount discount_amount,
       fact_mms_allocated_transaction_item.dim_mms_transaction_reason_key,
       case when fact_mms_allocated_transaction_item.adjustment_flag = 'Y' then 'Adjustment'
            when fact_mms_allocated_transaction_item.refund_flag = 'Y' then 'Refund'
            when fact_mms_allocated_transaction_item.automated_refund_flag = 'Y' then 'Refund'
            when fact_mms_allocated_transaction_item.charge_flag = 'Y' then 'Charge'
            when fact_mms_allocated_transaction_item.sale_flag= 'Y' then 'Sale'
        end TransactionType,
       cast('-998' as varchar(32)) sales_channel_dim_description_key,
       cast(fact_mms_allocated_transaction_item.mms_tran_id as varchar(255)) transaction_id,
       fact_mms_allocated_transaction_item.tran_item_id line_number,
       null autoship_flag,
       0 shipping_and_handling_amount,
       null product_cost
from fact_mms_allocated_transaction_item
join d_mms_member on fact_mms_allocated_transaction_item.dim_mms_member_key = d_mms_member.dim_mms_member_key
union all
select 'Magento' sales_source,
       fact_magento_allocated_transaction_item.fact_magento_transaction_item_key,
       fact_magento_allocated_transaction_item.dim_club_key allocated_dim_club_key,
       fact_magento_allocated_transaction_item.dim_magento_product_key dim_product_key,
       fact_magento_allocated_transaction_item.dim_mms_member_key,
       fact_magento_allocated_transaction_item.dim_mms_membership_key,
       fact_magento_allocated_transaction_item.commissioned_sales_dim_employee_key,
       fact_magento_allocated_transaction_item.dim_reporting_hierarchy_key,
       fact_magento_allocated_transaction_item.invoice_dim_date_key transaction_dim_date_key,
	   -997 as transaction_dim_time_key, /*--added---	 */
       fact_magento_allocated_transaction_item.allocated_month_starting_dim_date_key,
       fact_magento_allocated_transaction_item.allocated_quantity,
       fact_magento_allocated_transaction_item.allocated_amount,
       fact_magento_allocated_transaction_item.transaction_quantity,
       fact_magento_allocated_transaction_item.transaction_amount,
       fact_magento_allocated_transaction_item.original_currency_code,
       cast(null as varchar) as payment_types,
       fact_magento_allocated_transaction_item.discount_amount,
       cast('-998' as varchar(32)) as dim_mms_transaction_reason_key,
       case when fact_magento_allocated_transaction_item.refund_flag = 'Y' then 'Refund'
            when fact_magento_allocated_transaction_item.sale_flag = 'Y' then 'Sale'
            else 'Unknown'
        end transaction_type,
       cast('-998' as varchar(32)) as sales_channel_dim_description_key,
       cast(fact_magento_allocated_transaction_item.order_number as varchar(255)) transaction_id,
       cast(order_item_id as varchar) line_number,
       cast(null as varchar) autoship_flag,
       fact_magento_allocated_transaction_item.shipping_and_handling_amount,
       fact_magento_allocated_transaction_item.product_cost
from marketing.v_fact_magento_allocated_transaction_item fact_magento_allocated_transaction_item
union all
select 'HealthCheckUSA' sales_source,
       fact_healthcheckusa_allocated_transaction_item.fact_healthcheckusa_allocated_transaction_item_key,
       fact_healthcheckusa_allocated_transaction_item.dim_club_key allocated_dim_club_key,
       fact_healthcheckusa_allocated_transaction_item.dim_healthcheckusa_product_key dim_product_key,
       cast('-998' as varchar(32)) dim_mms_member_key,
       cast('-998' as varchar(32)) dim_mms_membership_key,
       fact_healthcheckusa_allocated_transaction_item.sales_dim_employee_key,
       fact_healthcheckusa_allocated_transaction_item.dim_reporting_hierarchy_key,
       fact_healthcheckusa_allocated_transaction_item.transaction_post_dim_date_key transaction_dim_date_key,
	   fact_healthcheckusa_allocated_transaction_item.transaction_post_dim_time_key transaction_dim_time_key,
       fact_healthcheckusa_allocated_transaction_item.allocated_month_starting_dim_date_key,
       fact_healthcheckusa_allocated_transaction_item.sales_quantity allocated_quantity,
       fact_healthcheckusa_allocated_transaction_item.sales_amount allocated_amount,
       fact_healthcheckusa_allocated_transaction_item.sales_quantity transaction_quantity,
       fact_healthcheckusa_allocated_transaction_item.sales_amount transaction_amount,
       fact_healthcheckusa_allocated_transaction_item.original_currency_code,
       cast(null as varchar) as payment_types,
       fact_healthcheckusa_allocated_transaction_item.discount_amount,
       cast('-998' as varchar(32)) as dim_mms_transaction_reason_key,
       case when fact_healthcheckusa_allocated_transaction_item.refund_flag = 'Y' then 'Refund'
            else 'Sale'
        end transaction_type,
       cast('-998' as varchar(32)) as sales_channel_dim_description_key,
       cast(fact_healthcheckusa_allocated_transaction_item.order_number as varchar(255)) transaction_id,
       cast(null as varchar) line_number,
       cast(null as varchar) autoship_flag,
       0 shipping_and_handling_amount,
       cast(null as decimal(12,2)) product_cost
from marketing.v_fact_healthcheckusa_allocated_transaction_item fact_healthcheckusa_allocated_transaction_item;