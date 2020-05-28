CREATE PROC [dbo].[proc_fact_allocated_transaction_item] @dv_batch_id [bigint] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF OBJECT_ID('tempdb.dbo.#bulk', 'U') IS NOT NULL DROP TABLE #bulk
create table dbo.#bulk with(distribution = hash(source_fact_table_key), heap) as ------HEAP, don't compress yet
select 'MMS'+cast(fact_mms_allocated_transaction_item.fact_mms_allocated_transaction_item_key as varchar(32)) fact_allocated_transaction_item_id,
       'MMS' sales_source,
       fact_mms_allocated_transaction_item.fact_mms_allocated_transaction_item_key source_fact_table_key,
       fact_mms_allocated_transaction_item.dim_club_key allocated_dim_club_key,
       fact_mms_allocated_transaction_item.primary_sales_dim_employee_key,
       fact_mms_allocated_transaction_item.transaction_post_dim_date_key transaction_dim_date_key,
       fact_mms_allocated_transaction_item.transaction_post_dim_time_key transaction_dim_time_key, /*--added---     */
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
        end transaction_type,
       cast('-998' as varchar(32)) sales_channel_dim_description_key,
       cast(fact_mms_allocated_transaction_item.mms_tran_id as varchar(255)) transaction_id,
       fact_mms_allocated_transaction_item.tran_item_id line_number,
       
       product_master.source_system,
       product_master.dim_cafe_product_key dim_product_key,
       convert(varchar(50), product_master.product_id) source_product_id,
       product_master.product_description,
       product_master.dim_reporting_hierarchy_key,
       product_master.reporting_division,
       product_master.reporting_sub_division,
       product_master.reporting_department,
       product_master.reporting_product_group,
       isnull(product_master.allocation_rule,'') allocation_rule,
       
       null ecommerce_shipment_number,
       null ecommerce_order_number,
       null ecommerce_autoship_flag,
       null ecommerce_shipping_and_handling_amount,
       null ecommerce_product_cost,
       'N' ecommerce_deferral_flag,
       
       fact_mms_allocated_transaction_item.mms_tran_id mms_tran_id,
       fact_mms_allocated_transaction_item.tran_item_id mms_tran_item_id,

       dim_exerp_initial_participation_employee.sale_employee_id exerp_sale_employee_id,
       dim_exerp_initial_participation_employee.service_employee_id exerp_service_employee_id,
       
       fact_mms_allocated_transaction_item.dim_mms_member_key
  from dbo.fact_mms_allocated_transaction_item
  join dim_date dd1
    on fact_mms_allocated_transaction_item.allocated_month_ending_dim_date_key = dd1.dim_date_key
  join d_udwcloudsync_product_master_history product_master
    on fact_mms_allocated_transaction_item.dim_mms_product_key = product_master.dim_mms_product_key
   and product_master.effective_date_time < dd1.calendar_date
   and product_master.expiration_date_time >= dd1.calendar_date
   and product_master.dim_mms_product_key not in ('-999','-998','-997') 
  left join dim_exerp_initial_participation_employee
    on fact_mms_allocated_transaction_item.tran_item_id = dim_exerp_initial_participation_employee.tran_item_id
union all
select 'Hybris'+fact_hybris_transaction_item.fact_hybris_transaction_item_key fact_allocated_transaction_item_id,
       'Hybris' sales_source,
       fact_hybris_transaction_item.fact_hybris_transaction_item_key source_fact_table_key,
       fact_hybris_transaction_item.dim_club_key allocated_dim_club_key,
       fact_hybris_transaction_item.sales_dim_employee_key primary_sales_dim_employee_key,
       fact_hybris_transaction_item.settlement_dim_date_key transaction_dim_date_key,
       fact_hybris_transaction_item.settlement_dim_time_key transaction_dim_time_key, /*--added---       */
       fact_hybris_transaction_item.allocated_month_starting_dim_date_key,
       fact_hybris_transaction_item.transaction_quantity allocated_quantity,
       fact_hybris_transaction_item.transaction_amount allocated_amount,
       fact_hybris_transaction_item.transaction_quantity,
       fact_hybris_transaction_item.transaction_amount,
       fact_hybris_transaction_item.original_currency_code,
       cast(NULL as varchar(4000)) as payment_types,
       fact_hybris_transaction_item.discount_amount,
       cast('-998' as varchar(32)) as dim_mms_transaction_reason_key,
       case when fact_hybris_transaction_item.refund_flag = 'Y' then'Refund' else 'Sale' end transaction_type,
       cast('-998' as varchar(32)) as sales_channel_dim_description_key,
       cast(fact_hybris_transaction_item.order_code as varchar(255)) transaction_id,
       fact_hybris_transaction_item.entry_number line_number,
       product_master.source_system,
       product_master.dim_hybris_product_key dim_product_key,
       convert(varchar(50), product_master.product_sku) source_product_id,
       product_master.product_description,
       product_master.dim_reporting_hierarchy_key,
       product_master.reporting_division,
       product_master.reporting_sub_division,
       product_master.reporting_department,
       product_master.reporting_product_group,
       isnull(product_master.allocation_rule,'') allocation_rule,
       cast(fact_hybris_transaction_item.order_code as varchar(255)) ecommerce_shipment_number,
       fact_hybris_transaction_item.entry_number ecommerce_order_number,
       fact_hybris_transaction_item.auto_ship_flag ecommerce_autoship_flag,
       0 ecommerce_shipping_and_handling_amount,
       hybris_product.product_cost ecommerce_product_cost,
       case when product_master.reporting_product_group in ('Weight Loss Challenges','90 Day Weight Loss') then 'Y' else 'N' end ecommerce_deferral_flag,
       null mms_tran_id,
       null mms_tran_item_id,
       null exerp_sale_employee_id,
       null exerp_service_employee_id,

       fact_hybris_transaction_item.dim_mms_member_key
  from fact_hybris_transaction_item
  join d_udwcloudsync_product_master_history product_master
    on fact_hybris_transaction_item.dim_hybris_product_key = product_master.dim_hybris_product_key
   and product_master.effective_date_time < fact_hybris_transaction_item.allocated_recalculate_through_datetime
   and product_master.expiration_date_time >= fact_hybris_transaction_item.allocated_recalculate_through_datetime
   and product_master.dim_hybris_product_key not in ('-999','-998','-997')
  join d_hybris_all_products hybris_product
    on fact_Hybris_transaction_item.dim_hybris_product_key = hybris_product.dim_hybris_product_key
 where product_master.reporting_product_group <> ''
union all
select 'Cafe'+fact_cafe_sales_transaction_item.fact_cafe_sales_transaction_item_key fact_allocated_transaction_item_id,
       'Cafe' sales_source,
       fact_cafe_sales_transaction_item.fact_cafe_sales_transaction_item_key source_fact_table_Key,
       fact_cafe_sales_transaction_item.dim_club_key allocated_dim_club_key,
       fact_cafe_sales_transaction_item.order_commissionable_dim_employee_key primary_sales_dim_employee_key,
       fact_cafe_sales_transaction_item.order_close_dim_date_key transaction_dim_date_key,
       fact_cafe_sales_transaction_item.order_close_dim_time_key transaction_dim_time_key, /*--added---      */
       fact_cafe_sales_transaction_item.allocated_month_starting_dim_date_key,
       fact_cafe_sales_transaction_item.item_quantity allocated_quantity,
       fact_cafe_sales_transaction_item.item_sales_dollar_amount_excluding_tax allocated_amount,
       fact_cafe_sales_transaction_item.item_quantity transaction_quantity,
       fact_cafe_sales_transaction_item.item_sales_dollar_amount_excluding_tax transaction_amount,
       fact_cafe_sales_transaction_item.original_currency_code,
       cast(NULL as varchar(4000)) as payment_types,
       fact_cafe_sales_transaction_item.item_discount_amount discount_amount,
       cast('-998' as varchar(32)) as dim_mms_transaction_reason_key,
       case when fact_cafe_sales_transaction_item.order_refund_flag = 'Y' or fact_cafe_sales_transaction_item.item_refund_flag = 'Y' then 'Refund' else 'Sale' end transaction_type,
       cast('-998' as varchar(32)) as sales_channel_dim_description_key,
       cast(fact_cafe_sales_transaction_item.order_hdr_id as varchar(255)) transaction_id,
       fact_cafe_sales_transaction_item.check_seq line_number,
       product_master.source_system,
       product_master.dim_cafe_product_key dim_product_key,
       convert(varchar(50), product_master.product_id) source_product_id,
       product_master.product_description,
       product_master.dim_reporting_hierarchy_key,
       product_master.reporting_division,
       product_master.reporting_sub_division,
       product_master.reporting_department,
       product_master.reporting_product_group,
       isnull(product_master.allocation_rule,'') allocation_rule,
       null ecommerce_shipment_number,
       null ecommerce_order_number,
       null ecommerce_autoship_flag,
       null ecommerce_shipping_and_handling_amount,
       null ecommerce_product_cost,
       'N' ecommerce_deferral_flag,
       null mms_tran_id,
       null mms_tran_item_id,
       null exerp_sale_employee_id,
       null exerp_service_employee_id,

       cast('-998' as varchar(32)) dim_mms_member_key
  from fact_cafe_sales_transaction_item
  join d_udwcloudsync_product_master_history product_master
    on fact_cafe_sales_transaction_item.dim_cafe_product_key = product_master.dim_cafe_product_key
   and product_master.effective_date_time < fact_cafe_sales_transaction_item.allocated_recalculate_through_datetime
   and product_master.expiration_date_time >= fact_cafe_sales_transaction_item.allocated_recalculate_through_datetime
   and product_master.dim_cafe_product_key not in ('-999','-998','-997')
 where product_master.reporting_product_group <> ''
   and (fact_cafe_sales_transaction_item.order_refund_flag = 'Y'
        or fact_cafe_sales_transaction_item.order_void_flag = 'N')
   and fact_cafe_sales_transaction_item.item_voided_flag = 'N'
union all
select 'Magento'+fact_magento_transaction_item.unique_key fact_allocated_transaction_item_id,
       'Magento' sales_source,
       fact_magento_transaction_item.unique_key source_fact_table_key,
       fact_magento_transaction_item.allocated_dim_club_key,
       fact_magento_transaction_item.dim_employee_key commissioned_sales_dim_employee_key,
       fact_magento_transaction_item.allocated_dim_date_key transaction_dim_date_key,
       -997 as transaction_dim_time_key, /*--added---     */
       fact_magento_transaction_item.allocated_month_starting_dim_date_key,
       fact_magento_transaction_item.transaction_quantity allocated_quantity,
       fact_magento_transaction_item.allocated_amount allocated_amount,
       fact_magento_transaction_item.transaction_quantity,
       fact_magento_transaction_item.transaction_amount,
       fact_magento_transaction_item.original_currency_code,
       cast(null as varchar) as payment_types,
       fact_magento_transaction_item.transaction_discount_amount discount_amount,
       cast('-998' as varchar(32)) as dim_mms_transaction_reason_key,
       case when fact_magento_transaction_item.refund_flag = 'Y' then 'Refund'
            when fact_magento_transaction_item.refund_flag = 'N' then 'Sale'
            else 'Unknown'
        end transaction_type,
       cast('-998' as varchar(32)) as sales_channel_dim_description_key,
       cast(fact_magento_transaction_item.order_number as varchar(255)) transaction_id,
       cast(order_item_id as varchar) line_number,
       product_master.source_system,
       product_master.dim_magento_product_key dim_product_key,
       convert(varchar(50), product_master.product_sku) source_product_id,
       product_master.product_description,
       product_master.dim_reporting_hierarchy_key,
       product_master.reporting_division,
       product_master.reporting_sub_division,
       product_master.reporting_department,
       product_master.reporting_product_group,
       isnull(product_master.allocation_rule,'') allocation_rule,
       cast(fact_magento_transaction_item.order_number as varchar(255)) ecommerce_shipment_number,
       cast(order_item_id as varchar) ecommerce_order_number,
       cast(null as varchar) ecommerce_autoship_flag,
       fact_magento_transaction_item.shipping_amount ecommerce_shipping_and_handling_amount,
       fact_magento_transaction_item.product_cost ecommerce_product_cost,
       case when product_master.reporting_product_group in ('Weight Loss Challenges','90 Day Weight Loss') then 'Y' else 'N' end ecommerce_deferral_flag,
       null mms_tran_id,
       null mms_tran_item_id,
       null exerp_sale_employee_id,
       null exerp_service_employee_id,
       
       fact_magento_transaction_item.dim_mms_member_key
  from fact_magento_tran_item fact_magento_transaction_item
  join d_udwcloudsync_product_master_history product_master
    on fact_magento_transaction_item.dim_magento_product_key = product_master.dim_magento_product_key
   and product_master.effective_date_time < fact_magento_transaction_item.allocated_recalculate_through_datetime
   and product_master.expiration_date_time >= fact_magento_transaction_item.allocated_recalculate_through_datetime
   and product_master.dim_magento_product_key not in ('-999','-998','-997')
 where product_master.reporting_product_group <> ''
union all
select 'HealthCheckUSA'+fact_healthcheckusa_allocated_transaction_item.fact_healthcheckusa_allocated_transaction_item_key fact_allocated_transaction_item_id,
       'HealthCheckUSA' sales_source,
       fact_healthcheckusa_allocated_transaction_item.fact_healthcheckusa_allocated_transaction_item_key source_fact_table_key,
       fact_healthcheckusa_allocated_transaction_item.dim_club_key allocated_dim_club_key,
       fact_healthcheckusa_allocated_transaction_item.sales_dim_employee_key,
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
       product_master.source_system,
       product_master.dim_healthcheckusa_product_key dim_product_key,
       convert(varchar(50), product_master.product_sku) source_product_id,
       product_master.product_description,
       product_master.dim_reporting_hierarchy_key,
       product_master.reporting_division,
       product_master.reporting_sub_division,
       product_master.reporting_department,
       product_master.reporting_product_group,
       isnull(product_master.allocation_rule,'') allocation_rule,
       cast(fact_healthcheckusa_allocated_transaction_item.order_number as varchar(255)) ecommerce_shipment_number,
       cast(null as varchar) ecommerce_order_number,
       cast(null as varchar) ecommerce_autoship_flag,
       0 ecommerce_shipping_and_handling_amount,
       null ecommerce_product_cost,
       case when product_master.reporting_product_group in ('Weight Loss Challenges','90 Day Weight Loss') then 'Y' else 'N' end ecommerce_deferral_flag,
       null mms_tran_id,
       null mms_tran_item_id,
       null exerp_sale_employee_id,
       null exerp_service_employee_id,
       
       cast('-998' as varchar(32)) dim_mms_member_key
  from fact_healthcheckusa_allocated_transaction_item
  join d_udwcloudsync_product_master_history product_master
    on fact_healthcheckusa_allocated_transaction_item.dim_healthcheckusa_product_key = product_master.dim_healthcheckusa_product_key
   and product_master.effective_date_time < fact_healthcheckusa_allocated_transaction_item.allocated_recalculate_through_datetime
   and product_master.expiration_date_time >= fact_healthcheckusa_allocated_transaction_item.allocated_recalculate_through_datetime
   and product_master.dim_healthcheckusa_product_key not in ('-999','-998','-997')
 where product_master.reporting_product_group <> ''

IF OBJECT_ID('dbo.fact_allocated_transaction_item_swap', 'U') IS NOT NULL DROP TABLE dbo.fact_allocated_transaction_item_swap; 
create table dbo.fact_allocated_transaction_item_swap with (distribution = hash(source_fact_table_key),clustered columnstore index) as --COMPRESS
select fact_allocated_transaction_item_id,
       sales_source,
       source_fact_table_key,
       allocated_dim_club_key,
       primary_sales_dim_employee_key,
       transaction_dim_date_key,
       transaction_dim_time_key,
       allocated_month_starting_dim_date_key,
       allocated_quantity,
       allocated_amount,
       transaction_quantity,
       transaction_amount,
       original_currency_code,
       payment_types,
       discount_amount,
       dim_mms_transaction_reason_key,
       transaction_type,
       sales_channel_dim_description_key,
       transaction_id,
       line_number,
       
       source_system,
       dim_product_key,
       source_product_id,
       product_description,
       dim_reporting_hierarchy_key,
       reporting_division,
       reporting_sub_division,
       reporting_department,
       reporting_product_group,
       allocation_rule,
       
       ecommerce_shipment_number,
       ecommerce_order_number,
       ecommerce_autoship_flag,
       ecommerce_shipping_and_handling_amount,
       ecommerce_product_cost,
       ecommerce_deferral_flag,
       
       mms_tran_id,
       mms_tran_item_id,

       exerp_sale_employee_id,
       exerp_service_employee_id,
       
       isnull(dim_mms_membership.dim_mms_membership_key,'-998') dim_mms_membership_key,
       dim_mms_membership.membership_id,
       dim_mms_membership.membership_type,
       
       isnull(d_mms_member.dim_mms_member_key,'-998') dim_mms_member_key,
       d_mms_member.member_id,
       d_mms_member.customer_name_last_first member_name,
       d_mms_member.first_name member_first_name,
       d_mms_member.last_name member_last_name
from #bulk f
left join d_mms_member
  on f.dim_mms_member_key = d_mms_member.dim_mms_member_key
 and f.dim_mms_member_key not in ('-999','-998','-997')
left join dim_mms_membership
  on d_mms_member.dim_mms_membership_key = dim_mms_membership.dim_mms_membership_key

  

if exists(select 1 from sys.tables where name = 'fact_allocated_transaction_item_swap')
begin
rename object fact_allocated_transaction_item to fact_allocated_transaction_item_old
rename object fact_allocated_transaction_item_swap to fact_allocated_transaction_item
drop table fact_allocated_transaction_item_old
end


end
