﻿CREATE PROC [dbo].[proc_d_magento_sales_order_item] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_order_item)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_order_item_insert') is not null drop table #p_magento_sales_order_item_insert
create table dbo.#p_magento_sales_order_item_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_order_item.p_magento_sales_order_item_id,
       p_magento_sales_order_item.bk_hash
  from dbo.p_magento_sales_order_item
 where p_magento_sales_order_item.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_order_item.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_order_item.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_order_item.bk_hash,
       p_magento_sales_order_item.item_id order_item_id,
       s_magento_sales_order_item.agreement_id agreement_id,
       s_magento_sales_order_item.amount_refunded amount_refunded,
       s_magento_sales_order_item.applied_rule_ids applied_rule_ids,
       s_magento_sales_order_item.base_amount_refunded base_amount_refunded,
       s_magento_sales_order_item.base_cost base_cost,
       s_magento_sales_order_item.base_discount_amount base_discount_amount,
       s_magento_sales_order_item.base_discount_invoiced base_discount_invoiced,
       s_magento_sales_order_item.base_discount_refunded base_discount_refunded,
       s_magento_sales_order_item.base_discount_tax_compensation_amount base_discount_tax_compensation_amount,
       s_magento_sales_order_item.base_discount_tax_compensation_invoiced base_discount_tax_compensation_invoiced,
       s_magento_sales_order_item.base_discount_tax_compensation_refunded base_discount_tax_compensation_refunded,
       s_magento_sales_order_item.base_original_price base_original_price,
       s_magento_sales_order_item.base_price base_price,
       s_magento_sales_order_item.base_price_incl_tax base_price_incl_tax,
       s_magento_sales_order_item.base_row_invoiced base_row_invoiced,
       s_magento_sales_order_item.base_row_total base_row_total,
       s_magento_sales_order_item.base_row_total_incl_tax base_row_total_incl_tax,
       s_magento_sales_order_item.base_tax_amount base_tax_amount,
       s_magento_sales_order_item.base_tax_before_discount base_tax_before_discount,
       s_magento_sales_order_item.base_tax_invoiced base_tax_invoiced,
       cast(s_magento_sales_order_item.lt_bucks_redeemed as decimal(12,2))/cast(s_magento_sales_order_item.qty_ordered as decimal(12,2)) bucks_per_quantity,
       s_magento_sales_order_item.created_at created_at,
       case when p_magento_sales_order_item.bk_hash in('-997', '-998', '-999') then p_magento_sales_order_item.bk_hash
           when s_magento_sales_order_item.created_at is null then '-998'
        else convert(varchar, s_magento_sales_order_item.created_at, 112)    end created_dim_date_key,
       case when p_magento_sales_order_item.bk_hash in ('-997','-998','-999') then p_magento_sales_order_item.bk_hash
       when s_magento_sales_order_item.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_order_item.created_at,114), 1, 5),':','') end created_dim_time_key,
       case when p_magento_sales_order_item.bk_hash in('-997', '-998', '-999') then p_magento_sales_order_item.bk_hash
           when l_magento_sales_order_item.order_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_order_item.order_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_magento_sales_order_bk_hash,
       case when l_magento_sales_order_item.mms_club_id is null then '-998'
       when isnumeric(l_magento_sales_order_item.mms_club_id) =0  then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_magento_sales_order_item.mms_club_id as varchar(8000)),'z#@$k%&P'))),2) end dim_club_key,
       case when p_magento_sales_order_item.bk_hash in('-997', '-998', '-999') then p_magento_sales_order_item.bk_hash
           when l_magento_sales_order_item.product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_order_item.product_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_magento_product_key,
       case when l_magento_sales_order_item.mms_id is null then '-998'
       when isnumeric(l_magento_sales_order_item.mms_id) =0  then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_magento_sales_order_item.mms_id as varchar(8000)),'z#@$k%&P'))),2) end dim_mms_product_key,
       s_magento_sales_order_item.discount_amount discount_amount,
       s_magento_sales_order_item.discount_invoiced discount_invoiced,
       s_magento_sales_order_item.discount_percent discount_percent,
       s_magento_sales_order_item.discount_refunded discount_refunded,
       s_magento_sales_order_item.discount_tax_compensation_amount discount_tax_compensation_amount,
       s_magento_sales_order_item.discount_tax_compensation_canceled discount_tax_compensation_canceled,
       s_magento_sales_order_item.discount_tax_compensation_invoiced discount_tax_compensation_invoiced,
       s_magento_sales_order_item.discount_tax_compensation_refunded discount_tax_compensation_refunded,
       l_magento_sales_order_item.email_template_id email_template_id,
       s_magento_sales_order_item.event_id event_id,
       l_magento_sales_order_item.ext_order_item_id ext_order_item_id,
       case when p_magento_sales_order_item.bk_hash in ('-997', '-998', '-999') then p_magento_sales_order_item.bk_hash
            when l_magento_sales_order_item.order_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_order_item.order_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_order_key,
       s_magento_sales_order_item.free_shipping free_shipping,
       s_magento_sales_order_item.gift_message_available gift_message_available,
       l_magento_sales_order_item.gift_message_id gift_message_id,
       l_magento_sales_order_item.gift_registry_item_id gift_registry_item_id,
       s_magento_sales_order_item.gw_id gw_id,
       case when s_magento_sales_order_item.is_qty_decimal= 1 then 'Y' else 'N' end is_qty_decimal,
       case when s_magento_sales_order_item.is_virtual= 1 then 'Y' else 'N' end is_virtual_flag,
       s_magento_sales_order_item.locked_do_invoice locked_do_invoice,
       s_magento_sales_order_item.locked_do_ship locked_do_ship,
       l_magento_sales_order_item.m1_order_item_id m1_order_item_id,
       s_magento_sales_order_item.name name,
       s_magento_sales_order_item.no_discount no_discount,
       s_magento_sales_order_item.original_price original_price,
       case when p_magento_sales_order_item.bk_hash in ('-997', '-998', '-999') then p_magento_sales_order_item.bk_hash
            when l_magento_sales_order_item.parent_item_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_order_item.parent_item_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end parent_fact_magento_order_item_key,
       l_magento_sales_order_item.parent_item_id parent_item_id,
       s_magento_sales_order_item.price price,
       s_magento_sales_order_item.price_incl_tax price_incl_tax,
       s_magento_sales_order_item.qty_backordered qty_backordered,
       s_magento_sales_order_item.qty_invoiced qty_invoiced,
       s_magento_sales_order_item.qty_ordered qty_ordered,
       s_magento_sales_order_item.qty_refunded qty_refunded,
       s_magento_sales_order_item.qty_returned qty_returned,
       s_magento_sales_order_item.qty_shipped qty_shipped,
       s_magento_sales_order_item.quote_item_id quote_item_id,
       case when s_magento_sales_order_item.qty_refunded = 0 then 0 else cast(s_magento_sales_order_item.lt_bucks_refunded as decimal(12,2))/cast(s_magento_sales_order_item.qty_refunded as decimal(12,2)) end refund_bucks_per_quantity,
       s_magento_sales_order_item.row_invoiced row_invoiced,
       s_magento_sales_order_item.row_total row_total,
       s_magento_sales_order_item.row_total_incl_tax row_total_incl_tax,
       s_magento_sales_order_item.row_weight row_weight,
       s_magento_sales_order_item.agreement_api_status sales_order_item_agreement_api_status,
       s_magento_sales_order_item.agreement_status sales_order_item_agreement_status,
       s_magento_sales_order_item.base_tax_refunded sales_order_item_base_tax_refunded,
       s_magento_sales_order_item.base_weee_tax_applied_amount sales_order_item_base_weee_tax_applied_amount,
       s_magento_sales_order_item.base_weee_tax_applied_row_amnt sales_order_item_base_weee_tax_applied_row_amnt,
       s_magento_sales_order_item.base_weee_tax_disposition sales_order_item_base_weee_tax_disposition,
       s_magento_sales_order_item.base_weee_tax_row_disposition sales_order_item_base_weee_tax_row_disposition,
       s_magento_sales_order_item.lt_bucks_redeemed sales_order_item_lt_bucks_redeemed,
       s_magento_sales_order_item.lt_bucks_refunded sales_order_item_lt_bucks_refunded,
       s_magento_sales_order_item.product_options sales_order_item_product_options,
       s_magento_sales_order_item.product_type sales_order_item_product_type,
       s_magento_sales_order_item.qty_canceled sales_order_item_qty_canceled,
       s_magento_sales_order_item.sku sku,
       l_magento_sales_order_item.store_id store_id,
       s_magento_sales_order_item.tax_amount tax_amount,
       s_magento_sales_order_item.tax_before_discount tax_before_discount,
       s_magento_sales_order_item.tax_canceled tax_canceled,
       s_magento_sales_order_item.tax_invoiced tax_invoiced,
       s_magento_sales_order_item.tax_percent tax_percent,
       s_magento_sales_order_item.tax_refunded tax_refunded,
       s_magento_sales_order_item.updated_at updated_at,
       case when p_magento_sales_order_item.bk_hash in('-997', '-998', '-999') then p_magento_sales_order_item.bk_hash
           when s_magento_sales_order_item.updated_at is null then '-998'
        else convert(varchar, s_magento_sales_order_item.updated_at, 112)    end updated_dim_date_key,
       case when p_magento_sales_order_item.bk_hash in ('-997','-998','-999') then p_magento_sales_order_item.bk_hash
       when s_magento_sales_order_item.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_order_item.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       s_magento_sales_order_item.vendor vendor,
       l_magento_sales_order_item.wd_cost_center_id wd_cost_center_id,
       l_magento_sales_order_item.wd_offering_id wd_offering_id,
       l_magento_sales_order_item.wd_region_id wd_region_id,
       l_magento_sales_order_item.wd_revenue_id wd_revenue_id,
       l_magento_sales_order_item.wd_spending_id wd_spending_id,
       s_magento_sales_order_item.weight weight,
       isnull(h_magento_sales_order_item.dv_deleted,0) dv_deleted,
       p_magento_sales_order_item.p_magento_sales_order_item_id,
       p_magento_sales_order_item.dv_batch_id,
       p_magento_sales_order_item.dv_load_date_time,
       p_magento_sales_order_item.dv_load_end_date_time
  from dbo.h_magento_sales_order_item
  join dbo.p_magento_sales_order_item
    on h_magento_sales_order_item.bk_hash = p_magento_sales_order_item.bk_hash
  join #p_magento_sales_order_item_insert
    on p_magento_sales_order_item.bk_hash = #p_magento_sales_order_item_insert.bk_hash
   and p_magento_sales_order_item.p_magento_sales_order_item_id = #p_magento_sales_order_item_insert.p_magento_sales_order_item_id
  join dbo.l_magento_sales_order_item
    on p_magento_sales_order_item.bk_hash = l_magento_sales_order_item.bk_hash
   and p_magento_sales_order_item.l_magento_sales_order_item_id = l_magento_sales_order_item.l_magento_sales_order_item_id
  join dbo.s_magento_sales_order_item
    on p_magento_sales_order_item.bk_hash = s_magento_sales_order_item.bk_hash
   and p_magento_sales_order_item.s_magento_sales_order_item_id = s_magento_sales_order_item.s_magento_sales_order_item_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_order_item
   where d_magento_sales_order_item.bk_hash in (select bk_hash from #p_magento_sales_order_item_insert)

  insert dbo.d_magento_sales_order_item(
             bk_hash,
             order_item_id,
             agreement_id,
             amount_refunded,
             applied_rule_ids,
             base_amount_refunded,
             base_cost,
             base_discount_amount,
             base_discount_invoiced,
             base_discount_refunded,
             base_discount_tax_compensation_amount,
             base_discount_tax_compensation_invoiced,
             base_discount_tax_compensation_refunded,
             base_original_price,
             base_price,
             base_price_incl_tax,
             base_row_invoiced,
             base_row_total,
             base_row_total_incl_tax,
             base_tax_amount,
             base_tax_before_discount,
             base_tax_invoiced,
             bucks_per_quantity,
             created_at,
             created_dim_date_key,
             created_dim_time_key,
             d_magento_sales_order_bk_hash,
             dim_club_key,
             dim_magento_product_key,
             dim_mms_product_key,
             discount_amount,
             discount_invoiced,
             discount_percent,
             discount_refunded,
             discount_tax_compensation_amount,
             discount_tax_compensation_canceled,
             discount_tax_compensation_invoiced,
             discount_tax_compensation_refunded,
             email_template_id,
             event_id,
             ext_order_item_id,
             fact_magento_order_key,
             free_shipping,
             gift_message_available,
             gift_message_id,
             gift_registry_item_id,
             gw_id,
             is_qty_decimal,
             is_virtual_flag,
             locked_do_invoice,
             locked_do_ship,
             m1_order_item_id,
             name,
             no_discount,
             original_price,
             parent_fact_magento_order_item_key,
             parent_item_id,
             price,
             price_incl_tax,
             qty_backordered,
             qty_invoiced,
             qty_ordered,
             qty_refunded,
             qty_returned,
             qty_shipped,
             quote_item_id,
             refund_bucks_per_quantity,
             row_invoiced,
             row_total,
             row_total_incl_tax,
             row_weight,
             sales_order_item_agreement_api_status,
             sales_order_item_agreement_status,
             sales_order_item_base_tax_refunded,
             sales_order_item_base_weee_tax_applied_amount,
             sales_order_item_base_weee_tax_applied_row_amnt,
             sales_order_item_base_weee_tax_disposition,
             sales_order_item_base_weee_tax_row_disposition,
             sales_order_item_lt_bucks_redeemed,
             sales_order_item_lt_bucks_refunded,
             sales_order_item_product_options,
             sales_order_item_product_type,
             sales_order_item_qty_canceled,
             sku,
             store_id,
             tax_amount,
             tax_before_discount,
             tax_canceled,
             tax_invoiced,
             tax_percent,
             tax_refunded,
             updated_at,
             updated_dim_date_key,
             updated_dim_time_key,
             vendor,
             wd_cost_center_id,
             wd_offering_id,
             wd_region_id,
             wd_revenue_id,
             wd_spending_id,
             weight,
             deleted_flag,
             p_magento_sales_order_item_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         order_item_id,
         agreement_id,
         amount_refunded,
         applied_rule_ids,
         base_amount_refunded,
         base_cost,
         base_discount_amount,
         base_discount_invoiced,
         base_discount_refunded,
         base_discount_tax_compensation_amount,
         base_discount_tax_compensation_invoiced,
         base_discount_tax_compensation_refunded,
         base_original_price,
         base_price,
         base_price_incl_tax,
         base_row_invoiced,
         base_row_total,
         base_row_total_incl_tax,
         base_tax_amount,
         base_tax_before_discount,
         base_tax_invoiced,
         bucks_per_quantity,
         created_at,
         created_dim_date_key,
         created_dim_time_key,
         d_magento_sales_order_bk_hash,
         dim_club_key,
         dim_magento_product_key,
         dim_mms_product_key,
         discount_amount,
         discount_invoiced,
         discount_percent,
         discount_refunded,
         discount_tax_compensation_amount,
         discount_tax_compensation_canceled,
         discount_tax_compensation_invoiced,
         discount_tax_compensation_refunded,
         email_template_id,
         event_id,
         ext_order_item_id,
         fact_magento_order_key,
         free_shipping,
         gift_message_available,
         gift_message_id,
         gift_registry_item_id,
         gw_id,
         is_qty_decimal,
         is_virtual_flag,
         locked_do_invoice,
         locked_do_ship,
         m1_order_item_id,
         name,
         no_discount,
         original_price,
         parent_fact_magento_order_item_key,
         parent_item_id,
         price,
         price_incl_tax,
         qty_backordered,
         qty_invoiced,
         qty_ordered,
         qty_refunded,
         qty_returned,
         qty_shipped,
         quote_item_id,
         refund_bucks_per_quantity,
         row_invoiced,
         row_total,
         row_total_incl_tax,
         row_weight,
         sales_order_item_agreement_api_status,
         sales_order_item_agreement_status,
         sales_order_item_base_tax_refunded,
         sales_order_item_base_weee_tax_applied_amount,
         sales_order_item_base_weee_tax_applied_row_amnt,
         sales_order_item_base_weee_tax_disposition,
         sales_order_item_base_weee_tax_row_disposition,
         sales_order_item_lt_bucks_redeemed,
         sales_order_item_lt_bucks_refunded,
         sales_order_item_product_options,
         sales_order_item_product_type,
         sales_order_item_qty_canceled,
         sku,
         store_id,
         tax_amount,
         tax_before_discount,
         tax_canceled,
         tax_invoiced,
         tax_percent,
         tax_refunded,
         updated_at,
         updated_dim_date_key,
         updated_dim_time_key,
         vendor,
         wd_cost_center_id,
         wd_offering_id,
         wd_region_id,
         wd_revenue_id,
         wd_spending_id,
         weight,
         dv_deleted,
         p_magento_sales_order_item_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_order_item)
--Done!
end
