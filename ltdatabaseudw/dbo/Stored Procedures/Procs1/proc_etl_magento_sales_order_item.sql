CREATE PROC [dbo].[proc_etl_magento_sales_order_item] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_order_item

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_order_item (
       bk_hash,
       item_id,
       order_id,
       parent_item_id,
       quote_item_id,
       store_id,
       created_at,
       updated_at,
       product_id,
       product_type,
       product_options,
       weight,
       is_virtual,
       sku,
       name,
       description,
       applied_rule_ids,
       additional_data,
       is_qty_decimal,
       no_discount,
       qty_backordered,
       qty_canceled,
       qty_invoiced,
       qty_ordered,
       qty_refunded,
       qty_shipped,
       base_cost,
       price,
       base_price,
       original_price,
       base_original_price,
       tax_percent,
       tax_amount,
       base_tax_amount,
       tax_invoiced,
       base_tax_invoiced,
       discount_percent,
       discount_amount,
       base_discount_amount,
       discount_invoiced,
       base_discount_invoiced,
       amount_refunded,
       base_amount_refunded,
       row_total,
       base_row_total,
       row_invoiced,
       base_row_invoiced,
       row_weight,
       base_tax_before_discount,
       tax_before_discount,
       ext_order_item_id,
       locked_do_invoice,
       locked_do_ship,
       price_incl_tax,
       base_price_incl_tax,
       row_total_incl_tax,
       base_row_total_incl_tax,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       discount_tax_compensation_invoiced,
       base_discount_tax_compensation_invoiced,
       discount_tax_compensation_refunded,
       base_discount_tax_compensation_refunded,
       tax_canceled,
       discount_tax_compensation_canceled,
       tax_refunded,
       base_tax_refunded,
       discount_refunded,
       base_discount_refunded,
       free_shipping,
       qty_returned,
       gift_message_id,
       gift_message_available,
       weee_tax_applied,
       weee_tax_applied_amount,
       weee_tax_applied_row_amount,
       weee_tax_disposition,
       weee_tax_row_disposition,
       base_weee_tax_applied_amount,
       base_weee_tax_applied_row_amnt,
       base_weee_tax_disposition,
       base_weee_tax_row_disposition,
       gw_id,
       gw_base_price,
       gw_price,
       gw_base_tax_amount,
       gw_tax_amount,
       gw_base_price_invoiced,
       gw_price_invoiced,
       gw_base_tax_amount_invoiced,
       gw_tax_amount_invoiced,
       gw_base_price_refunded,
       gw_price_refunded,
       gw_base_tax_amount_refunded,
       gw_tax_amount_refunded,
       event_id,
       giftregistry_item_id,
       m1_order_item_id,
       wd_region_id,
       wd_costcenter_id,
       wd_offering_id,
       wd_spending_id,
       wd_revenue_id,
       email_template_id,
       agreement_id,
       agreement_status,
       agreement_api_status,
       mms_id,
       mms_club_id,
       lt_bucks_redeemed,
       lt_bucks_refunded,
       vendor,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(item_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       item_id,
       order_id,
       parent_item_id,
       quote_item_id,
       store_id,
       created_at,
       updated_at,
       product_id,
       product_type,
       product_options,
       weight,
       is_virtual,
       sku,
       name,
       description,
       applied_rule_ids,
       additional_data,
       is_qty_decimal,
       no_discount,
       qty_backordered,
       qty_canceled,
       qty_invoiced,
       qty_ordered,
       qty_refunded,
       qty_shipped,
       base_cost,
       price,
       base_price,
       original_price,
       base_original_price,
       tax_percent,
       tax_amount,
       base_tax_amount,
       tax_invoiced,
       base_tax_invoiced,
       discount_percent,
       discount_amount,
       base_discount_amount,
       discount_invoiced,
       base_discount_invoiced,
       amount_refunded,
       base_amount_refunded,
       row_total,
       base_row_total,
       row_invoiced,
       base_row_invoiced,
       row_weight,
       base_tax_before_discount,
       tax_before_discount,
       ext_order_item_id,
       locked_do_invoice,
       locked_do_ship,
       price_incl_tax,
       base_price_incl_tax,
       row_total_incl_tax,
       base_row_total_incl_tax,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       discount_tax_compensation_invoiced,
       base_discount_tax_compensation_invoiced,
       discount_tax_compensation_refunded,
       base_discount_tax_compensation_refunded,
       tax_canceled,
       discount_tax_compensation_canceled,
       tax_refunded,
       base_tax_refunded,
       discount_refunded,
       base_discount_refunded,
       free_shipping,
       qty_returned,
       gift_message_id,
       gift_message_available,
       weee_tax_applied,
       weee_tax_applied_amount,
       weee_tax_applied_row_amount,
       weee_tax_disposition,
       weee_tax_row_disposition,
       base_weee_tax_applied_amount,
       base_weee_tax_applied_row_amnt,
       base_weee_tax_disposition,
       base_weee_tax_row_disposition,
       gw_id,
       gw_base_price,
       gw_price,
       gw_base_tax_amount,
       gw_tax_amount,
       gw_base_price_invoiced,
       gw_price_invoiced,
       gw_base_tax_amount_invoiced,
       gw_tax_amount_invoiced,
       gw_base_price_refunded,
       gw_price_refunded,
       gw_base_tax_amount_refunded,
       gw_tax_amount_refunded,
       event_id,
       giftregistry_item_id,
       m1_order_item_id,
       wd_region_id,
       wd_costcenter_id,
       wd_offering_id,
       wd_spending_id,
       wd_revenue_id,
       email_template_id,
       agreement_id,
       agreement_status,
       agreement_api_status,
       mms_id,
       mms_club_id,
       lt_bucks_redeemed,
       lt_bucks_refunded,
       vendor,
       isnull(cast(stage_magento_sales_order_item.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_order_item
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_order_item @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_order_item (
       bk_hash,
       item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_sales_order_item.bk_hash,
       stage_hash_magento_sales_order_item.item_id item_id,
       isnull(cast(stage_hash_magento_sales_order_item.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_order_item
  left join h_magento_sales_order_item
    on stage_hash_magento_sales_order_item.bk_hash = h_magento_sales_order_item.bk_hash
 where h_magento_sales_order_item_id is null
   and stage_hash_magento_sales_order_item.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_order_item
if object_id('tempdb..#l_magento_sales_order_item_inserts') is not null drop table #l_magento_sales_order_item_inserts
create table #l_magento_sales_order_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_order_item.bk_hash,
       stage_hash_magento_sales_order_item.item_id item_id,
       stage_hash_magento_sales_order_item.order_id order_id,
       stage_hash_magento_sales_order_item.parent_item_id parent_item_id,
       stage_hash_magento_sales_order_item.store_id store_id,
       stage_hash_magento_sales_order_item.product_id product_id,
       stage_hash_magento_sales_order_item.ext_order_item_id ext_order_item_id,
       stage_hash_magento_sales_order_item.gift_message_id gift_message_id,
       stage_hash_magento_sales_order_item.giftregistry_item_id gift_registry_item_id,
       stage_hash_magento_sales_order_item.m1_order_item_id m1_order_item_id,
       stage_hash_magento_sales_order_item.wd_region_id wd_region_id,
       stage_hash_magento_sales_order_item.wd_costcenter_id wd_cost_center_id,
       stage_hash_magento_sales_order_item.wd_offering_id wd_offering_id,
       stage_hash_magento_sales_order_item.wd_spending_id wd_spending_id,
       stage_hash_magento_sales_order_item.wd_revenue_id wd_revenue_id,
       stage_hash_magento_sales_order_item.email_template_id email_template_id,
       stage_hash_magento_sales_order_item.mms_id mms_id,
       stage_hash_magento_sales_order_item.mms_club_id mms_club_id,
       isnull(cast(stage_hash_magento_sales_order_item.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.order_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.parent_item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.store_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.product_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.ext_order_item_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gift_message_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.giftregistry_item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.m1_order_item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.wd_region_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.wd_costcenter_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.wd_offering_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.wd_spending_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.wd_revenue_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.email_template_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.mms_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.mms_club_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_order_item
 where stage_hash_magento_sales_order_item.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_order_item records
set @insert_date_time = getdate()
insert into l_magento_sales_order_item (
       bk_hash,
       item_id,
       order_id,
       parent_item_id,
       store_id,
       product_id,
       ext_order_item_id,
       gift_message_id,
       gift_registry_item_id,
       m1_order_item_id,
       wd_region_id,
       wd_cost_center_id,
       wd_offering_id,
       wd_spending_id,
       wd_revenue_id,
       email_template_id,
       mms_id,
       mms_club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_order_item_inserts.bk_hash,
       #l_magento_sales_order_item_inserts.item_id,
       #l_magento_sales_order_item_inserts.order_id,
       #l_magento_sales_order_item_inserts.parent_item_id,
       #l_magento_sales_order_item_inserts.store_id,
       #l_magento_sales_order_item_inserts.product_id,
       #l_magento_sales_order_item_inserts.ext_order_item_id,
       #l_magento_sales_order_item_inserts.gift_message_id,
       #l_magento_sales_order_item_inserts.gift_registry_item_id,
       #l_magento_sales_order_item_inserts.m1_order_item_id,
       #l_magento_sales_order_item_inserts.wd_region_id,
       #l_magento_sales_order_item_inserts.wd_cost_center_id,
       #l_magento_sales_order_item_inserts.wd_offering_id,
       #l_magento_sales_order_item_inserts.wd_spending_id,
       #l_magento_sales_order_item_inserts.wd_revenue_id,
       #l_magento_sales_order_item_inserts.email_template_id,
       #l_magento_sales_order_item_inserts.mms_id,
       #l_magento_sales_order_item_inserts.mms_club_id,
       case when l_magento_sales_order_item.l_magento_sales_order_item_id is null then isnull(#l_magento_sales_order_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_order_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_order_item_inserts
  left join p_magento_sales_order_item
    on #l_magento_sales_order_item_inserts.bk_hash = p_magento_sales_order_item.bk_hash
   and p_magento_sales_order_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_order_item
    on p_magento_sales_order_item.bk_hash = l_magento_sales_order_item.bk_hash
   and p_magento_sales_order_item.l_magento_sales_order_item_id = l_magento_sales_order_item.l_magento_sales_order_item_id
 where l_magento_sales_order_item.l_magento_sales_order_item_id is null
    or (l_magento_sales_order_item.l_magento_sales_order_item_id is not null
        and l_magento_sales_order_item.dv_hash <> #l_magento_sales_order_item_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_order_item
if object_id('tempdb..#s_magento_sales_order_item_inserts') is not null drop table #s_magento_sales_order_item_inserts
create table #s_magento_sales_order_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_order_item.bk_hash,
       stage_hash_magento_sales_order_item.item_id item_id,
       stage_hash_magento_sales_order_item.quote_item_id quote_item_id,
       stage_hash_magento_sales_order_item.created_at created_at,
       stage_hash_magento_sales_order_item.updated_at updated_at,
       stage_hash_magento_sales_order_item.product_type product_type,
       stage_hash_magento_sales_order_item.product_options product_options,
       stage_hash_magento_sales_order_item.weight weight,
       stage_hash_magento_sales_order_item.is_virtual is_virtual,
       stage_hash_magento_sales_order_item.sku sku,
       stage_hash_magento_sales_order_item.name name,
       stage_hash_magento_sales_order_item.description description,
       stage_hash_magento_sales_order_item.applied_rule_ids applied_rule_ids,
       stage_hash_magento_sales_order_item.additional_data additional_data,
       stage_hash_magento_sales_order_item.is_qty_decimal is_qty_decimal,
       stage_hash_magento_sales_order_item.no_discount no_discount,
       stage_hash_magento_sales_order_item.qty_backordered qty_backordered,
       stage_hash_magento_sales_order_item.qty_canceled qty_canceled,
       stage_hash_magento_sales_order_item.qty_invoiced qty_invoiced,
       stage_hash_magento_sales_order_item.qty_ordered qty_ordered,
       stage_hash_magento_sales_order_item.qty_refunded qty_refunded,
       stage_hash_magento_sales_order_item.qty_shipped qty_shipped,
       stage_hash_magento_sales_order_item.base_cost base_cost,
       stage_hash_magento_sales_order_item.price price,
       stage_hash_magento_sales_order_item.base_price base_price,
       stage_hash_magento_sales_order_item.original_price original_price,
       stage_hash_magento_sales_order_item.base_original_price base_original_price,
       stage_hash_magento_sales_order_item.tax_percent tax_percent,
       stage_hash_magento_sales_order_item.tax_amount tax_amount,
       stage_hash_magento_sales_order_item.base_tax_amount base_tax_amount,
       stage_hash_magento_sales_order_item.tax_invoiced tax_invoiced,
       stage_hash_magento_sales_order_item.base_tax_invoiced base_tax_invoiced,
       stage_hash_magento_sales_order_item.discount_percent discount_percent,
       stage_hash_magento_sales_order_item.discount_amount discount_amount,
       stage_hash_magento_sales_order_item.base_discount_amount base_discount_amount,
       stage_hash_magento_sales_order_item.discount_invoiced discount_invoiced,
       stage_hash_magento_sales_order_item.base_discount_invoiced base_discount_invoiced,
       stage_hash_magento_sales_order_item.amount_refunded amount_refunded,
       stage_hash_magento_sales_order_item.base_amount_refunded base_amount_refunded,
       stage_hash_magento_sales_order_item.row_total row_total,
       stage_hash_magento_sales_order_item.base_row_total base_row_total,
       stage_hash_magento_sales_order_item.row_invoiced row_invoiced,
       stage_hash_magento_sales_order_item.base_row_invoiced base_row_invoiced,
       stage_hash_magento_sales_order_item.row_weight row_weight,
       stage_hash_magento_sales_order_item.base_tax_before_discount base_tax_before_discount,
       stage_hash_magento_sales_order_item.tax_before_discount tax_before_discount,
       stage_hash_magento_sales_order_item.locked_do_invoice locked_do_invoice,
       stage_hash_magento_sales_order_item.locked_do_ship locked_do_ship,
       stage_hash_magento_sales_order_item.price_incl_tax price_incl_tax,
       stage_hash_magento_sales_order_item.base_price_incl_tax base_price_incl_tax,
       stage_hash_magento_sales_order_item.row_total_incl_tax row_total_incl_tax,
       stage_hash_magento_sales_order_item.base_row_total_incl_tax base_row_total_incl_tax,
       stage_hash_magento_sales_order_item.discount_tax_compensation_amount discount_tax_compensation_amount,
       stage_hash_magento_sales_order_item.base_discount_tax_compensation_amount base_discount_tax_compensation_amount,
       stage_hash_magento_sales_order_item.discount_tax_compensation_invoiced discount_tax_compensation_invoiced,
       stage_hash_magento_sales_order_item.base_discount_tax_compensation_invoiced base_discount_tax_compensation_invoiced,
       stage_hash_magento_sales_order_item.discount_tax_compensation_refunded discount_tax_compensation_refunded,
       stage_hash_magento_sales_order_item.base_discount_tax_compensation_refunded base_discount_tax_compensation_refunded,
       stage_hash_magento_sales_order_item.tax_canceled tax_canceled,
       stage_hash_magento_sales_order_item.discount_tax_compensation_canceled discount_tax_compensation_canceled,
       stage_hash_magento_sales_order_item.tax_refunded tax_refunded,
       stage_hash_magento_sales_order_item.base_tax_refunded base_tax_refunded,
       stage_hash_magento_sales_order_item.discount_refunded discount_refunded,
       stage_hash_magento_sales_order_item.base_discount_refunded base_discount_refunded,
       stage_hash_magento_sales_order_item.free_shipping free_shipping,
       stage_hash_magento_sales_order_item.qty_returned qty_returned,
       stage_hash_magento_sales_order_item.gift_message_available gift_message_available,
       stage_hash_magento_sales_order_item.weee_tax_applied weee_tax_applied,
       stage_hash_magento_sales_order_item.weee_tax_applied_amount weee_tax_applied_amount,
       stage_hash_magento_sales_order_item.weee_tax_applied_row_amount weee_tax_applied_row_amount,
       stage_hash_magento_sales_order_item.weee_tax_disposition weee_tax_disposition,
       stage_hash_magento_sales_order_item.weee_tax_row_disposition weee_tax_row_disposition,
       stage_hash_magento_sales_order_item.base_weee_tax_applied_amount base_weee_tax_applied_amount,
       stage_hash_magento_sales_order_item.base_weee_tax_applied_row_amnt base_weee_tax_applied_row_amnt,
       stage_hash_magento_sales_order_item.base_weee_tax_disposition base_weee_tax_disposition,
       stage_hash_magento_sales_order_item.base_weee_tax_row_disposition base_weee_tax_row_disposition,
       stage_hash_magento_sales_order_item.gw_id gw_id,
       stage_hash_magento_sales_order_item.gw_base_price gw_base_price,
       stage_hash_magento_sales_order_item.gw_price gw_price,
       stage_hash_magento_sales_order_item.gw_base_tax_amount gw_base_tax_amount,
       stage_hash_magento_sales_order_item.gw_tax_amount gw_tax_amount,
       stage_hash_magento_sales_order_item.gw_base_price_invoiced gw_base_price_invoiced,
       stage_hash_magento_sales_order_item.gw_price_invoiced gw_price_invoiced,
       stage_hash_magento_sales_order_item.gw_base_tax_amount_invoiced gw_base_tax_amount_invoiced,
       stage_hash_magento_sales_order_item.gw_tax_amount_invoiced gw_tax_amount_invoiced,
       stage_hash_magento_sales_order_item.gw_base_price_refunded gw_base_price_refunded,
       stage_hash_magento_sales_order_item.gw_price_refunded gw_price_refunded,
       stage_hash_magento_sales_order_item.gw_base_tax_amount_refunded gw_base_tax_amount_refunded,
       stage_hash_magento_sales_order_item.gw_tax_amount_refunded gw_tax_amount_refunded,
       stage_hash_magento_sales_order_item.event_id event_id,
       stage_hash_magento_sales_order_item.agreement_id agreement_id,
       stage_hash_magento_sales_order_item.agreement_status agreement_status,
       stage_hash_magento_sales_order_item.agreement_api_status agreement_api_status,
       stage_hash_magento_sales_order_item.lt_bucks_redeemed lt_bucks_redeemed,
       stage_hash_magento_sales_order_item.lt_bucks_refunded lt_bucks_refunded,
       stage_hash_magento_sales_order_item.vendor vendor,
       isnull(cast(stage_hash_magento_sales_order_item.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.quote_item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_order_item.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_order_item.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.product_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.product_options,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.weight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.is_virtual as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.sku,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.applied_rule_ids,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.additional_data,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.is_qty_decimal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.no_discount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.qty_backordered as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.qty_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.qty_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.qty_ordered as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.qty_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.qty_shipped as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_cost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.original_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_original_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.tax_percent as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.tax_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_tax_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.discount_percent as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.discount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_discount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.amount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_amount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.row_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_row_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.row_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_row_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.row_weight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_tax_before_discount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.tax_before_discount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.locked_do_invoice as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.locked_do_ship as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.price_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_price_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.row_total_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_row_total_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.discount_tax_compensation_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_discount_tax_compensation_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.discount_tax_compensation_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_discount_tax_compensation_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.tax_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.discount_tax_compensation_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.discount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_discount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.free_shipping as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.qty_returned as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gift_message_available as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.weee_tax_applied,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.weee_tax_applied_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.weee_tax_applied_row_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.weee_tax_disposition as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.weee_tax_row_disposition as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_weee_tax_applied_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_weee_tax_applied_row_amnt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_weee_tax_disposition as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.base_weee_tax_row_disposition as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_base_price_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_price_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_base_tax_amount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_tax_amount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_base_price_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_price_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_base_tax_amount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.gw_tax_amount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.event_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order_item.agreement_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.agreement_status as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.agreement_api_status as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.lt_bucks_redeemed as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.lt_bucks_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order_item.vendor as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_order_item
 where stage_hash_magento_sales_order_item.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_order_item records
set @insert_date_time = getdate()
insert into s_magento_sales_order_item (
       bk_hash,
       item_id,
       quote_item_id,
       created_at,
       updated_at,
       product_type,
       product_options,
       weight,
       is_virtual,
       sku,
       name,
       description,
       applied_rule_ids,
       additional_data,
       is_qty_decimal,
       no_discount,
       qty_backordered,
       qty_canceled,
       qty_invoiced,
       qty_ordered,
       qty_refunded,
       qty_shipped,
       base_cost,
       price,
       base_price,
       original_price,
       base_original_price,
       tax_percent,
       tax_amount,
       base_tax_amount,
       tax_invoiced,
       base_tax_invoiced,
       discount_percent,
       discount_amount,
       base_discount_amount,
       discount_invoiced,
       base_discount_invoiced,
       amount_refunded,
       base_amount_refunded,
       row_total,
       base_row_total,
       row_invoiced,
       base_row_invoiced,
       row_weight,
       base_tax_before_discount,
       tax_before_discount,
       locked_do_invoice,
       locked_do_ship,
       price_incl_tax,
       base_price_incl_tax,
       row_total_incl_tax,
       base_row_total_incl_tax,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       discount_tax_compensation_invoiced,
       base_discount_tax_compensation_invoiced,
       discount_tax_compensation_refunded,
       base_discount_tax_compensation_refunded,
       tax_canceled,
       discount_tax_compensation_canceled,
       tax_refunded,
       base_tax_refunded,
       discount_refunded,
       base_discount_refunded,
       free_shipping,
       qty_returned,
       gift_message_available,
       weee_tax_applied,
       weee_tax_applied_amount,
       weee_tax_applied_row_amount,
       weee_tax_disposition,
       weee_tax_row_disposition,
       base_weee_tax_applied_amount,
       base_weee_tax_applied_row_amnt,
       base_weee_tax_disposition,
       base_weee_tax_row_disposition,
       gw_id,
       gw_base_price,
       gw_price,
       gw_base_tax_amount,
       gw_tax_amount,
       gw_base_price_invoiced,
       gw_price_invoiced,
       gw_base_tax_amount_invoiced,
       gw_tax_amount_invoiced,
       gw_base_price_refunded,
       gw_price_refunded,
       gw_base_tax_amount_refunded,
       gw_tax_amount_refunded,
       event_id,
       agreement_id,
       agreement_status,
       agreement_api_status,
       lt_bucks_redeemed,
       lt_bucks_refunded,
       vendor,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_order_item_inserts.bk_hash,
       #s_magento_sales_order_item_inserts.item_id,
       #s_magento_sales_order_item_inserts.quote_item_id,
       #s_magento_sales_order_item_inserts.created_at,
       #s_magento_sales_order_item_inserts.updated_at,
       #s_magento_sales_order_item_inserts.product_type,
       #s_magento_sales_order_item_inserts.product_options,
       #s_magento_sales_order_item_inserts.weight,
       #s_magento_sales_order_item_inserts.is_virtual,
       #s_magento_sales_order_item_inserts.sku,
       #s_magento_sales_order_item_inserts.name,
       #s_magento_sales_order_item_inserts.description,
       #s_magento_sales_order_item_inserts.applied_rule_ids,
       #s_magento_sales_order_item_inserts.additional_data,
       #s_magento_sales_order_item_inserts.is_qty_decimal,
       #s_magento_sales_order_item_inserts.no_discount,
       #s_magento_sales_order_item_inserts.qty_backordered,
       #s_magento_sales_order_item_inserts.qty_canceled,
       #s_magento_sales_order_item_inserts.qty_invoiced,
       #s_magento_sales_order_item_inserts.qty_ordered,
       #s_magento_sales_order_item_inserts.qty_refunded,
       #s_magento_sales_order_item_inserts.qty_shipped,
       #s_magento_sales_order_item_inserts.base_cost,
       #s_magento_sales_order_item_inserts.price,
       #s_magento_sales_order_item_inserts.base_price,
       #s_magento_sales_order_item_inserts.original_price,
       #s_magento_sales_order_item_inserts.base_original_price,
       #s_magento_sales_order_item_inserts.tax_percent,
       #s_magento_sales_order_item_inserts.tax_amount,
       #s_magento_sales_order_item_inserts.base_tax_amount,
       #s_magento_sales_order_item_inserts.tax_invoiced,
       #s_magento_sales_order_item_inserts.base_tax_invoiced,
       #s_magento_sales_order_item_inserts.discount_percent,
       #s_magento_sales_order_item_inserts.discount_amount,
       #s_magento_sales_order_item_inserts.base_discount_amount,
       #s_magento_sales_order_item_inserts.discount_invoiced,
       #s_magento_sales_order_item_inserts.base_discount_invoiced,
       #s_magento_sales_order_item_inserts.amount_refunded,
       #s_magento_sales_order_item_inserts.base_amount_refunded,
       #s_magento_sales_order_item_inserts.row_total,
       #s_magento_sales_order_item_inserts.base_row_total,
       #s_magento_sales_order_item_inserts.row_invoiced,
       #s_magento_sales_order_item_inserts.base_row_invoiced,
       #s_magento_sales_order_item_inserts.row_weight,
       #s_magento_sales_order_item_inserts.base_tax_before_discount,
       #s_magento_sales_order_item_inserts.tax_before_discount,
       #s_magento_sales_order_item_inserts.locked_do_invoice,
       #s_magento_sales_order_item_inserts.locked_do_ship,
       #s_magento_sales_order_item_inserts.price_incl_tax,
       #s_magento_sales_order_item_inserts.base_price_incl_tax,
       #s_magento_sales_order_item_inserts.row_total_incl_tax,
       #s_magento_sales_order_item_inserts.base_row_total_incl_tax,
       #s_magento_sales_order_item_inserts.discount_tax_compensation_amount,
       #s_magento_sales_order_item_inserts.base_discount_tax_compensation_amount,
       #s_magento_sales_order_item_inserts.discount_tax_compensation_invoiced,
       #s_magento_sales_order_item_inserts.base_discount_tax_compensation_invoiced,
       #s_magento_sales_order_item_inserts.discount_tax_compensation_refunded,
       #s_magento_sales_order_item_inserts.base_discount_tax_compensation_refunded,
       #s_magento_sales_order_item_inserts.tax_canceled,
       #s_magento_sales_order_item_inserts.discount_tax_compensation_canceled,
       #s_magento_sales_order_item_inserts.tax_refunded,
       #s_magento_sales_order_item_inserts.base_tax_refunded,
       #s_magento_sales_order_item_inserts.discount_refunded,
       #s_magento_sales_order_item_inserts.base_discount_refunded,
       #s_magento_sales_order_item_inserts.free_shipping,
       #s_magento_sales_order_item_inserts.qty_returned,
       #s_magento_sales_order_item_inserts.gift_message_available,
       #s_magento_sales_order_item_inserts.weee_tax_applied,
       #s_magento_sales_order_item_inserts.weee_tax_applied_amount,
       #s_magento_sales_order_item_inserts.weee_tax_applied_row_amount,
       #s_magento_sales_order_item_inserts.weee_tax_disposition,
       #s_magento_sales_order_item_inserts.weee_tax_row_disposition,
       #s_magento_sales_order_item_inserts.base_weee_tax_applied_amount,
       #s_magento_sales_order_item_inserts.base_weee_tax_applied_row_amnt,
       #s_magento_sales_order_item_inserts.base_weee_tax_disposition,
       #s_magento_sales_order_item_inserts.base_weee_tax_row_disposition,
       #s_magento_sales_order_item_inserts.gw_id,
       #s_magento_sales_order_item_inserts.gw_base_price,
       #s_magento_sales_order_item_inserts.gw_price,
       #s_magento_sales_order_item_inserts.gw_base_tax_amount,
       #s_magento_sales_order_item_inserts.gw_tax_amount,
       #s_magento_sales_order_item_inserts.gw_base_price_invoiced,
       #s_magento_sales_order_item_inserts.gw_price_invoiced,
       #s_magento_sales_order_item_inserts.gw_base_tax_amount_invoiced,
       #s_magento_sales_order_item_inserts.gw_tax_amount_invoiced,
       #s_magento_sales_order_item_inserts.gw_base_price_refunded,
       #s_magento_sales_order_item_inserts.gw_price_refunded,
       #s_magento_sales_order_item_inserts.gw_base_tax_amount_refunded,
       #s_magento_sales_order_item_inserts.gw_tax_amount_refunded,
       #s_magento_sales_order_item_inserts.event_id,
       #s_magento_sales_order_item_inserts.agreement_id,
       #s_magento_sales_order_item_inserts.agreement_status,
       #s_magento_sales_order_item_inserts.agreement_api_status,
       #s_magento_sales_order_item_inserts.lt_bucks_redeemed,
       #s_magento_sales_order_item_inserts.lt_bucks_refunded,
       #s_magento_sales_order_item_inserts.vendor,
       case when s_magento_sales_order_item.s_magento_sales_order_item_id is null then isnull(#s_magento_sales_order_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_order_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_order_item_inserts
  left join p_magento_sales_order_item
    on #s_magento_sales_order_item_inserts.bk_hash = p_magento_sales_order_item.bk_hash
   and p_magento_sales_order_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_order_item
    on p_magento_sales_order_item.bk_hash = s_magento_sales_order_item.bk_hash
   and p_magento_sales_order_item.s_magento_sales_order_item_id = s_magento_sales_order_item.s_magento_sales_order_item_id
 where s_magento_sales_order_item.s_magento_sales_order_item_id is null
    or (s_magento_sales_order_item.s_magento_sales_order_item_id is not null
        and s_magento_sales_order_item.dv_hash <> #s_magento_sales_order_item_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_order_item @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_order_item @current_dv_batch_id

end
