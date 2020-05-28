CREATE PROC [dbo].[proc_etl_magento_sales_order] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_order

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_order (
       bk_hash,
       entity_id,
       state,
       status,
       coupon_code,
       protect_code,
       shipping_description,
       is_virtual,
       store_id,
       customer_id,
       base_discount_amount,
       base_discount_canceled,
       base_discount_invoiced,
       base_discount_refunded,
       base_grand_total,
       base_shipping_amount,
       base_shipping_canceled,
       base_shipping_invoiced,
       base_shipping_refunded,
       base_shipping_tax_amount,
       base_shipping_tax_refunded,
       base_subtotal,
       base_subtotal_canceled,
       base_subtotal_invoiced,
       base_subtotal_refunded,
       base_tax_amount,
       base_tax_canceled,
       base_tax_invoiced,
       base_tax_refunded,
       base_to_global_rate,
       base_to_order_rate,
       base_total_canceled,
       base_total_invoiced,
       base_total_invoiced_cost,
       base_total_offline_refunded,
       base_total_online_refunded,
       base_total_paid,
       base_total_qty_ordered,
       base_total_refunded,
       discount_amount,
       discount_canceled,
       discount_invoiced,
       discount_refunded,
       grand_total,
       shipping_amount,
       shipping_canceled,
       shipping_invoiced,
       shipping_refunded,
       shipping_tax_amount,
       shipping_tax_refunded,
       store_to_base_rate,
       store_to_order_rate,
       subtotal,
       subtotal_canceled,
       subtotal_invoiced,
       subtotal_refunded,
       tax_amount,
       tax_canceled,
       tax_invoiced,
       tax_refunded,
       total_canceled,
       total_invoiced,
       total_offline_refunded,
       total_online_refunded,
       total_paid,
       total_qty_ordered,
       total_refunded,
       can_ship_partially,
       can_ship_partially_item,
       customer_is_guest,
       customer_note_notify,
       billing_address_id,
       customer_group_id,
       edit_increment,
       email_sent,
       send_email,
       forced_shipment_with_invoice,
       payment_auth_expiration,
       quote_address_id,
       quote_id,
       shipping_address_id,
       adjustment_negative,
       adjustment_positive,
       base_adjustment_negative,
       base_adjustment_positive,
       base_shipping_discount_amount,
       base_subtotal_incl_tax,
       base_total_due,
       payment_authorization_amount,
       shipping_discount_amount,
       subtotal_incl_tax,
       total_due,
       weight,
       customer_dob,
       increment_id,
       applied_rule_ids,
       base_currency_code,
       customer_email,
       customer_firstname,
       customer_lastname,
       customer_middlename,
       customer_prefix,
       customer_suffix,
       customer_taxvat,
       discount_description,
       ext_customer_id,
       ext_order_id,
       global_currency_code,
       hold_before_state,
       hold_before_status,
       order_currency_code,
       original_increment_id,
       relation_child_id,
       relation_child_real_id,
       relation_parent_id,
       relation_parent_real_id,
       remote_ip,
       shipping_method,
       store_currency_code,
       store_name,
       x_forwarded_for,
       customer_note,
       created_at,
       updated_at,
       total_item_count,
       customer_gender,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       shipping_discount_tax_compensation_amount,
       base_shipping_discount_tax_compensation_amnt,
       discount_tax_compensation_invoiced,
       base_discount_tax_compensation_invoiced,
       discount_tax_compensation_refunded,
       base_discount_tax_compensation_refunded,
       shipping_incl_tax,
       base_shipping_incl_tax,
       coupon_rule_name,
       paypal_ipn_customer_notified,
       base_customer_balance_amount,
       customer_balance_amount,
       base_customer_balance_invoiced,
       customer_balance_invoiced,
       base_customer_balance_refunded,
       customer_balance_refunded,
       bs_customer_bal_total_refunded,
       customer_bal_total_refunded,
       gift_cards,
       base_gift_cards_amount,
       gift_cards_amount,
       base_gift_cards_invoiced,
       gift_cards_invoiced,
       base_gift_cards_refunded,
       gift_cards_refunded,
       gift_message_id,
       gw_id,
       gw_allow_gift_receipt,
       gw_add_card,
       gw_base_price,
       gw_price,
       gw_items_base_price,
       gw_items_price,
       gw_card_base_price,
       gw_card_price,
       gw_base_tax_amount,
       gw_tax_amount,
       gw_items_base_tax_amount,
       gw_items_tax_amount,
       gw_card_base_tax_amount,
       gw_card_tax_amount,
       gw_base_price_incl_tax,
       gw_price_incl_tax,
       gw_items_base_price_incl_tax,
       gw_items_price_incl_tax,
       gw_card_base_price_incl_tax,
       gw_card_price_incl_tax,
       gw_base_price_invoiced,
       gw_price_invoiced,
       gw_items_base_price_invoiced,
       gw_items_price_invoiced,
       gw_card_base_price_invoiced,
       gw_card_price_invoiced,
       gw_base_tax_amount_invoiced,
       gw_tax_amount_invoiced,
       gw_items_base_tax_invoiced,
       gw_items_tax_invoiced,
       gw_card_base_tax_invoiced,
       gw_card_tax_invoiced,
       gw_base_price_refunded,
       gw_price_refunded,
       gw_items_base_price_refunded,
       gw_items_price_refunded,
       gw_card_base_price_refunded,
       gw_card_price_refunded,
       gw_base_tax_amount_refunded,
       gw_tax_amount_refunded,
       gw_items_base_tax_refunded,
       gw_items_tax_refunded,
       gw_card_base_tax_refunded,
       gw_card_tax_refunded,
       reward_points_balance,
       base_reward_currency_amount,
       reward_currency_amount,
       base_rwrd_crrncy_amt_invoiced,
       rwrd_currency_amount_invoiced,
       base_rwrd_crrncy_amnt_refnded,
       rwrd_crrncy_amnt_refunded,
       reward_points_balance_refund,
       m1_order_id,
       trainer_id,
       trainer_name,
       mms_party_id,
       lt_bucks_redeemed,
       lt_bucks_refunded,
       customer_mms_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       state,
       status,
       coupon_code,
       protect_code,
       shipping_description,
       is_virtual,
       store_id,
       customer_id,
       base_discount_amount,
       base_discount_canceled,
       base_discount_invoiced,
       base_discount_refunded,
       base_grand_total,
       base_shipping_amount,
       base_shipping_canceled,
       base_shipping_invoiced,
       base_shipping_refunded,
       base_shipping_tax_amount,
       base_shipping_tax_refunded,
       base_subtotal,
       base_subtotal_canceled,
       base_subtotal_invoiced,
       base_subtotal_refunded,
       base_tax_amount,
       base_tax_canceled,
       base_tax_invoiced,
       base_tax_refunded,
       base_to_global_rate,
       base_to_order_rate,
       base_total_canceled,
       base_total_invoiced,
       base_total_invoiced_cost,
       base_total_offline_refunded,
       base_total_online_refunded,
       base_total_paid,
       base_total_qty_ordered,
       base_total_refunded,
       discount_amount,
       discount_canceled,
       discount_invoiced,
       discount_refunded,
       grand_total,
       shipping_amount,
       shipping_canceled,
       shipping_invoiced,
       shipping_refunded,
       shipping_tax_amount,
       shipping_tax_refunded,
       store_to_base_rate,
       store_to_order_rate,
       subtotal,
       subtotal_canceled,
       subtotal_invoiced,
       subtotal_refunded,
       tax_amount,
       tax_canceled,
       tax_invoiced,
       tax_refunded,
       total_canceled,
       total_invoiced,
       total_offline_refunded,
       total_online_refunded,
       total_paid,
       total_qty_ordered,
       total_refunded,
       can_ship_partially,
       can_ship_partially_item,
       customer_is_guest,
       customer_note_notify,
       billing_address_id,
       customer_group_id,
       edit_increment,
       email_sent,
       send_email,
       forced_shipment_with_invoice,
       payment_auth_expiration,
       quote_address_id,
       quote_id,
       shipping_address_id,
       adjustment_negative,
       adjustment_positive,
       base_adjustment_negative,
       base_adjustment_positive,
       base_shipping_discount_amount,
       base_subtotal_incl_tax,
       base_total_due,
       payment_authorization_amount,
       shipping_discount_amount,
       subtotal_incl_tax,
       total_due,
       weight,
       customer_dob,
       increment_id,
       applied_rule_ids,
       base_currency_code,
       customer_email,
       customer_firstname,
       customer_lastname,
       customer_middlename,
       customer_prefix,
       customer_suffix,
       customer_taxvat,
       discount_description,
       ext_customer_id,
       ext_order_id,
       global_currency_code,
       hold_before_state,
       hold_before_status,
       order_currency_code,
       original_increment_id,
       relation_child_id,
       relation_child_real_id,
       relation_parent_id,
       relation_parent_real_id,
       remote_ip,
       shipping_method,
       store_currency_code,
       store_name,
       x_forwarded_for,
       customer_note,
       created_at,
       updated_at,
       total_item_count,
       customer_gender,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       shipping_discount_tax_compensation_amount,
       base_shipping_discount_tax_compensation_amnt,
       discount_tax_compensation_invoiced,
       base_discount_tax_compensation_invoiced,
       discount_tax_compensation_refunded,
       base_discount_tax_compensation_refunded,
       shipping_incl_tax,
       base_shipping_incl_tax,
       coupon_rule_name,
       paypal_ipn_customer_notified,
       base_customer_balance_amount,
       customer_balance_amount,
       base_customer_balance_invoiced,
       customer_balance_invoiced,
       base_customer_balance_refunded,
       customer_balance_refunded,
       bs_customer_bal_total_refunded,
       customer_bal_total_refunded,
       gift_cards,
       base_gift_cards_amount,
       gift_cards_amount,
       base_gift_cards_invoiced,
       gift_cards_invoiced,
       base_gift_cards_refunded,
       gift_cards_refunded,
       gift_message_id,
       gw_id,
       gw_allow_gift_receipt,
       gw_add_card,
       gw_base_price,
       gw_price,
       gw_items_base_price,
       gw_items_price,
       gw_card_base_price,
       gw_card_price,
       gw_base_tax_amount,
       gw_tax_amount,
       gw_items_base_tax_amount,
       gw_items_tax_amount,
       gw_card_base_tax_amount,
       gw_card_tax_amount,
       gw_base_price_incl_tax,
       gw_price_incl_tax,
       gw_items_base_price_incl_tax,
       gw_items_price_incl_tax,
       gw_card_base_price_incl_tax,
       gw_card_price_incl_tax,
       gw_base_price_invoiced,
       gw_price_invoiced,
       gw_items_base_price_invoiced,
       gw_items_price_invoiced,
       gw_card_base_price_invoiced,
       gw_card_price_invoiced,
       gw_base_tax_amount_invoiced,
       gw_tax_amount_invoiced,
       gw_items_base_tax_invoiced,
       gw_items_tax_invoiced,
       gw_card_base_tax_invoiced,
       gw_card_tax_invoiced,
       gw_base_price_refunded,
       gw_price_refunded,
       gw_items_base_price_refunded,
       gw_items_price_refunded,
       gw_card_base_price_refunded,
       gw_card_price_refunded,
       gw_base_tax_amount_refunded,
       gw_tax_amount_refunded,
       gw_items_base_tax_refunded,
       gw_items_tax_refunded,
       gw_card_base_tax_refunded,
       gw_card_tax_refunded,
       reward_points_balance,
       base_reward_currency_amount,
       reward_currency_amount,
       base_rwrd_crrncy_amt_invoiced,
       rwrd_currency_amount_invoiced,
       base_rwrd_crrncy_amnt_refnded,
       rwrd_crrncy_amnt_refunded,
       reward_points_balance_refund,
       m1_order_id,
       trainer_id,
       trainer_name,
       mms_party_id,
       lt_bucks_redeemed,
       lt_bucks_refunded,
       customer_mms_id,
       isnull(cast(stage_magento_sales_order.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_order
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_order @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_order (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_magento_sales_order.bk_hash,
       stage_hash_magento_sales_order.entity_id entity_id,
       isnull(cast(stage_hash_magento_sales_order.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_order
  left join h_magento_sales_order
    on stage_hash_magento_sales_order.bk_hash = h_magento_sales_order.bk_hash
 where h_magento_sales_order_id is null
   and stage_hash_magento_sales_order.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_order
if object_id('tempdb..#l_magento_sales_order_inserts') is not null drop table #l_magento_sales_order_inserts
create table #l_magento_sales_order_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_order.bk_hash,
       stage_hash_magento_sales_order.entity_id entity_id,
       stage_hash_magento_sales_order.store_id store_id,
       stage_hash_magento_sales_order.customer_id customer_id,
       stage_hash_magento_sales_order.billing_address_id billing_address_id,
       stage_hash_magento_sales_order.customer_group_id customer_group_id,
       stage_hash_magento_sales_order.quote_address_id quote_address_id,
       stage_hash_magento_sales_order.quote_id quote_id,
       stage_hash_magento_sales_order.shipping_address_id shipping_address_id,
       stage_hash_magento_sales_order.increment_id increment_id,
       stage_hash_magento_sales_order.ext_customer_id ext_customer_id,
       stage_hash_magento_sales_order.ext_order_id ext_order_id,
       stage_hash_magento_sales_order.original_increment_id original_increment_id,
       stage_hash_magento_sales_order.relation_child_id relation_child_id,
       stage_hash_magento_sales_order.relation_child_real_id relation_child_real_id,
       stage_hash_magento_sales_order.relation_parent_id relation_parent_id,
       stage_hash_magento_sales_order.relation_parent_real_id relation_parent_real_id,
       stage_hash_magento_sales_order.gift_message_id gift_message_id,
       stage_hash_magento_sales_order.gw_id gw_id,
       stage_hash_magento_sales_order.m1_order_id m1_order_id,
       stage_hash_magento_sales_order.customer_mms_id customer_mms_id,
       isnull(cast(stage_hash_magento_sales_order.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.store_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.billing_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.quote_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.quote_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.increment_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.ext_customer_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.ext_order_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.original_increment_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.relation_child_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.relation_child_real_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.relation_parent_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.relation_parent_real_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gift_message_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.m1_order_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_mms_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_order
 where stage_hash_magento_sales_order.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_order records
set @insert_date_time = getdate()
insert into l_magento_sales_order (
       bk_hash,
       entity_id,
       store_id,
       customer_id,
       billing_address_id,
       customer_group_id,
       quote_address_id,
       quote_id,
       shipping_address_id,
       increment_id,
       ext_customer_id,
       ext_order_id,
       original_increment_id,
       relation_child_id,
       relation_child_real_id,
       relation_parent_id,
       relation_parent_real_id,
       gift_message_id,
       gw_id,
       m1_order_id,
       customer_mms_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_order_inserts.bk_hash,
       #l_magento_sales_order_inserts.entity_id,
       #l_magento_sales_order_inserts.store_id,
       #l_magento_sales_order_inserts.customer_id,
       #l_magento_sales_order_inserts.billing_address_id,
       #l_magento_sales_order_inserts.customer_group_id,
       #l_magento_sales_order_inserts.quote_address_id,
       #l_magento_sales_order_inserts.quote_id,
       #l_magento_sales_order_inserts.shipping_address_id,
       #l_magento_sales_order_inserts.increment_id,
       #l_magento_sales_order_inserts.ext_customer_id,
       #l_magento_sales_order_inserts.ext_order_id,
       #l_magento_sales_order_inserts.original_increment_id,
       #l_magento_sales_order_inserts.relation_child_id,
       #l_magento_sales_order_inserts.relation_child_real_id,
       #l_magento_sales_order_inserts.relation_parent_id,
       #l_magento_sales_order_inserts.relation_parent_real_id,
       #l_magento_sales_order_inserts.gift_message_id,
       #l_magento_sales_order_inserts.gw_id,
       #l_magento_sales_order_inserts.m1_order_id,
       #l_magento_sales_order_inserts.customer_mms_id,
       case when l_magento_sales_order.l_magento_sales_order_id is null then isnull(#l_magento_sales_order_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_order_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_order_inserts
  left join p_magento_sales_order
    on #l_magento_sales_order_inserts.bk_hash = p_magento_sales_order.bk_hash
   and p_magento_sales_order.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_order
    on p_magento_sales_order.bk_hash = l_magento_sales_order.bk_hash
   and p_magento_sales_order.l_magento_sales_order_id = l_magento_sales_order.l_magento_sales_order_id
 where l_magento_sales_order.l_magento_sales_order_id is null
    or (l_magento_sales_order.l_magento_sales_order_id is not null
        and l_magento_sales_order.dv_hash <> #l_magento_sales_order_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_order
if object_id('tempdb..#s_magento_sales_order_inserts') is not null drop table #s_magento_sales_order_inserts
create table #s_magento_sales_order_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_order.bk_hash,
       stage_hash_magento_sales_order.entity_id entity_id,
       stage_hash_magento_sales_order.state state,
       stage_hash_magento_sales_order.status status,
       stage_hash_magento_sales_order.coupon_code coupon_code,
       stage_hash_magento_sales_order.protect_code protect_code,
       stage_hash_magento_sales_order.shipping_description shipping_description,
       stage_hash_magento_sales_order.is_virtual is_virtual,
       stage_hash_magento_sales_order.base_discount_amount base_discount_amount,
       stage_hash_magento_sales_order.base_discount_canceled base_discount_canceled,
       stage_hash_magento_sales_order.base_discount_invoiced base_discount_invoiced,
       stage_hash_magento_sales_order.base_discount_refunded base_discount_refunded,
       stage_hash_magento_sales_order.base_grand_total base_grand_total,
       stage_hash_magento_sales_order.base_shipping_amount base_shipping_amount,
       stage_hash_magento_sales_order.base_shipping_canceled base_shipping_canceled,
       stage_hash_magento_sales_order.base_shipping_invoiced base_shipping_invoiced,
       stage_hash_magento_sales_order.base_shipping_refunded base_shipping_refunded,
       stage_hash_magento_sales_order.base_shipping_tax_amount base_shipping_tax_amount,
       stage_hash_magento_sales_order.base_shipping_tax_refunded base_shipping_tax_refunded,
       stage_hash_magento_sales_order.base_subtotal base_subtotal,
       stage_hash_magento_sales_order.base_subtotal_canceled base_subtotal_canceled,
       stage_hash_magento_sales_order.base_subtotal_invoiced base_subtotal_invoiced,
       stage_hash_magento_sales_order.base_subtotal_refunded base_subtotal_refunded,
       stage_hash_magento_sales_order.base_tax_amount base_tax_amount,
       stage_hash_magento_sales_order.base_tax_canceled base_tax_canceled,
       stage_hash_magento_sales_order.base_tax_invoiced base_tax_invoiced,
       stage_hash_magento_sales_order.base_tax_refunded base_tax_refunded,
       stage_hash_magento_sales_order.base_to_global_rate base_to_global_rate,
       stage_hash_magento_sales_order.base_to_order_rate base_to_order_rate,
       stage_hash_magento_sales_order.base_total_canceled base_total_canceled,
       stage_hash_magento_sales_order.base_total_invoiced base_total_invoiced,
       stage_hash_magento_sales_order.base_total_invoiced_cost base_total_invoiced_cost,
       stage_hash_magento_sales_order.base_total_offline_refunded base_total_offline_refunded,
       stage_hash_magento_sales_order.base_total_online_refunded base_total_online_refunded,
       stage_hash_magento_sales_order.base_total_paid base_total_paid,
       stage_hash_magento_sales_order.base_total_qty_ordered base_total_qty_ordered,
       stage_hash_magento_sales_order.base_total_refunded base_total_refunded,
       stage_hash_magento_sales_order.discount_amount discount_amount,
       stage_hash_magento_sales_order.discount_canceled discount_canceled,
       stage_hash_magento_sales_order.discount_invoiced discount_invoiced,
       stage_hash_magento_sales_order.discount_refunded discount_refunded,
       stage_hash_magento_sales_order.grand_total grand_total,
       stage_hash_magento_sales_order.shipping_amount shipping_amount,
       stage_hash_magento_sales_order.shipping_canceled shipping_canceled,
       stage_hash_magento_sales_order.shipping_invoiced shipping_invoiced,
       stage_hash_magento_sales_order.shipping_refunded shipping_refunded,
       stage_hash_magento_sales_order.shipping_tax_amount shipping_tax_amount,
       stage_hash_magento_sales_order.shipping_tax_refunded shipping_tax_refunded,
       stage_hash_magento_sales_order.store_to_base_rate store_to_base_rate,
       stage_hash_magento_sales_order.store_to_order_rate store_to_order_rate,
       stage_hash_magento_sales_order.subtotal subtotal,
       stage_hash_magento_sales_order.subtotal_canceled subtotal_canceled,
       stage_hash_magento_sales_order.subtotal_invoiced subtotal_invoiced,
       stage_hash_magento_sales_order.subtotal_refunded subtotal_refunded,
       stage_hash_magento_sales_order.tax_amount tax_amount,
       stage_hash_magento_sales_order.tax_canceled tax_canceled,
       stage_hash_magento_sales_order.tax_invoiced tax_invoiced,
       stage_hash_magento_sales_order.tax_refunded tax_refunded,
       stage_hash_magento_sales_order.total_canceled total_canceled,
       stage_hash_magento_sales_order.total_invoiced total_invoiced,
       stage_hash_magento_sales_order.total_offline_refunded total_offline_refunded,
       stage_hash_magento_sales_order.total_online_refunded total_online_refunded,
       stage_hash_magento_sales_order.total_paid total_paid,
       stage_hash_magento_sales_order.total_qty_ordered total_qty_ordered,
       stage_hash_magento_sales_order.total_refunded total_refunded,
       stage_hash_magento_sales_order.can_ship_partially can_ship_partially,
       stage_hash_magento_sales_order.can_ship_partially_item can_ship_partially_item,
       stage_hash_magento_sales_order.customer_is_guest customer_is_guest,
       stage_hash_magento_sales_order.customer_note_notify customer_note_notify,
       stage_hash_magento_sales_order.edit_increment edit_increment,
       stage_hash_magento_sales_order.email_sent email_sent,
       stage_hash_magento_sales_order.send_email send_email,
       stage_hash_magento_sales_order.forced_shipment_with_invoice forced_shipment_with_invoice,
       stage_hash_magento_sales_order.payment_auth_expiration payment_auth_expiration,
       stage_hash_magento_sales_order.adjustment_negative adjustment_negative,
       stage_hash_magento_sales_order.adjustment_positive adjustment_positive,
       stage_hash_magento_sales_order.base_adjustment_negative base_adjustment_negative,
       stage_hash_magento_sales_order.base_adjustment_positive base_adjustment_positive,
       stage_hash_magento_sales_order.base_shipping_discount_amount base_shipping_discount_amount,
       stage_hash_magento_sales_order.base_subtotal_incl_tax base_subtotal_incl_tax,
       stage_hash_magento_sales_order.base_total_due base_total_due,
       stage_hash_magento_sales_order.payment_authorization_amount payment_authorization_amount,
       stage_hash_magento_sales_order.shipping_discount_amount shipping_discount_amount,
       stage_hash_magento_sales_order.subtotal_incl_tax subtotal_incl_tax,
       stage_hash_magento_sales_order.total_due total_due,
       stage_hash_magento_sales_order.weight weight,
       stage_hash_magento_sales_order.customer_dob customer_dob,
       stage_hash_magento_sales_order.applied_rule_ids applied_rule_ids,
       stage_hash_magento_sales_order.base_currency_code base_currency_code,
       stage_hash_magento_sales_order.customer_email customer_email,
       stage_hash_magento_sales_order.customer_firstname customer_first_name,
       stage_hash_magento_sales_order.customer_lastname customer_last_name,
       stage_hash_magento_sales_order.customer_middlename customer_middle_name,
       stage_hash_magento_sales_order.customer_prefix customer_prefix,
       stage_hash_magento_sales_order.customer_suffix customer_suffix,
       stage_hash_magento_sales_order.customer_taxvat customer_taxvat,
       stage_hash_magento_sales_order.discount_description discount_description,
       stage_hash_magento_sales_order.global_currency_code global_currency_code,
       stage_hash_magento_sales_order.hold_before_state hold_before_state,
       stage_hash_magento_sales_order.hold_before_status hold_before_status,
       stage_hash_magento_sales_order.order_currency_code order_currency_code,
       stage_hash_magento_sales_order.remote_ip remote_ip,
       stage_hash_magento_sales_order.shipping_method shipping_method,
       stage_hash_magento_sales_order.store_currency_code store_currency_code,
       stage_hash_magento_sales_order.store_name store_name,
       stage_hash_magento_sales_order.x_forwarded_for x_forwarded_for,
       stage_hash_magento_sales_order.customer_note customer_note,
       stage_hash_magento_sales_order.created_at created_at,
       stage_hash_magento_sales_order.updated_at updated_at,
       stage_hash_magento_sales_order.total_item_count total_item_count,
       stage_hash_magento_sales_order.customer_gender customer_gender,
       stage_hash_magento_sales_order.discount_tax_compensation_amount discount_tax_compensation_amount,
       stage_hash_magento_sales_order.base_discount_tax_compensation_amount base_discount_tax_compensation_amount,
       stage_hash_magento_sales_order.shipping_discount_tax_compensation_amount shipping_discount_tax_compensation_amount,
       stage_hash_magento_sales_order.base_shipping_discount_tax_compensation_amnt base_shipping_discount_tax_compensation_amount,
       stage_hash_magento_sales_order.discount_tax_compensation_invoiced discount_tax_compensation_invoiced,
       stage_hash_magento_sales_order.base_discount_tax_compensation_invoiced base_discount_tax_compensation_invoiced,
       stage_hash_magento_sales_order.discount_tax_compensation_refunded discount_tax_compensation_refunded,
       stage_hash_magento_sales_order.base_discount_tax_compensation_refunded base_discount_tax_compensation_refunded,
       stage_hash_magento_sales_order.shipping_incl_tax shipping_incl_tax,
       stage_hash_magento_sales_order.base_shipping_incl_tax base_shipping_incl_tax,
       stage_hash_magento_sales_order.coupon_rule_name coupon_rule_name,
       stage_hash_magento_sales_order.paypal_ipn_customer_notified paypal_ipn_customer_notified,
       stage_hash_magento_sales_order.base_customer_balance_amount base_customer_balance_amount,
       stage_hash_magento_sales_order.customer_balance_amount customer_balance_amount,
       stage_hash_magento_sales_order.base_customer_balance_invoiced base_customer_balance_invoiced,
       stage_hash_magento_sales_order.customer_balance_invoiced customer_balance_invoiced,
       stage_hash_magento_sales_order.base_customer_balance_refunded base_customer_balance_refunded,
       stage_hash_magento_sales_order.customer_balance_refunded customer_balance_refunded,
       stage_hash_magento_sales_order.bs_customer_bal_total_refunded bs_customer_bal_total_refunded,
       stage_hash_magento_sales_order.customer_bal_total_refunded customer_bal_total_refunded,
       stage_hash_magento_sales_order.gift_cards gift_cards,
       stage_hash_magento_sales_order.base_gift_cards_amount base_gift_cards_amount,
       stage_hash_magento_sales_order.gift_cards_amount gift_cards_amount,
       stage_hash_magento_sales_order.base_gift_cards_invoiced base_gift_cards_invoiced,
       stage_hash_magento_sales_order.gift_cards_invoiced gift_cards_invoiced,
       stage_hash_magento_sales_order.base_gift_cards_refunded base_gift_cards_refunded,
       stage_hash_magento_sales_order.gift_cards_refunded gift_cards_refunded,
       stage_hash_magento_sales_order.gw_allow_gift_receipt gw_allow_gift_receipt,
       stage_hash_magento_sales_order.gw_add_card gw_add_card,
       stage_hash_magento_sales_order.gw_base_price gw_base_price,
       stage_hash_magento_sales_order.gw_price gw_price,
       stage_hash_magento_sales_order.gw_items_base_price gw_items_base_price,
       stage_hash_magento_sales_order.gw_items_price gw_items_price,
       stage_hash_magento_sales_order.gw_card_base_price gw_card_base_price,
       stage_hash_magento_sales_order.gw_card_price gw_card_price,
       stage_hash_magento_sales_order.gw_base_tax_amount gw_base_tax_amount,
       stage_hash_magento_sales_order.gw_tax_amount gw_tax_amount,
       stage_hash_magento_sales_order.gw_items_base_tax_amount gw_items_base_tax_amount,
       stage_hash_magento_sales_order.gw_items_tax_amount gw_items_tax_amount,
       stage_hash_magento_sales_order.gw_card_base_tax_amount gw_card_base_tax_amount,
       stage_hash_magento_sales_order.gw_card_tax_amount gw_card_tax_amount,
       stage_hash_magento_sales_order.gw_base_price_incl_tax gw_base_price_incl_tax,
       stage_hash_magento_sales_order.gw_price_incl_tax gw_price_incl_tax,
       stage_hash_magento_sales_order.gw_items_base_price_incl_tax gw_items_base_price_incl_tax,
       stage_hash_magento_sales_order.gw_items_price_incl_tax gw_items_price_incl_tax,
       stage_hash_magento_sales_order.gw_card_base_price_incl_tax gw_card_base_price_incl_tax,
       stage_hash_magento_sales_order.gw_card_price_incl_tax gw_card_price_incl_tax,
       stage_hash_magento_sales_order.gw_base_price_invoiced gw_base_price_invoiced,
       stage_hash_magento_sales_order.gw_price_invoiced gw_price_invoiced,
       stage_hash_magento_sales_order.gw_items_base_price_invoiced gw_items_base_price_invoiced,
       stage_hash_magento_sales_order.gw_items_price_invoiced gw_items_price_invoiced,
       stage_hash_magento_sales_order.gw_card_base_price_invoiced gw_card_base_price_invoiced,
       stage_hash_magento_sales_order.gw_card_price_invoiced gw_card_price_invoiced,
       stage_hash_magento_sales_order.gw_base_tax_amount_invoiced gw_base_tax_amount_invoiced,
       stage_hash_magento_sales_order.gw_tax_amount_invoiced gw_tax_amount_invoiced,
       stage_hash_magento_sales_order.gw_items_base_tax_invoiced gw_items_base_tax_invoiced,
       stage_hash_magento_sales_order.gw_items_tax_invoiced gw_items_tax_invoiced,
       stage_hash_magento_sales_order.gw_card_base_tax_invoiced gw_card_base_tax_invoiced,
       stage_hash_magento_sales_order.gw_card_tax_invoiced gw_card_tax_invoiced,
       stage_hash_magento_sales_order.gw_base_price_refunded gw_base_price_refunded,
       stage_hash_magento_sales_order.gw_price_refunded gw_price_refunded,
       stage_hash_magento_sales_order.gw_items_base_price_refunded gw_items_base_price_refunded,
       stage_hash_magento_sales_order.gw_items_price_refunded gw_items_price_refunded,
       stage_hash_magento_sales_order.gw_card_base_price_refunded gw_card_base_price_refunded,
       stage_hash_magento_sales_order.gw_card_price_refunded gw_card_price_refunded,
       stage_hash_magento_sales_order.gw_base_tax_amount_refunded gw_base_tax_amount_refunded,
       stage_hash_magento_sales_order.gw_tax_amount_refunded gw_tax_amount_refunded,
       stage_hash_magento_sales_order.gw_items_base_tax_refunded gw_items_base_tax_refunded,
       stage_hash_magento_sales_order.gw_items_tax_refunded gw_items_tax_refunded,
       stage_hash_magento_sales_order.gw_card_base_tax_refunded gw_card_base_tax_refunded,
       stage_hash_magento_sales_order.gw_card_tax_refunded gw_card_tax_refunded,
       stage_hash_magento_sales_order.reward_points_balance reward_points_balance,
       stage_hash_magento_sales_order.base_reward_currency_amount base_reward_currency_amount,
       stage_hash_magento_sales_order.reward_currency_amount reward_currency_amount,
       stage_hash_magento_sales_order.base_rwrd_crrncy_amt_invoiced base_rwrd_crrncy_amount_invoiced,
       stage_hash_magento_sales_order.rwrd_currency_amount_invoiced rwrd_currency_amount_invoiced,
       stage_hash_magento_sales_order.base_rwrd_crrncy_amnt_refnded base_rwrd_crrncy_amount_refnded,
       stage_hash_magento_sales_order.rwrd_crrncy_amnt_refunded rwrd_crrncy_amount_refunded,
       stage_hash_magento_sales_order.reward_points_balance_refund reward_points_balance_refund,
       stage_hash_magento_sales_order.trainer_id trainer_id,
       stage_hash_magento_sales_order.trainer_name trainer_name,
       stage_hash_magento_sales_order.mms_party_id mms_party_id,
       stage_hash_magento_sales_order.lt_bucks_redeemed lt_bucks_redeemed,
       stage_hash_magento_sales_order.lt_bucks_refunded lt_bucks_refunded,
       isnull(cast(stage_hash_magento_sales_order.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.coupon_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.protect_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.shipping_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.is_virtual as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_discount_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_discount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_discount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_grand_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_subtotal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_subtotal_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_subtotal_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_subtotal_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_tax_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_tax_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_to_global_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_to_order_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_invoiced_cost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_offline_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_online_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_paid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_qty_ordered as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.discount_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.discount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.discount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.grand_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.store_to_base_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.store_to_order_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.subtotal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.subtotal_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.subtotal_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.subtotal_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.tax_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.tax_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_canceled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_offline_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_online_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_paid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_qty_ordered as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.can_ship_partially as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.can_ship_partially_item as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_is_guest as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_note_notify as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.edit_increment as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.email_sent as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.send_email as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.forced_shipment_with_invoice as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.payment_auth_expiration as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.adjustment_negative as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.adjustment_positive as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_adjustment_negative as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_adjustment_positive as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_subtotal_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_total_due as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.payment_authorization_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.subtotal_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_due as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.weight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_order.customer_dob,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.applied_rule_ids,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.base_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.customer_email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.customer_firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.customer_lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.customer_middlename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.customer_prefix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.customer_suffix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.customer_taxvat,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.discount_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.global_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.hold_before_state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.hold_before_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.order_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.remote_ip,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.shipping_method,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.store_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.store_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.x_forwarded_for,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.customer_note,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_order.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_order.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.total_item_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_gender as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_discount_tax_compensation_amnt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.discount_tax_compensation_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_discount_tax_compensation_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.discount_tax_compensation_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_discount_tax_compensation_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.shipping_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_shipping_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.coupon_rule_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.paypal_ipn_customer_notified as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_customer_balance_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_balance_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_customer_balance_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_balance_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_customer_balance_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_balance_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.bs_customer_bal_total_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.customer_bal_total_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.gift_cards,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_gift_cards_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gift_cards_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_gift_cards_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gift_cards_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_gift_cards_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gift_cards_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_allow_gift_receipt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_add_card as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_base_price_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_price_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_base_price_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_price_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_base_price_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_price_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_base_price_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_price_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_base_price_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_price_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_base_price_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_price_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_base_tax_amount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_tax_amount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_base_tax_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_tax_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_base_tax_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_tax_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_base_price_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_price_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_base_price_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_price_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_base_price_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_price_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_base_tax_amount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_tax_amount_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_base_tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_items_tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_base_tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.gw_card_tax_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.reward_points_balance as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_reward_currency_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.reward_currency_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_rwrd_crrncy_amt_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.rwrd_currency_amount_invoiced as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.base_rwrd_crrncy_amnt_refnded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.rwrd_crrncy_amnt_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.reward_points_balance_refund as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.trainer_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.trainer_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_order.mms_party_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.lt_bucks_redeemed as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_order.lt_bucks_refunded as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_order
 where stage_hash_magento_sales_order.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_order records
set @insert_date_time = getdate()
insert into s_magento_sales_order (
       bk_hash,
       entity_id,
       state,
       status,
       coupon_code,
       protect_code,
       shipping_description,
       is_virtual,
       base_discount_amount,
       base_discount_canceled,
       base_discount_invoiced,
       base_discount_refunded,
       base_grand_total,
       base_shipping_amount,
       base_shipping_canceled,
       base_shipping_invoiced,
       base_shipping_refunded,
       base_shipping_tax_amount,
       base_shipping_tax_refunded,
       base_subtotal,
       base_subtotal_canceled,
       base_subtotal_invoiced,
       base_subtotal_refunded,
       base_tax_amount,
       base_tax_canceled,
       base_tax_invoiced,
       base_tax_refunded,
       base_to_global_rate,
       base_to_order_rate,
       base_total_canceled,
       base_total_invoiced,
       base_total_invoiced_cost,
       base_total_offline_refunded,
       base_total_online_refunded,
       base_total_paid,
       base_total_qty_ordered,
       base_total_refunded,
       discount_amount,
       discount_canceled,
       discount_invoiced,
       discount_refunded,
       grand_total,
       shipping_amount,
       shipping_canceled,
       shipping_invoiced,
       shipping_refunded,
       shipping_tax_amount,
       shipping_tax_refunded,
       store_to_base_rate,
       store_to_order_rate,
       subtotal,
       subtotal_canceled,
       subtotal_invoiced,
       subtotal_refunded,
       tax_amount,
       tax_canceled,
       tax_invoiced,
       tax_refunded,
       total_canceled,
       total_invoiced,
       total_offline_refunded,
       total_online_refunded,
       total_paid,
       total_qty_ordered,
       total_refunded,
       can_ship_partially,
       can_ship_partially_item,
       customer_is_guest,
       customer_note_notify,
       edit_increment,
       email_sent,
       send_email,
       forced_shipment_with_invoice,
       payment_auth_expiration,
       adjustment_negative,
       adjustment_positive,
       base_adjustment_negative,
       base_adjustment_positive,
       base_shipping_discount_amount,
       base_subtotal_incl_tax,
       base_total_due,
       payment_authorization_amount,
       shipping_discount_amount,
       subtotal_incl_tax,
       total_due,
       weight,
       customer_dob,
       applied_rule_ids,
       base_currency_code,
       customer_email,
       customer_first_name,
       customer_last_name,
       customer_middle_name,
       customer_prefix,
       customer_suffix,
       customer_taxvat,
       discount_description,
       global_currency_code,
       hold_before_state,
       hold_before_status,
       order_currency_code,
       remote_ip,
       shipping_method,
       store_currency_code,
       store_name,
       x_forwarded_for,
       customer_note,
       created_at,
       updated_at,
       total_item_count,
       customer_gender,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       shipping_discount_tax_compensation_amount,
       base_shipping_discount_tax_compensation_amount,
       discount_tax_compensation_invoiced,
       base_discount_tax_compensation_invoiced,
       discount_tax_compensation_refunded,
       base_discount_tax_compensation_refunded,
       shipping_incl_tax,
       base_shipping_incl_tax,
       coupon_rule_name,
       paypal_ipn_customer_notified,
       base_customer_balance_amount,
       customer_balance_amount,
       base_customer_balance_invoiced,
       customer_balance_invoiced,
       base_customer_balance_refunded,
       customer_balance_refunded,
       bs_customer_bal_total_refunded,
       customer_bal_total_refunded,
       gift_cards,
       base_gift_cards_amount,
       gift_cards_amount,
       base_gift_cards_invoiced,
       gift_cards_invoiced,
       base_gift_cards_refunded,
       gift_cards_refunded,
       gw_allow_gift_receipt,
       gw_add_card,
       gw_base_price,
       gw_price,
       gw_items_base_price,
       gw_items_price,
       gw_card_base_price,
       gw_card_price,
       gw_base_tax_amount,
       gw_tax_amount,
       gw_items_base_tax_amount,
       gw_items_tax_amount,
       gw_card_base_tax_amount,
       gw_card_tax_amount,
       gw_base_price_incl_tax,
       gw_price_incl_tax,
       gw_items_base_price_incl_tax,
       gw_items_price_incl_tax,
       gw_card_base_price_incl_tax,
       gw_card_price_incl_tax,
       gw_base_price_invoiced,
       gw_price_invoiced,
       gw_items_base_price_invoiced,
       gw_items_price_invoiced,
       gw_card_base_price_invoiced,
       gw_card_price_invoiced,
       gw_base_tax_amount_invoiced,
       gw_tax_amount_invoiced,
       gw_items_base_tax_invoiced,
       gw_items_tax_invoiced,
       gw_card_base_tax_invoiced,
       gw_card_tax_invoiced,
       gw_base_price_refunded,
       gw_price_refunded,
       gw_items_base_price_refunded,
       gw_items_price_refunded,
       gw_card_base_price_refunded,
       gw_card_price_refunded,
       gw_base_tax_amount_refunded,
       gw_tax_amount_refunded,
       gw_items_base_tax_refunded,
       gw_items_tax_refunded,
       gw_card_base_tax_refunded,
       gw_card_tax_refunded,
       reward_points_balance,
       base_reward_currency_amount,
       reward_currency_amount,
       base_rwrd_crrncy_amount_invoiced,
       rwrd_currency_amount_invoiced,
       base_rwrd_crrncy_amount_refnded,
       rwrd_crrncy_amount_refunded,
       reward_points_balance_refund,
       trainer_id,
       trainer_name,
       mms_party_id,
       lt_bucks_redeemed,
       lt_bucks_refunded,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_order_inserts.bk_hash,
       #s_magento_sales_order_inserts.entity_id,
       #s_magento_sales_order_inserts.state,
       #s_magento_sales_order_inserts.status,
       #s_magento_sales_order_inserts.coupon_code,
       #s_magento_sales_order_inserts.protect_code,
       #s_magento_sales_order_inserts.shipping_description,
       #s_magento_sales_order_inserts.is_virtual,
       #s_magento_sales_order_inserts.base_discount_amount,
       #s_magento_sales_order_inserts.base_discount_canceled,
       #s_magento_sales_order_inserts.base_discount_invoiced,
       #s_magento_sales_order_inserts.base_discount_refunded,
       #s_magento_sales_order_inserts.base_grand_total,
       #s_magento_sales_order_inserts.base_shipping_amount,
       #s_magento_sales_order_inserts.base_shipping_canceled,
       #s_magento_sales_order_inserts.base_shipping_invoiced,
       #s_magento_sales_order_inserts.base_shipping_refunded,
       #s_magento_sales_order_inserts.base_shipping_tax_amount,
       #s_magento_sales_order_inserts.base_shipping_tax_refunded,
       #s_magento_sales_order_inserts.base_subtotal,
       #s_magento_sales_order_inserts.base_subtotal_canceled,
       #s_magento_sales_order_inserts.base_subtotal_invoiced,
       #s_magento_sales_order_inserts.base_subtotal_refunded,
       #s_magento_sales_order_inserts.base_tax_amount,
       #s_magento_sales_order_inserts.base_tax_canceled,
       #s_magento_sales_order_inserts.base_tax_invoiced,
       #s_magento_sales_order_inserts.base_tax_refunded,
       #s_magento_sales_order_inserts.base_to_global_rate,
       #s_magento_sales_order_inserts.base_to_order_rate,
       #s_magento_sales_order_inserts.base_total_canceled,
       #s_magento_sales_order_inserts.base_total_invoiced,
       #s_magento_sales_order_inserts.base_total_invoiced_cost,
       #s_magento_sales_order_inserts.base_total_offline_refunded,
       #s_magento_sales_order_inserts.base_total_online_refunded,
       #s_magento_sales_order_inserts.base_total_paid,
       #s_magento_sales_order_inserts.base_total_qty_ordered,
       #s_magento_sales_order_inserts.base_total_refunded,
       #s_magento_sales_order_inserts.discount_amount,
       #s_magento_sales_order_inserts.discount_canceled,
       #s_magento_sales_order_inserts.discount_invoiced,
       #s_magento_sales_order_inserts.discount_refunded,
       #s_magento_sales_order_inserts.grand_total,
       #s_magento_sales_order_inserts.shipping_amount,
       #s_magento_sales_order_inserts.shipping_canceled,
       #s_magento_sales_order_inserts.shipping_invoiced,
       #s_magento_sales_order_inserts.shipping_refunded,
       #s_magento_sales_order_inserts.shipping_tax_amount,
       #s_magento_sales_order_inserts.shipping_tax_refunded,
       #s_magento_sales_order_inserts.store_to_base_rate,
       #s_magento_sales_order_inserts.store_to_order_rate,
       #s_magento_sales_order_inserts.subtotal,
       #s_magento_sales_order_inserts.subtotal_canceled,
       #s_magento_sales_order_inserts.subtotal_invoiced,
       #s_magento_sales_order_inserts.subtotal_refunded,
       #s_magento_sales_order_inserts.tax_amount,
       #s_magento_sales_order_inserts.tax_canceled,
       #s_magento_sales_order_inserts.tax_invoiced,
       #s_magento_sales_order_inserts.tax_refunded,
       #s_magento_sales_order_inserts.total_canceled,
       #s_magento_sales_order_inserts.total_invoiced,
       #s_magento_sales_order_inserts.total_offline_refunded,
       #s_magento_sales_order_inserts.total_online_refunded,
       #s_magento_sales_order_inserts.total_paid,
       #s_magento_sales_order_inserts.total_qty_ordered,
       #s_magento_sales_order_inserts.total_refunded,
       #s_magento_sales_order_inserts.can_ship_partially,
       #s_magento_sales_order_inserts.can_ship_partially_item,
       #s_magento_sales_order_inserts.customer_is_guest,
       #s_magento_sales_order_inserts.customer_note_notify,
       #s_magento_sales_order_inserts.edit_increment,
       #s_magento_sales_order_inserts.email_sent,
       #s_magento_sales_order_inserts.send_email,
       #s_magento_sales_order_inserts.forced_shipment_with_invoice,
       #s_magento_sales_order_inserts.payment_auth_expiration,
       #s_magento_sales_order_inserts.adjustment_negative,
       #s_magento_sales_order_inserts.adjustment_positive,
       #s_magento_sales_order_inserts.base_adjustment_negative,
       #s_magento_sales_order_inserts.base_adjustment_positive,
       #s_magento_sales_order_inserts.base_shipping_discount_amount,
       #s_magento_sales_order_inserts.base_subtotal_incl_tax,
       #s_magento_sales_order_inserts.base_total_due,
       #s_magento_sales_order_inserts.payment_authorization_amount,
       #s_magento_sales_order_inserts.shipping_discount_amount,
       #s_magento_sales_order_inserts.subtotal_incl_tax,
       #s_magento_sales_order_inserts.total_due,
       #s_magento_sales_order_inserts.weight,
       #s_magento_sales_order_inserts.customer_dob,
       #s_magento_sales_order_inserts.applied_rule_ids,
       #s_magento_sales_order_inserts.base_currency_code,
       #s_magento_sales_order_inserts.customer_email,
       #s_magento_sales_order_inserts.customer_first_name,
       #s_magento_sales_order_inserts.customer_last_name,
       #s_magento_sales_order_inserts.customer_middle_name,
       #s_magento_sales_order_inserts.customer_prefix,
       #s_magento_sales_order_inserts.customer_suffix,
       #s_magento_sales_order_inserts.customer_taxvat,
       #s_magento_sales_order_inserts.discount_description,
       #s_magento_sales_order_inserts.global_currency_code,
       #s_magento_sales_order_inserts.hold_before_state,
       #s_magento_sales_order_inserts.hold_before_status,
       #s_magento_sales_order_inserts.order_currency_code,
       #s_magento_sales_order_inserts.remote_ip,
       #s_magento_sales_order_inserts.shipping_method,
       #s_magento_sales_order_inserts.store_currency_code,
       #s_magento_sales_order_inserts.store_name,
       #s_magento_sales_order_inserts.x_forwarded_for,
       #s_magento_sales_order_inserts.customer_note,
       #s_magento_sales_order_inserts.created_at,
       #s_magento_sales_order_inserts.updated_at,
       #s_magento_sales_order_inserts.total_item_count,
       #s_magento_sales_order_inserts.customer_gender,
       #s_magento_sales_order_inserts.discount_tax_compensation_amount,
       #s_magento_sales_order_inserts.base_discount_tax_compensation_amount,
       #s_magento_sales_order_inserts.shipping_discount_tax_compensation_amount,
       #s_magento_sales_order_inserts.base_shipping_discount_tax_compensation_amount,
       #s_magento_sales_order_inserts.discount_tax_compensation_invoiced,
       #s_magento_sales_order_inserts.base_discount_tax_compensation_invoiced,
       #s_magento_sales_order_inserts.discount_tax_compensation_refunded,
       #s_magento_sales_order_inserts.base_discount_tax_compensation_refunded,
       #s_magento_sales_order_inserts.shipping_incl_tax,
       #s_magento_sales_order_inserts.base_shipping_incl_tax,
       #s_magento_sales_order_inserts.coupon_rule_name,
       #s_magento_sales_order_inserts.paypal_ipn_customer_notified,
       #s_magento_sales_order_inserts.base_customer_balance_amount,
       #s_magento_sales_order_inserts.customer_balance_amount,
       #s_magento_sales_order_inserts.base_customer_balance_invoiced,
       #s_magento_sales_order_inserts.customer_balance_invoiced,
       #s_magento_sales_order_inserts.base_customer_balance_refunded,
       #s_magento_sales_order_inserts.customer_balance_refunded,
       #s_magento_sales_order_inserts.bs_customer_bal_total_refunded,
       #s_magento_sales_order_inserts.customer_bal_total_refunded,
       #s_magento_sales_order_inserts.gift_cards,
       #s_magento_sales_order_inserts.base_gift_cards_amount,
       #s_magento_sales_order_inserts.gift_cards_amount,
       #s_magento_sales_order_inserts.base_gift_cards_invoiced,
       #s_magento_sales_order_inserts.gift_cards_invoiced,
       #s_magento_sales_order_inserts.base_gift_cards_refunded,
       #s_magento_sales_order_inserts.gift_cards_refunded,
       #s_magento_sales_order_inserts.gw_allow_gift_receipt,
       #s_magento_sales_order_inserts.gw_add_card,
       #s_magento_sales_order_inserts.gw_base_price,
       #s_magento_sales_order_inserts.gw_price,
       #s_magento_sales_order_inserts.gw_items_base_price,
       #s_magento_sales_order_inserts.gw_items_price,
       #s_magento_sales_order_inserts.gw_card_base_price,
       #s_magento_sales_order_inserts.gw_card_price,
       #s_magento_sales_order_inserts.gw_base_tax_amount,
       #s_magento_sales_order_inserts.gw_tax_amount,
       #s_magento_sales_order_inserts.gw_items_base_tax_amount,
       #s_magento_sales_order_inserts.gw_items_tax_amount,
       #s_magento_sales_order_inserts.gw_card_base_tax_amount,
       #s_magento_sales_order_inserts.gw_card_tax_amount,
       #s_magento_sales_order_inserts.gw_base_price_incl_tax,
       #s_magento_sales_order_inserts.gw_price_incl_tax,
       #s_magento_sales_order_inserts.gw_items_base_price_incl_tax,
       #s_magento_sales_order_inserts.gw_items_price_incl_tax,
       #s_magento_sales_order_inserts.gw_card_base_price_incl_tax,
       #s_magento_sales_order_inserts.gw_card_price_incl_tax,
       #s_magento_sales_order_inserts.gw_base_price_invoiced,
       #s_magento_sales_order_inserts.gw_price_invoiced,
       #s_magento_sales_order_inserts.gw_items_base_price_invoiced,
       #s_magento_sales_order_inserts.gw_items_price_invoiced,
       #s_magento_sales_order_inserts.gw_card_base_price_invoiced,
       #s_magento_sales_order_inserts.gw_card_price_invoiced,
       #s_magento_sales_order_inserts.gw_base_tax_amount_invoiced,
       #s_magento_sales_order_inserts.gw_tax_amount_invoiced,
       #s_magento_sales_order_inserts.gw_items_base_tax_invoiced,
       #s_magento_sales_order_inserts.gw_items_tax_invoiced,
       #s_magento_sales_order_inserts.gw_card_base_tax_invoiced,
       #s_magento_sales_order_inserts.gw_card_tax_invoiced,
       #s_magento_sales_order_inserts.gw_base_price_refunded,
       #s_magento_sales_order_inserts.gw_price_refunded,
       #s_magento_sales_order_inserts.gw_items_base_price_refunded,
       #s_magento_sales_order_inserts.gw_items_price_refunded,
       #s_magento_sales_order_inserts.gw_card_base_price_refunded,
       #s_magento_sales_order_inserts.gw_card_price_refunded,
       #s_magento_sales_order_inserts.gw_base_tax_amount_refunded,
       #s_magento_sales_order_inserts.gw_tax_amount_refunded,
       #s_magento_sales_order_inserts.gw_items_base_tax_refunded,
       #s_magento_sales_order_inserts.gw_items_tax_refunded,
       #s_magento_sales_order_inserts.gw_card_base_tax_refunded,
       #s_magento_sales_order_inserts.gw_card_tax_refunded,
       #s_magento_sales_order_inserts.reward_points_balance,
       #s_magento_sales_order_inserts.base_reward_currency_amount,
       #s_magento_sales_order_inserts.reward_currency_amount,
       #s_magento_sales_order_inserts.base_rwrd_crrncy_amount_invoiced,
       #s_magento_sales_order_inserts.rwrd_currency_amount_invoiced,
       #s_magento_sales_order_inserts.base_rwrd_crrncy_amount_refnded,
       #s_magento_sales_order_inserts.rwrd_crrncy_amount_refunded,
       #s_magento_sales_order_inserts.reward_points_balance_refund,
       #s_magento_sales_order_inserts.trainer_id,
       #s_magento_sales_order_inserts.trainer_name,
       #s_magento_sales_order_inserts.mms_party_id,
       #s_magento_sales_order_inserts.lt_bucks_redeemed,
       #s_magento_sales_order_inserts.lt_bucks_refunded,
       case when s_magento_sales_order.s_magento_sales_order_id is null then isnull(#s_magento_sales_order_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_order_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_order_inserts
  left join p_magento_sales_order
    on #s_magento_sales_order_inserts.bk_hash = p_magento_sales_order.bk_hash
   and p_magento_sales_order.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_order
    on p_magento_sales_order.bk_hash = s_magento_sales_order.bk_hash
   and p_magento_sales_order.s_magento_sales_order_id = s_magento_sales_order.s_magento_sales_order_id
 where s_magento_sales_order.s_magento_sales_order_id is null
    or (s_magento_sales_order.s_magento_sales_order_id is not null
        and s_magento_sales_order.dv_hash <> #s_magento_sales_order_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_order @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_order @current_dv_batch_id

end
