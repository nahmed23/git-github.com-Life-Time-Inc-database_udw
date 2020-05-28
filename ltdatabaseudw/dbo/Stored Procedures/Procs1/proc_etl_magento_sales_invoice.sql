CREATE PROC [dbo].[proc_etl_magento_sales_invoice] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_invoice

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_invoice (
       bk_hash,
       entity_id,
       store_id,
       base_grand_total,
       shipping_tax_amount,
       tax_amount,
       base_tax_amount,
       store_to_order_rate,
       base_shipping_tax_amount,
       base_discount_amount,
       base_to_order_rate,
       grand_total,
       shipping_amount,
       subtotal_incl_tax,
       base_subtotal_incl_tax,
       store_to_base_rate,
       base_shipping_amount,
       total_qty,
       base_to_global_rate,
       subtotal,
       base_subtotal,
       discount_amount,
       billing_address_id,
       is_used_for_refund,
       order_id,
       email_sent,
       send_email,
       can_void_flag,
       state,
       shipping_address_id,
       store_currency_code,
       transaction_id,
       order_currency_code,
       base_currency_code,
       global_currency_code,
       increment_id,
       created_at,
       updated_at,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       shipping_discount_tax_compensation_amount,
       base_shipping_discount_tax_compensation_amnt,
       shipping_incl_tax,
       base_shipping_incl_tax,
       base_total_refunded,
       discount_description,
       customer_note,
       customer_note_notify,
       base_customer_balance_amount,
       customer_balance_amount,
       base_gift_cards_amount,
       gift_cards_amount,
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
       base_reward_currency_amount,
       reward_currency_amount,
       reward_points_balance,
       m1_invoice_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       store_id,
       base_grand_total,
       shipping_tax_amount,
       tax_amount,
       base_tax_amount,
       store_to_order_rate,
       base_shipping_tax_amount,
       base_discount_amount,
       base_to_order_rate,
       grand_total,
       shipping_amount,
       subtotal_incl_tax,
       base_subtotal_incl_tax,
       store_to_base_rate,
       base_shipping_amount,
       total_qty,
       base_to_global_rate,
       subtotal,
       base_subtotal,
       discount_amount,
       billing_address_id,
       is_used_for_refund,
       order_id,
       email_sent,
       send_email,
       can_void_flag,
       state,
       shipping_address_id,
       store_currency_code,
       transaction_id,
       order_currency_code,
       base_currency_code,
       global_currency_code,
       increment_id,
       created_at,
       updated_at,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       shipping_discount_tax_compensation_amount,
       base_shipping_discount_tax_compensation_amnt,
       shipping_incl_tax,
       base_shipping_incl_tax,
       base_total_refunded,
       discount_description,
       customer_note,
       customer_note_notify,
       base_customer_balance_amount,
       customer_balance_amount,
       base_gift_cards_amount,
       gift_cards_amount,
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
       base_reward_currency_amount,
       reward_currency_amount,
       reward_points_balance,
       m1_invoice_id,
       isnull(cast(stage_magento_sales_invoice.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_invoice
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_invoice @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_invoice (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_sales_invoice.bk_hash,
       stage_hash_magento_sales_invoice.entity_id entity_id,
       isnull(cast(stage_hash_magento_sales_invoice.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_invoice
  left join h_magento_sales_invoice
    on stage_hash_magento_sales_invoice.bk_hash = h_magento_sales_invoice.bk_hash
 where h_magento_sales_invoice_id is null
   and stage_hash_magento_sales_invoice.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_invoice
if object_id('tempdb..#l_magento_sales_invoice_inserts') is not null drop table #l_magento_sales_invoice_inserts
create table #l_magento_sales_invoice_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_invoice.bk_hash,
       stage_hash_magento_sales_invoice.entity_id entity_id,
       stage_hash_magento_sales_invoice.store_id store_id,
       stage_hash_magento_sales_invoice.billing_address_id billing_address_id,
       stage_hash_magento_sales_invoice.order_id order_id,
       stage_hash_magento_sales_invoice.shipping_address_id shipping_address_id,
       stage_hash_magento_sales_invoice.increment_id increment_id,
       stage_hash_magento_sales_invoice.m1_invoice_id m1_invoice_id,
       isnull(cast(stage_hash_magento_sales_invoice.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.store_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.billing_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.order_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.shipping_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_invoice.increment_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.m1_invoice_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_invoice
 where stage_hash_magento_sales_invoice.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_invoice records
set @insert_date_time = getdate()
insert into l_magento_sales_invoice (
       bk_hash,
       entity_id,
       store_id,
       billing_address_id,
       order_id,
       shipping_address_id,
       increment_id,
       m1_invoice_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_invoice_inserts.bk_hash,
       #l_magento_sales_invoice_inserts.entity_id,
       #l_magento_sales_invoice_inserts.store_id,
       #l_magento_sales_invoice_inserts.billing_address_id,
       #l_magento_sales_invoice_inserts.order_id,
       #l_magento_sales_invoice_inserts.shipping_address_id,
       #l_magento_sales_invoice_inserts.increment_id,
       #l_magento_sales_invoice_inserts.m1_invoice_id,
       case when l_magento_sales_invoice.l_magento_sales_invoice_id is null then isnull(#l_magento_sales_invoice_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_invoice_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_invoice_inserts
  left join p_magento_sales_invoice
    on #l_magento_sales_invoice_inserts.bk_hash = p_magento_sales_invoice.bk_hash
   and p_magento_sales_invoice.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_invoice
    on p_magento_sales_invoice.bk_hash = l_magento_sales_invoice.bk_hash
   and p_magento_sales_invoice.l_magento_sales_invoice_id = l_magento_sales_invoice.l_magento_sales_invoice_id
 where l_magento_sales_invoice.l_magento_sales_invoice_id is null
    or (l_magento_sales_invoice.l_magento_sales_invoice_id is not null
        and l_magento_sales_invoice.dv_hash <> #l_magento_sales_invoice_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_invoice
if object_id('tempdb..#s_magento_sales_invoice_inserts') is not null drop table #s_magento_sales_invoice_inserts
create table #s_magento_sales_invoice_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_invoice.bk_hash,
       stage_hash_magento_sales_invoice.entity_id entity_id,
       stage_hash_magento_sales_invoice.base_grand_total base_grand_total,
       stage_hash_magento_sales_invoice.shipping_tax_amount shipping_tax_amount,
       stage_hash_magento_sales_invoice.tax_amount tax_amount,
       stage_hash_magento_sales_invoice.base_tax_amount base_tax_amount,
       stage_hash_magento_sales_invoice.store_to_order_rate store_to_order_rate,
       stage_hash_magento_sales_invoice.base_shipping_tax_amount base_shipping_tax_amount,
       stage_hash_magento_sales_invoice.base_discount_amount base_discount_amount,
       stage_hash_magento_sales_invoice.base_to_order_rate base_to_order_rate,
       stage_hash_magento_sales_invoice.grand_total grand_total,
       stage_hash_magento_sales_invoice.shipping_amount shipping_amount,
       stage_hash_magento_sales_invoice.subtotal_incl_tax subtotal_incl_tax,
       stage_hash_magento_sales_invoice.base_subtotal_incl_tax base_subtotal_incl_tax,
       stage_hash_magento_sales_invoice.store_to_base_rate store_to_base_rate,
       stage_hash_magento_sales_invoice.base_shipping_amount base_shipping_amount,
       stage_hash_magento_sales_invoice.total_qty total_qty,
       stage_hash_magento_sales_invoice.base_to_global_rate base_to_global_rate,
       stage_hash_magento_sales_invoice.subtotal subtotal,
       stage_hash_magento_sales_invoice.base_subtotal base_subtotal,
       stage_hash_magento_sales_invoice.discount_amount discount_amount,
       stage_hash_magento_sales_invoice.is_used_for_refund is_used_for_refund,
       stage_hash_magento_sales_invoice.email_sent email_sent,
       stage_hash_magento_sales_invoice.send_email send_email,
       stage_hash_magento_sales_invoice.can_void_flag can_void_flag,
       stage_hash_magento_sales_invoice.state state,
       stage_hash_magento_sales_invoice.store_currency_code store_currency_code,
       stage_hash_magento_sales_invoice.transaction_id transaction_id,
       stage_hash_magento_sales_invoice.order_currency_code order_currency_code,
       stage_hash_magento_sales_invoice.base_currency_code base_currency_code,
       stage_hash_magento_sales_invoice.global_currency_code global_currency_code,
       stage_hash_magento_sales_invoice.created_at created_at,
       stage_hash_magento_sales_invoice.updated_at updated_at,
       stage_hash_magento_sales_invoice.discount_tax_compensation_amount discount_tax_compensation_amount,
       stage_hash_magento_sales_invoice.base_discount_tax_compensation_amount base_discount_tax_compensation_amount,
       stage_hash_magento_sales_invoice.shipping_discount_tax_compensation_amount shipping_discount_tax_compensation_amount,
       stage_hash_magento_sales_invoice.base_shipping_discount_tax_compensation_amnt base_shipping_discount_tax_compensation_amnt,
       stage_hash_magento_sales_invoice.shipping_incl_tax shipping_incl_tax,
       stage_hash_magento_sales_invoice.base_shipping_incl_tax base_shipping_incl_tax,
       stage_hash_magento_sales_invoice.base_total_refunded base_total_refunded,
       stage_hash_magento_sales_invoice.discount_description discount_description,
       stage_hash_magento_sales_invoice.customer_note customer_note,
       stage_hash_magento_sales_invoice.customer_note_notify customer_note_notify,
       stage_hash_magento_sales_invoice.base_customer_balance_amount base_customer_balance_amount,
       stage_hash_magento_sales_invoice.customer_balance_amount customer_balance_amount,
       stage_hash_magento_sales_invoice.base_gift_cards_amount base_gift_cards_amount,
       stage_hash_magento_sales_invoice.gift_cards_amount gift_cards_amount,
       stage_hash_magento_sales_invoice.gw_base_price gw_base_price,
       stage_hash_magento_sales_invoice.gw_price gw_price,
       stage_hash_magento_sales_invoice.gw_items_base_price gw_items_base_price,
       stage_hash_magento_sales_invoice.gw_items_price gw_items_price,
       stage_hash_magento_sales_invoice.gw_card_base_price gw_card_base_price,
       stage_hash_magento_sales_invoice.gw_card_price gw_card_price,
       stage_hash_magento_sales_invoice.gw_base_tax_amount gw_base_tax_amount,
       stage_hash_magento_sales_invoice.gw_tax_amount gw_tax_amount,
       stage_hash_magento_sales_invoice.gw_items_base_tax_amount gw_items_base_tax_amount,
       stage_hash_magento_sales_invoice.gw_items_tax_amount gw_items_tax_amount,
       stage_hash_magento_sales_invoice.gw_card_base_tax_amount gw_card_base_tax_amount,
       stage_hash_magento_sales_invoice.gw_card_tax_amount gw_card_tax_amount,
       stage_hash_magento_sales_invoice.base_reward_currency_amount base_reward_currency_amount,
       stage_hash_magento_sales_invoice.reward_currency_amount reward_currency_amount,
       stage_hash_magento_sales_invoice.reward_points_balance reward_points_balance,
       isnull(cast(stage_hash_magento_sales_invoice.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_grand_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.shipping_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.store_to_order_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_shipping_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_to_order_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.grand_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.shipping_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.subtotal_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_subtotal_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.store_to_base_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_shipping_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.total_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_to_global_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.subtotal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_subtotal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.is_used_for_refund as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.email_sent as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.send_email as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.can_void_flag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.state as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_invoice.store_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_invoice.transaction_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_invoice.order_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_invoice.base_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_invoice.global_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_invoice.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_invoice.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.shipping_discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_shipping_discount_tax_compensation_amnt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.shipping_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_shipping_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_total_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_invoice.discount_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_invoice.customer_note,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.customer_note_notify as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_customer_balance_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.customer_balance_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_gift_cards_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gift_cards_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_items_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_items_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_card_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_card_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_items_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_items_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_card_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.gw_card_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.base_reward_currency_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.reward_currency_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_invoice.reward_points_balance as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_invoice
 where stage_hash_magento_sales_invoice.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_invoice records
set @insert_date_time = getdate()
insert into s_magento_sales_invoice (
       bk_hash,
       entity_id,
       base_grand_total,
       shipping_tax_amount,
       tax_amount,
       base_tax_amount,
       store_to_order_rate,
       base_shipping_tax_amount,
       base_discount_amount,
       base_to_order_rate,
       grand_total,
       shipping_amount,
       subtotal_incl_tax,
       base_subtotal_incl_tax,
       store_to_base_rate,
       base_shipping_amount,
       total_qty,
       base_to_global_rate,
       subtotal,
       base_subtotal,
       discount_amount,
       is_used_for_refund,
       email_sent,
       send_email,
       can_void_flag,
       state,
       store_currency_code,
       transaction_id,
       order_currency_code,
       base_currency_code,
       global_currency_code,
       created_at,
       updated_at,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       shipping_discount_tax_compensation_amount,
       base_shipping_discount_tax_compensation_amnt,
       shipping_incl_tax,
       base_shipping_incl_tax,
       base_total_refunded,
       discount_description,
       customer_note,
       customer_note_notify,
       base_customer_balance_amount,
       customer_balance_amount,
       base_gift_cards_amount,
       gift_cards_amount,
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
       base_reward_currency_amount,
       reward_currency_amount,
       reward_points_balance,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_invoice_inserts.bk_hash,
       #s_magento_sales_invoice_inserts.entity_id,
       #s_magento_sales_invoice_inserts.base_grand_total,
       #s_magento_sales_invoice_inserts.shipping_tax_amount,
       #s_magento_sales_invoice_inserts.tax_amount,
       #s_magento_sales_invoice_inserts.base_tax_amount,
       #s_magento_sales_invoice_inserts.store_to_order_rate,
       #s_magento_sales_invoice_inserts.base_shipping_tax_amount,
       #s_magento_sales_invoice_inserts.base_discount_amount,
       #s_magento_sales_invoice_inserts.base_to_order_rate,
       #s_magento_sales_invoice_inserts.grand_total,
       #s_magento_sales_invoice_inserts.shipping_amount,
       #s_magento_sales_invoice_inserts.subtotal_incl_tax,
       #s_magento_sales_invoice_inserts.base_subtotal_incl_tax,
       #s_magento_sales_invoice_inserts.store_to_base_rate,
       #s_magento_sales_invoice_inserts.base_shipping_amount,
       #s_magento_sales_invoice_inserts.total_qty,
       #s_magento_sales_invoice_inserts.base_to_global_rate,
       #s_magento_sales_invoice_inserts.subtotal,
       #s_magento_sales_invoice_inserts.base_subtotal,
       #s_magento_sales_invoice_inserts.discount_amount,
       #s_magento_sales_invoice_inserts.is_used_for_refund,
       #s_magento_sales_invoice_inserts.email_sent,
       #s_magento_sales_invoice_inserts.send_email,
       #s_magento_sales_invoice_inserts.can_void_flag,
       #s_magento_sales_invoice_inserts.state,
       #s_magento_sales_invoice_inserts.store_currency_code,
       #s_magento_sales_invoice_inserts.transaction_id,
       #s_magento_sales_invoice_inserts.order_currency_code,
       #s_magento_sales_invoice_inserts.base_currency_code,
       #s_magento_sales_invoice_inserts.global_currency_code,
       #s_magento_sales_invoice_inserts.created_at,
       #s_magento_sales_invoice_inserts.updated_at,
       #s_magento_sales_invoice_inserts.discount_tax_compensation_amount,
       #s_magento_sales_invoice_inserts.base_discount_tax_compensation_amount,
       #s_magento_sales_invoice_inserts.shipping_discount_tax_compensation_amount,
       #s_magento_sales_invoice_inserts.base_shipping_discount_tax_compensation_amnt,
       #s_magento_sales_invoice_inserts.shipping_incl_tax,
       #s_magento_sales_invoice_inserts.base_shipping_incl_tax,
       #s_magento_sales_invoice_inserts.base_total_refunded,
       #s_magento_sales_invoice_inserts.discount_description,
       #s_magento_sales_invoice_inserts.customer_note,
       #s_magento_sales_invoice_inserts.customer_note_notify,
       #s_magento_sales_invoice_inserts.base_customer_balance_amount,
       #s_magento_sales_invoice_inserts.customer_balance_amount,
       #s_magento_sales_invoice_inserts.base_gift_cards_amount,
       #s_magento_sales_invoice_inserts.gift_cards_amount,
       #s_magento_sales_invoice_inserts.gw_base_price,
       #s_magento_sales_invoice_inserts.gw_price,
       #s_magento_sales_invoice_inserts.gw_items_base_price,
       #s_magento_sales_invoice_inserts.gw_items_price,
       #s_magento_sales_invoice_inserts.gw_card_base_price,
       #s_magento_sales_invoice_inserts.gw_card_price,
       #s_magento_sales_invoice_inserts.gw_base_tax_amount,
       #s_magento_sales_invoice_inserts.gw_tax_amount,
       #s_magento_sales_invoice_inserts.gw_items_base_tax_amount,
       #s_magento_sales_invoice_inserts.gw_items_tax_amount,
       #s_magento_sales_invoice_inserts.gw_card_base_tax_amount,
       #s_magento_sales_invoice_inserts.gw_card_tax_amount,
       #s_magento_sales_invoice_inserts.base_reward_currency_amount,
       #s_magento_sales_invoice_inserts.reward_currency_amount,
       #s_magento_sales_invoice_inserts.reward_points_balance,
       case when s_magento_sales_invoice.s_magento_sales_invoice_id is null then isnull(#s_magento_sales_invoice_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_invoice_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_invoice_inserts
  left join p_magento_sales_invoice
    on #s_magento_sales_invoice_inserts.bk_hash = p_magento_sales_invoice.bk_hash
   and p_magento_sales_invoice.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_invoice
    on p_magento_sales_invoice.bk_hash = s_magento_sales_invoice.bk_hash
   and p_magento_sales_invoice.s_magento_sales_invoice_id = s_magento_sales_invoice.s_magento_sales_invoice_id
 where s_magento_sales_invoice.s_magento_sales_invoice_id is null
    or (s_magento_sales_invoice.s_magento_sales_invoice_id is not null
        and s_magento_sales_invoice.dv_hash <> #s_magento_sales_invoice_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_invoice @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_invoice @current_dv_batch_id

end
