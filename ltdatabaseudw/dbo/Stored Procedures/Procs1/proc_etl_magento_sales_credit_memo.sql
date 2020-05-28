CREATE PROC [dbo].[proc_etl_magento_sales_credit_memo] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_creditmemo

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_creditmemo (
       bk_hash,
       entity_id,
       store_id,
       adjustment_positive,
       base_shipping_tax_amount,
       store_to_order_rate,
       base_discount_amount,
       base_to_order_rate,
       grand_total,
       base_adjustment_negative,
       base_subtotal_incl_tax,
       shipping_amount,
       subtotal_incl_tax,
       adjustment_negative,
       base_shipping_amount,
       store_to_base_rate,
       base_to_global_rate,
       base_adjustment,
       base_subtotal,
       discount_amount,
       subtotal,
       adjustment,
       base_grand_total,
       base_adjustment_positive,
       base_tax_amount,
       shipping_tax_amount,
       tax_amount,
       order_id,
       email_sent,
       send_email,
       creditmemo_status,
       state,
       shipping_address_id,
       billing_address_id,
       invoice_id,
       store_currency_code,
       order_currency_code,
       base_currency_code,
       global_currency_code,
       transaction_id,
       increment_id,
       created_at,
       updated_at,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       shipping_discount_tax_compensation_amount,
       base_shipping_discount_tax_compensation_amnt,
       shipping_incl_tax,
       base_shipping_incl_tax,
       discount_description,
       customer_note,
       customer_note_notify,
       base_customer_balance_amount,
       customer_balance_amount,
       bs_customer_bal_total_refunded,
       customer_bal_total_refunded,
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
       reward_points_balance_refund,
       m1_creditmemo_id,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       store_id,
       adjustment_positive,
       base_shipping_tax_amount,
       store_to_order_rate,
       base_discount_amount,
       base_to_order_rate,
       grand_total,
       base_adjustment_negative,
       base_subtotal_incl_tax,
       shipping_amount,
       subtotal_incl_tax,
       adjustment_negative,
       base_shipping_amount,
       store_to_base_rate,
       base_to_global_rate,
       base_adjustment,
       base_subtotal,
       discount_amount,
       subtotal,
       adjustment,
       base_grand_total,
       base_adjustment_positive,
       base_tax_amount,
       shipping_tax_amount,
       tax_amount,
       order_id,
       email_sent,
       send_email,
       creditmemo_status,
       state,
       shipping_address_id,
       billing_address_id,
       invoice_id,
       store_currency_code,
       order_currency_code,
       base_currency_code,
       global_currency_code,
       transaction_id,
       increment_id,
       created_at,
       updated_at,
       discount_tax_compensation_amount,
       base_discount_tax_compensation_amount,
       shipping_discount_tax_compensation_amount,
       base_shipping_discount_tax_compensation_amnt,
       shipping_incl_tax,
       base_shipping_incl_tax,
       discount_description,
       customer_note,
       customer_note_notify,
       base_customer_balance_amount,
       customer_balance_amount,
       bs_customer_bal_total_refunded,
       customer_bal_total_refunded,
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
       reward_points_balance_refund,
       m1_creditmemo_id,
       isnull(cast(stage_magento_sales_creditmemo.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_creditmemo
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_credit_memo @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_credit_memo (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_magento_sales_creditmemo.bk_hash,
       stage_hash_magento_sales_creditmemo.entity_id entity_id,
       isnull(cast(stage_hash_magento_sales_creditmemo.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_creditmemo
  left join h_magento_sales_credit_memo
    on stage_hash_magento_sales_creditmemo.bk_hash = h_magento_sales_credit_memo.bk_hash
 where h_magento_sales_credit_memo_id is null
   and stage_hash_magento_sales_creditmemo.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_credit_memo
if object_id('tempdb..#l_magento_sales_credit_memo_inserts') is not null drop table #l_magento_sales_credit_memo_inserts
create table #l_magento_sales_credit_memo_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_creditmemo.bk_hash,
       stage_hash_magento_sales_creditmemo.entity_id entity_id,
       stage_hash_magento_sales_creditmemo.store_id store_id,
       stage_hash_magento_sales_creditmemo.order_id order_id,
       stage_hash_magento_sales_creditmemo.shipping_address_id shipping_address_id,
       stage_hash_magento_sales_creditmemo.billing_address_id billing_address_id,
       stage_hash_magento_sales_creditmemo.invoice_id invoice_id,
       stage_hash_magento_sales_creditmemo.transaction_id transaction_id,
       stage_hash_magento_sales_creditmemo.increment_id increment_id,
       stage_hash_magento_sales_creditmemo.m1_creditmemo_id m1_credit_memo_id,
       isnull(cast(stage_hash_magento_sales_creditmemo.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.store_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.order_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.shipping_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.billing_address_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.invoice_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_creditmemo.transaction_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_creditmemo.increment_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.m1_creditmemo_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_creditmemo
 where stage_hash_magento_sales_creditmemo.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_credit_memo records
set @insert_date_time = getdate()
insert into l_magento_sales_credit_memo (
       bk_hash,
       entity_id,
       store_id,
       order_id,
       shipping_address_id,
       billing_address_id,
       invoice_id,
       transaction_id,
       increment_id,
       m1_credit_memo_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_credit_memo_inserts.bk_hash,
       #l_magento_sales_credit_memo_inserts.entity_id,
       #l_magento_sales_credit_memo_inserts.store_id,
       #l_magento_sales_credit_memo_inserts.order_id,
       #l_magento_sales_credit_memo_inserts.shipping_address_id,
       #l_magento_sales_credit_memo_inserts.billing_address_id,
       #l_magento_sales_credit_memo_inserts.invoice_id,
       #l_magento_sales_credit_memo_inserts.transaction_id,
       #l_magento_sales_credit_memo_inserts.increment_id,
       #l_magento_sales_credit_memo_inserts.m1_credit_memo_id,
       case when l_magento_sales_credit_memo.l_magento_sales_credit_memo_id is null then isnull(#l_magento_sales_credit_memo_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_credit_memo_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_credit_memo_inserts
  left join p_magento_sales_credit_memo
    on #l_magento_sales_credit_memo_inserts.bk_hash = p_magento_sales_credit_memo.bk_hash
   and p_magento_sales_credit_memo.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_credit_memo
    on p_magento_sales_credit_memo.bk_hash = l_magento_sales_credit_memo.bk_hash
   and p_magento_sales_credit_memo.l_magento_sales_credit_memo_id = l_magento_sales_credit_memo.l_magento_sales_credit_memo_id
 where l_magento_sales_credit_memo.l_magento_sales_credit_memo_id is null
    or (l_magento_sales_credit_memo.l_magento_sales_credit_memo_id is not null
        and l_magento_sales_credit_memo.dv_hash <> #l_magento_sales_credit_memo_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_credit_memo
if object_id('tempdb..#s_magento_sales_credit_memo_inserts') is not null drop table #s_magento_sales_credit_memo_inserts
create table #s_magento_sales_credit_memo_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_creditmemo.bk_hash,
       stage_hash_magento_sales_creditmemo.entity_id entity_id,
       stage_hash_magento_sales_creditmemo.adjustment_positive adjustment_positive,
       stage_hash_magento_sales_creditmemo.base_shipping_tax_amount base_shipping_tax_amount,
       stage_hash_magento_sales_creditmemo.store_to_order_rate store_to_order_rate,
       stage_hash_magento_sales_creditmemo.base_discount_amount base_discount_amount,
       stage_hash_magento_sales_creditmemo.base_to_order_rate base_to_order_rate,
       stage_hash_magento_sales_creditmemo.grand_total grand_total,
       stage_hash_magento_sales_creditmemo.base_adjustment_negative base_adjustment_negative,
       stage_hash_magento_sales_creditmemo.base_subtotal_incl_tax base_subtotal_incl_tax,
       stage_hash_magento_sales_creditmemo.shipping_amount shipping_amount,
       stage_hash_magento_sales_creditmemo.subtotal_incl_tax subtotal_incl_tax,
       stage_hash_magento_sales_creditmemo.adjustment_negative adjustment_negative,
       stage_hash_magento_sales_creditmemo.base_shipping_amount base_shipping_amount,
       stage_hash_magento_sales_creditmemo.store_to_base_rate store_to_base_rate,
       stage_hash_magento_sales_creditmemo.base_to_global_rate base_to_global_rate,
       stage_hash_magento_sales_creditmemo.base_adjustment base_adjustment,
       stage_hash_magento_sales_creditmemo.base_subtotal base_subtotal,
       stage_hash_magento_sales_creditmemo.discount_amount discount_amount,
       stage_hash_magento_sales_creditmemo.subtotal subtotal,
       stage_hash_magento_sales_creditmemo.adjustment adjustment,
       stage_hash_magento_sales_creditmemo.base_grand_total base_grand_total,
       stage_hash_magento_sales_creditmemo.base_adjustment_positive base_adjustment_positive,
       stage_hash_magento_sales_creditmemo.base_tax_amount base_tax_amount,
       stage_hash_magento_sales_creditmemo.shipping_tax_amount shipping_tax_amount,
       stage_hash_magento_sales_creditmemo.tax_amount tax_amount,
       stage_hash_magento_sales_creditmemo.email_sent email_sent,
       stage_hash_magento_sales_creditmemo.send_email send_email,
       stage_hash_magento_sales_creditmemo.creditmemo_status credit_memo_status,
       stage_hash_magento_sales_creditmemo.state state,
       stage_hash_magento_sales_creditmemo.store_currency_code store_currency_code,
       stage_hash_magento_sales_creditmemo.order_currency_code order_currency_code,
       stage_hash_magento_sales_creditmemo.base_currency_code base_currency_code,
       stage_hash_magento_sales_creditmemo.global_currency_code global_currency_code,
       stage_hash_magento_sales_creditmemo.created_at created_at,
       stage_hash_magento_sales_creditmemo.updated_at updated_at,
       stage_hash_magento_sales_creditmemo.discount_tax_compensation_amount discount_tax_compensation_amount,
       stage_hash_magento_sales_creditmemo.base_discount_tax_compensation_amount base_discount_tax_compensation_amount,
       stage_hash_magento_sales_creditmemo.shipping_discount_tax_compensation_amount shipping_discount_tax_compensation_amount,
       stage_hash_magento_sales_creditmemo.base_shipping_discount_tax_compensation_amnt base_shipping_discount_tax_compensation_amnt,
       stage_hash_magento_sales_creditmemo.shipping_incl_tax shipping_incl_tax,
       stage_hash_magento_sales_creditmemo.base_shipping_incl_tax base_shipping_incl_tax,
       stage_hash_magento_sales_creditmemo.discount_description discount_description,
       stage_hash_magento_sales_creditmemo.customer_note customer_note,
       stage_hash_magento_sales_creditmemo.customer_note_notify customer_note_notify,
       stage_hash_magento_sales_creditmemo.base_customer_balance_amount base_customer_balance_amount,
       stage_hash_magento_sales_creditmemo.customer_balance_amount customer_balance_amount,
       stage_hash_magento_sales_creditmemo.bs_customer_bal_total_refunded bs_customer_bal_total_refunded,
       stage_hash_magento_sales_creditmemo.customer_bal_total_refunded customer_bal_total_refunded,
       stage_hash_magento_sales_creditmemo.base_gift_cards_amount base_gift_cards_amount,
       stage_hash_magento_sales_creditmemo.gift_cards_amount gift_cards_amount,
       stage_hash_magento_sales_creditmemo.gw_base_price gw_base_price,
       stage_hash_magento_sales_creditmemo.gw_price gw_price,
       stage_hash_magento_sales_creditmemo.gw_items_base_price gw_items_base_price,
       stage_hash_magento_sales_creditmemo.gw_items_price gw_items_price,
       stage_hash_magento_sales_creditmemo.gw_card_base_price gw_card_base_price,
       stage_hash_magento_sales_creditmemo.gw_card_price gw_card_price,
       stage_hash_magento_sales_creditmemo.gw_base_tax_amount gw_base_tax_amount,
       stage_hash_magento_sales_creditmemo.gw_tax_amount gw_tax_amount,
       stage_hash_magento_sales_creditmemo.gw_items_base_tax_amount gw_items_base_tax_amount,
       stage_hash_magento_sales_creditmemo.gw_items_tax_amount gw_items_tax_amount,
       stage_hash_magento_sales_creditmemo.gw_card_base_tax_amount gw_card_base_tax_amount,
       stage_hash_magento_sales_creditmemo.gw_card_tax_amount gw_card_tax_amount,
       stage_hash_magento_sales_creditmemo.base_reward_currency_amount base_reward_currency_amount,
       stage_hash_magento_sales_creditmemo.reward_currency_amount reward_currency_amount,
       stage_hash_magento_sales_creditmemo.reward_points_balance reward_points_balance,
       stage_hash_magento_sales_creditmemo.reward_points_balance_refund reward_points_balance_refund,
       isnull(cast(stage_hash_magento_sales_creditmemo.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.adjustment_positive as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_shipping_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.store_to_order_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_to_order_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.grand_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_adjustment_negative as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_subtotal_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.shipping_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.subtotal_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.adjustment_negative as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_shipping_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.store_to_base_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_to_global_rate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_adjustment as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_subtotal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.discount_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.subtotal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.adjustment as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_grand_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_adjustment_positive as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.shipping_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.email_sent as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.send_email as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.creditmemo_status as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.state as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_creditmemo.store_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_creditmemo.order_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_creditmemo.base_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_creditmemo.global_currency_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_creditmemo.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_sales_creditmemo.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.shipping_discount_tax_compensation_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_shipping_discount_tax_compensation_amnt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.shipping_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_shipping_incl_tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_creditmemo.discount_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_creditmemo.customer_note,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.customer_note_notify as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_customer_balance_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.customer_balance_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.bs_customer_bal_total_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.customer_bal_total_refunded as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_gift_cards_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gift_cards_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_items_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_items_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_card_base_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_card_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_items_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_items_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_card_base_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.gw_card_tax_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.base_reward_currency_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.reward_currency_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.reward_points_balance as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_creditmemo.reward_points_balance_refund as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_creditmemo
 where stage_hash_magento_sales_creditmemo.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_credit_memo records
set @insert_date_time = getdate()
insert into s_magento_sales_credit_memo (
       bk_hash,
       entity_id,
       adjustment_positive,
       base_shipping_tax_amount,
       store_to_order_rate,
       base_discount_amount,
       base_to_order_rate,
       grand_total,
       base_adjustment_negative,
       base_subtotal_incl_tax,
       shipping_amount,
       subtotal_incl_tax,
       adjustment_negative,
       base_shipping_amount,
       store_to_base_rate,
       base_to_global_rate,
       base_adjustment,
       base_subtotal,
       discount_amount,
       subtotal,
       adjustment,
       base_grand_total,
       base_adjustment_positive,
       base_tax_amount,
       shipping_tax_amount,
       tax_amount,
       email_sent,
       send_email,
       credit_memo_status,
       state,
       store_currency_code,
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
       discount_description,
       customer_note,
       customer_note_notify,
       base_customer_balance_amount,
       customer_balance_amount,
       bs_customer_bal_total_refunded,
       customer_bal_total_refunded,
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
       reward_points_balance_refund,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_credit_memo_inserts.bk_hash,
       #s_magento_sales_credit_memo_inserts.entity_id,
       #s_magento_sales_credit_memo_inserts.adjustment_positive,
       #s_magento_sales_credit_memo_inserts.base_shipping_tax_amount,
       #s_magento_sales_credit_memo_inserts.store_to_order_rate,
       #s_magento_sales_credit_memo_inserts.base_discount_amount,
       #s_magento_sales_credit_memo_inserts.base_to_order_rate,
       #s_magento_sales_credit_memo_inserts.grand_total,
       #s_magento_sales_credit_memo_inserts.base_adjustment_negative,
       #s_magento_sales_credit_memo_inserts.base_subtotal_incl_tax,
       #s_magento_sales_credit_memo_inserts.shipping_amount,
       #s_magento_sales_credit_memo_inserts.subtotal_incl_tax,
       #s_magento_sales_credit_memo_inserts.adjustment_negative,
       #s_magento_sales_credit_memo_inserts.base_shipping_amount,
       #s_magento_sales_credit_memo_inserts.store_to_base_rate,
       #s_magento_sales_credit_memo_inserts.base_to_global_rate,
       #s_magento_sales_credit_memo_inserts.base_adjustment,
       #s_magento_sales_credit_memo_inserts.base_subtotal,
       #s_magento_sales_credit_memo_inserts.discount_amount,
       #s_magento_sales_credit_memo_inserts.subtotal,
       #s_magento_sales_credit_memo_inserts.adjustment,
       #s_magento_sales_credit_memo_inserts.base_grand_total,
       #s_magento_sales_credit_memo_inserts.base_adjustment_positive,
       #s_magento_sales_credit_memo_inserts.base_tax_amount,
       #s_magento_sales_credit_memo_inserts.shipping_tax_amount,
       #s_magento_sales_credit_memo_inserts.tax_amount,
       #s_magento_sales_credit_memo_inserts.email_sent,
       #s_magento_sales_credit_memo_inserts.send_email,
       #s_magento_sales_credit_memo_inserts.credit_memo_status,
       #s_magento_sales_credit_memo_inserts.state,
       #s_magento_sales_credit_memo_inserts.store_currency_code,
       #s_magento_sales_credit_memo_inserts.order_currency_code,
       #s_magento_sales_credit_memo_inserts.base_currency_code,
       #s_magento_sales_credit_memo_inserts.global_currency_code,
       #s_magento_sales_credit_memo_inserts.created_at,
       #s_magento_sales_credit_memo_inserts.updated_at,
       #s_magento_sales_credit_memo_inserts.discount_tax_compensation_amount,
       #s_magento_sales_credit_memo_inserts.base_discount_tax_compensation_amount,
       #s_magento_sales_credit_memo_inserts.shipping_discount_tax_compensation_amount,
       #s_magento_sales_credit_memo_inserts.base_shipping_discount_tax_compensation_amnt,
       #s_magento_sales_credit_memo_inserts.shipping_incl_tax,
       #s_magento_sales_credit_memo_inserts.base_shipping_incl_tax,
       #s_magento_sales_credit_memo_inserts.discount_description,
       #s_magento_sales_credit_memo_inserts.customer_note,
       #s_magento_sales_credit_memo_inserts.customer_note_notify,
       #s_magento_sales_credit_memo_inserts.base_customer_balance_amount,
       #s_magento_sales_credit_memo_inserts.customer_balance_amount,
       #s_magento_sales_credit_memo_inserts.bs_customer_bal_total_refunded,
       #s_magento_sales_credit_memo_inserts.customer_bal_total_refunded,
       #s_magento_sales_credit_memo_inserts.base_gift_cards_amount,
       #s_magento_sales_credit_memo_inserts.gift_cards_amount,
       #s_magento_sales_credit_memo_inserts.gw_base_price,
       #s_magento_sales_credit_memo_inserts.gw_price,
       #s_magento_sales_credit_memo_inserts.gw_items_base_price,
       #s_magento_sales_credit_memo_inserts.gw_items_price,
       #s_magento_sales_credit_memo_inserts.gw_card_base_price,
       #s_magento_sales_credit_memo_inserts.gw_card_price,
       #s_magento_sales_credit_memo_inserts.gw_base_tax_amount,
       #s_magento_sales_credit_memo_inserts.gw_tax_amount,
       #s_magento_sales_credit_memo_inserts.gw_items_base_tax_amount,
       #s_magento_sales_credit_memo_inserts.gw_items_tax_amount,
       #s_magento_sales_credit_memo_inserts.gw_card_base_tax_amount,
       #s_magento_sales_credit_memo_inserts.gw_card_tax_amount,
       #s_magento_sales_credit_memo_inserts.base_reward_currency_amount,
       #s_magento_sales_credit_memo_inserts.reward_currency_amount,
       #s_magento_sales_credit_memo_inserts.reward_points_balance,
       #s_magento_sales_credit_memo_inserts.reward_points_balance_refund,
       case when s_magento_sales_credit_memo.s_magento_sales_credit_memo_id is null then isnull(#s_magento_sales_credit_memo_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_credit_memo_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_credit_memo_inserts
  left join p_magento_sales_credit_memo
    on #s_magento_sales_credit_memo_inserts.bk_hash = p_magento_sales_credit_memo.bk_hash
   and p_magento_sales_credit_memo.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_credit_memo
    on p_magento_sales_credit_memo.bk_hash = s_magento_sales_credit_memo.bk_hash
   and p_magento_sales_credit_memo.s_magento_sales_credit_memo_id = s_magento_sales_credit_memo.s_magento_sales_credit_memo_id
 where s_magento_sales_credit_memo.s_magento_sales_credit_memo_id is null
    or (s_magento_sales_credit_memo.s_magento_sales_credit_memo_id is not null
        and s_magento_sales_credit_memo.dv_hash <> #s_magento_sales_credit_memo_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_credit_memo @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_credit_memo @current_dv_batch_id

end
