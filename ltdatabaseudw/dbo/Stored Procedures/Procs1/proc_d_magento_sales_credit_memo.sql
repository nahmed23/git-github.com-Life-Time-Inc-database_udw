CREATE PROC [dbo].[proc_d_magento_sales_credit_memo] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_credit_memo)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_credit_memo_insert') is not null drop table #p_magento_sales_credit_memo_insert
create table dbo.#p_magento_sales_credit_memo_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_credit_memo.p_magento_sales_credit_memo_id,
       p_magento_sales_credit_memo.bk_hash
  from dbo.p_magento_sales_credit_memo
 where p_magento_sales_credit_memo.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_credit_memo.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_credit_memo.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_credit_memo.bk_hash,
       p_magento_sales_credit_memo.entity_id entity_id,
       s_magento_sales_credit_memo.adjustment_positive adjustment_positive,
       convert(varchar(8),DATEADD(mm,DATEDIFF(mm,0,s_magento_sales_credit_memo.created_at),0),112) allocated_month_starting_dim_date_key,
       dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_magento_sales_credit_memo.created_at)),0)) allocated_recalculate_through_datetime,
       convert(varchar(8),dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_magento_sales_credit_memo.created_at)),0)),112) allocated_recalculate_through_dim_date_key,
       s_magento_sales_credit_memo.base_adjustment base_adjustment,
       s_magento_sales_credit_memo.base_adjustment_negative base_adjustment_negative,
       s_magento_sales_credit_memo.base_discount_amount base_discount_amount,
       s_magento_sales_credit_memo.base_discount_tax_compensation_amount base_discount_tax_compensation_amount,
       s_magento_sales_credit_memo.base_shipping_amount base_shipping_amount,
       s_magento_sales_credit_memo.base_shipping_discount_tax_compensation_amnt base_shipping_discount_tax_compensation_amnt,
       s_magento_sales_credit_memo.base_shipping_incl_tax base_shipping_incl_tax,
       s_magento_sales_credit_memo.base_shipping_tax_amount base_shipping_tax_amount,
       s_magento_sales_credit_memo.base_subtotal base_subtotal,
       s_magento_sales_credit_memo.base_subtotal_incl_tax base_subtotal_incl_tax,
       s_magento_sales_credit_memo.base_to_global_rate base_to_global_rate,
       s_magento_sales_credit_memo.base_to_order_rate base_to_order_rate,
       l_magento_sales_credit_memo.billing_address_id billing_address_id,
       s_magento_sales_credit_memo.bs_customer_bal_total_refunded bs_customer_bal_total_refunded,
       s_magento_sales_credit_memo.created_at created_at,
       case when p_magento_sales_credit_memo.bk_hash in('-997', '-998', '-999') then p_magento_sales_credit_memo.bk_hash
           when s_magento_sales_credit_memo.created_at is null then '-998'
        else convert(varchar, s_magento_sales_credit_memo.created_at, 112)    end created_dim_date_key,
       case when p_magento_sales_credit_memo.bk_hash in ('-997','-998','-999') then p_magento_sales_credit_memo.bk_hash
       when s_magento_sales_credit_memo.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_credit_memo.created_at,114), 1, 5),':','') end created_dim_time_key,
       case when p_magento_sales_credit_memo.bk_hash in('-997', '-998', '-999') then p_magento_sales_credit_memo.bk_hash
           when l_magento_sales_credit_memo.order_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_credit_memo.order_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_magento_sales_order_entity_bk_hash,
       s_magento_sales_credit_memo.discount_amount discount_amount,
       s_magento_sales_credit_memo.discount_tax_compensation_amount discount_tax_compensation_amount,
       case when p_magento_sales_credit_memo.bk_hash in ('-997', '-998', '-999') then p_magento_sales_credit_memo.bk_hash
            when l_magento_sales_credit_memo.invoice_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_credit_memo.invoice_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_invoice_key,
       case when p_magento_sales_credit_memo.bk_hash in ('-997', '-998', '-999') then p_magento_sales_credit_memo.bk_hash
            when l_magento_sales_credit_memo.order_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_credit_memo.order_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_order_key,
       case when p_magento_sales_credit_memo.bk_hash in ('-997', '-998', '-999') then p_magento_sales_credit_memo.bk_hash
            when l_magento_sales_credit_memo.transaction_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_magento_sales_credit_memo.transaction_id as varchar(100)),'z#@$k%&P'))),2) 
        end fact_magento_payment_key,
       l_magento_sales_credit_memo.increment_id increment_id,
       l_magento_sales_credit_memo.invoice_id invoice_id,
       s_magento_sales_credit_memo.adjustment sales_credit_memo_adjustment,
       s_magento_sales_credit_memo.adjustment_negative sales_credit_memo_adjustment_negative,
       s_magento_sales_credit_memo.base_adjustment_positive sales_credit_memo_base_adjustment_positive,
       s_magento_sales_credit_memo.base_currency_code sales_credit_memo_base_currency_code,
       s_magento_sales_credit_memo.base_customer_balance_amount sales_credit_memo_base_customer_balance_amount,
       s_magento_sales_credit_memo.base_gift_cards_amount sales_credit_memo_base_gift_cards_amount,
       s_magento_sales_credit_memo.base_grand_total sales_credit_memo_base_grand_total,
       s_magento_sales_credit_memo.base_reward_currency_amount sales_credit_memo_base_reward_currency_amount,
       s_magento_sales_credit_memo.base_tax_amount sales_credit_memo_base_tax_amount,
       s_magento_sales_credit_memo.credit_memo_status sales_credit_memo_credit_memo_status,
       s_magento_sales_credit_memo.customer_bal_total_refunded sales_credit_memo_customer_bal_total_refunded,
       s_magento_sales_credit_memo.customer_balance_amount sales_credit_memo_customer_balance_amount,
       s_magento_sales_credit_memo.discount_description sales_credit_memo_discount_description,
       s_magento_sales_credit_memo.email_sent sales_credit_memo_email_sent,
       s_magento_sales_credit_memo.gift_cards_amount sales_credit_memo_gift_cards_amount,
       s_magento_sales_credit_memo.global_currency_code sales_credit_memo_global_currency_code,
       s_magento_sales_credit_memo.grand_total sales_credit_memo_grand_total,
       s_magento_sales_credit_memo.order_currency_code sales_credit_memo_order_currency_code,
       s_magento_sales_credit_memo.reward_currency_amount sales_credit_memo_reward_currency_amount,
       s_magento_sales_credit_memo.reward_points_balance sales_credit_memo_reward_points_balance,
       s_magento_sales_credit_memo.reward_points_balance_refund sales_credit_memo_reward_points_balance_refund,
       s_magento_sales_credit_memo.send_email sales_credit_memo_send_email,
       s_magento_sales_credit_memo.shipping_amount sales_credit_memo_shipping_amount,
       s_magento_sales_credit_memo.shipping_tax_amount sales_credit_memo_shipping_tax_amount,
       s_magento_sales_credit_memo.state sales_credit_memo_state,
       s_magento_sales_credit_memo.store_currency_code sales_credit_memo_store_currency_code,
       s_magento_sales_credit_memo.subtotal sales_credit_memo_subtotal,
       s_magento_sales_credit_memo.subtotal_incl_tax sales_credit_memo_subtotal_incl_tax,
       s_magento_sales_credit_memo.tax_amount sales_credit_memo_tax_amount,
       l_magento_sales_credit_memo.shipping_address_id shipping_address_id,
       s_magento_sales_credit_memo.shipping_discount_tax_compensation_amount shipping_discount_tax_compensation_amount,
       s_magento_sales_credit_memo.shipping_incl_tax shipping_incl_tax,
       l_magento_sales_credit_memo.store_id store_id,
       s_magento_sales_credit_memo.store_to_base_rate store_to_base_rate,
       s_magento_sales_credit_memo.store_to_order_rate store_to_order_rate,
       l_magento_sales_credit_memo.transaction_id transaction_id,
       s_magento_sales_credit_memo.updated_at updated_at,
       case when p_magento_sales_credit_memo.bk_hash in('-997', '-998', '-999') then p_magento_sales_credit_memo.bk_hash
           when s_magento_sales_credit_memo.updated_at is null then '-998'
        else convert(varchar, s_magento_sales_credit_memo.updated_at, 112)    end updated_dim_date_key,
       case when p_magento_sales_credit_memo.bk_hash in ('-997','-998','-999') then p_magento_sales_credit_memo.bk_hash
       when s_magento_sales_credit_memo.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_credit_memo.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_magento_sales_credit_memo.dv_deleted,0) dv_deleted,
       p_magento_sales_credit_memo.p_magento_sales_credit_memo_id,
       p_magento_sales_credit_memo.dv_batch_id,
       p_magento_sales_credit_memo.dv_load_date_time,
       p_magento_sales_credit_memo.dv_load_end_date_time
  from dbo.h_magento_sales_credit_memo
  join dbo.p_magento_sales_credit_memo
    on h_magento_sales_credit_memo.bk_hash = p_magento_sales_credit_memo.bk_hash
  join #p_magento_sales_credit_memo_insert
    on p_magento_sales_credit_memo.bk_hash = #p_magento_sales_credit_memo_insert.bk_hash
   and p_magento_sales_credit_memo.p_magento_sales_credit_memo_id = #p_magento_sales_credit_memo_insert.p_magento_sales_credit_memo_id
  join dbo.l_magento_sales_credit_memo
    on p_magento_sales_credit_memo.bk_hash = l_magento_sales_credit_memo.bk_hash
   and p_magento_sales_credit_memo.l_magento_sales_credit_memo_id = l_magento_sales_credit_memo.l_magento_sales_credit_memo_id
  join dbo.s_magento_sales_credit_memo
    on p_magento_sales_credit_memo.bk_hash = s_magento_sales_credit_memo.bk_hash
   and p_magento_sales_credit_memo.s_magento_sales_credit_memo_id = s_magento_sales_credit_memo.s_magento_sales_credit_memo_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_credit_memo
   where d_magento_sales_credit_memo.bk_hash in (select bk_hash from #p_magento_sales_credit_memo_insert)

  insert dbo.d_magento_sales_credit_memo(
             bk_hash,
             entity_id,
             adjustment_positive,
             allocated_month_starting_dim_date_key,
             allocated_recalculate_through_datetime,
             allocated_recalculate_through_dim_date_key,
             base_adjustment,
             base_adjustment_negative,
             base_discount_amount,
             base_discount_tax_compensation_amount,
             base_shipping_amount,
             base_shipping_discount_tax_compensation_amnt,
             base_shipping_incl_tax,
             base_shipping_tax_amount,
             base_subtotal,
             base_subtotal_incl_tax,
             base_to_global_rate,
             base_to_order_rate,
             billing_address_id,
             bs_customer_bal_total_refunded,
             created_at,
             created_dim_date_key,
             created_dim_time_key,
             d_magento_sales_order_entity_bk_hash,
             discount_amount,
             discount_tax_compensation_amount,
             fact_magento_invoice_key,
             fact_magento_order_key,
             fact_magento_payment_key,
             increment_id,
             invoice_id,
             sales_credit_memo_adjustment,
             sales_credit_memo_adjustment_negative,
             sales_credit_memo_base_adjustment_positive,
             sales_credit_memo_base_currency_code,
             sales_credit_memo_base_customer_balance_amount,
             sales_credit_memo_base_gift_cards_amount,
             sales_credit_memo_base_grand_total,
             sales_credit_memo_base_reward_currency_amount,
             sales_credit_memo_base_tax_amount,
             sales_credit_memo_credit_memo_status,
             sales_credit_memo_customer_bal_total_refunded,
             sales_credit_memo_customer_balance_amount,
             sales_credit_memo_discount_description,
             sales_credit_memo_email_sent,
             sales_credit_memo_gift_cards_amount,
             sales_credit_memo_global_currency_code,
             sales_credit_memo_grand_total,
             sales_credit_memo_order_currency_code,
             sales_credit_memo_reward_currency_amount,
             sales_credit_memo_reward_points_balance,
             sales_credit_memo_reward_points_balance_refund,
             sales_credit_memo_send_email,
             sales_credit_memo_shipping_amount,
             sales_credit_memo_shipping_tax_amount,
             sales_credit_memo_state,
             sales_credit_memo_store_currency_code,
             sales_credit_memo_subtotal,
             sales_credit_memo_subtotal_incl_tax,
             sales_credit_memo_tax_amount,
             shipping_address_id,
             shipping_discount_tax_compensation_amount,
             shipping_incl_tax,
             store_id,
             store_to_base_rate,
             store_to_order_rate,
             transaction_id,
             updated_at,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_magento_sales_credit_memo_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         entity_id,
         adjustment_positive,
         allocated_month_starting_dim_date_key,
         allocated_recalculate_through_datetime,
         allocated_recalculate_through_dim_date_key,
         base_adjustment,
         base_adjustment_negative,
         base_discount_amount,
         base_discount_tax_compensation_amount,
         base_shipping_amount,
         base_shipping_discount_tax_compensation_amnt,
         base_shipping_incl_tax,
         base_shipping_tax_amount,
         base_subtotal,
         base_subtotal_incl_tax,
         base_to_global_rate,
         base_to_order_rate,
         billing_address_id,
         bs_customer_bal_total_refunded,
         created_at,
         created_dim_date_key,
         created_dim_time_key,
         d_magento_sales_order_entity_bk_hash,
         discount_amount,
         discount_tax_compensation_amount,
         fact_magento_invoice_key,
         fact_magento_order_key,
         fact_magento_payment_key,
         increment_id,
         invoice_id,
         sales_credit_memo_adjustment,
         sales_credit_memo_adjustment_negative,
         sales_credit_memo_base_adjustment_positive,
         sales_credit_memo_base_currency_code,
         sales_credit_memo_base_customer_balance_amount,
         sales_credit_memo_base_gift_cards_amount,
         sales_credit_memo_base_grand_total,
         sales_credit_memo_base_reward_currency_amount,
         sales_credit_memo_base_tax_amount,
         sales_credit_memo_credit_memo_status,
         sales_credit_memo_customer_bal_total_refunded,
         sales_credit_memo_customer_balance_amount,
         sales_credit_memo_discount_description,
         sales_credit_memo_email_sent,
         sales_credit_memo_gift_cards_amount,
         sales_credit_memo_global_currency_code,
         sales_credit_memo_grand_total,
         sales_credit_memo_order_currency_code,
         sales_credit_memo_reward_currency_amount,
         sales_credit_memo_reward_points_balance,
         sales_credit_memo_reward_points_balance_refund,
         sales_credit_memo_send_email,
         sales_credit_memo_shipping_amount,
         sales_credit_memo_shipping_tax_amount,
         sales_credit_memo_state,
         sales_credit_memo_store_currency_code,
         sales_credit_memo_subtotal,
         sales_credit_memo_subtotal_incl_tax,
         sales_credit_memo_tax_amount,
         shipping_address_id,
         shipping_discount_tax_compensation_amount,
         shipping_incl_tax,
         store_id,
         store_to_base_rate,
         store_to_order_rate,
         transaction_id,
         updated_at,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_magento_sales_credit_memo_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_credit_memo)
--Done!
end
