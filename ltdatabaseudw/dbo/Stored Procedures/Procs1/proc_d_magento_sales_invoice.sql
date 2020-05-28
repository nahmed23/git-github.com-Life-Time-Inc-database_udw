CREATE PROC [dbo].[proc_d_magento_sales_invoice] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_invoice)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_invoice_insert') is not null drop table #p_magento_sales_invoice_insert
create table dbo.#p_magento_sales_invoice_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_invoice.p_magento_sales_invoice_id,
       p_magento_sales_invoice.bk_hash
  from dbo.p_magento_sales_invoice
 where p_magento_sales_invoice.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_invoice.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_invoice.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_invoice.bk_hash,
       p_magento_sales_invoice.entity_id entity_id,
       convert(varchar(8),DATEADD(mm,DATEDIFF(mm,0,s_magento_sales_invoice.created_at),0),112) allocated_month_starting_dim_date_key,
       dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_magento_sales_invoice.created_at)),0)) allocated_recalculate_through_datetime,
       convert(varchar(8),dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_magento_sales_invoice.created_at)),0)),112) allocated_recalculate_through_dim_date_key,
       s_magento_sales_invoice.base_currency_code base_currency_code,
       s_magento_sales_invoice.base_customer_balance_amount base_customer_balance_amount,
       s_magento_sales_invoice.base_discount_amount base_discount_amount,
       s_magento_sales_invoice.base_discount_tax_compensation_amount base_discount_tax_compensation_amount,
       s_magento_sales_invoice.base_gift_cards_amount base_gift_cards_amount,
       s_magento_sales_invoice.base_grand_total base_grand_total,
       s_magento_sales_invoice.base_reward_currency_amount base_reward_currency_amount,
       s_magento_sales_invoice.base_shipping_amount base_shipping_amount,
       s_magento_sales_invoice.base_shipping_discount_tax_compensation_amnt base_shipping_discount_tax_compensation_amnt,
       s_magento_sales_invoice.base_shipping_incl_tax base_shipping_incl_tax,
       s_magento_sales_invoice.base_shipping_tax_amount base_shipping_tax_amount,
       s_magento_sales_invoice.base_subtotal base_subtotal,
       s_magento_sales_invoice.base_subtotal_incl_tax base_subtotal_incl_tax,
       s_magento_sales_invoice.base_tax_amount base_tax_amount,
       s_magento_sales_invoice.base_to_global_rate base_to_global_rate,
       s_magento_sales_invoice.base_to_order_rate base_to_order_rate,
       s_magento_sales_invoice.base_total_refunded base_total_refunded,
       l_magento_sales_invoice.billing_address_id billing_address_id,
       case when s_magento_sales_invoice.can_void_flag= 1 then 'Y' else 'N' end can_void_flag,
       s_magento_sales_invoice.created_at created_at,
       case when p_magento_sales_invoice.bk_hash in('-997', '-998', '-999') then p_magento_sales_invoice.bk_hash
           when s_magento_sales_invoice.created_at is null then '-998'
        else convert(varchar, s_magento_sales_invoice.created_at, 112)    end created_dim_date_key,
       case when p_magento_sales_invoice.bk_hash in ('-997','-998','-999') then p_magento_sales_invoice.bk_hash
       when s_magento_sales_invoice.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_invoice.created_at,114), 1, 5),':','') end created_dim_time_key,
       s_magento_sales_invoice.customer_balance_amount customer_balance_amount,
       s_magento_sales_invoice.customer_note_notify customer_note_notify,
       case when p_magento_sales_invoice.bk_hash in('-997', '-998', '-999') then p_magento_sales_invoice.bk_hash
           when l_magento_sales_invoice.order_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_invoice.order_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_magento_sales_order_bk_hash,
       s_magento_sales_invoice.discount_amount discount_amount,
       s_magento_sales_invoice.discount_description discount_description,
       s_magento_sales_invoice.discount_tax_compensation_amount discount_tax_compensation_amount,
       s_magento_sales_invoice.email_sent email_sent,
       case when p_magento_sales_invoice.bk_hash in ('-997', '-998', '-999') then p_magento_sales_invoice.bk_hash
            when l_magento_sales_invoice.order_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_invoice.order_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_order_key,
       case when p_magento_sales_invoice.bk_hash in ('-997', '-998', '-999') then p_magento_sales_invoice.bk_hash
            when s_magento_sales_invoice.transaction_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_magento_sales_invoice.transaction_id as varchar(100)),'z#@$k%&P'))),2) 
        end fact_magento_payment_key,
       s_magento_sales_invoice.gift_cards_amount gift_cards_amount,
       s_magento_sales_invoice.global_currency_code global_currency_code,
       s_magento_sales_invoice.grand_total grand_total,
       l_magento_sales_invoice.increment_id increment_id,
       case when s_magento_sales_invoice.is_used_for_refund= 1 then 'Y' else 'N' end is_used_for_refund_flag,
       l_magento_sales_invoice.m1_invoice_id m1_invoice_id,
       s_magento_sales_invoice.order_currency_code order_currency_code,
       s_magento_sales_invoice.reward_currency_amount reward_currency_amount,
       s_magento_sales_invoice.reward_points_balance reward_points_balance,
       s_magento_sales_invoice.send_email send_email,
       l_magento_sales_invoice.shipping_address_id shipping_address_id,
       s_magento_sales_invoice.shipping_amount shipping_amount,
       s_magento_sales_invoice.shipping_discount_tax_compensation_amount shipping_discount_tax_compensation_amount,
       s_magento_sales_invoice.shipping_incl_tax shipping_incl_tax,
       s_magento_sales_invoice.shipping_tax_amount shipping_tax_amount,
       s_magento_sales_invoice.state state,
       s_magento_sales_invoice.store_currency_code store_currency_code,
       l_magento_sales_invoice.store_id store_id,
       s_magento_sales_invoice.store_to_base_rate store_to_base_rate,
       s_magento_sales_invoice.store_to_order_rate store_to_order_rate,
       s_magento_sales_invoice.subtotal subtotal,
       s_magento_sales_invoice.subtotal_incl_tax subtotal_incl_tax,
       s_magento_sales_invoice.tax_amount tax_amount,
       s_magento_sales_invoice.total_qty total_qty,
       s_magento_sales_invoice.transaction_id transaction_id,
       s_magento_sales_invoice.updated_at updated_at,
       case when p_magento_sales_invoice.bk_hash in('-997', '-998', '-999') then p_magento_sales_invoice.bk_hash
           when s_magento_sales_invoice.updated_at is null then '-998'
        else convert(varchar, s_magento_sales_invoice.updated_at, 112)    end updated_dim_date_key,
       case when p_magento_sales_invoice.bk_hash in ('-997','-998','-999') then p_magento_sales_invoice.bk_hash
       when s_magento_sales_invoice.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_invoice.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_magento_sales_invoice.dv_deleted,0) dv_deleted,
       p_magento_sales_invoice.p_magento_sales_invoice_id,
       p_magento_sales_invoice.dv_batch_id,
       p_magento_sales_invoice.dv_load_date_time,
       p_magento_sales_invoice.dv_load_end_date_time
  from dbo.h_magento_sales_invoice
  join dbo.p_magento_sales_invoice
    on h_magento_sales_invoice.bk_hash = p_magento_sales_invoice.bk_hash
  join #p_magento_sales_invoice_insert
    on p_magento_sales_invoice.bk_hash = #p_magento_sales_invoice_insert.bk_hash
   and p_magento_sales_invoice.p_magento_sales_invoice_id = #p_magento_sales_invoice_insert.p_magento_sales_invoice_id
  join dbo.l_magento_sales_invoice
    on p_magento_sales_invoice.bk_hash = l_magento_sales_invoice.bk_hash
   and p_magento_sales_invoice.l_magento_sales_invoice_id = l_magento_sales_invoice.l_magento_sales_invoice_id
  join dbo.s_magento_sales_invoice
    on p_magento_sales_invoice.bk_hash = s_magento_sales_invoice.bk_hash
   and p_magento_sales_invoice.s_magento_sales_invoice_id = s_magento_sales_invoice.s_magento_sales_invoice_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_invoice
   where d_magento_sales_invoice.bk_hash in (select bk_hash from #p_magento_sales_invoice_insert)

  insert dbo.d_magento_sales_invoice(
             bk_hash,
             entity_id,
             allocated_month_starting_dim_date_key,
             allocated_recalculate_through_datetime,
             allocated_recalculate_through_dim_date_key,
             base_currency_code,
             base_customer_balance_amount,
             base_discount_amount,
             base_discount_tax_compensation_amount,
             base_gift_cards_amount,
             base_grand_total,
             base_reward_currency_amount,
             base_shipping_amount,
             base_shipping_discount_tax_compensation_amnt,
             base_shipping_incl_tax,
             base_shipping_tax_amount,
             base_subtotal,
             base_subtotal_incl_tax,
             base_tax_amount,
             base_to_global_rate,
             base_to_order_rate,
             base_total_refunded,
             billing_address_id,
             can_void_flag,
             created_at,
             created_dim_date_key,
             created_dim_time_key,
             customer_balance_amount,
             customer_note_notify,
             d_magento_sales_order_bk_hash,
             discount_amount,
             discount_description,
             discount_tax_compensation_amount,
             email_sent,
             fact_magento_order_key,
             fact_magento_payment_key,
             gift_cards_amount,
             global_currency_code,
             grand_total,
             increment_id,
             is_used_for_refund_flag,
             m1_invoice_id,
             order_currency_code,
             reward_currency_amount,
             reward_points_balance,
             send_email,
             shipping_address_id,
             shipping_amount,
             shipping_discount_tax_compensation_amount,
             shipping_incl_tax,
             shipping_tax_amount,
             state,
             store_currency_code,
             store_id,
             store_to_base_rate,
             store_to_order_rate,
             subtotal,
             subtotal_incl_tax,
             tax_amount,
             total_qty,
             transaction_id,
             updated_at,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_magento_sales_invoice_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         entity_id,
         allocated_month_starting_dim_date_key,
         allocated_recalculate_through_datetime,
         allocated_recalculate_through_dim_date_key,
         base_currency_code,
         base_customer_balance_amount,
         base_discount_amount,
         base_discount_tax_compensation_amount,
         base_gift_cards_amount,
         base_grand_total,
         base_reward_currency_amount,
         base_shipping_amount,
         base_shipping_discount_tax_compensation_amnt,
         base_shipping_incl_tax,
         base_shipping_tax_amount,
         base_subtotal,
         base_subtotal_incl_tax,
         base_tax_amount,
         base_to_global_rate,
         base_to_order_rate,
         base_total_refunded,
         billing_address_id,
         can_void_flag,
         created_at,
         created_dim_date_key,
         created_dim_time_key,
         customer_balance_amount,
         customer_note_notify,
         d_magento_sales_order_bk_hash,
         discount_amount,
         discount_description,
         discount_tax_compensation_amount,
         email_sent,
         fact_magento_order_key,
         fact_magento_payment_key,
         gift_cards_amount,
         global_currency_code,
         grand_total,
         increment_id,
         is_used_for_refund_flag,
         m1_invoice_id,
         order_currency_code,
         reward_currency_amount,
         reward_points_balance,
         send_email,
         shipping_address_id,
         shipping_amount,
         shipping_discount_tax_compensation_amount,
         shipping_incl_tax,
         shipping_tax_amount,
         state,
         store_currency_code,
         store_id,
         store_to_base_rate,
         store_to_order_rate,
         subtotal,
         subtotal_incl_tax,
         tax_amount,
         total_qty,
         transaction_id,
         updated_at,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_magento_sales_invoice_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_invoice)
--Done!
end
