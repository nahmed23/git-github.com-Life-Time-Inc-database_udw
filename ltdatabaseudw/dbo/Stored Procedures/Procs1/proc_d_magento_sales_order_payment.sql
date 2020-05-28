CREATE PROC [dbo].[proc_d_magento_sales_order_payment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_order_payment)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_order_payment_insert') is not null drop table #p_magento_sales_order_payment_insert
create table dbo.#p_magento_sales_order_payment_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_order_payment.p_magento_sales_order_payment_id,
       p_magento_sales_order_payment.bk_hash
  from dbo.p_magento_sales_order_payment
 where p_magento_sales_order_payment.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_order_payment.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_order_payment.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_order_payment.bk_hash,
       p_magento_sales_order_payment.bk_hash fact_magento_sales_order_payment_key,
       p_magento_sales_order_payment.entity_id entity_id,
       s_magento_sales_order_payment.amount_authorized amount_authorized,
       s_magento_sales_order_payment.amount_canceled amount_canceled,
       s_magento_sales_order_payment.amount_ordered amount_ordered,
       s_magento_sales_order_payment.amount_paid amount_paid,
       s_magento_sales_order_payment.amount_refunded amount_refunded,
       s_magento_sales_order_payment.base_amount_authorized base_amount_authorized,
       s_magento_sales_order_payment.base_amount_canceled base_amount_canceled,
       s_magento_sales_order_payment.base_amount_ordered base_amount_ordered,
       s_magento_sales_order_payment.base_amount_paid base_amount_paid,
       s_magento_sales_order_payment.base_amount_paid_online base_amount_paid_online,
       s_magento_sales_order_payment.base_amount_refunded base_amount_refunded,
       s_magento_sales_order_payment.base_amount_refunded_online base_amount_refunded_online,
       s_magento_sales_order_payment.base_shipping_amount base_shipping_amount,
       s_magento_sales_order_payment.base_shipping_captured base_shipping_captured,
       s_magento_sales_order_payment.base_shipping_refunded base_shipping_refunded,
              case when s_magento_sales_order_payment.additional_information like '%credittranid%' and s_magento_sales_order_payment.additional_information like '%batchnumber%' then
               substring(s_magento_sales_order_payment.additional_information,patindex('%"batchNumber":[^{]%',
                                                         s_magento_sales_order_payment.additional_information)+14,
                                                         patindex('%,"creditTranId":[^{]%',s_magento_sales_order_payment.additional_information) - patindex('%"batchNumber":[^{]%',s_magento_sales_order_payment.additional_information) - 14)
                    else null end batch_number,
       s_magento_sales_order_payment.cc_last_4 cc_last_4,
       s_magento_sales_order_payment.cc_type cc_type,
              case when s_magento_sales_order_payment.additional_information like '%credittranid%' and s_magento_sales_order_payment.additional_information like '%storedValueTranId%' then
               substring(s_magento_sales_order_payment.additional_information,patindex('%"creditTranId":[^{]%',
                                                         s_magento_sales_order_payment.additional_information)+15,
                                                         patindex('%,"storedValueTranId":[^{]%',s_magento_sales_order_payment.additional_information) - patindex('%"creditTranId":[^{]%',s_magento_sales_order_payment.additional_information) - 15)
                    else null end credit_tran_id,
       case when p_magento_sales_order_payment.bk_hash in('-997', '-998', '-999') then p_magento_sales_order_payment.bk_hash
           when l_magento_sales_order_payment.parent_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_order_payment.parent_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_magento_sales_order_key,
       l_magento_sales_order_payment.last_trans_id last_trans_id,
       s_magento_sales_order_payment.method method,
       l_magento_sales_order_payment.quote_payment_id quote_payment_id,
       s_magento_sales_order_payment.shipping_captured shipping_captured,
       s_magento_sales_order_payment.shipping_refunded shipping_refunded,
       isnull(h_magento_sales_order_payment.dv_deleted,0) dv_deleted,
       p_magento_sales_order_payment.p_magento_sales_order_payment_id,
       p_magento_sales_order_payment.dv_batch_id,
       p_magento_sales_order_payment.dv_load_date_time,
       p_magento_sales_order_payment.dv_load_end_date_time
  from dbo.h_magento_sales_order_payment
  join dbo.p_magento_sales_order_payment
    on h_magento_sales_order_payment.bk_hash = p_magento_sales_order_payment.bk_hash
  join #p_magento_sales_order_payment_insert
    on p_magento_sales_order_payment.bk_hash = #p_magento_sales_order_payment_insert.bk_hash
   and p_magento_sales_order_payment.p_magento_sales_order_payment_id = #p_magento_sales_order_payment_insert.p_magento_sales_order_payment_id
  join dbo.l_magento_sales_order_payment
    on p_magento_sales_order_payment.bk_hash = l_magento_sales_order_payment.bk_hash
   and p_magento_sales_order_payment.l_magento_sales_order_payment_id = l_magento_sales_order_payment.l_magento_sales_order_payment_id
  join dbo.s_magento_sales_order_payment
    on p_magento_sales_order_payment.bk_hash = s_magento_sales_order_payment.bk_hash
   and p_magento_sales_order_payment.s_magento_sales_order_payment_id = s_magento_sales_order_payment.s_magento_sales_order_payment_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_order_payment
   where d_magento_sales_order_payment.bk_hash in (select bk_hash from #p_magento_sales_order_payment_insert)

  insert dbo.d_magento_sales_order_payment(
             bk_hash,
             fact_magento_sales_order_payment_key,
             entity_id,
             amount_authorized,
             amount_canceled,
             amount_ordered,
             amount_paid,
             amount_refunded,
             base_amount_authorized,
             base_amount_canceled,
             base_amount_ordered,
             base_amount_paid,
             base_amount_paid_online,
             base_amount_refunded,
             base_amount_refunded_online,
             base_shipping_amount,
             base_shipping_captured,
             base_shipping_refunded,
             batch_number,
             cc_last_4,
             cc_type,
             credit_tran_id,
             fact_magento_sales_order_key,
             last_trans_id,
             method,
             quote_payment_id,
             shipping_captured,
             shipping_refunded,
             deleted_flag,
             p_magento_sales_order_payment_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_magento_sales_order_payment_key,
         entity_id,
         amount_authorized,
         amount_canceled,
         amount_ordered,
         amount_paid,
         amount_refunded,
         base_amount_authorized,
         base_amount_canceled,
         base_amount_ordered,
         base_amount_paid,
         base_amount_paid_online,
         base_amount_refunded,
         base_amount_refunded_online,
         base_shipping_amount,
         base_shipping_captured,
         base_shipping_refunded,
         batch_number,
         cc_last_4,
         cc_type,
         credit_tran_id,
         fact_magento_sales_order_key,
         last_trans_id,
         method,
         quote_payment_id,
         shipping_captured,
         shipping_refunded,
         dv_deleted,
         p_magento_sales_order_payment_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_order_payment)
--Done!
end
