CREATE PROC [dbo].[proc_d_magento_payment_breakdown] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_payment_breakdown)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_payment_breakdown_insert') is not null drop table #p_magento_payment_breakdown_insert
create table dbo.#p_magento_payment_breakdown_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_payment_breakdown.p_magento_payment_breakdown_id,
       p_magento_payment_breakdown.bk_hash
  from dbo.p_magento_payment_breakdown
 where p_magento_payment_breakdown.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   --and (p_magento_payment_breakdown.dv_batch_id > @max_dv_batch_id
     --   or p_magento_payment_breakdown.dv_batch_id = @current_dv_batch_id)
     and p_magento_payment_breakdown.dv_batch_id = @current_dv_batch_id
	 
-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_payment_breakdown.bk_hash,
       p_magento_payment_breakdown.bk_hash fact_magento_payment_breakdown_key,
       p_magento_payment_breakdown.OrderNum OrderNum,
       p_magento_payment_breakdown.TranDate transaction_date,
       p_magento_payment_breakdown.OENum OENum,
       l_magento_payment_breakdown.club_id club_id,
       s_magento_payment_breakdown.cost_center cost_center,
       case when (capture_amex > 0 or refund_amex > 0) then case when (capture_amex-refund_amex) >= 0 then 'true' else 'false' end
        when (capture_master > 0 or refund_master > 0 or capture_visa > 0 or refund_visa > 0) then case when (capture_master+capture_visa-refund_master-refund_visa) >= 0 then 'true' else 'false' end
        when (capture_discover > 0 or refund_discover > 0) then case when (capture_discover-refund_discover) >= 0 then 'true' else 'false' end
        when (capture_paypal > 0 or refund_paypal > 0) then case when (capture_paypal-refund_paypal) >= 0 then 'true' else 'false' end
        when (capture_ltbucks > 0 or refund_ltbucks > 0) then case when (capture_ltbucks-refund_ltbucks) >= 0 then 'true' else 'false' end end deposit,
       s_magento_payment_breakdown.line_company line_company,
       s_magento_payment_breakdown.revenue_category revenue_category,
       s_magento_payment_breakdown.OrderShippingAmount shipping_amount,
       s_magento_payment_breakdown.spend_category spend_category,
       case when (s_magento_payment_breakdown.refund_amex > 0 OR s_magento_payment_breakdown.capture_amex > 0) then 'AMEX'
       when (s_magento_payment_breakdown.refund_discover > 0 OR s_magento_payment_breakdown.capture_discover > 0) then 'DISC'
       when (s_magento_payment_breakdown.refund_visa > 0  OR s_magento_payment_breakdown.capture_visa > 0) then 'VMC'
       when (s_magento_payment_breakdown.refund_paypal > 0 OR s_magento_payment_breakdown.capture_paypal > 0) then 'PAYPAL'
       when (s_magento_payment_breakdown.refund_master > 0 OR s_magento_payment_breakdown.capture_master > 0) then 'VMC' 
       when (s_magento_payment_breakdown.refund_ltbucks > 0 OR s_magento_payment_breakdown.capture_ltbucks > 0) then 'LTBUCKS' end tender_type_id,
       case when (capture_amex > 0 or refund_amex > 0) then capture_amex-refund_amex
        when (capture_master > 0 or refund_master > 0 or capture_visa > 0 or refund_visa > 0) then capture_master+capture_visa-refund_master-refund_visa
        when (capture_discover > 0 or refund_discover > 0) then capture_discover-refund_discover
        when (capture_paypal > 0 or refund_paypal > 0) then capture_paypal-refund_paypal
        when (capture_ltbucks > 0 or refund_ltbucks > 0) then capture_ltbucks-refund_ltbucks end transaction_amount,
       case when (capture_amex > 0 or refund_amex > 0) then capture_amex-refund_amex
        when (capture_master > 0 or refund_master > 0 or capture_visa > 0 or refund_visa > 0) then capture_master+capture_visa-refund_master-refund_visa
        when (capture_discover > 0 or refund_discover > 0) then capture_discover-refund_discover
        when (capture_paypal > 0 or refund_paypal > 0) then capture_paypal-refund_paypal
        when (capture_ltbucks > 0 or refund_ltbucks > 0) then capture_ltbucks-refund_ltbucks end transaction_line_amount,
       isnull(s_magento_payment_breakdown.spend_category , s_magento_payment_breakdown.revenue_category) transaction_line_category_id,
       concat(s_magento_payment_breakdown.OrderNum , l_magento_payment_breakdown.product_id) transaction_line_memo,
       s_magento_payment_breakdown.tax transaction_line_tax_amount,
       case when (capture_amex > 0 or refund_amex > 0) then case when (capture_amex-refund_amex) >= 0 then 'false' else 'true' end
        when (capture_master > 0 or refund_master > 0 or capture_visa > 0 or refund_visa > 0) then case when (capture_master+capture_visa-refund_master-refund_visa) >= 0 then 'false' else 'true' end
        when (capture_discover > 0 or refund_discover > 0) then case when (capture_discover-refund_discover) >= 0 then 'false' else 'true' end
        when (capture_paypal > 0 or refund_paypal > 0) then case when (capture_paypal-refund_paypal) >= 0 then 'false' else 'true' end
        when (capture_ltbucks > 0 or refund_ltbucks > 0) then case when (capture_ltbucks-refund_ltbucks) >= 0 then 'false' else 'true' end end withdrawal,
       s_magento_payment_breakdown.workday_region workday_region,
       isnull(h_magento_payment_breakdown.dv_deleted,0) dv_deleted,
       p_magento_payment_breakdown.p_magento_payment_breakdown_id,
       p_magento_payment_breakdown.dv_batch_id,
       p_magento_payment_breakdown.dv_load_date_time,
       p_magento_payment_breakdown.dv_load_end_date_time
  from dbo.h_magento_payment_breakdown
  join dbo.p_magento_payment_breakdown
    on h_magento_payment_breakdown.bk_hash = p_magento_payment_breakdown.bk_hash
  join #p_magento_payment_breakdown_insert
    on p_magento_payment_breakdown.bk_hash = #p_magento_payment_breakdown_insert.bk_hash
   and p_magento_payment_breakdown.p_magento_payment_breakdown_id = #p_magento_payment_breakdown_insert.p_magento_payment_breakdown_id
  join dbo.l_magento_payment_breakdown
    on p_magento_payment_breakdown.bk_hash = l_magento_payment_breakdown.bk_hash
   and p_magento_payment_breakdown.l_magento_payment_breakdown_id = l_magento_payment_breakdown.l_magento_payment_breakdown_id
  join dbo.s_magento_payment_breakdown
    on p_magento_payment_breakdown.bk_hash = s_magento_payment_breakdown.bk_hash
   and p_magento_payment_breakdown.s_magento_payment_breakdown_id = s_magento_payment_breakdown.s_magento_payment_breakdown_id

   
     truncate table dbo.d_magento_payment_breakdown
-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  --delete dbo.d_magento_payment_breakdown
   --where d_magento_payment_breakdown.bk_hash in (select bk_hash from #p_magento_payment_breakdown_insert)

  insert dbo.d_magento_payment_breakdown(
             bk_hash,
             fact_magento_payment_breakdown_key,
             OrderNum,
             transaction_date,
             OENum,
             club_id,
             cost_center,
             deposit,
             line_company,
             revenue_category,
             shipping_amount,
             spend_category,
             tender_type_id,
             transaction_amount,
             transaction_line_amount,
             transaction_line_category_id,
             transaction_line_memo,
             transaction_line_tax_amount,
             withdrawal,
             workday_region,
             deleted_flag,
             p_magento_payment_breakdown_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_magento_payment_breakdown_key,
         OrderNum,
         transaction_date,
         OENum,
         club_id,
         cost_center,
         deposit,
         line_company,
         revenue_category,
         shipping_amount,
         spend_category,
         tender_type_id,
         transaction_amount,
         transaction_line_amount,
         transaction_line_category_id,
         transaction_line_memo,
         transaction_line_tax_amount,
         withdrawal,
         workday_region,
         dv_deleted,
         p_magento_payment_breakdown_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_payment_breakdown)
--Done!
end
