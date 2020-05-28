CREATE PROC [dbo].[proc_d_mms_tran_item_discount] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_tran_item_discount)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_tran_item_discount_insert') is not null drop table #p_mms_tran_item_discount_insert
create table dbo.#p_mms_tran_item_discount_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_tran_item_discount.p_mms_tran_item_discount_id,
       p_mms_tran_item_discount.bk_hash
  from dbo.p_mms_tran_item_discount
 where p_mms_tran_item_discount.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_tran_item_discount.dv_batch_id > @max_dv_batch_id
        or p_mms_tran_item_discount.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_tran_item_discount.bk_hash,
       p_mms_tran_item_discount.bk_hash fact_mms_sales_transaction_discount_key,
       p_mms_tran_item_discount.tran_item_discount_id tran_item_discount_id,
       s_mms_tran_item_discount.applied_discount_amount applied_discount_amount,
       s_mms_tran_item_discount.inserted_date_time inserted_date_time,
       isnull(l_mms_tran_item_discount.pricing_discount_id,-998) pricing_discount_id,
       s_mms_tran_item_discount.promotion_code promotion_code,
       isnull(l_mms_tran_item_discount.tran_item_id,-998) tran_item_id,
       l_mms_tran_item_discount.val_discount_reason_id val_discount_reason_id,
       h_mms_tran_item_discount.dv_deleted,
       p_mms_tran_item_discount.p_mms_tran_item_discount_id,
       p_mms_tran_item_discount.dv_batch_id,
       p_mms_tran_item_discount.dv_load_date_time,
       p_mms_tran_item_discount.dv_load_end_date_time
  from dbo.h_mms_tran_item_discount
  join dbo.p_mms_tran_item_discount
    on h_mms_tran_item_discount.bk_hash = p_mms_tran_item_discount.bk_hash  join #p_mms_tran_item_discount_insert
    on p_mms_tran_item_discount.bk_hash = #p_mms_tran_item_discount_insert.bk_hash
   and p_mms_tran_item_discount.p_mms_tran_item_discount_id = #p_mms_tran_item_discount_insert.p_mms_tran_item_discount_id
  join dbo.l_mms_tran_item_discount
    on p_mms_tran_item_discount.bk_hash = l_mms_tran_item_discount.bk_hash
   and p_mms_tran_item_discount.l_mms_tran_item_discount_id = l_mms_tran_item_discount.l_mms_tran_item_discount_id
  join dbo.s_mms_tran_item_discount
    on p_mms_tran_item_discount.bk_hash = s_mms_tran_item_discount.bk_hash
   and p_mms_tran_item_discount.s_mms_tran_item_discount_id = s_mms_tran_item_discount.s_mms_tran_item_discount_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_tran_item_discount
   where d_mms_tran_item_discount.bk_hash in (select bk_hash from #p_mms_tran_item_discount_insert)

  insert dbo.d_mms_tran_item_discount(
             bk_hash,
             fact_mms_sales_transaction_discount_key,
             tran_item_discount_id,
             applied_discount_amount,
             inserted_date_time,
             pricing_discount_id,
             promotion_code,
             tran_item_id,
             val_discount_reason_id,
             deleted_flag,
             p_mms_tran_item_discount_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_sales_transaction_discount_key,
         tran_item_discount_id,
         applied_discount_amount,
         inserted_date_time,
         pricing_discount_id,
         promotion_code,
         tran_item_id,
         val_discount_reason_id,
         dv_deleted,
         p_mms_tran_item_discount_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_tran_item_discount)
--Done!
end
