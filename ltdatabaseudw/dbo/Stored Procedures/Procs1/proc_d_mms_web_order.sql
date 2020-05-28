CREATE PROC [dbo].[proc_d_mms_web_order] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_web_order)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_web_order_insert') is not null drop table #p_mms_web_order_insert
create table dbo.#p_mms_web_order_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_web_order.p_mms_web_order_id,
       p_mms_web_order.bk_hash
  from dbo.p_mms_web_order
 where p_mms_web_order.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_web_order.dv_batch_id > @max_dv_batch_id
        or p_mms_web_order.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_web_order.bk_hash,
       p_mms_web_order.web_order_id web_order_id,
       l_mms_web_order.val_product_sales_channel_id val_product_sales_channel_id,
       p_mms_web_order.p_mms_web_order_id,
       p_mms_web_order.dv_batch_id,
       p_mms_web_order.dv_load_date_time,
       p_mms_web_order.dv_load_end_date_time
  from dbo.p_mms_web_order
  join #p_mms_web_order_insert
    on p_mms_web_order.bk_hash = #p_mms_web_order_insert.bk_hash
   and p_mms_web_order.p_mms_web_order_id = #p_mms_web_order_insert.p_mms_web_order_id
  join dbo.l_mms_web_order
    on p_mms_web_order.bk_hash = l_mms_web_order.bk_hash
   and p_mms_web_order.l_mms_web_order_id = l_mms_web_order.l_mms_web_order_id
  join dbo.s_mms_web_order
    on p_mms_web_order.bk_hash = s_mms_web_order.bk_hash
   and p_mms_web_order.s_mms_web_order_id = s_mms_web_order.s_mms_web_order_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_web_order
   where d_mms_web_order.bk_hash in (select bk_hash from #p_mms_web_order_insert)

  insert dbo.d_mms_web_order(
             bk_hash,
             web_order_id,
             val_product_sales_channel_id,
             p_mms_web_order_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         web_order_id,
         val_product_sales_channel_id,
         p_mms_web_order_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_web_order)
--Done!
end
