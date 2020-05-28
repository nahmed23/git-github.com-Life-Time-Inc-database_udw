CREATE PROC [dbo].[proc_d_ig_it_trn_order_item] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_order_item)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_trn_order_item_insert') is not null drop table #p_ig_it_trn_order_item_insert
create table dbo.#p_ig_it_trn_order_item_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_order_item.p_ig_it_trn_order_item_id,
       p_ig_it_trn_order_item.bk_hash
  from dbo.p_ig_it_trn_order_item
 where p_ig_it_trn_order_item.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_trn_order_item.dv_batch_id > @max_dv_batch_id
        or p_ig_it_trn_order_item.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_order_item.bk_hash,
       p_ig_it_trn_order_item.bk_hash fact_cafe_sales_transaction_item_key,
       p_ig_it_trn_order_item.check_seq check_seq,
       p_ig_it_trn_order_item.order_hdr_id order_hdr_id,
       case when p_ig_it_trn_order_item.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_item.bk_hash  
      when p_ig_it_trn_order_item.order_hdr_id is null then '-998'
   else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_it_trn_order_item.order_hdr_id as int) as varchar(500)),'z#@$k%&P'))),2) end d_ig_it_trn_order_header_bk_hash,
       case when p_ig_it_trn_order_item.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_item.bk_hash
          when l_ig_it_trn_order_item.discoup_id is null then '-998'
          else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_it_trn_order_item.discoup_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_discount_coupon_key,
       case when p_ig_it_trn_order_item.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_item.bk_hash  
      when l_ig_it_trn_order_item.menu_item_id is null then '-998'
   else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ig_it_trn_order_item.menu_item_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_product_key,
       s_ig_it_trn_order_item.discount_amt item_discount_amount,
       case when l_ig_it_trn_order_item.void_reason_id = 254 then -1 * s_ig_it_trn_order_item.item_qty else s_ig_it_trn_order_item.item_qty end item_quantity,
       case when l_ig_it_trn_order_item.void_reason_id = 254 then 'Y'
   else 'N' end item_refund_flag,
       sign(case when l_ig_it_trn_order_item.void_reason_id = 254 then -1 * s_ig_it_trn_order_item.item_qty else s_ig_it_trn_order_item.item_qty end) * abs(isnull(s_ig_it_trn_order_item.sales_amt_gross,0)) item_sales_amount_gross,
       sign(case when l_ig_it_trn_order_item.void_reason_id = 254 then -1 * s_ig_it_trn_order_item.item_qty else s_ig_it_trn_order_item.item_qty end) * abs(isnull(s_ig_it_trn_order_item.sales_amt_gross,0) - isnull(s_ig_it_trn_order_item.discount_amt,0)) item_sales_dollar_amount_excluding_tax,
       s_ig_it_trn_order_item.tax_amt_incl_sales item_tax_amount,
       case when l_ig_it_trn_order_item.void_reason_id >= 1 and l_ig_it_trn_order_item.void_reason_id <= 253 then 'Y'
     else 'N' end item_voided_flag,
       isnull(h_ig_it_trn_order_item.dv_deleted,0) dv_deleted,
       p_ig_it_trn_order_item.p_ig_it_trn_order_item_id,
       p_ig_it_trn_order_item.dv_batch_id,
       p_ig_it_trn_order_item.dv_load_date_time,
       p_ig_it_trn_order_item.dv_load_end_date_time
  from dbo.h_ig_it_trn_order_item
  join dbo.p_ig_it_trn_order_item
    on h_ig_it_trn_order_item.bk_hash = p_ig_it_trn_order_item.bk_hash
  join #p_ig_it_trn_order_item_insert
    on p_ig_it_trn_order_item.bk_hash = #p_ig_it_trn_order_item_insert.bk_hash
   and p_ig_it_trn_order_item.p_ig_it_trn_order_item_id = #p_ig_it_trn_order_item_insert.p_ig_it_trn_order_item_id
  join dbo.l_ig_it_trn_order_item
    on p_ig_it_trn_order_item.bk_hash = l_ig_it_trn_order_item.bk_hash
   and p_ig_it_trn_order_item.l_ig_it_trn_order_item_id = l_ig_it_trn_order_item.l_ig_it_trn_order_item_id
  join dbo.s_ig_it_trn_order_item
    on p_ig_it_trn_order_item.bk_hash = s_ig_it_trn_order_item.bk_hash
   and p_ig_it_trn_order_item.s_ig_it_trn_order_item_id = s_ig_it_trn_order_item.s_ig_it_trn_order_item_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_trn_order_item
   where d_ig_it_trn_order_item.bk_hash in (select bk_hash from #p_ig_it_trn_order_item_insert)

  insert dbo.d_ig_it_trn_order_item(
             bk_hash,
             fact_cafe_sales_transaction_item_key,
             check_seq,
             order_hdr_id,
             d_ig_it_trn_order_header_bk_hash,
             dim_cafe_discount_coupon_key,
             dim_cafe_product_key,
             item_discount_amount,
             item_quantity,
             item_refund_flag,
             item_sales_amount_gross,
             item_sales_dollar_amount_excluding_tax,
             item_tax_amount,
             item_voided_flag,
             deleted_flag,
             p_ig_it_trn_order_item_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_cafe_sales_transaction_item_key,
         check_seq,
         order_hdr_id,
         d_ig_it_trn_order_header_bk_hash,
         dim_cafe_discount_coupon_key,
         dim_cafe_product_key,
         item_discount_amount,
         item_quantity,
         item_refund_flag,
         item_sales_amount_gross,
         item_sales_dollar_amount_excluding_tax,
         item_tax_amount,
         item_voided_flag,
         dv_deleted,
         p_ig_it_trn_order_item_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_order_item)
--Done!
end
