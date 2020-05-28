CREATE PROC [dbo].[proc_d_magento_sales_credit_memo_item] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_credit_memo_item)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_credit_memo_item_insert') is not null drop table #p_magento_sales_credit_memo_item_insert
create table dbo.#p_magento_sales_credit_memo_item_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_credit_memo_item.p_magento_sales_credit_memo_item_id,
       p_magento_sales_credit_memo_item.bk_hash
  from dbo.p_magento_sales_credit_memo_item
 where p_magento_sales_credit_memo_item.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_credit_memo_item.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_credit_memo_item.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_credit_memo_item.bk_hash,
       p_magento_sales_credit_memo_item.entity_id sales_credit_memo_item_id,
       s_magento_sales_credit_memo_item.base_cost base_cost,
       s_magento_sales_credit_memo_item.base_discount_amount base_discount_amount,
       s_magento_sales_credit_memo_item.base_discount_tax_compensation_amount base_discount_tax_compensation_amount,
       s_magento_sales_credit_memo_item.base_price base_price,
       s_magento_sales_credit_memo_item.base_price_incl_tax base_price_incl_tax,
       s_magento_sales_credit_memo_item.base_row_total base_row_total,
       s_magento_sales_credit_memo_item.base_row_total_incl_tax base_row_total_incl_tax,
       s_magento_sales_credit_memo_item.base_tax_amount base_tax_amount,
       s_magento_sales_credit_memo_item.base_weee_tax_applied_amount base_weee_tax_applied_amount,
       s_magento_sales_credit_memo_item.base_weee_tax_applied_row_amnt base_weee_tax_applied_row_amnt,
       s_magento_sales_credit_memo_item.base_weee_tax_disposition base_weee_tax_disposition,
       s_magento_sales_credit_memo_item.base_weee_tax_row_disposition base_weee_tax_row_disposition,
       case when p_magento_sales_credit_memo_item.bk_hash in('-997', '-998', '-999') then p_magento_sales_credit_memo_item.bk_hash
           when l_magento_sales_credit_memo_item.parent_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_credit_memo_item.parent_id as int) as varchar(500)),'z#@$k%&P'))),2)    end d_magento_sales_credit_memo_bk_hash,
       case when p_magento_sales_credit_memo_item.bk_hash in('-997', '-998', '-999') then p_magento_sales_credit_memo_item.bk_hash
           when l_magento_sales_credit_memo_item.product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_credit_memo_item.product_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_magento_product_key,
       s_magento_sales_credit_memo_item.discount_amount discount_amount,
       s_magento_sales_credit_memo_item.discount_tax_compensation_amount discount_tax_compensation_amount,
       case when p_magento_sales_credit_memo_item.bk_hash in ('-997', '-998', '-999') then p_magento_sales_credit_memo_item.bk_hash
            when l_magento_sales_credit_memo_item.order_item_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_credit_memo_item.order_item_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_order_item_key,
       s_magento_sales_credit_memo_item.name name,
       l_magento_sales_credit_memo_item.order_item_id order_item_id,
       s_magento_sales_credit_memo_item.price price,
       s_magento_sales_credit_memo_item.price_incl_tax price_incl_tax,
       s_magento_sales_credit_memo_item.qty qty,
       s_magento_sales_credit_memo_item.row_total row_total,
       s_magento_sales_credit_memo_item.row_total_incl_tax row_total_incl_tax,
       l_magento_sales_credit_memo_item.sku sku,
       s_magento_sales_credit_memo_item.tax_amount tax_amount,
       s_magento_sales_credit_memo_item.tax_ratio tax_ratio,
       s_magento_sales_credit_memo_item.weee_tax_applied weee_tax_applied,
       s_magento_sales_credit_memo_item.weee_tax_applied_amount weee_tax_applied_amount,
       s_magento_sales_credit_memo_item.weee_tax_applied_row_amount weee_tax_applied_row_amount,
       s_magento_sales_credit_memo_item.weee_tax_disposition weee_tax_disposition,
       s_magento_sales_credit_memo_item.weee_tax_row_disposition weee_tax_row_disposition,
       isnull(h_magento_sales_credit_memo_item.dv_deleted,0) dv_deleted,
       p_magento_sales_credit_memo_item.p_magento_sales_credit_memo_item_id,
       p_magento_sales_credit_memo_item.dv_batch_id,
       p_magento_sales_credit_memo_item.dv_load_date_time,
       p_magento_sales_credit_memo_item.dv_load_end_date_time
  from dbo.h_magento_sales_credit_memo_item
  join dbo.p_magento_sales_credit_memo_item
    on h_magento_sales_credit_memo_item.bk_hash = p_magento_sales_credit_memo_item.bk_hash
  join #p_magento_sales_credit_memo_item_insert
    on p_magento_sales_credit_memo_item.bk_hash = #p_magento_sales_credit_memo_item_insert.bk_hash
   and p_magento_sales_credit_memo_item.p_magento_sales_credit_memo_item_id = #p_magento_sales_credit_memo_item_insert.p_magento_sales_credit_memo_item_id
  join dbo.l_magento_sales_credit_memo_item
    on p_magento_sales_credit_memo_item.bk_hash = l_magento_sales_credit_memo_item.bk_hash
   and p_magento_sales_credit_memo_item.l_magento_sales_credit_memo_item_id = l_magento_sales_credit_memo_item.l_magento_sales_credit_memo_item_id
  join dbo.s_magento_sales_credit_memo_item
    on p_magento_sales_credit_memo_item.bk_hash = s_magento_sales_credit_memo_item.bk_hash
   and p_magento_sales_credit_memo_item.s_magento_sales_credit_memo_item_id = s_magento_sales_credit_memo_item.s_magento_sales_credit_memo_item_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_credit_memo_item
   where d_magento_sales_credit_memo_item.bk_hash in (select bk_hash from #p_magento_sales_credit_memo_item_insert)

  insert dbo.d_magento_sales_credit_memo_item(
             bk_hash,
             sales_credit_memo_item_id,
             base_cost,
             base_discount_amount,
             base_discount_tax_compensation_amount,
             base_price,
             base_price_incl_tax,
             base_row_total,
             base_row_total_incl_tax,
             base_tax_amount,
             base_weee_tax_applied_amount,
             base_weee_tax_applied_row_amnt,
             base_weee_tax_disposition,
             base_weee_tax_row_disposition,
             d_magento_sales_credit_memo_bk_hash,
             dim_magento_product_key,
             discount_amount,
             discount_tax_compensation_amount,
             fact_magento_order_item_key,
             name,
             order_item_id,
             price,
             price_incl_tax,
             qty,
             row_total,
             row_total_incl_tax,
             sku,
             tax_amount,
             tax_ratio,
             weee_tax_applied,
             weee_tax_applied_amount,
             weee_tax_applied_row_amount,
             weee_tax_disposition,
             weee_tax_row_disposition,
             deleted_flag,
             p_magento_sales_credit_memo_item_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         sales_credit_memo_item_id,
         base_cost,
         base_discount_amount,
         base_discount_tax_compensation_amount,
         base_price,
         base_price_incl_tax,
         base_row_total,
         base_row_total_incl_tax,
         base_tax_amount,
         base_weee_tax_applied_amount,
         base_weee_tax_applied_row_amnt,
         base_weee_tax_disposition,
         base_weee_tax_row_disposition,
         d_magento_sales_credit_memo_bk_hash,
         dim_magento_product_key,
         discount_amount,
         discount_tax_compensation_amount,
         fact_magento_order_item_key,
         name,
         order_item_id,
         price,
         price_incl_tax,
         qty,
         row_total,
         row_total_incl_tax,
         sku,
         tax_amount,
         tax_ratio,
         weee_tax_applied,
         weee_tax_applied_amount,
         weee_tax_applied_row_amount,
         weee_tax_disposition,
         weee_tax_row_disposition,
         dv_deleted,
         p_magento_sales_credit_memo_item_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_credit_memo_item)
--Done!
end
