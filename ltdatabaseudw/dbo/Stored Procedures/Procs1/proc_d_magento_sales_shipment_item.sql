CREATE PROC [dbo].[proc_d_magento_sales_shipment_item] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_shipment_item)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_shipment_item_insert') is not null drop table #p_magento_sales_shipment_item_insert
create table dbo.#p_magento_sales_shipment_item_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_shipment_item.p_magento_sales_shipment_item_id,
       p_magento_sales_shipment_item.bk_hash
  from dbo.p_magento_sales_shipment_item
 where p_magento_sales_shipment_item.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_shipment_item.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_shipment_item.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_shipment_item.bk_hash,
       p_magento_sales_shipment_item.entity_id sales_shipment_item_id,
       case when p_magento_sales_shipment_item.bk_hash in('-997', '-998', '-999') then p_magento_sales_shipment_item.bk_hash
           when l_magento_sales_shipment_item.parent_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_shipment_item.parent_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_sales_shipment_bk_hash,
       case when p_magento_sales_shipment_item.bk_hash in('-997', '-998', '-999') then p_magento_sales_shipment_item.bk_hash
           when l_magento_sales_shipment_item.product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_shipment_item.product_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_magento_product_key,
       case when p_magento_sales_shipment_item.bk_hash in ('-997', '-998', '-999') then p_magento_sales_shipment_item.bk_hash
            when l_magento_sales_shipment_item.order_item_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_shipment_item.order_item_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_order_item_key,
       case when p_magento_sales_shipment_item.bk_hash in ('-997', '-998', '-999') then p_magento_sales_shipment_item.bk_hash
            when l_magento_sales_shipment_item.parent_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_shipment_item.parent_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_shipment_key,
       l_magento_sales_shipment_item.order_item_id order_item_id,
       s_magento_sales_shipment_item.row_total row_total,
       s_magento_sales_shipment_item.description sales_shipment_item_description,
       s_magento_sales_shipment_item.name sales_shipment_item_name,
       s_magento_sales_shipment_item.price sales_shipment_item_price,
       s_magento_sales_shipment_item.qty sales_shipment_item_qty,
       l_magento_sales_shipment_item.sku sales_shipment_item_sku,
       s_magento_sales_shipment_item.weight sales_shipment_item_weight,
       isnull(h_magento_sales_shipment_item.dv_deleted,0) dv_deleted,
       p_magento_sales_shipment_item.p_magento_sales_shipment_item_id,
       p_magento_sales_shipment_item.dv_batch_id,
       p_magento_sales_shipment_item.dv_load_date_time,
       p_magento_sales_shipment_item.dv_load_end_date_time
  from dbo.h_magento_sales_shipment_item
  join dbo.p_magento_sales_shipment_item
    on h_magento_sales_shipment_item.bk_hash = p_magento_sales_shipment_item.bk_hash
  join #p_magento_sales_shipment_item_insert
    on p_magento_sales_shipment_item.bk_hash = #p_magento_sales_shipment_item_insert.bk_hash
   and p_magento_sales_shipment_item.p_magento_sales_shipment_item_id = #p_magento_sales_shipment_item_insert.p_magento_sales_shipment_item_id
  join dbo.l_magento_sales_shipment_item
    on p_magento_sales_shipment_item.bk_hash = l_magento_sales_shipment_item.bk_hash
   and p_magento_sales_shipment_item.l_magento_sales_shipment_item_id = l_magento_sales_shipment_item.l_magento_sales_shipment_item_id
  join dbo.s_magento_sales_shipment_item
    on p_magento_sales_shipment_item.bk_hash = s_magento_sales_shipment_item.bk_hash
   and p_magento_sales_shipment_item.s_magento_sales_shipment_item_id = s_magento_sales_shipment_item.s_magento_sales_shipment_item_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_shipment_item
   where d_magento_sales_shipment_item.bk_hash in (select bk_hash from #p_magento_sales_shipment_item_insert)

  insert dbo.d_magento_sales_shipment_item(
             bk_hash,
             sales_shipment_item_id,
             d_magento_sales_shipment_bk_hash,
             dim_magento_product_key,
             fact_magento_order_item_key,
             fact_magento_shipment_key,
             order_item_id,
             row_total,
             sales_shipment_item_description,
             sales_shipment_item_name,
             sales_shipment_item_price,
             sales_shipment_item_qty,
             sales_shipment_item_sku,
             sales_shipment_item_weight,
             deleted_flag,
             p_magento_sales_shipment_item_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         sales_shipment_item_id,
         d_magento_sales_shipment_bk_hash,
         dim_magento_product_key,
         fact_magento_order_item_key,
         fact_magento_shipment_key,
         order_item_id,
         row_total,
         sales_shipment_item_description,
         sales_shipment_item_name,
         sales_shipment_item_price,
         sales_shipment_item_qty,
         sales_shipment_item_sku,
         sales_shipment_item_weight,
         dv_deleted,
         p_magento_sales_shipment_item_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_shipment_item)
--Done!
end
