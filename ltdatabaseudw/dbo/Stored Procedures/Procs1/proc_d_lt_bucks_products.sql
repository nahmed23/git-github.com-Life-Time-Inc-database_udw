CREATE PROC [dbo].[proc_d_lt_bucks_products] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_products)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_lt_bucks_products_insert') is not null drop table #p_lt_bucks_products_insert
create table dbo.#p_lt_bucks_products_insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_products.p_lt_bucks_products_id,
       p_lt_bucks_products.bk_hash
  from dbo.p_lt_bucks_products
 where p_lt_bucks_products.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_lt_bucks_products.dv_batch_id > @max_dv_batch_id
        or p_lt_bucks_products.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_products.bk_hash,
       p_lt_bucks_products.bk_hash dim_products_key,
       p_lt_bucks_products.product_id product_id,
       isnull(s_lt_bucks_products.date_created,'1900.01.01') created_date_time,
       isnull(s_lt_bucks_products.date_updated,'1900.01.01') date_updated,
       isnull(s_lt_bucks_products.last_modified_timestamp,'1900.01.01') last_modified_timestamp,
       isnull(s_lt_bucks_products.price,'0') price,
       case when s_lt_bucks_products.active = 1
                        then 'Y'
                   else 'N'
               end product_active_flag,
       isnull(s_lt_bucks_products.product_desc,'') product_description,
       case when s_lt_bucks_products.is_deleted = 1
                        then 'Y'
                   else 'N'
               end product_is_soft_deleted_flag,
       isnull(s_lt_bucks_products.name,'') product_name,
       isnull(s_lt_bucks_products.per,'') product_per,
       isnull(s_lt_bucks_products.sku,'') sku,
       p_lt_bucks_products.p_lt_bucks_products_id,
       p_lt_bucks_products.dv_batch_id,
       p_lt_bucks_products.dv_load_date_time,
       p_lt_bucks_products.dv_load_end_date_time
  from dbo.h_lt_bucks_products
  join dbo.p_lt_bucks_products
    on h_lt_bucks_products.bk_hash = p_lt_bucks_products.bk_hash  join #p_lt_bucks_products_insert
    on p_lt_bucks_products.bk_hash = #p_lt_bucks_products_insert.bk_hash
   and p_lt_bucks_products.p_lt_bucks_products_id = #p_lt_bucks_products_insert.p_lt_bucks_products_id
  join dbo.l_lt_bucks_products
    on p_lt_bucks_products.bk_hash = l_lt_bucks_products.bk_hash
   and p_lt_bucks_products.l_lt_bucks_products_id = l_lt_bucks_products.l_lt_bucks_products_id
  join dbo.s_lt_bucks_products
    on p_lt_bucks_products.bk_hash = s_lt_bucks_products.bk_hash
   and p_lt_bucks_products.s_lt_bucks_products_id = s_lt_bucks_products.s_lt_bucks_products_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_lt_bucks_products
   where d_lt_bucks_products.bk_hash in (select bk_hash from #p_lt_bucks_products_insert)

  insert dbo.d_lt_bucks_products(
             bk_hash,
             dim_products_key,
             product_id,
             created_date_time,
             date_updated,
             last_modified_timestamp,
             price,
             product_active_flag,
             product_description,
             product_is_soft_deleted_flag,
             product_name,
             product_per,
             sku,
             p_lt_bucks_products_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_products_key,
         product_id,
         created_date_time,
         date_updated,
         last_modified_timestamp,
         price,
         product_active_flag,
         product_description,
         product_is_soft_deleted_flag,
         product_name,
         product_per,
         sku,
         p_lt_bucks_products_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_products)
--Done!
end
