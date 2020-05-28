CREATE PROC [dbo].[proc_d_magento_catalog_rule_product] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_rule_product)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_catalog_rule_product_insert') is not null drop table #p_magento_catalog_rule_product_insert
create table dbo.#p_magento_catalog_rule_product_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_rule_product.p_magento_catalog_rule_product_id,
       p_magento_catalog_rule_product.bk_hash
  from dbo.p_magento_catalog_rule_product
 where p_magento_catalog_rule_product.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_catalog_rule_product.dv_batch_id > @max_dv_batch_id
        or p_magento_catalog_rule_product.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_rule_product.bk_hash,
       p_magento_catalog_rule_product.rule_product_id rule_product_id,
       s_magento_catalog_rule_product.action_amount action_amount,
       s_magento_catalog_rule_product.action_operator action_operator,
       s_magento_catalog_rule_product.action_stop action_stop,
       case when p_magento_catalog_rule_product.bk_hash in ('-997', '-998', '-999') then p_magento_catalog_rule_product.bk_hash
           when s_magento_catalog_rule_product.from_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_catalog_rule_product.from_time,114), 1, 5),':','') end catalog_rule_product_from_dim_time_key,
       case when p_magento_catalog_rule_product.bk_hash in ('-997', '-998', '-999') then p_magento_catalog_rule_product.bk_hash
           when s_magento_catalog_rule_product.to_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_catalog_rule_product.to_time,114), 1, 5),':','') end catalog_rule_product_to_dim_time_key,
       l_magento_catalog_rule_product.customer_group_id customer_group_id,
       l_magento_catalog_rule_product.product_id product_id,
       l_magento_catalog_rule_product.rule_id rule_id,
       s_magento_catalog_rule_product.sort_order sort_order,
       s_magento_catalog_rule_product.website_id website_id,
       isnull(h_magento_catalog_rule_product.dv_deleted,0) dv_deleted,
       p_magento_catalog_rule_product.p_magento_catalog_rule_product_id,
       p_magento_catalog_rule_product.dv_batch_id,
       p_magento_catalog_rule_product.dv_load_date_time,
       p_magento_catalog_rule_product.dv_load_end_date_time
  from dbo.h_magento_catalog_rule_product
  join dbo.p_magento_catalog_rule_product
    on h_magento_catalog_rule_product.bk_hash = p_magento_catalog_rule_product.bk_hash
  join #p_magento_catalog_rule_product_insert
    on p_magento_catalog_rule_product.bk_hash = #p_magento_catalog_rule_product_insert.bk_hash
   and p_magento_catalog_rule_product.p_magento_catalog_rule_product_id = #p_magento_catalog_rule_product_insert.p_magento_catalog_rule_product_id
  join dbo.l_magento_catalog_rule_product
    on p_magento_catalog_rule_product.bk_hash = l_magento_catalog_rule_product.bk_hash
   and p_magento_catalog_rule_product.l_magento_catalog_rule_product_id = l_magento_catalog_rule_product.l_magento_catalog_rule_product_id
  join dbo.s_magento_catalog_rule_product
    on p_magento_catalog_rule_product.bk_hash = s_magento_catalog_rule_product.bk_hash
   and p_magento_catalog_rule_product.s_magento_catalog_rule_product_id = s_magento_catalog_rule_product.s_magento_catalog_rule_product_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_catalog_rule_product
   where d_magento_catalog_rule_product.bk_hash in (select bk_hash from #p_magento_catalog_rule_product_insert)

  insert dbo.d_magento_catalog_rule_product(
             bk_hash,
             rule_product_id,
             action_amount,
             action_operator,
             action_stop,
             catalog_rule_product_from_dim_time_key,
             catalog_rule_product_to_dim_time_key,
             customer_group_id,
             product_id,
             rule_id,
             sort_order,
             website_id,
             deleted_flag,
             p_magento_catalog_rule_product_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         rule_product_id,
         action_amount,
         action_operator,
         action_stop,
         catalog_rule_product_from_dim_time_key,
         catalog_rule_product_to_dim_time_key,
         customer_group_id,
         product_id,
         rule_id,
         sort_order,
         website_id,
         dv_deleted,
         p_magento_catalog_rule_product_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_rule_product)
--Done!
end
