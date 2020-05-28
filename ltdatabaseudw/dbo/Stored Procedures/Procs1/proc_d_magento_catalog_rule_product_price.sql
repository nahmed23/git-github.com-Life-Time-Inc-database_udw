CREATE PROC [dbo].[proc_d_magento_catalog_rule_product_price] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_rule_product_price)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_catalog_rule_product_price_insert') is not null drop table #p_magento_catalog_rule_product_price_insert
create table dbo.#p_magento_catalog_rule_product_price_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_rule_product_price.p_magento_catalog_rule_product_price_id,
       p_magento_catalog_rule_product_price.bk_hash
  from dbo.p_magento_catalog_rule_product_price
 where p_magento_catalog_rule_product_price.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_catalog_rule_product_price.dv_batch_id > @max_dv_batch_id
        or p_magento_catalog_rule_product_price.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_rule_product_price.bk_hash,
       p_magento_catalog_rule_product_price.rule_product_price_id rule_product_price_id,
       l_magento_catalog_rule_product_price.customer_group_id customer_group_id,
       s_magento_catalog_rule_product_price.earliest_end_date earliest_end_date,
       case when p_magento_catalog_rule_product_price.bk_hash in('-997', '-998', '-999') then p_magento_catalog_rule_product_price.bk_hash
       when s_magento_catalog_rule_product_price.earliest_end_date is null then '-998'
       else convert(varchar, s_magento_catalog_rule_product_price.earliest_end_date, 112)    end earliest_end_dim_date_key,
       s_magento_catalog_rule_product_price.latest_start_date latest_start_date,
       case when p_magento_catalog_rule_product_price.bk_hash in('-997', '-998', '-999') then p_magento_catalog_rule_product_price.bk_hash
       when s_magento_catalog_rule_product_price.latest_start_date is null then '-998'
       else convert(varchar, s_magento_catalog_rule_product_price.latest_start_date, 112)    end latest_start_dim_date_key,
       l_magento_catalog_rule_product_price.product_id product_id,
       s_magento_catalog_rule_product_price.rule_date rule_date,
       case when p_magento_catalog_rule_product_price.bk_hash in('-997', '-998', '-999') then p_magento_catalog_rule_product_price.bk_hash
       when s_magento_catalog_rule_product_price.rule_date is null then '-998'
       else convert(varchar, s_magento_catalog_rule_product_price.rule_date, 112)    end rule_dim_date_key,
       s_magento_catalog_rule_product_price.rule_price rule_price,
       l_magento_catalog_rule_product_price.website_id website_id,
       isnull(h_magento_catalog_rule_product_price.dv_deleted,0) dv_deleted,
       p_magento_catalog_rule_product_price.p_magento_catalog_rule_product_price_id,
       p_magento_catalog_rule_product_price.dv_batch_id,
       p_magento_catalog_rule_product_price.dv_load_date_time,
       p_magento_catalog_rule_product_price.dv_load_end_date_time
  from dbo.h_magento_catalog_rule_product_price
  join dbo.p_magento_catalog_rule_product_price
    on h_magento_catalog_rule_product_price.bk_hash = p_magento_catalog_rule_product_price.bk_hash
  join #p_magento_catalog_rule_product_price_insert
    on p_magento_catalog_rule_product_price.bk_hash = #p_magento_catalog_rule_product_price_insert.bk_hash
   and p_magento_catalog_rule_product_price.p_magento_catalog_rule_product_price_id = #p_magento_catalog_rule_product_price_insert.p_magento_catalog_rule_product_price_id
  join dbo.l_magento_catalog_rule_product_price
    on p_magento_catalog_rule_product_price.bk_hash = l_magento_catalog_rule_product_price.bk_hash
   and p_magento_catalog_rule_product_price.l_magento_catalog_rule_product_price_id = l_magento_catalog_rule_product_price.l_magento_catalog_rule_product_price_id
  join dbo.s_magento_catalog_rule_product_price
    on p_magento_catalog_rule_product_price.bk_hash = s_magento_catalog_rule_product_price.bk_hash
   and p_magento_catalog_rule_product_price.s_magento_catalog_rule_product_price_id = s_magento_catalog_rule_product_price.s_magento_catalog_rule_product_price_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_catalog_rule_product_price
   where d_magento_catalog_rule_product_price.bk_hash in (select bk_hash from #p_magento_catalog_rule_product_price_insert)

  insert dbo.d_magento_catalog_rule_product_price(
             bk_hash,
             rule_product_price_id,
             customer_group_id,
             earliest_end_date,
             earliest_end_dim_date_key,
             latest_start_date,
             latest_start_dim_date_key,
             product_id,
             rule_date,
             rule_dim_date_key,
             rule_price,
             website_id,
             deleted_flag,
             p_magento_catalog_rule_product_price_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         rule_product_price_id,
         customer_group_id,
         earliest_end_date,
         earliest_end_dim_date_key,
         latest_start_date,
         latest_start_dim_date_key,
         product_id,
         rule_date,
         rule_dim_date_key,
         rule_price,
         website_id,
         dv_deleted,
         p_magento_catalog_rule_product_price_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_rule_product_price)
--Done!
end
