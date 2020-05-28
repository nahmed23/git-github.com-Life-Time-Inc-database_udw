CREATE PROC [dbo].[proc_d_magento_catalog_product_entity_int] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_product_entity_int)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_catalog_product_entity_int_insert') is not null drop table #p_magento_catalog_product_entity_int_insert
create table dbo.#p_magento_catalog_product_entity_int_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_product_entity_int.p_magento_catalog_product_entity_int_id,
       p_magento_catalog_product_entity_int.bk_hash
  from dbo.p_magento_catalog_product_entity_int
 where p_magento_catalog_product_entity_int.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_catalog_product_entity_int.dv_batch_id > @max_dv_batch_id
        or p_magento_catalog_product_entity_int.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_product_entity_int.bk_hash,
       p_magento_catalog_product_entity_int.value_id value_id,
       case when p_magento_catalog_product_entity_int.bk_hash in('-997', '-998', '-999') then p_magento_catalog_product_entity_int.bk_hash
           when l_magento_catalog_product_entity_int.attribute_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_catalog_product_entity_int.attribute_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_catalog_eav_attribute_bk_hash,
       case when p_magento_catalog_product_entity_int.bk_hash in('-997', '-998', '-999') then p_magento_catalog_product_entity_int.bk_hash
           when l_magento_catalog_product_entity_int.row_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_catalog_product_entity_int.row_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_catalog_product_entity_bk_hash,
       l_magento_catalog_product_entity_int.store_id store_id,
       s_magento_catalog_product_entity_int.value [value],
       isnull(h_magento_catalog_product_entity_int.dv_deleted,0) dv_deleted,
       p_magento_catalog_product_entity_int.p_magento_catalog_product_entity_int_id,
       p_magento_catalog_product_entity_int.dv_batch_id,
       p_magento_catalog_product_entity_int.dv_load_date_time,
       p_magento_catalog_product_entity_int.dv_load_end_date_time
  from dbo.h_magento_catalog_product_entity_int
  join dbo.p_magento_catalog_product_entity_int
    on h_magento_catalog_product_entity_int.bk_hash = p_magento_catalog_product_entity_int.bk_hash
  join #p_magento_catalog_product_entity_int_insert
    on p_magento_catalog_product_entity_int.bk_hash = #p_magento_catalog_product_entity_int_insert.bk_hash
   and p_magento_catalog_product_entity_int.p_magento_catalog_product_entity_int_id = #p_magento_catalog_product_entity_int_insert.p_magento_catalog_product_entity_int_id
  join dbo.l_magento_catalog_product_entity_int
    on p_magento_catalog_product_entity_int.bk_hash = l_magento_catalog_product_entity_int.bk_hash
   and p_magento_catalog_product_entity_int.l_magento_catalog_product_entity_int_id = l_magento_catalog_product_entity_int.l_magento_catalog_product_entity_int_id
  join dbo.s_magento_catalog_product_entity_int
    on p_magento_catalog_product_entity_int.bk_hash = s_magento_catalog_product_entity_int.bk_hash
   and p_magento_catalog_product_entity_int.s_magento_catalog_product_entity_int_id = s_magento_catalog_product_entity_int.s_magento_catalog_product_entity_int_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_catalog_product_entity_int
   where d_magento_catalog_product_entity_int.bk_hash in (select bk_hash from #p_magento_catalog_product_entity_int_insert)

  insert dbo.d_magento_catalog_product_entity_int(
             bk_hash,
             value_id,
             d_magento_catalog_eav_attribute_bk_hash,
             d_magento_catalog_product_entity_bk_hash,
             store_id,
             [value],
             deleted_flag,
             p_magento_catalog_product_entity_int_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         value_id,
         d_magento_catalog_eav_attribute_bk_hash,
         d_magento_catalog_product_entity_bk_hash,
         store_id,
         [value],
         dv_deleted,
         p_magento_catalog_product_entity_int_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_product_entity_int)
--Done!
end
