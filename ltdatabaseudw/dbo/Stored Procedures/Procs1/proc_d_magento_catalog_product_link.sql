CREATE PROC [dbo].[proc_d_magento_catalog_product_link] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_product_link)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_catalog_product_link_insert') is not null drop table #p_magento_catalog_product_link_insert
create table dbo.#p_magento_catalog_product_link_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_product_link.p_magento_catalog_product_link_id,
       p_magento_catalog_product_link.bk_hash
  from dbo.p_magento_catalog_product_link
 where p_magento_catalog_product_link.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_catalog_product_link.dv_batch_id > @max_dv_batch_id
        or p_magento_catalog_product_link.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_product_link.bk_hash,
       p_magento_catalog_product_link.link_id link_id,
       case when p_magento_catalog_product_link.bk_hash in('-997', '-998', '-999') then p_magento_catalog_product_link.bk_hash
           when l_magento_catalog_product_link.product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_catalog_product_link.product_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_catalog_product_entity_bk_hash,
       l_magento_catalog_product_link.link_type_id link_type_id,
       l_magento_catalog_product_link.linked_product_id linked_product_id,
       isnull(h_magento_catalog_product_link.dv_deleted,0) dv_deleted,
       p_magento_catalog_product_link.p_magento_catalog_product_link_id,
       p_magento_catalog_product_link.dv_batch_id,
       p_magento_catalog_product_link.dv_load_date_time,
       p_magento_catalog_product_link.dv_load_end_date_time
  from dbo.h_magento_catalog_product_link
  join dbo.p_magento_catalog_product_link
    on h_magento_catalog_product_link.bk_hash = p_magento_catalog_product_link.bk_hash
  join #p_magento_catalog_product_link_insert
    on p_magento_catalog_product_link.bk_hash = #p_magento_catalog_product_link_insert.bk_hash
   and p_magento_catalog_product_link.p_magento_catalog_product_link_id = #p_magento_catalog_product_link_insert.p_magento_catalog_product_link_id
  join dbo.l_magento_catalog_product_link
    on p_magento_catalog_product_link.bk_hash = l_magento_catalog_product_link.bk_hash
   and p_magento_catalog_product_link.l_magento_catalog_product_link_id = l_magento_catalog_product_link.l_magento_catalog_product_link_id
  join dbo.s_magento_catalog_product_link
    on p_magento_catalog_product_link.bk_hash = s_magento_catalog_product_link.bk_hash
   and p_magento_catalog_product_link.s_magento_catalog_product_link_id = s_magento_catalog_product_link.s_magento_catalog_product_link_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_catalog_product_link
   where d_magento_catalog_product_link.bk_hash in (select bk_hash from #p_magento_catalog_product_link_insert)

  insert dbo.d_magento_catalog_product_link(
             bk_hash,
             link_id,
             d_magento_catalog_product_entity_bk_hash,
             link_type_id,
             linked_product_id,
             deleted_flag,
             p_magento_catalog_product_link_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         link_id,
         d_magento_catalog_product_entity_bk_hash,
         link_type_id,
         linked_product_id,
         dv_deleted,
         p_magento_catalog_product_link_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_product_link)
--Done!
end
