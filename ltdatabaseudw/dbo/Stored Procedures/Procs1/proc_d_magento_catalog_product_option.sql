CREATE PROC [dbo].[proc_d_magento_catalog_product_option] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_product_option)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_catalog_product_option_insert') is not null drop table #p_magento_catalog_product_option_insert
create table dbo.#p_magento_catalog_product_option_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_product_option.p_magento_catalog_product_option_id,
       p_magento_catalog_product_option.bk_hash
  from dbo.p_magento_catalog_product_option
 where p_magento_catalog_product_option.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_catalog_product_option.dv_batch_id > @max_dv_batch_id
        or p_magento_catalog_product_option.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_product_option.bk_hash,
       p_magento_catalog_product_option.option_id option_id,
       s_magento_catalog_product_option.type catalog_product_option_type,
       case when p_magento_catalog_product_option.bk_hash in('-997', '-998', '-999') then p_magento_catalog_product_option.bk_hash
           when l_magento_catalog_product_option.product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_catalog_product_option.product_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_catalog_category_product_bk_hash,
       s_magento_catalog_product_option.file_extension file_extension,
       s_magento_catalog_product_option.image_size_x image_size_x,
       s_magento_catalog_product_option.image_size_y image_size_y,
       case when s_magento_catalog_product_option.is_require = 1 then 'Y'        else 'N'  end is_requires_flag,
       s_magento_catalog_product_option.max_characters max_characters,
       l_magento_catalog_product_option.sku sku,
       s_magento_catalog_product_option.sort_order sort_order,
       isnull(h_magento_catalog_product_option.dv_deleted,0) dv_deleted,
       p_magento_catalog_product_option.p_magento_catalog_product_option_id,
       p_magento_catalog_product_option.dv_batch_id,
       p_magento_catalog_product_option.dv_load_date_time,
       p_magento_catalog_product_option.dv_load_end_date_time
  from dbo.h_magento_catalog_product_option
  join dbo.p_magento_catalog_product_option
    on h_magento_catalog_product_option.bk_hash = p_magento_catalog_product_option.bk_hash
  join #p_magento_catalog_product_option_insert
    on p_magento_catalog_product_option.bk_hash = #p_magento_catalog_product_option_insert.bk_hash
   and p_magento_catalog_product_option.p_magento_catalog_product_option_id = #p_magento_catalog_product_option_insert.p_magento_catalog_product_option_id
  join dbo.l_magento_catalog_product_option
    on p_magento_catalog_product_option.bk_hash = l_magento_catalog_product_option.bk_hash
   and p_magento_catalog_product_option.l_magento_catalog_product_option_id = l_magento_catalog_product_option.l_magento_catalog_product_option_id
  join dbo.s_magento_catalog_product_option
    on p_magento_catalog_product_option.bk_hash = s_magento_catalog_product_option.bk_hash
   and p_magento_catalog_product_option.s_magento_catalog_product_option_id = s_magento_catalog_product_option.s_magento_catalog_product_option_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_catalog_product_option
   where d_magento_catalog_product_option.bk_hash in (select bk_hash from #p_magento_catalog_product_option_insert)

  insert dbo.d_magento_catalog_product_option(
             bk_hash,
             option_id,
             catalog_product_option_type,
             d_magento_catalog_category_product_bk_hash,
             file_extension,
             image_size_x,
             image_size_y,
             is_requires_flag,
             max_characters,
             sku,
             sort_order,
             deleted_flag,
             p_magento_catalog_product_option_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         option_id,
         catalog_product_option_type,
         d_magento_catalog_category_product_bk_hash,
         file_extension,
         image_size_x,
         image_size_y,
         is_requires_flag,
         max_characters,
         sku,
         sort_order,
         dv_deleted,
         p_magento_catalog_product_option_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_product_option)
--Done!
end
