CREATE PROC [dbo].[proc_d_hybris_all_products] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_hybris_all_products)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_hybris_all_products_insert') is not null drop table #p_hybris_all_products_insert
create table dbo.#p_hybris_all_products_insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_all_products.p_hybris_all_products_id,
       p_hybris_all_products.bk_hash
  from dbo.p_hybris_all_products
 where p_hybris_all_products.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_hybris_all_products.dv_batch_id > @max_dv_batch_id
        or p_hybris_all_products.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_all_products.bk_hash,
       p_hybris_all_products.bk_hash dim_hybris_product_key,
       p_hybris_all_products.code code,
       s_hybris_all_products.accept_lt_bucks_flag accept_lt_bucks_flag,
       l_hybris_all_products.active_catalog_version active_catalog_version,
       s_hybris_all_products.auto_ship_flag auto_ship_flag,
       s_hybris_all_products.caption caption,
       s_hybris_all_products.catalog_name catalog_name,
       l_hybris_all_products.catalog_version catalog_version,
       s_hybris_all_products.catalog_version_name catalog_version_name,
       s_hybris_all_products.created_ts created_ts,
       s_hybris_all_products.creation_time creation_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast('E-Commerce' as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast('E-Commerce' as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast('E-Commerce' as varchar(255)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast('' as varchar(255)),'z#@$k%&P'))),2) default_dim_reporting_hierarchy_key,
       s_hybris_all_products.description description,
       s_hybris_all_products.e_gift_card_flag e_gift_card_flag,
       s_hybris_all_products.ean ean,
       s_hybris_all_products.electronic_shipping_flag electronic_shipping_flag,
       s_hybris_all_products.fulfillment_partner fulfillment_partner,
       s_hybris_all_products.lt_bucks_earned lt_bucks_earned,
       s_hybris_all_products.ltf_offer_flag ltf_offer_flag,
       s_hybris_all_products.ltf_only_product ltf_only_product,
       s_hybris_all_products.modified_time modified_time,
       s_hybris_all_products.name name,
       s_hybris_all_products.offer_external_link_flag offer_external_link_flag,
       s_hybris_all_products.offer_link offer_link,
       s_hybris_all_products.offline_datetime offline_datetime,
       s_hybris_all_products.online_datetime online_datetime,
       s_hybris_all_products.product_category product_category,
       s_hybris_all_products.product_cost product_cost,
       s_hybris_all_products.product_height product_height,
       s_hybris_all_products.product_length product_length,
       s_hybris_all_products.product_stock_level product_stock_level,
       s_hybris_all_products.product_stock_status product_stock_status,
       s_hybris_all_products.product_sub_category product_sub_category,
       s_hybris_all_products.product_type product_type,
       s_hybris_all_products.product_width product_width,
       s_hybris_all_products.summary summary,
       s_hybris_all_products.weight weight,
       isnull(h_hybris_all_products.dv_deleted,0) dv_deleted,
       p_hybris_all_products.p_hybris_all_products_id,
       p_hybris_all_products.dv_batch_id,
       p_hybris_all_products.dv_load_date_time,
       p_hybris_all_products.dv_load_end_date_time
  from dbo.h_hybris_all_products
  join dbo.p_hybris_all_products
    on h_hybris_all_products.bk_hash = p_hybris_all_products.bk_hash
  join #p_hybris_all_products_insert
    on p_hybris_all_products.bk_hash = #p_hybris_all_products_insert.bk_hash
   and p_hybris_all_products.p_hybris_all_products_id = #p_hybris_all_products_insert.p_hybris_all_products_id
  join dbo.l_hybris_all_products
    on p_hybris_all_products.bk_hash = l_hybris_all_products.bk_hash
   and p_hybris_all_products.l_hybris_all_products_id = l_hybris_all_products.l_hybris_all_products_id
  join dbo.s_hybris_all_products
    on p_hybris_all_products.bk_hash = s_hybris_all_products.bk_hash
   and p_hybris_all_products.s_hybris_all_products_id = s_hybris_all_products.s_hybris_all_products_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_hybris_all_products
   where d_hybris_all_products.bk_hash in (select bk_hash from #p_hybris_all_products_insert)

  insert dbo.d_hybris_all_products(
             bk_hash,
             dim_hybris_product_key,
             code,
             accept_lt_bucks_flag,
             active_catalog_version,
             auto_ship_flag,
             caption,
             catalog_name,
             catalog_version,
             catalog_version_name,
             created_ts,
             creation_time,
             default_dim_reporting_hierarchy_key,
             description,
             e_gift_card_flag,
             ean,
             electronic_shipping_flag,
             fulfillment_partner,
             lt_bucks_earned,
             ltf_offer_flag,
             ltf_only_product,
             modified_time,
             name,
             offer_external_link_flag,
             offer_link,
             offline_datetime,
             online_datetime,
             product_category,
             product_cost,
             product_height,
             product_length,
             product_stock_level,
             product_stock_status,
             product_sub_category,
             product_type,
             product_width,
             summary,
             weight,
             deleted_flag,
             p_hybris_all_products_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_hybris_product_key,
         code,
         accept_lt_bucks_flag,
         active_catalog_version,
         auto_ship_flag,
         caption,
         catalog_name,
         catalog_version,
         catalog_version_name,
         created_ts,
         creation_time,
         default_dim_reporting_hierarchy_key,
         description,
         e_gift_card_flag,
         ean,
         electronic_shipping_flag,
         fulfillment_partner,
         lt_bucks_earned,
         ltf_offer_flag,
         ltf_only_product,
         modified_time,
         name,
         offer_external_link_flag,
         offer_link,
         offline_datetime,
         online_datetime,
         product_category,
         product_cost,
         product_height,
         product_length,
         product_stock_level,
         product_stock_status,
         product_sub_category,
         product_type,
         product_width,
         summary,
         weight,
         dv_deleted,
         p_hybris_all_products_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_hybris_all_products)
--Done!
end
