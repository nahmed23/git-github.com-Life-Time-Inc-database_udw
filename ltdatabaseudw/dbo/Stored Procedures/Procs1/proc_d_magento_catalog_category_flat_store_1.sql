CREATE PROC [dbo].[proc_d_magento_catalog_category_flat_store_1] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_category_flat_store_1)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_catalog_category_flat_store_1_insert') is not null drop table #p_magento_catalog_category_flat_store_1_insert
create table dbo.#p_magento_catalog_category_flat_store_1_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_category_flat_store_1.p_magento_catalog_category_flat_store_1_id,
       p_magento_catalog_category_flat_store_1.bk_hash
  from dbo.p_magento_catalog_category_flat_store_1
 where p_magento_catalog_category_flat_store_1.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_catalog_category_flat_store_1.dv_batch_id > @max_dv_batch_id
        or p_magento_catalog_category_flat_store_1.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_category_flat_store_1.bk_hash,
       p_magento_catalog_category_flat_store_1.entity_id catalog_category_flat_store_id,
       s_magento_catalog_category_flat_store_1.all_children all_children,
       l_magento_catalog_category_flat_store_1.attribute_set_id attribute_set_id,
       s_magento_catalog_category_flat_store_1.automatic_sorting automatic_sorting,
       s_magento_catalog_category_flat_store_1.available_sort_by available_sort_by,
       s_magento_catalog_category_flat_store_1.children children,
       s_magento_catalog_category_flat_store_1.children_count children_count,
       s_magento_catalog_category_flat_store_1.created_at created_at,
       case when p_magento_catalog_category_flat_store_1.bk_hash in('-997', '-998', '-999') then p_magento_catalog_category_flat_store_1.bk_hash
           when s_magento_catalog_category_flat_store_1.created_at is null then '-998'
        else convert(varchar, s_magento_catalog_category_flat_store_1.created_at, 112)    end created_dim_date_key,
       case when p_magento_catalog_category_flat_store_1.bk_hash in ('-997','-998','-999') then p_magento_catalog_category_flat_store_1.bk_hash
       when s_magento_catalog_category_flat_store_1.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_catalog_category_flat_store_1.created_at,114), 1, 5),':','') end created_dim_time_key,
       s_magento_catalog_category_flat_store_1.created_in created_in,
       s_magento_catalog_category_flat_store_1.custom_apply_to_products custom_apply_to_products,
       s_magento_catalog_category_flat_store_1.custom_design custom_design,
       s_magento_catalog_category_flat_store_1.custom_design_from custom_design_from,
       case when p_magento_catalog_category_flat_store_1.bk_hash in('-997', '-998', '-999') then p_magento_catalog_category_flat_store_1.bk_hash
           when s_magento_catalog_category_flat_store_1.custom_design_from is null then '-998'
        else convert(varchar, s_magento_catalog_category_flat_store_1.custom_design_from, 112)    end custom_design_from_dim_date_key,
       case when p_magento_catalog_category_flat_store_1.bk_hash in ('-997','-998','-999') then p_magento_catalog_category_flat_store_1.bk_hash
       when s_magento_catalog_category_flat_store_1.custom_design_from is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_catalog_category_flat_store_1.custom_design_from,114), 1, 5),':','') end custom_design_from_dim_time_key,
       s_magento_catalog_category_flat_store_1.custom_design_to custom_design_to,
       case when p_magento_catalog_category_flat_store_1.bk_hash in('-997', '-998', '-999') then p_magento_catalog_category_flat_store_1.bk_hash
           when s_magento_catalog_category_flat_store_1.custom_design_to is null then '-998'
        else convert(varchar, s_magento_catalog_category_flat_store_1.custom_design_to, 112)    end custom_design_to_dim_date_key,
       case when p_magento_catalog_category_flat_store_1.bk_hash in ('-997','-998','-999') then p_magento_catalog_category_flat_store_1.bk_hash
       when s_magento_catalog_category_flat_store_1.custom_design_to is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_catalog_category_flat_store_1.custom_design_to,114), 1, 5),':','') end custom_design_to_dim_time_key,
       s_magento_catalog_category_flat_store_1.custom_layout_update custom_layout_update,
       s_magento_catalog_category_flat_store_1.custom_use_parent_settings custom_use_parent_settings,
       s_magento_catalog_category_flat_store_1.default_sort_by default_sort_by,
       s_magento_catalog_category_flat_store_1.description description,
       case when p_magento_catalog_category_flat_store_1.bk_hash in ('-997','-998','-999') then p_magento_catalog_category_flat_store_1.bk_hash     
         when l_magento_catalog_category_flat_store_1.entity_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_catalog_category_flat_store_1.entity_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_magento_category_key,
       s_magento_catalog_category_flat_store_1.display_mode display_mode,
       s_magento_catalog_category_flat_store_1.featured_category featured_category,
       s_magento_catalog_category_flat_store_1.featured_image featured_image,
       s_magento_catalog_category_flat_store_1.filter_price_range filter_price_range,
       case when s_magento_catalog_category_flat_store_1.include_in_menu= 1 then 'Y' else 'N' end include_in_menu_flag,
       case when s_magento_catalog_category_flat_store_1.is_active= 1 then 'Y' else 'N' end is_active_flag,
       case when s_magento_catalog_category_flat_store_1.is_anchor= 1 then 'Y' else 'N' end is_anchor_flag,
       case when s_magento_catalog_category_flat_store_1.is_virtual_category= 1 then 'Y' else 'N' end is_virtual_category_flag,
       s_magento_catalog_category_flat_store_1.landing_page landing_page,
       s_magento_catalog_category_flat_store_1.level level,
       s_magento_catalog_category_flat_store_1.meta_description meta_description,
       s_magento_catalog_category_flat_store_1.meta_keywords meta_keywords,
       s_magento_catalog_category_flat_store_1.meta_title meta_title,
       s_magento_catalog_category_flat_store_1.name name,
       s_magento_catalog_category_flat_store_1.page_layout page_layout,
       l_magento_catalog_category_flat_store_1.parent_id parent_id,
       s_magento_catalog_category_flat_store_1.path path,
       s_magento_catalog_category_flat_store_1.path_in_store path_in_store,
       s_magento_catalog_category_flat_store_1.position position,
       l_magento_catalog_category_flat_store_1.row_id row_id,
       s_magento_catalog_category_flat_store_1.short_description short_description,
       l_magento_catalog_category_flat_store_1.store_id store_id,
       s_magento_catalog_category_flat_store_1.thumbnail thumbnail,
       s_magento_catalog_category_flat_store_1.updated_at updated_at,
       case when p_magento_catalog_category_flat_store_1.bk_hash in('-997', '-998', '-999') then p_magento_catalog_category_flat_store_1.bk_hash
           when s_magento_catalog_category_flat_store_1.updated_at is null then '-998'
        else convert(varchar, s_magento_catalog_category_flat_store_1.updated_at, 112)    end updated_dim_date_key,
       case when p_magento_catalog_category_flat_store_1.bk_hash in ('-997','-998','-999') then p_magento_catalog_category_flat_store_1.bk_hash
       when s_magento_catalog_category_flat_store_1.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_catalog_category_flat_store_1.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       s_magento_catalog_category_flat_store_1.updated_in updated_in,
       s_magento_catalog_category_flat_store_1.url_key url_key,
       s_magento_catalog_category_flat_store_1.url_path url_path,
       s_magento_catalog_category_flat_store_1.use_name_in_product_search use_name_in_product_search,
       s_magento_catalog_category_flat_store_1.virtual_category_root virtual_category_root,
       s_magento_catalog_category_flat_store_1.virtual_rule virtual_rule,
       isnull(h_magento_catalog_category_flat_store_1.dv_deleted,0) dv_deleted,
       p_magento_catalog_category_flat_store_1.p_magento_catalog_category_flat_store_1_id,
       p_magento_catalog_category_flat_store_1.dv_batch_id,
       p_magento_catalog_category_flat_store_1.dv_load_date_time,
       p_magento_catalog_category_flat_store_1.dv_load_end_date_time
  from dbo.h_magento_catalog_category_flat_store_1
  join dbo.p_magento_catalog_category_flat_store_1
    on h_magento_catalog_category_flat_store_1.bk_hash = p_magento_catalog_category_flat_store_1.bk_hash
  join #p_magento_catalog_category_flat_store_1_insert
    on p_magento_catalog_category_flat_store_1.bk_hash = #p_magento_catalog_category_flat_store_1_insert.bk_hash
   and p_magento_catalog_category_flat_store_1.p_magento_catalog_category_flat_store_1_id = #p_magento_catalog_category_flat_store_1_insert.p_magento_catalog_category_flat_store_1_id
  join dbo.l_magento_catalog_category_flat_store_1
    on p_magento_catalog_category_flat_store_1.bk_hash = l_magento_catalog_category_flat_store_1.bk_hash
   and p_magento_catalog_category_flat_store_1.l_magento_catalog_category_flat_store_1_id = l_magento_catalog_category_flat_store_1.l_magento_catalog_category_flat_store_1_id
  join dbo.s_magento_catalog_category_flat_store_1
    on p_magento_catalog_category_flat_store_1.bk_hash = s_magento_catalog_category_flat_store_1.bk_hash
   and p_magento_catalog_category_flat_store_1.s_magento_catalog_category_flat_store_1_id = s_magento_catalog_category_flat_store_1.s_magento_catalog_category_flat_store_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_catalog_category_flat_store_1
   where d_magento_catalog_category_flat_store_1.bk_hash in (select bk_hash from #p_magento_catalog_category_flat_store_1_insert)

  insert dbo.d_magento_catalog_category_flat_store_1(
             bk_hash,
             catalog_category_flat_store_id,
             all_children,
             attribute_set_id,
             automatic_sorting,
             available_sort_by,
             children,
             children_count,
             created_at,
             created_dim_date_key,
             created_dim_time_key,
             created_in,
             custom_apply_to_products,
             custom_design,
             custom_design_from,
             custom_design_from_dim_date_key,
             custom_design_from_dim_time_key,
             custom_design_to,
             custom_design_to_dim_date_key,
             custom_design_to_dim_time_key,
             custom_layout_update,
             custom_use_parent_settings,
             default_sort_by,
             description,
             dim_magento_category_key,
             display_mode,
             featured_category,
             featured_image,
             filter_price_range,
             include_in_menu_flag,
             is_active_flag,
             is_anchor_flag,
             is_virtual_category_flag,
             landing_page,
             level,
             meta_description,
             meta_keywords,
             meta_title,
             name,
             page_layout,
             parent_id,
             path,
             path_in_store,
             position,
             row_id,
             short_description,
             store_id,
             thumbnail,
             updated_at,
             updated_dim_date_key,
             updated_dim_time_key,
             updated_in,
             url_key,
             url_path,
             use_name_in_product_search,
             virtual_category_root,
             virtual_rule,
             deleted_flag,
             p_magento_catalog_category_flat_store_1_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         catalog_category_flat_store_id,
         all_children,
         attribute_set_id,
         automatic_sorting,
         available_sort_by,
         children,
         children_count,
         created_at,
         created_dim_date_key,
         created_dim_time_key,
         created_in,
         custom_apply_to_products,
         custom_design,
         custom_design_from,
         custom_design_from_dim_date_key,
         custom_design_from_dim_time_key,
         custom_design_to,
         custom_design_to_dim_date_key,
         custom_design_to_dim_time_key,
         custom_layout_update,
         custom_use_parent_settings,
         default_sort_by,
         description,
         dim_magento_category_key,
         display_mode,
         featured_category,
         featured_image,
         filter_price_range,
         include_in_menu_flag,
         is_active_flag,
         is_anchor_flag,
         is_virtual_category_flag,
         landing_page,
         level,
         meta_description,
         meta_keywords,
         meta_title,
         name,
         page_layout,
         parent_id,
         path,
         path_in_store,
         position,
         row_id,
         short_description,
         store_id,
         thumbnail,
         updated_at,
         updated_dim_date_key,
         updated_dim_time_key,
         updated_in,
         url_key,
         url_path,
         use_name_in_product_search,
         virtual_category_root,
         virtual_rule,
         dv_deleted,
         p_magento_catalog_category_flat_store_1_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_category_flat_store_1)
--Done!
end
