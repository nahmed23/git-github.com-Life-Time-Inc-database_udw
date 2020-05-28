CREATE PROC [dbo].[proc_etl_magento_catalog_category_flat_store_1] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_catalog_category_flat_store_1

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_catalog_category_flat_store_1 (
       bk_hash,
       entity_id,
       row_id,
       created_in,
       updated_in,
       attribute_set_id,
       parent_id,
       created_at,
       updated_at,
       path,
       position,
       level,
       children_count,
       store_id,
       all_children,
       automatic_sorting,
       available_sort_by,
       children,
       custom_apply_to_products,
       custom_design,
       custom_design_from,
       custom_design_to,
       custom_layout_update,
       custom_use_parent_settings,
       default_sort_by,
       description,
       display_mode,
       featured_category,
       featured_image,
       filter_price_range,
       include_in_menu,
       is_active,
       is_anchor,
       is_virtual_category,
       landing_page,
       meta_description,
       meta_keywords,
       meta_title,
       name,
       page_layout,
       path_in_store,
       short_description,
       thumbnail,
       url_key,
       url_path,
       use_name_in_product_search,
       virtual_category_root,
       virtual_rule,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       row_id,
       created_in,
       updated_in,
       attribute_set_id,
       parent_id,
       created_at,
       updated_at,
       path,
       position,
       level,
       children_count,
       store_id,
       all_children,
       automatic_sorting,
       available_sort_by,
       children,
       custom_apply_to_products,
       custom_design,
       custom_design_from,
       custom_design_to,
       custom_layout_update,
       custom_use_parent_settings,
       default_sort_by,
       description,
       display_mode,
       featured_category,
       featured_image,
       filter_price_range,
       include_in_menu,
       is_active,
       is_anchor,
       is_virtual_category,
       landing_page,
       meta_description,
       meta_keywords,
       meta_title,
       name,
       page_layout,
       path_in_store,
       short_description,
       thumbnail,
       url_key,
       url_path,
       use_name_in_product_search,
       virtual_category_root,
       virtual_rule,
       isnull(cast(stage_magento_catalog_category_flat_store_1.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_catalog_category_flat_store_1
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_catalog_category_flat_store_1 @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_catalog_category_flat_store_1 (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_catalog_category_flat_store_1.bk_hash,
       stage_hash_magento_catalog_category_flat_store_1.entity_id entity_id,
       isnull(cast(stage_hash_magento_catalog_category_flat_store_1.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_catalog_category_flat_store_1
  left join h_magento_catalog_category_flat_store_1
    on stage_hash_magento_catalog_category_flat_store_1.bk_hash = h_magento_catalog_category_flat_store_1.bk_hash
 where h_magento_catalog_category_flat_store_1_id is null
   and stage_hash_magento_catalog_category_flat_store_1.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_catalog_category_flat_store_1
if object_id('tempdb..#l_magento_catalog_category_flat_store_1_inserts') is not null drop table #l_magento_catalog_category_flat_store_1_inserts
create table #l_magento_catalog_category_flat_store_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_catalog_category_flat_store_1.bk_hash,
       stage_hash_magento_catalog_category_flat_store_1.entity_id entity_id,
       stage_hash_magento_catalog_category_flat_store_1.row_id row_id,
       stage_hash_magento_catalog_category_flat_store_1.parent_id parent_id,
       stage_hash_magento_catalog_category_flat_store_1.attribute_set_id attribute_set_id,
       stage_hash_magento_catalog_category_flat_store_1.store_id store_id,
       isnull(cast(stage_hash_magento_catalog_category_flat_store_1.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.row_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.parent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.attribute_set_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.store_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_catalog_category_flat_store_1
 where stage_hash_magento_catalog_category_flat_store_1.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_catalog_category_flat_store_1 records
set @insert_date_time = getdate()
insert into l_magento_catalog_category_flat_store_1 (
       bk_hash,
       entity_id,
       row_id,
       parent_id,
       attribute_set_id,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_catalog_category_flat_store_1_inserts.bk_hash,
       #l_magento_catalog_category_flat_store_1_inserts.entity_id,
       #l_magento_catalog_category_flat_store_1_inserts.row_id,
       #l_magento_catalog_category_flat_store_1_inserts.parent_id,
       #l_magento_catalog_category_flat_store_1_inserts.attribute_set_id,
       #l_magento_catalog_category_flat_store_1_inserts.store_id,
       case when l_magento_catalog_category_flat_store_1.l_magento_catalog_category_flat_store_1_id is null then isnull(#l_magento_catalog_category_flat_store_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_catalog_category_flat_store_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_catalog_category_flat_store_1_inserts
  left join p_magento_catalog_category_flat_store_1
    on #l_magento_catalog_category_flat_store_1_inserts.bk_hash = p_magento_catalog_category_flat_store_1.bk_hash
   and p_magento_catalog_category_flat_store_1.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_catalog_category_flat_store_1
    on p_magento_catalog_category_flat_store_1.bk_hash = l_magento_catalog_category_flat_store_1.bk_hash
   and p_magento_catalog_category_flat_store_1.l_magento_catalog_category_flat_store_1_id = l_magento_catalog_category_flat_store_1.l_magento_catalog_category_flat_store_1_id
 where l_magento_catalog_category_flat_store_1.l_magento_catalog_category_flat_store_1_id is null
    or (l_magento_catalog_category_flat_store_1.l_magento_catalog_category_flat_store_1_id is not null
        and l_magento_catalog_category_flat_store_1.dv_hash <> #l_magento_catalog_category_flat_store_1_inserts.source_hash)

--calculate hash and lookup to current s_magento_catalog_category_flat_store_1
if object_id('tempdb..#s_magento_catalog_category_flat_store_1_inserts') is not null drop table #s_magento_catalog_category_flat_store_1_inserts
create table #s_magento_catalog_category_flat_store_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_catalog_category_flat_store_1.bk_hash,
       stage_hash_magento_catalog_category_flat_store_1.entity_id entity_id,
       stage_hash_magento_catalog_category_flat_store_1.created_in created_in,
       stage_hash_magento_catalog_category_flat_store_1.updated_in updated_in,
       stage_hash_magento_catalog_category_flat_store_1.created_at created_at,
       stage_hash_magento_catalog_category_flat_store_1.updated_at updated_at,
       stage_hash_magento_catalog_category_flat_store_1.path path,
       stage_hash_magento_catalog_category_flat_store_1.position position,
       stage_hash_magento_catalog_category_flat_store_1.level level,
       stage_hash_magento_catalog_category_flat_store_1.children_count children_count,
       stage_hash_magento_catalog_category_flat_store_1.all_children all_children,
       stage_hash_magento_catalog_category_flat_store_1.automatic_sorting automatic_sorting,
       stage_hash_magento_catalog_category_flat_store_1.available_sort_by available_sort_by,
       stage_hash_magento_catalog_category_flat_store_1.children children,
       stage_hash_magento_catalog_category_flat_store_1.custom_apply_to_products custom_apply_to_products,
       stage_hash_magento_catalog_category_flat_store_1.custom_design custom_design,
       stage_hash_magento_catalog_category_flat_store_1.custom_design_from custom_design_from,
       stage_hash_magento_catalog_category_flat_store_1.custom_design_to custom_design_to,
       stage_hash_magento_catalog_category_flat_store_1.custom_layout_update custom_layout_update,
       stage_hash_magento_catalog_category_flat_store_1.custom_use_parent_settings custom_use_parent_settings,
       stage_hash_magento_catalog_category_flat_store_1.default_sort_by default_sort_by,
       stage_hash_magento_catalog_category_flat_store_1.description description,
       stage_hash_magento_catalog_category_flat_store_1.display_mode display_mode,
       stage_hash_magento_catalog_category_flat_store_1.featured_category featured_category,
       stage_hash_magento_catalog_category_flat_store_1.featured_image featured_image,
       stage_hash_magento_catalog_category_flat_store_1.filter_price_range filter_price_range,
       stage_hash_magento_catalog_category_flat_store_1.include_in_menu include_in_menu,
       stage_hash_magento_catalog_category_flat_store_1.is_active is_active,
       stage_hash_magento_catalog_category_flat_store_1.is_anchor is_anchor,
       stage_hash_magento_catalog_category_flat_store_1.is_virtual_category is_virtual_category,
       stage_hash_magento_catalog_category_flat_store_1.landing_page landing_page,
       stage_hash_magento_catalog_category_flat_store_1.meta_description meta_description,
       stage_hash_magento_catalog_category_flat_store_1.meta_keywords meta_keywords,
       stage_hash_magento_catalog_category_flat_store_1.meta_title meta_title,
       stage_hash_magento_catalog_category_flat_store_1.name name,
       stage_hash_magento_catalog_category_flat_store_1.page_layout page_layout,
       stage_hash_magento_catalog_category_flat_store_1.path_in_store path_in_store,
       stage_hash_magento_catalog_category_flat_store_1.short_description short_description,
       stage_hash_magento_catalog_category_flat_store_1.thumbnail thumbnail,
       stage_hash_magento_catalog_category_flat_store_1.url_key url_key,
       stage_hash_magento_catalog_category_flat_store_1.url_path url_path,
       stage_hash_magento_catalog_category_flat_store_1.use_name_in_product_search use_name_in_product_search,
       stage_hash_magento_catalog_category_flat_store_1.virtual_category_root virtual_category_root,
       stage_hash_magento_catalog_category_flat_store_1.virtual_rule virtual_rule,
       isnull(cast(stage_hash_magento_catalog_category_flat_store_1.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.created_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.updated_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_catalog_category_flat_store_1.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_catalog_category_flat_store_1.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.path,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.position as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.level as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.children_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.all_children,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.automatic_sorting,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.available_sort_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.children,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.custom_apply_to_products as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.custom_design,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_catalog_category_flat_store_1.custom_design_from,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_catalog_category_flat_store_1.custom_design_to,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.custom_layout_update,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.custom_use_parent_settings as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.default_sort_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.display_mode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.featured_category as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.featured_image,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.filter_price_range as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.include_in_menu as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.is_active as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.is_anchor as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.is_virtual_category as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.landing_page as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.meta_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.meta_keywords,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.meta_title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.page_layout,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.path_in_store,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.short_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.thumbnail,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.url_key,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.url_path,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.use_name_in_product_search as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_category_flat_store_1.virtual_category_root as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_category_flat_store_1.virtual_rule,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_catalog_category_flat_store_1
 where stage_hash_magento_catalog_category_flat_store_1.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_catalog_category_flat_store_1 records
set @insert_date_time = getdate()
insert into s_magento_catalog_category_flat_store_1 (
       bk_hash,
       entity_id,
       created_in,
       updated_in,
       created_at,
       updated_at,
       path,
       position,
       level,
       children_count,
       all_children,
       automatic_sorting,
       available_sort_by,
       children,
       custom_apply_to_products,
       custom_design,
       custom_design_from,
       custom_design_to,
       custom_layout_update,
       custom_use_parent_settings,
       default_sort_by,
       description,
       display_mode,
       featured_category,
       featured_image,
       filter_price_range,
       include_in_menu,
       is_active,
       is_anchor,
       is_virtual_category,
       landing_page,
       meta_description,
       meta_keywords,
       meta_title,
       name,
       page_layout,
       path_in_store,
       short_description,
       thumbnail,
       url_key,
       url_path,
       use_name_in_product_search,
       virtual_category_root,
       virtual_rule,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_catalog_category_flat_store_1_inserts.bk_hash,
       #s_magento_catalog_category_flat_store_1_inserts.entity_id,
       #s_magento_catalog_category_flat_store_1_inserts.created_in,
       #s_magento_catalog_category_flat_store_1_inserts.updated_in,
       #s_magento_catalog_category_flat_store_1_inserts.created_at,
       #s_magento_catalog_category_flat_store_1_inserts.updated_at,
       #s_magento_catalog_category_flat_store_1_inserts.path,
       #s_magento_catalog_category_flat_store_1_inserts.position,
       #s_magento_catalog_category_flat_store_1_inserts.level,
       #s_magento_catalog_category_flat_store_1_inserts.children_count,
       #s_magento_catalog_category_flat_store_1_inserts.all_children,
       #s_magento_catalog_category_flat_store_1_inserts.automatic_sorting,
       #s_magento_catalog_category_flat_store_1_inserts.available_sort_by,
       #s_magento_catalog_category_flat_store_1_inserts.children,
       #s_magento_catalog_category_flat_store_1_inserts.custom_apply_to_products,
       #s_magento_catalog_category_flat_store_1_inserts.custom_design,
       #s_magento_catalog_category_flat_store_1_inserts.custom_design_from,
       #s_magento_catalog_category_flat_store_1_inserts.custom_design_to,
       #s_magento_catalog_category_flat_store_1_inserts.custom_layout_update,
       #s_magento_catalog_category_flat_store_1_inserts.custom_use_parent_settings,
       #s_magento_catalog_category_flat_store_1_inserts.default_sort_by,
       #s_magento_catalog_category_flat_store_1_inserts.description,
       #s_magento_catalog_category_flat_store_1_inserts.display_mode,
       #s_magento_catalog_category_flat_store_1_inserts.featured_category,
       #s_magento_catalog_category_flat_store_1_inserts.featured_image,
       #s_magento_catalog_category_flat_store_1_inserts.filter_price_range,
       #s_magento_catalog_category_flat_store_1_inserts.include_in_menu,
       #s_magento_catalog_category_flat_store_1_inserts.is_active,
       #s_magento_catalog_category_flat_store_1_inserts.is_anchor,
       #s_magento_catalog_category_flat_store_1_inserts.is_virtual_category,
       #s_magento_catalog_category_flat_store_1_inserts.landing_page,
       #s_magento_catalog_category_flat_store_1_inserts.meta_description,
       #s_magento_catalog_category_flat_store_1_inserts.meta_keywords,
       #s_magento_catalog_category_flat_store_1_inserts.meta_title,
       #s_magento_catalog_category_flat_store_1_inserts.name,
       #s_magento_catalog_category_flat_store_1_inserts.page_layout,
       #s_magento_catalog_category_flat_store_1_inserts.path_in_store,
       #s_magento_catalog_category_flat_store_1_inserts.short_description,
       #s_magento_catalog_category_flat_store_1_inserts.thumbnail,
       #s_magento_catalog_category_flat_store_1_inserts.url_key,
       #s_magento_catalog_category_flat_store_1_inserts.url_path,
       #s_magento_catalog_category_flat_store_1_inserts.use_name_in_product_search,
       #s_magento_catalog_category_flat_store_1_inserts.virtual_category_root,
       #s_magento_catalog_category_flat_store_1_inserts.virtual_rule,
       case when s_magento_catalog_category_flat_store_1.s_magento_catalog_category_flat_store_1_id is null then isnull(#s_magento_catalog_category_flat_store_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_catalog_category_flat_store_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_catalog_category_flat_store_1_inserts
  left join p_magento_catalog_category_flat_store_1
    on #s_magento_catalog_category_flat_store_1_inserts.bk_hash = p_magento_catalog_category_flat_store_1.bk_hash
   and p_magento_catalog_category_flat_store_1.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_catalog_category_flat_store_1
    on p_magento_catalog_category_flat_store_1.bk_hash = s_magento_catalog_category_flat_store_1.bk_hash
   and p_magento_catalog_category_flat_store_1.s_magento_catalog_category_flat_store_1_id = s_magento_catalog_category_flat_store_1.s_magento_catalog_category_flat_store_1_id
 where s_magento_catalog_category_flat_store_1.s_magento_catalog_category_flat_store_1_id is null
    or (s_magento_catalog_category_flat_store_1.s_magento_catalog_category_flat_store_1_id is not null
        and s_magento_catalog_category_flat_store_1.dv_hash <> #s_magento_catalog_category_flat_store_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_catalog_category_flat_store_1 @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_catalog_category_flat_store_1 @current_dv_batch_id

end
