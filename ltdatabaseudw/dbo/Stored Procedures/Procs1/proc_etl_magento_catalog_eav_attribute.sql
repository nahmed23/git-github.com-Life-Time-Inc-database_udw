CREATE PROC [dbo].[proc_etl_magento_catalog_eav_attribute] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_catalog_eav_attribute

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_catalog_eav_attribute (
       bk_hash,
       attribute_id,
       frontend_input_renderer,
       is_global,
       is_visible,
       is_searchable,
       is_filterable,
       is_comparable,
       is_visible_on_front,
       is_html_allowed_on_front,
       is_used_for_price_rules,
       is_filterable_in_search,
       used_in_product_listing,
       used_for_sort_by,
       apply_to,
       is_visible_in_advanced_search,
       position,
       is_wysiwyg_enabled,
       is_used_for_promo_rules,
       is_required_in_admin_store,
       is_used_in_grid,
       is_visible_in_grid,
       is_filterable_in_grid,
       search_weight,
       additional_data,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(attribute_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       attribute_id,
       frontend_input_renderer,
       is_global,
       is_visible,
       is_searchable,
       is_filterable,
       is_comparable,
       is_visible_on_front,
       is_html_allowed_on_front,
       is_used_for_price_rules,
       is_filterable_in_search,
       used_in_product_listing,
       used_for_sort_by,
       apply_to,
       is_visible_in_advanced_search,
       position,
       is_wysiwyg_enabled,
       is_used_for_promo_rules,
       is_required_in_admin_store,
       is_used_in_grid,
       is_visible_in_grid,
       is_filterable_in_grid,
       search_weight,
       additional_data,
       dummy_modified_date_time,
       isnull(cast(stage_magento_catalog_eav_attribute.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_catalog_eav_attribute
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_catalog_eav_attribute @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_catalog_eav_attribute (
       bk_hash,
       attribute_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_catalog_eav_attribute.bk_hash,
       stage_hash_magento_catalog_eav_attribute.attribute_id attribute_id,
       isnull(cast(stage_hash_magento_catalog_eav_attribute.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_catalog_eav_attribute
  left join h_magento_catalog_eav_attribute
    on stage_hash_magento_catalog_eav_attribute.bk_hash = h_magento_catalog_eav_attribute.bk_hash
 where h_magento_catalog_eav_attribute_id is null
   and stage_hash_magento_catalog_eav_attribute.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_magento_catalog_eav_attribute
if object_id('tempdb..#s_magento_catalog_eav_attribute_inserts') is not null drop table #s_magento_catalog_eav_attribute_inserts
create table #s_magento_catalog_eav_attribute_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_catalog_eav_attribute.bk_hash,
       stage_hash_magento_catalog_eav_attribute.attribute_id attribute_id,
       stage_hash_magento_catalog_eav_attribute.frontend_input_renderer frontend_input_renderer,
       stage_hash_magento_catalog_eav_attribute.is_global is_global,
       stage_hash_magento_catalog_eav_attribute.is_visible is_visible,
       stage_hash_magento_catalog_eav_attribute.is_searchable is_searchable,
       stage_hash_magento_catalog_eav_attribute.is_filterable is_filterable,
       stage_hash_magento_catalog_eav_attribute.is_comparable is_comparable,
       stage_hash_magento_catalog_eav_attribute.is_visible_on_front is_visible_on_front,
       stage_hash_magento_catalog_eav_attribute.is_html_allowed_on_front is_html_allowed_on_front,
       stage_hash_magento_catalog_eav_attribute.is_used_for_price_rules is_used_for_price_rules,
       stage_hash_magento_catalog_eav_attribute.is_filterable_in_search is_filterable_in_search,
       stage_hash_magento_catalog_eav_attribute.used_in_product_listing used_in_product_listing,
       stage_hash_magento_catalog_eav_attribute.used_for_sort_by used_for_sort_by,
       stage_hash_magento_catalog_eav_attribute.apply_to apply_to,
       stage_hash_magento_catalog_eav_attribute.is_visible_in_advanced_search is_visible_in_advanced_search,
       stage_hash_magento_catalog_eav_attribute.position position,
       stage_hash_magento_catalog_eav_attribute.is_wysiwyg_enabled is_wysiwyg_enabled,
       stage_hash_magento_catalog_eav_attribute.is_used_for_promo_rules is_used_for_promo_rules,
       stage_hash_magento_catalog_eav_attribute.is_required_in_admin_store is_required_in_admin_store,
       stage_hash_magento_catalog_eav_attribute.is_used_in_grid is_used_in_grid,
       stage_hash_magento_catalog_eav_attribute.is_visible_in_grid is_visible_in_grid,
       stage_hash_magento_catalog_eav_attribute.is_filterable_in_grid is_filterable_in_grid,
       stage_hash_magento_catalog_eav_attribute.search_weight search_weight,
       stage_hash_magento_catalog_eav_attribute.additional_data additional_data,
       stage_hash_magento_catalog_eav_attribute.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_catalog_eav_attribute.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.attribute_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_eav_attribute.frontend_input_renderer,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_global as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_visible as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_searchable as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_filterable as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_comparable as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_visible_on_front as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_html_allowed_on_front as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_used_for_price_rules as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_filterable_in_search as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.used_in_product_listing as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.used_for_sort_by as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_eav_attribute.apply_to,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_visible_in_advanced_search as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.position as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_wysiwyg_enabled as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_used_for_promo_rules as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_required_in_admin_store as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_used_in_grid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_visible_in_grid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.is_filterable_in_grid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_catalog_eav_attribute.search_weight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_catalog_eav_attribute.additional_data,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_catalog_eav_attribute.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_catalog_eav_attribute
 where stage_hash_magento_catalog_eav_attribute.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_catalog_eav_attribute records
set @insert_date_time = getdate()
insert into s_magento_catalog_eav_attribute (
       bk_hash,
       attribute_id,
       frontend_input_renderer,
       is_global,
       is_visible,
       is_searchable,
       is_filterable,
       is_comparable,
       is_visible_on_front,
       is_html_allowed_on_front,
       is_used_for_price_rules,
       is_filterable_in_search,
       used_in_product_listing,
       used_for_sort_by,
       apply_to,
       is_visible_in_advanced_search,
       position,
       is_wysiwyg_enabled,
       is_used_for_promo_rules,
       is_required_in_admin_store,
       is_used_in_grid,
       is_visible_in_grid,
       is_filterable_in_grid,
       search_weight,
       additional_data,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_catalog_eav_attribute_inserts.bk_hash,
       #s_magento_catalog_eav_attribute_inserts.attribute_id,
       #s_magento_catalog_eav_attribute_inserts.frontend_input_renderer,
       #s_magento_catalog_eav_attribute_inserts.is_global,
       #s_magento_catalog_eav_attribute_inserts.is_visible,
       #s_magento_catalog_eav_attribute_inserts.is_searchable,
       #s_magento_catalog_eav_attribute_inserts.is_filterable,
       #s_magento_catalog_eav_attribute_inserts.is_comparable,
       #s_magento_catalog_eav_attribute_inserts.is_visible_on_front,
       #s_magento_catalog_eav_attribute_inserts.is_html_allowed_on_front,
       #s_magento_catalog_eav_attribute_inserts.is_used_for_price_rules,
       #s_magento_catalog_eav_attribute_inserts.is_filterable_in_search,
       #s_magento_catalog_eav_attribute_inserts.used_in_product_listing,
       #s_magento_catalog_eav_attribute_inserts.used_for_sort_by,
       #s_magento_catalog_eav_attribute_inserts.apply_to,
       #s_magento_catalog_eav_attribute_inserts.is_visible_in_advanced_search,
       #s_magento_catalog_eav_attribute_inserts.position,
       #s_magento_catalog_eav_attribute_inserts.is_wysiwyg_enabled,
       #s_magento_catalog_eav_attribute_inserts.is_used_for_promo_rules,
       #s_magento_catalog_eav_attribute_inserts.is_required_in_admin_store,
       #s_magento_catalog_eav_attribute_inserts.is_used_in_grid,
       #s_magento_catalog_eav_attribute_inserts.is_visible_in_grid,
       #s_magento_catalog_eav_attribute_inserts.is_filterable_in_grid,
       #s_magento_catalog_eav_attribute_inserts.search_weight,
       #s_magento_catalog_eav_attribute_inserts.additional_data,
       #s_magento_catalog_eav_attribute_inserts.dummy_modified_date_time,
       case when s_magento_catalog_eav_attribute.s_magento_catalog_eav_attribute_id is null then isnull(#s_magento_catalog_eav_attribute_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_catalog_eav_attribute_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_catalog_eav_attribute_inserts
  left join p_magento_catalog_eav_attribute
    on #s_magento_catalog_eav_attribute_inserts.bk_hash = p_magento_catalog_eav_attribute.bk_hash
   and p_magento_catalog_eav_attribute.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_catalog_eav_attribute
    on p_magento_catalog_eav_attribute.bk_hash = s_magento_catalog_eav_attribute.bk_hash
   and p_magento_catalog_eav_attribute.s_magento_catalog_eav_attribute_id = s_magento_catalog_eav_attribute.s_magento_catalog_eav_attribute_id
 where s_magento_catalog_eav_attribute.s_magento_catalog_eav_attribute_id is null
    or (s_magento_catalog_eav_attribute.s_magento_catalog_eav_attribute_id is not null
        and s_magento_catalog_eav_attribute.dv_hash <> #s_magento_catalog_eav_attribute_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_catalog_eav_attribute @current_dv_batch_id

end
