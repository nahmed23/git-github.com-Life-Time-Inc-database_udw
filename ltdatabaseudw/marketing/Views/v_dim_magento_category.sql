﻿CREATE VIEW [marketing].[v_dim_magento_category]
AS select d_magento_catalog_category_flat_store_1.catalog_category_flat_store_id catalog_category_flat_store_id,
       d_magento_catalog_category_flat_store_1.all_children all_children,
       d_magento_catalog_category_flat_store_1.attribute_set_id attribute_set_id,
       d_magento_catalog_category_flat_store_1.automatic_sorting automatic_sorting,
       d_magento_catalog_category_flat_store_1.available_sort_by available_sort_by,
       d_magento_catalog_category_flat_store_1.children children,
       d_magento_catalog_category_flat_store_1.children_count children_count,
       d_magento_catalog_category_flat_store_1.created_at created_at,
       d_magento_catalog_category_flat_store_1.created_dim_date_key created_dim_date_key,
       d_magento_catalog_category_flat_store_1.created_dim_time_key created_dim_time_key,
       d_magento_catalog_category_flat_store_1.created_in created_in,
       d_magento_catalog_category_flat_store_1.custom_apply_to_products custom_apply_to_products,
       d_magento_catalog_category_flat_store_1.custom_design custom_design,
       d_magento_catalog_category_flat_store_1.custom_design_from custom_design_from,
       d_magento_catalog_category_flat_store_1.custom_design_from_dim_date_key custom_design_from_dim_date_key,
       d_magento_catalog_category_flat_store_1.custom_design_from_dim_time_key custom_design_from_dim_time_key,
       d_magento_catalog_category_flat_store_1.custom_design_to custom_design_to,
       d_magento_catalog_category_flat_store_1.custom_design_to_dim_date_key custom_design_to_dim_date_key,
       d_magento_catalog_category_flat_store_1.custom_design_to_dim_time_key custom_design_to_dim_time_key,
       d_magento_catalog_category_flat_store_1.custom_layout_update custom_layout_update,
       d_magento_catalog_category_flat_store_1.custom_use_parent_settings custom_use_parent_settings,
       d_magento_catalog_category_flat_store_1.default_sort_by default_sort_by,
       d_magento_catalog_category_flat_store_1.description description,
       d_magento_catalog_category_flat_store_1.dim_magento_category_key dim_magento_category_key,
       d_magento_catalog_category_flat_store_1.display_mode display_mode,
       d_magento_catalog_category_flat_store_1.featured_category featured_category,
       d_magento_catalog_category_flat_store_1.featured_image featured_image,
       d_magento_catalog_category_flat_store_1.filter_price_range filter_price_range,
       d_magento_catalog_category_flat_store_1.include_in_menu_flag include_in_menu_flag,
       d_magento_catalog_category_flat_store_1.is_active_flag is_active_flag,
       d_magento_catalog_category_flat_store_1.is_anchor_flag is_anchor_flag,
       d_magento_catalog_category_flat_store_1.is_virtual_category_flag is_virtual_category_flag,
       d_magento_catalog_category_flat_store_1.landing_page landing_page,
       d_magento_catalog_category_flat_store_1.level level,
       d_magento_catalog_category_flat_store_1.meta_description meta_description,
       d_magento_catalog_category_flat_store_1.meta_keywords meta_keywords,
       d_magento_catalog_category_flat_store_1.meta_title meta_title,
       d_magento_catalog_category_flat_store_1.name name,
       d_magento_catalog_category_flat_store_1.page_layout page_layout,
       d_magento_catalog_category_flat_store_1.parent_id parent_id,
       d_magento_catalog_category_flat_store_1.path path,
       d_magento_catalog_category_flat_store_1.path_in_store path_in_store,
       d_magento_catalog_category_flat_store_1.position position,
       d_magento_catalog_category_flat_store_1.row_id row_id,
       d_magento_catalog_category_flat_store_1.short_description short_description,
       d_magento_catalog_category_flat_store_1.store_id store_id,
       d_magento_catalog_category_flat_store_1.thumbnail thumbnail,
       d_magento_catalog_category_flat_store_1.updated_at updated_at,
       d_magento_catalog_category_flat_store_1.updated_dim_date_key updated_dim_date_key,
       d_magento_catalog_category_flat_store_1.updated_dim_time_key updated_dim_time_key,
       d_magento_catalog_category_flat_store_1.updated_in updated_in,
       d_magento_catalog_category_flat_store_1.url_key url_key,
       d_magento_catalog_category_flat_store_1.url_path url_path,
       d_magento_catalog_category_flat_store_1.use_name_in_product_search use_name_in_product_search,
       d_magento_catalog_category_flat_store_1.virtual_category_root virtual_category_root,
       d_magento_catalog_category_flat_store_1.virtual_rule virtual_rule
  from dbo.d_magento_catalog_category_flat_store_1;