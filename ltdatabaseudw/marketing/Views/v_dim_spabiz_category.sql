CREATE VIEW [marketing].[v_dim_spabiz_category] AS select d_spabiz_category.dim_spabiz_category_key dim_spabiz_category_key,
       d_spabiz_category.category_id category_id,
       d_spabiz_category.store_number store_number,
       d_spabiz_category.category_name category_name,
       d_spabiz_category.deleted_date_time deleted_date_time,
       d_spabiz_category.deleted_flag deleted_flag,
       d_spabiz_category.dim_spabiz_data_type_key dim_spabiz_data_type_key,
       d_spabiz_category.dim_spabiz_store_key dim_spabiz_store_key,
       d_spabiz_category.edit_date_time edit_date_time,
       d_spabiz_category.parent_category_bk_hash parent_category_bk_hash,
       d_spabiz_category.sub_category_flag sub_category_flag
  from dbo.d_spabiz_category;