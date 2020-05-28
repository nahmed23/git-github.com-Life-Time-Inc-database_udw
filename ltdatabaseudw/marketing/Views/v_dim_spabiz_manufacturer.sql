CREATE VIEW [marketing].[v_dim_spabiz_manufacturer] AS select d_spabiz_manufacturer.dim_spabiz_manufacturer_key dim_spabiz_manufacturer_key,
       d_spabiz_manufacturer.manufacturer_id manufacturer_id,
       d_spabiz_manufacturer.store_number store_number,
       d_spabiz_manufacturer.deleted_date_time deleted_date_time,
       d_spabiz_manufacturer.deleted_flag deleted_flag,
       d_spabiz_manufacturer.dim_spabiz_commission_product_mapping_key dim_spabiz_commission_product_mapping_key,
       d_spabiz_manufacturer.dim_spabiz_store_key dim_spabiz_store_key,
       d_spabiz_manufacturer.edit_date_time edit_date_time,
       d_spabiz_manufacturer.name name,
       d_spabiz_manufacturer.quick_id quick_id
  from dbo.d_spabiz_manufacturer;