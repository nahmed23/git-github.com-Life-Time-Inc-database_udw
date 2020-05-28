CREATE VIEW [marketing].[v_dim_spabiz_location_mapping] AS select d_spabiz_location_mapping.dim_spabiz_location_mapping_key dim_spabiz_location_mapping_key,
       d_spabiz_location_mapping.spabiz_store_number spabiz_store_number,
       d_spabiz_location_mapping.store_name store_name,
       d_spabiz_location_mapping.workday_id workday_id
  from dbo.d_spabiz_location_mapping;