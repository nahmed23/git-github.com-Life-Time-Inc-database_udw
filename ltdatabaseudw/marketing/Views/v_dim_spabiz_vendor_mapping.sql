CREATE VIEW [marketing].[v_dim_spabiz_vendor_mapping] AS select d_spabiz_vendor_mapping.dim_spabiz_vendor_mapping_key dim_spabiz_vendor_mapping_key,
       d_spabiz_vendor_mapping.spabiz_vendor_database_id spabiz_vendor_database_id,
       d_spabiz_vendor_mapping.vendor_mapping_id vendor_mapping_id,
       d_spabiz_vendor_mapping.workday_supplier_id workday_supplier_id
  from dbo.d_spabiz_vendor_mapping;