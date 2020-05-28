CREATE VIEW [marketing].[v_dim_spabiz_commission_product_mapping] AS select d_spabiz_commission_product_mapping.dim_spabiz_commission_product_mapping_key dim_spabiz_commission_product_mapping_key,
       d_spabiz_commission_product_mapping.product_name product_name,
       d_spabiz_commission_product_mapping.mapping_group_name mapping_group_name,
       d_spabiz_commission_product_mapping.product_mapping_type product_mapping_type
  from dbo.d_spabiz_commission_product_mapping;