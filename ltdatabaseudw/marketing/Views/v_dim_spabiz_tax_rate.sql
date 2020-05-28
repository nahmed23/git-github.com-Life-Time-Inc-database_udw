CREATE VIEW [marketing].[v_dim_spabiz_tax_rate] AS select d_dim_spabiz_tax_rate.dim_spabiz_tax_rate_key dim_spabiz_tax_rate_key,
       d_dim_spabiz_tax_rate.tax_id tax_id,
       d_dim_spabiz_tax_rate.store_number store_number,
       d_dim_spabiz_tax_rate.amount amount,
       d_dim_spabiz_tax_rate.deleted_date_time deleted_date_time,
       d_dim_spabiz_tax_rate.deleted_flag deleted_flag,
       d_dim_spabiz_tax_rate.dim_spabiz_store_key dim_spabiz_store_key,
       d_dim_spabiz_tax_rate.edit_date_time edit_date_time,
       d_dim_spabiz_tax_rate.name name,
       d_dim_spabiz_tax_rate.report_cycle_dim_description_key report_cycle_dim_description_key,
       d_dim_spabiz_tax_rate.report_cycle_id report_cycle_id,
       d_dim_spabiz_tax_rate.tax_type_dim_description_key tax_type_dim_description_key,
       d_dim_spabiz_tax_rate.tax_type_id tax_type_id
  from dbo.d_dim_spabiz_tax_rate;