CREATE VIEW [marketing].[v_dim_spabiz_data_type] AS select d_spabiz_data_type.dim_spabiz_data_type_key dim_spabiz_data_type_key,
       d_spabiz_data_type.data_type_id data_type_id,
       d_spabiz_data_type.store_number store_number,
       d_spabiz_data_type.data_type_name data_type_name,
       d_spabiz_data_type.edit_date_time edit_date_time
  from dbo.d_spabiz_data_type;