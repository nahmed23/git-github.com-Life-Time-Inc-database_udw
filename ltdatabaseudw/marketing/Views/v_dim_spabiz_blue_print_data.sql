CREATE VIEW [marketing].[v_dim_spabiz_blue_print_data] AS select d_spabiz_blue_print_data.d_dim_spabiz_blue_print_data_key d_dim_spabiz_blue_print_data_key,
       d_spabiz_blue_print_data.blue_print_data_id blue_print_data_id,
       d_spabiz_blue_print_data.store_number store_number,
       d_spabiz_blue_print_data.answer answer,
       d_spabiz_blue_print_data.answer_text answer_text,
       d_spabiz_blue_print_data.dim_spabiz_store_key dim_spabiz_store_key,
       d_spabiz_blue_print_data.edit_date_time edit_date_time
  from dbo.d_spabiz_blue_print_data;