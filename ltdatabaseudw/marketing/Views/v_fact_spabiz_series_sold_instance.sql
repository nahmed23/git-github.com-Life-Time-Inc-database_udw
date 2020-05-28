CREATE VIEW [marketing].[v_fact_spabiz_series_sold_instance] AS select d_fact_spabiz_series_sold_instance.fact_spabiz_series_sold_instance_key fact_spabiz_series_sold_instance_key,
       d_fact_spabiz_series_sold_instance.series_data_id series_data_id,
       d_fact_spabiz_series_sold_instance.store_number store_number,
       d_fact_spabiz_series_sold_instance.dim_spabiz_series_key dim_spabiz_series_key,
       d_fact_spabiz_series_sold_instance.dim_spabiz_service_key dim_spabiz_service_key,
       d_fact_spabiz_series_sold_instance.dim_spabiz_store_key dim_spabiz_store_key,
       d_fact_spabiz_series_sold_instance.edit_date_time edit_date_time,
       d_fact_spabiz_series_sold_instance.price_type_dim_description_key price_type_dim_description_key,
       d_fact_spabiz_series_sold_instance.price_type_id price_type_id,
       d_fact_spabiz_series_sold_instance.service_price service_price
  from dbo.d_fact_spabiz_series_sold_instance;