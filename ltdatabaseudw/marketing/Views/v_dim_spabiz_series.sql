CREATE VIEW [marketing].[v_dim_spabiz_series] AS select dim_spabiz_series.dim_spabiz_series_key dim_spabiz_series_key,
       dim_spabiz_series.series_id series_id,
       dim_spabiz_series.store_number store_number,
       dim_spabiz_series.category category,
       dim_spabiz_series.deleted_date_time deleted_date_time,
       dim_spabiz_series.deleted_flag deleted_flag,
       dim_spabiz_series.dim_spabiz_store_key dim_spabiz_store_key,
       dim_spabiz_series.edit_date_time edit_date_time,
       dim_spabiz_series.p_spabiz_series_id p_spabiz_series_id,
       dim_spabiz_series.quick_id quick_id,
       dim_spabiz_series.segment segment,
       dim_spabiz_series.series_name series_name,
       dim_spabiz_series.taxable_flag taxable_flag
  from dbo.dim_spabiz_series;