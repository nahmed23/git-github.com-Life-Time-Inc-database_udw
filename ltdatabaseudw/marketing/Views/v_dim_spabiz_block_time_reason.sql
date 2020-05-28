CREATE VIEW [marketing].[v_dim_spabiz_block_time_reason] AS select d_spabiz_block_time.dim_spabiz_block_time_reason_key dim_spabiz_block_time_reason_key,
       d_spabiz_block_time.block_time_reason_id block_time_reason_id,
       d_spabiz_block_time.store_number store_number,
       d_spabiz_block_time.deleted_date_time deleted_date_time,
       d_spabiz_block_time.deleted_flag deleted_flag,
       d_spabiz_block_time.dim_spabiz_store_key dim_spabiz_store_key,
       d_spabiz_block_time.edit_date_time edit_date_time,
       d_spabiz_block_time.name name,
       d_spabiz_block_time.reduces_productivity_flag reduces_productivity_flag
  from dbo.d_spabiz_block_time;