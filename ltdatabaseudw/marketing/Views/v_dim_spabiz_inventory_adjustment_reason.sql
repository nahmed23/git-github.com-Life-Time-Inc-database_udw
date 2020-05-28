CREATE VIEW [marketing].[v_dim_spabiz_inventory_adjustment_reason] AS select d_dim_spabiz_inventory_adjustment_reason.d_dim_spabiz_inventory_adjustment_reason_id d_dim_spabiz_inventory_adjustment_reason_id,
       d_dim_spabiz_inventory_adjustment_reason.d_dim_spabiz_inventory_adjustment_reason_key d_dim_spabiz_inventory_adjustment_reason_key,
       d_dim_spabiz_inventory_adjustment_reason.inventory_adjustment_reason_id inventory_adjustment_reason_id,
       d_dim_spabiz_inventory_adjustment_reason.store_number store_number,
       d_dim_spabiz_inventory_adjustment_reason.deleted_date_time deleted_date_time,
       d_dim_spabiz_inventory_adjustment_reason.deleted_flag deleted_flag,
       d_dim_spabiz_inventory_adjustment_reason.dim_spabiz_store_key dim_spabiz_store_key,
       d_dim_spabiz_inventory_adjustment_reason.edit_date_time edit_date_time,
       d_dim_spabiz_inventory_adjustment_reason.name name
  from dbo.d_dim_spabiz_inventory_adjustment_reason;