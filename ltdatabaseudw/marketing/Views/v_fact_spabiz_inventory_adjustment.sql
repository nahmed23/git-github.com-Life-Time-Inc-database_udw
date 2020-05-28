CREATE VIEW [marketing].[v_fact_spabiz_inventory_adjustment] AS select d_spabiz_inv_adj.fact_spabiz_inventory_adjustment_key fact_spabiz_inventory_adjustment_key,
       d_spabiz_inv_adj.inv_adj_id inv_adj_id,
       d_spabiz_inv_adj.store_number store_number,
       d_spabiz_inv_adj.created_date_time created_date_time,
       d_spabiz_inv_adj.dim_spabiz_staff_key dim_spabiz_staff_key,
       d_spabiz_inv_adj.dim_spabiz_store_key dim_spabiz_store_key,
       d_spabiz_inv_adj.edit_date_time edit_date_time,
       d_spabiz_inv_adj.status_dim_description_key status_dim_description_key,
       d_spabiz_inv_adj.status_id status_id,
       d_spabiz_inv_adj.total total
  from dbo.d_spabiz_inv_adj;