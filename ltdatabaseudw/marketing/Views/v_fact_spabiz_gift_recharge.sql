CREATE VIEW [marketing].[v_fact_spabiz_gift_recharge] AS select d_spabiz_gift_recharge.fact_spabiz_gift_recharge_key fact_spabiz_gift_recharge_key,
       d_spabiz_gift_recharge.gift_recharge_id gift_recharge_id,
       d_spabiz_gift_recharge.store_number store_number,
       d_spabiz_gift_recharge.expiration_date_time expiration_date_time,
       d_spabiz_gift_recharge.dim_spabiz_store_key dim_spabiz_store_key,
       d_spabiz_gift_recharge.edit_date_time edit_date_time,
       d_spabiz_gift_recharge.fact_spabiz_gift_certificate_key fact_spabiz_gift_certificate_key,
       d_spabiz_gift_recharge.fact_spabiz_ticket_item_key fact_spabiz_ticket_item_key,
       d_spabiz_gift_recharge.fact_spabiz_ticket_key fact_spabiz_ticket_key,
       d_spabiz_gift_recharge.gift_recharge_amount gift_recharge_amount
  from dbo.d_spabiz_gift_recharge;