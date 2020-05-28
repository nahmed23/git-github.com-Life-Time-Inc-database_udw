CREATE VIEW [marketing].[v_fact_spabiz_line_item_allocated_bucks] AS select fact_spabiz_line_item_allocated_bucks_id,
         allocated_bucks_payment_amount,
         dim_spabiz_segment_key,
         fact_spabiz_ticket_item_key,
         fact_spabiz_ticket_key,
         line_item_amount,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user,
         dv_updated_date_time,
         dv_update_user
    from dbo.fact_spabiz_line_item_allocated_bucks;