﻿CREATE VIEW [marketing].[v_fact_spabiz_ticket_item] AS select d_spabiz_ticket_data.fact_spabiz_ticket_item_key fact_spabiz_ticket_item_key,
       d_spabiz_ticket_data.ticket_data_id ticket_data_id,
       d_spabiz_ticket_data.store_number store_number,
       d_spabiz_ticket_data.commission_amount commission_amount,
       d_spabiz_ticket_data.commission_discount_amount commission_discount_amount,
       d_spabiz_ticket_data.cost cost,
       d_spabiz_ticket_data.dim_spabiz_category_key dim_spabiz_category_key,
       d_spabiz_ticket_data.dim_spabiz_customer_key dim_spabiz_customer_key,
       d_spabiz_ticket_data.dim_spabiz_data_type_key dim_spabiz_data_type_key,
       d_spabiz_ticket_data.dim_spabiz_discount_key dim_spabiz_discount_key,
       d_spabiz_ticket_data.dim_spabiz_gift_certificate_key dim_spabiz_gift_certificate_key,
       d_spabiz_ticket_data.dim_spabiz_product_key dim_spabiz_product_key,
       d_spabiz_ticket_data.dim_spabiz_series_key dim_spabiz_series_key,
       d_spabiz_ticket_data.dim_spabiz_service_key dim_spabiz_service_key,
       d_spabiz_ticket_data.dual_commission_flag dual_commission_flag,
       d_spabiz_ticket_data.edit_date_time edit_date_time,
       d_spabiz_ticket_data.employee_commission_amount employee_commission_amount,
       d_spabiz_ticket_data.employee_commission_discount_amount employee_commission_discount_amount,
       d_spabiz_ticket_data.end_date_time end_date_time,
       d_spabiz_ticket_data.ext_price ext_price,
       d_spabiz_ticket_data.fact_spabiz_ticket_key fact_spabiz_ticket_key,
       d_spabiz_ticket_data.first_dim_spabiz_staff_key first_dim_spabiz_staff_key,
       d_spabiz_ticket_data.item_discount_amount item_discount_amount,
       d_spabiz_ticket_data.item_id_store_number_hash item_id_store_number_hash,
       d_spabiz_ticket_data.line_number line_number,
       d_spabiz_ticket_data.other_amount other_amount,
       d_spabiz_ticket_data.other_quantity other_quantity,
       d_spabiz_ticket_data.product_amount product_amount,
       d_spabiz_ticket_data.product_quantity product_quantity,
       d_spabiz_ticket_data.quantity quantity,
       d_spabiz_ticket_data.retail_price retail_price,
       d_spabiz_ticket_data.second_dim_spabiz_staff_key second_dim_spabiz_staff_key,
       d_spabiz_ticket_data.service_amount service_amount,
       d_spabiz_ticket_data.service_quantity service_quantity,
       d_spabiz_ticket_data.service_shop_charge service_shop_charge,
       d_spabiz_ticket_data.start_date_time start_date_time,
       d_spabiz_ticket_data.status_dim_description_key status_dim_description_key,
       d_spabiz_ticket_data.status_id status_id,
       d_spabiz_ticket_data.sub_dim_spabiz_category_key sub_dim_spabiz_category_key,
       d_spabiz_ticket_data.ticket_id ticket_id,
       d_spabiz_ticket_data.ticket_item_date_time ticket_item_date_time,
       d_spabiz_ticket_data.ticket_total_discount_amount ticket_total_discount_amount
  from dbo.d_spabiz_ticket_data;