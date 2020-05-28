CREATE VIEW [marketing].[v_dim_spabiz_appointment_group] AS select d_spabiz_ap_group.d_dim_spabiz_appointment_group_key d_dim_spabiz_appointment_group_key,
       d_spabiz_ap_group.appointment_group_id appointment_group_id,
       d_spabiz_ap_group.store_number store_number,
       d_spabiz_ap_group.deleted_date_time deleted_date_time,
       d_spabiz_ap_group.deleted_flag deleted_flag,
       d_spabiz_ap_group.dim_spabiz_store_key dim_spabiz_store_key,
       d_spabiz_ap_group.edit_date_time edit_date_time,
       d_spabiz_ap_group.name name,
       d_spabiz_ap_group.tab_flag tab_flag
  from dbo.d_spabiz_ap_group;