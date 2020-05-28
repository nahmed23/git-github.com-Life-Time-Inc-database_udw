CREATE VIEW [Sandbox].[v_fact_exacttarget_open]
AS select h_exacttarget_open.bk_hash fact_exacttarget_open_key,
       h_exacttarget_open.client_id client_id,
       h_exacttarget_open.send_id send_id,
       h_exacttarget_open.subscriber_id subscriber_id,
       h_exacttarget_open.list_id list_id,
       h_exacttarget_open.batch_id batch_id,
       h_exacttarget_open.area_code area_code,
       h_exacttarget_open.browser browser,
       h_exacttarget_open.city city,
       h_exacttarget_open.country country,
       h_exacttarget_open.device device,
       h_exacttarget_open.email_address email_address,
       h_exacttarget_open.email_client email_client,
       s_exacttarget_open.event_date event_date,
       h_exacttarget_open.event_type event_type,
       h_exacttarget_open.ip_address ip_address,
       h_exacttarget_open.is_unique is_unique,
       h_exacttarget_open.latitude latitude,
       h_exacttarget_open.longitude longitude,
       h_exacttarget_open.metro_code metro_code,
       h_exacttarget_open.operating_system operating_system,
       h_exacttarget_open.region region,
       h_exacttarget_open.subscriber_key subscriber_key,
       h_exacttarget_open.triggered_send_external_key triggered_send_external_key,
       s_exacttarget_open.dv_batch_id dv_batch_id
  from dbo.h_exacttarget_open
  join dbo.s_exacttarget_open
    on h_exacttarget_open.bk_hash = s_exacttarget_open.bk_hash;