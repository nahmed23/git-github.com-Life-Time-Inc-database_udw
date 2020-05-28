CREATE VIEW [marketing].[v_fact_exacttarget_conversions] AS select p_exacttarget_conversions.bk_hash fact_exacttarget_conversions_key,
       s_exacttarget_conversions.client_id client_id,
       s_exacttarget_conversions.send_id send_id,
       s_exacttarget_conversions.subscriber_id subscriber_id,
       s_exacttarget_conversions.list_id list_id,
       s_exacttarget_conversions.batch_id batch_id,
       s_exacttarget_conversions.conversion_data conversion_data,
       s_exacttarget_conversions.email_address email_address,
       s_exacttarget_conversions.event_date event_date,
       s_exacttarget_conversions.event_type event_type,
       s_exacttarget_conversions.link_alias link_alias,
       s_exacttarget_conversions.referring_url referring_url,
       s_exacttarget_conversions.subscriber_key subscriber_key,
       s_exacttarget_conversions.triggered_send_external_key triggered_send_external_key,
       s_exacttarget_conversions.url_id url_id
  from dbo.p_exacttarget_conversions
  join dbo.s_exacttarget_conversions
    on p_exacttarget_conversions.bk_hash = s_exacttarget_conversions.bk_hash 
   and p_exacttarget_conversions.s_exacttarget_conversions_id = s_exacttarget_conversions.s_exacttarget_conversions_id;