CREATE VIEW [marketing].[v_fact_exacttarget_click_impression] AS select p_exacttarget_click_impression.bk_hash fact_exacttarget_click_impression_key,
       s_exacttarget_click_impression.client_id client_id,
       s_exacttarget_click_impression.send_id send_id,
       s_exacttarget_click_impression.subscriber_id subscriber_id,
       s_exacttarget_click_impression.list_id list_id,
       s_exacttarget_click_impression.batch_id batch_id,
       s_exacttarget_click_impression.alias alias,
       s_exacttarget_click_impression.email_address email_address,
       s_exacttarget_click_impression.event_date event_date,
       s_exacttarget_click_impression.event_type event_type,
       s_exacttarget_click_impression.impression_region_name impression_region_name,
       s_exacttarget_click_impression.send_url_id send_url_id,
       s_exacttarget_click_impression.subscriber_key subscriber_key,
       s_exacttarget_click_impression.triggered_send_external_key triggered_send_external_key,
       s_exacttarget_click_impression.url url,
       s_exacttarget_click_impression.url_id url_id
  from dbo.p_exacttarget_click_impression
  join dbo.s_exacttarget_click_impression
    on p_exacttarget_click_impression.bk_hash = s_exacttarget_click_impression.bk_hash 
   and p_exacttarget_click_impression.s_exacttarget_click_impression_id = s_exacttarget_click_impression.s_exacttarget_click_impression_id;