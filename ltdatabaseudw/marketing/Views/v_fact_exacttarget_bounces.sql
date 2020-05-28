CREATE VIEW [marketing].[v_fact_exacttarget_bounces] AS select p_exacttarget_bounces.bk_hash fact_exacttarget_bounces_key,
       s_exacttarget_bounces.client_id client_id,
       s_exacttarget_bounces.send_id send_id,
       s_exacttarget_bounces.subscriber_id subscriber_id,
       s_exacttarget_bounces.list_id list_id,
       s_exacttarget_bounces.batch_id batch_id,
       s_exacttarget_bounces.bounce_category bounce_category,
       s_exacttarget_bounces.bounce_reason bounce_reason,
       s_exacttarget_bounces.email_address email_address,
       s_exacttarget_bounces.event_date event_date,
       s_exacttarget_bounces.event_type event_type,
       s_exacttarget_bounces.smtp_code smtp_code,
       s_exacttarget_bounces.subscriber_key subscriber_key,
       s_exacttarget_bounces.triggered_send_external_key triggered_send_external_key,
       p_exacttarget_bounces.dv_batch_id dv_batch_id
  from dbo.p_exacttarget_bounces
  join dbo.s_exacttarget_bounces
    on p_exacttarget_bounces.bk_hash = s_exacttarget_bounces.bk_hash 
   and p_exacttarget_bounces.s_exacttarget_bounces_id = s_exacttarget_bounces.s_exacttarget_bounces_id;