CREATE VIEW [marketing].[v_fact_exacttarget_not_sent] AS select p_exacttarget_not_sent.bk_hash fact_exacttarget_not_sent_key,
       s_exacttarget_not_sent.client_id client_id,
       s_exacttarget_not_sent.send_id send_id,
       s_exacttarget_not_sent.subscriber_id subscriber_id,
       s_exacttarget_not_sent.list_id list_id,
       s_exacttarget_not_sent.batch_id batch_id,
       s_exacttarget_not_sent.email_address email_address,
       s_exacttarget_not_sent.event_date event_date,
       s_exacttarget_not_sent.event_type event_type,
       s_exacttarget_not_sent.reason reason,
       s_exacttarget_not_sent.subscriber_key subscriber_key,
       s_exacttarget_not_sent.triggered_send_external_key triggered_send_external_key
  from dbo.p_exacttarget_not_sent
  join dbo.s_exacttarget_not_sent
    on p_exacttarget_not_sent.bk_hash = s_exacttarget_not_sent.bk_hash 
   and p_exacttarget_not_sent.s_exacttarget_not_sent_id = s_exacttarget_not_sent.s_exacttarget_not_sent_id;