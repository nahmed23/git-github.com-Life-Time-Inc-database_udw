CREATE VIEW [marketing].[v_fact_exacttarget_unsubs] AS select p_exacttarget_unsubs.bk_hash fact_exacttarget_unsubs_key,
       s_exacttarget_unsubs.client_id client_id,
       s_exacttarget_unsubs.send_id send_id,
       s_exacttarget_unsubs.subscriber_id subscriber_id,
       s_exacttarget_unsubs.list_id list_id,
       s_exacttarget_unsubs.batch_id batch_id,
       s_exacttarget_unsubs.email_address email_address,
       s_exacttarget_unsubs.event_date event_date,
       s_exacttarget_unsubs.event_type event_type,
       s_exacttarget_unsubs.subscriber_key subscriber_key,
       s_exacttarget_unsubs.triggered_send_external_key triggered_send_external_key,
       s_exacttarget_unsubs.unsub_reason unsub_reason
  from dbo.p_exacttarget_unsubs
  join dbo.s_exacttarget_unsubs
    on p_exacttarget_unsubs.bk_hash = s_exacttarget_unsubs.bk_hash 
   and p_exacttarget_unsubs.s_exacttarget_unsubs_id = s_exacttarget_unsubs.s_exacttarget_unsubs_id;