CREATE VIEW [marketing].[v_fact_exacttarget_complaints] AS select p_exacttarget_complaints.bk_hash fact_exacttarget_complaints_key,
       s_exacttarget_complaints.client_id client_id,
       s_exacttarget_complaints.send_id send_id,
       s_exacttarget_complaints.subscriber_id subscriber_id,
       s_exacttarget_complaints.list_id list_id,
       s_exacttarget_complaints.batch_id batch_id,
       s_exacttarget_complaints.domain domain,
       s_exacttarget_complaints.email_address email_address,
       s_exacttarget_complaints.event_date event_date,
       s_exacttarget_complaints.event_type event_type,
       s_exacttarget_complaints.subscriber_key subscriber_key,
       s_exacttarget_complaints.triggered_send_external_key triggered_send_external_key
  from dbo.p_exacttarget_complaints
  join dbo.s_exacttarget_complaints
    on p_exacttarget_complaints.bk_hash = s_exacttarget_complaints.bk_hash 
   and p_exacttarget_complaints.s_exacttarget_complaints_id = s_exacttarget_complaints.s_exacttarget_complaints_id;