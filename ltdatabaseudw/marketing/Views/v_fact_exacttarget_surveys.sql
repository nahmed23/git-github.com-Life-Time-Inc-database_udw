CREATE VIEW [marketing].[v_fact_exacttarget_surveys] AS select p_exacttarget_surveys.bk_hash fact_exacttarget_surveys_key,
       s_exacttarget_surveys.client_id client_id,
       s_exacttarget_surveys.send_id send_id,
       s_exacttarget_surveys.subscriber_id subscriber_id,
       s_exacttarget_surveys.list_id list_id,
       s_exacttarget_surveys.batch_id batch_id,
       s_exacttarget_surveys.answer answer,
       s_exacttarget_surveys.email_address email_address,
       s_exacttarget_surveys.event_date event_date,
       s_exacttarget_surveys.event_type event_type,
       s_exacttarget_surveys.question question,
       s_exacttarget_surveys.subscriber_key subscriber_key,
       s_exacttarget_surveys.triggered_send_external_key triggered_send_external_key
  from dbo.p_exacttarget_surveys
  join dbo.s_exacttarget_surveys
    on p_exacttarget_surveys.bk_hash = s_exacttarget_surveys.bk_hash 
   and p_exacttarget_surveys.s_exacttarget_surveys_id = s_exacttarget_surveys.s_exacttarget_surveys_id;