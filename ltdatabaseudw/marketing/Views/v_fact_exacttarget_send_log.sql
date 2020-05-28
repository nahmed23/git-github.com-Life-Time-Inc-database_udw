CREATE VIEW [marketing].[v_fact_exacttarget_send_log] AS select p_exacttarget_send_log.bk_hash fact_exacttarget_send_log_key,
       s_exacttarget_send_log.job_id job_id,
       s_exacttarget_send_log.list_id list_id,
       s_exacttarget_send_log.batch_id batch_id,
       s_exacttarget_send_log.sub_id sub_id,
       s_exacttarget_send_log.triggered_send_id triggered_send_id,
       s_exacttarget_send_log.member_id member_id,
       s_exacttarget_send_log.email_address email_address,
       s_exacttarget_send_log.error_code error_code,
       s_exacttarget_send_log.inserted_date_time inserted_date_time,
       s_exacttarget_send_log.subscriber_key subscriber_key,
	   s_exacttarget_send_log.eid eid,
	   s_exacttarget_send_log.contact contact,
	   s_exacttarget_send_log.primary_lead primary_lead
  from dbo.p_exacttarget_send_log
  join dbo.s_exacttarget_send_log
    on p_exacttarget_send_log.s_exacttarget_send_log_id = s_exacttarget_send_log.s_exacttarget_send_log_id
   and p_exacttarget_send_log.bk_hash = s_exacttarget_send_log.bk_hash
 where p_exacttarget_send_log.dv_load_end_date_time = convert(datetime, '9999.12.31', 102);