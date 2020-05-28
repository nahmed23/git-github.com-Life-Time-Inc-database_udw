CREATE VIEW [marketing].[v_fact_exacttarget_send_job_impression] AS select p_exacttarget_send_job_impression.bk_hash fact_exacttarget_send_job_impression_key,
       s_exacttarget_send_job_impression.client_id client_id,
       s_exacttarget_send_job_impression.send_id send_id,
       s_exacttarget_send_job_impression.impression_region_id impression_region_id,
       s_exacttarget_send_job_impression.event_date event_date,
       s_exacttarget_send_job_impression.fixed_content fixed_content,
       s_exacttarget_send_job_impression.impression_region_name impression_region_name
  from dbo.p_exacttarget_send_job_impression
  join dbo.s_exacttarget_send_job_impression
    on p_exacttarget_send_job_impression.bk_hash = s_exacttarget_send_job_impression.bk_hash 
   and p_exacttarget_send_job_impression.s_exacttarget_send_job_impression_id = s_exacttarget_send_job_impression.s_exacttarget_send_job_impression_id;