CREATE VIEW [marketing].[v_dim_exacttarget_send_jobs] AS select p_exacttarget_send_jobs.bk_hash dim_exacttarget_send_jobs_key,
       s_exacttarget_send_jobs.client_id client_id,
       s_exacttarget_send_jobs.sched_time sched_time,
       s_exacttarget_send_jobs.send_id send_id,
       s_exacttarget_send_jobs.sent_time sent_time,
       s_exacttarget_send_jobs.additional additional,
       s_exacttarget_send_jobs.email_name email_name,
       s_exacttarget_send_jobs.from_email from_email,
       s_exacttarget_send_jobs.from_name from_name,
       s_exacttarget_send_jobs.is_multipart is_multipart,
       s_exacttarget_send_jobs.job_status job_status,
       s_exacttarget_send_jobs.preview_url preview_url,
       s_exacttarget_send_jobs.send_definition_external_key send_definition_external_key,
       s_exacttarget_send_jobs.subject subject,
       s_exacttarget_send_jobs.triggered_send_external_key triggered_send_external_key
  from dbo.p_exacttarget_send_jobs
  join dbo.s_exacttarget_send_jobs
    on p_exacttarget_send_jobs.bk_hash = s_exacttarget_send_jobs.bk_hash 
   and p_exacttarget_send_jobs.s_exacttarget_send_jobs_id = s_exacttarget_send_jobs.s_exacttarget_send_jobs_id;