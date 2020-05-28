CREATE VIEW [marketing].[v_fact_exacttarget_status_change] AS select p_exacttarget_status_change.bk_hash fact_exacttarget_status_change_key,
       s_exacttarget_status_change.client_id client_id,
       s_exacttarget_status_change.subscriber_id subscriber_id,
       s_exacttarget_status_change.date_changed date_changed,
       s_exacttarget_status_change.email_address email_address,
       s_exacttarget_status_change.new_status new_status,
       s_exacttarget_status_change.old_status old_status,
       s_exacttarget_status_change.subscriber_key subscriber_key
  from dbo.p_exacttarget_status_change
  join dbo.s_exacttarget_status_change
    on p_exacttarget_status_change.bk_hash = s_exacttarget_status_change.bk_hash 
   and p_exacttarget_status_change.s_exacttarget_status_change_id = s_exacttarget_status_change.s_exacttarget_status_change_id;