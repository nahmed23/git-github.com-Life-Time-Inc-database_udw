CREATE VIEW [marketing].[v_dim_exacttarget_subscribers] AS select p_exacttarget_subscribers.bk_hash dim_exacttarget_subscribers_key,
       s_exacttarget_subscribers.client_id client_id,
       s_exacttarget_subscribers.subscriber_id subscriber_id,
       s_exacttarget_subscribers.date_created date_created,
       s_exacttarget_subscribers.date_held date_held,
       s_exacttarget_subscribers.date_unsubscribed date_unsubscribed,
       s_exacttarget_subscribers.email_address email_address,
       s_exacttarget_subscribers.status status,
       s_exacttarget_subscribers.subscriber_key subscriber_key,
       p_exacttarget_subscribers.dv_batch_id
  from dbo.p_exacttarget_subscribers
  join dbo.s_exacttarget_subscribers
    on p_exacttarget_subscribers.bk_hash = s_exacttarget_subscribers.bk_hash 
   and p_exacttarget_subscribers.s_exacttarget_subscribers_id = s_exacttarget_subscribers.s_exacttarget_subscribers_id;