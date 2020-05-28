CREATE VIEW [marketing].[v_fact_exacttarget_sent]
AS select p_exacttarget_sent.bk_hash fact_exacttarget_sent_key,
			       s_exacttarget_sent.client_id client_id,
			       s_exacttarget_sent.send_id send_id,
			       s_exacttarget_sent.subscriber_id subscriber_id,
			       s_exacttarget_sent.list_id list_id,
			       s_exacttarget_sent.batch_id batch_id,
			       s_exacttarget_sent.campaign_id campaign_id,
			       s_exacttarget_sent.email_address email_address,
			       s_exacttarget_sent.event_date event_date,
			       s_exacttarget_sent.event_type event_type,
			       s_exacttarget_sent.subscriber_key subscriber_key,
			       s_exacttarget_sent.triggered_send_external_key triggered_send_external_key,
			       p_exacttarget_sent.dv_batch_id dv_batch_id
			  from dbo.p_exacttarget_sent
			  join dbo.s_exacttarget_sent
			    on p_exacttarget_sent.bk_hash = s_exacttarget_sent.bk_hash 
			   and p_exacttarget_sent.s_exacttarget_sent_id = s_exacttarget_sent.s_exacttarget_sent_id
			  where p_exacttarget_sent.client_id in (6198976, 6410090, 6198979, 6194782, 6413530, 6420122, 6420121);