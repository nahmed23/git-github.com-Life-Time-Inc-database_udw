CREATE VIEW [marketing].[v_fact_exacttarget_membership_email_request] AS SELECT sj.email_name, sl.member_id, s.email_address, s.event_date, b.bounce_category, o.LastOpenDate,s.campaign_id
from (select event_date, email_address, send_id,  subscriber_id, campaign_id
        from [marketing].[v_fact_exacttarget_sent]
       ) s
join [marketing].[v_dim_exacttarget_send_jobs] sj
  on s.send_id = sj.send_id
left join [marketing].[v_fact_exacttarget_send_log] sl 
  on sl.sub_id = s.subscriber_id 
 and sl.job_id = s.send_id
left join [marketing].[v_fact_exacttarget_bounces] b 
  on b.subscriber_id = s.subscriber_id 
 and b.send_id = s.send_id
left join (select send_id, subscriber_id, max(event_date) as LastOpendate
             from marketing.v_fact_exacttarget_sent
            group by send_id, subscriber_id) o
  on o.send_id = s.send_id
and o.subscriber_id = s.subscriber_id;