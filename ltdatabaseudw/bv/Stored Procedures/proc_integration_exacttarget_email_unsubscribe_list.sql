CREATE PROC [bv].[proc_integration_exacttarget_email_unsubscribe_list] @dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

select '' LBIC_ID,
       '' CRM_CAMPAIGN_ID,
       '' CRM_SEGMENT_ID,
       '' CRM_CUSTOMER_TRACKING_ID,
       '' CP_CAMPAIGN_ID,
       '' CP_OCCURRENCE_NUMBER,
       '' CP_SEGMENT_ID,
       '' CP_CONTENT_ID,
       email_address EMAIL,
       min(event_date) UNSUBSCRIBE_TIME
from (
select email_address, event_date
from s_exacttarget_unsubs
where s_exacttarget_unsubs_id > 0
and dv_batch_id >= @dv_batch_id
union
select email_address, date_unsubscribed
from s_exacttarget_subscribers
where s_exacttarget_subscribers_id > 0
and (client_id= 6194782 OR client_id = 6185214) 
and Status = 'unsub'
and dv_batch_id >= @dv_batch_id
and s_exacttarget_subscribers_id in (select s_exacttarget_subscribers_id from p_exacttarget_subscribers where dv_load_end_date_time = 'dec 31, 9999')
) x

group by email_address



end
