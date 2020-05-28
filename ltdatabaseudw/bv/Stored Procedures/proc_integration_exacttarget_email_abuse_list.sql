CREATE PROC [bv].[proc_integration_exacttarget_email_abuse_list] @dv_batch_id [bigint] AS
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
       min(event_date) ABUSE_TIME
from s_exacttarget_complaints
where s_exacttarget_complaints_id > 0
and dv_batch_id >= @dv_batch_id
group by email_address



end



