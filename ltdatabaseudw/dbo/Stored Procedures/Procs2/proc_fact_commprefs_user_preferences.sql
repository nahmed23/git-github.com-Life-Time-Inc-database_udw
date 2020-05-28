CREATE PROC [dbo].[proc_fact_commprefs_user_preferences] @dv_batch_id [varchar](500) AS
begin
/*
EXEC dbo.proc_fact_commprefs_user_preferences @dv_batch_id = '20200513070108'
*/
set xact_abort on
set nocount on

--for scripting purposes only---
--declare @dv_batch_id varchar(500)
--set @dv_batch_id = 20200513070108 --20200514070112 --20200513070108
--------------------------------

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_commprefs_user_preferences)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

/*
RJ ADDED >> Get the full list of email address with a change for the current batch
> This table will drive the data collected and metric calculations
*/
if object_id('tempdb..#email_list_with_changes') is not null drop table #email_list_with_changes
IF (@load_dv_batch_id = -1) --Use for full reload of the data
BEGIN
	create table #email_list_with_changes with (distribution=hash(email_address), location=user_db) as
	select distinct lower(email_address) as email_address, @load_dv_batch_id as load_dv_batch_id
	from marketing.v_dim_exacttarget_subscribers (nolock)
	where email_address is not null
END

IF (@load_dv_batch_id > 0)  --Use for incremental/CDC loading of the data
BEGIN
	create table #email_list_with_changes with (distribution=hash(email_address), location=user_db) as
	select lower(email_address) as email_address, @load_dv_batch_id as load_dv_batch_id
	from d_mms_email_address_status 
	where d_mms_email_address_status.bk_hash NOT IN ('-997','-998','-999') and
		  d_mms_email_address_status.dv_batch_id = @load_dv_batch_id 
	union
	select lower(email_address) as email_address, @load_dv_batch_id as load_dv_batch_id
	from marketing.v_dim_exacttarget_subscribers
	where email_address is not null
	and status = 'undeliverable'
	and dv_batch_id = @load_dv_batch_id
	union
	select lower(email_address) as email_address, @load_dv_batch_id as load_dv_batch_id
	from marketing.v_fact_exacttarget_open
	where email_address is not null
	and dv_batch_id = @load_dv_batch_id 
	union
	select lower(email_address) as email_address, @load_dv_batch_id as load_dv_batch_id
	from marketing.v_fact_exacttarget_sent
	where email_address is not null
	and dv_batch_id = @load_dv_batch_id 
	union
	select lower(email_address) as email_address, @load_dv_batch_id as load_dv_batch_id
	from marketing.v_fact_exacttarget_bounces
	where email_address is not null
	and dv_batch_id = @load_dv_batch_id 
	union
	select lower(email_address) as email_address, @load_dv_batch_id as load_dv_batch_id
	from marketing.v_fact_exacttarget_clicks
	where email_address is not null
	and dv_batch_id = @load_dv_batch_id 
	union
	--code taken from first union statement that builds #commprefs_user_preferences
	select lower(d_commprefs_communication_values.value) as email_address, @load_dv_batch_id as load_dv_batch_id
	from d_commprefs_communication_values
	left join d_commprefs_party_communication_values
	on d_commprefs_communication_values.bk_hash = d_commprefs_party_communication_values.d_commprefs_communication_values_bk_hash
	left join d_commprefs_membership_segment_parties
	on d_commprefs_party_communication_values.d_commprefs_parties_bk_hash=d_commprefs_membership_segment_parties.d_commprefs_parties_bk_hash
	left join d_commprefs_communication_type_channel_membership_segments
	on d_commprefs_communication_type_channel_membership_segments.d_commprefs_membership_segments_bk_hash=d_commprefs_membership_segment_parties.d_commprefs_membership_segments_bk_hash
	left join d_commprefs_communication_type_channels
	on d_commprefs_communication_type_channel_membership_segments.d_commprefs_communication_type_channels_bk_hash=d_commprefs_communication_type_channels.d_commprefs_communication_type_channels_key
	left join d_commprefs_communication_types
	on d_commprefs_communication_type_channels.d_commprefs_communication_types_bk_hash = d_commprefs_communication_types.bk_hash
	left join d_commprefs_communication_preferences
	on d_commprefs_communication_preferences.d_commprefs_communication_values_bk_hash=d_commprefs_communication_values.d_commprefs_communication_values_key
	and d_commprefs_communication_type_channels.d_commprefs_communication_type_channels_key=d_commprefs_communication_preferences.d_commprefs_communication_type_channels_bk_hash
	where d_commprefs_communication_types.active_on_dim_date_key <= convert(varchar(10), getdate(), 112)
	and (d_commprefs_communication_types.active_until_dim_date_key ='-998'
		 or d_commprefs_communication_types.active_until_dim_date_key >= convert(varchar(10), getdate(), 112)
		 )
	and d_commprefs_communication_type_channels.deleted_date_key='-998'
	and d_commprefs_communication_values.channel_key='email'
	and d_commprefs_communication_types.name='Alerts & Account Notifications'
	and (d_commprefs_communication_values.dv_batch_id = @load_dv_batch_id
	or d_commprefs_party_communication_values.dv_batch_id = @load_dv_batch_id
	or d_commprefs_membership_segment_parties.dv_batch_id = @load_dv_batch_id
	or d_commprefs_communication_type_channel_membership_segments.dv_batch_id = @load_dv_batch_id
	or d_commprefs_communication_types.dv_batch_id = @load_dv_batch_id
	or d_commprefs_communication_preferences.dv_batch_id = @load_dv_batch_id
	or d_commprefs_communication_type_channels.dv_batch_id = @load_dv_batch_id
	)
	union
	--code from the second statement of the union that builds #commprefs_user_preferences
	select lower(d_commprefs_communication_values.value) as email_address, @load_dv_batch_id as load_dv_batch_id
	from d_commprefs_communication_values
	left join d_commprefs_communication_type_channel_membership_segments
	on d_commprefs_communication_type_channel_membership_segments.d_commprefs_membership_segments_bk_hash=
	(select dim_commprefs_membership_segments_key from d_commprefs_membership_segments where membership_segments_key_value='non_member')
	left join d_commprefs_communication_type_channels
	on d_commprefs_communication_type_channel_membership_segments.d_commprefs_communication_type_channels_bk_hash=d_commprefs_communication_type_channels.d_commprefs_communication_type_channels_key
	left join d_commprefs_communication_types
	on d_commprefs_communication_type_channels.d_commprefs_communication_types_bk_hash = d_commprefs_communication_types.bk_hash
	left join d_commprefs_communication_preferences
	on d_commprefs_communication_preferences.d_commprefs_communication_values_bk_hash=d_commprefs_communication_values.d_commprefs_communication_values_key
	and d_commprefs_communication_type_channels.d_commprefs_communication_type_channels_key=d_commprefs_communication_preferences.d_commprefs_communication_type_channels_bk_hash
	where d_commprefs_communication_types.active_on_dim_date_key <= convert(varchar(10), getdate(), 112)
	and (d_commprefs_communication_types.active_until_dim_date_key ='-998'
		 or d_commprefs_communication_types.active_until_dim_date_key >= convert(varchar(10), getdate(), 112)
		 )
	and d_commprefs_communication_type_channels.deleted_date_key='-998'
	and d_commprefs_communication_values.channel_key='email'
	and d_commprefs_communication_types.name='Alerts & Account Notifications'
	and d_commprefs_communication_values.d_commprefs_communication_values_key not in (
			SELECT d_commprefs_communication_values_bk_hash FROM dbo.d_commprefs_party_communication_values
			)
	and (d_commprefs_communication_values.dv_batch_id = @load_dv_batch_id
	or d_commprefs_communication_type_channel_membership_segments.dv_batch_id = @load_dv_batch_id
	or d_commprefs_communication_types.dv_batch_id = @load_dv_batch_id
	or d_commprefs_communication_preferences.dv_batch_id = @load_dv_batch_id
	or d_commprefs_communication_type_channels.dv_batch_id = @load_dv_batch_id
	)
END

 /* Get the mms_email_address_status data in the currentBatch*/
if object_id('tempdb..#mms_email_address_status_current_batch') is not null drop table #mms_email_address_status_current_batch
create table dbo.#mms_email_address_status_current_batch with(distribution=hash(email_address), location=user_db) as
select lower(d_mms_email_address_status.email_address) as email_address,
       status_from_date,
	   status_thru_date,
	   d_mms_email_address_status.email_address as 'subscriber_key',
	   case when [description]='Subscribed' then 'Active'
            when [description]='Unsubscribed' then 'Unsubscribed'
	   else  ''  end as "status",
	   d_mms_email_address_status.dv_load_date_time,
	   d_mms_email_address_status.dv_batch_id
from d_mms_email_address_status
--RJ ADDED >> used to get data for all email addresses with changes in the current batch--
join #email_list_with_changes on #email_list_with_changes.email_address = lower(d_mms_email_address_status.email_address)
------------------------------------------------------------------------------------------
left join dim_description on val_communication_preference_status_key=dim_description_key
where d_mms_email_address_status.bk_hash NOT IN ('-997','-998','-999') 

 /* Get the exacttarget_subscribers data in the currentBatch*/
if object_id('tempdb..#dim_exacttarget_subscribers_current_batch') is not null drop table #dim_exacttarget_subscribers_current_batch
create table dbo.#dim_exacttarget_subscribers_current_batch with(distribution=hash(email_address), location=user_db) as
select distinct lower(v_dim_exacttarget_subscribers.email_address) as email_address,
       1 as invalid_bounce
into #dim_exacttarget_subscribers_current_batch
from marketing.v_dim_exacttarget_subscribers
--RJ ADDED >> used to get data for all email addresses with changes in the current batch--
join #email_list_with_changes on #email_list_with_changes.email_address = lower(v_dim_exacttarget_subscribers.email_address)
------------------------------------------------------------------------------------------
where v_dim_exacttarget_subscribers.email_address is not null
and status = 'undeliverable'

 /* Get email_all_subscriber data in the currentBatch from above two temp tables*/
 if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(email_address), location=user_db) as
select coalesce(#mms_email_address_status_current_batch.email_address,#dim_exacttarget_subscribers_current_batch.email_address) as email_address,
       #mms_email_address_status_current_batch.status_from_date as global_opt_status_from_date,
       case when #mms_email_address_status_current_batch.status='Active' or status is null then 1
	     else 0 end as global_opt_in,
       case when #mms_email_address_status_current_batch.status='Active' or status is null then 1
	     else 0 end as lt_insider_opt_in,
	   case when #mms_email_address_status_current_batch.status='Active' or status is null then 1
	     else 0 end as promotional_opt_in,
	   isnull(invalid_bounce,0) as invalid_bounce,  --RJ ADDED >> isnull to always populate a value
	   #mms_email_address_status_current_batch.dv_load_date_time,
	   #mms_email_address_status_current_batch.dv_batch_id
from #mms_email_address_status_current_batch
full join #dim_exacttarget_subscribers_current_batch
on #mms_email_address_status_current_batch.email_address = #dim_exacttarget_subscribers_current_batch.email_address

 /* Get the exacttarget_open data. We can't consider this for the current batch as we have to get the max of event date*/
if object_id('tempdb..#fact_exacttarget_open_current_batch') is not null drop table #fact_exacttarget_open_current_batch
create table dbo.#fact_exacttarget_open_current_batch with(distribution=hash(email_address), location=user_db) as
select lower(v_fact_exacttarget_open.email_address) as email_address,
       max(event_date) as 'last_open_date'
from marketing.v_fact_exacttarget_open
--RJ ADDED >> used to get data for all email addresses with changes in the current batch--
join #email_list_with_changes on #email_list_with_changes.email_address = lower(v_fact_exacttarget_open.email_address)
------------------------------------------------------------------------------------------
group by v_fact_exacttarget_open.email_address

 /* Get the exacttarget_bounces data in the currentBatch*/
if object_id('tempdb..#fact_exacttarget_bounces_current_batch') is not null drop table #fact_exacttarget_bounces_current_batch
create table dbo.#fact_exacttarget_bounces_current_batch with(distribution=hash(email_address), location=user_db) as
select lower(t2.email_address) as email_address,
       max(event_date) as 'last_bounce_date',
	   sum(case when bounce_category like '%hard%' then 1 else 0 end) as 'total_hard_bounces'
into #fact_exacttarget_bounces_current_batch
from marketing.v_fact_exacttarget_bounces t2
--RJ ADDED >> used to get data for all email addresses with changes in the current batch--
join #email_list_with_changes on #email_list_with_changes.email_address = lower(t2.email_address)
------------------------------------------------------------------------------------------
group by t2.email_address

 /* Get the exacttarget_clicks data in the currentBatch*/
if object_id('tempdb..#fact_exacttarget_clicks_current_batch') is not null drop table #fact_exacttarget_clicks_current_batch
create table dbo.#fact_exacttarget_clicks_current_batch with(distribution=hash(email_address), location=user_db) as
select lower(v_fact_exacttarget_clicks.email_address) as email_address,
       max(event_date) as 'last_click_date'
from marketing.v_fact_exacttarget_clicks
--RJ ADDED >> used to get data for all email addresses with changes in the current batch--
join #email_list_with_changes on #email_list_with_changes.email_address = lower(v_fact_exacttarget_clicks.email_address)
------------------------------------------------------------------------------------------
group by v_fact_exacttarget_clicks.email_address

if object_id('tempdb..#LastEngagement') is not null drop table #LastEngagement
create table #LastEngagement with(distribution=hash(email_address), location=user_db) as
SELECT 
	#email_list_with_changes.email_address,
	case 
		when isnull(#fact_exacttarget_open_current_batch.last_open_date,'Jan 1, 1753') >= isnull(#fact_exacttarget_clicks_current_batch.last_click_date,'Jan 1, 1753')
			then #fact_exacttarget_open_current_batch.last_open_date
		else #fact_exacttarget_clicks_current_batch.last_click_date
	end as last_engagement_date
FROM #email_list_with_changes
	left join #fact_exacttarget_open_current_batch 
		on #fact_exacttarget_open_current_batch.email_address = #email_list_with_changes.email_address
	left join #fact_exacttarget_clicks_current_batch 
		on #fact_exacttarget_clicks_current_batch.email_address = #email_list_with_changes.email_address


 /* Get the exacttarget_sent data in the currentBatch*/
if object_id('tempdb..#fact_exacttarget_sent_current_batch') is not null drop table #fact_exacttarget_sent_current_batch
create table dbo.#fact_exacttarget_sent_current_batch with(distribution=hash(email_address), location=user_db) as
select 
	lower(v_fact_exacttarget_sent.email_address) as email_address,
	min(event_date) as 'first_sent_date',  
	max(event_date) as 'last_sent_date',  
	count(1) as 'total_sent'
	--exacttarget_sent_ae_el logic----
	,
	sum(case when client_id=6198976 then 1 else 0 end) as el_total_sent,
	max(case when client_id=6198976 then event_date else NULL end) as 'el_last_sent_date',
	sum(case when client_id in (6198979,6410090) then 1 else 0 end) as ae_total_sent,
	max(case when client_id in (6198979,6410090) then event_date else NULL end) as 'ae_last_sent_date'
	--last_engagement_date and total_sent_after_last_engage--
	, 
	cte.last_engagement_date,
	sum(case 
			when v_fact_exacttarget_sent.event_date>isnull(cte.last_engagement_date,'1/1/1753') then 1 
			else 0 
		end) as 'total_sent_after_last_engage'
from marketing.v_fact_exacttarget_sent
--RJ ADDED >> used to get data for all email addresses with changes in the current batch--
join #email_list_with_changes on #email_list_with_changes.email_address = lower(v_fact_exacttarget_sent.email_address)
left join #LastEngagement cte on cte.email_address = #email_list_with_changes.email_address
------------------------------------------------------------------------------------------
group by v_fact_exacttarget_sent.email_address,
	cte.last_engagement_date

 /* Get the CommPrefs UserPreference data in the currentBatch for all the batchid*/
/*
RJ NOTE >>  this probably needs to be reviewed to ensure that data is not dropping out because of
			the LEFT JOIN logic that also filters in the WHERE condition
*/
if object_id('tempdb..#commprefs_user_preferences') is not null drop table #commprefs_user_preferences
create table dbo.#commprefs_user_preferences with(distribution=hash(email_address), location=user_db) as
select distinct d_commprefs_communication_types.name,
     case when d_commprefs_communication_types.slug='life_time_insider' then 1 else 0 end as lt_insider_opt_in,
	 case when d_commprefs_communication_types.slug='alerts_and_account_notifications' then 1 else 0 end as notifications_opt_in,
	 case when d_commprefs_communication_types.slug='flourish' then 1 else 0 end as flourish_opt_in,
	 case when d_commprefs_communication_types.slug='promotional' then 1 else 0 end as promotional_opt_in,
     d_commprefs_communication_values.channel_key,
	 lower(d_commprefs_communication_values.value) as email_address,
     case when isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_party_communication_values.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_membership_segment_parties.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753')
		 when isnull(d_commprefs_party_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_membership_segment_parties.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_party_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_party_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_party_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_commprefs_party_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_party_communication_values.dv_load_date_time,'Jan 1, 1753')
		 when isnull(d_commprefs_membership_segment_parties.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_membership_segment_parties.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_membership_segment_parties.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_commprefs_membership_segment_parties.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_membership_segment_parties.dv_load_date_time,'Jan 1, 1753')
		 when isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753')
		 when isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
		 when isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
     else isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
		 case when isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_party_communication_values.dv_batch_id,-1)
                and isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_membership_segment_parties.dv_batch_id,-1)
                and isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1)
                and isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_types.dv_batch_id,-1)
                and isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
				and isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_communication_values.dv_batch_id,-1)
		 when isnull(d_commprefs_party_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_membership_segment_parties.dv_batch_id,-1)
                and isnull(d_commprefs_party_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1)
                and isnull(d_commprefs_party_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_types.dv_batch_id,-1)
                and isnull(d_commprefs_party_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
				and isnull(d_commprefs_party_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_party_communication_values.dv_batch_id,-1)
		 when isnull(d_commprefs_membership_segment_parties.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1)
                and isnull(d_commprefs_membership_segment_parties.dv_batch_id,-1) >= isnull(d_commprefs_communication_types.dv_batch_id,-1)
                and isnull(d_commprefs_membership_segment_parties.dv_batch_id,-1) >= isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
				and isnull(d_commprefs_membership_segment_parties.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_membership_segment_parties.dv_batch_id,-1)
		 when isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1) >= isnull(d_commprefs_communication_types.dv_batch_id,-1)
                and isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1) >= isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
				and isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1)
		 when isnull(d_commprefs_communication_types.dv_batch_id,-1) >= isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
				and isnull(d_commprefs_communication_types.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_communication_types.dv_batch_id,-1)
		 when isnull(d_commprefs_communication_types.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_communication_types.dv_batch_id,-1)
     else isnull(d_commprefs_communication_type_channels.dv_batch_id,-1) end dv_batch_id
from d_commprefs_communication_values
--RJ ADDED >> used to get data for all email addresses with changes in the current batch--
join #email_list_with_changes on #email_list_with_changes.email_address = lower(d_commprefs_communication_values.value)
------------------------------------------------------------------------------------------
left join d_commprefs_party_communication_values
on d_commprefs_communication_values.bk_hash = d_commprefs_party_communication_values.d_commprefs_communication_values_bk_hash
left join d_commprefs_membership_segment_parties
on d_commprefs_party_communication_values.d_commprefs_parties_bk_hash=d_commprefs_membership_segment_parties.d_commprefs_parties_bk_hash
left join d_commprefs_communication_type_channel_membership_segments
on d_commprefs_communication_type_channel_membership_segments.d_commprefs_membership_segments_bk_hash=d_commprefs_membership_segment_parties.d_commprefs_membership_segments_bk_hash
left join d_commprefs_communication_type_channels
on d_commprefs_communication_type_channel_membership_segments.d_commprefs_communication_type_channels_bk_hash=d_commprefs_communication_type_channels.d_commprefs_communication_type_channels_key
left join d_commprefs_communication_types
on d_commprefs_communication_type_channels.d_commprefs_communication_types_bk_hash = d_commprefs_communication_types.bk_hash
left join d_commprefs_communication_preferences
on d_commprefs_communication_preferences.d_commprefs_communication_values_bk_hash=d_commprefs_communication_values.d_commprefs_communication_values_key
	and d_commprefs_communication_type_channels.d_commprefs_communication_type_channels_key=d_commprefs_communication_preferences.d_commprefs_communication_type_channels_bk_hash
where d_commprefs_communication_types.active_on_dim_date_key <= convert(varchar(10), getdate(), 112)
and (d_commprefs_communication_types.active_until_dim_date_key ='-998'
     or d_commprefs_communication_types.active_until_dim_date_key >= convert(varchar(10), getdate(), 112)
     )
and d_commprefs_communication_type_channels.deleted_date_key='-998'
and d_commprefs_communication_values.channel_key='email'
and d_commprefs_communication_types.name='Alerts & Account Notifications'
union
select d_commprefs_communication_types.name,
     case when d_commprefs_communication_types.slug='life_time_insider' then 1 else 0 end as lt_insider_opt_in,
	 case when d_commprefs_communication_types.slug='alerts_and_account_notifications' then 1 else 0 end as notifications_opt_in,
	 case when d_commprefs_communication_types.slug='flourish' then 1 else 0 end as flourish_opt_in,
	 case when d_commprefs_communication_types.slug='promotional' then 1 else 0 end as promotional_opt_in,
     d_commprefs_communication_values.channel_key,
	 lower(d_commprefs_communication_values.value) as email_address,
     case when isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_communication_values.dv_load_date_time,'Jan 1, 1753')
		 when isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_communication_type_channel_membership_segments.dv_load_date_time,'Jan 1, 1753')
         when isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_communication_types.dv_load_date_time,'Jan 1, 1753')
		 when isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753') >= isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753')
         then isnull(d_commprefs_communication_preferences.dv_load_date_time,'Jan 1, 1753')
	else isnull(d_commprefs_communication_type_channels.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
     case when isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1)
                and isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_types.dv_batch_id,-1)
                and isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
				and isnull(d_commprefs_communication_values.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_communication_values.dv_batch_id,-1)
		 when isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1) >= isnull(d_commprefs_communication_types.dv_batch_id,-1)
                and isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1) >= isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
				and isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_communication_type_channel_membership_segments.dv_batch_id,-1)
         when isnull(d_commprefs_communication_types.dv_batch_id,-1) >= isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
				and isnull(d_commprefs_communication_types.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_communication_types.dv_batch_id,-1)
		 when isnull(d_commprefs_communication_preferences.dv_batch_id,-1) >= isnull(d_commprefs_communication_type_channels.dv_batch_id,-1)
         then isnull(d_commprefs_communication_preferences.dv_batch_id,-1)
	else isnull(d_commprefs_communication_type_channels.dv_batch_id,-1) end dv_batch_id
from d_commprefs_communication_values
--RJ ADDED >> used to get data for all email addresses with changes in the current batch--
join #email_list_with_changes on #email_list_with_changes.email_address = lower(d_commprefs_communication_values.value)
------------------------------------------------------------------------------------------
left join d_commprefs_communication_type_channel_membership_segments
on d_commprefs_communication_type_channel_membership_segments.d_commprefs_membership_segments_bk_hash=
(select dim_commprefs_membership_segments_key from d_commprefs_membership_segments where membership_segments_key_value='non_member')
left join d_commprefs_communication_type_channels
on d_commprefs_communication_type_channel_membership_segments.d_commprefs_communication_type_channels_bk_hash=d_commprefs_communication_type_channels.d_commprefs_communication_type_channels_key
left join d_commprefs_communication_types
on d_commprefs_communication_type_channels.d_commprefs_communication_types_bk_hash = d_commprefs_communication_types.bk_hash
left join d_commprefs_communication_preferences
on d_commprefs_communication_preferences.d_commprefs_communication_values_bk_hash=d_commprefs_communication_values.d_commprefs_communication_values_key
and d_commprefs_communication_type_channels.d_commprefs_communication_type_channels_key=d_commprefs_communication_preferences.d_commprefs_communication_type_channels_bk_hash
where d_commprefs_communication_types.active_on_dim_date_key <= convert(varchar(10), getdate(), 112)
and (d_commprefs_communication_types.active_until_dim_date_key ='-998'
     or d_commprefs_communication_types.active_until_dim_date_key >= convert(varchar(10), getdate(), 112)
     )
and d_commprefs_communication_type_channels.deleted_date_key='-998'
and d_commprefs_communication_values.channel_key='email'
and d_commprefs_communication_types.name='Alerts & Account Notifications'
and d_commprefs_communication_values.d_commprefs_communication_values_key not in (
		SELECT d_commprefs_communication_values_bk_hash FROM dbo.d_commprefs_party_communication_values
		)

 /* Get the CommPrefs UserPreference data in the currentBatch*/
if object_id('tempdb..#commprefs_user_preferences_current_batch') is not null drop table #commprefs_user_preferences_current_batch
create table dbo.#commprefs_user_preferences_current_batch with(distribution=hash(email_address), location=user_db) as
select name,
       lt_insider_opt_in,
       notifications_opt_in,
       flourish_opt_in,
       promotional_opt_in,
       channel_key,
       email_address,
       max(dv_load_date_time) dv_load_date_time,
       max(dv_batch_id) dv_batch_id
from dbo.#commprefs_user_preferences
group by name,
       lt_insider_opt_in,
       notifications_opt_in,
       flourish_opt_in,
       promotional_opt_in,
       channel_key,
       email_address

/*/*etl_step_2*/*/
--RJ CHANGED >> change table loaded to be @etl_step_3 to eliminate a processing step
if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(email_address), location=user_db) as
select all_email_id.email_address,
       coalesce(#etl_step_1.global_opt_in,1)global_opt_in,
	   coalesce(#etl_step_1.invalid_bounce,0) invalid_bounce,
	   coalesce(case when #commprefs_user_preferences_current_batch.promotional_opt_in is null
                or #commprefs_user_preferences_current_batch.promotional_opt_in=0 then null
                else #commprefs_user_preferences_current_batch.promotional_opt_in end,
	        case when #etl_step_1.global_opt_in is null or #etl_step_1.global_opt_in =1 then 1 else 0 end) promotional_opt_in,
       coalesce(case when #commprefs_user_preferences_current_batch.lt_insider_opt_in is null
                or #commprefs_user_preferences_current_batch.lt_insider_opt_in=0 then null
                else #commprefs_user_preferences_current_batch.lt_insider_opt_in end,
	        case when #etl_step_1.global_opt_in is null or #etl_step_1.global_opt_in =1 then 1 else 0 end) lt_insider_opt_in,
       coalesce(#commprefs_user_preferences_current_batch.flourish_opt_in,0)flourish_opt_in,
	   #commprefs_user_preferences_current_batch.notifications_opt_in,
	   #etl_step_1.global_opt_status_from_date,
	   NULL as invalid_bounce_date,
	   #fact_exacttarget_sent_current_batch.first_sent_date,
	   #fact_exacttarget_sent_current_batch.last_sent_date,
	   #fact_exacttarget_open_current_batch.last_open_date,
	   #fact_exacttarget_clicks_current_batch.last_click_date,
	   #fact_exacttarget_sent_current_batch.last_engagement_date, 
	   #fact_exacttarget_sent_current_batch.total_sent,
	   #fact_exacttarget_bounces_current_batch.total_hard_bounces,
	   #fact_exacttarget_bounces_current_batch.last_bounce_date,
	   #fact_exacttarget_sent_current_batch.ae_last_sent_date,
	   #fact_exacttarget_sent_current_batch.ae_total_sent,
	   #fact_exacttarget_sent_current_batch.el_last_sent_date,
	   #fact_exacttarget_sent_current_batch.el_total_sent,
	   #fact_exacttarget_sent_current_batch.total_sent_after_last_engage, 
	   case when isnull(#etl_step_1.dv_load_date_time,'Jan 1, 1753') >= isnull(#commprefs_user_preferences_current_batch.dv_load_date_time,'Jan 1, 1753')
          then isnull(#etl_step_1.dv_load_date_time,'Jan 1, 1753')
       else isnull(#commprefs_user_preferences_current_batch.dv_load_date_time,'Jan 1, 1753') end as dv_load_date_time,
       case when isnull(#etl_step_1.dv_batch_id,'-1') >= isnull(#commprefs_user_preferences_current_batch.dv_batch_id,'-1')
          then isnull(#etl_step_1.dv_batch_id,'-1')
       else isnull(#commprefs_user_preferences_current_batch.dv_batch_id,'-1') end as dv_batch_id
from #email_list_with_changes all_email_id
left join #etl_step_1
on all_email_id.email_address=#etl_step_1.email_address
left join #commprefs_user_preferences_current_batch
on all_email_id.email_address=#commprefs_user_preferences_current_batch.email_address
left join #fact_exacttarget_sent_current_batch
on all_email_id.email_address = #fact_exacttarget_sent_current_batch.email_address
left join #fact_exacttarget_open_current_batch
on all_email_id.email_address = #fact_exacttarget_open_current_batch.email_address
left join #fact_exacttarget_clicks_current_batch
on all_email_id.email_address = #fact_exacttarget_clicks_current_batch.email_address
left join #fact_exacttarget_bounces_current_batch
on all_email_id.email_address = #fact_exacttarget_bounces_current_batch.email_address

if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(email_address), location=user_db) as
select email_address,
       global_opt_in,
	   invalid_bounce,
	   promotional_opt_in,
	   lt_insider_opt_in,
       flourish_opt_in,
	   notifications_opt_in,
	   global_opt_status_from_date,
	   invalid_bounce_date,
	   first_sent_date,
	   last_sent_date,
	   last_open_date,
	   last_click_date,
	   last_engagement_date,
	   total_sent,
	   total_hard_bounces,
	   last_bounce_date,
	   ae_last_sent_date,
	   ae_total_sent,
	   el_last_sent_date,
	   el_total_sent,
       total_sent_after_last_engage,
	   case when datediff(day, first_sent_date,GETDATE()) > 90
                and last_engagement_date is not null and total_sent_after_last_engage >= 10
                and datediff(day,last_engagement_date,GETDATE()) > 90
            then 1
            when datediff(day, first_sent_date,GETDATE()) > 90
                and last_engagement_date is null and total_sent >= 10
            then 1
       else 0 end as no_engagement_3_months,
	   case when datediff(day, first_sent_date,GETDATE()) > 180
                and last_engagement_date is not null and total_sent_after_last_engage >= 10
                and datediff(day, last_engagement_date,GETDATE()) > 180
            then 1
            when datediff(day, first_sent_date,GETDATE()) > 180
                and last_engagement_date is null and total_sent >= 10
            then 1
       else 0 end as no_engagement_6_months,
	   case when datediff(day, first_sent_date,GETDATE()) > 365
                and last_engagement_date is not null and total_sent_after_last_engage >= 10
                and datediff(day, last_engagement_date,GETDATE()) > 365
            then 1
            when datediff(day, first_sent_date,GETDATE()) > 365
                and last_engagement_date is null and total_sent >= 10
            then 1
       else 0 end as no_engagement_12_months,
	   case when datediff(day, first_sent_date,GETDATE()) > 395
                and last_engagement_date is not null and total_sent_after_last_engage >= 10
                and datediff(day, last_engagement_date,GETDATE()) > 395
            then 1
            when datediff(day, first_sent_date,GETDATE()) > 395
                and last_engagement_date is null and total_sent >= 10
            then 1
       else 0 end as no_engagement_13_months,
	   dv_load_date_time,
	   coalesce(dv_batch_id,-1) dv_batch_id
from #etl_step_3


/* Delete and re-insert as a single transaction*/
/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/

begin tran
	
   delete dbo.fact_commprefs_user_preferences
   where email_address in (select email_address from dbo.#etl_step_4)

   insert into dbo.fact_commprefs_user_preferences
	(
	  ae_last_sent_date,
      ae_total_sent,
      el_last_sent_date,
      el_total_sent,
      email_address,
      first_sent_date,
      flourish_opt_in,
      global_opt_in,
      global_opt_status_from_date,
      invalid_bounce,
      invalid_bounce_date,
      last_bounce_date,
      last_click_date,
      last_engagement_date,
      last_open_date,
      last_sent_date,
      lt_insider_opt_in,
	  no_engagement_13_months,
      no_engagement_12_months,
      no_engagement_3_months,
      no_engagement_6_months,
      notifications_opt_in,
      promotional_opt_in,
      total_hard_bounces,
      total_sent,
      total_sent_after_last_engage,
      dv_load_date_time,
      dv_load_end_date_time,
      dv_batch_id,
      dv_inserted_date_time,
      dv_insert_user

	)
   select 
      ae_last_sent_date,
      ae_total_sent,
      el_last_sent_date,
      el_total_sent,
      email_address,
      first_sent_date,
      flourish_opt_in,
      global_opt_in,
      global_opt_status_from_date,
      invalid_bounce,
      invalid_bounce_date,
      last_bounce_date,
      last_click_date,
      last_engagement_date,
      last_open_date,
      last_sent_date,
      lt_insider_opt_in,
	  no_engagement_13_months,
      no_engagement_12_months,
      no_engagement_3_months,
      no_engagement_6_months,
      notifications_opt_in,
      promotional_opt_in,
      total_hard_bounces,
      total_sent,
      total_sent_after_last_engage,
	  dv_load_date_time,
      'dec 31, 9999' dv_load_end_date_time,
      dv_batch_id,
      getdate() dv_inserted_date_time,
      suser_sname() dv_insert_user
  from #etl_step_4

commit tran

end

