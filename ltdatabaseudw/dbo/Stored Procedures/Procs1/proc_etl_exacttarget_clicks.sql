CREATE PROC [dbo].[proc_etl_exacttarget_clicks] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_clicks @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_exacttarget_Clicks_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SendID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubscriberID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(BatchID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_exacttarget_Clicks
 where (stage_exacttarget_Clicks_id is not null
        or ClientID is not null
        or SendID is not null
        or SubscriberID is not null
        or ListID is not null
        or BatchID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_exacttarget_clicks_insert_stage_exacttarget_Clicks') is not null drop table #h_exacttarget_clicks_insert_stage_exacttarget_Clicks
create table #h_exacttarget_clicks_insert_stage_exacttarget_Clicks with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_exacttarget_Clicks.stage_exacttarget_Clicks_id stage_exacttarget_clicks_id,
       stage_exacttarget_Clicks.ClientID client_id,
       stage_exacttarget_Clicks.SendID send_id,
       stage_exacttarget_Clicks.SubscriberID subscriber_id,
       stage_exacttarget_Clicks.ListID list_id,
       stage_exacttarget_Clicks.BatchID batch_id,
       isnull(stage_exacttarget_Clicks.jan_one,'Jan 1, 1980') dv_load_date_time,
       h_exacttarget_clicks.h_exacttarget_clicks_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Clicks
  join #incrementals
    on stage_exacttarget_Clicks.stage_exacttarget_Clicks_id = #incrementals.source_table_id
   and stage_exacttarget_Clicks.dv_batch_id = #incrementals.dv_batch_id
  left join h_exacttarget_clicks
    on #incrementals.bk_hash = h_exacttarget_clicks.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_exacttarget_clicks_insert_stage_exacttarget_Clicks)

while @start <= @end
begin

insert into h_exacttarget_clicks (
       bk_hash,
       stage_exacttarget_clicks_id,
       client_id,
       send_id,
       subscriber_id,
       list_id,
       batch_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       stage_exacttarget_clicks_id,
       client_id,
       send_id,
       subscriber_id,
       list_id,
       batch_id,
       dv_load_date_time,
       @current_dv_batch_id,
       19,
       getdate(),
       @user
  from #h_exacttarget_clicks_insert_stage_exacttarget_Clicks
 where h_exacttarget_clicks_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_exacttarget_clicks_current') is not null drop table #p_exacttarget_clicks_current
create table #p_exacttarget_clicks_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_exacttarget_clicks.bk_hash,
       p_exacttarget_clicks.p_exacttarget_clicks_id,
       p_exacttarget_clicks.stage_exacttarget_clicks_id,
       p_exacttarget_clicks.client_id,
       p_exacttarget_clicks.send_id,
       p_exacttarget_clicks.subscriber_id,
       p_exacttarget_clicks.list_id,
       p_exacttarget_clicks.batch_id,
       p_exacttarget_clicks.s_exacttarget_clicks_id,
       p_exacttarget_clicks.dv_load_end_date_time
  from dbo.p_exacttarget_clicks
  join (select distinct bk_hash from #incrementals) inc
    on p_exacttarget_clicks.bk_hash = inc.bk_hash
 where p_exacttarget_clicks.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get s_exacttarget_clicks current hash
if object_id('tempdb..#s_exacttarget_clicks_current') is not null drop table #s_exacttarget_clicks_current
create table #s_exacttarget_clicks_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_exacttarget_clicks.s_exacttarget_clicks_id,
       s_exacttarget_clicks.bk_hash,
       s_exacttarget_clicks.dv_hash
  from dbo.s_exacttarget_clicks
  join #p_exacttarget_clicks_current
    on s_exacttarget_clicks.s_exacttarget_clicks_id = #p_exacttarget_clicks_current.s_exacttarget_clicks_id
   and s_exacttarget_clicks.bk_hash = #p_exacttarget_clicks_current.bk_hash

--calculate hash and lookup to current s_exacttarget_clicks
if object_id('tempdb..#s_exacttarget_clicks_inserts') is not null drop table #s_exacttarget_clicks_inserts
create table #s_exacttarget_clicks_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_exacttarget_Clicks.stage_exacttarget_Clicks_id stage_exacttarget_clicks_id,
       stage_exacttarget_Clicks.ClientID client_id,
       stage_exacttarget_Clicks.SendID send_id,
       stage_exacttarget_Clicks.SubscriberKey subscriber_key,
       stage_exacttarget_Clicks.EmailAddress email_address,
       stage_exacttarget_Clicks.SubscriberID subscriber_id,
       stage_exacttarget_Clicks.ListID list_id,
       stage_exacttarget_Clicks.EventDate event_date,
       stage_exacttarget_Clicks.EventType event_type,
       stage_exacttarget_Clicks.SendURLID send_url_id,
       stage_exacttarget_Clicks.BatchID batch_id,
       stage_exacttarget_Clicks.URL url,
       stage_exacttarget_Clicks.Alias alias,
       stage_exacttarget_Clicks.URLID url_id,
       stage_exacttarget_Clicks.TriggeredSendExternalKey triggered_send_external_key,
       stage_exacttarget_Clicks.IsUnique is_unique,
       stage_exacttarget_Clicks.IsUniqueForURL is_unique_for_url,
       stage_exacttarget_Clicks.IpAddress ip_address,
       stage_exacttarget_Clicks.Country country,
       stage_exacttarget_Clicks.Region region,
       stage_exacttarget_Clicks.City city,
       stage_exacttarget_Clicks.Latitude latitude,
       stage_exacttarget_Clicks.Longitude longitude,
       stage_exacttarget_Clicks.MetroCode metro_code,
       stage_exacttarget_Clicks.AreaCode area_code,
       stage_exacttarget_Clicks.Browser browser,
       stage_exacttarget_Clicks.EmailClient email_client,
       stage_exacttarget_Clicks.OperatingSystem operating_system,
       stage_exacttarget_Clicks.Device device,
       stage_exacttarget_Clicks.jan_one jan_one,
       stage_exacttarget_Clicks.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.stage_exacttarget_Clicks_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.SendID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.SubscriberKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.SubscriberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.ListID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Clicks.EventDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.EventType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.SendURLID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.BatchID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.URL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.Alias,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.URLID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.TriggeredSendExternalKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.IsUnique,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.IsUniqueForURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.IpAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.Country,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.Region,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.City,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.Latitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.Longitude as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.MetroCode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Clicks.AreaCode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.Browser,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.EmailClient,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.OperatingSystem,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Clicks.Device,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Clicks.jan_one,120),'z#@$k%&P'))),2) source_hash,
       #s_exacttarget_clicks_current.s_exacttarget_clicks_id,
       #s_exacttarget_clicks_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Clicks
  join #incrementals
    on stage_exacttarget_Clicks.stage_exacttarget_Clicks_id = #incrementals.source_table_id
   and stage_exacttarget_Clicks.dv_batch_id = #incrementals.dv_batch_id
  left join #s_exacttarget_clicks_current
    on #incrementals.bk_hash = #s_exacttarget_clicks_current.bk_hash

--Insert all updated and new s_exacttarget_clicks records
set @start = 1
set @end = (select max(r) from #s_exacttarget_clicks_inserts)

while @start <= @end
begin

insert into s_exacttarget_clicks (
       bk_hash,
       stage_exacttarget_clicks_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
       event_type,
       send_url_id,
       batch_id,
       url,
       alias,
       url_id,
       triggered_send_external_key,
       is_unique,
       is_unique_for_url,
       ip_address,
       country,
       region,
       city,
       latitude,
       longitude,
       metro_code,
       area_code,
       browser,
       email_client,
       operating_system,
       device,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       stage_exacttarget_clicks_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
       event_type,
       send_url_id,
       batch_id,
       url,
       alias,
       url_id,
       triggered_send_external_key,
       is_unique,
       is_unique_for_url,
       ip_address,
       country,
       region,
       city,
       latitude,
       longitude,
       metro_code,
       area_code,
       browser,
       email_client,
       operating_system,
       device,
       jan_one,
       case when s_exacttarget_clicks_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       source_hash,
       getdate(),
       @user
  from #s_exacttarget_clicks_inserts
 where (s_exacttarget_clicks_id is null
        or (s_exacttarget_clicks_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_exacttarget_clicks @current_dv_batch_id

--Done!
drop table #incrementals
end
