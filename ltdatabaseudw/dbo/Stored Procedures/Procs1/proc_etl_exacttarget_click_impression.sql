CREATE PROC [dbo].[proc_etl_exacttarget_click_impression] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_click_impression @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_exacttarget_ClickImpression_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_ClickImpression_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SendID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubscriberID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(BatchID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_exacttarget_ClickImpression
 where (stage_exacttarget_ClickImpression_id is not null
        or ClientID is not null
        or SendID is not null
        or SubscriberID is not null
        or ListID is not null
        or BatchID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_exacttarget_click_impression_insert_stage_exacttarget_ClickImpression') is not null drop table #h_exacttarget_click_impression_insert_stage_exacttarget_ClickImpression
create table #h_exacttarget_click_impression_insert_stage_exacttarget_ClickImpression with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_exacttarget_ClickImpression.stage_exacttarget_ClickImpression_id stage_exacttarget_click_impression_id,
       stage_exacttarget_ClickImpression.ClientID client_id,
       stage_exacttarget_ClickImpression.SendID send_id,
       stage_exacttarget_ClickImpression.SubscriberID subscriber_id,
       stage_exacttarget_ClickImpression.ListID list_id,
       stage_exacttarget_ClickImpression.BatchID batch_id,
       isnull(stage_exacttarget_ClickImpression.jan_one,'Jan 1, 1980') dv_load_date_time,
       h_exacttarget_click_impression.h_exacttarget_click_impression_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_ClickImpression
  join #incrementals
    on stage_exacttarget_ClickImpression.stage_exacttarget_ClickImpression_id = #incrementals.source_table_id
   and stage_exacttarget_ClickImpression.dv_batch_id = #incrementals.dv_batch_id
  left join h_exacttarget_click_impression
    on #incrementals.bk_hash = h_exacttarget_click_impression.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_exacttarget_click_impression_insert_stage_exacttarget_ClickImpression)

while @start <= @end
begin

insert into h_exacttarget_click_impression (
       bk_hash,
       stage_exacttarget_click_impression_id,
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
       stage_exacttarget_click_impression_id,
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
  from #h_exacttarget_click_impression_insert_stage_exacttarget_ClickImpression
 where h_exacttarget_click_impression_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_exacttarget_click_impression_current') is not null drop table #p_exacttarget_click_impression_current
create table #p_exacttarget_click_impression_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_exacttarget_click_impression.bk_hash,
       p_exacttarget_click_impression.p_exacttarget_click_impression_id,
       p_exacttarget_click_impression.stage_exacttarget_click_impression_id,
       p_exacttarget_click_impression.client_id,
       p_exacttarget_click_impression.send_id,
       p_exacttarget_click_impression.subscriber_id,
       p_exacttarget_click_impression.list_id,
       p_exacttarget_click_impression.batch_id,
       p_exacttarget_click_impression.s_exacttarget_click_impression_id,
       p_exacttarget_click_impression.dv_load_end_date_time
  from dbo.p_exacttarget_click_impression
  join (select distinct bk_hash from #incrementals) inc
    on p_exacttarget_click_impression.bk_hash = inc.bk_hash
 where p_exacttarget_click_impression.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get s_exacttarget_click_impression current hash
if object_id('tempdb..#s_exacttarget_click_impression_current') is not null drop table #s_exacttarget_click_impression_current
create table #s_exacttarget_click_impression_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_exacttarget_click_impression.s_exacttarget_click_impression_id,
       s_exacttarget_click_impression.bk_hash,
       s_exacttarget_click_impression.dv_hash
  from dbo.s_exacttarget_click_impression
  join #p_exacttarget_click_impression_current
    on s_exacttarget_click_impression.s_exacttarget_click_impression_id = #p_exacttarget_click_impression_current.s_exacttarget_click_impression_id
   and s_exacttarget_click_impression.bk_hash = #p_exacttarget_click_impression_current.bk_hash

--calculate hash and lookup to current s_exacttarget_click_impression
if object_id('tempdb..#s_exacttarget_click_impression_inserts') is not null drop table #s_exacttarget_click_impression_inserts
create table #s_exacttarget_click_impression_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_exacttarget_ClickImpression.stage_exacttarget_ClickImpression_id stage_exacttarget_click_impression_id,
       stage_exacttarget_ClickImpression.ClientID client_id,
       stage_exacttarget_ClickImpression.SendID send_id,
       stage_exacttarget_ClickImpression.SubscriberKey subscriber_key,
       stage_exacttarget_ClickImpression.EmailAddress email_address,
       stage_exacttarget_ClickImpression.SubscriberID subscriber_id,
       stage_exacttarget_ClickImpression.ListID list_id,
       stage_exacttarget_ClickImpression.EventDate event_date,
       stage_exacttarget_ClickImpression.EventType event_type,
       stage_exacttarget_ClickImpression.BatchID batch_id,
       stage_exacttarget_ClickImpression.SendURLID send_url_id,
       stage_exacttarget_ClickImpression.URLID url_id,
       stage_exacttarget_ClickImpression.URL url,
       stage_exacttarget_ClickImpression.Alias alias,
       stage_exacttarget_ClickImpression.TriggeredSendExternalKey triggered_send_external_key,
       stage_exacttarget_ClickImpression.ImpressionRegionName impression_region_name,
       stage_exacttarget_ClickImpression.jan_one jan_one,
       stage_exacttarget_ClickImpression.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_ClickImpression.stage_exacttarget_ClickImpression_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_ClickImpression.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_ClickImpression.SendID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_ClickImpression.SubscriberKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_ClickImpression.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_ClickImpression.SubscriberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_ClickImpression.ListID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_ClickImpression.EventDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_ClickImpression.EventType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_ClickImpression.BatchID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_ClickImpression.SendURLID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_ClickImpression.URLID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_ClickImpression.URL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_ClickImpression.Alias,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_ClickImpression.TriggeredSendExternalKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_ClickImpression.ImpressionRegionName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_ClickImpression.jan_one,120),'z#@$k%&P'))),2) source_hash,
       #s_exacttarget_click_impression_current.s_exacttarget_click_impression_id,
       #s_exacttarget_click_impression_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_ClickImpression
  join #incrementals
    on stage_exacttarget_ClickImpression.stage_exacttarget_ClickImpression_id = #incrementals.source_table_id
   and stage_exacttarget_ClickImpression.dv_batch_id = #incrementals.dv_batch_id
  left join #s_exacttarget_click_impression_current
    on #incrementals.bk_hash = #s_exacttarget_click_impression_current.bk_hash

--Insert all updated and new s_exacttarget_click_impression records
set @start = 1
set @end = (select max(r) from #s_exacttarget_click_impression_inserts)

while @start <= @end
begin

insert into s_exacttarget_click_impression (
       bk_hash,
       stage_exacttarget_click_impression_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
       event_type,
       batch_id,
       send_url_id,
       url_id,
       url,
       alias,
       triggered_send_external_key,
       impression_region_name,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       stage_exacttarget_click_impression_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
       event_type,
       batch_id,
       send_url_id,
       url_id,
       url,
       alias,
       triggered_send_external_key,
       impression_region_name,
       jan_one,
       case when s_exacttarget_click_impression_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       source_hash,
       getdate(),
       @user
  from #s_exacttarget_click_impression_inserts
 where (s_exacttarget_click_impression_id is null
        or (s_exacttarget_click_impression_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_exacttarget_click_impression @current_dv_batch_id

--Done!
drop table #incrementals
end
