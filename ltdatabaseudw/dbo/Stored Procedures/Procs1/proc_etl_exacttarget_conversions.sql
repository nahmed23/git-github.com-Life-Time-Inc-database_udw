CREATE PROC [dbo].[proc_etl_exacttarget_conversions] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_conversions @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_exacttarget_Conversions_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Conversions_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SendID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubscriberID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(BatchID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_exacttarget_Conversions
 where (stage_exacttarget_Conversions_id is not null
        or ClientID is not null
        or SendID is not null
        or SubscriberID is not null
        or ListID is not null
        or BatchID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_exacttarget_conversions_insert_stage_exacttarget_Conversions') is not null drop table #h_exacttarget_conversions_insert_stage_exacttarget_Conversions
create table #h_exacttarget_conversions_insert_stage_exacttarget_Conversions with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_exacttarget_Conversions.stage_exacttarget_Conversions_id stage_exacttarget_conversions_id,
       stage_exacttarget_Conversions.ClientID client_id,
       stage_exacttarget_Conversions.SendID send_id,
       stage_exacttarget_Conversions.SubscriberID subscriber_id,
       stage_exacttarget_Conversions.ListID list_id,
       stage_exacttarget_Conversions.BatchID batch_id,
       isnull(stage_exacttarget_Conversions.jan_one,'Jan 1, 1980') dv_load_date_time,
       h_exacttarget_conversions.h_exacttarget_conversions_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Conversions
  join #incrementals
    on stage_exacttarget_Conversions.stage_exacttarget_Conversions_id = #incrementals.source_table_id
   and stage_exacttarget_Conversions.dv_batch_id = #incrementals.dv_batch_id
  left join h_exacttarget_conversions
    on #incrementals.bk_hash = h_exacttarget_conversions.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_exacttarget_conversions_insert_stage_exacttarget_Conversions)

while @start <= @end
begin

insert into h_exacttarget_conversions (
       bk_hash,
       stage_exacttarget_conversions_id,
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
       stage_exacttarget_conversions_id,
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
  from #h_exacttarget_conversions_insert_stage_exacttarget_Conversions
 where h_exacttarget_conversions_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_exacttarget_conversions_current') is not null drop table #p_exacttarget_conversions_current
create table #p_exacttarget_conversions_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_exacttarget_conversions.bk_hash,
       p_exacttarget_conversions.p_exacttarget_conversions_id,
       p_exacttarget_conversions.stage_exacttarget_Conversions_id,
       p_exacttarget_conversions.client_id,
       p_exacttarget_conversions.send_id,
       p_exacttarget_conversions.subscriber_id,
       p_exacttarget_conversions.list_id,
       p_exacttarget_conversions.batch_id,
       p_exacttarget_conversions.s_exacttarget_conversions_id,
       p_exacttarget_conversions.dv_load_end_date_time
  from dbo.p_exacttarget_conversions
  join (select distinct bk_hash from #incrementals) inc
    on p_exacttarget_conversions.bk_hash = inc.bk_hash
 where p_exacttarget_conversions.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get s_exacttarget_conversions current hash
if object_id('tempdb..#s_exacttarget_conversions_current') is not null drop table #s_exacttarget_conversions_current
create table #s_exacttarget_conversions_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_exacttarget_conversions.s_exacttarget_conversions_id,
       s_exacttarget_conversions.bk_hash,
       s_exacttarget_conversions.dv_hash
  from dbo.s_exacttarget_conversions
  join #p_exacttarget_conversions_current
    on s_exacttarget_conversions.s_exacttarget_conversions_id = #p_exacttarget_conversions_current.s_exacttarget_conversions_id
   and s_exacttarget_conversions.bk_hash = #p_exacttarget_conversions_current.bk_hash

--calculate hash and lookup to current s_exacttarget_conversions
if object_id('tempdb..#s_exacttarget_conversions_inserts') is not null drop table #s_exacttarget_conversions_inserts
create table #s_exacttarget_conversions_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_exacttarget_Conversions.stage_exacttarget_Conversions_id stage_exacttarget_conversions_id,
       stage_exacttarget_Conversions.ClientID client_id,
       stage_exacttarget_Conversions.SendID send_id,
       stage_exacttarget_Conversions.SubscriberKey subscriber_key,
       stage_exacttarget_Conversions.EmailAddress email_address,
       stage_exacttarget_Conversions.SubscriberID subscriber_id,
       stage_exacttarget_Conversions.ListID list_id,
       stage_exacttarget_Conversions.EventDate event_date,
       stage_exacttarget_Conversions.EventType event_type,
       stage_exacttarget_Conversions.ReferringURL referring_url,
       stage_exacttarget_Conversions.LinkAlias link_alias,
       stage_exacttarget_Conversions.ConversionData conversion_data,
       stage_exacttarget_Conversions.BatchID batch_id,
       stage_exacttarget_Conversions.TriggeredSendExternalKey triggered_send_external_key,
       stage_exacttarget_Conversions.URLID url_id,
       stage_exacttarget_Conversions.jan_one jan_one,
       stage_exacttarget_Conversions.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Conversions.stage_exacttarget_Conversions_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Conversions.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Conversions.SendID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Conversions.SubscriberKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Conversions.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Conversions.SubscriberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Conversions.ListID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Conversions.EventDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Conversions.EventType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Conversions.ReferringURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Conversions.LinkAlias,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Conversions.ConversionData,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Conversions.BatchID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Conversions.TriggeredSendExternalKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Conversions.URLID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Conversions.jan_one,120),'z#@$k%&P'))),2) source_hash,
       #s_exacttarget_conversions_current.s_exacttarget_conversions_id,
       #s_exacttarget_conversions_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Conversions
  join #incrementals
    on stage_exacttarget_Conversions.stage_exacttarget_Conversions_id = #incrementals.source_table_id
   and stage_exacttarget_Conversions.dv_batch_id = #incrementals.dv_batch_id
  left join #s_exacttarget_conversions_current
    on #incrementals.bk_hash = #s_exacttarget_conversions_current.bk_hash

--Insert all updated and new s_exacttarget_conversions records
set @start = 1
set @end = (select max(r) from #s_exacttarget_conversions_inserts)

while @start <= @end
begin

insert into s_exacttarget_conversions (
       bk_hash,
       stage_exacttarget_conversions_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
       event_type,
       referring_url,
       link_alias,
       conversion_data,
       batch_id,
       triggered_send_external_key,
       url_id,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       stage_exacttarget_conversions_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
       event_type,
       referring_url,
       link_alias,
       conversion_data,
       batch_id,
       triggered_send_external_key,
       url_id,
       jan_one,
       case when s_exacttarget_conversions_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       source_hash,
       getdate(),
       @user
  from #s_exacttarget_conversions_inserts
 where (s_exacttarget_conversions_id is null
        or (s_exacttarget_conversions_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_exacttarget_conversions @current_dv_batch_id

--Done!
drop table #incrementals
end
