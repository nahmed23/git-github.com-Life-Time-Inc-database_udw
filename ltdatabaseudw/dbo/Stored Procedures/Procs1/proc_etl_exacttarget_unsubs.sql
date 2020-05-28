CREATE PROC [dbo].[proc_etl_exacttarget_unsubs] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @start int, @end int, @task_description varchar(500), @row_count int,   @user varchar(50), @start_c_id bigint , @c int

set @user = suser_sname()

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_unsubs @current_dv_batch_id

if object_id('tempdb..#incrementals') is not null drop table #incrementals
create table #incrementals with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select stage_exacttarget_Unsubs_id source_table_id,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Unsubs_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SendID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubscriberID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(BatchID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dv_batch_id
  from dbo.stage_exacttarget_Unsubs
 where (stage_exacttarget_Unsubs_id is not null
        or ClientID is not null
        or SendID is not null
        or SubscriberID is not null
        or ListID is not null
        or BatchID is not null)
   and dv_batch_id = @current_dv_batch_id

--Find new hub business keys
if object_id('tempdb..#h_exacttarget_unsubs_insert_stage_exacttarget_Unsubs') is not null drop table #h_exacttarget_unsubs_insert_stage_exacttarget_Unsubs
create table #h_exacttarget_unsubs_insert_stage_exacttarget_Unsubs with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select #incrementals.bk_hash,
       stage_exacttarget_Unsubs.stage_exacttarget_Unsubs_id stage_exacttarget_unsubs_id,
       stage_exacttarget_Unsubs.ClientID client_id,
       stage_exacttarget_Unsubs.SendID send_id,
       stage_exacttarget_Unsubs.SubscriberID subscriber_id,
       stage_exacttarget_Unsubs.ListID list_id,
       stage_exacttarget_Unsubs.BatchID batch_id,
       isnull(stage_exacttarget_Unsubs.jan_one,'Jan 1, 1980') dv_load_date_time,
       h_exacttarget_unsubs.h_exacttarget_unsubs_id,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Unsubs
  join #incrementals
    on stage_exacttarget_Unsubs.stage_exacttarget_Unsubs_id = #incrementals.source_table_id
   and stage_exacttarget_Unsubs.dv_batch_id = #incrementals.dv_batch_id
  left join h_exacttarget_unsubs
    on #incrementals.bk_hash = h_exacttarget_unsubs.bk_hash

--Insert/update new hub business keys
set @start = 1
set @end = (select max(r) from #h_exacttarget_unsubs_insert_stage_exacttarget_Unsubs)

while @start <= @end
begin

insert into h_exacttarget_unsubs (
       bk_hash,
       stage_exacttarget_unsubs_id,
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
       stage_exacttarget_unsubs_id,
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
  from #h_exacttarget_unsubs_insert_stage_exacttarget_Unsubs
 where h_exacttarget_unsubs_id is null
   and r >= @start
   and r < @start + 1000000

set @start = @start + 1000000
end

--Get PIT data for records that already exist
if object_id('tempdb..#p_exacttarget_unsubs_current') is not null drop table #p_exacttarget_unsubs_current
create table #p_exacttarget_unsubs_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select p_exacttarget_unsubs.bk_hash,
       p_exacttarget_unsubs.p_exacttarget_unsubs_id,
       p_exacttarget_unsubs.stage_exacttarget_unsubs_id,
       p_exacttarget_unsubs.client_id,
       p_exacttarget_unsubs.send_id,
       p_exacttarget_unsubs.subscriber_id,
       p_exacttarget_unsubs.list_id,
       p_exacttarget_unsubs.batch_id,
       p_exacttarget_unsubs.s_exacttarget_unsubs_id,
       p_exacttarget_unsubs.dv_load_end_date_time
  from dbo.p_exacttarget_unsubs
  join (select distinct bk_hash from #incrementals) inc
    on p_exacttarget_unsubs.bk_hash = inc.bk_hash
 where p_exacttarget_unsubs.dv_load_end_date_time = convert(datetime,'dec 31, 9999',120)

--Get s_exacttarget_unsubs current hash
if object_id('tempdb..#s_exacttarget_unsubs_current') is not null drop table #s_exacttarget_unsubs_current
create table #s_exacttarget_unsubs_current with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select s_exacttarget_unsubs.s_exacttarget_unsubs_id,
       s_exacttarget_unsubs.bk_hash,
       s_exacttarget_unsubs.dv_hash
  from dbo.s_exacttarget_unsubs
  join #p_exacttarget_unsubs_current
    on s_exacttarget_unsubs.s_exacttarget_unsubs_id = #p_exacttarget_unsubs_current.s_exacttarget_unsubs_id
   and s_exacttarget_unsubs.bk_hash = #p_exacttarget_unsubs_current.bk_hash

--calculate hash and lookup to current s_exacttarget_unsubs
if object_id('tempdb..#s_exacttarget_unsubs_inserts') is not null drop table #s_exacttarget_unsubs_inserts
create table #s_exacttarget_unsubs_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select #incrementals.bk_hash,
       stage_exacttarget_Unsubs.stage_exacttarget_Unsubs_id stage_exacttarget_unsubs_id,
       stage_exacttarget_Unsubs.ClientID client_id,
       stage_exacttarget_Unsubs.SendID send_id,
       stage_exacttarget_Unsubs.SubscriberKey subscriber_key,
       stage_exacttarget_Unsubs.EmailAddress email_address,
       stage_exacttarget_Unsubs.SubscriberID subscriber_id,
       stage_exacttarget_Unsubs.ListID list_id,
       stage_exacttarget_Unsubs.EventDate event_date,
       stage_exacttarget_Unsubs.EventType event_type,
       stage_exacttarget_Unsubs.BatchID batch_id,
       stage_exacttarget_Unsubs.TriggeredSendExternalKey triggered_send_external_key,
       stage_exacttarget_Unsubs.UnsubReason unsub_reason,
       stage_exacttarget_Unsubs.jan_one jan_one,
       stage_exacttarget_Unsubs.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Unsubs.stage_exacttarget_Unsubs_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Unsubs.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Unsubs.SendID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Unsubs.SubscriberKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Unsubs.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Unsubs.SubscriberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_exacttarget_Unsubs.ListID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Unsubs.EventDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Unsubs.EventType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Unsubs.BatchID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Unsubs.TriggeredSendExternalKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_exacttarget_Unsubs.UnsubReason,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_exacttarget_Unsubs.jan_one,120),'z#@$k%&P'))),2) source_hash,
       #s_exacttarget_unsubs_current.s_exacttarget_unsubs_id,
       #s_exacttarget_unsubs_current.dv_hash,
       row_number() over (order by #incrementals.bk_hash) r
  from dbo.stage_exacttarget_Unsubs
  join #incrementals
    on stage_exacttarget_Unsubs.stage_exacttarget_Unsubs_id = #incrementals.source_table_id
   and stage_exacttarget_Unsubs.dv_batch_id = #incrementals.dv_batch_id
  left join #s_exacttarget_unsubs_current
    on #incrementals.bk_hash = #s_exacttarget_unsubs_current.bk_hash

--Insert all updated and new s_exacttarget_unsubs records
set @start = 1
set @end = (select max(r) from #s_exacttarget_unsubs_inserts)

while @start <= @end
begin

insert into s_exacttarget_unsubs (
       bk_hash,
       stage_exacttarget_unsubs_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
       event_type,
       batch_id,
       triggered_send_external_key,
       unsub_reason,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select bk_hash,
       stage_exacttarget_unsubs_id,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       event_date,
       event_type,
       batch_id,
       triggered_send_external_key,
       unsub_reason,
       jan_one,
       case when s_exacttarget_unsubs_id is null then isnull(dv_load_date_time,convert(datetime,'jan 1, 1980',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       source_hash,
       getdate(),
       @user
  from #s_exacttarget_unsubs_inserts
 where (s_exacttarget_unsubs_id is null
        or (s_exacttarget_unsubs_id is not null
            and dv_hash <> source_hash))
   and r >= @start
   and r < @start+1000000

set @start = @start+1000000
end

--Run the PIT proc
exec dbo.proc_p_exacttarget_unsubs @current_dv_batch_id

--Done!
drop table #incrementals
end
