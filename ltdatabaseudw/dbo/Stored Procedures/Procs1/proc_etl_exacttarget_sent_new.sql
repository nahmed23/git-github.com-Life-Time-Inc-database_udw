CREATE PROC [dbo].[proc_etl_exacttarget_sent_new] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

delete from stage_hash_exacttarget_Sent where dv_batch_id = @current_dv_batch_id

set @insert_date_time = getdate()
insert into dbo.stage_hash_exacttarget_Sent (
       bk_hash,
       ClientID,
       SendID,
       SubscriberKey,
       EmailAddress,
       SubscriberID,
       ListID,
       EventDate,
       EventType,
       BatchID,
       TriggeredSendExternalKey,
       CampaignID,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_exacttarget_Sent.ClientID as varchar(500)),'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(cast(stage_exacttarget_Sent.SendID as varchar(500)),'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(stage_exacttarget_Sent.SubscriberKey,'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(stage_exacttarget_Sent.EmailAddress,'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(cast(stage_exacttarget_Sent.SubscriberID as varchar(500)),'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(cast(stage_exacttarget_Sent.ListID as varchar(500)),'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(stage_exacttarget_Sent.BatchID,'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(stage_exacttarget_Sent.TriggeredSendExternalKey,'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(stage_exacttarget_Sent.CampaignID,'z#@$k%&P')
                                                )),2) bk_hash,
       ClientID,
       SendID,
       SubscriberKey,
       EmailAddress,
       SubscriberID,
       ListID,
       EventDate,
       EventType,
       BatchID,
       TriggeredSendExternalKey,
       CampaignID,
       jan_one,
       isnull(cast(stage_exacttarget_Sent.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exacttarget_Sent
 where dv_batch_id = @current_dv_batch_id
   and clientid is not null
   and sendid is not null
   and subscriberid is not null
   and listid is not null
   and batchid is not null

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exacttarget_sent (
       bk_hash,
       client_id,
       send_id,
       subscriber_key,
       email_address,
       subscriber_id,
       list_id,
       batch_id,
       triggered_send_external_key,
       campaign_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exacttarget_Sent.bk_hash,
       stage_hash_exacttarget_Sent.ClientID,
       stage_hash_exacttarget_Sent.SendID,
       stage_hash_exacttarget_Sent.SubscriberKey,
       stage_hash_exacttarget_Sent.EmailAddress,
       stage_hash_exacttarget_Sent.SubscriberID,
       stage_hash_exacttarget_Sent.ListID,
       stage_hash_exacttarget_Sent.EventDate,
       stage_hash_exacttarget_Sent.EventType,
       stage_hash_exacttarget_Sent.BatchID,
       stage_hash_exacttarget_Sent.TriggeredSendExternalKey,
       stage_hash_exacttarget_Sent.CampaignID,
       isnull(cast(stage_hash_exacttarget_Sent.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       19,
       @insert_date_time,
       @user
  from stage_hash_exacttarget_Sent
  left join h_exacttarget_sent
    on stage_hash_exacttarget_Sent.bk_hash = h_exacttarget_sent.bk_hash
 where h_exacttarget_sent_id is null
   and stage_hash_exacttarget_Sent.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_exacttarget_sent
if object_id('tempdb..#s_exacttarget_sent_inserts') is not null drop table #s_exacttarget_sent_inserts
create table #s_exacttarget_sent_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exacttarget_Sent.bk_hash,
       stage_hash_exacttarget_Sent.EventDate event_date,
       stage_hash_exacttarget_Sent.EventType event_type,
       stage_hash_exacttarget_Sent.jan_one jan_one,
       stage_hash_exacttarget_Sent.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_Sent.EventDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_Sent.EventType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_Sent.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exacttarget_Sent
 where stage_hash_exacttarget_Sent.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exacttarget_sent records
set @insert_date_time = getdate()
insert into s_exacttarget_sent (
       bk_hash,
       event_date,
       event_type,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exacttarget_sent_inserts.bk_hash,
       #s_exacttarget_sent_inserts.event_date,
       #s_exacttarget_sent_inserts.event_type,
       #s_exacttarget_sent_inserts.jan_one,
       case when s_exacttarget_sent.s_exacttarget_sent_id is null then isnull(#s_exacttarget_sent_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #s_exacttarget_sent_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exacttarget_sent_inserts
  left join s_exacttarget_sent
    on #s_exacttarget_sent_inserts.bk_hash = s_exacttarget_sent.bk_hash
   and #s_exacttarget_sent_inserts.source_hash = s_exacttarget_sent.dv_hash
 where s_exacttarget_sent.s_exacttarget_sent_id is null



end
