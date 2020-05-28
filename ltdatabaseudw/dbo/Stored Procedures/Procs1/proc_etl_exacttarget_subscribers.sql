CREATE PROC [dbo].[proc_etl_exacttarget_subscribers] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

delete from stage_hash_exacttarget_Subscribers where dv_batch_id = @current_dv_batch_id

set @insert_date_time = getdate()
insert into dbo.stage_hash_exacttarget_Subscribers (
       bk_hash,
       ClientID,
       SubscriberKey,
       EmailAddress,
       SubscriberID,
       Status,
       DateHeld,
       DateCreated,
       DateUnsubscribed,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubscriberID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ClientID,
       SubscriberKey,
       EmailAddress,
       SubscriberID,
       Status,
       DateHeld,
       DateCreated,
       DateUnsubscribed,
       jan_one,
       isnull(cast(stage_exacttarget_Subscribers.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exacttarget_Subscribers
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_subscribers @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exacttarget_subscribers (
       bk_hash,
       client_id,
       subscriber_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exacttarget_Subscribers.bk_hash,
       stage_hash_exacttarget_Subscribers.ClientID client_id,
       stage_hash_exacttarget_Subscribers.SubscriberID subscriber_id,
       isnull(cast(stage_hash_exacttarget_Subscribers.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       19,
       @insert_date_time,
       @user
  from stage_hash_exacttarget_Subscribers
  left join h_exacttarget_subscribers
    on stage_hash_exacttarget_Subscribers.bk_hash = h_exacttarget_subscribers.bk_hash
 where h_exacttarget_subscribers_id is null
   and stage_hash_exacttarget_Subscribers.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_exacttarget_subscribers
if object_id('tempdb..#s_exacttarget_subscribers_inserts') is not null drop table #s_exacttarget_subscribers_inserts
create table #s_exacttarget_subscribers_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exacttarget_Subscribers.bk_hash,
       stage_hash_exacttarget_Subscribers.ClientID client_id,
       stage_hash_exacttarget_Subscribers.SubscriberKey subscriber_key,
       stage_hash_exacttarget_Subscribers.EmailAddress email_address,
       stage_hash_exacttarget_Subscribers.SubscriberID subscriber_id,
       stage_hash_exacttarget_Subscribers.Status status,
       stage_hash_exacttarget_Subscribers.DateHeld date_held,
       stage_hash_exacttarget_Subscribers.DateCreated date_created,
       stage_hash_exacttarget_Subscribers.DateUnsubscribed date_unsubscribed,
       stage_hash_exacttarget_Subscribers.jan_one jan_one,
       stage_hash_exacttarget_Subscribers.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exacttarget_Subscribers.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_Subscribers.SubscriberKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_Subscribers.EmailAddress,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_exacttarget_Subscribers.SubscriberID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_Subscribers.Status,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_Subscribers.DateHeld,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_Subscribers.DateCreated,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_Subscribers.DateUnsubscribed,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_Subscribers.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exacttarget_Subscribers
 where stage_hash_exacttarget_Subscribers.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exacttarget_subscribers records
set @insert_date_time = getdate()
insert into s_exacttarget_subscribers (
       bk_hash,
       client_id,
       subscriber_key,
       email_address,
       subscriber_id,
       status,
       date_held,
       date_created,
       date_unsubscribed,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exacttarget_subscribers_inserts.bk_hash,
       #s_exacttarget_subscribers_inserts.client_id,
       #s_exacttarget_subscribers_inserts.subscriber_key,
       #s_exacttarget_subscribers_inserts.email_address,
       #s_exacttarget_subscribers_inserts.subscriber_id,
       #s_exacttarget_subscribers_inserts.status,
       #s_exacttarget_subscribers_inserts.date_held,
       #s_exacttarget_subscribers_inserts.date_created,
       #s_exacttarget_subscribers_inserts.date_unsubscribed,
       #s_exacttarget_subscribers_inserts.jan_one,
       case when s_exacttarget_subscribers.s_exacttarget_subscribers_id is null then isnull(#s_exacttarget_subscribers_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #s_exacttarget_subscribers_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exacttarget_subscribers_inserts
  left join p_exacttarget_subscribers
    on #s_exacttarget_subscribers_inserts.bk_hash = p_exacttarget_subscribers.bk_hash
   and p_exacttarget_subscribers.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exacttarget_subscribers
    on p_exacttarget_subscribers.bk_hash = s_exacttarget_subscribers.bk_hash
   and p_exacttarget_subscribers.s_exacttarget_subscribers_id = s_exacttarget_subscribers.s_exacttarget_subscribers_id
 where s_exacttarget_subscribers.s_exacttarget_subscribers_id is null
    or (s_exacttarget_subscribers.s_exacttarget_subscribers_id is not null
        and s_exacttarget_subscribers.dv_hash <> #s_exacttarget_subscribers_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exacttarget_subscribers @current_dv_batch_id

end
