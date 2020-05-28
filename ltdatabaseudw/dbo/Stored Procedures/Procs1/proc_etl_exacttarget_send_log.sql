CREATE PROC [dbo].[proc_etl_exacttarget_send_log] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exacttarget_SendLog

set @insert_date_time = getdate()
insert into dbo.stage_hash_exacttarget_SendLog (
       bk_hash,
       JobID,
       ListID,
       BatchID,
       SubID,
       TriggeredSendID,
       ErrorCode_,
       Member_ID,
       SubscriberKey,
       EmailAddress,
       InsertedDateTime,
       eid,
       contact,
       primarylead,
       jan_one,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(JobID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ListID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(BatchID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SubID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(TriggeredSendID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(Member_ID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       JobID,
       ListID,
       BatchID,
       SubID,
       TriggeredSendID,
       ErrorCode_,
       Member_ID,
       SubscriberKey,
       EmailAddress,
       InsertedDateTime,
       eid,
       contact,
       primarylead,
       jan_one,
       isnull(cast(stage_exacttarget_SendLog.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exacttarget_SendLog
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_send_log @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exacttarget_send_log (
       bk_hash,
       job_id,
       list_id,
       batch_id,
       sub_id,
       triggered_send_id,
       member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exacttarget_SendLog.bk_hash,
       stage_hash_exacttarget_SendLog.JobID job_id,
       stage_hash_exacttarget_SendLog.ListID list_id,
       stage_hash_exacttarget_SendLog.BatchID batch_id,
       stage_hash_exacttarget_SendLog.SubID sub_id,
       stage_hash_exacttarget_SendLog.TriggeredSendID triggered_send_id,
       stage_hash_exacttarget_SendLog.Member_ID member_id,
       isnull(cast(stage_hash_exacttarget_SendLog.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       19,
       @insert_date_time,
       @user
  from stage_hash_exacttarget_SendLog
  left join h_exacttarget_send_log
    on stage_hash_exacttarget_SendLog.bk_hash = h_exacttarget_send_log.bk_hash
 where h_exacttarget_send_log_id is null
   and stage_hash_exacttarget_SendLog.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_exacttarget_send_log
if object_id('tempdb..#s_exacttarget_send_log_inserts') is not null drop table #s_exacttarget_send_log_inserts
create table #s_exacttarget_send_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exacttarget_SendLog.bk_hash,
       stage_hash_exacttarget_SendLog.JobID job_id,
       stage_hash_exacttarget_SendLog.ListID list_id,
       stage_hash_exacttarget_SendLog.BatchID batch_id,
       stage_hash_exacttarget_SendLog.SubID sub_id,
       stage_hash_exacttarget_SendLog.TriggeredSendID triggered_send_id,
       stage_hash_exacttarget_SendLog.ErrorCode_ error_code,
       stage_hash_exacttarget_SendLog.Member_ID member_id,
       stage_hash_exacttarget_SendLog.SubscriberKey subscriber_key,
       stage_hash_exacttarget_SendLog.EmailAddress email_address,
       stage_hash_exacttarget_SendLog.InsertedDateTime inserted_date_time,
       stage_hash_exacttarget_SendLog.eid eid,
       stage_hash_exacttarget_SendLog.contact contact,
       stage_hash_exacttarget_SendLog.primarylead primary_lead,
       stage_hash_exacttarget_SendLog.jan_one jan_one,
       isnull(cast(stage_hash_exacttarget_SendLog.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exacttarget_SendLog.JobID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exacttarget_SendLog.ListID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exacttarget_SendLog.BatchID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exacttarget_SendLog.SubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exacttarget_SendLog.TriggeredSendID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exacttarget_SendLog.ErrorCode_,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exacttarget_SendLog.Member_ID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exacttarget_SendLog.SubscriberKey,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exacttarget_SendLog.EmailAddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_SendLog.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exacttarget_SendLog.eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exacttarget_SendLog.contact,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exacttarget_SendLog.primarylead,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_SendLog.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exacttarget_SendLog
 where stage_hash_exacttarget_SendLog.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exacttarget_send_log records
set @insert_date_time = getdate()
insert into s_exacttarget_send_log (
       bk_hash,
       job_id,
       list_id,
       batch_id,
       sub_id,
       triggered_send_id,
       error_code,
       member_id,
       subscriber_key,
       email_address,
       inserted_date_time,
       eid,
       contact,
       primary_lead,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exacttarget_send_log_inserts.bk_hash,
       #s_exacttarget_send_log_inserts.job_id,
       #s_exacttarget_send_log_inserts.list_id,
       #s_exacttarget_send_log_inserts.batch_id,
       #s_exacttarget_send_log_inserts.sub_id,
       #s_exacttarget_send_log_inserts.triggered_send_id,
       #s_exacttarget_send_log_inserts.error_code,
       #s_exacttarget_send_log_inserts.member_id,
       #s_exacttarget_send_log_inserts.subscriber_key,
       #s_exacttarget_send_log_inserts.email_address,
       #s_exacttarget_send_log_inserts.inserted_date_time,
       #s_exacttarget_send_log_inserts.eid,
       #s_exacttarget_send_log_inserts.contact,
       #s_exacttarget_send_log_inserts.primary_lead,
       #s_exacttarget_send_log_inserts.jan_one,
       case when s_exacttarget_send_log.s_exacttarget_send_log_id is null then isnull(#s_exacttarget_send_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #s_exacttarget_send_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exacttarget_send_log_inserts
  left join p_exacttarget_send_log
    on #s_exacttarget_send_log_inserts.bk_hash = p_exacttarget_send_log.bk_hash
   and p_exacttarget_send_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exacttarget_send_log
    on p_exacttarget_send_log.bk_hash = s_exacttarget_send_log.bk_hash
   and p_exacttarget_send_log.s_exacttarget_send_log_id = s_exacttarget_send_log.s_exacttarget_send_log_id
 where s_exacttarget_send_log.s_exacttarget_send_log_id is null
    or (s_exacttarget_send_log.s_exacttarget_send_log_id is not null
        and s_exacttarget_send_log.dv_hash <> #s_exacttarget_send_log_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exacttarget_send_log @current_dv_batch_id

end
