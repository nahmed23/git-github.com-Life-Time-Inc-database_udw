CREATE PROC [dbo].[proc_etl_exacttarget_send_jobs] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

delete from stage_hash_exacttarget_SendJobs where dv_batch_id = @current_dv_batch_id

set @insert_date_time = getdate()
insert into dbo.stage_hash_exacttarget_SendJobs (
       bk_hash,
       ClientID,
       SendID,
       FromName,
       FromEmail,
       SchedTime,
       SentTime,
       Subject,
       EmailName,
       TriggeredSendExternalKey,
       SendDefinitionExternalKey,
       JobStatus,
       PreviewURL,
       IsMultipart,
       Additional,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ClientID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(SendID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ClientID,
       SendID,
       FromName,
       FromEmail,
       SchedTime,
       SentTime,
       Subject,
       EmailName,
       TriggeredSendExternalKey,
       SendDefinitionExternalKey,
       JobStatus,
       PreviewURL,
       IsMultipart,
       Additional,
       jan_one,
       isnull(cast(stage_exacttarget_SendJobs.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exacttarget_SendJobs
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exacttarget_send_jobs @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exacttarget_send_jobs (
       bk_hash,
       client_id,
       send_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exacttarget_SendJobs.bk_hash,
       stage_hash_exacttarget_SendJobs.ClientID client_id,
       stage_hash_exacttarget_SendJobs.SendID send_id,
       isnull(cast(stage_hash_exacttarget_SendJobs.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       19,
       @insert_date_time,
       @user
  from stage_hash_exacttarget_SendJobs
  left join h_exacttarget_send_jobs
    on stage_hash_exacttarget_SendJobs.bk_hash = h_exacttarget_send_jobs.bk_hash
 where h_exacttarget_send_jobs_id is null
   and stage_hash_exacttarget_SendJobs.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_exacttarget_send_jobs
if object_id('tempdb..#s_exacttarget_send_jobs_inserts') is not null drop table #s_exacttarget_send_jobs_inserts
create table #s_exacttarget_send_jobs_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exacttarget_SendJobs.bk_hash,
       stage_hash_exacttarget_SendJobs.ClientID client_id,
       stage_hash_exacttarget_SendJobs.SendID send_id,
       stage_hash_exacttarget_SendJobs.FromName from_name,
       stage_hash_exacttarget_SendJobs.FromEmail from_email,
       stage_hash_exacttarget_SendJobs.SchedTime sched_time,
       stage_hash_exacttarget_SendJobs.SentTime sent_time,
       stage_hash_exacttarget_SendJobs.Subject subject,
       stage_hash_exacttarget_SendJobs.EmailName email_name,
       stage_hash_exacttarget_SendJobs.TriggeredSendExternalKey triggered_send_external_key,
       stage_hash_exacttarget_SendJobs.SendDefinitionExternalKey send_definition_external_key,
       stage_hash_exacttarget_SendJobs.JobStatus job_status,
       stage_hash_exacttarget_SendJobs.PreviewURL preview_url,
       stage_hash_exacttarget_SendJobs.IsMultipart is_multipart,
       stage_hash_exacttarget_SendJobs.Additional additional,
       stage_hash_exacttarget_SendJobs.jan_one jan_one,
       stage_hash_exacttarget_SendJobs.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exacttarget_SendJobs.ClientID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_exacttarget_SendJobs.SendID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.FromName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.FromEmail,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_SendJobs.SchedTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_SendJobs.SentTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.Subject,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.EmailName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.TriggeredSendExternalKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.SendDefinitionExternalKey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.JobStatus,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.PreviewURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.IsMultipart,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_exacttarget_SendJobs.Additional,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_exacttarget_SendJobs.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exacttarget_SendJobs
 where stage_hash_exacttarget_SendJobs.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exacttarget_send_jobs records
set @insert_date_time = getdate()
insert into s_exacttarget_send_jobs (
       bk_hash,
       client_id,
       send_id,
       from_name,
       from_email,
       sched_time,
       sent_time,
       subject,
       email_name,
       triggered_send_external_key,
       send_definition_external_key,
       job_status,
       preview_url,
       is_multipart,
       additional,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exacttarget_send_jobs_inserts.bk_hash,
       #s_exacttarget_send_jobs_inserts.client_id,
       #s_exacttarget_send_jobs_inserts.send_id,
       #s_exacttarget_send_jobs_inserts.from_name,
       #s_exacttarget_send_jobs_inserts.from_email,
       #s_exacttarget_send_jobs_inserts.sched_time,
       #s_exacttarget_send_jobs_inserts.sent_time,
       #s_exacttarget_send_jobs_inserts.subject,
       #s_exacttarget_send_jobs_inserts.email_name,
       #s_exacttarget_send_jobs_inserts.triggered_send_external_key,
       #s_exacttarget_send_jobs_inserts.send_definition_external_key,
       #s_exacttarget_send_jobs_inserts.job_status,
       #s_exacttarget_send_jobs_inserts.preview_url,
       #s_exacttarget_send_jobs_inserts.is_multipart,
       #s_exacttarget_send_jobs_inserts.additional,
       #s_exacttarget_send_jobs_inserts.jan_one,
       case when s_exacttarget_send_jobs.s_exacttarget_send_jobs_id is null then isnull(#s_exacttarget_send_jobs_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #s_exacttarget_send_jobs_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exacttarget_send_jobs_inserts
  left join p_exacttarget_send_jobs
    on #s_exacttarget_send_jobs_inserts.bk_hash = p_exacttarget_send_jobs.bk_hash
   and p_exacttarget_send_jobs.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exacttarget_send_jobs
    on p_exacttarget_send_jobs.bk_hash = s_exacttarget_send_jobs.bk_hash
   and p_exacttarget_send_jobs.s_exacttarget_send_jobs_id = s_exacttarget_send_jobs.s_exacttarget_send_jobs_id
 where s_exacttarget_send_jobs.s_exacttarget_send_jobs_id is null
    or (s_exacttarget_send_jobs.s_exacttarget_send_jobs_id is not null
        and s_exacttarget_send_jobs.dv_hash <> #s_exacttarget_send_jobs_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exacttarget_send_jobs @current_dv_batch_id

end
