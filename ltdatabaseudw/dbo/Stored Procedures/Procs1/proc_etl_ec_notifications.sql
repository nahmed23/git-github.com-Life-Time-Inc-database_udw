CREATE PROC [dbo].[proc_etl_ec_notifications] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_Notifications

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_Notifications (
       bk_hash,
       NotificationId,
       [To],
       [From],
       Subject,
       Message,
       MessageType,
       Status,
       Received,
       SourceType,
       SourceId,
       CreatedDate,
       UpdatedDate,
       SourceThreadId,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(NotificationId as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       NotificationId,
       [To],
       [From],
       Subject,
       Message,
       MessageType,
       Status,
       Received,
       SourceType,
       SourceId,
       CreatedDate,
       UpdatedDate,
       SourceThreadId,
       isnull(cast(stage_ec_Notifications.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_Notifications
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_notifications @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_notifications (
       bk_hash,
       notification_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_Notifications.bk_hash,
       stage_hash_ec_Notifications.NotificationId notification_id,
       isnull(cast(stage_hash_ec_Notifications.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_Notifications
  left join h_ec_notifications
    on stage_hash_ec_Notifications.bk_hash = h_ec_notifications.bk_hash
 where h_ec_notifications_id is null
   and stage_hash_ec_Notifications.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ec_notifications
if object_id('tempdb..#l_ec_notifications_inserts') is not null drop table #l_ec_notifications_inserts
create table #l_ec_notifications_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Notifications.bk_hash,
       stage_hash_ec_Notifications.NotificationId notification_id,
       stage_hash_ec_Notifications.SourceId source_id,
       stage_hash_ec_Notifications.SourceThreadId source_thread_id,
       isnull(cast(stage_hash_ec_Notifications.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_Notifications.NotificationId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Notifications.SourceId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Notifications.SourceThreadId,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Notifications
 where stage_hash_ec_Notifications.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ec_notifications records
set @insert_date_time = getdate()
insert into l_ec_notifications (
       bk_hash,
       notification_id,
       source_id,
       source_thread_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ec_notifications_inserts.bk_hash,
       #l_ec_notifications_inserts.notification_id,
       #l_ec_notifications_inserts.source_id,
       #l_ec_notifications_inserts.source_thread_id,
       case when l_ec_notifications.l_ec_notifications_id is null then isnull(#l_ec_notifications_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #l_ec_notifications_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ec_notifications_inserts
  left join p_ec_notifications
    on #l_ec_notifications_inserts.bk_hash = p_ec_notifications.bk_hash
   and p_ec_notifications.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ec_notifications
    on p_ec_notifications.bk_hash = l_ec_notifications.bk_hash
   and p_ec_notifications.l_ec_notifications_id = l_ec_notifications.l_ec_notifications_id
 where l_ec_notifications.l_ec_notifications_id is null
    or (l_ec_notifications.l_ec_notifications_id is not null
        and l_ec_notifications.dv_hash <> #l_ec_notifications_inserts.source_hash)

--calculate hash and lookup to current s_ec_notifications
if object_id('tempdb..#s_ec_notifications_inserts') is not null drop table #s_ec_notifications_inserts
create table #s_ec_notifications_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Notifications.bk_hash,
       stage_hash_ec_Notifications.NotificationId notification_id,
       stage_hash_ec_Notifications.[To] [to],
       stage_hash_ec_Notifications.[From] [from],
       stage_hash_ec_Notifications.Subject subject,
       stage_hash_ec_Notifications.Message message,
       stage_hash_ec_Notifications.MessageType message_type,
       stage_hash_ec_Notifications.Status status,
       stage_hash_ec_Notifications.Received received,
       stage_hash_ec_Notifications.SourceType source_type,
       stage_hash_ec_Notifications.CreatedDate created_date,
       stage_hash_ec_Notifications.UpdatedDate updated_date,
       isnull(cast(stage_hash_ec_Notifications.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_Notifications.NotificationId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Notifications.[To] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Notifications.[From] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Notifications.Subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Notifications.Message,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Notifications.MessageType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Notifications.Status as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Notifications.Received,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Notifications.SourceType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Notifications.CreatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Notifications.UpdatedDate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Notifications
 where stage_hash_ec_Notifications.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_notifications records
set @insert_date_time = getdate()
insert into s_ec_notifications (
       bk_hash,
       notification_id,
       [to],
       [from],
       subject,
       message,
       message_type,
       status,
       received,
       source_type,
       created_date,
       updated_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_notifications_inserts.bk_hash,
       #s_ec_notifications_inserts.notification_id,
       #s_ec_notifications_inserts.[to],
       #s_ec_notifications_inserts.[from],
       #s_ec_notifications_inserts.subject,
       #s_ec_notifications_inserts.message,
       #s_ec_notifications_inserts.message_type,
       #s_ec_notifications_inserts.status,
       #s_ec_notifications_inserts.received,
       #s_ec_notifications_inserts.source_type,
       #s_ec_notifications_inserts.created_date,
       #s_ec_notifications_inserts.updated_date,
       case when s_ec_notifications.s_ec_notifications_id is null then isnull(#s_ec_notifications_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_notifications_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_notifications_inserts
  left join p_ec_notifications
    on #s_ec_notifications_inserts.bk_hash = p_ec_notifications.bk_hash
   and p_ec_notifications.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_notifications
    on p_ec_notifications.bk_hash = s_ec_notifications.bk_hash
   and p_ec_notifications.s_ec_notifications_id = s_ec_notifications.s_ec_notifications_id
 where s_ec_notifications.s_ec_notifications_id is null
    or (s_ec_notifications.s_ec_notifications_id is not null
        and s_ec_notifications.dv_hash <> #s_ec_notifications_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_notifications @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_notifications @current_dv_batch_id

end
