CREATE PROC [dbo].[proc_etl_mms_membership_message] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_MembershipMessage

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_MembershipMessage (
       bk_hash,
       MembershipMessageID,
       MembershipID,
       OpenEmployeeID,
       CloseEmployeeID,
       OpenDateTime,
       CloseDateTime,
       ValMembershipMessageTypeID,
       ValMessageStatusID,
       ReceivedDateTime,
       Comment,
       UTCOpenDateTime,
       OpenDateTimeZone,
       UTCCloseDateTime,
       CloseDateTimeZone,
       UTCReceivedDateTime,
       ReceivedDateTimeZone,
       InsertedDateTime,
       OpenClubID,
       CloseClubID,
       UpdatedDateTime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(MembershipMessageID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       MembershipMessageID,
       MembershipID,
       OpenEmployeeID,
       CloseEmployeeID,
       OpenDateTime,
       CloseDateTime,
       ValMembershipMessageTypeID,
       ValMessageStatusID,
       ReceivedDateTime,
       Comment,
       UTCOpenDateTime,
       OpenDateTimeZone,
       UTCCloseDateTime,
       CloseDateTimeZone,
       UTCReceivedDateTime,
       ReceivedDateTimeZone,
       InsertedDateTime,
       OpenClubID,
       CloseClubID,
       UpdatedDateTime,
       isnull(cast(stage_mms_MembershipMessage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_MembershipMessage
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_membership_message @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_membership_message (
       bk_hash,
       membership_message_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_MembershipMessage.bk_hash,
       stage_hash_mms_MembershipMessage.MembershipMessageID membership_message_id,
       isnull(cast(stage_hash_mms_MembershipMessage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_MembershipMessage
  left join h_mms_membership_message
    on stage_hash_mms_MembershipMessage.bk_hash = h_mms_membership_message.bk_hash
 where h_mms_membership_message_id is null
   and stage_hash_mms_MembershipMessage.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_membership_message
if object_id('tempdb..#l_mms_membership_message_inserts') is not null drop table #l_mms_membership_message_inserts
create table #l_mms_membership_message_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipMessage.bk_hash,
       stage_hash_mms_MembershipMessage.MembershipMessageID membership_message_id,
       stage_hash_mms_MembershipMessage.MembershipID membership_id,
       stage_hash_mms_MembershipMessage.OpenEmployeeID open_employee_id,
       stage_hash_mms_MembershipMessage.CloseEmployeeID close_employee_id,
       stage_hash_mms_MembershipMessage.ValMembershipMessageTypeID val_membership_message_type_id,
       stage_hash_mms_MembershipMessage.ValMessageStatusID val_message_status_id,
       stage_hash_mms_MembershipMessage.OpenClubID open_club_id,
       stage_hash_mms_MembershipMessage.CloseClubID close_club_id,
       isnull(cast(stage_hash_mms_MembershipMessage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.MembershipMessageID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.MembershipID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.OpenEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.CloseEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.ValMembershipMessageTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.ValMessageStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.OpenClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.CloseClubID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipMessage
 where stage_hash_mms_MembershipMessage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_membership_message records
set @insert_date_time = getdate()
insert into l_mms_membership_message (
       bk_hash,
       membership_message_id,
       membership_id,
       open_employee_id,
       close_employee_id,
       val_membership_message_type_id,
       val_message_status_id,
       open_club_id,
       close_club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_membership_message_inserts.bk_hash,
       #l_mms_membership_message_inserts.membership_message_id,
       #l_mms_membership_message_inserts.membership_id,
       #l_mms_membership_message_inserts.open_employee_id,
       #l_mms_membership_message_inserts.close_employee_id,
       #l_mms_membership_message_inserts.val_membership_message_type_id,
       #l_mms_membership_message_inserts.val_message_status_id,
       #l_mms_membership_message_inserts.open_club_id,
       #l_mms_membership_message_inserts.close_club_id,
       case when l_mms_membership_message.l_mms_membership_message_id is null then isnull(#l_mms_membership_message_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_membership_message_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_membership_message_inserts
  left join p_mms_membership_message
    on #l_mms_membership_message_inserts.bk_hash = p_mms_membership_message.bk_hash
   and p_mms_membership_message.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_membership_message
    on p_mms_membership_message.bk_hash = l_mms_membership_message.bk_hash
   and p_mms_membership_message.l_mms_membership_message_id = l_mms_membership_message.l_mms_membership_message_id
 where l_mms_membership_message.l_mms_membership_message_id is null
    or (l_mms_membership_message.l_mms_membership_message_id is not null
        and l_mms_membership_message.dv_hash <> #l_mms_membership_message_inserts.source_hash)

--calculate hash and lookup to current s_mms_membership_message
if object_id('tempdb..#s_mms_membership_message_inserts') is not null drop table #s_mms_membership_message_inserts
create table #s_mms_membership_message_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_MembershipMessage.bk_hash,
       stage_hash_mms_MembershipMessage.MembershipMessageID membership_message_id,
       stage_hash_mms_MembershipMessage.OpenDateTime open_date_time,
       stage_hash_mms_MembershipMessage.CloseDateTime close_date_time,
       stage_hash_mms_MembershipMessage.ReceivedDateTime received_date_time,
       stage_hash_mms_MembershipMessage.Comment comment,
       stage_hash_mms_MembershipMessage.UTCOpenDateTime utc_open_date_time,
       stage_hash_mms_MembershipMessage.OpenDateTimeZone open_date_time_zone,
       stage_hash_mms_MembershipMessage.UTCCloseDateTime utc_close_date_time,
       stage_hash_mms_MembershipMessage.CloseDateTimeZone close_date_time_zone,
       stage_hash_mms_MembershipMessage.UTCReceivedDateTime utc_received_date_time,
       stage_hash_mms_MembershipMessage.ReceivedDateTimeZone received_date_time_zone,
       stage_hash_mms_MembershipMessage.InsertedDateTime inserted_date_time,
       stage_hash_mms_MembershipMessage.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_MembershipMessage.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_MembershipMessage.MembershipMessageID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipMessage.OpenDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipMessage.CloseDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipMessage.ReceivedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipMessage.Comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipMessage.UTCOpenDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipMessage.OpenDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipMessage.UTCCloseDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipMessage.CloseDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipMessage.UTCReceivedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_MembershipMessage.ReceivedDateTimeZone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipMessage.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_MembershipMessage.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_MembershipMessage
 where stage_hash_mms_MembershipMessage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_membership_message records
set @insert_date_time = getdate()
insert into s_mms_membership_message (
       bk_hash,
       membership_message_id,
       open_date_time,
       close_date_time,
       received_date_time,
       comment,
       utc_open_date_time,
       open_date_time_zone,
       utc_close_date_time,
       close_date_time_zone,
       utc_received_date_time,
       received_date_time_zone,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_membership_message_inserts.bk_hash,
       #s_mms_membership_message_inserts.membership_message_id,
       #s_mms_membership_message_inserts.open_date_time,
       #s_mms_membership_message_inserts.close_date_time,
       #s_mms_membership_message_inserts.received_date_time,
       #s_mms_membership_message_inserts.comment,
       #s_mms_membership_message_inserts.utc_open_date_time,
       #s_mms_membership_message_inserts.open_date_time_zone,
       #s_mms_membership_message_inserts.utc_close_date_time,
       #s_mms_membership_message_inserts.close_date_time_zone,
       #s_mms_membership_message_inserts.utc_received_date_time,
       #s_mms_membership_message_inserts.received_date_time_zone,
       #s_mms_membership_message_inserts.inserted_date_time,
       #s_mms_membership_message_inserts.updated_date_time,
       case when s_mms_membership_message.s_mms_membership_message_id is null then isnull(#s_mms_membership_message_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_membership_message_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_membership_message_inserts
  left join p_mms_membership_message
    on #s_mms_membership_message_inserts.bk_hash = p_mms_membership_message.bk_hash
   and p_mms_membership_message.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_membership_message
    on p_mms_membership_message.bk_hash = s_mms_membership_message.bk_hash
   and p_mms_membership_message.s_mms_membership_message_id = s_mms_membership_message.s_mms_membership_message_id
 where s_mms_membership_message.s_mms_membership_message_id is null
    or (s_mms_membership_message.s_mms_membership_message_id is not null
        and s_mms_membership_message.dv_hash <> #s_mms_membership_message_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_membership_message @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_membership_message_history @current_dv_batch_id

end
