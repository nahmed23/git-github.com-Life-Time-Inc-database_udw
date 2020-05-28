CREATE PROC [dbo].[proc_etl_mms_drawer_activity] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_DrawerActivity

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_DrawerActivity (
       bk_hash,
       DrawerActivityID,
       DrawerID,
       OpenDateTime,
       CloseDateTime,
       OpenEmployeeID,
       CloseEmployeeID,
       ValDrawerStatusID,
       UTCOpenDateTime,
       OpenDateTimeZone,
       UTCCloseDateTime,
       CloseDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       PendDateTime,
       PendEmployeeID,
       PendDateTimeZone,
       UTCPendDateTime,
       ClosingComments,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(DrawerActivityID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       DrawerActivityID,
       DrawerID,
       OpenDateTime,
       CloseDateTime,
       OpenEmployeeID,
       CloseEmployeeID,
       ValDrawerStatusID,
       UTCOpenDateTime,
       OpenDateTimeZone,
       UTCCloseDateTime,
       CloseDateTimeZone,
       InsertedDateTime,
       UpdatedDateTime,
       PendDateTime,
       PendEmployeeID,
       PendDateTimeZone,
       UTCPendDateTime,
       ClosingComments,
       isnull(cast(stage_mms_DrawerActivity.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_DrawerActivity
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_drawer_activity @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_drawer_activity (
       bk_hash,
       drawer_activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_DrawerActivity.bk_hash,
       stage_hash_mms_DrawerActivity.DrawerActivityID drawer_activity_id,
       isnull(cast(stage_hash_mms_DrawerActivity.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_DrawerActivity
  left join h_mms_drawer_activity
    on stage_hash_mms_DrawerActivity.bk_hash = h_mms_drawer_activity.bk_hash
 where h_mms_drawer_activity_id is null
   and stage_hash_mms_DrawerActivity.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_drawer_activity
if object_id('tempdb..#l_mms_drawer_activity_inserts') is not null drop table #l_mms_drawer_activity_inserts
create table #l_mms_drawer_activity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_DrawerActivity.bk_hash,
       stage_hash_mms_DrawerActivity.DrawerActivityID drawer_activity_id,
       stage_hash_mms_DrawerActivity.DrawerID drawer_id,
       stage_hash_mms_DrawerActivity.OpenEmployeeID open_employee_id,
       stage_hash_mms_DrawerActivity.CloseEmployeeID close_employee_id,
       stage_hash_mms_DrawerActivity.PendEmployeeID pend_employee_id,
       stage_hash_mms_DrawerActivity.ValDrawerStatusID val_drawer_status_id,
       isnull(cast(stage_hash_mms_DrawerActivity.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerActivity.DrawerActivityID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerActivity.DrawerID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerActivity.OpenEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerActivity.CloseEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerActivity.PendEmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerActivity.ValDrawerStatusID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_DrawerActivity
 where stage_hash_mms_DrawerActivity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_drawer_activity records
set @insert_date_time = getdate()
insert into l_mms_drawer_activity (
       bk_hash,
       drawer_activity_id,
       drawer_id,
       open_employee_id,
       close_employee_id,
       pend_employee_id,
       val_drawer_status_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_drawer_activity_inserts.bk_hash,
       #l_mms_drawer_activity_inserts.drawer_activity_id,
       #l_mms_drawer_activity_inserts.drawer_id,
       #l_mms_drawer_activity_inserts.open_employee_id,
       #l_mms_drawer_activity_inserts.close_employee_id,
       #l_mms_drawer_activity_inserts.pend_employee_id,
       #l_mms_drawer_activity_inserts.val_drawer_status_id,
       case when l_mms_drawer_activity.l_mms_drawer_activity_id is null then isnull(#l_mms_drawer_activity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_drawer_activity_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_drawer_activity_inserts
  left join p_mms_drawer_activity
    on #l_mms_drawer_activity_inserts.bk_hash = p_mms_drawer_activity.bk_hash
   and p_mms_drawer_activity.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_drawer_activity
    on p_mms_drawer_activity.bk_hash = l_mms_drawer_activity.bk_hash
   and p_mms_drawer_activity.l_mms_drawer_activity_id = l_mms_drawer_activity.l_mms_drawer_activity_id
 where l_mms_drawer_activity.l_mms_drawer_activity_id is null
    or (l_mms_drawer_activity.l_mms_drawer_activity_id is not null
        and l_mms_drawer_activity.dv_hash <> #l_mms_drawer_activity_inserts.source_hash)

--calculate hash and lookup to current s_mms_drawer_activity
if object_id('tempdb..#s_mms_drawer_activity_inserts') is not null drop table #s_mms_drawer_activity_inserts
create table #s_mms_drawer_activity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_DrawerActivity.bk_hash,
       stage_hash_mms_DrawerActivity.DrawerActivityID drawer_activity_id,
       stage_hash_mms_DrawerActivity.OpenDateTime open_date_time,
       stage_hash_mms_DrawerActivity.CloseDateTime close_date_time,
       stage_hash_mms_DrawerActivity.UTCOpenDateTime utc_open_date_time,
       stage_hash_mms_DrawerActivity.OpenDateTimeZone open_date_time_zone,
       stage_hash_mms_DrawerActivity.UTCCloseDateTime utc_close_date_time,
       stage_hash_mms_DrawerActivity.CloseDateTimeZone close_date_time_zone,
       stage_hash_mms_DrawerActivity.InsertedDateTime inserted_date_time,
       stage_hash_mms_DrawerActivity.UpdatedDateTime updated_date_time,
       stage_hash_mms_DrawerActivity.PendDateTime pend_date_time,
       stage_hash_mms_DrawerActivity.PendDateTimeZone pend_date_time_zone,
       stage_hash_mms_DrawerActivity.UTCPendDateTime utc_pend_date_time,
       stage_hash_mms_DrawerActivity.ClosingComments closing_comments,
       isnull(cast(stage_hash_mms_DrawerActivity.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_DrawerActivity.DrawerActivityID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerActivity.OpenDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerActivity.CloseDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerActivity.UTCOpenDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_DrawerActivity.OpenDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerActivity.UTCCloseDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_DrawerActivity.CloseDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerActivity.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerActivity.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerActivity.PendDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_DrawerActivity.PendDateTimeZone,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_DrawerActivity.UTCPendDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_DrawerActivity.ClosingComments,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_DrawerActivity
 where stage_hash_mms_DrawerActivity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_drawer_activity records
set @insert_date_time = getdate()
insert into s_mms_drawer_activity (
       bk_hash,
       drawer_activity_id,
       open_date_time,
       close_date_time,
       utc_open_date_time,
       open_date_time_zone,
       utc_close_date_time,
       close_date_time_zone,
       inserted_date_time,
       updated_date_time,
       pend_date_time,
       pend_date_time_zone,
       utc_pend_date_time,
       closing_comments,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_drawer_activity_inserts.bk_hash,
       #s_mms_drawer_activity_inserts.drawer_activity_id,
       #s_mms_drawer_activity_inserts.open_date_time,
       #s_mms_drawer_activity_inserts.close_date_time,
       #s_mms_drawer_activity_inserts.utc_open_date_time,
       #s_mms_drawer_activity_inserts.open_date_time_zone,
       #s_mms_drawer_activity_inserts.utc_close_date_time,
       #s_mms_drawer_activity_inserts.close_date_time_zone,
       #s_mms_drawer_activity_inserts.inserted_date_time,
       #s_mms_drawer_activity_inserts.updated_date_time,
       #s_mms_drawer_activity_inserts.pend_date_time,
       #s_mms_drawer_activity_inserts.pend_date_time_zone,
       #s_mms_drawer_activity_inserts.utc_pend_date_time,
       #s_mms_drawer_activity_inserts.closing_comments,
       case when s_mms_drawer_activity.s_mms_drawer_activity_id is null then isnull(#s_mms_drawer_activity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_drawer_activity_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_drawer_activity_inserts
  left join p_mms_drawer_activity
    on #s_mms_drawer_activity_inserts.bk_hash = p_mms_drawer_activity.bk_hash
   and p_mms_drawer_activity.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_drawer_activity
    on p_mms_drawer_activity.bk_hash = s_mms_drawer_activity.bk_hash
   and p_mms_drawer_activity.s_mms_drawer_activity_id = s_mms_drawer_activity.s_mms_drawer_activity_id
 where s_mms_drawer_activity.s_mms_drawer_activity_id is null
    or (s_mms_drawer_activity.s_mms_drawer_activity_id is not null
        and s_mms_drawer_activity.dv_hash <> #s_mms_drawer_activity_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_drawer_activity @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_drawer_activity @current_dv_batch_id

end
