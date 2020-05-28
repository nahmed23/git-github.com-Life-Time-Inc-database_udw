CREATE PROC [dbo].[proc_etl_fitmetrix_api_activities] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_fitmetrix_api_activities

set @insert_date_time = getdate()
insert into dbo.stage_hash_fitmetrix_api_activities (
       bk_hash,
       ACTIVITYID,
       ACTIVITYNAME,
       ACTIVITYADDED,
       EXTERNALID,
       LEVEL,
       FACILITYID,
       ACTIVITYTYPEID,
       NEEDLOANERS,
       CHECKINTYPE,
       ALLOWRESERVATION,
       NOINTEGRATIONSYNC,
       ICON,
       ISASSESSMENT,
       IMAGE,
       ISDELETED,
       ISAPPOINTMENTACTIVITY,
       APPNAME,
       POSITION,
       [DELETE],
       APPIMAGE,
       APPICON,
       ISMANUALATTENDANCE,
       DURATIONMINUTES,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ACTIVITYID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ACTIVITYID,
       ACTIVITYNAME,
       ACTIVITYADDED,
       EXTERNALID,
       LEVEL,
       FACILITYID,
       ACTIVITYTYPEID,
       NEEDLOANERS,
       CHECKINTYPE,
       ALLOWRESERVATION,
       NOINTEGRATIONSYNC,
       ICON,
       ISASSESSMENT,
       IMAGE,
       ISDELETED,
       ISAPPOINTMENTACTIVITY,
       APPNAME,
       POSITION,
       [DELETE],
       APPIMAGE,
       APPICON,
       ISMANUALATTENDANCE,
       DURATIONMINUTES,
       dummy_modified_date_time,
       isnull(cast(stage_fitmetrix_api_activities.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_fitmetrix_api_activities
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_fitmetrix_api_activities @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_fitmetrix_api_activities (
       bk_hash,
       activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_fitmetrix_api_activities.bk_hash,
       stage_hash_fitmetrix_api_activities.ACTIVITYID activity_id,
       isnull(cast(stage_hash_fitmetrix_api_activities.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       29,
       @insert_date_time,
       @user
  from stage_hash_fitmetrix_api_activities
  left join h_fitmetrix_api_activities
    on stage_hash_fitmetrix_api_activities.bk_hash = h_fitmetrix_api_activities.bk_hash
 where h_fitmetrix_api_activities_id is null
   and stage_hash_fitmetrix_api_activities.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_fitmetrix_api_activities
if object_id('tempdb..#l_fitmetrix_api_activities_inserts') is not null drop table #l_fitmetrix_api_activities_inserts
create table #l_fitmetrix_api_activities_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_activities.bk_hash,
       stage_hash_fitmetrix_api_activities.ACTIVITYID activity_id,
       stage_hash_fitmetrix_api_activities.EXTERNALID external_id,
       stage_hash_fitmetrix_api_activities.FACILITYID facility_id,
       stage_hash_fitmetrix_api_activities.ACTIVITYTYPEID activity_type_id,
       isnull(cast(stage_hash_fitmetrix_api_activities.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_activities.ACTIVITYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.EXTERNALID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_activities.FACILITYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_activities.ACTIVITYTYPEID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_activities
 where stage_hash_fitmetrix_api_activities.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_fitmetrix_api_activities records
set @insert_date_time = getdate()
insert into l_fitmetrix_api_activities (
       bk_hash,
       activity_id,
       external_id,
       facility_id,
       activity_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_fitmetrix_api_activities_inserts.bk_hash,
       #l_fitmetrix_api_activities_inserts.activity_id,
       #l_fitmetrix_api_activities_inserts.external_id,
       #l_fitmetrix_api_activities_inserts.facility_id,
       #l_fitmetrix_api_activities_inserts.activity_type_id,
       case when l_fitmetrix_api_activities.l_fitmetrix_api_activities_id is null then isnull(#l_fitmetrix_api_activities_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #l_fitmetrix_api_activities_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_fitmetrix_api_activities_inserts
  left join p_fitmetrix_api_activities
    on #l_fitmetrix_api_activities_inserts.bk_hash = p_fitmetrix_api_activities.bk_hash
   and p_fitmetrix_api_activities.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_fitmetrix_api_activities
    on p_fitmetrix_api_activities.bk_hash = l_fitmetrix_api_activities.bk_hash
   and p_fitmetrix_api_activities.l_fitmetrix_api_activities_id = l_fitmetrix_api_activities.l_fitmetrix_api_activities_id
 where l_fitmetrix_api_activities.l_fitmetrix_api_activities_id is null
    or (l_fitmetrix_api_activities.l_fitmetrix_api_activities_id is not null
        and l_fitmetrix_api_activities.dv_hash <> #l_fitmetrix_api_activities_inserts.source_hash)

--calculate hash and lookup to current s_fitmetrix_api_activities
if object_id('tempdb..#s_fitmetrix_api_activities_inserts') is not null drop table #s_fitmetrix_api_activities_inserts
create table #s_fitmetrix_api_activities_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_activities.bk_hash,
       stage_hash_fitmetrix_api_activities.ACTIVITYID activity_id,
       stage_hash_fitmetrix_api_activities.ACTIVITYNAME activity_name,
       stage_hash_fitmetrix_api_activities.ACTIVITYADDED activity_added,
       stage_hash_fitmetrix_api_activities.LEVEL level,
       stage_hash_fitmetrix_api_activities.NEEDLOANERS need_loaners,
       stage_hash_fitmetrix_api_activities.CHECKINTYPE check_in_type,
       stage_hash_fitmetrix_api_activities.ALLOWRESERVATION allow_reservation,
       stage_hash_fitmetrix_api_activities.NOINTEGRATIONSYNC non_integration_sync,
       stage_hash_fitmetrix_api_activities.ICON icon,
       stage_hash_fitmetrix_api_activities.ISASSESSMENT is_assessment,
       stage_hash_fitmetrix_api_activities.IMAGE image,
       stage_hash_fitmetrix_api_activities.ISDELETED is_deleted,
       stage_hash_fitmetrix_api_activities.ISAPPOINTMENTACTIVITY is_appointment_activity,
       stage_hash_fitmetrix_api_activities.APPNAME app_name,
       stage_hash_fitmetrix_api_activities.POSITION position,
       stage_hash_fitmetrix_api_activities.[DELETE] [delete],
       stage_hash_fitmetrix_api_activities.APPIMAGE app_image,
       stage_hash_fitmetrix_api_activities.APPICON app_icon,
       stage_hash_fitmetrix_api_activities.ISMANUALATTENDANCE is_manual_attendance,
       stage_hash_fitmetrix_api_activities.DURATIONMINUTES duration_minutes,
       stage_hash_fitmetrix_api_activities.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_fitmetrix_api_activities.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_activities.ACTIVITYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.ACTIVITYNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.ACTIVITYADDED,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.LEVEL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.NEEDLOANERS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.CHECKINTYPE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.ALLOWRESERVATION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.NOINTEGRATIONSYNC,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.ICON,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.ISASSESSMENT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.IMAGE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.ISDELETED,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.ISAPPOINTMENTACTIVITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.APPNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_activities.POSITION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.[DELETE],'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.APPIMAGE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.APPICON,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_activities.ISMANUALATTENDANCE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_activities.DURATIONMINUTES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_fitmetrix_api_activities.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_activities
 where stage_hash_fitmetrix_api_activities.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_fitmetrix_api_activities records
set @insert_date_time = getdate()
insert into s_fitmetrix_api_activities (
       bk_hash,
       activity_id,
       activity_name,
       activity_added,
       level,
       need_loaners,
       check_in_type,
       allow_reservation,
       non_integration_sync,
       icon,
       is_assessment,
       image,
       is_deleted,
       is_appointment_activity,
       app_name,
       position,
       [delete],
       app_image,
       app_icon,
       is_manual_attendance,
       duration_minutes,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_fitmetrix_api_activities_inserts.bk_hash,
       #s_fitmetrix_api_activities_inserts.activity_id,
       #s_fitmetrix_api_activities_inserts.activity_name,
       #s_fitmetrix_api_activities_inserts.activity_added,
       #s_fitmetrix_api_activities_inserts.level,
       #s_fitmetrix_api_activities_inserts.need_loaners,
       #s_fitmetrix_api_activities_inserts.check_in_type,
       #s_fitmetrix_api_activities_inserts.allow_reservation,
       #s_fitmetrix_api_activities_inserts.non_integration_sync,
       #s_fitmetrix_api_activities_inserts.icon,
       #s_fitmetrix_api_activities_inserts.is_assessment,
       #s_fitmetrix_api_activities_inserts.image,
       #s_fitmetrix_api_activities_inserts.is_deleted,
       #s_fitmetrix_api_activities_inserts.is_appointment_activity,
       #s_fitmetrix_api_activities_inserts.app_name,
       #s_fitmetrix_api_activities_inserts.position,
       #s_fitmetrix_api_activities_inserts.[delete],
       #s_fitmetrix_api_activities_inserts.app_image,
       #s_fitmetrix_api_activities_inserts.app_icon,
       #s_fitmetrix_api_activities_inserts.is_manual_attendance,
       #s_fitmetrix_api_activities_inserts.duration_minutes,
       #s_fitmetrix_api_activities_inserts.dummy_modified_date_time,
       case when s_fitmetrix_api_activities.s_fitmetrix_api_activities_id is null then isnull(#s_fitmetrix_api_activities_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #s_fitmetrix_api_activities_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_fitmetrix_api_activities_inserts
  left join p_fitmetrix_api_activities
    on #s_fitmetrix_api_activities_inserts.bk_hash = p_fitmetrix_api_activities.bk_hash
   and p_fitmetrix_api_activities.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_fitmetrix_api_activities
    on p_fitmetrix_api_activities.bk_hash = s_fitmetrix_api_activities.bk_hash
   and p_fitmetrix_api_activities.s_fitmetrix_api_activities_id = s_fitmetrix_api_activities.s_fitmetrix_api_activities_id
 where s_fitmetrix_api_activities.s_fitmetrix_api_activities_id is null
    or (s_fitmetrix_api_activities.s_fitmetrix_api_activities_id is not null
        and s_fitmetrix_api_activities.dv_hash <> #s_fitmetrix_api_activities_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_fitmetrix_api_activities @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_fitmetrix_api_activities @current_dv_batch_id

end
