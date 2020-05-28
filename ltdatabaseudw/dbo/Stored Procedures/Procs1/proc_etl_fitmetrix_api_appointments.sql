CREATE PROC [dbo].[proc_etl_fitmetrix_api_appointments] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_fitmetrix_api_appointments

set @insert_date_time = getdate()
insert into dbo.stage_hash_fitmetrix_api_appointments (
       bk_hash,
       APPOINTMENTID,
       NAME,
       FACILITYLOCATIONID,
       ACTIVITYID,
       ACTIVE,
       DESCRIPTION,
       INSTRUCTORID,
       STARTDATETIME,
       ENDDATETIME,
       EXTERNALID,
       EXTERNALIDALT,
       ISAVAILABLE,
       HIDDEN,
       ISCANCELLED,
       ISENROLLED,
       ISWAITLISTAVAILABLE,
       MAXCAPACITY,
       TOTALBOOKED,
       TOTALBOOKEDWAITLIST,
       WEBBOOKED,
       WEBBOOKEDCAPACITY,
       MESSAGES,
       DATEMODIFIED,
       DATEADDED,
       FACILITYLOCATIONRESOURCEID,
       DATECOMPLETED,
       COLORCODE,
       ACTIVITYTYPEID,
       ADDITIONALNOTES,
       STARTED,
       ISENROLLMENT,
       ISSUBSTITUTE,
       CANCELOFFSET,
       PTPCOMPLETED,
       ISAPPOINTMENT,
       INSTRUCTORFIRSTNAME,
       INSTRUCTORLASTNAME,
       INSTRUCTORIMAGE,
       INSTRUCTORGENDER,
       OPENSPOTS,
       BOOKEDSPOTS,
       WAITLISTSIZE,
       PTPNOTEST,
       CHECKEDIN,
       WORKOUTID,
       APP,
       APPICON,
       MANUALCAPACITY,
       ISEDITABLE,
       EXTERNALID_base64_decoded,
       EXTERNALIDALT_base64_decoded,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(APPOINTMENTID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       APPOINTMENTID,
       NAME,
       FACILITYLOCATIONID,
       ACTIVITYID,
       ACTIVE,
       DESCRIPTION,
       INSTRUCTORID,
       STARTDATETIME,
       ENDDATETIME,
       EXTERNALID,
       EXTERNALIDALT,
       ISAVAILABLE,
       HIDDEN,
       ISCANCELLED,
       ISENROLLED,
       ISWAITLISTAVAILABLE,
       MAXCAPACITY,
       TOTALBOOKED,
       TOTALBOOKEDWAITLIST,
       WEBBOOKED,
       WEBBOOKEDCAPACITY,
       MESSAGES,
       DATEMODIFIED,
       DATEADDED,
       FACILITYLOCATIONRESOURCEID,
       DATECOMPLETED,
       COLORCODE,
       ACTIVITYTYPEID,
       ADDITIONALNOTES,
       STARTED,
       ISENROLLMENT,
       ISSUBSTITUTE,
       CANCELOFFSET,
       PTPCOMPLETED,
       ISAPPOINTMENT,
       INSTRUCTORFIRSTNAME,
       INSTRUCTORLASTNAME,
       INSTRUCTORIMAGE,
       INSTRUCTORGENDER,
       OPENSPOTS,
       BOOKEDSPOTS,
       WAITLISTSIZE,
       PTPNOTEST,
       CHECKEDIN,
       WORKOUTID,
       APP,
       APPICON,
       MANUALCAPACITY,
       ISEDITABLE,
       EXTERNALID_base64_decoded,
       EXTERNALIDALT_base64_decoded,
       dummy_modified_date_time,
       isnull(cast(stage_fitmetrix_api_appointments.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_fitmetrix_api_appointments
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_fitmetrix_api_appointments @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_fitmetrix_api_appointments (
       bk_hash,
       appointment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_fitmetrix_api_appointments.bk_hash,
       stage_hash_fitmetrix_api_appointments.APPOINTMENTID appointment_id,
       isnull(cast(stage_hash_fitmetrix_api_appointments.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       29,
       @insert_date_time,
       @user
  from stage_hash_fitmetrix_api_appointments
  left join h_fitmetrix_api_appointments
    on stage_hash_fitmetrix_api_appointments.bk_hash = h_fitmetrix_api_appointments.bk_hash
 where h_fitmetrix_api_appointments_id is null
   and stage_hash_fitmetrix_api_appointments.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_fitmetrix_api_appointments
if object_id('tempdb..#l_fitmetrix_api_appointments_inserts') is not null drop table #l_fitmetrix_api_appointments_inserts
create table #l_fitmetrix_api_appointments_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_appointments.bk_hash,
       stage_hash_fitmetrix_api_appointments.APPOINTMENTID appointment_id,
       stage_hash_fitmetrix_api_appointments.FACILITYLOCATIONID facility_location_id,
       stage_hash_fitmetrix_api_appointments.ACTIVITYID activity_id,
       stage_hash_fitmetrix_api_appointments.INSTRUCTORID instructor_id,
       stage_hash_fitmetrix_api_appointments.EXTERNALID external_id,
       stage_hash_fitmetrix_api_appointments.EXTERNALIDALT external_id_alt,
       stage_hash_fitmetrix_api_appointments.FACILITYLOCATIONRESOURCEID facility_location_resource_id,
       stage_hash_fitmetrix_api_appointments.ACTIVITYTYPEID activity_type_id,
       stage_hash_fitmetrix_api_appointments.WORKOUTID workout_id,
       stage_hash_fitmetrix_api_appointments.EXTERNALID_base64_decoded external_id_base64_decoded,
       stage_hash_fitmetrix_api_appointments.EXTERNALIDALT_base64_decoded external_id_alternate_base64_decoded,
       isnull(cast(stage_hash_fitmetrix_api_appointments.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.APPOINTMENTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.FACILITYLOCATIONID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.ACTIVITYID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.INSTRUCTORID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.EXTERNALID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.EXTERNALIDALT,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.FACILITYLOCATIONRESOURCEID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.ACTIVITYTYPEID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.WORKOUTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_fitmetrix_api_appointments.EXTERNALID_base64_decoded, 2),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_fitmetrix_api_appointments.EXTERNALIDALT_base64_decoded, 2),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_appointments
 where stage_hash_fitmetrix_api_appointments.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_fitmetrix_api_appointments records
set @insert_date_time = getdate()
insert into l_fitmetrix_api_appointments (
       bk_hash,
       appointment_id,
       facility_location_id,
       activity_id,
       instructor_id,
       external_id,
       external_id_alt,
       facility_location_resource_id,
       activity_type_id,
       workout_id,
       external_id_base64_decoded,
       external_id_alternate_base64_decoded,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_fitmetrix_api_appointments_inserts.bk_hash,
       #l_fitmetrix_api_appointments_inserts.appointment_id,
       #l_fitmetrix_api_appointments_inserts.facility_location_id,
       #l_fitmetrix_api_appointments_inserts.activity_id,
       #l_fitmetrix_api_appointments_inserts.instructor_id,
       #l_fitmetrix_api_appointments_inserts.external_id,
       #l_fitmetrix_api_appointments_inserts.external_id_alt,
       #l_fitmetrix_api_appointments_inserts.facility_location_resource_id,
       #l_fitmetrix_api_appointments_inserts.activity_type_id,
       #l_fitmetrix_api_appointments_inserts.workout_id,
       #l_fitmetrix_api_appointments_inserts.external_id_base64_decoded,
       #l_fitmetrix_api_appointments_inserts.external_id_alternate_base64_decoded,
       case when l_fitmetrix_api_appointments.l_fitmetrix_api_appointments_id is null then isnull(#l_fitmetrix_api_appointments_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #l_fitmetrix_api_appointments_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_fitmetrix_api_appointments_inserts
  left join p_fitmetrix_api_appointments
    on #l_fitmetrix_api_appointments_inserts.bk_hash = p_fitmetrix_api_appointments.bk_hash
   and p_fitmetrix_api_appointments.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_fitmetrix_api_appointments
    on p_fitmetrix_api_appointments.bk_hash = l_fitmetrix_api_appointments.bk_hash
   and p_fitmetrix_api_appointments.l_fitmetrix_api_appointments_id = l_fitmetrix_api_appointments.l_fitmetrix_api_appointments_id
 where l_fitmetrix_api_appointments.l_fitmetrix_api_appointments_id is null
    or (l_fitmetrix_api_appointments.l_fitmetrix_api_appointments_id is not null
        and l_fitmetrix_api_appointments.dv_hash <> #l_fitmetrix_api_appointments_inserts.source_hash)

--calculate hash and lookup to current s_fitmetrix_api_appointments
if object_id('tempdb..#s_fitmetrix_api_appointments_inserts') is not null drop table #s_fitmetrix_api_appointments_inserts
create table #s_fitmetrix_api_appointments_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_appointments.bk_hash,
       stage_hash_fitmetrix_api_appointments.APPOINTMENTID appointment_id,
       stage_hash_fitmetrix_api_appointments.NAME name,
       stage_hash_fitmetrix_api_appointments.ACTIVE active,
       stage_hash_fitmetrix_api_appointments.DESCRIPTION description,
       stage_hash_fitmetrix_api_appointments.STARTDATETIME start_date_time,
       stage_hash_fitmetrix_api_appointments.ENDDATETIME end_date_time,
       stage_hash_fitmetrix_api_appointments.ISAVAILABLE is_available,
       stage_hash_fitmetrix_api_appointments.HIDDEN hidden,
       stage_hash_fitmetrix_api_appointments.ISCANCELLED is_cancelled,
       stage_hash_fitmetrix_api_appointments.ISENROLLED is_enrolled,
       stage_hash_fitmetrix_api_appointments.ISWAITLISTAVAILABLE is_wait_list_available,
       stage_hash_fitmetrix_api_appointments.MAXCAPACITY max_capacity,
       stage_hash_fitmetrix_api_appointments.TOTALBOOKED total_booked,
       stage_hash_fitmetrix_api_appointments.TOTALBOOKEDWAITLIST total_booked_wait_list,
       stage_hash_fitmetrix_api_appointments.WEBBOOKED web_booked,
       stage_hash_fitmetrix_api_appointments.WEBBOOKEDCAPACITY web_booked_capacity,
       stage_hash_fitmetrix_api_appointments.MESSAGES message,
       stage_hash_fitmetrix_api_appointments.DATEMODIFIED date_modified,
       stage_hash_fitmetrix_api_appointments.DATEADDED date_added,
       stage_hash_fitmetrix_api_appointments.DATECOMPLETED date_completed,
       stage_hash_fitmetrix_api_appointments.COLORCODE color_code,
       stage_hash_fitmetrix_api_appointments.ADDITIONALNOTES additional_notes,
       stage_hash_fitmetrix_api_appointments.STARTED started,
       stage_hash_fitmetrix_api_appointments.ISENROLLMENT is_enrollment,
       stage_hash_fitmetrix_api_appointments.ISSUBSTITUTE is_substitute,
       stage_hash_fitmetrix_api_appointments.CANCELOFFSET cancel_offset,
       stage_hash_fitmetrix_api_appointments.PTPCOMPLETED ptp_completed,
       stage_hash_fitmetrix_api_appointments.ISAPPOINTMENT is_appointment,
       stage_hash_fitmetrix_api_appointments.INSTRUCTORFIRSTNAME instructor_first_name,
       stage_hash_fitmetrix_api_appointments.INSTRUCTORLASTNAME instructor_last_name,
       stage_hash_fitmetrix_api_appointments.INSTRUCTORIMAGE instructor_image,
       stage_hash_fitmetrix_api_appointments.INSTRUCTORGENDER instructor_gender,
       stage_hash_fitmetrix_api_appointments.OPENSPOTS open_spots,
       stage_hash_fitmetrix_api_appointments.BOOKEDSPOTS booked_spots,
       stage_hash_fitmetrix_api_appointments.WAITLISTSIZE wait_list_size,
       stage_hash_fitmetrix_api_appointments.PTPNOTEST pt_no_test,
       stage_hash_fitmetrix_api_appointments.CHECKEDIN checked_in,
       stage_hash_fitmetrix_api_appointments.APP app,
       stage_hash_fitmetrix_api_appointments.APPICON app_icon,
       stage_hash_fitmetrix_api_appointments.MANUALCAPACITY manual_capacity,
       stage_hash_fitmetrix_api_appointments.ISEDITABLE is_editable,
       stage_hash_fitmetrix_api_appointments.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_fitmetrix_api_appointments.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.APPOINTMENTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.NAME,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ACTIVE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.DESCRIPTION,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.STARTDATETIME,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ENDDATETIME,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ISAVAILABLE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.HIDDEN,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ISCANCELLED,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ISENROLLED,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ISWAITLISTAVAILABLE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.MAXCAPACITY as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.TOTALBOOKED as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.TOTALBOOKEDWAITLIST as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.WEBBOOKED as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.WEBBOOKEDCAPACITY as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.MESSAGES,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.DATEMODIFIED,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.DATEADDED,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.DATECOMPLETED,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.COLORCODE as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ADDITIONALNOTES,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.STARTED,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ISENROLLMENT,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ISSUBSTITUTE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.CANCELOFFSET as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.PTPCOMPLETED,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ISAPPOINTMENT,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.INSTRUCTORFIRSTNAME,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.INSTRUCTORLASTNAME,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.INSTRUCTORIMAGE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.INSTRUCTORGENDER,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.OPENSPOTS,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.BOOKEDSPOTS,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.WAITLISTSIZE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.PTPNOTEST,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointments.CHECKEDIN as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.APP,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.APPICON,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.MANUALCAPACITY,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointments.ISEDITABLE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_fitmetrix_api_appointments.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_appointments
 where stage_hash_fitmetrix_api_appointments.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_fitmetrix_api_appointments records
set @insert_date_time = getdate()
insert into s_fitmetrix_api_appointments (
       bk_hash,
       appointment_id,
       name,
       active,
       description,
       start_date_time,
       end_date_time,
       is_available,
       hidden,
       is_cancelled,
       is_enrolled,
       is_wait_list_available,
       max_capacity,
       total_booked,
       total_booked_wait_list,
       web_booked,
       web_booked_capacity,
       message,
       date_modified,
       date_added,
       date_completed,
       color_code,
       additional_notes,
       started,
       is_enrollment,
       is_substitute,
       cancel_offset,
       ptp_completed,
       is_appointment,
       instructor_first_name,
       instructor_last_name,
       instructor_image,
       instructor_gender,
       open_spots,
       booked_spots,
       wait_list_size,
       pt_no_test,
       checked_in,
       app,
       app_icon,
       manual_capacity,
       is_editable,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_fitmetrix_api_appointments_inserts.bk_hash,
       #s_fitmetrix_api_appointments_inserts.appointment_id,
       #s_fitmetrix_api_appointments_inserts.name,
       #s_fitmetrix_api_appointments_inserts.active,
       #s_fitmetrix_api_appointments_inserts.description,
       #s_fitmetrix_api_appointments_inserts.start_date_time,
       #s_fitmetrix_api_appointments_inserts.end_date_time,
       #s_fitmetrix_api_appointments_inserts.is_available,
       #s_fitmetrix_api_appointments_inserts.hidden,
       #s_fitmetrix_api_appointments_inserts.is_cancelled,
       #s_fitmetrix_api_appointments_inserts.is_enrolled,
       #s_fitmetrix_api_appointments_inserts.is_wait_list_available,
       #s_fitmetrix_api_appointments_inserts.max_capacity,
       #s_fitmetrix_api_appointments_inserts.total_booked,
       #s_fitmetrix_api_appointments_inserts.total_booked_wait_list,
       #s_fitmetrix_api_appointments_inserts.web_booked,
       #s_fitmetrix_api_appointments_inserts.web_booked_capacity,
       #s_fitmetrix_api_appointments_inserts.message,
       #s_fitmetrix_api_appointments_inserts.date_modified,
       #s_fitmetrix_api_appointments_inserts.date_added,
       #s_fitmetrix_api_appointments_inserts.date_completed,
       #s_fitmetrix_api_appointments_inserts.color_code,
       #s_fitmetrix_api_appointments_inserts.additional_notes,
       #s_fitmetrix_api_appointments_inserts.started,
       #s_fitmetrix_api_appointments_inserts.is_enrollment,
       #s_fitmetrix_api_appointments_inserts.is_substitute,
       #s_fitmetrix_api_appointments_inserts.cancel_offset,
       #s_fitmetrix_api_appointments_inserts.ptp_completed,
       #s_fitmetrix_api_appointments_inserts.is_appointment,
       #s_fitmetrix_api_appointments_inserts.instructor_first_name,
       #s_fitmetrix_api_appointments_inserts.instructor_last_name,
       #s_fitmetrix_api_appointments_inserts.instructor_image,
       #s_fitmetrix_api_appointments_inserts.instructor_gender,
       #s_fitmetrix_api_appointments_inserts.open_spots,
       #s_fitmetrix_api_appointments_inserts.booked_spots,
       #s_fitmetrix_api_appointments_inserts.wait_list_size,
       #s_fitmetrix_api_appointments_inserts.pt_no_test,
       #s_fitmetrix_api_appointments_inserts.checked_in,
       #s_fitmetrix_api_appointments_inserts.app,
       #s_fitmetrix_api_appointments_inserts.app_icon,
       #s_fitmetrix_api_appointments_inserts.manual_capacity,
       #s_fitmetrix_api_appointments_inserts.is_editable,
       #s_fitmetrix_api_appointments_inserts.dummy_modified_date_time,
       case when s_fitmetrix_api_appointments.s_fitmetrix_api_appointments_id is null then isnull(#s_fitmetrix_api_appointments_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #s_fitmetrix_api_appointments_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_fitmetrix_api_appointments_inserts
  left join p_fitmetrix_api_appointments
    on #s_fitmetrix_api_appointments_inserts.bk_hash = p_fitmetrix_api_appointments.bk_hash
   and p_fitmetrix_api_appointments.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_fitmetrix_api_appointments
    on p_fitmetrix_api_appointments.bk_hash = s_fitmetrix_api_appointments.bk_hash
   and p_fitmetrix_api_appointments.s_fitmetrix_api_appointments_id = s_fitmetrix_api_appointments.s_fitmetrix_api_appointments_id
 where s_fitmetrix_api_appointments.s_fitmetrix_api_appointments_id is null
    or (s_fitmetrix_api_appointments.s_fitmetrix_api_appointments_id is not null
        and s_fitmetrix_api_appointments.dv_hash <> #s_fitmetrix_api_appointments_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_fitmetrix_api_appointments @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_fitmetrix_api_appointments @current_dv_batch_id

end
