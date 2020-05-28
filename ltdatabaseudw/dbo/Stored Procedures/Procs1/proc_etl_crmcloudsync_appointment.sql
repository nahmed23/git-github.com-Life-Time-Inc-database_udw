CREATE PROC [dbo].[proc_etl_crmcloudsync_appointment] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_Appointment

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_Appointment (
       bk_hash,
       activityid,
       activitytypecode,
       activitytypecodename,
       actualdurationminutes,
       actualend,
       actualstart,
       category,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       description,
       exchangerate,
       globalobjectid,
       importsequencenumber,
       instancetypecode,
       instancetypecodename,
       isalldayevent,
       isalldayeventname,
       isbilled,
       isbilledname,
       ismapiprivate,
       ismapiprivatename,
       isregularactivity,
       isregularactivityname,
       isworkflowcreated,
       isworkflowcreatedname,
       location,
       ltf_appointmenttype,
       ltf_appointmenttypename,
       ltf_clubappointmentsid,
       ltf_clubappointmentsidname,
       ltf_clubid,
       ltf_clubidname,
       ltf_udwid,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedfieldsmask,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       optionalattendees,
       organizer,
       originalstartdate,
       outlookownerapptid,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       prioritycode,
       prioritycodename,
       processid,
       regardingobjectid,
       regardingobjectidname,
       regardingobjectidyominame,
       regardingobjecttypecode,
       requiredattendees,
       scheduleddurationminutes,
       scheduledend,
       scheduledstart,
       seriesid,
       serviceid,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       subcategory,
       subject,
       timezoneruleversionnumber,
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       ltf_metontour,
       UpdatedDateTime,
       UpdateUser,
       ltf_webbookingsource,
       ltf_webbookingsourcename,
       ltf_checkinflag,
       ltf_checkinflagname,
       ltf_program,
       ltf_programname,
       traversedpath,
       ltf_qrcode,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(activityid,'z#@$k%&P'))),2) bk_hash,
       activityid,
       activitytypecode,
       activitytypecodename,
       actualdurationminutes,
       actualend,
       actualstart,
       category,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       description,
       exchangerate,
       globalobjectid,
       importsequencenumber,
       instancetypecode,
       instancetypecodename,
       isalldayevent,
       isalldayeventname,
       isbilled,
       isbilledname,
       ismapiprivate,
       ismapiprivatename,
       isregularactivity,
       isregularactivityname,
       isworkflowcreated,
       isworkflowcreatedname,
       location,
       ltf_appointmenttype,
       ltf_appointmenttypename,
       ltf_clubappointmentsid,
       ltf_clubappointmentsidname,
       ltf_clubid,
       ltf_clubidname,
       ltf_udwid,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedfieldsmask,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       optionalattendees,
       organizer,
       originalstartdate,
       outlookownerapptid,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       prioritycode,
       prioritycodename,
       processid,
       regardingobjectid,
       regardingobjectidname,
       regardingobjectidyominame,
       regardingobjecttypecode,
       requiredattendees,
       scheduleddurationminutes,
       scheduledend,
       scheduledstart,
       seriesid,
       serviceid,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       subcategory,
       subject,
       timezoneruleversionnumber,
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       ltf_metontour,
       UpdatedDateTime,
       UpdateUser,
       ltf_webbookingsource,
       ltf_webbookingsourcename,
       ltf_checkinflag,
       ltf_checkinflagname,
       ltf_program,
       ltf_programname,
       traversedpath,
       ltf_qrcode,
       isnull(cast(stage_crmcloudsync_Appointment.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_Appointment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_appointment @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_appointment (
       bk_hash,
       activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_Appointment.bk_hash,
       stage_hash_crmcloudsync_Appointment.activityid activity_id,
       isnull(cast(stage_hash_crmcloudsync_Appointment.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_Appointment
  left join h_crmcloudsync_appointment
    on stage_hash_crmcloudsync_Appointment.bk_hash = h_crmcloudsync_appointment.bk_hash
 where h_crmcloudsync_appointment_id is null
   and stage_hash_crmcloudsync_Appointment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_appointment
if object_id('tempdb..#l_crmcloudsync_appointment_inserts') is not null drop table #l_crmcloudsync_appointment_inserts
create table #l_crmcloudsync_appointment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Appointment.bk_hash,
       stage_hash_crmcloudsync_Appointment.activityid activity_id,
       stage_hash_crmcloudsync_Appointment.createdby created_by,
       stage_hash_crmcloudsync_Appointment.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_Appointment.isalldayevent is_all_day_event,
       stage_hash_crmcloudsync_Appointment.isbilled is_billed,
       stage_hash_crmcloudsync_Appointment.ismapiprivate is_mapi_private,
       stage_hash_crmcloudsync_Appointment.isregularactivity is_regular_activity,
       stage_hash_crmcloudsync_Appointment.isworkflowcreated is_workflow_created,
       stage_hash_crmcloudsync_Appointment.ltf_clubappointmentsid ltf_club_appointment_sid,
       stage_hash_crmcloudsync_Appointment.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_Appointment.modifiedby modified_by,
       stage_hash_crmcloudsync_Appointment.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_Appointment.outlookownerapptid outlook_owner_appt_id,
       stage_hash_crmcloudsync_Appointment.ownerid owner_id,
       stage_hash_crmcloudsync_Appointment.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_Appointment.owningteam owning_team,
       stage_hash_crmcloudsync_Appointment.owninguser owning_user,
       stage_hash_crmcloudsync_Appointment.processid process_id,
       stage_hash_crmcloudsync_Appointment.regardingobjectid regarding_object_id,
       stage_hash_crmcloudsync_Appointment.seriesid series_id,
       stage_hash_crmcloudsync_Appointment.serviceid service_id,
       stage_hash_crmcloudsync_Appointment.stageid stage_id,
       stage_hash_crmcloudsync_Appointment.transactioncurrencyid transaction_currency_id,
       isnull(cast(stage_hash_crmcloudsync_Appointment.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.isalldayevent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.isbilled,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ismapiprivate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.isregularactivity,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.isworkflowcreated,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_clubappointmentsid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.outlookownerapptid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.regardingobjectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.seriesid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.serviceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.transactioncurrencyid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Appointment
 where stage_hash_crmcloudsync_Appointment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_appointment records
set @insert_date_time = getdate()
insert into l_crmcloudsync_appointment (
       bk_hash,
       activity_id,
       created_by,
       created_on_behalf_by,
       is_all_day_event,
       is_billed,
       is_mapi_private,
       is_regular_activity,
       is_workflow_created,
       ltf_club_appointment_sid,
       ltf_club_id,
       modified_by,
       modified_on_behalf_by,
       outlook_owner_appt_id,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       process_id,
       regarding_object_id,
       series_id,
       service_id,
       stage_id,
       transaction_currency_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_appointment_inserts.bk_hash,
       #l_crmcloudsync_appointment_inserts.activity_id,
       #l_crmcloudsync_appointment_inserts.created_by,
       #l_crmcloudsync_appointment_inserts.created_on_behalf_by,
       #l_crmcloudsync_appointment_inserts.is_all_day_event,
       #l_crmcloudsync_appointment_inserts.is_billed,
       #l_crmcloudsync_appointment_inserts.is_mapi_private,
       #l_crmcloudsync_appointment_inserts.is_regular_activity,
       #l_crmcloudsync_appointment_inserts.is_workflow_created,
       #l_crmcloudsync_appointment_inserts.ltf_club_appointment_sid,
       #l_crmcloudsync_appointment_inserts.ltf_club_id,
       #l_crmcloudsync_appointment_inserts.modified_by,
       #l_crmcloudsync_appointment_inserts.modified_on_behalf_by,
       #l_crmcloudsync_appointment_inserts.outlook_owner_appt_id,
       #l_crmcloudsync_appointment_inserts.owner_id,
       #l_crmcloudsync_appointment_inserts.owning_business_unit,
       #l_crmcloudsync_appointment_inserts.owning_team,
       #l_crmcloudsync_appointment_inserts.owning_user,
       #l_crmcloudsync_appointment_inserts.process_id,
       #l_crmcloudsync_appointment_inserts.regarding_object_id,
       #l_crmcloudsync_appointment_inserts.series_id,
       #l_crmcloudsync_appointment_inserts.service_id,
       #l_crmcloudsync_appointment_inserts.stage_id,
       #l_crmcloudsync_appointment_inserts.transaction_currency_id,
       case when l_crmcloudsync_appointment.l_crmcloudsync_appointment_id is null then isnull(#l_crmcloudsync_appointment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_appointment_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_appointment_inserts
  left join p_crmcloudsync_appointment
    on #l_crmcloudsync_appointment_inserts.bk_hash = p_crmcloudsync_appointment.bk_hash
   and p_crmcloudsync_appointment.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_appointment
    on p_crmcloudsync_appointment.bk_hash = l_crmcloudsync_appointment.bk_hash
   and p_crmcloudsync_appointment.l_crmcloudsync_appointment_id = l_crmcloudsync_appointment.l_crmcloudsync_appointment_id
 where l_crmcloudsync_appointment.l_crmcloudsync_appointment_id is null
    or (l_crmcloudsync_appointment.l_crmcloudsync_appointment_id is not null
        and l_crmcloudsync_appointment.dv_hash <> #l_crmcloudsync_appointment_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_appointment
if object_id('tempdb..#s_crmcloudsync_appointment_inserts') is not null drop table #s_crmcloudsync_appointment_inserts
create table #s_crmcloudsync_appointment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Appointment.bk_hash,
       stage_hash_crmcloudsync_Appointment.activityid activity_id,
       stage_hash_crmcloudsync_Appointment.activitytypecode activity_type_code,
       stage_hash_crmcloudsync_Appointment.activitytypecodename activity_type_code_name,
       stage_hash_crmcloudsync_Appointment.actualdurationminutes actual_duration_minutes,
       stage_hash_crmcloudsync_Appointment.actualend actual_end,
       stage_hash_crmcloudsync_Appointment.actualstart actual_start,
       stage_hash_crmcloudsync_Appointment.category category,
       stage_hash_crmcloudsync_Appointment.createdbyname created_by_name,
       stage_hash_crmcloudsync_Appointment.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_Appointment.createdon created_on,
       stage_hash_crmcloudsync_Appointment.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_Appointment.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Appointment.description description,
       stage_hash_crmcloudsync_Appointment.exchangerate exchange_rate,
       stage_hash_crmcloudsync_Appointment.globalobjectid global_object_id,
       stage_hash_crmcloudsync_Appointment.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_Appointment.instancetypecode instance_type_code,
       stage_hash_crmcloudsync_Appointment.instancetypecodename instance_type_code_name,
       stage_hash_crmcloudsync_Appointment.isalldayeventname is_all_day_event_name,
       stage_hash_crmcloudsync_Appointment.isbilledname is_billed_name,
       stage_hash_crmcloudsync_Appointment.ismapiprivatename is_mapi_private_name,
       stage_hash_crmcloudsync_Appointment.isregularactivityname is_regular_activity_name,
       stage_hash_crmcloudsync_Appointment.isworkflowcreatedname is_workflow_created_name,
       stage_hash_crmcloudsync_Appointment.location location,
       stage_hash_crmcloudsync_Appointment.ltf_appointmenttype ltf_appointment_type,
       stage_hash_crmcloudsync_Appointment.ltf_appointmenttypename ltf_appointment_type_name,
       stage_hash_crmcloudsync_Appointment.ltf_clubappointmentsidname ltf_club_appointment_sid_name,
       stage_hash_crmcloudsync_Appointment.ltf_udwid ltf_udw_id,
       stage_hash_crmcloudsync_Appointment.ltf_clubidname ltf_club_id_name,
       stage_hash_crmcloudsync_Appointment.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_Appointment.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_Appointment.modifiedfieldsmask modified_fields_mask,
       stage_hash_crmcloudsync_Appointment.modifiedon modified_on,
       stage_hash_crmcloudsync_Appointment.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_Appointment.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Appointment.optionalattendees optional_attendees,
       stage_hash_crmcloudsync_Appointment.organizer organizer,
       stage_hash_crmcloudsync_Appointment.originalstartdate original_start_date,
       stage_hash_crmcloudsync_Appointment.overriddencreatedon over_ridden_created_on,
       stage_hash_crmcloudsync_Appointment.owneridname owner_id_name,
       stage_hash_crmcloudsync_Appointment.owneridtype owner_id_type,
       stage_hash_crmcloudsync_Appointment.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_Appointment.prioritycode priority_code,
       stage_hash_crmcloudsync_Appointment.prioritycodename priority_code_name,
       stage_hash_crmcloudsync_Appointment.regardingobjectidname regarding_object_id_name,
       stage_hash_crmcloudsync_Appointment.regardingobjectidyominame regarding_object_id_yomi_name,
       stage_hash_crmcloudsync_Appointment.regardingobjecttypecode regarding_object_type_code,
       stage_hash_crmcloudsync_Appointment.requiredattendees required_attendees,
       stage_hash_crmcloudsync_Appointment.scheduleddurationminutes scheduled_duration_minutes,
       stage_hash_crmcloudsync_Appointment.scheduledend scheduled_end,
       stage_hash_crmcloudsync_Appointment.scheduledstart scheduled_start,
       stage_hash_crmcloudsync_Appointment.statecode state_code,
       stage_hash_crmcloudsync_Appointment.statecodename state_code_name,
       stage_hash_crmcloudsync_Appointment.statuscode status_code,
       stage_hash_crmcloudsync_Appointment.statuscodename status_code_name,
       stage_hash_crmcloudsync_Appointment.subcategory sub_category,
       stage_hash_crmcloudsync_Appointment.subject subject,
       stage_hash_crmcloudsync_Appointment.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_Appointment.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_Appointment.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_Appointment.versionnumber version_number,
       stage_hash_crmcloudsync_Appointment.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_Appointment.InsertUser insert_user,
       stage_hash_crmcloudsync_Appointment.ltf_metontour ltf_met_on_tour,
       stage_hash_crmcloudsync_Appointment.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_Appointment.UpdateUser update_user,
       stage_hash_crmcloudsync_Appointment.ltf_webbookingsource ltf_web_booking_source,
       stage_hash_crmcloudsync_Appointment.ltf_webbookingsourcename ltf_web_booking_source_name,
       stage_hash_crmcloudsync_Appointment.ltf_checkinflag ltf_check_in_flag,
       stage_hash_crmcloudsync_Appointment.ltf_checkinflagname ltf_check_in_flag_name,
       stage_hash_crmcloudsync_Appointment.ltf_program ltf_program,
       stage_hash_crmcloudsync_Appointment.ltf_programname ltf_program_name,
       stage_hash_crmcloudsync_Appointment.traversedpath traversed_path,
       stage_hash_crmcloudsync_Appointment.ltf_qrcode ltf_qr_code,
       isnull(cast(stage_hash_crmcloudsync_Appointment.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.activitytypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.activitytypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.actualdurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.actualend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.actualstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.category,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.globalobjectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.instancetypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.instancetypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.isalldayeventname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.isbilledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ismapiprivatename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.isregularactivityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.isworkflowcreatedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.location,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.ltf_appointmenttype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_appointmenttypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_clubappointmentsidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_udwid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_clubidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.modifiedfieldsmask,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.optionalattendees,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.organizer,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.originalstartdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.prioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.prioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.regardingobjectidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.regardingobjectidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.regardingobjecttypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.requiredattendees,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.scheduleddurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.scheduledend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.scheduledstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.subcategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_metontour,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Appointment.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.ltf_webbookingsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_webbookingsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.ltf_checkinflag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_checkinflagname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Appointment.ltf_program as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_programname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.traversedpath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Appointment.ltf_qrcode,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Appointment
 where stage_hash_crmcloudsync_Appointment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_appointment records
set @insert_date_time = getdate()
insert into s_crmcloudsync_appointment (
       bk_hash,
       activity_id,
       activity_type_code,
       activity_type_code_name,
       actual_duration_minutes,
       actual_end,
       actual_start,
       category,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       description,
       exchange_rate,
       global_object_id,
       import_sequence_number,
       instance_type_code,
       instance_type_code_name,
       is_all_day_event_name,
       is_billed_name,
       is_mapi_private_name,
       is_regular_activity_name,
       is_workflow_created_name,
       location,
       ltf_appointment_type,
       ltf_appointment_type_name,
       ltf_club_appointment_sid_name,
       ltf_udw_id,
       ltf_club_id_name,
       modified_by_name,
       modified_by_yomi_name,
       modified_fields_mask,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       optional_attendees,
       organizer,
       original_start_date,
       over_ridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       priority_code,
       priority_code_name,
       regarding_object_id_name,
       regarding_object_id_yomi_name,
       regarding_object_type_code,
       required_attendees,
       scheduled_duration_minutes,
       scheduled_end,
       scheduled_start,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       sub_category,
       subject,
       time_zone_rule_version_number,
       transaction_currency_id_name,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       ltf_met_on_tour,
       updated_date_time,
       update_user,
       ltf_web_booking_source,
       ltf_web_booking_source_name,
       ltf_check_in_flag,
       ltf_check_in_flag_name,
       ltf_program,
       ltf_program_name,
       traversed_path,
       ltf_qr_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_appointment_inserts.bk_hash,
       #s_crmcloudsync_appointment_inserts.activity_id,
       #s_crmcloudsync_appointment_inserts.activity_type_code,
       #s_crmcloudsync_appointment_inserts.activity_type_code_name,
       #s_crmcloudsync_appointment_inserts.actual_duration_minutes,
       #s_crmcloudsync_appointment_inserts.actual_end,
       #s_crmcloudsync_appointment_inserts.actual_start,
       #s_crmcloudsync_appointment_inserts.category,
       #s_crmcloudsync_appointment_inserts.created_by_name,
       #s_crmcloudsync_appointment_inserts.created_by_yomi_name,
       #s_crmcloudsync_appointment_inserts.created_on,
       #s_crmcloudsync_appointment_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_appointment_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_appointment_inserts.description,
       #s_crmcloudsync_appointment_inserts.exchange_rate,
       #s_crmcloudsync_appointment_inserts.global_object_id,
       #s_crmcloudsync_appointment_inserts.import_sequence_number,
       #s_crmcloudsync_appointment_inserts.instance_type_code,
       #s_crmcloudsync_appointment_inserts.instance_type_code_name,
       #s_crmcloudsync_appointment_inserts.is_all_day_event_name,
       #s_crmcloudsync_appointment_inserts.is_billed_name,
       #s_crmcloudsync_appointment_inserts.is_mapi_private_name,
       #s_crmcloudsync_appointment_inserts.is_regular_activity_name,
       #s_crmcloudsync_appointment_inserts.is_workflow_created_name,
       #s_crmcloudsync_appointment_inserts.location,
       #s_crmcloudsync_appointment_inserts.ltf_appointment_type,
       #s_crmcloudsync_appointment_inserts.ltf_appointment_type_name,
       #s_crmcloudsync_appointment_inserts.ltf_club_appointment_sid_name,
       #s_crmcloudsync_appointment_inserts.ltf_udw_id,
       #s_crmcloudsync_appointment_inserts.ltf_club_id_name,
       #s_crmcloudsync_appointment_inserts.modified_by_name,
       #s_crmcloudsync_appointment_inserts.modified_by_yomi_name,
       #s_crmcloudsync_appointment_inserts.modified_fields_mask,
       #s_crmcloudsync_appointment_inserts.modified_on,
       #s_crmcloudsync_appointment_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_appointment_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_appointment_inserts.optional_attendees,
       #s_crmcloudsync_appointment_inserts.organizer,
       #s_crmcloudsync_appointment_inserts.original_start_date,
       #s_crmcloudsync_appointment_inserts.over_ridden_created_on,
       #s_crmcloudsync_appointment_inserts.owner_id_name,
       #s_crmcloudsync_appointment_inserts.owner_id_type,
       #s_crmcloudsync_appointment_inserts.owner_id_yomi_name,
       #s_crmcloudsync_appointment_inserts.priority_code,
       #s_crmcloudsync_appointment_inserts.priority_code_name,
       #s_crmcloudsync_appointment_inserts.regarding_object_id_name,
       #s_crmcloudsync_appointment_inserts.regarding_object_id_yomi_name,
       #s_crmcloudsync_appointment_inserts.regarding_object_type_code,
       #s_crmcloudsync_appointment_inserts.required_attendees,
       #s_crmcloudsync_appointment_inserts.scheduled_duration_minutes,
       #s_crmcloudsync_appointment_inserts.scheduled_end,
       #s_crmcloudsync_appointment_inserts.scheduled_start,
       #s_crmcloudsync_appointment_inserts.state_code,
       #s_crmcloudsync_appointment_inserts.state_code_name,
       #s_crmcloudsync_appointment_inserts.status_code,
       #s_crmcloudsync_appointment_inserts.status_code_name,
       #s_crmcloudsync_appointment_inserts.sub_category,
       #s_crmcloudsync_appointment_inserts.subject,
       #s_crmcloudsync_appointment_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_appointment_inserts.transaction_currency_id_name,
       #s_crmcloudsync_appointment_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_appointment_inserts.version_number,
       #s_crmcloudsync_appointment_inserts.inserted_date_time,
       #s_crmcloudsync_appointment_inserts.insert_user,
       #s_crmcloudsync_appointment_inserts.ltf_met_on_tour,
       #s_crmcloudsync_appointment_inserts.updated_date_time,
       #s_crmcloudsync_appointment_inserts.update_user,
       #s_crmcloudsync_appointment_inserts.ltf_web_booking_source,
       #s_crmcloudsync_appointment_inserts.ltf_web_booking_source_name,
       #s_crmcloudsync_appointment_inserts.ltf_check_in_flag,
       #s_crmcloudsync_appointment_inserts.ltf_check_in_flag_name,
       #s_crmcloudsync_appointment_inserts.ltf_program,
       #s_crmcloudsync_appointment_inserts.ltf_program_name,
       #s_crmcloudsync_appointment_inserts.traversed_path,
       #s_crmcloudsync_appointment_inserts.ltf_qr_code,
       case when s_crmcloudsync_appointment.s_crmcloudsync_appointment_id is null then isnull(#s_crmcloudsync_appointment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_appointment_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_appointment_inserts
  left join p_crmcloudsync_appointment
    on #s_crmcloudsync_appointment_inserts.bk_hash = p_crmcloudsync_appointment.bk_hash
   and p_crmcloudsync_appointment.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_appointment
    on p_crmcloudsync_appointment.bk_hash = s_crmcloudsync_appointment.bk_hash
   and p_crmcloudsync_appointment.s_crmcloudsync_appointment_id = s_crmcloudsync_appointment.s_crmcloudsync_appointment_id
 where s_crmcloudsync_appointment.s_crmcloudsync_appointment_id is null
    or (s_crmcloudsync_appointment.s_crmcloudsync_appointment_id is not null
        and s_crmcloudsync_appointment.dv_hash <> #s_crmcloudsync_appointment_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_appointment @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_appointment @current_dv_batch_id

end
