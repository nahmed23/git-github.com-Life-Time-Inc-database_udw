CREATE PROC [dbo].[proc_etl_crmcloudsync_phone_call] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_PhoneCall

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_PhoneCall (
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
       directioncode,
       directioncodename,
       exchangerate,
       [from],
       importsequencenumber,
       isbilled,
       isbilledname,
       isregularactivity,
       isregularactivityname,
       isworkflowcreated,
       isworkflowcreatedname,
       leftvoicemail,
       leftvoicemailname,
       ltf_wrapupcode,
       ltf_wrapupcodename,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       new_callid,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       phonenumber,
       prioritycode,
       prioritycodename,
       processid,
       regardingobjectid,
       regardingobjectidname,
       regardingobjectidyominame,
       regardingobjecttypecode,
       scheduleddurationminutes,
       scheduledend,
       scheduledstart,
       serviceid,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       subcategory,
       subject,
       timezoneruleversionnumber,
       [to],
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_program,
       ltf_programname,
       ltf_callername,
       ltf_callsubtype,
       ltf_callsubtypename,
       ltf_calltype,
       ltf_calltypename,
       ltf_club,
       ltf_clubid,
       ltf_clubidname,
       ltf_clubname,
       activityadditionalparams,
       traversedpath,
       ltf_mostrecentcasl,
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
       directioncode,
       directioncodename,
       exchangerate,
       [from],
       importsequencenumber,
       isbilled,
       isbilledname,
       isregularactivity,
       isregularactivityname,
       isworkflowcreated,
       isworkflowcreatedname,
       leftvoicemail,
       leftvoicemailname,
       ltf_wrapupcode,
       ltf_wrapupcodename,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       new_callid,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       phonenumber,
       prioritycode,
       prioritycodename,
       processid,
       regardingobjectid,
       regardingobjectidname,
       regardingobjectidyominame,
       regardingobjecttypecode,
       scheduleddurationminutes,
       scheduledend,
       scheduledstart,
       serviceid,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       subcategory,
       subject,
       timezoneruleversionnumber,
       [to],
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_program,
       ltf_programname,
       ltf_callername,
       ltf_callsubtype,
       ltf_callsubtypename,
       ltf_calltype,
       ltf_calltypename,
       ltf_club,
       ltf_clubid,
       ltf_clubidname,
       ltf_clubname,
       activityadditionalparams,
       traversedpath,
       ltf_mostrecentcasl,
       isnull(cast(stage_crmcloudsync_PhoneCall.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_PhoneCall
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_phone_call @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_phone_call (
       bk_hash,
       activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_crmcloudsync_PhoneCall.bk_hash,
       stage_hash_crmcloudsync_PhoneCall.activityid activity_id,
       isnull(cast(stage_hash_crmcloudsync_PhoneCall.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_PhoneCall
  left join h_crmcloudsync_phone_call
    on stage_hash_crmcloudsync_PhoneCall.bk_hash = h_crmcloudsync_phone_call.bk_hash
 where h_crmcloudsync_phone_call_id is null
   and stage_hash_crmcloudsync_PhoneCall.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_phone_call
if object_id('tempdb..#l_crmcloudsync_phone_call_inserts') is not null drop table #l_crmcloudsync_phone_call_inserts
create table #l_crmcloudsync_phone_call_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_PhoneCall.bk_hash,
       stage_hash_crmcloudsync_PhoneCall.activityid activity_id,
       stage_hash_crmcloudsync_PhoneCall.prioritycode priority_code,
       stage_hash_crmcloudsync_PhoneCall.statecode state_code,
       stage_hash_crmcloudsync_PhoneCall.versionnumber version_number,
       isnull(cast(stage_hash_crmcloudsync_PhoneCall.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.prioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.versionnumber as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_PhoneCall
 where stage_hash_crmcloudsync_PhoneCall.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_phone_call records
set @insert_date_time = getdate()
insert into l_crmcloudsync_phone_call (
       bk_hash,
       activity_id,
       priority_code,
       state_code,
       version_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_phone_call_inserts.bk_hash,
       #l_crmcloudsync_phone_call_inserts.activity_id,
       #l_crmcloudsync_phone_call_inserts.priority_code,
       #l_crmcloudsync_phone_call_inserts.state_code,
       #l_crmcloudsync_phone_call_inserts.version_number,
       case when l_crmcloudsync_phone_call.l_crmcloudsync_phone_call_id is null then isnull(#l_crmcloudsync_phone_call_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_phone_call_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_phone_call_inserts
  left join p_crmcloudsync_phone_call
    on #l_crmcloudsync_phone_call_inserts.bk_hash = p_crmcloudsync_phone_call.bk_hash
   and p_crmcloudsync_phone_call.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_phone_call
    on p_crmcloudsync_phone_call.bk_hash = l_crmcloudsync_phone_call.bk_hash
   and p_crmcloudsync_phone_call.l_crmcloudsync_phone_call_id = l_crmcloudsync_phone_call.l_crmcloudsync_phone_call_id
 where l_crmcloudsync_phone_call.l_crmcloudsync_phone_call_id is null
    or (l_crmcloudsync_phone_call.l_crmcloudsync_phone_call_id is not null
        and l_crmcloudsync_phone_call.dv_hash <> #l_crmcloudsync_phone_call_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_phone_call
if object_id('tempdb..#s_crmcloudsync_phone_call_inserts') is not null drop table #s_crmcloudsync_phone_call_inserts
create table #s_crmcloudsync_phone_call_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_PhoneCall.bk_hash,
       stage_hash_crmcloudsync_PhoneCall.activityid activity_id,
       stage_hash_crmcloudsync_PhoneCall.activitytypecode activity_type_code,
       stage_hash_crmcloudsync_PhoneCall.activitytypecodename activity_type_code_name,
       stage_hash_crmcloudsync_PhoneCall.actualdurationminutes actual_duration_minutes,
       stage_hash_crmcloudsync_PhoneCall.actualend actual_end,
       stage_hash_crmcloudsync_PhoneCall.actualstart actual_start,
       stage_hash_crmcloudsync_PhoneCall.category category,
       stage_hash_crmcloudsync_PhoneCall.createdby created_by,
       stage_hash_crmcloudsync_PhoneCall.createdbyname created_by_name,
       stage_hash_crmcloudsync_PhoneCall.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_PhoneCall.createdon created_on,
       stage_hash_crmcloudsync_PhoneCall.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_PhoneCall.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_PhoneCall.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_PhoneCall.description description,
       stage_hash_crmcloudsync_PhoneCall.directioncode direction_code,
       stage_hash_crmcloudsync_PhoneCall.directioncodename direction_code_name,
       stage_hash_crmcloudsync_PhoneCall.exchangerate exchange_rate,
       stage_hash_crmcloudsync_PhoneCall.[from] [from],
       stage_hash_crmcloudsync_PhoneCall.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_PhoneCall.isbilled is_billed,
       stage_hash_crmcloudsync_PhoneCall.isbilledname is_billed_name,
       stage_hash_crmcloudsync_PhoneCall.isregularactivity is_regular_activity,
       stage_hash_crmcloudsync_PhoneCall.isregularactivityname is_regular_activity_name,
       stage_hash_crmcloudsync_PhoneCall.isworkflowcreated is_workflow_created,
       stage_hash_crmcloudsync_PhoneCall.isworkflowcreatedname is_workflow_created_name,
       stage_hash_crmcloudsync_PhoneCall.leftvoicemail left_voice_mail,
       stage_hash_crmcloudsync_PhoneCall.leftvoicemailname left_voice_mail_name,
       stage_hash_crmcloudsync_PhoneCall.ltf_wrapupcode ltf_wrap_up_code,
       stage_hash_crmcloudsync_PhoneCall.ltf_wrapupcodename ltf_wrap_up_code_name,
       stage_hash_crmcloudsync_PhoneCall.modifiedby modified_by,
       stage_hash_crmcloudsync_PhoneCall.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_PhoneCall.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_PhoneCall.modifiedon modified_on,
       stage_hash_crmcloudsync_PhoneCall.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_PhoneCall.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_PhoneCall.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_PhoneCall.new_callid new_callid,
       stage_hash_crmcloudsync_PhoneCall.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_PhoneCall.ownerid owner_id,
       stage_hash_crmcloudsync_PhoneCall.owneridname owner_id_name,
       stage_hash_crmcloudsync_PhoneCall.owneridtype owner_id_type,
       stage_hash_crmcloudsync_PhoneCall.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_PhoneCall.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_PhoneCall.owningteam owning_team,
       stage_hash_crmcloudsync_PhoneCall.owninguser owning_user,
       stage_hash_crmcloudsync_PhoneCall.phonenumber phone_number,
       stage_hash_crmcloudsync_PhoneCall.prioritycodename priority_code_name,
       stage_hash_crmcloudsync_PhoneCall.processid process_id,
       stage_hash_crmcloudsync_PhoneCall.regardingobjectid regarding_object_id,
       stage_hash_crmcloudsync_PhoneCall.regardingobjectidname regarding_object_id_name,
       stage_hash_crmcloudsync_PhoneCall.regardingobjectidyominame regarding_object_id_yomi_name,
       stage_hash_crmcloudsync_PhoneCall.regardingobjecttypecode regarding_object_type_code,
       stage_hash_crmcloudsync_PhoneCall.scheduleddurationminutes scheduled_duration_minutes,
       stage_hash_crmcloudsync_PhoneCall.scheduledend scheduled_end,
       stage_hash_crmcloudsync_PhoneCall.scheduledstart scheduled_start,
       stage_hash_crmcloudsync_PhoneCall.serviceid service_id,
       stage_hash_crmcloudsync_PhoneCall.stageid stage_id,
       stage_hash_crmcloudsync_PhoneCall.statecodename state_code_name,
       stage_hash_crmcloudsync_PhoneCall.statuscode status_code,
       stage_hash_crmcloudsync_PhoneCall.statuscodename status_code_name,
       stage_hash_crmcloudsync_PhoneCall.subcategory sub_category,
       stage_hash_crmcloudsync_PhoneCall.subject subject,
       stage_hash_crmcloudsync_PhoneCall.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_PhoneCall.[to] [to],
       stage_hash_crmcloudsync_PhoneCall.transactioncurrencyid transaction_currency_id,
       stage_hash_crmcloudsync_PhoneCall.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_PhoneCall.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_PhoneCall.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_PhoneCall.InsertUser insert_user,
       stage_hash_crmcloudsync_PhoneCall.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_PhoneCall.UpdateUser update_user,
       stage_hash_crmcloudsync_PhoneCall.ltf_program ltf_program,
       stage_hash_crmcloudsync_PhoneCall.ltf_programname ltf_program_name,
       stage_hash_crmcloudsync_PhoneCall.ltf_callername ltf_caller_name,
       stage_hash_crmcloudsync_PhoneCall.ltf_callsubtype ltf_call_sub_type,
       stage_hash_crmcloudsync_PhoneCall.ltf_callsubtypename ltf_call_sub_type_name,
       stage_hash_crmcloudsync_PhoneCall.ltf_calltype ltf_call_type,
       stage_hash_crmcloudsync_PhoneCall.ltf_calltypename ltf_call_type_name,
       stage_hash_crmcloudsync_PhoneCall.ltf_club ltf_club,
       stage_hash_crmcloudsync_PhoneCall.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_PhoneCall.ltf_clubidname ltf_club_id_name,
       stage_hash_crmcloudsync_PhoneCall.ltf_clubname ltf_club_name,
       stage_hash_crmcloudsync_PhoneCall.activityadditionalparams activity_additional_params,
       stage_hash_crmcloudsync_PhoneCall.traversedpath traversed_path,
       stage_hash_crmcloudsync_PhoneCall.ltf_mostrecentcasl ltf_most_recent_casl,
       isnull(cast(stage_hash_crmcloudsync_PhoneCall.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.activitytypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.activitytypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.actualdurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.actualend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.actualstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.category,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.directioncode as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.directioncodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.[from],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.isbilled as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.isbilledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.isregularactivity as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.isregularactivityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.isworkflowcreated as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.isworkflowcreatedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.leftvoicemail as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.leftvoicemailname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.ltf_wrapupcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_wrapupcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.new_callid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.phonenumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.prioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.regardingobjectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.regardingobjectidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.regardingobjectidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.regardingobjecttypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.scheduleddurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.scheduledend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.scheduledstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.serviceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.subcategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.[to],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.transactioncurrencyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.ltf_program as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_programname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_callername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.ltf_callsubtype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_callsubtypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_PhoneCall.ltf_calltype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_calltypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_club,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_clubidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.ltf_clubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.activityadditionalparams,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_PhoneCall.traversedpath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_PhoneCall.ltf_mostrecentcasl,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_PhoneCall
 where stage_hash_crmcloudsync_PhoneCall.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_phone_call records
set @insert_date_time = getdate()
insert into s_crmcloudsync_phone_call (
       bk_hash,
       activity_id,
       activity_type_code,
       activity_type_code_name,
       actual_duration_minutes,
       actual_end,
       actual_start,
       category,
       created_by,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       description,
       direction_code,
       direction_code_name,
       exchange_rate,
       [from],
       import_sequence_number,
       is_billed,
       is_billed_name,
       is_regular_activity,
       is_regular_activity_name,
       is_workflow_created,
       is_workflow_created_name,
       left_voice_mail,
       left_voice_mail_name,
       ltf_wrap_up_code,
       ltf_wrap_up_code_name,
       modified_by,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       new_callid,
       overridden_created_on,
       owner_id,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       owning_business_unit,
       owning_team,
       owning_user,
       phone_number,
       priority_code_name,
       process_id,
       regarding_object_id,
       regarding_object_id_name,
       regarding_object_id_yomi_name,
       regarding_object_type_code,
       scheduled_duration_minutes,
       scheduled_end,
       scheduled_start,
       service_id,
       stage_id,
       state_code_name,
       status_code,
       status_code_name,
       sub_category,
       subject,
       time_zone_rule_version_number,
       [to],
       transaction_currency_id,
       transaction_currency_id_name,
       utc_conversion_time_zone_code,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       ltf_program,
       ltf_program_name,
       ltf_caller_name,
       ltf_call_sub_type,
       ltf_call_sub_type_name,
       ltf_call_type,
       ltf_call_type_name,
       ltf_club,
       ltf_club_id,
       ltf_club_id_name,
       ltf_club_name,
       activity_additional_params,
       traversed_path,
       ltf_most_recent_casl,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_phone_call_inserts.bk_hash,
       #s_crmcloudsync_phone_call_inserts.activity_id,
       #s_crmcloudsync_phone_call_inserts.activity_type_code,
       #s_crmcloudsync_phone_call_inserts.activity_type_code_name,
       #s_crmcloudsync_phone_call_inserts.actual_duration_minutes,
       #s_crmcloudsync_phone_call_inserts.actual_end,
       #s_crmcloudsync_phone_call_inserts.actual_start,
       #s_crmcloudsync_phone_call_inserts.category,
       #s_crmcloudsync_phone_call_inserts.created_by,
       #s_crmcloudsync_phone_call_inserts.created_by_name,
       #s_crmcloudsync_phone_call_inserts.created_by_yomi_name,
       #s_crmcloudsync_phone_call_inserts.created_on,
       #s_crmcloudsync_phone_call_inserts.created_on_behalf_by,
       #s_crmcloudsync_phone_call_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_phone_call_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_phone_call_inserts.description,
       #s_crmcloudsync_phone_call_inserts.direction_code,
       #s_crmcloudsync_phone_call_inserts.direction_code_name,
       #s_crmcloudsync_phone_call_inserts.exchange_rate,
       #s_crmcloudsync_phone_call_inserts.[from],
       #s_crmcloudsync_phone_call_inserts.import_sequence_number,
       #s_crmcloudsync_phone_call_inserts.is_billed,
       #s_crmcloudsync_phone_call_inserts.is_billed_name,
       #s_crmcloudsync_phone_call_inserts.is_regular_activity,
       #s_crmcloudsync_phone_call_inserts.is_regular_activity_name,
       #s_crmcloudsync_phone_call_inserts.is_workflow_created,
       #s_crmcloudsync_phone_call_inserts.is_workflow_created_name,
       #s_crmcloudsync_phone_call_inserts.left_voice_mail,
       #s_crmcloudsync_phone_call_inserts.left_voice_mail_name,
       #s_crmcloudsync_phone_call_inserts.ltf_wrap_up_code,
       #s_crmcloudsync_phone_call_inserts.ltf_wrap_up_code_name,
       #s_crmcloudsync_phone_call_inserts.modified_by,
       #s_crmcloudsync_phone_call_inserts.modified_by_name,
       #s_crmcloudsync_phone_call_inserts.modified_by_yomi_name,
       #s_crmcloudsync_phone_call_inserts.modified_on,
       #s_crmcloudsync_phone_call_inserts.modified_on_behalf_by,
       #s_crmcloudsync_phone_call_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_phone_call_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_phone_call_inserts.new_callid,
       #s_crmcloudsync_phone_call_inserts.overridden_created_on,
       #s_crmcloudsync_phone_call_inserts.owner_id,
       #s_crmcloudsync_phone_call_inserts.owner_id_name,
       #s_crmcloudsync_phone_call_inserts.owner_id_type,
       #s_crmcloudsync_phone_call_inserts.owner_id_yomi_name,
       #s_crmcloudsync_phone_call_inserts.owning_business_unit,
       #s_crmcloudsync_phone_call_inserts.owning_team,
       #s_crmcloudsync_phone_call_inserts.owning_user,
       #s_crmcloudsync_phone_call_inserts.phone_number,
       #s_crmcloudsync_phone_call_inserts.priority_code_name,
       #s_crmcloudsync_phone_call_inserts.process_id,
       #s_crmcloudsync_phone_call_inserts.regarding_object_id,
       #s_crmcloudsync_phone_call_inserts.regarding_object_id_name,
       #s_crmcloudsync_phone_call_inserts.regarding_object_id_yomi_name,
       #s_crmcloudsync_phone_call_inserts.regarding_object_type_code,
       #s_crmcloudsync_phone_call_inserts.scheduled_duration_minutes,
       #s_crmcloudsync_phone_call_inserts.scheduled_end,
       #s_crmcloudsync_phone_call_inserts.scheduled_start,
       #s_crmcloudsync_phone_call_inserts.service_id,
       #s_crmcloudsync_phone_call_inserts.stage_id,
       #s_crmcloudsync_phone_call_inserts.state_code_name,
       #s_crmcloudsync_phone_call_inserts.status_code,
       #s_crmcloudsync_phone_call_inserts.status_code_name,
       #s_crmcloudsync_phone_call_inserts.sub_category,
       #s_crmcloudsync_phone_call_inserts.subject,
       #s_crmcloudsync_phone_call_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_phone_call_inserts.[to],
       #s_crmcloudsync_phone_call_inserts.transaction_currency_id,
       #s_crmcloudsync_phone_call_inserts.transaction_currency_id_name,
       #s_crmcloudsync_phone_call_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_phone_call_inserts.inserted_date_time,
       #s_crmcloudsync_phone_call_inserts.insert_user,
       #s_crmcloudsync_phone_call_inserts.updated_date_time,
       #s_crmcloudsync_phone_call_inserts.update_user,
       #s_crmcloudsync_phone_call_inserts.ltf_program,
       #s_crmcloudsync_phone_call_inserts.ltf_program_name,
       #s_crmcloudsync_phone_call_inserts.ltf_caller_name,
       #s_crmcloudsync_phone_call_inserts.ltf_call_sub_type,
       #s_crmcloudsync_phone_call_inserts.ltf_call_sub_type_name,
       #s_crmcloudsync_phone_call_inserts.ltf_call_type,
       #s_crmcloudsync_phone_call_inserts.ltf_call_type_name,
       #s_crmcloudsync_phone_call_inserts.ltf_club,
       #s_crmcloudsync_phone_call_inserts.ltf_club_id,
       #s_crmcloudsync_phone_call_inserts.ltf_club_id_name,
       #s_crmcloudsync_phone_call_inserts.ltf_club_name,
       #s_crmcloudsync_phone_call_inserts.activity_additional_params,
       #s_crmcloudsync_phone_call_inserts.traversed_path,
       #s_crmcloudsync_phone_call_inserts.ltf_most_recent_casl,
       case when s_crmcloudsync_phone_call.s_crmcloudsync_phone_call_id is null then isnull(#s_crmcloudsync_phone_call_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_phone_call_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_phone_call_inserts
  left join p_crmcloudsync_phone_call
    on #s_crmcloudsync_phone_call_inserts.bk_hash = p_crmcloudsync_phone_call.bk_hash
   and p_crmcloudsync_phone_call.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_phone_call
    on p_crmcloudsync_phone_call.bk_hash = s_crmcloudsync_phone_call.bk_hash
   and p_crmcloudsync_phone_call.s_crmcloudsync_phone_call_id = s_crmcloudsync_phone_call.s_crmcloudsync_phone_call_id
 where s_crmcloudsync_phone_call.s_crmcloudsync_phone_call_id is null
    or (s_crmcloudsync_phone_call.s_crmcloudsync_phone_call_id is not null
        and s_crmcloudsync_phone_call.dv_hash <> #s_crmcloudsync_phone_call_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_phone_call @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_phone_call @current_dv_batch_id

end
