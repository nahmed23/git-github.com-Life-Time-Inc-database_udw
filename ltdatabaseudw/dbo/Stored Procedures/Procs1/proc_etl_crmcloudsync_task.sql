CREATE PROC [dbo].[proc_etl_crmcloudsync_task] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_Task

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_Task (
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
       importsequencenumber,
       isbilled,
       isbilledname,
       isregularactivity,
       isregularactivityname,
       isworkflowcreated,
       isworkflowcreatedname,
       ltf_myhealthscorescheduleddate,
       ltf_tasktype,
       ltf_tasktypename,
       ltf_trainingservicesscheduleddate,
       ltf_udwid,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       percentcomplete,
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
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       ltf_healthscorescheduledname,
       ltf_trainingservicesscheduledname,
       ltf_whometontour,
       UpdatedDateTime,
       UpdateUser,
       traversedpath,
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
       importsequencenumber,
       isbilled,
       isbilledname,
       isregularactivity,
       isregularactivityname,
       isworkflowcreated,
       isworkflowcreatedname,
       ltf_myhealthscorescheduleddate,
       ltf_tasktype,
       ltf_tasktypename,
       ltf_trainingservicesscheduleddate,
       ltf_udwid,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       percentcomplete,
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
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       ltf_healthscorescheduledname,
       ltf_trainingservicesscheduledname,
       ltf_whometontour,
       UpdatedDateTime,
       UpdateUser,
       traversedpath,
       isnull(cast(stage_crmcloudsync_Task.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_Task
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_task @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_task (
       bk_hash,
       activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_Task.bk_hash,
       stage_hash_crmcloudsync_Task.activityid activity_id,
       isnull(cast(stage_hash_crmcloudsync_Task.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_Task
  left join h_crmcloudsync_task
    on stage_hash_crmcloudsync_Task.bk_hash = h_crmcloudsync_task.bk_hash
 where h_crmcloudsync_task_id is null
   and stage_hash_crmcloudsync_Task.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_task
if object_id('tempdb..#l_crmcloudsync_task_inserts') is not null drop table #l_crmcloudsync_task_inserts
create table #l_crmcloudsync_task_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Task.bk_hash,
       stage_hash_crmcloudsync_Task.activityid activity_id,
       stage_hash_crmcloudsync_Task.createdby created_by,
       stage_hash_crmcloudsync_Task.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_Task.ltf_udwid ltf_udw_id,
       stage_hash_crmcloudsync_Task.modifiedby modified_by,
       stage_hash_crmcloudsync_Task.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_Task.ownerid owner_id,
       stage_hash_crmcloudsync_Task.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_Task.owningteam owning_team,
       stage_hash_crmcloudsync_Task.owninguser owning_user,
       stage_hash_crmcloudsync_Task.processid process_id,
       stage_hash_crmcloudsync_Task.regardingobjectid regarding_object_id,
       stage_hash_crmcloudsync_Task.serviceid service_id,
       stage_hash_crmcloudsync_Task.stageid stage_id,
       stage_hash_crmcloudsync_Task.transactioncurrencyid transaction_currency_id,
       isnull(cast(stage_hash_crmcloudsync_Task.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.ltf_udwid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.regardingobjectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.serviceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.transactioncurrencyid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Task
 where stage_hash_crmcloudsync_Task.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_task records
set @insert_date_time = getdate()
insert into l_crmcloudsync_task (
       bk_hash,
       activity_id,
       created_by,
       created_on_behalf_by,
       ltf_udw_id,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       process_id,
       regarding_object_id,
       service_id,
       stage_id,
       transaction_currency_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_task_inserts.bk_hash,
       #l_crmcloudsync_task_inserts.activity_id,
       #l_crmcloudsync_task_inserts.created_by,
       #l_crmcloudsync_task_inserts.created_on_behalf_by,
       #l_crmcloudsync_task_inserts.ltf_udw_id,
       #l_crmcloudsync_task_inserts.modified_by,
       #l_crmcloudsync_task_inserts.modified_on_behalf_by,
       #l_crmcloudsync_task_inserts.owner_id,
       #l_crmcloudsync_task_inserts.owning_business_unit,
       #l_crmcloudsync_task_inserts.owning_team,
       #l_crmcloudsync_task_inserts.owning_user,
       #l_crmcloudsync_task_inserts.process_id,
       #l_crmcloudsync_task_inserts.regarding_object_id,
       #l_crmcloudsync_task_inserts.service_id,
       #l_crmcloudsync_task_inserts.stage_id,
       #l_crmcloudsync_task_inserts.transaction_currency_id,
       case when l_crmcloudsync_task.l_crmcloudsync_task_id is null then isnull(#l_crmcloudsync_task_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_task_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_task_inserts
  left join p_crmcloudsync_task
    on #l_crmcloudsync_task_inserts.bk_hash = p_crmcloudsync_task.bk_hash
   and p_crmcloudsync_task.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_task
    on p_crmcloudsync_task.bk_hash = l_crmcloudsync_task.bk_hash
   and p_crmcloudsync_task.l_crmcloudsync_task_id = l_crmcloudsync_task.l_crmcloudsync_task_id
 where l_crmcloudsync_task.l_crmcloudsync_task_id is null
    or (l_crmcloudsync_task.l_crmcloudsync_task_id is not null
        and l_crmcloudsync_task.dv_hash <> #l_crmcloudsync_task_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_task
if object_id('tempdb..#s_crmcloudsync_task_inserts') is not null drop table #s_crmcloudsync_task_inserts
create table #s_crmcloudsync_task_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Task.bk_hash,
       stage_hash_crmcloudsync_Task.activityid activity_id,
       stage_hash_crmcloudsync_Task.activitytypecode activity_type_code,
       stage_hash_crmcloudsync_Task.activitytypecodename activity_type_code_name,
       stage_hash_crmcloudsync_Task.actualdurationminutes actual_duration_minutes,
       stage_hash_crmcloudsync_Task.actualend actual_end,
       stage_hash_crmcloudsync_Task.actualstart actual_start,
       stage_hash_crmcloudsync_Task.category category,
       stage_hash_crmcloudsync_Task.createdbyname created_by_name,
       stage_hash_crmcloudsync_Task.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_Task.createdon created_on,
       stage_hash_crmcloudsync_Task.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_Task.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Task.description description,
       stage_hash_crmcloudsync_Task.exchangerate exchange_rate,
       stage_hash_crmcloudsync_Task.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_Task.isbilled is_billed,
       stage_hash_crmcloudsync_Task.isbilledname is_billed_name,
       stage_hash_crmcloudsync_Task.isregularactivity is_regular_activity,
       stage_hash_crmcloudsync_Task.isregularactivityname is_regular_activity_name,
       stage_hash_crmcloudsync_Task.isworkflowcreated is_workflow_created,
       stage_hash_crmcloudsync_Task.isworkflowcreatedname is_workflow_created_name,
       stage_hash_crmcloudsync_Task.ltf_myhealthscorescheduleddate ltf_my_health_score_scheduled_date,
       stage_hash_crmcloudsync_Task.ltf_tasktype ltf_task_type,
       stage_hash_crmcloudsync_Task.ltf_tasktypename ltf_task_type_name,
       stage_hash_crmcloudsync_Task.ltf_trainingservicesscheduleddate ltf_training_services_scheduled_date,
       stage_hash_crmcloudsync_Task.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_Task.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_Task.modifiedon modified_on,
       stage_hash_crmcloudsync_Task.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_Task.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Task.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_Task.owneridname owner_id_name,
       stage_hash_crmcloudsync_Task.owneridtype owner_id_type,
       stage_hash_crmcloudsync_Task.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_Task.percentcomplete percent_complete,
       stage_hash_crmcloudsync_Task.prioritycode priority_code,
       stage_hash_crmcloudsync_Task.prioritycodename priority_code_name,
       stage_hash_crmcloudsync_Task.regardingobjectidname regarding_object_id_name,
       stage_hash_crmcloudsync_Task.regardingobjectidyominame regarding_object_id_yomi_name,
       stage_hash_crmcloudsync_Task.regardingobjecttypecode regarding_object_type_code,
       stage_hash_crmcloudsync_Task.scheduleddurationminutes scheduled_duration_minutes,
       stage_hash_crmcloudsync_Task.scheduledend scheduled_end,
       stage_hash_crmcloudsync_Task.scheduledstart scheduled_start,
       stage_hash_crmcloudsync_Task.statecode state_code,
       stage_hash_crmcloudsync_Task.statecodename state_code_name,
       stage_hash_crmcloudsync_Task.statuscode status_code,
       stage_hash_crmcloudsync_Task.statuscodename status_code_name,
       stage_hash_crmcloudsync_Task.subcategory sub_category,
       stage_hash_crmcloudsync_Task.subject subject,
       stage_hash_crmcloudsync_Task.timezoneruleversionnumber timezone_rule_version_number,
       stage_hash_crmcloudsync_Task.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_Task.utcconversiontimezonecode utc_conversion_timezone_code,
       stage_hash_crmcloudsync_Task.versionnumber version_number,
       stage_hash_crmcloudsync_Task.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_Task.InsertUser insert_user,
       stage_hash_crmcloudsync_Task.ltf_healthscorescheduledname ltf_health_score_scheduled_name,
       stage_hash_crmcloudsync_Task.ltf_trainingservicesscheduledname ltf_training_services_scheduled_name,
       stage_hash_crmcloudsync_Task.ltf_whometontour ltf_who_me_ton_tour,
       stage_hash_crmcloudsync_Task.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_Task.UpdateUser update_user,
       stage_hash_crmcloudsync_Task.traversedpath traversed_path,
       isnull(cast(stage_hash_crmcloudsync_Task.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.activitytypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.activitytypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.actualdurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.actualend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.actualstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.category,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.isbilled as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.isbilledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.isregularactivity as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.isregularactivityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.isworkflowcreated as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.isworkflowcreatedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.ltf_myhealthscorescheduleddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.ltf_tasktype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.ltf_tasktypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.ltf_trainingservicesscheduleddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.percentcomplete as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.prioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.prioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.regardingobjectidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.regardingobjectidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.regardingobjecttypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.scheduleddurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.scheduledend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.scheduledstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.subcategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Task.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.ltf_healthscorescheduledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.ltf_trainingservicesscheduledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.ltf_whometontour,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Task.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Task.traversedpath,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Task
 where stage_hash_crmcloudsync_Task.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_task records
set @insert_date_time = getdate()
insert into s_crmcloudsync_task (
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
       import_sequence_number,
       is_billed,
       is_billed_name,
       is_regular_activity,
       is_regular_activity_name,
       is_workflow_created,
       is_workflow_created_name,
       ltf_my_health_score_scheduled_date,
       ltf_task_type,
       ltf_task_type_name,
       ltf_training_services_scheduled_date,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       overridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       percent_complete,
       priority_code,
       priority_code_name,
       regarding_object_id_name,
       regarding_object_id_yomi_name,
       regarding_object_type_code,
       scheduled_duration_minutes,
       scheduled_end,
       scheduled_start,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       sub_category,
       subject,
       timezone_rule_version_number,
       transaction_currency_id_name,
       utc_conversion_timezone_code,
       version_number,
       inserted_date_time,
       insert_user,
       ltf_health_score_scheduled_name,
       ltf_training_services_scheduled_name,
       ltf_who_me_ton_tour,
       updated_date_time,
       update_user,
       traversed_path,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_task_inserts.bk_hash,
       #s_crmcloudsync_task_inserts.activity_id,
       #s_crmcloudsync_task_inserts.activity_type_code,
       #s_crmcloudsync_task_inserts.activity_type_code_name,
       #s_crmcloudsync_task_inserts.actual_duration_minutes,
       #s_crmcloudsync_task_inserts.actual_end,
       #s_crmcloudsync_task_inserts.actual_start,
       #s_crmcloudsync_task_inserts.category,
       #s_crmcloudsync_task_inserts.created_by_name,
       #s_crmcloudsync_task_inserts.created_by_yomi_name,
       #s_crmcloudsync_task_inserts.created_on,
       #s_crmcloudsync_task_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_task_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_task_inserts.description,
       #s_crmcloudsync_task_inserts.exchange_rate,
       #s_crmcloudsync_task_inserts.import_sequence_number,
       #s_crmcloudsync_task_inserts.is_billed,
       #s_crmcloudsync_task_inserts.is_billed_name,
       #s_crmcloudsync_task_inserts.is_regular_activity,
       #s_crmcloudsync_task_inserts.is_regular_activity_name,
       #s_crmcloudsync_task_inserts.is_workflow_created,
       #s_crmcloudsync_task_inserts.is_workflow_created_name,
       #s_crmcloudsync_task_inserts.ltf_my_health_score_scheduled_date,
       #s_crmcloudsync_task_inserts.ltf_task_type,
       #s_crmcloudsync_task_inserts.ltf_task_type_name,
       #s_crmcloudsync_task_inserts.ltf_training_services_scheduled_date,
       #s_crmcloudsync_task_inserts.modified_by_name,
       #s_crmcloudsync_task_inserts.modified_by_yomi_name,
       #s_crmcloudsync_task_inserts.modified_on,
       #s_crmcloudsync_task_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_task_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_task_inserts.overridden_created_on,
       #s_crmcloudsync_task_inserts.owner_id_name,
       #s_crmcloudsync_task_inserts.owner_id_type,
       #s_crmcloudsync_task_inserts.owner_id_yomi_name,
       #s_crmcloudsync_task_inserts.percent_complete,
       #s_crmcloudsync_task_inserts.priority_code,
       #s_crmcloudsync_task_inserts.priority_code_name,
       #s_crmcloudsync_task_inserts.regarding_object_id_name,
       #s_crmcloudsync_task_inserts.regarding_object_id_yomi_name,
       #s_crmcloudsync_task_inserts.regarding_object_type_code,
       #s_crmcloudsync_task_inserts.scheduled_duration_minutes,
       #s_crmcloudsync_task_inserts.scheduled_end,
       #s_crmcloudsync_task_inserts.scheduled_start,
       #s_crmcloudsync_task_inserts.state_code,
       #s_crmcloudsync_task_inserts.state_code_name,
       #s_crmcloudsync_task_inserts.status_code,
       #s_crmcloudsync_task_inserts.status_code_name,
       #s_crmcloudsync_task_inserts.sub_category,
       #s_crmcloudsync_task_inserts.subject,
       #s_crmcloudsync_task_inserts.timezone_rule_version_number,
       #s_crmcloudsync_task_inserts.transaction_currency_id_name,
       #s_crmcloudsync_task_inserts.utc_conversion_timezone_code,
       #s_crmcloudsync_task_inserts.version_number,
       #s_crmcloudsync_task_inserts.inserted_date_time,
       #s_crmcloudsync_task_inserts.insert_user,
       #s_crmcloudsync_task_inserts.ltf_health_score_scheduled_name,
       #s_crmcloudsync_task_inserts.ltf_training_services_scheduled_name,
       #s_crmcloudsync_task_inserts.ltf_who_me_ton_tour,
       #s_crmcloudsync_task_inserts.updated_date_time,
       #s_crmcloudsync_task_inserts.update_user,
       #s_crmcloudsync_task_inserts.traversed_path,
       case when s_crmcloudsync_task.s_crmcloudsync_task_id is null then isnull(#s_crmcloudsync_task_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_task_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_task_inserts
  left join p_crmcloudsync_task
    on #s_crmcloudsync_task_inserts.bk_hash = p_crmcloudsync_task.bk_hash
   and p_crmcloudsync_task.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_task
    on p_crmcloudsync_task.bk_hash = s_crmcloudsync_task.bk_hash
   and p_crmcloudsync_task.s_crmcloudsync_task_id = s_crmcloudsync_task.s_crmcloudsync_task_id
 where s_crmcloudsync_task.s_crmcloudsync_task_id is null
    or (s_crmcloudsync_task.s_crmcloudsync_task_id is not null
        and s_crmcloudsync_task.dv_hash <> #s_crmcloudsync_task_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_task @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_task @current_dv_batch_id

end
