CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_live_chat] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_ltf_livechat

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_ltf_livechat (
       bk_hash,
       activityadditionalparams,
       activityid,
       activitytypecode,
       activitytypecodename,
       actualdurationminutes,
       actualend,
       actualstart,
       bcc,
       cc,
       community,
       communityname,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       customers,
       deliverylastattemptedon,
       deliveryprioritycode,
       deliveryprioritycodename,
       description,
       exchangeitemid,
       exchangerate,
       exchangeweblink,
       [from],
       importsequencenumber,
       instancetypecode,
       instancetypecodename,
       isbilled,
       isbilledname,
       ismapiprivate,
       ismapiprivatename,
       isregularactivity,
       isregularactivityname,
       isworkflowcreated,
       isworkflowcreatedname,
       lastonholdtime,
       leftvoicemail,
       leftvoicemailname,
       ltf_appointmentstart,
       ltf_appointmentsubject,
       ltf_chatwrapup,
       ltf_chatwrapupname,
       ltf_club,
       ltf_clubname,
       ltf_disqualifyreason,
       ltf_disqualifyreasonname,
       ltf_emailaddress1,
       ltf_employeeid,
       ltf_firstname,
       ltf_imspromo,
       ltf_lastname,
       ltf_membershiplevel,
       ltf_membershiplevelname,
       ltf_mmsclubid,
       ltf_notes,
       ltf_originallead,
       ltf_originalleadname,
       ltf_originalleadyominame,
       ltf_parkuntil,
       ltf_phone,
       ltf_proactiveorreactive,
       ltf_proactiveorreactivename,
       ltf_readytoprocess,
       ltf_readytoprocessname,
       ltf_recommendedmembership,
       ltf_recommendedmembershipname,
       ltf_referringurl,
       ltf_requestdate,
       ltf_requestid,
       ltf_routingmessage,
       ltf_routingstep,
       ltf_routingstepname,
       ltf_sendimsjoin,
       ltf_sendimsjoinname,
       ltf_serviceline,
       ltf_transcript,
       ltf_type,
       ltf_typename,
       ltf_webteam,
       ltf_webteamname,
       ltf_webteamyominame,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       onholdtime,
       optionalattendees,
       organizer,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       partners,
       postponeactivityprocessinguntil,
       prioritycode,
       prioritycodename,
       processid,
       regardingobjectid,
       regardingobjectidname,
       regardingobjectidyominame,
       regardingobjecttypecode,
       requiredattendees,
       resources,
       scheduleddurationminutes,
       scheduledend,
       scheduledstart,
       sendermailboxid,
       sendermailboxidname,
       senton,
       seriesid,
       serviceid,
       serviceidname,
       slaid,
       slainvokedid,
       slainvokedidname,
       slaname,
       sortdate,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       subject,
       timezoneruleversionnumber,
       [to],
       transactioncurrencyid,
       transactioncurrencyidname,
       traversedpath,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_mostrecentcasl,
       ltf_lineofbusiness,
       ltf_lineofbusinessname,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(activityid,'z#@$k%&P'))),2) bk_hash,
       activityadditionalparams,
       activityid,
       activitytypecode,
       activitytypecodename,
       actualdurationminutes,
       actualend,
       actualstart,
       bcc,
       cc,
       community,
       communityname,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       customers,
       deliverylastattemptedon,
       deliveryprioritycode,
       deliveryprioritycodename,
       description,
       exchangeitemid,
       exchangerate,
       exchangeweblink,
       [from],
       importsequencenumber,
       instancetypecode,
       instancetypecodename,
       isbilled,
       isbilledname,
       ismapiprivate,
       ismapiprivatename,
       isregularactivity,
       isregularactivityname,
       isworkflowcreated,
       isworkflowcreatedname,
       lastonholdtime,
       leftvoicemail,
       leftvoicemailname,
       ltf_appointmentstart,
       ltf_appointmentsubject,
       ltf_chatwrapup,
       ltf_chatwrapupname,
       ltf_club,
       ltf_clubname,
       ltf_disqualifyreason,
       ltf_disqualifyreasonname,
       ltf_emailaddress1,
       ltf_employeeid,
       ltf_firstname,
       ltf_imspromo,
       ltf_lastname,
       ltf_membershiplevel,
       ltf_membershiplevelname,
       ltf_mmsclubid,
       ltf_notes,
       ltf_originallead,
       ltf_originalleadname,
       ltf_originalleadyominame,
       ltf_parkuntil,
       ltf_phone,
       ltf_proactiveorreactive,
       ltf_proactiveorreactivename,
       ltf_readytoprocess,
       ltf_readytoprocessname,
       ltf_recommendedmembership,
       ltf_recommendedmembershipname,
       ltf_referringurl,
       ltf_requestdate,
       ltf_requestid,
       ltf_routingmessage,
       ltf_routingstep,
       ltf_routingstepname,
       ltf_sendimsjoin,
       ltf_sendimsjoinname,
       ltf_serviceline,
       ltf_transcript,
       ltf_type,
       ltf_typename,
       ltf_webteam,
       ltf_webteamname,
       ltf_webteamyominame,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       onholdtime,
       optionalattendees,
       organizer,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       partners,
       postponeactivityprocessinguntil,
       prioritycode,
       prioritycodename,
       processid,
       regardingobjectid,
       regardingobjectidname,
       regardingobjectidyominame,
       regardingobjecttypecode,
       requiredattendees,
       resources,
       scheduleddurationminutes,
       scheduledend,
       scheduledstart,
       sendermailboxid,
       sendermailboxidname,
       senton,
       seriesid,
       serviceid,
       serviceidname,
       slaid,
       slainvokedid,
       slainvokedidname,
       slaname,
       sortdate,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       subject,
       timezoneruleversionnumber,
       [to],
       transactioncurrencyid,
       transactioncurrencyidname,
       traversedpath,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_mostrecentcasl,
       ltf_lineofbusiness,
       ltf_lineofbusinessname,
       isnull(cast(stage_crmcloudsync_ltf_livechat.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_ltf_livechat
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_live_chat @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_live_chat (
       bk_hash,
       activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_ltf_livechat.bk_hash,
       stage_hash_crmcloudsync_ltf_livechat.activityid activity_id,
       isnull(cast(stage_hash_crmcloudsync_ltf_livechat.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_ltf_livechat
  left join h_crmcloudsync_ltf_live_chat
    on stage_hash_crmcloudsync_ltf_livechat.bk_hash = h_crmcloudsync_ltf_live_chat.bk_hash
 where h_crmcloudsync_ltf_live_chat_id is null
   and stage_hash_crmcloudsync_ltf_livechat.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_live_chat
if object_id('tempdb..#l_crmcloudsync_ltf_live_chat_inserts') is not null drop table #l_crmcloudsync_ltf_live_chat_inserts
create table #l_crmcloudsync_ltf_live_chat_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_ltf_livechat.bk_hash,
       stage_hash_crmcloudsync_ltf_livechat.activityid activity_id,
       stage_hash_crmcloudsync_ltf_livechat.createdby created_by,
       stage_hash_crmcloudsync_ltf_livechat.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_ltf_livechat.customers customers,
       stage_hash_crmcloudsync_ltf_livechat.deliveryprioritycode delivery_priority_code,
       stage_hash_crmcloudsync_ltf_livechat.exchangeitemid exchange_item_id,
       stage_hash_crmcloudsync_ltf_livechat.instancetypecode instance_type_code,
       stage_hash_crmcloudsync_ltf_livechat.ltf_club ltf_club,
       stage_hash_crmcloudsync_ltf_livechat.ltf_employeeid ltf_employee_id,
       stage_hash_crmcloudsync_ltf_livechat.ltf_mmsclubid ltf_mms_club_id,
       stage_hash_crmcloudsync_ltf_livechat.ltf_originallead ltf_original_lead,
       stage_hash_crmcloudsync_ltf_livechat.ltf_recommendedmembership ltf_recommended_membership,
       stage_hash_crmcloudsync_ltf_livechat.ltf_requestid ltf_request_id,
       stage_hash_crmcloudsync_ltf_livechat.ltf_webteam ltf_web_team,
       stage_hash_crmcloudsync_ltf_livechat.modifiedby modified_by,
       stage_hash_crmcloudsync_ltf_livechat.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_ltf_livechat.ownerid owner_id,
       stage_hash_crmcloudsync_ltf_livechat.owningteam owning_team,
       stage_hash_crmcloudsync_ltf_livechat.owninguser owning_user,
       stage_hash_crmcloudsync_ltf_livechat.processid process_id,
       stage_hash_crmcloudsync_ltf_livechat.regardingobjectid regarding_object_id,
       stage_hash_crmcloudsync_ltf_livechat.sendermailboxid sender_mail_box_id,
       stage_hash_crmcloudsync_ltf_livechat.seriesid series_id,
       stage_hash_crmcloudsync_ltf_livechat.serviceid service_id,
       stage_hash_crmcloudsync_ltf_livechat.slaid sla_id,
       stage_hash_crmcloudsync_ltf_livechat.slainvokedid sla_invoked_id,
       stage_hash_crmcloudsync_ltf_livechat.stageid stage_id,
       stage_hash_crmcloudsync_ltf_livechat.transactioncurrencyid transaction_currency_id,
       isnull(cast(stage_hash_crmcloudsync_ltf_livechat.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.customers,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.deliveryprioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.exchangeitemid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.instancetypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_club,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_employeeid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_mmsclubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_originallead,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.ltf_recommendedmembership as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_requestid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_webteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.regardingobjectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.sendermailboxid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.seriesid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.serviceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.slaid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.slainvokedid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.transactioncurrencyid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_ltf_livechat
 where stage_hash_crmcloudsync_ltf_livechat.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_live_chat records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_live_chat (
       bk_hash,
       activity_id,
       created_by,
       created_on_behalf_by,
       customers,
       delivery_priority_code,
       exchange_item_id,
       instance_type_code,
       ltf_club,
       ltf_employee_id,
       ltf_mms_club_id,
       ltf_original_lead,
       ltf_recommended_membership,
       ltf_request_id,
       ltf_web_team,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_team,
       owning_user,
       process_id,
       regarding_object_id,
       sender_mail_box_id,
       series_id,
       service_id,
       sla_id,
       sla_invoked_id,
       stage_id,
       transaction_currency_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_live_chat_inserts.bk_hash,
       #l_crmcloudsync_ltf_live_chat_inserts.activity_id,
       #l_crmcloudsync_ltf_live_chat_inserts.created_by,
       #l_crmcloudsync_ltf_live_chat_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_live_chat_inserts.customers,
       #l_crmcloudsync_ltf_live_chat_inserts.delivery_priority_code,
       #l_crmcloudsync_ltf_live_chat_inserts.exchange_item_id,
       #l_crmcloudsync_ltf_live_chat_inserts.instance_type_code,
       #l_crmcloudsync_ltf_live_chat_inserts.ltf_club,
       #l_crmcloudsync_ltf_live_chat_inserts.ltf_employee_id,
       #l_crmcloudsync_ltf_live_chat_inserts.ltf_mms_club_id,
       #l_crmcloudsync_ltf_live_chat_inserts.ltf_original_lead,
       #l_crmcloudsync_ltf_live_chat_inserts.ltf_recommended_membership,
       #l_crmcloudsync_ltf_live_chat_inserts.ltf_request_id,
       #l_crmcloudsync_ltf_live_chat_inserts.ltf_web_team,
       #l_crmcloudsync_ltf_live_chat_inserts.modified_by,
       #l_crmcloudsync_ltf_live_chat_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_live_chat_inserts.owner_id,
       #l_crmcloudsync_ltf_live_chat_inserts.owning_team,
       #l_crmcloudsync_ltf_live_chat_inserts.owning_user,
       #l_crmcloudsync_ltf_live_chat_inserts.process_id,
       #l_crmcloudsync_ltf_live_chat_inserts.regarding_object_id,
       #l_crmcloudsync_ltf_live_chat_inserts.sender_mail_box_id,
       #l_crmcloudsync_ltf_live_chat_inserts.series_id,
       #l_crmcloudsync_ltf_live_chat_inserts.service_id,
       #l_crmcloudsync_ltf_live_chat_inserts.sla_id,
       #l_crmcloudsync_ltf_live_chat_inserts.sla_invoked_id,
       #l_crmcloudsync_ltf_live_chat_inserts.stage_id,
       #l_crmcloudsync_ltf_live_chat_inserts.transaction_currency_id,
       case when l_crmcloudsync_ltf_live_chat.l_crmcloudsync_ltf_live_chat_id is null then isnull(#l_crmcloudsync_ltf_live_chat_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_live_chat_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_live_chat_inserts
  left join p_crmcloudsync_ltf_live_chat
    on #l_crmcloudsync_ltf_live_chat_inserts.bk_hash = p_crmcloudsync_ltf_live_chat.bk_hash
   and p_crmcloudsync_ltf_live_chat.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_live_chat
    on p_crmcloudsync_ltf_live_chat.bk_hash = l_crmcloudsync_ltf_live_chat.bk_hash
   and p_crmcloudsync_ltf_live_chat.l_crmcloudsync_ltf_live_chat_id = l_crmcloudsync_ltf_live_chat.l_crmcloudsync_ltf_live_chat_id
 where l_crmcloudsync_ltf_live_chat.l_crmcloudsync_ltf_live_chat_id is null
    or (l_crmcloudsync_ltf_live_chat.l_crmcloudsync_ltf_live_chat_id is not null
        and l_crmcloudsync_ltf_live_chat.dv_hash <> #l_crmcloudsync_ltf_live_chat_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_live_chat
if object_id('tempdb..#s_crmcloudsync_ltf_live_chat_inserts') is not null drop table #s_crmcloudsync_ltf_live_chat_inserts
create table #s_crmcloudsync_ltf_live_chat_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_ltf_livechat.bk_hash,
       stage_hash_crmcloudsync_ltf_livechat.activityadditionalparams activity_additional_params,
       stage_hash_crmcloudsync_ltf_livechat.activityid activity_id,
       stage_hash_crmcloudsync_ltf_livechat.activitytypecode activity_type_code,
       stage_hash_crmcloudsync_ltf_livechat.activitytypecodename activity_type_code_name,
       stage_hash_crmcloudsync_ltf_livechat.actualdurationminutes actual_duration_minutes,
       stage_hash_crmcloudsync_ltf_livechat.actualend actual_end,
       stage_hash_crmcloudsync_ltf_livechat.actualstart actual_start,
       stage_hash_crmcloudsync_ltf_livechat.bcc bcc,
       stage_hash_crmcloudsync_ltf_livechat.cc cc,
       stage_hash_crmcloudsync_ltf_livechat.community community,
       stage_hash_crmcloudsync_ltf_livechat.communityname community_name,
       stage_hash_crmcloudsync_ltf_livechat.createdbyname created_by_name,
       stage_hash_crmcloudsync_ltf_livechat.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_ltf_livechat.createdon created_on,
       stage_hash_crmcloudsync_ltf_livechat.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_ltf_livechat.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_ltf_livechat.deliverylastattemptedon delivery_last_attempted_on,
       stage_hash_crmcloudsync_ltf_livechat.deliveryprioritycodename delivery_priority_code_name,
       stage_hash_crmcloudsync_ltf_livechat.description description,
       stage_hash_crmcloudsync_ltf_livechat.exchangerate exchange_rate,
       stage_hash_crmcloudsync_ltf_livechat.exchangeweblink exchange_web_link,
       stage_hash_crmcloudsync_ltf_livechat.[from] [from],
       stage_hash_crmcloudsync_ltf_livechat.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_ltf_livechat.instancetypecodename instance_type_code_name,
       stage_hash_crmcloudsync_ltf_livechat.isbilled is_billed,
       stage_hash_crmcloudsync_ltf_livechat.isbilledname is_billed_name,
       stage_hash_crmcloudsync_ltf_livechat.ismapiprivate is_mapi_private,
       stage_hash_crmcloudsync_ltf_livechat.ismapiprivatename is_mapi_private_name,
       stage_hash_crmcloudsync_ltf_livechat.isregularactivity is_regular_activity,
       stage_hash_crmcloudsync_ltf_livechat.isregularactivityname is_regular_activity_name,
       stage_hash_crmcloudsync_ltf_livechat.isworkflowcreated is_workflow_created,
       stage_hash_crmcloudsync_ltf_livechat.isworkflowcreatedname is_workflow_created_name,
       stage_hash_crmcloudsync_ltf_livechat.lastonholdtime last_on_hold_time,
       stage_hash_crmcloudsync_ltf_livechat.leftvoicemail left_voice_mail,
       stage_hash_crmcloudsync_ltf_livechat.leftvoicemailname left_voice_mail_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_appointmentstart ltf_appointment_start,
       stage_hash_crmcloudsync_ltf_livechat.ltf_appointmentsubject ltf_appointment_subject,
       stage_hash_crmcloudsync_ltf_livechat.ltf_chatwrapup ltf_chat_wrap_up,
       stage_hash_crmcloudsync_ltf_livechat.ltf_chatwrapupname ltf_chat_wrap_up_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_clubname ltf_club_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_disqualifyreason ltf_disqualify_reason,
       stage_hash_crmcloudsync_ltf_livechat.ltf_disqualifyreasonname ltf_disqualify_reason_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_emailaddress1 ltf_email_address_1,
       stage_hash_crmcloudsync_ltf_livechat.ltf_firstname ltf_first_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_imspromo ltf_ims_promo,
       stage_hash_crmcloudsync_ltf_livechat.ltf_lastname ltf_last_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_membershiplevel ltf_membership_level,
       stage_hash_crmcloudsync_ltf_livechat.ltf_membershiplevelname ltf_membership_level_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_notes ltf_notes,
       stage_hash_crmcloudsync_ltf_livechat.ltf_originalleadname ltf_original_lead_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_originalleadyominame ltf_original_lead_yomi_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_parkuntil ltf_park_until,
       stage_hash_crmcloudsync_ltf_livechat.ltf_phone ltf_phone,
       stage_hash_crmcloudsync_ltf_livechat.ltf_proactiveorreactive ltf_proactive_or_reactive,
       stage_hash_crmcloudsync_ltf_livechat.ltf_proactiveorreactivename ltf_proactive_or_reactive_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_readytoprocess ltf_ready_to_process,
       stage_hash_crmcloudsync_ltf_livechat.ltf_readytoprocessname ltf_ready_to_process_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_recommendedmembershipname ltf_recommended_membership_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_referringurl ltf_referring_url,
       stage_hash_crmcloudsync_ltf_livechat.ltf_requestdate ltf_request_date,
       stage_hash_crmcloudsync_ltf_livechat.ltf_routingmessage ltf_routing_message,
       stage_hash_crmcloudsync_ltf_livechat.ltf_routingstep ltf_routing_step,
       stage_hash_crmcloudsync_ltf_livechat.ltf_routingstepname ltf_routing_step_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_sendimsjoin ltf_send_ims_join,
       stage_hash_crmcloudsync_ltf_livechat.ltf_sendimsjoinname ltf_send_ims_join_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_serviceline ltf_service_line,
       stage_hash_crmcloudsync_ltf_livechat.ltf_transcript ltf_transcript,
       stage_hash_crmcloudsync_ltf_livechat.ltf_type ltf_type,
       stage_hash_crmcloudsync_ltf_livechat.ltf_typename ltf_type_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_webteamname ltf_web_team_name,
       stage_hash_crmcloudsync_ltf_livechat.ltf_webteamyominame ltf_web_team_yomi_name,
       stage_hash_crmcloudsync_ltf_livechat.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_ltf_livechat.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_ltf_livechat.modifiedon modified_on,
       stage_hash_crmcloudsync_ltf_livechat.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_ltf_livechat.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_ltf_livechat.onholdtime on_hold_time,
       stage_hash_crmcloudsync_ltf_livechat.optionalattendees optional_attendees,
       stage_hash_crmcloudsync_ltf_livechat.organizer organizer,
       stage_hash_crmcloudsync_ltf_livechat.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_ltf_livechat.owneridname ownerid_name,
       stage_hash_crmcloudsync_ltf_livechat.owneridtype owner_id_type,
       stage_hash_crmcloudsync_ltf_livechat.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_ltf_livechat.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_ltf_livechat.partners partners,
       stage_hash_crmcloudsync_ltf_livechat.postponeactivityprocessinguntil postpone_activity_processing_until,
       stage_hash_crmcloudsync_ltf_livechat.prioritycode priority_code,
       stage_hash_crmcloudsync_ltf_livechat.prioritycodename priority_code_name,
       stage_hash_crmcloudsync_ltf_livechat.regardingobjectidname regarding_object_id_name,
       stage_hash_crmcloudsync_ltf_livechat.regardingobjectidyominame regarding_object_id_yomi_name,
       stage_hash_crmcloudsync_ltf_livechat.regardingobjecttypecode regarding_object_type_code,
       stage_hash_crmcloudsync_ltf_livechat.requiredattendees required_attendees,
       stage_hash_crmcloudsync_ltf_livechat.resources resources,
       stage_hash_crmcloudsync_ltf_livechat.scheduleddurationminutes scheduled_duration_minutes,
       stage_hash_crmcloudsync_ltf_livechat.scheduledend scheduled_end,
       stage_hash_crmcloudsync_ltf_livechat.scheduledstart scheduled_start,
       stage_hash_crmcloudsync_ltf_livechat.sendermailboxidname sender_mail_box_id_name,
       stage_hash_crmcloudsync_ltf_livechat.senton sent_on,
       stage_hash_crmcloudsync_ltf_livechat.serviceidname service_id_name,
       stage_hash_crmcloudsync_ltf_livechat.slainvokedidname sla_invoked_id_name,
       stage_hash_crmcloudsync_ltf_livechat.slaname sla_name,
       stage_hash_crmcloudsync_ltf_livechat.sortdate sort_date,
       stage_hash_crmcloudsync_ltf_livechat.statecode state_code,
       stage_hash_crmcloudsync_ltf_livechat.statecodename state_code_name,
       stage_hash_crmcloudsync_ltf_livechat.statuscode status_code,
       stage_hash_crmcloudsync_ltf_livechat.statuscodename status_code_name,
       stage_hash_crmcloudsync_ltf_livechat.subject subject,
       stage_hash_crmcloudsync_ltf_livechat.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_ltf_livechat.[to] [to],
       stage_hash_crmcloudsync_ltf_livechat.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_ltf_livechat.traversedpath traversed_path,
       stage_hash_crmcloudsync_ltf_livechat.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_ltf_livechat.versionnumber version_number,
       stage_hash_crmcloudsync_ltf_livechat.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_ltf_livechat.InsertUser insert_user,
       stage_hash_crmcloudsync_ltf_livechat.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_ltf_livechat.UpdateUser update_user,
       stage_hash_crmcloudsync_ltf_livechat.ltf_mostrecentcasl ltf_most_recent_casl,
       stage_hash_crmcloudsync_ltf_livechat.ltf_lineofbusiness ltf_line_of_business,
       stage_hash_crmcloudsync_ltf_livechat.ltf_lineofbusinessname ltf_line_of_business_name,
       isnull(cast(stage_hash_crmcloudsync_ltf_livechat.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.activityadditionalparams,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.activitytypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.activitytypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.actualdurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.actualend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.actualstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.bcc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.cc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.community as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.communityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.deliverylastattemptedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.deliveryprioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.exchangeweblink,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.[from],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.instancetypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.isbilled,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.isbilledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ismapiprivate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ismapiprivatename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.isregularactivity,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.isregularactivityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.isworkflowcreated,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.isworkflowcreatedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.lastonholdtime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.leftvoicemail,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.leftvoicemailname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.ltf_appointmentstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_appointmentsubject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.ltf_chatwrapup as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_chatwrapupname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_clubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.ltf_disqualifyreason as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_disqualifyreasonname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_emailaddress1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_imspromo,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.ltf_membershiplevel as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_membershiplevelname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_notes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_originalleadname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_originalleadyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.ltf_parkuntil,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.ltf_proactiveorreactive as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_proactiveorreactivename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_readytoprocess,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_readytoprocessname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_recommendedmembershipname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_referringurl,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.ltf_requestdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_routingmessage,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.ltf_routingstep as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_routingstepname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_sendimsjoin,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_sendimsjoinname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_serviceline,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_transcript,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.ltf_type as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_typename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_webteamname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_webteamyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.onholdtime as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.optionalattendees,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.organizer,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.partners,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.postponeactivityprocessinguntil,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.prioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.prioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.regardingobjectidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.regardingobjectidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.regardingobjecttypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.requiredattendees,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.resources,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.scheduleddurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.scheduledend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.scheduledstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.sendermailboxidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.senton,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.serviceidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.slainvokedidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.slaname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.sortdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.[to],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.traversedpath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltf_livechat.ltf_mostrecentcasl,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltf_livechat.ltf_lineofbusiness as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltf_livechat.ltf_lineofbusinessname,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_ltf_livechat
 where stage_hash_crmcloudsync_ltf_livechat.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_live_chat records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_live_chat (
       bk_hash,
       activity_additional_params,
       activity_id,
       activity_type_code,
       activity_type_code_name,
       actual_duration_minutes,
       actual_end,
       actual_start,
       bcc,
       cc,
       community,
       community_name,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       delivery_last_attempted_on,
       delivery_priority_code_name,
       description,
       exchange_rate,
       exchange_web_link,
       [from],
       import_sequence_number,
       instance_type_code_name,
       is_billed,
       is_billed_name,
       is_mapi_private,
       is_mapi_private_name,
       is_regular_activity,
       is_regular_activity_name,
       is_workflow_created,
       is_workflow_created_name,
       last_on_hold_time,
       left_voice_mail,
       left_voice_mail_name,
       ltf_appointment_start,
       ltf_appointment_subject,
       ltf_chat_wrap_up,
       ltf_chat_wrap_up_name,
       ltf_club_name,
       ltf_disqualify_reason,
       ltf_disqualify_reason_name,
       ltf_email_address_1,
       ltf_first_name,
       ltf_ims_promo,
       ltf_last_name,
       ltf_membership_level,
       ltf_membership_level_name,
       ltf_notes,
       ltf_original_lead_name,
       ltf_original_lead_yomi_name,
       ltf_park_until,
       ltf_phone,
       ltf_proactive_or_reactive,
       ltf_proactive_or_reactive_name,
       ltf_ready_to_process,
       ltf_ready_to_process_name,
       ltf_recommended_membership_name,
       ltf_referring_url,
       ltf_request_date,
       ltf_routing_message,
       ltf_routing_step,
       ltf_routing_step_name,
       ltf_send_ims_join,
       ltf_send_ims_join_name,
       ltf_service_line,
       ltf_transcript,
       ltf_type,
       ltf_type_name,
       ltf_web_team_name,
       ltf_web_team_yomi_name,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       on_hold_time,
       optional_attendees,
       organizer,
       overridden_created_on,
       ownerid_name,
       owner_id_type,
       owner_id_yomi_name,
       owning_business_unit,
       partners,
       postpone_activity_processing_until,
       priority_code,
       priority_code_name,
       regarding_object_id_name,
       regarding_object_id_yomi_name,
       regarding_object_type_code,
       required_attendees,
       resources,
       scheduled_duration_minutes,
       scheduled_end,
       scheduled_start,
       sender_mail_box_id_name,
       sent_on,
       service_id_name,
       sla_invoked_id_name,
       sla_name,
       sort_date,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       subject,
       time_zone_rule_version_number,
       [to],
       transaction_currency_id_name,
       traversed_path,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       ltf_most_recent_casl,
       ltf_line_of_business,
       ltf_line_of_business_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_live_chat_inserts.bk_hash,
       #s_crmcloudsync_ltf_live_chat_inserts.activity_additional_params,
       #s_crmcloudsync_ltf_live_chat_inserts.activity_id,
       #s_crmcloudsync_ltf_live_chat_inserts.activity_type_code,
       #s_crmcloudsync_ltf_live_chat_inserts.activity_type_code_name,
       #s_crmcloudsync_ltf_live_chat_inserts.actual_duration_minutes,
       #s_crmcloudsync_ltf_live_chat_inserts.actual_end,
       #s_crmcloudsync_ltf_live_chat_inserts.actual_start,
       #s_crmcloudsync_ltf_live_chat_inserts.bcc,
       #s_crmcloudsync_ltf_live_chat_inserts.cc,
       #s_crmcloudsync_ltf_live_chat_inserts.community,
       #s_crmcloudsync_ltf_live_chat_inserts.community_name,
       #s_crmcloudsync_ltf_live_chat_inserts.created_by_name,
       #s_crmcloudsync_ltf_live_chat_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_live_chat_inserts.created_on,
       #s_crmcloudsync_ltf_live_chat_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_live_chat_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_live_chat_inserts.delivery_last_attempted_on,
       #s_crmcloudsync_ltf_live_chat_inserts.delivery_priority_code_name,
       #s_crmcloudsync_ltf_live_chat_inserts.description,
       #s_crmcloudsync_ltf_live_chat_inserts.exchange_rate,
       #s_crmcloudsync_ltf_live_chat_inserts.exchange_web_link,
       #s_crmcloudsync_ltf_live_chat_inserts.[from],
       #s_crmcloudsync_ltf_live_chat_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_live_chat_inserts.instance_type_code_name,
       #s_crmcloudsync_ltf_live_chat_inserts.is_billed,
       #s_crmcloudsync_ltf_live_chat_inserts.is_billed_name,
       #s_crmcloudsync_ltf_live_chat_inserts.is_mapi_private,
       #s_crmcloudsync_ltf_live_chat_inserts.is_mapi_private_name,
       #s_crmcloudsync_ltf_live_chat_inserts.is_regular_activity,
       #s_crmcloudsync_ltf_live_chat_inserts.is_regular_activity_name,
       #s_crmcloudsync_ltf_live_chat_inserts.is_workflow_created,
       #s_crmcloudsync_ltf_live_chat_inserts.is_workflow_created_name,
       #s_crmcloudsync_ltf_live_chat_inserts.last_on_hold_time,
       #s_crmcloudsync_ltf_live_chat_inserts.left_voice_mail,
       #s_crmcloudsync_ltf_live_chat_inserts.left_voice_mail_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_appointment_start,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_appointment_subject,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_chat_wrap_up,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_chat_wrap_up_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_club_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_disqualify_reason,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_disqualify_reason_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_email_address_1,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_first_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_ims_promo,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_last_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_membership_level,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_membership_level_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_notes,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_original_lead_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_original_lead_yomi_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_park_until,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_phone,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_proactive_or_reactive,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_proactive_or_reactive_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_ready_to_process,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_ready_to_process_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_recommended_membership_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_referring_url,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_request_date,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_routing_message,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_routing_step,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_routing_step_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_send_ims_join,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_send_ims_join_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_service_line,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_transcript,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_type,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_type_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_web_team_name,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_web_team_yomi_name,
       #s_crmcloudsync_ltf_live_chat_inserts.modified_by_name,
       #s_crmcloudsync_ltf_live_chat_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_live_chat_inserts.modified_on,
       #s_crmcloudsync_ltf_live_chat_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_live_chat_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_live_chat_inserts.on_hold_time,
       #s_crmcloudsync_ltf_live_chat_inserts.optional_attendees,
       #s_crmcloudsync_ltf_live_chat_inserts.organizer,
       #s_crmcloudsync_ltf_live_chat_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_live_chat_inserts.ownerid_name,
       #s_crmcloudsync_ltf_live_chat_inserts.owner_id_type,
       #s_crmcloudsync_ltf_live_chat_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_live_chat_inserts.owning_business_unit,
       #s_crmcloudsync_ltf_live_chat_inserts.partners,
       #s_crmcloudsync_ltf_live_chat_inserts.postpone_activity_processing_until,
       #s_crmcloudsync_ltf_live_chat_inserts.priority_code,
       #s_crmcloudsync_ltf_live_chat_inserts.priority_code_name,
       #s_crmcloudsync_ltf_live_chat_inserts.regarding_object_id_name,
       #s_crmcloudsync_ltf_live_chat_inserts.regarding_object_id_yomi_name,
       #s_crmcloudsync_ltf_live_chat_inserts.regarding_object_type_code,
       #s_crmcloudsync_ltf_live_chat_inserts.required_attendees,
       #s_crmcloudsync_ltf_live_chat_inserts.resources,
       #s_crmcloudsync_ltf_live_chat_inserts.scheduled_duration_minutes,
       #s_crmcloudsync_ltf_live_chat_inserts.scheduled_end,
       #s_crmcloudsync_ltf_live_chat_inserts.scheduled_start,
       #s_crmcloudsync_ltf_live_chat_inserts.sender_mail_box_id_name,
       #s_crmcloudsync_ltf_live_chat_inserts.sent_on,
       #s_crmcloudsync_ltf_live_chat_inserts.service_id_name,
       #s_crmcloudsync_ltf_live_chat_inserts.sla_invoked_id_name,
       #s_crmcloudsync_ltf_live_chat_inserts.sla_name,
       #s_crmcloudsync_ltf_live_chat_inserts.sort_date,
       #s_crmcloudsync_ltf_live_chat_inserts.state_code,
       #s_crmcloudsync_ltf_live_chat_inserts.state_code_name,
       #s_crmcloudsync_ltf_live_chat_inserts.status_code,
       #s_crmcloudsync_ltf_live_chat_inserts.status_code_name,
       #s_crmcloudsync_ltf_live_chat_inserts.subject,
       #s_crmcloudsync_ltf_live_chat_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_live_chat_inserts.[to],
       #s_crmcloudsync_ltf_live_chat_inserts.transaction_currency_id_name,
       #s_crmcloudsync_ltf_live_chat_inserts.traversed_path,
       #s_crmcloudsync_ltf_live_chat_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_live_chat_inserts.version_number,
       #s_crmcloudsync_ltf_live_chat_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_live_chat_inserts.insert_user,
       #s_crmcloudsync_ltf_live_chat_inserts.updated_date_time,
       #s_crmcloudsync_ltf_live_chat_inserts.update_user,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_most_recent_casl,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_line_of_business,
       #s_crmcloudsync_ltf_live_chat_inserts.ltf_line_of_business_name,
       case when s_crmcloudsync_ltf_live_chat.s_crmcloudsync_ltf_live_chat_id is null then isnull(#s_crmcloudsync_ltf_live_chat_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_live_chat_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_live_chat_inserts
  left join p_crmcloudsync_ltf_live_chat
    on #s_crmcloudsync_ltf_live_chat_inserts.bk_hash = p_crmcloudsync_ltf_live_chat.bk_hash
   and p_crmcloudsync_ltf_live_chat.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_live_chat
    on p_crmcloudsync_ltf_live_chat.bk_hash = s_crmcloudsync_ltf_live_chat.bk_hash
   and p_crmcloudsync_ltf_live_chat.s_crmcloudsync_ltf_live_chat_id = s_crmcloudsync_ltf_live_chat.s_crmcloudsync_ltf_live_chat_id
 where s_crmcloudsync_ltf_live_chat.s_crmcloudsync_ltf_live_chat_id is null
    or (s_crmcloudsync_ltf_live_chat.s_crmcloudsync_ltf_live_chat_id is not null
        and s_crmcloudsync_ltf_live_chat.dv_hash <> #s_crmcloudsync_ltf_live_chat_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_live_chat @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_live_chat @current_dv_batch_id

end
