CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_inquiry] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_Inquiry

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_Inquiry (
       bk_hash,
       activityid,
       activitytypecode,
       activitytypecodename,
       actualdurationminutes,
       actualend,
       actualstart,
       bcc,
       cc,
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
       exchangerate,
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
       leftvoicemail,
       leftvoicemailname,
       ltf_address1_city,
       ltf_address1_country,
       ltf_address1_line1,
       ltf_address1_line2,
       ltf_address1_postalcode,
       ltf_address1_stateorprovince,
       ltf_age,
       ltf_besttimetocontact,
       ltf_birtdate,
       ltf_birthdate,
       ltf_birthyear,
       ltf_cid,
       ltf_communicationconsent,
       ltf_communicationconsentname,
       ltf_consentdatetime,
       ltf_consentipaddress,
       ltf_consenttext,
       ltf_currentmember,
       ltf_currentmembername,
       ltf_custservemail,
       ltf_dcmp,
       ltf_destinationqueue,
       ltf_duplicatecontactfound,
       ltf_duplicatecontactfoundname,
       ltf_duplicateleadfound,
       ltf_duplicateleadfoundname,
       ltf_emailaddress1,
       ltf_emailtemplate,
       ltf_employeenumber,
       ltf_employer,
       ltf_exacttargetemailsent,
       ltf_exacttargetemailsentname,
       ltf_firstname,
       ltf_gcid,
       ltf_gclid,
       ltf_gendercode,
       ltf_gendercodename,
       ltf_group,
       ltf_inquirysource,
       ltf_inquirytype,
       ltf_interests,
       ltf_keywords,
       ltf_landingpage,
       ltf_lastname,
       ltf_latitude,
       ltf_leadtype,
       ltf_longitude,
       ltf_memberid,
       ltf_membershipinforequested,
       ltf_mmsclubid,
       ltf_primarygoal,
       ltf_referringcontactid,
       ltf_referringcontactidname,
       ltf_referringcontactidyominame,
       ltf_referringmemberid,
       ltf_registrationcode,
       ltf_requesttype,
       ltf_telephone1,
       ltf_telephone2,
       ltf_undereighteen,
       ltf_undereighteenname,
       ltf_utmcampaign,
       ltf_utmcontent,
       ltf_utmmedium,
       ltf_utmsource,
       ltf_utmterm,
       ltf_visitcount,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
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
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       Community,
       CommunityName,
       ltf_visitorid,
       ltf_clubname,
       ltf_clubnamename,
       ltf_device,
       ltf_operatingsystem,
       ltf_referringdomain,
       ltf_referringpage,
       ltf_useridleadid,
       traversedpath,
       activityadditionalparams,
       ltf_utmaudience,
       ltf_utmimage,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(activityid,'z#@$k%&P'))),2) bk_hash,
       activityid,
       activitytypecode,
       activitytypecodename,
       actualdurationminutes,
       actualend,
       actualstart,
       bcc,
       cc,
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
       exchangerate,
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
       leftvoicemail,
       leftvoicemailname,
       ltf_address1_city,
       ltf_address1_country,
       ltf_address1_line1,
       ltf_address1_line2,
       ltf_address1_postalcode,
       ltf_address1_stateorprovince,
       ltf_age,
       ltf_besttimetocontact,
       ltf_birtdate,
       ltf_birthdate,
       ltf_birthyear,
       ltf_cid,
       ltf_communicationconsent,
       ltf_communicationconsentname,
       ltf_consentdatetime,
       ltf_consentipaddress,
       ltf_consenttext,
       ltf_currentmember,
       ltf_currentmembername,
       ltf_custservemail,
       ltf_dcmp,
       ltf_destinationqueue,
       ltf_duplicatecontactfound,
       ltf_duplicatecontactfoundname,
       ltf_duplicateleadfound,
       ltf_duplicateleadfoundname,
       ltf_emailaddress1,
       ltf_emailtemplate,
       ltf_employeenumber,
       ltf_employer,
       ltf_exacttargetemailsent,
       ltf_exacttargetemailsentname,
       ltf_firstname,
       ltf_gcid,
       ltf_gclid,
       ltf_gendercode,
       ltf_gendercodename,
       ltf_group,
       ltf_inquirysource,
       ltf_inquirytype,
       ltf_interests,
       ltf_keywords,
       ltf_landingpage,
       ltf_lastname,
       ltf_latitude,
       ltf_leadtype,
       ltf_longitude,
       ltf_memberid,
       ltf_membershipinforequested,
       ltf_mmsclubid,
       ltf_primarygoal,
       ltf_referringcontactid,
       ltf_referringcontactidname,
       ltf_referringcontactidyominame,
       ltf_referringmemberid,
       ltf_registrationcode,
       ltf_requesttype,
       ltf_telephone1,
       ltf_telephone2,
       ltf_undereighteen,
       ltf_undereighteenname,
       ltf_utmcampaign,
       ltf_utmcontent,
       ltf_utmmedium,
       ltf_utmsource,
       ltf_utmterm,
       ltf_visitcount,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
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
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       Community,
       CommunityName,
       ltf_visitorid,
       ltf_clubname,
       ltf_clubnamename,
       ltf_device,
       ltf_operatingsystem,
       ltf_referringdomain,
       ltf_referringpage,
       ltf_useridleadid,
       traversedpath,
       activityadditionalparams,
       ltf_utmaudience,
       ltf_utmimage,
       isnull(cast(stage_crmcloudsync_LTF_Inquiry.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_Inquiry
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_inquiry @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_inquiry (
       bk_hash,
       activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_Inquiry.bk_hash,
       stage_hash_crmcloudsync_LTF_Inquiry.activityid activity_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_Inquiry
  left join h_crmcloudsync_ltf_inquiry
    on stage_hash_crmcloudsync_LTF_Inquiry.bk_hash = h_crmcloudsync_ltf_inquiry.bk_hash
 where h_crmcloudsync_ltf_inquiry_id is null
   and stage_hash_crmcloudsync_LTF_Inquiry.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_inquiry
if object_id('tempdb..#l_crmcloudsync_ltf_inquiry_inserts') is not null drop table #l_crmcloudsync_ltf_inquiry_inserts
create table #l_crmcloudsync_ltf_inquiry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Inquiry.bk_hash,
       stage_hash_crmcloudsync_LTF_Inquiry.activityid activity_id,
       stage_hash_crmcloudsync_LTF_Inquiry.createdby created_by,
       stage_hash_crmcloudsync_LTF_Inquiry.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_cid ltf_cid,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_gcid ltf_gc_id,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_gclid ltf_gcl_id,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_memberid ltf_member_id,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_mmsclubid ltf_mms_clubid,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringcontactid ltf_referring_contact_id,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringmemberid ltf_referring_member_id,
       stage_hash_crmcloudsync_LTF_Inquiry.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_Inquiry.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Inquiry.ownerid owner_id,
       stage_hash_crmcloudsync_LTF_Inquiry.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_LTF_Inquiry.owningteam owning_team,
       stage_hash_crmcloudsync_LTF_Inquiry.owninguser owning_user,
       stage_hash_crmcloudsync_LTF_Inquiry.processid process_id,
       stage_hash_crmcloudsync_LTF_Inquiry.regardingobjectid regarding_object_id,
       stage_hash_crmcloudsync_LTF_Inquiry.sendermailboxid sender_mail_box_id,
       stage_hash_crmcloudsync_LTF_Inquiry.seriesid series_id,
       stage_hash_crmcloudsync_LTF_Inquiry.serviceid service_id,
       stage_hash_crmcloudsync_LTF_Inquiry.stageid stage_id,
       stage_hash_crmcloudsync_LTF_Inquiry.transactioncurrencyid transaction_currency_id,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_visitorid ltf_visitor_id,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_clubname ltf_club_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_useridleadid ltf_user_id_lead_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_cid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_gcid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_gclid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_memberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_mmsclubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringcontactid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringmemberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.regardingobjectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.sendermailboxid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.seriesid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.serviceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.transactioncurrencyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_visitorid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_clubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_useridleadid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Inquiry
 where stage_hash_crmcloudsync_LTF_Inquiry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_inquiry records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_inquiry (
       bk_hash,
       activity_id,
       created_by,
       created_on_behalf_by,
       ltf_cid,
       ltf_gc_id,
       ltf_gcl_id,
       ltf_member_id,
       ltf_mms_clubid,
       ltf_referring_contact_id,
       ltf_referring_member_id,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       process_id,
       regarding_object_id,
       sender_mail_box_id,
       series_id,
       service_id,
       stage_id,
       transaction_currency_id,
       ltf_visitor_id,
       ltf_club_name,
       ltf_user_id_lead_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_inquiry_inserts.bk_hash,
       #l_crmcloudsync_ltf_inquiry_inserts.activity_id,
       #l_crmcloudsync_ltf_inquiry_inserts.created_by,
       #l_crmcloudsync_ltf_inquiry_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_cid,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_gc_id,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_gcl_id,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_member_id,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_mms_clubid,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_referring_contact_id,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_referring_member_id,
       #l_crmcloudsync_ltf_inquiry_inserts.modified_by,
       #l_crmcloudsync_ltf_inquiry_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_inquiry_inserts.owner_id,
       #l_crmcloudsync_ltf_inquiry_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_inquiry_inserts.owning_team,
       #l_crmcloudsync_ltf_inquiry_inserts.owning_user,
       #l_crmcloudsync_ltf_inquiry_inserts.process_id,
       #l_crmcloudsync_ltf_inquiry_inserts.regarding_object_id,
       #l_crmcloudsync_ltf_inquiry_inserts.sender_mail_box_id,
       #l_crmcloudsync_ltf_inquiry_inserts.series_id,
       #l_crmcloudsync_ltf_inquiry_inserts.service_id,
       #l_crmcloudsync_ltf_inquiry_inserts.stage_id,
       #l_crmcloudsync_ltf_inquiry_inserts.transaction_currency_id,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_visitor_id,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_club_name,
       #l_crmcloudsync_ltf_inquiry_inserts.ltf_user_id_lead_id,
       case when l_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id is null then isnull(#l_crmcloudsync_ltf_inquiry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_inquiry_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_inquiry_inserts
  left join p_crmcloudsync_ltf_inquiry
    on #l_crmcloudsync_ltf_inquiry_inserts.bk_hash = p_crmcloudsync_ltf_inquiry.bk_hash
   and p_crmcloudsync_ltf_inquiry.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_inquiry
    on p_crmcloudsync_ltf_inquiry.bk_hash = l_crmcloudsync_ltf_inquiry.bk_hash
   and p_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id = l_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id
 where l_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id is null
    or (l_crmcloudsync_ltf_inquiry.l_crmcloudsync_ltf_inquiry_id is not null
        and l_crmcloudsync_ltf_inquiry.dv_hash <> #l_crmcloudsync_ltf_inquiry_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_inquiry
if object_id('tempdb..#s_crmcloudsync_ltf_inquiry_inserts') is not null drop table #s_crmcloudsync_ltf_inquiry_inserts
create table #s_crmcloudsync_ltf_inquiry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Inquiry.bk_hash,
       stage_hash_crmcloudsync_LTF_Inquiry.activityid activity_id,
       stage_hash_crmcloudsync_LTF_Inquiry.activitytypecode activity_type_code,
       stage_hash_crmcloudsync_LTF_Inquiry.activitytypecodename activity_type_code_name,
       stage_hash_crmcloudsync_LTF_Inquiry.actualdurationminutes actual_duration_minutes,
       stage_hash_crmcloudsync_LTF_Inquiry.actualend actual_end,
       stage_hash_crmcloudsync_LTF_Inquiry.actualstart actual_start,
       stage_hash_crmcloudsync_LTF_Inquiry.bcc bcc,
       stage_hash_crmcloudsync_LTF_Inquiry.cc cc,
       stage_hash_crmcloudsync_LTF_Inquiry.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_Inquiry.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Inquiry.createdon created_on,
       stage_hash_crmcloudsync_LTF_Inquiry.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Inquiry.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Inquiry.customers customers,
       stage_hash_crmcloudsync_LTF_Inquiry.deliverylastattemptedon delivery_last_attempted_on,
       stage_hash_crmcloudsync_LTF_Inquiry.deliveryprioritycode delivery_priority_code,
       stage_hash_crmcloudsync_LTF_Inquiry.deliveryprioritycodename delivery_priority_code_name,
       stage_hash_crmcloudsync_LTF_Inquiry.description description,
       stage_hash_crmcloudsync_LTF_Inquiry.exchangerate exchange_rate,
       stage_hash_crmcloudsync_LTF_Inquiry.[from] [from],
       stage_hash_crmcloudsync_LTF_Inquiry.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_Inquiry.instancetypecode instance_type_code,
       stage_hash_crmcloudsync_LTF_Inquiry.instancetypecodename instance_type_code_name,
       stage_hash_crmcloudsync_LTF_Inquiry.isbilled is_billed,
       stage_hash_crmcloudsync_LTF_Inquiry.isbilledname is_billed_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ismapiprivate is_mapi_private,
       stage_hash_crmcloudsync_LTF_Inquiry.ismapiprivatename is_mapi_private_name,
       stage_hash_crmcloudsync_LTF_Inquiry.isregularactivity is_regular_activity,
       stage_hash_crmcloudsync_LTF_Inquiry.isregularactivityname is_regular_activity_name,
       stage_hash_crmcloudsync_LTF_Inquiry.isworkflowcreated is_workflow_created,
       stage_hash_crmcloudsync_LTF_Inquiry.isworkflowcreatedname is_workflow_created_name,
       stage_hash_crmcloudsync_LTF_Inquiry.leftvoicemail left_voice_mail,
       stage_hash_crmcloudsync_LTF_Inquiry.leftvoicemailname left_voice_mail_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_city ltf_address_1_city,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_country ltf_address_1_country,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_line1 ltf_address_1_line_1,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_line2 ltf_address_1_line_2,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_postalcode ltf_address_1_postal_code,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_stateorprovince ltf_address_1_state_or_province,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_age ltf_age,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_besttimetocontact ltf_best_time_to_contact,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_birtdate ltf_birt_date,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_birthdate ltf_birth_date,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_birthyear ltf_birth_year,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_communicationconsent ltf_communication_consent,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_communicationconsentname ltf_communication_consent_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_consentdatetime ltf_consent_date_time,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_consentipaddress ltf_consent_ip_address,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_consenttext ltf_consent_text,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_currentmember ltf_current_member,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_currentmembername ltf_current_member_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_custservemail ltf_cust_serve_mail,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_dcmp ltf_dcmp,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_destinationqueue ltf_destination_queue,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_duplicatecontactfound ltf_duplicate_contact_found,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_duplicatecontactfoundname ltf_duplicate_contact_found_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_duplicateleadfound ltf_duplicate_lead_found,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_duplicateleadfoundname ltf_duplicate_lead_foundname,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_emailaddress1 ltf_email_address1,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_emailtemplate ltf_email_template,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_employeenumber ltf_employee_number,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_employer ltf_employer,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_exacttargetemailsent ltf_exact_target_email_sent,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_exacttargetemailsentname ltf_exact_target_email_sent_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_firstname ltf_first_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_gendercode ltf_gender_code,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_gendercodename ltf_gender_code_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_group ltf_group,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_inquirysource ltf_inquiry_source,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_inquirytype ltf_inquiry_type,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_interests ltf_interests,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_keywords ltf_keywords,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_landingpage ltf_landing_page,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_lastname ltf_last_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_latitude ltf_latitude,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_leadtype ltf_lead_type,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_longitude ltf_longitude,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_membershipinforequested ltf_membership_info_requested,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_primarygoal ltf_primary_goal,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringcontactidname ltf_referring_contactid_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringcontactidyominame ltf_referring_contact_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_registrationcode ltf_registration_code,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_requesttype ltf_request_type,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_telephone1 ltf_telephone1,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_telephone2 ltf_telephone2,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_undereighteen ltf_under_eighteen,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_undereighteenname ltf_under_eighteen_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmcampaign ltf_utm_campaign,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmcontent ltf_utm_content,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmmedium ltf_utm_medium,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmsource ltf_utm_source,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmterm ltf_utm_term,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_visitcount ltf_visit_count,
       stage_hash_crmcloudsync_LTF_Inquiry.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_Inquiry.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Inquiry.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_Inquiry.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Inquiry.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Inquiry.optionalattendees optional_attendees,
       stage_hash_crmcloudsync_LTF_Inquiry.organizer organizer,
       stage_hash_crmcloudsync_LTF_Inquiry.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_Inquiry.owneridname owner_id_name,
       stage_hash_crmcloudsync_LTF_Inquiry.owneridtype owner_id_type,
       stage_hash_crmcloudsync_LTF_Inquiry.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Inquiry.partners partners,
       stage_hash_crmcloudsync_LTF_Inquiry.postponeactivityprocessinguntil postpone_activity_processing_until,
       stage_hash_crmcloudsync_LTF_Inquiry.prioritycode priority_code,
       stage_hash_crmcloudsync_LTF_Inquiry.prioritycodename priority_code_name,
       stage_hash_crmcloudsync_LTF_Inquiry.regardingobjectidname regarding_object_id_name,
       stage_hash_crmcloudsync_LTF_Inquiry.regardingobjectidyominame regarding_object_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Inquiry.regardingobjecttypecode regarding_object_type_code,
       stage_hash_crmcloudsync_LTF_Inquiry.requiredattendees required_attendees,
       stage_hash_crmcloudsync_LTF_Inquiry.resources resources,
       stage_hash_crmcloudsync_LTF_Inquiry.scheduleddurationminutes scheduled_duration_minutes,
       stage_hash_crmcloudsync_LTF_Inquiry.scheduledend scheduled_end,
       stage_hash_crmcloudsync_LTF_Inquiry.scheduledstart scheduled_start,
       stage_hash_crmcloudsync_LTF_Inquiry.sendermailboxidname sender_mail_box_id_name,
       stage_hash_crmcloudsync_LTF_Inquiry.senton senton,
       stage_hash_crmcloudsync_LTF_Inquiry.serviceidname service_id_name,
       stage_hash_crmcloudsync_LTF_Inquiry.statecode state_code,
       stage_hash_crmcloudsync_LTF_Inquiry.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_Inquiry.statuscode status_code,
       stage_hash_crmcloudsync_LTF_Inquiry.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_Inquiry.subject subject,
       stage_hash_crmcloudsync_LTF_Inquiry.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_Inquiry.[to] [to],
       stage_hash_crmcloudsync_LTF_Inquiry.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_LTF_Inquiry.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_Inquiry.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_Inquiry.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_Inquiry.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_Inquiry.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_Inquiry.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_Inquiry.Community community,
       stage_hash_crmcloudsync_LTF_Inquiry.CommunityName community_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_clubnamename ltf_club_name_name,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_device ltf_device,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_operatingsystem ltf_operating_system,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringdomain ltf_referring_domain,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringpage ltf_referring_page,
       stage_hash_crmcloudsync_LTF_Inquiry.traversedpath traversed_path,
       stage_hash_crmcloudsync_LTF_Inquiry.activityadditionalparams activity_additional_params,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmaudience ltf_utm_audience,
       stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmimage ltf_utm_image,
       isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.activitytypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.activitytypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.actualdurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.actualend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.actualstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.bcc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.cc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.customers,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.deliverylastattemptedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.deliveryprioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.deliveryprioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.[from],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.instancetypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.instancetypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.isbilled as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.isbilledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ismapiprivate as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ismapiprivatename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.isregularactivity as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.isregularactivityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.isworkflowcreated as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.isworkflowcreatedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.leftvoicemail as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.leftvoicemailname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_address1_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_age as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_besttimetocontact,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.ltf_birtdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.ltf_birthdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_birthyear,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_communicationconsent as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_communicationconsentname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.ltf_consentdatetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_consentipaddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_consenttext,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_currentmember as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_currentmembername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_custservemail,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_dcmp,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_destinationqueue,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_duplicatecontactfound as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_duplicatecontactfoundname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_duplicateleadfound as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_duplicateleadfoundname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_emailaddress1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_emailtemplate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_employeenumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_employer,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_exacttargetemailsent as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_exacttargetemailsentname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_gendercode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_gendercodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_group,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_inquirysource,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_inquirytype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_interests,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_keywords,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_landingpage,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_latitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_leadtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_membershipinforequested,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_primarygoal,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringcontactidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringcontactidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_registrationcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_requesttype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_telephone1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_telephone2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_undereighteen as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_undereighteenname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmcampaign,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmcontent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmmedium,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmsource,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmterm,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.ltf_visitcount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.optionalattendees,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.organizer,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.partners,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.postponeactivityprocessinguntil,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.prioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.prioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.regardingobjectidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.regardingobjectidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.regardingobjecttypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.requiredattendees,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.resources,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.scheduleddurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.scheduledend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.scheduledstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.sendermailboxidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.senton,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.serviceidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.[to],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Inquiry.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Inquiry.Community as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.CommunityName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_clubnamename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_device,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_operatingsystem,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringdomain,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_referringpage,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.traversedpath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.activityadditionalparams,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmaudience,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Inquiry.ltf_utmimage,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Inquiry
 where stage_hash_crmcloudsync_LTF_Inquiry.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_inquiry records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_inquiry (
       bk_hash,
       activity_id,
       activity_type_code,
       activity_type_code_name,
       actual_duration_minutes,
       actual_end,
       actual_start,
       bcc,
       cc,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       customers,
       delivery_last_attempted_on,
       delivery_priority_code,
       delivery_priority_code_name,
       description,
       exchange_rate,
       [from],
       import_sequence_number,
       instance_type_code,
       instance_type_code_name,
       is_billed,
       is_billed_name,
       is_mapi_private,
       is_mapi_private_name,
       is_regular_activity,
       is_regular_activity_name,
       is_workflow_created,
       is_workflow_created_name,
       left_voice_mail,
       left_voice_mail_name,
       ltf_address_1_city,
       ltf_address_1_country,
       ltf_address_1_line_1,
       ltf_address_1_line_2,
       ltf_address_1_postal_code,
       ltf_address_1_state_or_province,
       ltf_age,
       ltf_best_time_to_contact,
       ltf_birt_date,
       ltf_birth_date,
       ltf_birth_year,
       ltf_communication_consent,
       ltf_communication_consent_name,
       ltf_consent_date_time,
       ltf_consent_ip_address,
       ltf_consent_text,
       ltf_current_member,
       ltf_current_member_name,
       ltf_cust_serve_mail,
       ltf_dcmp,
       ltf_destination_queue,
       ltf_duplicate_contact_found,
       ltf_duplicate_contact_found_name,
       ltf_duplicate_lead_found,
       ltf_duplicate_lead_foundname,
       ltf_email_address1,
       ltf_email_template,
       ltf_employee_number,
       ltf_employer,
       ltf_exact_target_email_sent,
       ltf_exact_target_email_sent_name,
       ltf_first_name,
       ltf_gender_code,
       ltf_gender_code_name,
       ltf_group,
       ltf_inquiry_source,
       ltf_inquiry_type,
       ltf_interests,
       ltf_keywords,
       ltf_landing_page,
       ltf_last_name,
       ltf_latitude,
       ltf_lead_type,
       ltf_longitude,
       ltf_membership_info_requested,
       ltf_primary_goal,
       ltf_referring_contactid_name,
       ltf_referring_contact_id_yomi_name,
       ltf_registration_code,
       ltf_request_type,
       ltf_telephone1,
       ltf_telephone2,
       ltf_under_eighteen,
       ltf_under_eighteen_name,
       ltf_utm_campaign,
       ltf_utm_content,
       ltf_utm_medium,
       ltf_utm_source,
       ltf_utm_term,
       ltf_visit_count,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       optional_attendees,
       organizer,
       overridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
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
       senton,
       service_id_name,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       subject,
       time_zone_rule_version_number,
       [to],
       transaction_currency_id_name,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       community,
       community_name,
       ltf_club_name_name,
       ltf_device,
       ltf_operating_system,
       ltf_referring_domain,
       ltf_referring_page,
       traversed_path,
       activity_additional_params,
       ltf_utm_audience,
       ltf_utm_image,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_inquiry_inserts.bk_hash,
       #s_crmcloudsync_ltf_inquiry_inserts.activity_id,
       #s_crmcloudsync_ltf_inquiry_inserts.activity_type_code,
       #s_crmcloudsync_ltf_inquiry_inserts.activity_type_code_name,
       #s_crmcloudsync_ltf_inquiry_inserts.actual_duration_minutes,
       #s_crmcloudsync_ltf_inquiry_inserts.actual_end,
       #s_crmcloudsync_ltf_inquiry_inserts.actual_start,
       #s_crmcloudsync_ltf_inquiry_inserts.bcc,
       #s_crmcloudsync_ltf_inquiry_inserts.cc,
       #s_crmcloudsync_ltf_inquiry_inserts.created_by_name,
       #s_crmcloudsync_ltf_inquiry_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_inquiry_inserts.created_on,
       #s_crmcloudsync_ltf_inquiry_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_inquiry_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_inquiry_inserts.customers,
       #s_crmcloudsync_ltf_inquiry_inserts.delivery_last_attempted_on,
       #s_crmcloudsync_ltf_inquiry_inserts.delivery_priority_code,
       #s_crmcloudsync_ltf_inquiry_inserts.delivery_priority_code_name,
       #s_crmcloudsync_ltf_inquiry_inserts.description,
       #s_crmcloudsync_ltf_inquiry_inserts.exchange_rate,
       #s_crmcloudsync_ltf_inquiry_inserts.[from],
       #s_crmcloudsync_ltf_inquiry_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_inquiry_inserts.instance_type_code,
       #s_crmcloudsync_ltf_inquiry_inserts.instance_type_code_name,
       #s_crmcloudsync_ltf_inquiry_inserts.is_billed,
       #s_crmcloudsync_ltf_inquiry_inserts.is_billed_name,
       #s_crmcloudsync_ltf_inquiry_inserts.is_mapi_private,
       #s_crmcloudsync_ltf_inquiry_inserts.is_mapi_private_name,
       #s_crmcloudsync_ltf_inquiry_inserts.is_regular_activity,
       #s_crmcloudsync_ltf_inquiry_inserts.is_regular_activity_name,
       #s_crmcloudsync_ltf_inquiry_inserts.is_workflow_created,
       #s_crmcloudsync_ltf_inquiry_inserts.is_workflow_created_name,
       #s_crmcloudsync_ltf_inquiry_inserts.left_voice_mail,
       #s_crmcloudsync_ltf_inquiry_inserts.left_voice_mail_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_address_1_city,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_address_1_country,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_address_1_line_1,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_address_1_line_2,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_address_1_postal_code,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_address_1_state_or_province,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_age,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_best_time_to_contact,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_birt_date,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_birth_date,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_birth_year,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_communication_consent,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_communication_consent_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_consent_date_time,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_consent_ip_address,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_consent_text,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_current_member,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_current_member_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_cust_serve_mail,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_dcmp,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_destination_queue,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_duplicate_contact_found,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_duplicate_contact_found_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_duplicate_lead_found,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_duplicate_lead_foundname,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_email_address1,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_email_template,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_employee_number,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_employer,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_exact_target_email_sent,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_exact_target_email_sent_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_first_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_gender_code,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_gender_code_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_group,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_inquiry_source,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_inquiry_type,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_interests,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_keywords,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_landing_page,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_last_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_latitude,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_lead_type,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_longitude,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_membership_info_requested,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_primary_goal,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_referring_contactid_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_referring_contact_id_yomi_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_registration_code,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_request_type,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_telephone1,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_telephone2,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_under_eighteen,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_under_eighteen_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_utm_campaign,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_utm_content,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_utm_medium,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_utm_source,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_utm_term,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_visit_count,
       #s_crmcloudsync_ltf_inquiry_inserts.modified_by_name,
       #s_crmcloudsync_ltf_inquiry_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_inquiry_inserts.modified_on,
       #s_crmcloudsync_ltf_inquiry_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_inquiry_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_inquiry_inserts.optional_attendees,
       #s_crmcloudsync_ltf_inquiry_inserts.organizer,
       #s_crmcloudsync_ltf_inquiry_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_inquiry_inserts.owner_id_name,
       #s_crmcloudsync_ltf_inquiry_inserts.owner_id_type,
       #s_crmcloudsync_ltf_inquiry_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_inquiry_inserts.partners,
       #s_crmcloudsync_ltf_inquiry_inserts.postpone_activity_processing_until,
       #s_crmcloudsync_ltf_inquiry_inserts.priority_code,
       #s_crmcloudsync_ltf_inquiry_inserts.priority_code_name,
       #s_crmcloudsync_ltf_inquiry_inserts.regarding_object_id_name,
       #s_crmcloudsync_ltf_inquiry_inserts.regarding_object_id_yomi_name,
       #s_crmcloudsync_ltf_inquiry_inserts.regarding_object_type_code,
       #s_crmcloudsync_ltf_inquiry_inserts.required_attendees,
       #s_crmcloudsync_ltf_inquiry_inserts.resources,
       #s_crmcloudsync_ltf_inquiry_inserts.scheduled_duration_minutes,
       #s_crmcloudsync_ltf_inquiry_inserts.scheduled_end,
       #s_crmcloudsync_ltf_inquiry_inserts.scheduled_start,
       #s_crmcloudsync_ltf_inquiry_inserts.sender_mail_box_id_name,
       #s_crmcloudsync_ltf_inquiry_inserts.senton,
       #s_crmcloudsync_ltf_inquiry_inserts.service_id_name,
       #s_crmcloudsync_ltf_inquiry_inserts.state_code,
       #s_crmcloudsync_ltf_inquiry_inserts.state_code_name,
       #s_crmcloudsync_ltf_inquiry_inserts.status_code,
       #s_crmcloudsync_ltf_inquiry_inserts.status_code_name,
       #s_crmcloudsync_ltf_inquiry_inserts.subject,
       #s_crmcloudsync_ltf_inquiry_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_inquiry_inserts.[to],
       #s_crmcloudsync_ltf_inquiry_inserts.transaction_currency_id_name,
       #s_crmcloudsync_ltf_inquiry_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_inquiry_inserts.version_number,
       #s_crmcloudsync_ltf_inquiry_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_inquiry_inserts.insert_user,
       #s_crmcloudsync_ltf_inquiry_inserts.updated_date_time,
       #s_crmcloudsync_ltf_inquiry_inserts.update_user,
       #s_crmcloudsync_ltf_inquiry_inserts.community,
       #s_crmcloudsync_ltf_inquiry_inserts.community_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_club_name_name,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_device,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_operating_system,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_referring_domain,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_referring_page,
       #s_crmcloudsync_ltf_inquiry_inserts.traversed_path,
       #s_crmcloudsync_ltf_inquiry_inserts.activity_additional_params,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_utm_audience,
       #s_crmcloudsync_ltf_inquiry_inserts.ltf_utm_image,
       case when s_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id is null then isnull(#s_crmcloudsync_ltf_inquiry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_inquiry_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_inquiry_inserts
  left join p_crmcloudsync_ltf_inquiry
    on #s_crmcloudsync_ltf_inquiry_inserts.bk_hash = p_crmcloudsync_ltf_inquiry.bk_hash
   and p_crmcloudsync_ltf_inquiry.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_inquiry
    on p_crmcloudsync_ltf_inquiry.bk_hash = s_crmcloudsync_ltf_inquiry.bk_hash
   and p_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id = s_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id
 where s_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id is null
    or (s_crmcloudsync_ltf_inquiry.s_crmcloudsync_ltf_inquiry_id is not null
        and s_crmcloudsync_ltf_inquiry.dv_hash <> #s_crmcloudsync_ltf_inquiry_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_inquiry @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_inquiry @current_dv_batch_id

end
