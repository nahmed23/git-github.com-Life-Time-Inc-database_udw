CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_guest_visit] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_GuestVisit

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_GuestVisit (
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
       ltf_address1_addresstypecode,
       ltf_address1_addresstypecodename,
       ltf_address1_city,
       ltf_address1_country,
       ltf_address1_county,
       ltf_address1_line1,
       ltf_address1_line2,
       ltf_address1_line3,
       ltf_address1_postalcode,
       ltf_address1_postofficebox,
       ltf_address1_stateorprovince,
       ltf_businessunitguestvisitid,
       ltf_businessunitguestvisitidname,
       ltf_clubid,
       ltf_clubidname,
       ltf_dateofbirth,
       ltf_emailaddress1,
       ltf_employer,
       ltf_firstname,
       ltf_gender,
       ltf_gendername,
       ltf_guesttype,
       ltf_guesttypename,
       ltf_lastname,
       ltf_middlename,
       ltf_mobilephone,
       ltf_pager,
       ltf_referredby,
       ltf_referredbyname,
       ltf_referredbyyominame,
       ltf_telephone1,
       ltf_telephone2,
       ltf_telephone3,
       ltf_websiteurl,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       new_clubname,
       new_clubnamename,
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
       activityadditionalparams,
       community,
       communityname,
       ltf_assignedmea,
       ltf_assignedmeaname,
       ltf_assignedmeayominame,
       traversedpath,
       ltf_appointmentid,
       ltf_appointmentidname,
       ltf_clubcloseto,
       ltf_clubclosetoname,
       ltf_Interests,
       ltf_leadid,
       ltf_leadidname,
       ltf_matchingcontactcount,
       ltf_matchingleadcount,
       ltf_membershipinterest,
       ltf_membershipinterestname,
       ltf_online,
       ltf_onlinename,
       ltf_outofarea,
       ltf_outofareaname,
       ltf_partyid,
       ltf_referralsource,
       ltf_referralsourcename,
       ltf_referringmemberid,
       ltf_requestdate,
       ltf_requestid,
       ltf_sameday,
       ltf_samedayname,
       ltf_agreementsignature,
       ltf_campaigninstance,
       ltf_campaigninstancename,
       ltf_deductguestpriv,
       ltf_qrcodeused,
       ltf_source,
       ltf_timeout,
       ltf_timeoutservice,
       ltf_prospectid,
       ltf_mostrecentcasl,
       ltf_sendid,
       ltf_referringcorpacctid,
       ltf_gracevisit,
       ltf_lineofbusiness,
       ltf_lineofbusinessname,
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
       ltf_address1_addresstypecode,
       ltf_address1_addresstypecodename,
       ltf_address1_city,
       ltf_address1_country,
       ltf_address1_county,
       ltf_address1_line1,
       ltf_address1_line2,
       ltf_address1_line3,
       ltf_address1_postalcode,
       ltf_address1_postofficebox,
       ltf_address1_stateorprovince,
       ltf_businessunitguestvisitid,
       ltf_businessunitguestvisitidname,
       ltf_clubid,
       ltf_clubidname,
       ltf_dateofbirth,
       ltf_emailaddress1,
       ltf_employer,
       ltf_firstname,
       ltf_gender,
       ltf_gendername,
       ltf_guesttype,
       ltf_guesttypename,
       ltf_lastname,
       ltf_middlename,
       ltf_mobilephone,
       ltf_pager,
       ltf_referredby,
       ltf_referredbyname,
       ltf_referredbyyominame,
       ltf_telephone1,
       ltf_telephone2,
       ltf_telephone3,
       ltf_websiteurl,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       new_clubname,
       new_clubnamename,
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
       activityadditionalparams,
       community,
       communityname,
       ltf_assignedmea,
       ltf_assignedmeaname,
       ltf_assignedmeayominame,
       traversedpath,
       ltf_appointmentid,
       ltf_appointmentidname,
       ltf_clubcloseto,
       ltf_clubclosetoname,
       ltf_Interests,
       ltf_leadid,
       ltf_leadidname,
       ltf_matchingcontactcount,
       ltf_matchingleadcount,
       ltf_membershipinterest,
       ltf_membershipinterestname,
       ltf_online,
       ltf_onlinename,
       ltf_outofarea,
       ltf_outofareaname,
       ltf_partyid,
       ltf_referralsource,
       ltf_referralsourcename,
       ltf_referringmemberid,
       ltf_requestdate,
       ltf_requestid,
       ltf_sameday,
       ltf_samedayname,
       ltf_agreementsignature,
       ltf_campaigninstance,
       ltf_campaigninstancename,
       ltf_deductguestpriv,
       ltf_qrcodeused,
       ltf_source,
       ltf_timeout,
       ltf_timeoutservice,
       ltf_prospectid,
       ltf_mostrecentcasl,
       ltf_sendid,
       ltf_referringcorpacctid,
       ltf_gracevisit,
       ltf_lineofbusiness,
       ltf_lineofbusinessname,
       isnull(cast(stage_crmcloudsync_LTF_GuestVisit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_GuestVisit
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_guest_visit @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_guest_visit (
       bk_hash,
       activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_GuestVisit.bk_hash,
       stage_hash_crmcloudsync_LTF_GuestVisit.activityid activity_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_GuestVisit
  left join h_crmcloudsync_ltf_guest_visit
    on stage_hash_crmcloudsync_LTF_GuestVisit.bk_hash = h_crmcloudsync_ltf_guest_visit.bk_hash
 where h_crmcloudsync_ltf_guest_visit_id is null
   and stage_hash_crmcloudsync_LTF_GuestVisit.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_guest_visit
if object_id('tempdb..#l_crmcloudsync_ltf_guest_visit_inserts') is not null drop table #l_crmcloudsync_ltf_guest_visit_inserts
create table #l_crmcloudsync_ltf_guest_visit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_GuestVisit.bk_hash,
       stage_hash_crmcloudsync_LTF_GuestVisit.activityid activity_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.createdby created_by,
       stage_hash_crmcloudsync_LTF_GuestVisit.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_businessunitguestvisitid ltf_business_unit_guest_visit_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referredby ltf_referred_by,
       stage_hash_crmcloudsync_LTF_GuestVisit.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_GuestVisit.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_GuestVisit.new_clubname new_club_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ownerid owner_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_LTF_GuestVisit.owningteam owning_team,
       stage_hash_crmcloudsync_LTF_GuestVisit.owninguser owning_user,
       stage_hash_crmcloudsync_LTF_GuestVisit.processid process_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.regardingobjectid regarding_object_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.sendermailboxid sender_mail_box_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.seriesid series_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.serviceid service_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.stageid stage_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.transactioncurrencyid transaction_currency_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_assignedmea ltf_assigned_mea,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_appointmentid ltf_appointment_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_leadid ltf_lead_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_partyid ltf_party_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referringmemberid ltf_referring_member_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_requestid ltf_request_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_campaigninstance ltf_campaign_instance,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_prospectid ltf_prospect_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_sendid ltf_send_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referringcorpacctid ltf_referring_corpacct_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_businessunitguestvisitid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referredby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.new_clubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.regardingobjectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.sendermailboxid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.seriesid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.serviceid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.transactioncurrencyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_assignedmea,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_appointmentid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_leadid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_partyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referringmemberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_requestid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_campaigninstance,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_prospectid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_sendid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referringcorpacctid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_GuestVisit
 where stage_hash_crmcloudsync_LTF_GuestVisit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_guest_visit records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_guest_visit (
       bk_hash,
       activity_id,
       created_by,
       created_on_behalf_by,
       ltf_business_unit_guest_visit_id,
       ltf_club_id,
       ltf_referred_by,
       modified_by,
       modified_on_behalf_by,
       new_club_name,
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
       ltf_assigned_mea,
       ltf_appointment_id,
       ltf_lead_id,
       ltf_party_id,
       ltf_referring_member_id,
       ltf_request_id,
       ltf_campaign_instance,
       ltf_prospect_id,
       ltf_send_id,
       ltf_referring_corpacct_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_guest_visit_inserts.bk_hash,
       #l_crmcloudsync_ltf_guest_visit_inserts.activity_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.created_by,
       #l_crmcloudsync_ltf_guest_visit_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_business_unit_guest_visit_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_club_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_referred_by,
       #l_crmcloudsync_ltf_guest_visit_inserts.modified_by,
       #l_crmcloudsync_ltf_guest_visit_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_guest_visit_inserts.new_club_name,
       #l_crmcloudsync_ltf_guest_visit_inserts.owner_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_guest_visit_inserts.owning_team,
       #l_crmcloudsync_ltf_guest_visit_inserts.owning_user,
       #l_crmcloudsync_ltf_guest_visit_inserts.process_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.regarding_object_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.sender_mail_box_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.series_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.service_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.stage_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.transaction_currency_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_assigned_mea,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_appointment_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_lead_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_party_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_referring_member_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_request_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_campaign_instance,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_prospect_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_send_id,
       #l_crmcloudsync_ltf_guest_visit_inserts.ltf_referring_corpacct_id,
       case when l_crmcloudsync_ltf_guest_visit.l_crmcloudsync_ltf_guest_visit_id is null then isnull(#l_crmcloudsync_ltf_guest_visit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_guest_visit_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_guest_visit_inserts
  left join p_crmcloudsync_ltf_guest_visit
    on #l_crmcloudsync_ltf_guest_visit_inserts.bk_hash = p_crmcloudsync_ltf_guest_visit.bk_hash
   and p_crmcloudsync_ltf_guest_visit.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_guest_visit
    on p_crmcloudsync_ltf_guest_visit.bk_hash = l_crmcloudsync_ltf_guest_visit.bk_hash
   and p_crmcloudsync_ltf_guest_visit.l_crmcloudsync_ltf_guest_visit_id = l_crmcloudsync_ltf_guest_visit.l_crmcloudsync_ltf_guest_visit_id
 where l_crmcloudsync_ltf_guest_visit.l_crmcloudsync_ltf_guest_visit_id is null
    or (l_crmcloudsync_ltf_guest_visit.l_crmcloudsync_ltf_guest_visit_id is not null
        and l_crmcloudsync_ltf_guest_visit.dv_hash <> #l_crmcloudsync_ltf_guest_visit_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_guest_visit
if object_id('tempdb..#s_crmcloudsync_ltf_guest_visit_inserts') is not null drop table #s_crmcloudsync_ltf_guest_visit_inserts
create table #s_crmcloudsync_ltf_guest_visit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_GuestVisit.bk_hash,
       stage_hash_crmcloudsync_LTF_GuestVisit.activityid activity_id,
       stage_hash_crmcloudsync_LTF_GuestVisit.activitytypecode activity_type_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.activitytypecodename activity_type_code_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.actualdurationminutes actual_duration_minutes,
       stage_hash_crmcloudsync_LTF_GuestVisit.actualend actual_end,
       stage_hash_crmcloudsync_LTF_GuestVisit.actualstart actual_start,
       stage_hash_crmcloudsync_LTF_GuestVisit.bcc bcc,
       stage_hash_crmcloudsync_LTF_GuestVisit.cc cc,
       stage_hash_crmcloudsync_LTF_GuestVisit.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.createdon created_on,
       stage_hash_crmcloudsync_LTF_GuestVisit.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.customers customers,
       stage_hash_crmcloudsync_LTF_GuestVisit.deliverylastattemptedon delivery_last_attempted_on,
       stage_hash_crmcloudsync_LTF_GuestVisit.deliveryprioritycode delivery_priority_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.deliveryprioritycodename delivery_priority_code_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.description description,
       stage_hash_crmcloudsync_LTF_GuestVisit.exchangerate exchange_rate,
       stage_hash_crmcloudsync_LTF_GuestVisit.[from] [from],
       stage_hash_crmcloudsync_LTF_GuestVisit.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_GuestVisit.instancetypecode instance_type_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.instancetypecodename instance_type_code_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.isbilled is_billed,
       stage_hash_crmcloudsync_LTF_GuestVisit.isbilledname is_billed_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ismapiprivate is_mapi_private,
       stage_hash_crmcloudsync_LTF_GuestVisit.ismapiprivatename is_mapi_private_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.isregularactivity is_regular_activity,
       stage_hash_crmcloudsync_LTF_GuestVisit.isregularactivityname is_regular_activity_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.isworkflowcreated is_workflow_created,
       stage_hash_crmcloudsync_LTF_GuestVisit.isworkflowcreatedname is_workflow_created_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.leftvoicemail left_voice_mail,
       stage_hash_crmcloudsync_LTF_GuestVisit.leftvoicemailname left_voice_mail_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_addresstypecode ltf_address_1_address_type_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_addresstypecodename ltf_address_1_address_type_code_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_city ltf_address_1_city,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_country ltf_address_1_country,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_county ltf_address_1_county,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_line1 ltf_address_1_line_1,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_line2 ltf_address_1_line_2,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_line3 ltf_address_1_line_3,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_postalcode ltf_address_1_postal_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_postofficebox ltf_address_1_post_office_box,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_stateorprovince ltf_address_1_state_or_province,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_businessunitguestvisitidname ltf_business_unit_guest_visit_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_clubidname ltf_club_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_dateofbirth ltf_date_of_birth,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_emailaddress1 ltf_email_address_1,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_employer ltf_employer,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_firstname ltf_first_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_gender ltf_gender,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_gendername ltf_gender_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_guesttype ltf_guest_type,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_guesttypename ltf_guest_type_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_lastname ltf_last_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_middlename ltf_middle_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_mobilephone ltf_mobile_phone,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_pager ltf_pager,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referredbyname ltf_referred_by_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referredbyyominame ltf_referred_by_yomi_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_telephone1 ltf_telephone1,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_telephone2 ltf_telephone2,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_telephone3 ltf_telephone3,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_websiteurl ltf_websiteurl,
       stage_hash_crmcloudsync_LTF_GuestVisit.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_GuestVisit.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.new_clubnamename new_club_name_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.optionalattendees optional_attendees,
       stage_hash_crmcloudsync_LTF_GuestVisit.organizer organizer,
       stage_hash_crmcloudsync_LTF_GuestVisit.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_GuestVisit.owneridname owner_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.owneridtype owner_id_type,
       stage_hash_crmcloudsync_LTF_GuestVisit.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.partners partners,
       stage_hash_crmcloudsync_LTF_GuestVisit.postponeactivityprocessinguntil postpone_activity_processing_until,
       stage_hash_crmcloudsync_LTF_GuestVisit.prioritycode priority_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.prioritycodename priority_code_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.regardingobjectidname regarding_object_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.regardingobjectidyominame regarding_object_id_yomi_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.regardingobjecttypecode regarding_object_type_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.requiredattendees required_attendees,
       stage_hash_crmcloudsync_LTF_GuestVisit.resources resources,
       stage_hash_crmcloudsync_LTF_GuestVisit.scheduleddurationminutes scheduled_duration_minutes,
       stage_hash_crmcloudsync_LTF_GuestVisit.scheduledend scheduled_end,
       stage_hash_crmcloudsync_LTF_GuestVisit.scheduledstart scheduled_start,
       stage_hash_crmcloudsync_LTF_GuestVisit.sendermailboxidname sender_mail_box_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.senton sent_on,
       stage_hash_crmcloudsync_LTF_GuestVisit.serviceidname service_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.statecode state_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.statuscode status_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.subject subject,
       stage_hash_crmcloudsync_LTF_GuestVisit.timezoneruleversionnumber timezone_rule_version_number,
       stage_hash_crmcloudsync_LTF_GuestVisit.[to] [to],
       stage_hash_crmcloudsync_LTF_GuestVisit.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.utcconversiontimezonecode utc_conversion_timezone_code,
       stage_hash_crmcloudsync_LTF_GuestVisit.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_GuestVisit.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_GuestVisit.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_GuestVisit.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_GuestVisit.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_GuestVisit.activityadditionalparams activity_additional_params,
       stage_hash_crmcloudsync_LTF_GuestVisit.community community,
       stage_hash_crmcloudsync_LTF_GuestVisit.communityname community_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_assignedmeaname ltf_assigned_mea_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_assignedmeayominame ltf_assigned_mea_yomi_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.traversedpath traversed_path,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_appointmentidname ltf_appointment_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_clubcloseto ltf_club_close_to,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_clubclosetoname ltf_club_close_to_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_Interests ltf_interests,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_leadidname ltf_lead_id_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_matchingcontactcount ltf_matching_contact_count,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_matchingleadcount ltf_matching_lead_count,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_membershipinterest ltf_membership_interest,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_membershipinterestname ltf_membership_interest_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_online ltf_online,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_onlinename ltf_online_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_outofarea ltf_out_of_area,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_outofareaname ltf_out_of_area_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referralsource ltf_referral_source,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referralsourcename ltf_referral_source_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_requestdate ltf_request_date,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_sameday ltf_same_day,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_samedayname ltf_same_day_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_agreementsignature ltf_agreement_signature,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_campaigninstancename ltf_campaign_instance_name,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_deductguestpriv ltf_deduct_guest_priv,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_qrcodeused ltf_qr_code_used,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_source ltf_source,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_timeout ltf_timeout,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_timeoutservice ltf_timeout_service,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_mostrecentcasl ltf_most_recent_casl,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_gracevisit ltf_grace_visit,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_lineofbusiness ltf_line_of_business,
       stage_hash_crmcloudsync_LTF_GuestVisit.ltf_lineofbusinessname ltf_line_of_business_name,
       isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.activityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.activitytypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.activitytypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.actualdurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.actualend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.actualstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.bcc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.cc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.customers,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.deliverylastattemptedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.deliveryprioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.deliveryprioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.[from],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.instancetypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.instancetypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.isbilled as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.isbilledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ismapiprivate as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ismapiprivatename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.isregularactivity as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.isregularactivityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.isworkflowcreated as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.isworkflowcreatedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.leftvoicemail as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.leftvoicemailname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_addresstypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_addresstypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_line3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_postofficebox,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_address1_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_businessunitguestvisitidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_clubidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.ltf_dateofbirth,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_emailaddress1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_employer,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_gender as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_gendername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_guesttype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_guesttypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_middlename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_mobilephone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_pager,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referredbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referredbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_telephone1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_telephone2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_telephone3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_websiteurl,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.new_clubnamename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.optionalattendees,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.organizer,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.partners,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.postponeactivityprocessinguntil,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.prioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.prioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.regardingobjectidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.regardingobjectidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.regardingobjecttypecode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.requiredattendees,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.resources,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.scheduleddurationminutes as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.scheduledend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.scheduledstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.sendermailboxidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.senton,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.serviceidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.subject,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.[to],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.activityadditionalparams,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.community as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.communityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_assignedmeaname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_assignedmeayominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.traversedpath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_appointmentidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_clubcloseto as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_clubclosetoname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_Interests,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_leadidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_matchingcontactcount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_matchingleadcount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_membershipinterest as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_membershipinterestname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_online as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_onlinename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_outofarea as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_outofareaname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referralsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_referralsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.ltf_requestdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_sameday as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_samedayname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_agreementsignature,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_campaigninstancename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_deductguestpriv as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_qrcodeused as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_source,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_timeout as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_timeoutservice,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_GuestVisit.ltf_mostrecentcasl,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_gracevisit as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_lineofbusiness as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_GuestVisit.ltf_lineofbusinessname,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_GuestVisit
 where stage_hash_crmcloudsync_LTF_GuestVisit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_guest_visit records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_guest_visit (
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
       ltf_address_1_address_type_code,
       ltf_address_1_address_type_code_name,
       ltf_address_1_city,
       ltf_address_1_country,
       ltf_address_1_county,
       ltf_address_1_line_1,
       ltf_address_1_line_2,
       ltf_address_1_line_3,
       ltf_address_1_postal_code,
       ltf_address_1_post_office_box,
       ltf_address_1_state_or_province,
       ltf_business_unit_guest_visit_id_name,
       ltf_club_id_name,
       ltf_date_of_birth,
       ltf_email_address_1,
       ltf_employer,
       ltf_first_name,
       ltf_gender,
       ltf_gender_name,
       ltf_guest_type,
       ltf_guest_type_name,
       ltf_last_name,
       ltf_middle_name,
       ltf_mobile_phone,
       ltf_pager,
       ltf_referred_by_name,
       ltf_referred_by_yomi_name,
       ltf_telephone1,
       ltf_telephone2,
       ltf_telephone3,
       ltf_websiteurl,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       new_club_name_name,
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
       sent_on,
       service_id_name,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       subject,
       timezone_rule_version_number,
       [to],
       transaction_currency_id_name,
       utc_conversion_timezone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       activity_additional_params,
       community,
       community_name,
       ltf_assigned_mea_name,
       ltf_assigned_mea_yomi_name,
       traversed_path,
       ltf_appointment_id_name,
       ltf_club_close_to,
       ltf_club_close_to_name,
       ltf_interests,
       ltf_lead_id_name,
       ltf_matching_contact_count,
       ltf_matching_lead_count,
       ltf_membership_interest,
       ltf_membership_interest_name,
       ltf_online,
       ltf_online_name,
       ltf_out_of_area,
       ltf_out_of_area_name,
       ltf_referral_source,
       ltf_referral_source_name,
       ltf_request_date,
       ltf_same_day,
       ltf_same_day_name,
       ltf_agreement_signature,
       ltf_campaign_instance_name,
       ltf_deduct_guest_priv,
       ltf_qr_code_used,
       ltf_source,
       ltf_timeout,
       ltf_timeout_service,
       ltf_most_recent_casl,
       ltf_grace_visit,
       ltf_line_of_business,
       ltf_line_of_business_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_guest_visit_inserts.bk_hash,
       #s_crmcloudsync_ltf_guest_visit_inserts.activity_id,
       #s_crmcloudsync_ltf_guest_visit_inserts.activity_type_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.activity_type_code_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.actual_duration_minutes,
       #s_crmcloudsync_ltf_guest_visit_inserts.actual_end,
       #s_crmcloudsync_ltf_guest_visit_inserts.actual_start,
       #s_crmcloudsync_ltf_guest_visit_inserts.bcc,
       #s_crmcloudsync_ltf_guest_visit_inserts.cc,
       #s_crmcloudsync_ltf_guest_visit_inserts.created_by_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.created_on,
       #s_crmcloudsync_ltf_guest_visit_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.customers,
       #s_crmcloudsync_ltf_guest_visit_inserts.delivery_last_attempted_on,
       #s_crmcloudsync_ltf_guest_visit_inserts.delivery_priority_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.delivery_priority_code_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.description,
       #s_crmcloudsync_ltf_guest_visit_inserts.exchange_rate,
       #s_crmcloudsync_ltf_guest_visit_inserts.[from],
       #s_crmcloudsync_ltf_guest_visit_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_guest_visit_inserts.instance_type_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.instance_type_code_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.is_billed,
       #s_crmcloudsync_ltf_guest_visit_inserts.is_billed_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.is_mapi_private,
       #s_crmcloudsync_ltf_guest_visit_inserts.is_mapi_private_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.is_regular_activity,
       #s_crmcloudsync_ltf_guest_visit_inserts.is_regular_activity_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.is_workflow_created,
       #s_crmcloudsync_ltf_guest_visit_inserts.is_workflow_created_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.left_voice_mail,
       #s_crmcloudsync_ltf_guest_visit_inserts.left_voice_mail_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_address_type_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_address_type_code_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_city,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_country,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_county,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_line_1,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_line_2,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_line_3,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_postal_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_post_office_box,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_address_1_state_or_province,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_business_unit_guest_visit_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_club_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_date_of_birth,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_email_address_1,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_employer,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_first_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_gender,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_gender_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_guest_type,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_guest_type_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_last_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_middle_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_mobile_phone,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_pager,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_referred_by_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_referred_by_yomi_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_telephone1,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_telephone2,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_telephone3,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_websiteurl,
       #s_crmcloudsync_ltf_guest_visit_inserts.modified_by_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.modified_on,
       #s_crmcloudsync_ltf_guest_visit_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.new_club_name_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.optional_attendees,
       #s_crmcloudsync_ltf_guest_visit_inserts.organizer,
       #s_crmcloudsync_ltf_guest_visit_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_guest_visit_inserts.owner_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.owner_id_type,
       #s_crmcloudsync_ltf_guest_visit_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.partners,
       #s_crmcloudsync_ltf_guest_visit_inserts.postpone_activity_processing_until,
       #s_crmcloudsync_ltf_guest_visit_inserts.priority_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.priority_code_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.regarding_object_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.regarding_object_id_yomi_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.regarding_object_type_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.required_attendees,
       #s_crmcloudsync_ltf_guest_visit_inserts.resources,
       #s_crmcloudsync_ltf_guest_visit_inserts.scheduled_duration_minutes,
       #s_crmcloudsync_ltf_guest_visit_inserts.scheduled_end,
       #s_crmcloudsync_ltf_guest_visit_inserts.scheduled_start,
       #s_crmcloudsync_ltf_guest_visit_inserts.sender_mail_box_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.sent_on,
       #s_crmcloudsync_ltf_guest_visit_inserts.service_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.state_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.state_code_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.status_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.status_code_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.subject,
       #s_crmcloudsync_ltf_guest_visit_inserts.timezone_rule_version_number,
       #s_crmcloudsync_ltf_guest_visit_inserts.[to],
       #s_crmcloudsync_ltf_guest_visit_inserts.transaction_currency_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.utc_conversion_timezone_code,
       #s_crmcloudsync_ltf_guest_visit_inserts.version_number,
       #s_crmcloudsync_ltf_guest_visit_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_guest_visit_inserts.insert_user,
       #s_crmcloudsync_ltf_guest_visit_inserts.updated_date_time,
       #s_crmcloudsync_ltf_guest_visit_inserts.update_user,
       #s_crmcloudsync_ltf_guest_visit_inserts.activity_additional_params,
       #s_crmcloudsync_ltf_guest_visit_inserts.community,
       #s_crmcloudsync_ltf_guest_visit_inserts.community_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_assigned_mea_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_assigned_mea_yomi_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.traversed_path,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_appointment_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_club_close_to,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_club_close_to_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_interests,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_lead_id_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_matching_contact_count,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_matching_lead_count,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_membership_interest,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_membership_interest_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_online,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_online_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_out_of_area,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_out_of_area_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_referral_source,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_referral_source_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_request_date,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_same_day,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_same_day_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_agreement_signature,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_campaign_instance_name,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_deduct_guest_priv,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_qr_code_used,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_source,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_timeout,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_timeout_service,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_most_recent_casl,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_grace_visit,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_line_of_business,
       #s_crmcloudsync_ltf_guest_visit_inserts.ltf_line_of_business_name,
       case when s_crmcloudsync_ltf_guest_visit.s_crmcloudsync_ltf_guest_visit_id is null then isnull(#s_crmcloudsync_ltf_guest_visit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_guest_visit_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_guest_visit_inserts
  left join p_crmcloudsync_ltf_guest_visit
    on #s_crmcloudsync_ltf_guest_visit_inserts.bk_hash = p_crmcloudsync_ltf_guest_visit.bk_hash
   and p_crmcloudsync_ltf_guest_visit.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_guest_visit
    on p_crmcloudsync_ltf_guest_visit.bk_hash = s_crmcloudsync_ltf_guest_visit.bk_hash
   and p_crmcloudsync_ltf_guest_visit.s_crmcloudsync_ltf_guest_visit_id = s_crmcloudsync_ltf_guest_visit.s_crmcloudsync_ltf_guest_visit_id
 where s_crmcloudsync_ltf_guest_visit.s_crmcloudsync_ltf_guest_visit_id is null
    or (s_crmcloudsync_ltf_guest_visit.s_crmcloudsync_ltf_guest_visit_id is not null
        and s_crmcloudsync_ltf_guest_visit.dv_hash <> #s_crmcloudsync_ltf_guest_visit_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_guest_visit @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_guest_visit @current_dv_batch_id

end
