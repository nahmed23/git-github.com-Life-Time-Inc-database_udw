CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_outreach_member_summary] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_ltfoutreachmembersummary

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_ltfoutreachmembersummary (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_activationdate,
       ltf_claimedby,
       ltf_claimedbyname,
       ltf_claimedbyyominame,
       ltf_claimexpiration,
       ltf_clubid,
       ltf_clubidname,
       ltf_contact,
       ltf_contactname,
       ltf_contactyominame,
       ltf_daysincejoin,
       ltf_description,
       ltf_enrolledby,
       ltf_enrolledbyname,
       ltf_enrolledbyyominame,
       ltf_initialcontactbycycle,
       ltf_initialcontactbycyclename,
       ltf_initialcontactbycycleyominame,
       ltf_initialcontactdatecycle,
       ltf_initialcontacttypecycle,
       ltf_initialcontacttypecyclename,
       ltf_interceptattemptscycle,
       ltf_interceptcontactscycle,
       ltf_joindate,
       ltf_juniorcount,
       ltf_lastattemptby,
       ltf_lastattemptbyname,
       ltf_lastattemptbyyominame,
       ltf_lastattemptdate,
       ltf_lastattempttype,
       ltf_lastattempttypename,
       ltf_lastcontactby,
       ltf_lastcontactbyname,
       ltf_lastcontactbyyominame,
       ltf_lastcontactdate,
       ltf_lastcontacttype,
       ltf_lastcontacttypename,
       ltf_ltfemployee,
       ltf_ltfemployeename,
       ltf_lthealth,
       ltf_lthealthname,
       ltf_meetingattemptscycle,
       ltf_meetingcontactscycle,
       ltf_membernumber,
       ltf_membershipproduct,
       ltf_nextanniversary,
       ltf_outreach_member_summaryid,
       ltf_outreachrank,
       ltf_phoneattemptscycle,
       ltf_phonecontactscycle,
       ltf_product,
       ltf_productname,
       ltf_programcyclereference,
       ltf_programcyclereferencename,
       ltf_riskscore,
       ltf_role,
       ltf_rolename,
       ltf_segment,
       ltf_segmentname,
       ltf_starvalue,
       ltf_subscribercreateddate,
       ltf_subscriptioncreateddate,
       ltf_subscriptionid,
       ltf_subscriptionidname,
       ltf_subsegment_1,
       ltf_subsegment_10,
       ltf_subsegment_10name,
       ltf_subsegment_11,
       ltf_subsegment_11name,
       ltf_subsegment_12,
       ltf_subsegment_12name,
       ltf_subsegment_13,
       ltf_subsegment_13name,
       ltf_subsegment_14,
       ltf_subsegment_14name,
       ltf_subsegment_15,
       ltf_subsegment_15name,
       ltf_subsegment_16,
       ltf_subsegment_16name,
       ltf_subsegment_17,
       ltf_subsegment_17name,
       ltf_subsegment_18,
       ltf_subsegment_18name,
       ltf_subsegment_19,
       ltf_subsegment_19name,
       ltf_subsegment_1name,
       ltf_subsegment_2,
       ltf_subsegment_20,
       ltf_subsegment_20name,
       ltf_subsegment_21,
       ltf_subsegment_21name,
       ltf_subsegment_22,
       ltf_subsegment_22name,
       ltf_subsegment_23,
       ltf_subsegment_23name,
       ltf_subsegment_24,
       ltf_subsegment_24name,
       ltf_subsegment_25,
       ltf_subsegment_25name,
       ltf_subsegment_2name,
       ltf_subsegment_3,
       ltf_subsegment_3name,
       ltf_subsegment_4,
       ltf_subsegment_4name,
       ltf_subsegment_5,
       ltf_subsegment_5name,
       ltf_subsegment_6,
       ltf_subsegment_6name,
       ltf_subsegment_7,
       ltf_subsegment_7name,
       ltf_subsegment_8,
       ltf_subsegment_8name,
       ltf_subsegment_9,
       ltf_subsegment_9name,
       ltf_talkingpoints,
       ltf_targeted,
       ltf_targetedname,
       ltf_totalattemptscycle,
       ltf_totalcontactscycle,
       ltf_yearsofmembership,
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
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_outreach_member_summaryid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_activationdate,
       ltf_claimedby,
       ltf_claimedbyname,
       ltf_claimedbyyominame,
       ltf_claimexpiration,
       ltf_clubid,
       ltf_clubidname,
       ltf_contact,
       ltf_contactname,
       ltf_contactyominame,
       ltf_daysincejoin,
       ltf_description,
       ltf_enrolledby,
       ltf_enrolledbyname,
       ltf_enrolledbyyominame,
       ltf_initialcontactbycycle,
       ltf_initialcontactbycyclename,
       ltf_initialcontactbycycleyominame,
       ltf_initialcontactdatecycle,
       ltf_initialcontacttypecycle,
       ltf_initialcontacttypecyclename,
       ltf_interceptattemptscycle,
       ltf_interceptcontactscycle,
       ltf_joindate,
       ltf_juniorcount,
       ltf_lastattemptby,
       ltf_lastattemptbyname,
       ltf_lastattemptbyyominame,
       ltf_lastattemptdate,
       ltf_lastattempttype,
       ltf_lastattempttypename,
       ltf_lastcontactby,
       ltf_lastcontactbyname,
       ltf_lastcontactbyyominame,
       ltf_lastcontactdate,
       ltf_lastcontacttype,
       ltf_lastcontacttypename,
       ltf_ltfemployee,
       ltf_ltfemployeename,
       ltf_lthealth,
       ltf_lthealthname,
       ltf_meetingattemptscycle,
       ltf_meetingcontactscycle,
       ltf_membernumber,
       ltf_membershipproduct,
       ltf_nextanniversary,
       ltf_outreach_member_summaryid,
       ltf_outreachrank,
       ltf_phoneattemptscycle,
       ltf_phonecontactscycle,
       ltf_product,
       ltf_productname,
       ltf_programcyclereference,
       ltf_programcyclereferencename,
       ltf_riskscore,
       ltf_role,
       ltf_rolename,
       ltf_segment,
       ltf_segmentname,
       ltf_starvalue,
       ltf_subscribercreateddate,
       ltf_subscriptioncreateddate,
       ltf_subscriptionid,
       ltf_subscriptionidname,
       ltf_subsegment_1,
       ltf_subsegment_10,
       ltf_subsegment_10name,
       ltf_subsegment_11,
       ltf_subsegment_11name,
       ltf_subsegment_12,
       ltf_subsegment_12name,
       ltf_subsegment_13,
       ltf_subsegment_13name,
       ltf_subsegment_14,
       ltf_subsegment_14name,
       ltf_subsegment_15,
       ltf_subsegment_15name,
       ltf_subsegment_16,
       ltf_subsegment_16name,
       ltf_subsegment_17,
       ltf_subsegment_17name,
       ltf_subsegment_18,
       ltf_subsegment_18name,
       ltf_subsegment_19,
       ltf_subsegment_19name,
       ltf_subsegment_1name,
       ltf_subsegment_2,
       ltf_subsegment_20,
       ltf_subsegment_20name,
       ltf_subsegment_21,
       ltf_subsegment_21name,
       ltf_subsegment_22,
       ltf_subsegment_22name,
       ltf_subsegment_23,
       ltf_subsegment_23name,
       ltf_subsegment_24,
       ltf_subsegment_24name,
       ltf_subsegment_25,
       ltf_subsegment_25name,
       ltf_subsegment_2name,
       ltf_subsegment_3,
       ltf_subsegment_3name,
       ltf_subsegment_4,
       ltf_subsegment_4name,
       ltf_subsegment_5,
       ltf_subsegment_5name,
       ltf_subsegment_6,
       ltf_subsegment_6name,
       ltf_subsegment_7,
       ltf_subsegment_7name,
       ltf_subsegment_8,
       ltf_subsegment_8name,
       ltf_subsegment_9,
       ltf_subsegment_9name,
       ltf_talkingpoints,
       ltf_targeted,
       ltf_targetedname,
       ltf_totalattemptscycle,
       ltf_totalcontactscycle,
       ltf_yearsofmembership,
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
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       isnull(cast(stage_crmcloudsync_ltfoutreachmembersummary.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_ltfoutreachmembersummary
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_outreach_member_summary @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_outreach_member_summary (
       bk_hash,
       ltf_outreach_member_summary_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_ltfoutreachmembersummary.bk_hash,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_outreach_member_summaryid ltf_outreach_member_summary_id,
       isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_ltfoutreachmembersummary
  left join h_crmcloudsync_ltf_outreach_member_summary
    on stage_hash_crmcloudsync_ltfoutreachmembersummary.bk_hash = h_crmcloudsync_ltf_outreach_member_summary.bk_hash
 where h_crmcloudsync_ltf_outreach_member_summary_id is null
   and stage_hash_crmcloudsync_ltfoutreachmembersummary.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_outreach_member_summary
if object_id('tempdb..#l_crmcloudsync_ltf_outreach_member_summary_inserts') is not null drop table #l_crmcloudsync_ltf_outreach_member_summary_inserts
create table #l_crmcloudsync_ltf_outreach_member_summary_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_ltfoutreachmembersummary.bk_hash,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.createdby created_by,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_claimedby ltf_claimed_by,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_contact ltf_contact,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_enrolledby ltf_enrolled_by,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontactbycycle ltf_initial_contact_by_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattemptby ltf_last_attempt_by,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontactby ltf_last_contact_by,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_outreach_member_summaryid ltf_outreach_member_summary_id,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_product ltf_product,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_programcyclereference ltf_program_cycle_reference,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subscriptionid ltf_subscription_id,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedby modified_by,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ownerid owner_id,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.owningteam owning_team,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.owninguser owning_user,
       isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_claimedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_contact,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_enrolledby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontactbycycle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattemptby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontactby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_outreach_member_summaryid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_product,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_programcyclereference,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subscriptionid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.owninguser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_ltfoutreachmembersummary
 where stage_hash_crmcloudsync_ltfoutreachmembersummary.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_outreach_member_summary records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_outreach_member_summary (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_claimed_by,
       ltf_club_id,
       ltf_contact,
       ltf_enrolled_by,
       ltf_initial_contact_by_cycle,
       ltf_last_attempt_by,
       ltf_last_contact_by,
       ltf_outreach_member_summary_id,
       ltf_product,
       ltf_program_cycle_reference,
       ltf_subscription_id,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_outreach_member_summary_inserts.bk_hash,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.created_by,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_claimed_by,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_club_id,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_contact,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_enrolled_by,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_initial_contact_by_cycle,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_attempt_by,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_contact_by,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_outreach_member_summary_id,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_product,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_program_cycle_reference,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subscription_id,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.modified_by,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.owner_id,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.owning_team,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.owning_user,
       case when l_crmcloudsync_ltf_outreach_member_summary.l_crmcloudsync_ltf_outreach_member_summary_id is null then isnull(#l_crmcloudsync_ltf_outreach_member_summary_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_outreach_member_summary_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_outreach_member_summary_inserts
  left join p_crmcloudsync_ltf_outreach_member_summary
    on #l_crmcloudsync_ltf_outreach_member_summary_inserts.bk_hash = p_crmcloudsync_ltf_outreach_member_summary.bk_hash
   and p_crmcloudsync_ltf_outreach_member_summary.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_outreach_member_summary
    on p_crmcloudsync_ltf_outreach_member_summary.bk_hash = l_crmcloudsync_ltf_outreach_member_summary.bk_hash
   and p_crmcloudsync_ltf_outreach_member_summary.l_crmcloudsync_ltf_outreach_member_summary_id = l_crmcloudsync_ltf_outreach_member_summary.l_crmcloudsync_ltf_outreach_member_summary_id
 where l_crmcloudsync_ltf_outreach_member_summary.l_crmcloudsync_ltf_outreach_member_summary_id is null
    or (l_crmcloudsync_ltf_outreach_member_summary.l_crmcloudsync_ltf_outreach_member_summary_id is not null
        and l_crmcloudsync_ltf_outreach_member_summary.dv_hash <> #l_crmcloudsync_ltf_outreach_member_summary_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_outreach_member_summary
if object_id('tempdb..#s_crmcloudsync_ltf_outreach_member_summary_inserts') is not null drop table #s_crmcloudsync_ltf_outreach_member_summary_inserts
create table #s_crmcloudsync_ltf_outreach_member_summary_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_ltfoutreachmembersummary.bk_hash,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.createdbyname created_by_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.createdon created_on,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_activationdate ltf_activation_date,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_claimedbyname ltf_claimed_by_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_claimedbyyominame ltf_claimed_by_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_claimexpiration ltf_claim_expiration,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_clubidname ltf_club_id_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_contactname ltf_contact_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_contactyominame ltf_contact_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_daysincejoin ltf_day_since_join,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_description ltf_description,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_enrolledbyname ltf_enrolled_by_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_enrolledbyyominame ltf_enrolled_by_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontactbycyclename ltf_initial_contact_by_cycle_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontactbycycleyominame ltf_initial_contact_by_cycle_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontactdatecycle ltf_initial_contact_date_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontacttypecycle ltf_initial_contact_type_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontacttypecyclename ltf_initial_contact_type_cycle_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_interceptattemptscycle ltf_intercept_attempts_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_interceptcontactscycle ltf_intercept_contacts_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_joindate ltf_join_date,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_juniorcount ltf_junior_count,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattemptbyname ltf_last_attempt_by_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattemptbyyominame ltf_last_attempt_by_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattemptdate ltf_last_attempt_date,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattempttype ltf_last_attempt_type,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattempttypename ltf_last_attempt_type_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontactbyname ltf_last_contact_by_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontactbyyominame ltf_last_contact_by_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontactdate ltf_last_contact_date,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontacttype ltf_last_contact_type,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontacttypename ltf_last_contact_type_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_ltfemployee ltf_ltf_employee,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_ltfemployeename ltf_ltf_employee_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lthealth ltf_lthealth,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lthealthname ltf_lthealth_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_meetingattemptscycle ltf_meeting_attempts_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_meetingcontactscycle ltf_meeting_contacts_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_membernumber ltf_member_number,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_membershipproduct ltf_membership_product,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_nextanniversary ltf_next_anniversary,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_outreach_member_summaryid ltf_outreach_member_summary_id,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_outreachrank ltf_outreach_rank,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_phoneattemptscycle ltf_phone_attempts_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_phonecontactscycle ltf_phone_contacts_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_productname ltf_product_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_programcyclereferencename ltf_program_cycle_reference_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_riskscore ltf_risk_score,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_role ltf_role,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_rolename ltf_role_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_segment ltf_segment,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_segmentname ltf_segment_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_starvalue ltf_star_value,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subscribercreateddate ltf_subscriber_created_date,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subscriptioncreateddate ltf_subscription_created_date,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subscriptionidname ltf_subscription_id_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_1 ltf_subsegment_1,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_10 ltf_subsegment_10,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_10name ltf_subsegment_10_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_11 ltf_subsegment_11,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_11name ltf_subsegment_11_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_12 ltf_subsegment_12,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_12name ltf_subsegment_12_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_13 ltf_subsegment_13,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_13name ltf_subsegment_13_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_14 ltf_subsegment_14,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_14name ltf_subsegment_14_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_15 ltf_subsegment_15,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_15name ltf_subsegment_15_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_16 ltf_subsegment_16,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_16name ltf_subsegment_16_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_17 ltf_subsegment_17,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_17name ltf_subsegment_17_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_18 ltf_subsegment_18,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_18name ltf_subsegment_18_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_19 ltf_subsegment_19,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_19name ltf_subsegment_19_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_1name ltf_subsegment_1_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_2 ltf_subsegment_2,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_20 ltf_subsegment_20,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_20name ltf_subsegment_20_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_21 ltf_subsegment_21,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_21name ltf_subsegment_21_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_22 ltf_subsegment_22,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_22name ltf_subsegment_22_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_23 ltf_subsegment_23,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_23name ltf_subsegment_23_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_24 ltf_subsegment_24,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_24name ltf_subsegment_24_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_25 ltf_subsegment_25,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_25name ltf_subsegment_25_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_2name ltf_subsegment_2_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_3 ltf_subsegment_3,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_3name ltf_subsegment_3_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_4 ltf_subsegment_4,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_4name ltf_subsegment_4_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_5 ltf_subsegment_5,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_5name ltf_subsegment_5_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_6 ltf_subsegment_6,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_6name ltf_subsegment_6_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_7 ltf_subsegment_7,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_7name ltf_subsegment_7_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_8 ltf_subsegment_8,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_8name ltf_subsegment_8_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_9 ltf_subsegment_9,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_9name ltf_subsegment_9_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_talkingpoints ltf_talking_points,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_targeted ltf_targeted,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_targetedname ltf_targeted_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_totalattemptscycle ltf_total_attempts_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_totalcontactscycle ltf_total_contacts_cycle,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_yearsofmembership ltf_years_of_membership,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedon modified_on,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.owneridname owner_id_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.owneridtype owner_id_type,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.statecode state_code,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.statecodename state_code_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.statuscode status_code,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.statuscodename status_code_name,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.versionnumber version_number,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.InsertUser insert_user,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_ltfoutreachmembersummary.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_activationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_claimedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_claimedbyyominame,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_claimexpiration,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_clubidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_contactname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_contactyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_daysincejoin as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_enrolledbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_enrolledbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontactbycyclename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontactbycycleyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontactdatecycle,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontacttypecycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_initialcontacttypecyclename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_interceptattemptscycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_interceptcontactscycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_joindate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_juniorcount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattemptbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattemptbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattemptdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattempttype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastattempttypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontactbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontactbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontactdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontacttype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lastcontacttypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_ltfemployee as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_ltfemployeename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lthealth as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_lthealthname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_meetingattemptscycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_meetingcontactscycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_membernumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_membershipproduct,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_nextanniversary,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_outreach_member_summaryid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_outreachrank as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_phoneattemptscycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_phonecontactscycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_productname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_programcyclereferencename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_riskscore as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_role as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_rolename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_segment as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_segmentname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_starvalue as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subscribercreateddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subscriptioncreateddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subscriptionidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_1 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_10 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_10name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_11 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_11name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_12 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_12name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_13 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_13name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_14 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_14name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_15 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_15name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_16 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_16name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_17 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_17name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_18 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_18name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_19 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_19name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_1name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_2 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_20 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_20name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_21 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_21name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_22 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_22name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_23 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_23name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_24 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_24name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_25 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_25name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_2name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_3 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_3name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_4 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_4name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_5 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_5name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_6 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_6name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_7 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_7name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_8 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_8name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_9 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_subsegment_9name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_talkingpoints,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_targeted as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_targetedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_totalattemptscycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_totalcontactscycle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.ltf_yearsofmembership as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_ltfoutreachmembersummary.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_ltfoutreachmembersummary.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_ltfoutreachmembersummary.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_ltfoutreachmembersummary
 where stage_hash_crmcloudsync_ltfoutreachmembersummary.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_outreach_member_summary records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_outreach_member_summary (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_activation_date,
       ltf_claimed_by_name,
       ltf_claimed_by_yomi_name,
       ltf_claim_expiration,
       ltf_club_id_name,
       ltf_contact_name,
       ltf_contact_yomi_name,
       ltf_day_since_join,
       ltf_description,
       ltf_enrolled_by_name,
       ltf_enrolled_by_yomi_name,
       ltf_initial_contact_by_cycle_name,
       ltf_initial_contact_by_cycle_yomi_name,
       ltf_initial_contact_date_cycle,
       ltf_initial_contact_type_cycle,
       ltf_initial_contact_type_cycle_name,
       ltf_intercept_attempts_cycle,
       ltf_intercept_contacts_cycle,
       ltf_join_date,
       ltf_junior_count,
       ltf_last_attempt_by_name,
       ltf_last_attempt_by_yomi_name,
       ltf_last_attempt_date,
       ltf_last_attempt_type,
       ltf_last_attempt_type_name,
       ltf_last_contact_by_name,
       ltf_last_contact_by_yomi_name,
       ltf_last_contact_date,
       ltf_last_contact_type,
       ltf_last_contact_type_name,
       ltf_ltf_employee,
       ltf_ltf_employee_name,
       ltf_lthealth,
       ltf_lthealth_name,
       ltf_meeting_attempts_cycle,
       ltf_meeting_contacts_cycle,
       ltf_member_number,
       ltf_membership_product,
       ltf_next_anniversary,
       ltf_outreach_member_summary_id,
       ltf_outreach_rank,
       ltf_phone_attempts_cycle,
       ltf_phone_contacts_cycle,
       ltf_product_name,
       ltf_program_cycle_reference_name,
       ltf_risk_score,
       ltf_role,
       ltf_role_name,
       ltf_segment,
       ltf_segment_name,
       ltf_star_value,
       ltf_subscriber_created_date,
       ltf_subscription_created_date,
       ltf_subscription_id_name,
       ltf_subsegment_1,
       ltf_subsegment_10,
       ltf_subsegment_10_name,
       ltf_subsegment_11,
       ltf_subsegment_11_name,
       ltf_subsegment_12,
       ltf_subsegment_12_name,
       ltf_subsegment_13,
       ltf_subsegment_13_name,
       ltf_subsegment_14,
       ltf_subsegment_14_name,
       ltf_subsegment_15,
       ltf_subsegment_15_name,
       ltf_subsegment_16,
       ltf_subsegment_16_name,
       ltf_subsegment_17,
       ltf_subsegment_17_name,
       ltf_subsegment_18,
       ltf_subsegment_18_name,
       ltf_subsegment_19,
       ltf_subsegment_19_name,
       ltf_subsegment_1_name,
       ltf_subsegment_2,
       ltf_subsegment_20,
       ltf_subsegment_20_name,
       ltf_subsegment_21,
       ltf_subsegment_21_name,
       ltf_subsegment_22,
       ltf_subsegment_22_name,
       ltf_subsegment_23,
       ltf_subsegment_23_name,
       ltf_subsegment_24,
       ltf_subsegment_24_name,
       ltf_subsegment_25,
       ltf_subsegment_25_name,
       ltf_subsegment_2_name,
       ltf_subsegment_3,
       ltf_subsegment_3_name,
       ltf_subsegment_4,
       ltf_subsegment_4_name,
       ltf_subsegment_5,
       ltf_subsegment_5_name,
       ltf_subsegment_6,
       ltf_subsegment_6_name,
       ltf_subsegment_7,
       ltf_subsegment_7_name,
       ltf_subsegment_8,
       ltf_subsegment_8_name,
       ltf_subsegment_9,
       ltf_subsegment_9_name,
       ltf_talking_points,
       ltf_targeted,
       ltf_targeted_name,
       ltf_total_attempts_cycle,
       ltf_total_contacts_cycle,
       ltf_years_of_membership,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       overridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       time_zone_rule_version_number,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_outreach_member_summary_inserts.bk_hash,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.created_by_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.created_on,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_activation_date,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_claimed_by_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_claimed_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_claim_expiration,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_club_id_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_contact_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_contact_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_day_since_join,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_description,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_enrolled_by_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_enrolled_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_initial_contact_by_cycle_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_initial_contact_by_cycle_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_initial_contact_date_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_initial_contact_type_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_initial_contact_type_cycle_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_intercept_attempts_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_intercept_contacts_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_join_date,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_junior_count,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_attempt_by_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_attempt_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_attempt_date,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_attempt_type,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_attempt_type_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_contact_by_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_contact_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_contact_date,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_contact_type,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_last_contact_type_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_ltf_employee,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_ltf_employee_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_lthealth,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_lthealth_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_meeting_attempts_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_meeting_contacts_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_member_number,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_membership_product,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_next_anniversary,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_outreach_member_summary_id,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_outreach_rank,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_phone_attempts_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_phone_contacts_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_product_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_program_cycle_reference_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_risk_score,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_role,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_role_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_segment,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_segment_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_star_value,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subscriber_created_date,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subscription_created_date,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subscription_id_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_1,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_10,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_10_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_11,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_11_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_12,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_12_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_13,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_13_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_14,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_14_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_15,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_15_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_16,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_16_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_17,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_17_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_18,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_18_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_19,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_19_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_1_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_2,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_20,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_20_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_21,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_21_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_22,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_22_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_23,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_23_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_24,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_24_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_25,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_25_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_2_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_3,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_3_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_4,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_4_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_5,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_5_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_6,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_6_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_7,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_7_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_8,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_8_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_9,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_subsegment_9_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_talking_points,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_targeted,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_targeted_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_total_attempts_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_total_contacts_cycle,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.ltf_years_of_membership,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.modified_by_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.modified_on,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.owner_id_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.owner_id_type,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.state_code,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.state_code_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.status_code,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.status_code_name,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.version_number,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.insert_user,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.updated_date_time,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.update_user,
       case when s_crmcloudsync_ltf_outreach_member_summary.s_crmcloudsync_ltf_outreach_member_summary_id is null then isnull(#s_crmcloudsync_ltf_outreach_member_summary_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_outreach_member_summary_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_outreach_member_summary_inserts
  left join p_crmcloudsync_ltf_outreach_member_summary
    on #s_crmcloudsync_ltf_outreach_member_summary_inserts.bk_hash = p_crmcloudsync_ltf_outreach_member_summary.bk_hash
   and p_crmcloudsync_ltf_outreach_member_summary.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_outreach_member_summary
    on p_crmcloudsync_ltf_outreach_member_summary.bk_hash = s_crmcloudsync_ltf_outreach_member_summary.bk_hash
   and p_crmcloudsync_ltf_outreach_member_summary.s_crmcloudsync_ltf_outreach_member_summary_id = s_crmcloudsync_ltf_outreach_member_summary.s_crmcloudsync_ltf_outreach_member_summary_id
 where s_crmcloudsync_ltf_outreach_member_summary.s_crmcloudsync_ltf_outreach_member_summary_id is null
    or (s_crmcloudsync_ltf_outreach_member_summary.s_crmcloudsync_ltf_outreach_member_summary_id is not null
        and s_crmcloudsync_ltf_outreach_member_summary.dv_hash <> #s_crmcloudsync_ltf_outreach_member_summary_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_outreach_member_summary @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_outreach_member_summary @current_dv_batch_id

end
