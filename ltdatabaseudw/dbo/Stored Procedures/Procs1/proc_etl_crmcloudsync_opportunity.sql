CREATE PROC [dbo].[proc_etl_crmcloudsync_opportunity] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_Opportunity

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_Opportunity (
       bk_hash,
       accountid,
       accountidname,
       accountidyominame,
       actualclosedate,
       actualvalue,
       actualvalue_base,
       budgetamount,
       budgetamount_base,
       budgetstatus,
       budgettypename,
       campaignid,
       campaignidname,
       captureproposalfeedback,
       captureproposalfeedbackname,
       closeprobability,
       completefinalproposal,
       completefinalproposalname,
       completeinternalreview,
       completeinternalreviewname,
       confirminterest,
       confirminterestname,
       contactid,
       contactidname,
       contactidyominame,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       currentsituation,
       customerid,
       customeridname,
       customeridtype,
       customeridyominame,
       customerneed,
       customerpainpoints,
       decisionmaker,
       decisionmakername,
       description,
       developproposal,
       developproposalname,
       discountamount,
       discountamount_base,
       discountpercentage,
       estimatedclosedate,
       estimatedvalue,
       estimatedvalue_base,
       evaluatefit,
       evaluatefitname,
       exchangerate,
       filedebrief,
       filedebriefname,
       finaldecisiondate,
       freightamount,
       freightamount_base,
       identifycompetitors,
       identifycompetitorsname,
       identifycustomercontacts,
       identifycustomercontactsname,
       identifypursuitteam,
       identifypursuitteamname,
       importsequencenumber,
       initialcommunication,
       initialcommunicationname,
       isprivatename,
       isrevenuesystemcalculated,
       isrevenuesystemcalculatedname,
       ltf_besttimetocontact,
       ltf_besttimetocontactname,
       ltf_clubid,
       ltf_clubidname,
       ltf_clubproximity,
       ltf_clubproximityname,
       ltf_commitmentlevel,
       ltf_commitmentlevelname,
       ltf_commitmentreason,
       ltf_employerwellnessprogram,
       ltf_employerwellnessprogramname,
       ltf_exercisehistory,
       ltf_exercisehistoryname,
       ltf_imsjoinlink,
       ltf_injuriesorlimitations,
       ltf_injuriesorlimitationsdescription,
       ltf_injuriesorlimitationsname,
       ltf_inquirytype,
       ltf_lastactivity,
       ltf_leadsource,
       ltf_leadsourcename,
       ltf_leadtype,
       ltf_leadtypename,
       ltf_manageduntil,
       ltf_measurablegoal,
       ltf_measurablegoalid,
       ltf_measurablegoalidname,
       ltf_measurablegoalname,
       ltf_membershipinforequested,
       ltf_membershiplevel,
       ltf_membershiplevelname,
       ltf_membershiptype,
       ltf_membershiptypename,
       ltf_nugget,
       ltf_park,
       ltf_parkcomments,
       ltf_parkname,
       ltf_parkreason,
       ltf_parkreasonname,
       ltf_parkuntil,
       ltf_pasttrainerorcoach,
       ltf_pasttrainerorcoachname,
       ltf_primaryobjective,
       ltf_primaryobjectiveid,
       ltf_primaryobjectiveidname,
       ltf_primaryobjectivename,
       ltf_promocode,
       ltf_prospecttype,
       ltf_prospecttypename,
       ltf_referringcontactid,
       ltf_referringcontactidname,
       ltf_referringcontactidyominame,
       ltf_referringmemberid,
       ltf_registrationcode,
       ltf_requesttype,
       ltf_specificgoal,
       ltf_specificgoalid,
       ltf_specificgoalidname,
       ltf_specificgoalname,
       ltf_timegoal,
       ltf_todaysaction,
       ltf_todaysactionname,
       ltf_trainerorcoachpreference,
       ltf_trainerorcoachpreferencename,
       ltf_webteamid,
       ltf_webteamidname,
       ltf_webteamidyominame,
       ltf_workoutpreference,
       ltf_workoutpreferencename,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       name,
       need,
       needname,
       opportunityid,
       opportunityratingcode,
       opportunityratingcodename,
       originatingleadid,
       originatingleadidname,
       originatingleadidyominame,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       parentaccountid,
       parentaccountidname,
       parentaccountidyominame,
       parentcontactid,
       parentcontactidname,
       parentcontactidyominame,
       participatesinworkflow,
       participatesinworkflowname,
       presentfinalproposal,
       presentfinalproposalname,
       presentproposal,
       presentproposalname,
       pricelevelid,
       pricelevelidname,
       pricingerrorcode,
       pricingerrorcodename,
       prioritycode,
       prioritycodename,
       processid,
       proposedsolution,
       purchaseprocess,
       purchaseprocessname,
       purchasetimeframe,
       purchasetimeframename,
       pursuitdecision,
       pursuitdecisionname,
       qualificationcomments,
       quotecomments,
       resolvefeedback,
       resolvefeedbackname,
       salesstage,
       salesstagecode,
       salesstagecodename,
       salesstagename,
       schedulefollowup_prospect,
       schedulefollowup_qualify,
       scheduleproposalmeeting,
       sendthankyounote,
       sendthankyounotename,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       stepid,
       stepname,
       timeline,
       timelinename,
       timezoneruleversionnumber,
       totalamount,
       totalamount_base,
       totalamountlessfreight,
       totalamountlessfreight_base,
       totaldiscountamount,
       totaldiscountamount_base,
       totallineitemamount,
       totallineitemamount_base,
       totallineitemdiscountamount,
       totallineitemdiscountamount_base,
       totaltax,
       totaltax_base,
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_profilenotes,
       ltf_Programsofinterest,
       ltf_Programsofinterestname,
       ltf_visitorid,
       ltf_wanttodo,
       ltf_wanttodoname,
       ltf_whywanttodo,
       ltf_whywanttodoname,
       ltf_whometwith,
       ltf_recommendedmembership,
       ltf_recommendedmembershipname,
       ltf_resistance,
       ltf_resistancename,
       traversedpath,
       ltf_webtransfermethod,
       ltf_webtransfermethodname,
       ltf_basketball,
       ltf_climbing,
       ltf_commitment,
       ltf_cycling,
       ltf_guestpassexpirationdate,
       ltf_kidsclub,
       ltf_kidsclubname,
       ltf_nextfollowup,
       ltf_personaltraining,
       ltf_personaltrainingname,
       ltf_readytojoin,
       ltf_readytojoinname,
       ltf_signature,
       ltf_swimming,
       ltf_swimmingname,
       ltf_tennislessons,
       ltf_tennislessonsname,
       ltf_yoga,
       ltf_assignedbyapp,
       ltf_assignedbyappname,
       ltf_assignedtoclubdate,
       ltf_imsjoinsenddate,
       ltf_isimsjoin,
       ltf_isimsjoinname,
       ltf_numberover14list,
       ltf_numberover14listname,
       ltf_numberunder14list,
       ltf_numberunder14listname,
       ltf_promoquoted,
       ltf_selectedinterestids,
       ltf_originatingguestvisit,
       ltf_originatingguestvisitname,
       ltf_assignmentrequestdate,
       ltf_assignmentrequestid,
       ltf_lineofbusiness,
       ltf_channel,
       ltf_lineofbusinessname,
       ltf_channelname,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(opportunityid,'z#@$k%&P'))),2) bk_hash,
       accountid,
       accountidname,
       accountidyominame,
       actualclosedate,
       actualvalue,
       actualvalue_base,
       budgetamount,
       budgetamount_base,
       budgetstatus,
       budgettypename,
       campaignid,
       campaignidname,
       captureproposalfeedback,
       captureproposalfeedbackname,
       closeprobability,
       completefinalproposal,
       completefinalproposalname,
       completeinternalreview,
       completeinternalreviewname,
       confirminterest,
       confirminterestname,
       contactid,
       contactidname,
       contactidyominame,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       currentsituation,
       customerid,
       customeridname,
       customeridtype,
       customeridyominame,
       customerneed,
       customerpainpoints,
       decisionmaker,
       decisionmakername,
       description,
       developproposal,
       developproposalname,
       discountamount,
       discountamount_base,
       discountpercentage,
       estimatedclosedate,
       estimatedvalue,
       estimatedvalue_base,
       evaluatefit,
       evaluatefitname,
       exchangerate,
       filedebrief,
       filedebriefname,
       finaldecisiondate,
       freightamount,
       freightamount_base,
       identifycompetitors,
       identifycompetitorsname,
       identifycustomercontacts,
       identifycustomercontactsname,
       identifypursuitteam,
       identifypursuitteamname,
       importsequencenumber,
       initialcommunication,
       initialcommunicationname,
       isprivatename,
       isrevenuesystemcalculated,
       isrevenuesystemcalculatedname,
       ltf_besttimetocontact,
       ltf_besttimetocontactname,
       ltf_clubid,
       ltf_clubidname,
       ltf_clubproximity,
       ltf_clubproximityname,
       ltf_commitmentlevel,
       ltf_commitmentlevelname,
       ltf_commitmentreason,
       ltf_employerwellnessprogram,
       ltf_employerwellnessprogramname,
       ltf_exercisehistory,
       ltf_exercisehistoryname,
       ltf_imsjoinlink,
       ltf_injuriesorlimitations,
       ltf_injuriesorlimitationsdescription,
       ltf_injuriesorlimitationsname,
       ltf_inquirytype,
       ltf_lastactivity,
       ltf_leadsource,
       ltf_leadsourcename,
       ltf_leadtype,
       ltf_leadtypename,
       ltf_manageduntil,
       ltf_measurablegoal,
       ltf_measurablegoalid,
       ltf_measurablegoalidname,
       ltf_measurablegoalname,
       ltf_membershipinforequested,
       ltf_membershiplevel,
       ltf_membershiplevelname,
       ltf_membershiptype,
       ltf_membershiptypename,
       ltf_nugget,
       ltf_park,
       ltf_parkcomments,
       ltf_parkname,
       ltf_parkreason,
       ltf_parkreasonname,
       ltf_parkuntil,
       ltf_pasttrainerorcoach,
       ltf_pasttrainerorcoachname,
       ltf_primaryobjective,
       ltf_primaryobjectiveid,
       ltf_primaryobjectiveidname,
       ltf_primaryobjectivename,
       ltf_promocode,
       ltf_prospecttype,
       ltf_prospecttypename,
       ltf_referringcontactid,
       ltf_referringcontactidname,
       ltf_referringcontactidyominame,
       ltf_referringmemberid,
       ltf_registrationcode,
       ltf_requesttype,
       ltf_specificgoal,
       ltf_specificgoalid,
       ltf_specificgoalidname,
       ltf_specificgoalname,
       ltf_timegoal,
       ltf_todaysaction,
       ltf_todaysactionname,
       ltf_trainerorcoachpreference,
       ltf_trainerorcoachpreferencename,
       ltf_webteamid,
       ltf_webteamidname,
       ltf_webteamidyominame,
       ltf_workoutpreference,
       ltf_workoutpreferencename,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       name,
       need,
       needname,
       opportunityid,
       opportunityratingcode,
       opportunityratingcodename,
       originatingleadid,
       originatingleadidname,
       originatingleadidyominame,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       parentaccountid,
       parentaccountidname,
       parentaccountidyominame,
       parentcontactid,
       parentcontactidname,
       parentcontactidyominame,
       participatesinworkflow,
       participatesinworkflowname,
       presentfinalproposal,
       presentfinalproposalname,
       presentproposal,
       presentproposalname,
       pricelevelid,
       pricelevelidname,
       pricingerrorcode,
       pricingerrorcodename,
       prioritycode,
       prioritycodename,
       processid,
       proposedsolution,
       purchaseprocess,
       purchaseprocessname,
       purchasetimeframe,
       purchasetimeframename,
       pursuitdecision,
       pursuitdecisionname,
       qualificationcomments,
       quotecomments,
       resolvefeedback,
       resolvefeedbackname,
       salesstage,
       salesstagecode,
       salesstagecodename,
       salesstagename,
       schedulefollowup_prospect,
       schedulefollowup_qualify,
       scheduleproposalmeeting,
       sendthankyounote,
       sendthankyounotename,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       stepid,
       stepname,
       timeline,
       timelinename,
       timezoneruleversionnumber,
       totalamount,
       totalamount_base,
       totalamountlessfreight,
       totalamountlessfreight_base,
       totaldiscountamount,
       totaldiscountamount_base,
       totallineitemamount,
       totallineitemamount_base,
       totallineitemdiscountamount,
       totallineitemdiscountamount_base,
       totaltax,
       totaltax_base,
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_profilenotes,
       ltf_Programsofinterest,
       ltf_Programsofinterestname,
       ltf_visitorid,
       ltf_wanttodo,
       ltf_wanttodoname,
       ltf_whywanttodo,
       ltf_whywanttodoname,
       ltf_whometwith,
       ltf_recommendedmembership,
       ltf_recommendedmembershipname,
       ltf_resistance,
       ltf_resistancename,
       traversedpath,
       ltf_webtransfermethod,
       ltf_webtransfermethodname,
       ltf_basketball,
       ltf_climbing,
       ltf_commitment,
       ltf_cycling,
       ltf_guestpassexpirationdate,
       ltf_kidsclub,
       ltf_kidsclubname,
       ltf_nextfollowup,
       ltf_personaltraining,
       ltf_personaltrainingname,
       ltf_readytojoin,
       ltf_readytojoinname,
       ltf_signature,
       ltf_swimming,
       ltf_swimmingname,
       ltf_tennislessons,
       ltf_tennislessonsname,
       ltf_yoga,
       ltf_assignedbyapp,
       ltf_assignedbyappname,
       ltf_assignedtoclubdate,
       ltf_imsjoinsenddate,
       ltf_isimsjoin,
       ltf_isimsjoinname,
       ltf_numberover14list,
       ltf_numberover14listname,
       ltf_numberunder14list,
       ltf_numberunder14listname,
       ltf_promoquoted,
       ltf_selectedinterestids,
       ltf_originatingguestvisit,
       ltf_originatingguestvisitname,
       ltf_assignmentrequestdate,
       ltf_assignmentrequestid,
       ltf_lineofbusiness,
       ltf_channel,
       ltf_lineofbusinessname,
       ltf_channelname,
       isnull(cast(stage_crmcloudsync_Opportunity.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_Opportunity
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_opportunity @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_opportunity (
       bk_hash,
       opportunity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_Opportunity.bk_hash,
       stage_hash_crmcloudsync_Opportunity.opportunityid opportunity_id,
       isnull(cast(stage_hash_crmcloudsync_Opportunity.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_Opportunity
  left join h_crmcloudsync_opportunity
    on stage_hash_crmcloudsync_Opportunity.bk_hash = h_crmcloudsync_opportunity.bk_hash
 where h_crmcloudsync_opportunity_id is null
   and stage_hash_crmcloudsync_Opportunity.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_opportunity
if object_id('tempdb..#l_crmcloudsync_opportunity_inserts') is not null drop table #l_crmcloudsync_opportunity_inserts
create table #l_crmcloudsync_opportunity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Opportunity.bk_hash,
       stage_hash_crmcloudsync_Opportunity.accountid account_id,
       stage_hash_crmcloudsync_Opportunity.campaignid campaign_id,
       stage_hash_crmcloudsync_Opportunity.contactid contact_id,
       stage_hash_crmcloudsync_Opportunity.createdby created_by,
       stage_hash_crmcloudsync_Opportunity.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_Opportunity.customerid customer_id,
       stage_hash_crmcloudsync_Opportunity.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_Opportunity.ltf_measurablegoalid ltf_measurable_goal_id,
       stage_hash_crmcloudsync_Opportunity.ltf_primaryobjectiveid ltf_primary_objective_id,
       stage_hash_crmcloudsync_Opportunity.ltf_referringcontactid ltf_referring_contact_id,
       stage_hash_crmcloudsync_Opportunity.ltf_referringmemberid ltf_referring_member_id,
       stage_hash_crmcloudsync_Opportunity.ltf_specificgoalid ltf_specific_goal_id,
       stage_hash_crmcloudsync_Opportunity.ltf_webteamid ltf_web_team_id,
       stage_hash_crmcloudsync_Opportunity.modifiedby modified_by,
       stage_hash_crmcloudsync_Opportunity.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_Opportunity.opportunityid opportunity_id,
       stage_hash_crmcloudsync_Opportunity.originatingleadid originating_lead_id,
       stage_hash_crmcloudsync_Opportunity.ownerid owner_id,
       stage_hash_crmcloudsync_Opportunity.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_Opportunity.owningteam owning_team,
       stage_hash_crmcloudsync_Opportunity.owninguser owning_user,
       stage_hash_crmcloudsync_Opportunity.parentaccountid parent_account_id,
       stage_hash_crmcloudsync_Opportunity.parentcontactid parent_contact_id,
       stage_hash_crmcloudsync_Opportunity.pricelevelid price_level_id,
       stage_hash_crmcloudsync_Opportunity.processid process_id,
       stage_hash_crmcloudsync_Opportunity.stageid stage_id,
       stage_hash_crmcloudsync_Opportunity.stepid step_id,
       stage_hash_crmcloudsync_Opportunity.transactioncurrencyid transaction_currency_id,
       stage_hash_crmcloudsync_Opportunity.ltf_visitorid ltf_visitor_id,
       stage_hash_crmcloudsync_Opportunity.ltf_originatingguestvisit ltf_originating_guest_visit,
       isnull(cast(stage_hash_crmcloudsync_Opportunity.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.accountid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.campaignid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.contactid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.customerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_measurablegoalid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_primaryobjectiveid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_referringcontactid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_referringmemberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_specificgoalid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_webteamid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.opportunityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.originatingleadid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.parentaccountid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.parentcontactid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.pricelevelid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.stepid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.transactioncurrencyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_visitorid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_originatingguestvisit,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Opportunity
 where stage_hash_crmcloudsync_Opportunity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_opportunity records
set @insert_date_time = getdate()
insert into l_crmcloudsync_opportunity (
       bk_hash,
       account_id,
       campaign_id,
       contact_id,
       created_by,
       created_on_behalf_by,
       customer_id,
       ltf_club_id,
       ltf_measurable_goal_id,
       ltf_primary_objective_id,
       ltf_referring_contact_id,
       ltf_referring_member_id,
       ltf_specific_goal_id,
       ltf_web_team_id,
       modified_by,
       modified_on_behalf_by,
       opportunity_id,
       originating_lead_id,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       parent_account_id,
       parent_contact_id,
       price_level_id,
       process_id,
       stage_id,
       step_id,
       transaction_currency_id,
       ltf_visitor_id,
       ltf_originating_guest_visit,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_opportunity_inserts.bk_hash,
       #l_crmcloudsync_opportunity_inserts.account_id,
       #l_crmcloudsync_opportunity_inserts.campaign_id,
       #l_crmcloudsync_opportunity_inserts.contact_id,
       #l_crmcloudsync_opportunity_inserts.created_by,
       #l_crmcloudsync_opportunity_inserts.created_on_behalf_by,
       #l_crmcloudsync_opportunity_inserts.customer_id,
       #l_crmcloudsync_opportunity_inserts.ltf_club_id,
       #l_crmcloudsync_opportunity_inserts.ltf_measurable_goal_id,
       #l_crmcloudsync_opportunity_inserts.ltf_primary_objective_id,
       #l_crmcloudsync_opportunity_inserts.ltf_referring_contact_id,
       #l_crmcloudsync_opportunity_inserts.ltf_referring_member_id,
       #l_crmcloudsync_opportunity_inserts.ltf_specific_goal_id,
       #l_crmcloudsync_opportunity_inserts.ltf_web_team_id,
       #l_crmcloudsync_opportunity_inserts.modified_by,
       #l_crmcloudsync_opportunity_inserts.modified_on_behalf_by,
       #l_crmcloudsync_opportunity_inserts.opportunity_id,
       #l_crmcloudsync_opportunity_inserts.originating_lead_id,
       #l_crmcloudsync_opportunity_inserts.owner_id,
       #l_crmcloudsync_opportunity_inserts.owning_business_unit,
       #l_crmcloudsync_opportunity_inserts.owning_team,
       #l_crmcloudsync_opportunity_inserts.owning_user,
       #l_crmcloudsync_opportunity_inserts.parent_account_id,
       #l_crmcloudsync_opportunity_inserts.parent_contact_id,
       #l_crmcloudsync_opportunity_inserts.price_level_id,
       #l_crmcloudsync_opportunity_inserts.process_id,
       #l_crmcloudsync_opportunity_inserts.stage_id,
       #l_crmcloudsync_opportunity_inserts.step_id,
       #l_crmcloudsync_opportunity_inserts.transaction_currency_id,
       #l_crmcloudsync_opportunity_inserts.ltf_visitor_id,
       #l_crmcloudsync_opportunity_inserts.ltf_originating_guest_visit,
       case when l_crmcloudsync_opportunity.l_crmcloudsync_opportunity_id is null then isnull(#l_crmcloudsync_opportunity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_opportunity_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_opportunity_inserts
  left join p_crmcloudsync_opportunity
    on #l_crmcloudsync_opportunity_inserts.bk_hash = p_crmcloudsync_opportunity.bk_hash
   and p_crmcloudsync_opportunity.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_opportunity
    on p_crmcloudsync_opportunity.bk_hash = l_crmcloudsync_opportunity.bk_hash
   and p_crmcloudsync_opportunity.l_crmcloudsync_opportunity_id = l_crmcloudsync_opportunity.l_crmcloudsync_opportunity_id
 where l_crmcloudsync_opportunity.l_crmcloudsync_opportunity_id is null
    or (l_crmcloudsync_opportunity.l_crmcloudsync_opportunity_id is not null
        and l_crmcloudsync_opportunity.dv_hash <> #l_crmcloudsync_opportunity_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_opportunity
if object_id('tempdb..#s_crmcloudsync_opportunity_inserts') is not null drop table #s_crmcloudsync_opportunity_inserts
create table #s_crmcloudsync_opportunity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_Opportunity.bk_hash,
       stage_hash_crmcloudsync_Opportunity.accountidname account_id_name,
       stage_hash_crmcloudsync_Opportunity.accountidyominame account_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.actualclosedate actual_close_date,
       stage_hash_crmcloudsync_Opportunity.actualvalue actual_value,
       stage_hash_crmcloudsync_Opportunity.actualvalue_base actual_value_base,
       stage_hash_crmcloudsync_Opportunity.budgetamount budget_amount,
       stage_hash_crmcloudsync_Opportunity.budgetamount_base budget_amount_base,
       stage_hash_crmcloudsync_Opportunity.budgetstatus budget_status,
       stage_hash_crmcloudsync_Opportunity.budgettypename budget_type_name,
       stage_hash_crmcloudsync_Opportunity.campaignidname campaign_id_name,
       stage_hash_crmcloudsync_Opportunity.captureproposalfeedback capture_proposal_feedback,
       stage_hash_crmcloudsync_Opportunity.captureproposalfeedbackname capture_proposal_feedback_name,
       stage_hash_crmcloudsync_Opportunity.closeprobability close_probability,
       stage_hash_crmcloudsync_Opportunity.completefinalproposal complete_final_proposal,
       stage_hash_crmcloudsync_Opportunity.completefinalproposalname complete_final_proposal_name,
       stage_hash_crmcloudsync_Opportunity.completeinternalreview complete_internal_review,
       stage_hash_crmcloudsync_Opportunity.completeinternalreviewname complete_internal_review_name,
       stage_hash_crmcloudsync_Opportunity.confirminterest confirm_interest,
       stage_hash_crmcloudsync_Opportunity.confirminterestname confirm_interest_name,
       stage_hash_crmcloudsync_Opportunity.contactidname contact_id_name,
       stage_hash_crmcloudsync_Opportunity.contactidyominame contact_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.createdbyname created_by_name,
       stage_hash_crmcloudsync_Opportunity.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_Opportunity.createdon created_on,
       stage_hash_crmcloudsync_Opportunity.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_Opportunity.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Opportunity.currentsituation current_situation,
       stage_hash_crmcloudsync_Opportunity.customeridname customer_id_name,
       stage_hash_crmcloudsync_Opportunity.customeridtype customer_id_type,
       stage_hash_crmcloudsync_Opportunity.customeridyominame customer_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.customerneed customer_need,
       stage_hash_crmcloudsync_Opportunity.customerpainpoints customer_pain_points,
       stage_hash_crmcloudsync_Opportunity.decisionmaker decision_maker,
       stage_hash_crmcloudsync_Opportunity.decisionmakername decision_maker_name,
       stage_hash_crmcloudsync_Opportunity.description description,
       stage_hash_crmcloudsync_Opportunity.developproposal develop_proposal,
       stage_hash_crmcloudsync_Opportunity.developproposalname develop_proposal_name,
       stage_hash_crmcloudsync_Opportunity.discountamount discount_amount,
       stage_hash_crmcloudsync_Opportunity.discountamount_base discount_amount_base,
       stage_hash_crmcloudsync_Opportunity.discountpercentage discount_percentage,
       stage_hash_crmcloudsync_Opportunity.estimatedclosedate estimated_close_date,
       stage_hash_crmcloudsync_Opportunity.estimatedvalue estimated_value,
       stage_hash_crmcloudsync_Opportunity.estimatedvalue_base estimated_value_base,
       stage_hash_crmcloudsync_Opportunity.evaluatefit evaluate_fit,
       stage_hash_crmcloudsync_Opportunity.evaluatefitname evaluate_fit_name,
       stage_hash_crmcloudsync_Opportunity.exchangerate exchange_rate,
       stage_hash_crmcloudsync_Opportunity.filedebrief filede_brief,
       stage_hash_crmcloudsync_Opportunity.filedebriefname filede_brief_name,
       stage_hash_crmcloudsync_Opportunity.finaldecisiondate final_decision_date,
       stage_hash_crmcloudsync_Opportunity.freightamount freight_amount,
       stage_hash_crmcloudsync_Opportunity.freightamount_base freight_amount_base,
       stage_hash_crmcloudsync_Opportunity.identifycompetitors identify_competitors,
       stage_hash_crmcloudsync_Opportunity.identifycompetitorsname identify_competitors_name,
       stage_hash_crmcloudsync_Opportunity.identifycustomercontacts identify_customer_contacts,
       stage_hash_crmcloudsync_Opportunity.identifycustomercontactsname identify_customer_contacts_name,
       stage_hash_crmcloudsync_Opportunity.identifypursuitteam identify_pursuit_team,
       stage_hash_crmcloudsync_Opportunity.identifypursuitteamname identify_pursuit_team_name,
       stage_hash_crmcloudsync_Opportunity.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_Opportunity.initialcommunication initial_communication,
       stage_hash_crmcloudsync_Opportunity.initialcommunicationname initial_communication_name,
       stage_hash_crmcloudsync_Opportunity.isprivatename is_private_name,
       stage_hash_crmcloudsync_Opportunity.isrevenuesystemcalculated is_revenue_system_calculated,
       stage_hash_crmcloudsync_Opportunity.isrevenuesystemcalculatedname is_revenuesystem_calculated_name,
       stage_hash_crmcloudsync_Opportunity.ltf_besttimetocontact ltf_best_time_to_contact,
       stage_hash_crmcloudsync_Opportunity.ltf_besttimetocontactname ltf_best_time_to_contact_name,
       stage_hash_crmcloudsync_Opportunity.ltf_clubidname ltf_club_id_name,
       stage_hash_crmcloudsync_Opportunity.ltf_clubproximity ltf_club_proximity,
       stage_hash_crmcloudsync_Opportunity.ltf_clubproximityname ltf_club_proximity_name,
       stage_hash_crmcloudsync_Opportunity.ltf_commitmentlevel ltf_commitment_level,
       stage_hash_crmcloudsync_Opportunity.ltf_commitmentlevelname ltf_commitment_level_name,
       stage_hash_crmcloudsync_Opportunity.ltf_commitmentreason ltf_commitment_reason,
       stage_hash_crmcloudsync_Opportunity.ltf_employerwellnessprogram ltf_employer_wellness_program,
       stage_hash_crmcloudsync_Opportunity.ltf_employerwellnessprogramname ltf_employer_wellness_program_name,
       stage_hash_crmcloudsync_Opportunity.ltf_exercisehistory ltf_exercise_history,
       stage_hash_crmcloudsync_Opportunity.ltf_exercisehistoryname ltf_exercise_history_name,
       stage_hash_crmcloudsync_Opportunity.ltf_imsjoinlink ltf_ims_join_link,
       stage_hash_crmcloudsync_Opportunity.ltf_injuriesorlimitations ltf_injuries_or_limitations,
       stage_hash_crmcloudsync_Opportunity.ltf_injuriesorlimitationsdescription ltf_injuries_or_limitations_description,
       stage_hash_crmcloudsync_Opportunity.ltf_injuriesorlimitationsname ltf_injuries_or_limitations_name,
       stage_hash_crmcloudsync_Opportunity.ltf_inquirytype ltf_inquiry_type,
       stage_hash_crmcloudsync_Opportunity.ltf_lastactivity ltf_last_activity,
       stage_hash_crmcloudsync_Opportunity.ltf_leadsource ltf_lead_source,
       stage_hash_crmcloudsync_Opportunity.ltf_leadsourcename ltf_lead_source_name,
       stage_hash_crmcloudsync_Opportunity.ltf_leadtype ltf_lead_type,
       stage_hash_crmcloudsync_Opportunity.ltf_leadtypename ltf_lead_type_name,
       stage_hash_crmcloudsync_Opportunity.ltf_manageduntil ltf_managed_until,
       stage_hash_crmcloudsync_Opportunity.ltf_measurablegoal ltf_measurable_goal,
       stage_hash_crmcloudsync_Opportunity.ltf_measurablegoalidname ltf_measurable_goal_id_name,
       stage_hash_crmcloudsync_Opportunity.ltf_measurablegoalname ltf_measurable_goal_name,
       stage_hash_crmcloudsync_Opportunity.ltf_membershipinforequested ltf_membership_info_requested,
       stage_hash_crmcloudsync_Opportunity.ltf_membershiplevel ltf_membership_level,
       stage_hash_crmcloudsync_Opportunity.ltf_membershiplevelname ltf_membership_level_name,
       stage_hash_crmcloudsync_Opportunity.ltf_membershiptype ltf_membership_type,
       stage_hash_crmcloudsync_Opportunity.ltf_membershiptypename ltf_membership_type_name,
       stage_hash_crmcloudsync_Opportunity.ltf_nugget ltf_nugget,
       stage_hash_crmcloudsync_Opportunity.ltf_park ltf_park,
       stage_hash_crmcloudsync_Opportunity.ltf_parkcomments ltf_park_comments,
       stage_hash_crmcloudsync_Opportunity.ltf_parkname ltf_park_name,
       stage_hash_crmcloudsync_Opportunity.ltf_parkreason ltf_park_reason,
       stage_hash_crmcloudsync_Opportunity.ltf_parkreasonname ltf_park_reason_name,
       stage_hash_crmcloudsync_Opportunity.ltf_parkuntil ltf_park_until,
       stage_hash_crmcloudsync_Opportunity.ltf_pasttrainerorcoach ltf_past_trainer_or_coach,
       stage_hash_crmcloudsync_Opportunity.ltf_pasttrainerorcoachname ltf_past_trainer_or_coach_name,
       stage_hash_crmcloudsync_Opportunity.ltf_primaryobjective ltf_primary_objective,
       stage_hash_crmcloudsync_Opportunity.ltf_primaryobjectiveidname ltf_primary_objective_id_name,
       stage_hash_crmcloudsync_Opportunity.ltf_primaryobjectivename ltf_primary_objective_name,
       stage_hash_crmcloudsync_Opportunity.ltf_promocode ltf_promo_code,
       stage_hash_crmcloudsync_Opportunity.ltf_prospecttype ltf_prospect_type,
       stage_hash_crmcloudsync_Opportunity.ltf_prospecttypename ltf_prospect_type_name,
       stage_hash_crmcloudsync_Opportunity.ltf_referringcontactidname ltf_referring_contact_id_name,
       stage_hash_crmcloudsync_Opportunity.ltf_referringcontactidyominame ltf_referring_contact_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.ltf_registrationcode ltf_registration_code,
       stage_hash_crmcloudsync_Opportunity.ltf_requesttype ltf_request_type,
       stage_hash_crmcloudsync_Opportunity.ltf_specificgoal ltf_specific_goal,
       stage_hash_crmcloudsync_Opportunity.ltf_specificgoalidname ltf_specific_goal_id_name,
       stage_hash_crmcloudsync_Opportunity.ltf_specificgoalname ltf_specific_goal_name,
       stage_hash_crmcloudsync_Opportunity.ltf_timegoal ltf_time_goal,
       stage_hash_crmcloudsync_Opportunity.ltf_todaysaction ltf_todays_action,
       stage_hash_crmcloudsync_Opportunity.ltf_todaysactionname ltf_todays_action_name,
       stage_hash_crmcloudsync_Opportunity.ltf_trainerorcoachpreference ltf_trainer_or_coach_preference,
       stage_hash_crmcloudsync_Opportunity.ltf_trainerorcoachpreferencename ltf_trainer_or_coach_preference_name,
       stage_hash_crmcloudsync_Opportunity.ltf_webteamidname ltf_web_team_id_name,
       stage_hash_crmcloudsync_Opportunity.ltf_webteamidyominame ltf_web_team_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.ltf_workoutpreference ltf_workout_preference,
       stage_hash_crmcloudsync_Opportunity.ltf_workoutpreferencename ltf_workout_preference_name,
       stage_hash_crmcloudsync_Opportunity.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_Opportunity.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_Opportunity.modifiedon modified_on,
       stage_hash_crmcloudsync_Opportunity.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_Opportunity.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_Opportunity.name name,
       stage_hash_crmcloudsync_Opportunity.need need,
       stage_hash_crmcloudsync_Opportunity.needname need_name,
       stage_hash_crmcloudsync_Opportunity.opportunityid opportunity_id,
       stage_hash_crmcloudsync_Opportunity.opportunityratingcode opportunity_rating_code,
       stage_hash_crmcloudsync_Opportunity.opportunityratingcodename opportunity_rating_code_name,
       stage_hash_crmcloudsync_Opportunity.originatingleadidname originating_lead_id_name,
       stage_hash_crmcloudsync_Opportunity.originatingleadidyominame originating_lead_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_Opportunity.owneridname owner_id_name,
       stage_hash_crmcloudsync_Opportunity.owneridtype owner_id_type,
       stage_hash_crmcloudsync_Opportunity.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.parentaccountidname parent_account_id_name,
       stage_hash_crmcloudsync_Opportunity.parentaccountidyominame parent_account_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.parentcontactidname parent_contact_id_name,
       stage_hash_crmcloudsync_Opportunity.parentcontactidyominame parent_contact_id_yomi_name,
       stage_hash_crmcloudsync_Opportunity.participatesinworkflow participates_in_workflow,
       stage_hash_crmcloudsync_Opportunity.participatesinworkflowname participates_in_workflow_name,
       stage_hash_crmcloudsync_Opportunity.presentfinalproposal present_final_proposal,
       stage_hash_crmcloudsync_Opportunity.presentfinalproposalname present_final_proposal_name,
       stage_hash_crmcloudsync_Opportunity.presentproposal present_proposal,
       stage_hash_crmcloudsync_Opportunity.presentproposalname present_proposal_name,
       stage_hash_crmcloudsync_Opportunity.pricelevelidname price_level_id_name,
       stage_hash_crmcloudsync_Opportunity.pricingerrorcode pricing_error_code,
       stage_hash_crmcloudsync_Opportunity.pricingerrorcodename pricing_error_code_name,
       stage_hash_crmcloudsync_Opportunity.prioritycode priority_code,
       stage_hash_crmcloudsync_Opportunity.prioritycodename priority_code_name,
       stage_hash_crmcloudsync_Opportunity.proposedsolution proposed_solution,
       stage_hash_crmcloudsync_Opportunity.purchaseprocess purchase_process,
       stage_hash_crmcloudsync_Opportunity.purchaseprocessname purchase_process_name,
       stage_hash_crmcloudsync_Opportunity.purchasetimeframe purchase_time_frame,
       stage_hash_crmcloudsync_Opportunity.purchasetimeframename purchase_time_frame_name,
       stage_hash_crmcloudsync_Opportunity.pursuitdecision pursuit_decision,
       stage_hash_crmcloudsync_Opportunity.pursuitdecisionname pursuit_decision_name,
       stage_hash_crmcloudsync_Opportunity.qualificationcomments qualification_comments,
       stage_hash_crmcloudsync_Opportunity.quotecomments quote_comments,
       stage_hash_crmcloudsync_Opportunity.resolvefeedback resolve_feedback,
       stage_hash_crmcloudsync_Opportunity.resolvefeedbackname resolve_feedback_name,
       stage_hash_crmcloudsync_Opportunity.salesstage sales_stage,
       stage_hash_crmcloudsync_Opportunity.salesstagecode sales_stage_code,
       stage_hash_crmcloudsync_Opportunity.salesstagecodename sales_stage_code_name,
       stage_hash_crmcloudsync_Opportunity.salesstagename sales_stage_name,
       stage_hash_crmcloudsync_Opportunity.schedulefollowup_prospect schedule_follow_up_prospect,
       stage_hash_crmcloudsync_Opportunity.schedulefollowup_qualify schedule_follow_up_qualify,
       stage_hash_crmcloudsync_Opportunity.scheduleproposalmeeting schedule_proposal_meeting,
       stage_hash_crmcloudsync_Opportunity.sendthankyounote send_thank_you_note,
       stage_hash_crmcloudsync_Opportunity.sendthankyounotename send_thank_you_note_name,
       stage_hash_crmcloudsync_Opportunity.statecode state_code,
       stage_hash_crmcloudsync_Opportunity.statecodename state_code_name,
       stage_hash_crmcloudsync_Opportunity.statuscode status_code,
       stage_hash_crmcloudsync_Opportunity.statuscodename status_code_name,
       stage_hash_crmcloudsync_Opportunity.stepname step_name,
       stage_hash_crmcloudsync_Opportunity.timeline time_line,
       stage_hash_crmcloudsync_Opportunity.timelinename time_line_name,
       stage_hash_crmcloudsync_Opportunity.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_Opportunity.totalamount total_amount,
       stage_hash_crmcloudsync_Opportunity.totalamount_base total_amount_base,
       stage_hash_crmcloudsync_Opportunity.totalamountlessfreight total_amount_less_freight,
       stage_hash_crmcloudsync_Opportunity.totalamountlessfreight_base total_amount_less_freight_base,
       stage_hash_crmcloudsync_Opportunity.totaldiscountamount total_discount_amount,
       stage_hash_crmcloudsync_Opportunity.totaldiscountamount_base total_discount_amount_base,
       stage_hash_crmcloudsync_Opportunity.totallineitemamount total_line_item_amount,
       stage_hash_crmcloudsync_Opportunity.totallineitemamount_base total_line_item_amount_base,
       stage_hash_crmcloudsync_Opportunity.totallineitemdiscountamount total_line_item_discount_amount,
       stage_hash_crmcloudsync_Opportunity.totallineitemdiscountamount_base total_line_item_discount_amount_base,
       stage_hash_crmcloudsync_Opportunity.totaltax total_tax,
       stage_hash_crmcloudsync_Opportunity.totaltax_base total_tax_base,
       stage_hash_crmcloudsync_Opportunity.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_Opportunity.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_Opportunity.versionnumber version_number,
       stage_hash_crmcloudsync_Opportunity.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_Opportunity.InsertUser insert_user,
       stage_hash_crmcloudsync_Opportunity.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_Opportunity.UpdateUser update_user,
       stage_hash_crmcloudsync_Opportunity.ltf_profilenotes ltf_profile_notes,
       stage_hash_crmcloudsync_Opportunity.ltf_Programsofinterest ltf_programs_of_interest,
       stage_hash_crmcloudsync_Opportunity.ltf_Programsofinterestname ltf_programs_of_interest_name,
       stage_hash_crmcloudsync_Opportunity.ltf_wanttodo ltf_want_to_do,
       stage_hash_crmcloudsync_Opportunity.ltf_wanttodoname ltf_want_to_do_name,
       stage_hash_crmcloudsync_Opportunity.ltf_whywanttodo ltf_why_want_to_do,
       stage_hash_crmcloudsync_Opportunity.ltf_whywanttodoname ltf_why_want_to_do_name,
       stage_hash_crmcloudsync_Opportunity.ltf_whometwith ltf_who_met_with,
       stage_hash_crmcloudsync_Opportunity.ltf_recommendedmembership ltf_recommended_membership,
       stage_hash_crmcloudsync_Opportunity.ltf_recommendedmembershipname ltf_recommended_membership_name,
       stage_hash_crmcloudsync_Opportunity.ltf_resistance ltf_resistance,
       stage_hash_crmcloudsync_Opportunity.ltf_resistancename ltf_resistance_name,
       stage_hash_crmcloudsync_Opportunity.traversedpath traversed_path,
       stage_hash_crmcloudsync_Opportunity.ltf_webtransfermethod ltf_web_transfer_method,
       stage_hash_crmcloudsync_Opportunity.ltf_webtransfermethodname ltf_web_transfer_method_name,
       stage_hash_crmcloudsync_Opportunity.ltf_basketball ltf_basket_ball,
       stage_hash_crmcloudsync_Opportunity.ltf_climbing ltf_climbing,
       stage_hash_crmcloudsync_Opportunity.ltf_commitment ltf_commitment,
       stage_hash_crmcloudsync_Opportunity.ltf_cycling ltf_cycling,
       stage_hash_crmcloudsync_Opportunity.ltf_guestpassexpirationdate ltf_guest_pass_expiration_date,
       stage_hash_crmcloudsync_Opportunity.ltf_kidsclub ltf_kids_club,
       stage_hash_crmcloudsync_Opportunity.ltf_kidsclubname ltf_kids_club_name,
       stage_hash_crmcloudsync_Opportunity.ltf_nextfollowup ltf_next_follow_up,
       stage_hash_crmcloudsync_Opportunity.ltf_personaltraining ltf_personal_training,
       stage_hash_crmcloudsync_Opportunity.ltf_personaltrainingname ltf_personal_training_name,
       stage_hash_crmcloudsync_Opportunity.ltf_readytojoin ltf_ready_to_join,
       stage_hash_crmcloudsync_Opportunity.ltf_readytojoinname ltf_ready_to_join_name,
       stage_hash_crmcloudsync_Opportunity.ltf_signature ltf_signature,
       stage_hash_crmcloudsync_Opportunity.ltf_swimming ltf_swimming,
       stage_hash_crmcloudsync_Opportunity.ltf_swimmingname ltf_swimming_name,
       stage_hash_crmcloudsync_Opportunity.ltf_tennislessons ltf_tennis_lessons,
       stage_hash_crmcloudsync_Opportunity.ltf_tennislessonsname ltf_tennis_lessons_name,
       stage_hash_crmcloudsync_Opportunity.ltf_yoga ltf_yoga,
       stage_hash_crmcloudsync_Opportunity.ltf_assignedbyapp ltf_assigned_by_app,
       stage_hash_crmcloudsync_Opportunity.ltf_assignedbyappname ltf_assigned_by_app_name,
       stage_hash_crmcloudsync_Opportunity.ltf_assignedtoclubdate ltf_assigned_to_club_date,
       stage_hash_crmcloudsync_Opportunity.ltf_imsjoinsenddate ltf_ims_join_send_date,
       stage_hash_crmcloudsync_Opportunity.ltf_isimsjoin ltf_is_ims_join,
       stage_hash_crmcloudsync_Opportunity.ltf_isimsjoinname ltf_is_ims_join_name,
       stage_hash_crmcloudsync_Opportunity.ltf_numberover14list ltf_number_over_14_list,
       stage_hash_crmcloudsync_Opportunity.ltf_numberover14listname ltf_number_over_14_list_name,
       stage_hash_crmcloudsync_Opportunity.ltf_numberunder14list ltf_number_under_14_list,
       stage_hash_crmcloudsync_Opportunity.ltf_numberunder14listname ltf_number_under_14_list_name,
       stage_hash_crmcloudsync_Opportunity.ltf_promoquoted ltf_promo_quoted,
       stage_hash_crmcloudsync_Opportunity.ltf_selectedinterestids ltf_selected_interest_ids,
       stage_hash_crmcloudsync_Opportunity.ltf_originatingguestvisitname ltf_originating_guest_visit_name,
       stage_hash_crmcloudsync_Opportunity.ltf_assignmentrequestdate ltf_assignment_request_date,
       stage_hash_crmcloudsync_Opportunity.ltf_assignmentrequestid ltf_assignment_request_id,
       stage_hash_crmcloudsync_Opportunity.ltf_lineofbusiness ltf_line_of_business,
       stage_hash_crmcloudsync_Opportunity.ltf_channel ltf_channel,
       stage_hash_crmcloudsync_Opportunity.ltf_lineofbusinessname ltf_line_of_business_name,
       stage_hash_crmcloudsync_Opportunity.ltf_channelname ltf_channel_name,
       isnull(cast(stage_hash_crmcloudsync_Opportunity.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.accountidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.accountidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.actualclosedate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.actualvalue as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.actualvalue_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.budgetamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.budgetamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.budgetstatus as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.budgettypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.campaignidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.captureproposalfeedback as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.captureproposalfeedbackname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.closeprobability as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.completefinalproposal as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.completefinalproposalname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.completeinternalreview as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.completeinternalreviewname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.confirminterest as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.confirminterestname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.contactidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.contactidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.currentsituation,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.customeridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.customeridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.customeridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.customerneed,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.customerpainpoints,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.decisionmaker as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.decisionmakername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.developproposal as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.developproposalname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.discountamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.discountamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.discountpercentage as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.estimatedclosedate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.estimatedvalue as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.estimatedvalue_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.evaluatefit as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.evaluatefitname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.filedebrief as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.filedebriefname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.finaldecisiondate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.freightamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.freightamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.identifycompetitors as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.identifycompetitorsname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.identifycustomercontacts as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.identifycustomercontactsname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.identifypursuitteam as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.identifypursuitteamname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.initialcommunication as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.initialcommunicationname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.isprivatename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.isrevenuesystemcalculated as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.isrevenuesystemcalculatedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_besttimetocontact as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_besttimetocontactname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_clubidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_clubproximity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_clubproximityname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_commitmentlevel as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_commitmentlevelname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_commitmentreason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_employerwellnessprogram as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_employerwellnessprogramname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_exercisehistory as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_exercisehistoryname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_imsjoinlink,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_injuriesorlimitations as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_injuriesorlimitationsdescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_injuriesorlimitationsname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_inquirytype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_lastactivity,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_leadsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_leadsourcename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_leadtype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_leadtypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_manageduntil,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_measurablegoal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_measurablegoalidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_measurablegoalname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_membershipinforequested,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_membershiplevel as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_membershiplevelname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_membershiptype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_membershiptypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_nugget,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_park as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_parkcomments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_parkname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_parkreason as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_parkreasonname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_parkuntil,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_pasttrainerorcoach as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_pasttrainerorcoachname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_primaryobjective as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_primaryobjectiveidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_primaryobjectivename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_promocode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_prospecttype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_prospecttypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_referringcontactidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_referringcontactidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_registrationcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_requesttype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_specificgoal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_specificgoalidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_specificgoalname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_timegoal,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_todaysaction as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_todaysactionname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_trainerorcoachpreference as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_trainerorcoachpreferencename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_webteamidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_webteamidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_workoutpreference as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_workoutpreferencename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.need as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.needname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.opportunityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.opportunityratingcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.opportunityratingcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.originatingleadidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.originatingleadidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.parentaccountidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.parentaccountidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.parentcontactidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.parentcontactidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.participatesinworkflow as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.participatesinworkflowname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.presentfinalproposal as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.presentfinalproposalname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.presentproposal as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.presentproposalname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.pricelevelidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.pricingerrorcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.pricingerrorcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.prioritycode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.prioritycodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.proposedsolution,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.purchaseprocess as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.purchaseprocessname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.purchasetimeframe as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.purchasetimeframename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.pursuitdecision as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.pursuitdecisionname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.qualificationcomments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.quotecomments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.resolvefeedback as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.resolvefeedbackname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.salesstage as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.salesstagecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.salesstagecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.salesstagename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.schedulefollowup_prospect,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.schedulefollowup_qualify,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.scheduleproposalmeeting,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.sendthankyounote as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.sendthankyounotename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.stepname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.timeline as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.timelinename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totalamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totalamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totalamountlessfreight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totalamountlessfreight_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totaldiscountamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totaldiscountamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totallineitemamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totallineitemamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totallineitemdiscountamount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totallineitemdiscountamount_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totaltax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.totaltax_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_profilenotes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_Programsofinterest as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_Programsofinterestname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_wanttodo as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_wanttodoname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_whywanttodo as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_whywanttodoname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_whometwith,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_recommendedmembership as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_recommendedmembershipname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_resistance as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_resistancename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.traversedpath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_webtransfermethod as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_webtransfermethodname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_basketball as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_climbing as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_commitment as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_cycling as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_guestpassexpirationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_kidsclub as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_kidsclubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_nextfollowup,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_personaltraining as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_personaltrainingname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_readytojoin as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_readytojoinname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_signature,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_swimming as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_swimmingname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_tennislessons as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_tennislessonsname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_yoga as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_assignedbyapp as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_assignedbyappname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_assignedtoclubdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_imsjoinsenddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_isimsjoin as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_isimsjoinname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_numberover14list as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_numberover14listname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_numberunder14list as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_numberunder14listname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_promoquoted,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_selectedinterestids,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_originatingguestvisitname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_Opportunity.ltf_assignmentrequestdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_assignmentrequestid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_lineofbusiness as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_Opportunity.ltf_channel as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_lineofbusinessname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_Opportunity.ltf_channelname,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_Opportunity
 where stage_hash_crmcloudsync_Opportunity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_opportunity records
set @insert_date_time = getdate()
insert into s_crmcloudsync_opportunity (
       bk_hash,
       account_id_name,
       account_id_yomi_name,
       actual_close_date,
       actual_value,
       actual_value_base,
       budget_amount,
       budget_amount_base,
       budget_status,
       budget_type_name,
       campaign_id_name,
       capture_proposal_feedback,
       capture_proposal_feedback_name,
       close_probability,
       complete_final_proposal,
       complete_final_proposal_name,
       complete_internal_review,
       complete_internal_review_name,
       confirm_interest,
       confirm_interest_name,
       contact_id_name,
       contact_id_yomi_name,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       current_situation,
       customer_id_name,
       customer_id_type,
       customer_id_yomi_name,
       customer_need,
       customer_pain_points,
       decision_maker,
       decision_maker_name,
       description,
       develop_proposal,
       develop_proposal_name,
       discount_amount,
       discount_amount_base,
       discount_percentage,
       estimated_close_date,
       estimated_value,
       estimated_value_base,
       evaluate_fit,
       evaluate_fit_name,
       exchange_rate,
       filede_brief,
       filede_brief_name,
       final_decision_date,
       freight_amount,
       freight_amount_base,
       identify_competitors,
       identify_competitors_name,
       identify_customer_contacts,
       identify_customer_contacts_name,
       identify_pursuit_team,
       identify_pursuit_team_name,
       import_sequence_number,
       initial_communication,
       initial_communication_name,
       is_private_name,
       is_revenue_system_calculated,
       is_revenuesystem_calculated_name,
       ltf_best_time_to_contact,
       ltf_best_time_to_contact_name,
       ltf_club_id_name,
       ltf_club_proximity,
       ltf_club_proximity_name,
       ltf_commitment_level,
       ltf_commitment_level_name,
       ltf_commitment_reason,
       ltf_employer_wellness_program,
       ltf_employer_wellness_program_name,
       ltf_exercise_history,
       ltf_exercise_history_name,
       ltf_ims_join_link,
       ltf_injuries_or_limitations,
       ltf_injuries_or_limitations_description,
       ltf_injuries_or_limitations_name,
       ltf_inquiry_type,
       ltf_last_activity,
       ltf_lead_source,
       ltf_lead_source_name,
       ltf_lead_type,
       ltf_lead_type_name,
       ltf_managed_until,
       ltf_measurable_goal,
       ltf_measurable_goal_id_name,
       ltf_measurable_goal_name,
       ltf_membership_info_requested,
       ltf_membership_level,
       ltf_membership_level_name,
       ltf_membership_type,
       ltf_membership_type_name,
       ltf_nugget,
       ltf_park,
       ltf_park_comments,
       ltf_park_name,
       ltf_park_reason,
       ltf_park_reason_name,
       ltf_park_until,
       ltf_past_trainer_or_coach,
       ltf_past_trainer_or_coach_name,
       ltf_primary_objective,
       ltf_primary_objective_id_name,
       ltf_primary_objective_name,
       ltf_promo_code,
       ltf_prospect_type,
       ltf_prospect_type_name,
       ltf_referring_contact_id_name,
       ltf_referring_contact_id_yomi_name,
       ltf_registration_code,
       ltf_request_type,
       ltf_specific_goal,
       ltf_specific_goal_id_name,
       ltf_specific_goal_name,
       ltf_time_goal,
       ltf_todays_action,
       ltf_todays_action_name,
       ltf_trainer_or_coach_preference,
       ltf_trainer_or_coach_preference_name,
       ltf_web_team_id_name,
       ltf_web_team_id_yomi_name,
       ltf_workout_preference,
       ltf_workout_preference_name,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       name,
       need,
       need_name,
       opportunity_id,
       opportunity_rating_code,
       opportunity_rating_code_name,
       originating_lead_id_name,
       originating_lead_id_yomi_name,
       overridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       parent_account_id_name,
       parent_account_id_yomi_name,
       parent_contact_id_name,
       parent_contact_id_yomi_name,
       participates_in_workflow,
       participates_in_workflow_name,
       present_final_proposal,
       present_final_proposal_name,
       present_proposal,
       present_proposal_name,
       price_level_id_name,
       pricing_error_code,
       pricing_error_code_name,
       priority_code,
       priority_code_name,
       proposed_solution,
       purchase_process,
       purchase_process_name,
       purchase_time_frame,
       purchase_time_frame_name,
       pursuit_decision,
       pursuit_decision_name,
       qualification_comments,
       quote_comments,
       resolve_feedback,
       resolve_feedback_name,
       sales_stage,
       sales_stage_code,
       sales_stage_code_name,
       sales_stage_name,
       schedule_follow_up_prospect,
       schedule_follow_up_qualify,
       schedule_proposal_meeting,
       send_thank_you_note,
       send_thank_you_note_name,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       step_name,
       time_line,
       time_line_name,
       time_zone_rule_version_number,
       total_amount,
       total_amount_base,
       total_amount_less_freight,
       total_amount_less_freight_base,
       total_discount_amount,
       total_discount_amount_base,
       total_line_item_amount,
       total_line_item_amount_base,
       total_line_item_discount_amount,
       total_line_item_discount_amount_base,
       total_tax,
       total_tax_base,
       transaction_currency_id_name,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       ltf_profile_notes,
       ltf_programs_of_interest,
       ltf_programs_of_interest_name,
       ltf_want_to_do,
       ltf_want_to_do_name,
       ltf_why_want_to_do,
       ltf_why_want_to_do_name,
       ltf_who_met_with,
       ltf_recommended_membership,
       ltf_recommended_membership_name,
       ltf_resistance,
       ltf_resistance_name,
       traversed_path,
       ltf_web_transfer_method,
       ltf_web_transfer_method_name,
       ltf_basket_ball,
       ltf_climbing,
       ltf_commitment,
       ltf_cycling,
       ltf_guest_pass_expiration_date,
       ltf_kids_club,
       ltf_kids_club_name,
       ltf_next_follow_up,
       ltf_personal_training,
       ltf_personal_training_name,
       ltf_ready_to_join,
       ltf_ready_to_join_name,
       ltf_signature,
       ltf_swimming,
       ltf_swimming_name,
       ltf_tennis_lessons,
       ltf_tennis_lessons_name,
       ltf_yoga,
       ltf_assigned_by_app,
       ltf_assigned_by_app_name,
       ltf_assigned_to_club_date,
       ltf_ims_join_send_date,
       ltf_is_ims_join,
       ltf_is_ims_join_name,
       ltf_number_over_14_list,
       ltf_number_over_14_list_name,
       ltf_number_under_14_list,
       ltf_number_under_14_list_name,
       ltf_promo_quoted,
       ltf_selected_interest_ids,
       ltf_originating_guest_visit_name,
       ltf_assignment_request_date,
       ltf_assignment_request_id,
       ltf_line_of_business,
       ltf_channel,
       ltf_line_of_business_name,
       ltf_channel_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_opportunity_inserts.bk_hash,
       #s_crmcloudsync_opportunity_inserts.account_id_name,
       #s_crmcloudsync_opportunity_inserts.account_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.actual_close_date,
       #s_crmcloudsync_opportunity_inserts.actual_value,
       #s_crmcloudsync_opportunity_inserts.actual_value_base,
       #s_crmcloudsync_opportunity_inserts.budget_amount,
       #s_crmcloudsync_opportunity_inserts.budget_amount_base,
       #s_crmcloudsync_opportunity_inserts.budget_status,
       #s_crmcloudsync_opportunity_inserts.budget_type_name,
       #s_crmcloudsync_opportunity_inserts.campaign_id_name,
       #s_crmcloudsync_opportunity_inserts.capture_proposal_feedback,
       #s_crmcloudsync_opportunity_inserts.capture_proposal_feedback_name,
       #s_crmcloudsync_opportunity_inserts.close_probability,
       #s_crmcloudsync_opportunity_inserts.complete_final_proposal,
       #s_crmcloudsync_opportunity_inserts.complete_final_proposal_name,
       #s_crmcloudsync_opportunity_inserts.complete_internal_review,
       #s_crmcloudsync_opportunity_inserts.complete_internal_review_name,
       #s_crmcloudsync_opportunity_inserts.confirm_interest,
       #s_crmcloudsync_opportunity_inserts.confirm_interest_name,
       #s_crmcloudsync_opportunity_inserts.contact_id_name,
       #s_crmcloudsync_opportunity_inserts.contact_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.created_by_name,
       #s_crmcloudsync_opportunity_inserts.created_by_yomi_name,
       #s_crmcloudsync_opportunity_inserts.created_on,
       #s_crmcloudsync_opportunity_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_opportunity_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_opportunity_inserts.current_situation,
       #s_crmcloudsync_opportunity_inserts.customer_id_name,
       #s_crmcloudsync_opportunity_inserts.customer_id_type,
       #s_crmcloudsync_opportunity_inserts.customer_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.customer_need,
       #s_crmcloudsync_opportunity_inserts.customer_pain_points,
       #s_crmcloudsync_opportunity_inserts.decision_maker,
       #s_crmcloudsync_opportunity_inserts.decision_maker_name,
       #s_crmcloudsync_opportunity_inserts.description,
       #s_crmcloudsync_opportunity_inserts.develop_proposal,
       #s_crmcloudsync_opportunity_inserts.develop_proposal_name,
       #s_crmcloudsync_opportunity_inserts.discount_amount,
       #s_crmcloudsync_opportunity_inserts.discount_amount_base,
       #s_crmcloudsync_opportunity_inserts.discount_percentage,
       #s_crmcloudsync_opportunity_inserts.estimated_close_date,
       #s_crmcloudsync_opportunity_inserts.estimated_value,
       #s_crmcloudsync_opportunity_inserts.estimated_value_base,
       #s_crmcloudsync_opportunity_inserts.evaluate_fit,
       #s_crmcloudsync_opportunity_inserts.evaluate_fit_name,
       #s_crmcloudsync_opportunity_inserts.exchange_rate,
       #s_crmcloudsync_opportunity_inserts.filede_brief,
       #s_crmcloudsync_opportunity_inserts.filede_brief_name,
       #s_crmcloudsync_opportunity_inserts.final_decision_date,
       #s_crmcloudsync_opportunity_inserts.freight_amount,
       #s_crmcloudsync_opportunity_inserts.freight_amount_base,
       #s_crmcloudsync_opportunity_inserts.identify_competitors,
       #s_crmcloudsync_opportunity_inserts.identify_competitors_name,
       #s_crmcloudsync_opportunity_inserts.identify_customer_contacts,
       #s_crmcloudsync_opportunity_inserts.identify_customer_contacts_name,
       #s_crmcloudsync_opportunity_inserts.identify_pursuit_team,
       #s_crmcloudsync_opportunity_inserts.identify_pursuit_team_name,
       #s_crmcloudsync_opportunity_inserts.import_sequence_number,
       #s_crmcloudsync_opportunity_inserts.initial_communication,
       #s_crmcloudsync_opportunity_inserts.initial_communication_name,
       #s_crmcloudsync_opportunity_inserts.is_private_name,
       #s_crmcloudsync_opportunity_inserts.is_revenue_system_calculated,
       #s_crmcloudsync_opportunity_inserts.is_revenuesystem_calculated_name,
       #s_crmcloudsync_opportunity_inserts.ltf_best_time_to_contact,
       #s_crmcloudsync_opportunity_inserts.ltf_best_time_to_contact_name,
       #s_crmcloudsync_opportunity_inserts.ltf_club_id_name,
       #s_crmcloudsync_opportunity_inserts.ltf_club_proximity,
       #s_crmcloudsync_opportunity_inserts.ltf_club_proximity_name,
       #s_crmcloudsync_opportunity_inserts.ltf_commitment_level,
       #s_crmcloudsync_opportunity_inserts.ltf_commitment_level_name,
       #s_crmcloudsync_opportunity_inserts.ltf_commitment_reason,
       #s_crmcloudsync_opportunity_inserts.ltf_employer_wellness_program,
       #s_crmcloudsync_opportunity_inserts.ltf_employer_wellness_program_name,
       #s_crmcloudsync_opportunity_inserts.ltf_exercise_history,
       #s_crmcloudsync_opportunity_inserts.ltf_exercise_history_name,
       #s_crmcloudsync_opportunity_inserts.ltf_ims_join_link,
       #s_crmcloudsync_opportunity_inserts.ltf_injuries_or_limitations,
       #s_crmcloudsync_opportunity_inserts.ltf_injuries_or_limitations_description,
       #s_crmcloudsync_opportunity_inserts.ltf_injuries_or_limitations_name,
       #s_crmcloudsync_opportunity_inserts.ltf_inquiry_type,
       #s_crmcloudsync_opportunity_inserts.ltf_last_activity,
       #s_crmcloudsync_opportunity_inserts.ltf_lead_source,
       #s_crmcloudsync_opportunity_inserts.ltf_lead_source_name,
       #s_crmcloudsync_opportunity_inserts.ltf_lead_type,
       #s_crmcloudsync_opportunity_inserts.ltf_lead_type_name,
       #s_crmcloudsync_opportunity_inserts.ltf_managed_until,
       #s_crmcloudsync_opportunity_inserts.ltf_measurable_goal,
       #s_crmcloudsync_opportunity_inserts.ltf_measurable_goal_id_name,
       #s_crmcloudsync_opportunity_inserts.ltf_measurable_goal_name,
       #s_crmcloudsync_opportunity_inserts.ltf_membership_info_requested,
       #s_crmcloudsync_opportunity_inserts.ltf_membership_level,
       #s_crmcloudsync_opportunity_inserts.ltf_membership_level_name,
       #s_crmcloudsync_opportunity_inserts.ltf_membership_type,
       #s_crmcloudsync_opportunity_inserts.ltf_membership_type_name,
       #s_crmcloudsync_opportunity_inserts.ltf_nugget,
       #s_crmcloudsync_opportunity_inserts.ltf_park,
       #s_crmcloudsync_opportunity_inserts.ltf_park_comments,
       #s_crmcloudsync_opportunity_inserts.ltf_park_name,
       #s_crmcloudsync_opportunity_inserts.ltf_park_reason,
       #s_crmcloudsync_opportunity_inserts.ltf_park_reason_name,
       #s_crmcloudsync_opportunity_inserts.ltf_park_until,
       #s_crmcloudsync_opportunity_inserts.ltf_past_trainer_or_coach,
       #s_crmcloudsync_opportunity_inserts.ltf_past_trainer_or_coach_name,
       #s_crmcloudsync_opportunity_inserts.ltf_primary_objective,
       #s_crmcloudsync_opportunity_inserts.ltf_primary_objective_id_name,
       #s_crmcloudsync_opportunity_inserts.ltf_primary_objective_name,
       #s_crmcloudsync_opportunity_inserts.ltf_promo_code,
       #s_crmcloudsync_opportunity_inserts.ltf_prospect_type,
       #s_crmcloudsync_opportunity_inserts.ltf_prospect_type_name,
       #s_crmcloudsync_opportunity_inserts.ltf_referring_contact_id_name,
       #s_crmcloudsync_opportunity_inserts.ltf_referring_contact_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.ltf_registration_code,
       #s_crmcloudsync_opportunity_inserts.ltf_request_type,
       #s_crmcloudsync_opportunity_inserts.ltf_specific_goal,
       #s_crmcloudsync_opportunity_inserts.ltf_specific_goal_id_name,
       #s_crmcloudsync_opportunity_inserts.ltf_specific_goal_name,
       #s_crmcloudsync_opportunity_inserts.ltf_time_goal,
       #s_crmcloudsync_opportunity_inserts.ltf_todays_action,
       #s_crmcloudsync_opportunity_inserts.ltf_todays_action_name,
       #s_crmcloudsync_opportunity_inserts.ltf_trainer_or_coach_preference,
       #s_crmcloudsync_opportunity_inserts.ltf_trainer_or_coach_preference_name,
       #s_crmcloudsync_opportunity_inserts.ltf_web_team_id_name,
       #s_crmcloudsync_opportunity_inserts.ltf_web_team_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.ltf_workout_preference,
       #s_crmcloudsync_opportunity_inserts.ltf_workout_preference_name,
       #s_crmcloudsync_opportunity_inserts.modified_by_name,
       #s_crmcloudsync_opportunity_inserts.modified_by_yomi_name,
       #s_crmcloudsync_opportunity_inserts.modified_on,
       #s_crmcloudsync_opportunity_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_opportunity_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_opportunity_inserts.name,
       #s_crmcloudsync_opportunity_inserts.need,
       #s_crmcloudsync_opportunity_inserts.need_name,
       #s_crmcloudsync_opportunity_inserts.opportunity_id,
       #s_crmcloudsync_opportunity_inserts.opportunity_rating_code,
       #s_crmcloudsync_opportunity_inserts.opportunity_rating_code_name,
       #s_crmcloudsync_opportunity_inserts.originating_lead_id_name,
       #s_crmcloudsync_opportunity_inserts.originating_lead_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.overridden_created_on,
       #s_crmcloudsync_opportunity_inserts.owner_id_name,
       #s_crmcloudsync_opportunity_inserts.owner_id_type,
       #s_crmcloudsync_opportunity_inserts.owner_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.parent_account_id_name,
       #s_crmcloudsync_opportunity_inserts.parent_account_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.parent_contact_id_name,
       #s_crmcloudsync_opportunity_inserts.parent_contact_id_yomi_name,
       #s_crmcloudsync_opportunity_inserts.participates_in_workflow,
       #s_crmcloudsync_opportunity_inserts.participates_in_workflow_name,
       #s_crmcloudsync_opportunity_inserts.present_final_proposal,
       #s_crmcloudsync_opportunity_inserts.present_final_proposal_name,
       #s_crmcloudsync_opportunity_inserts.present_proposal,
       #s_crmcloudsync_opportunity_inserts.present_proposal_name,
       #s_crmcloudsync_opportunity_inserts.price_level_id_name,
       #s_crmcloudsync_opportunity_inserts.pricing_error_code,
       #s_crmcloudsync_opportunity_inserts.pricing_error_code_name,
       #s_crmcloudsync_opportunity_inserts.priority_code,
       #s_crmcloudsync_opportunity_inserts.priority_code_name,
       #s_crmcloudsync_opportunity_inserts.proposed_solution,
       #s_crmcloudsync_opportunity_inserts.purchase_process,
       #s_crmcloudsync_opportunity_inserts.purchase_process_name,
       #s_crmcloudsync_opportunity_inserts.purchase_time_frame,
       #s_crmcloudsync_opportunity_inserts.purchase_time_frame_name,
       #s_crmcloudsync_opportunity_inserts.pursuit_decision,
       #s_crmcloudsync_opportunity_inserts.pursuit_decision_name,
       #s_crmcloudsync_opportunity_inserts.qualification_comments,
       #s_crmcloudsync_opportunity_inserts.quote_comments,
       #s_crmcloudsync_opportunity_inserts.resolve_feedback,
       #s_crmcloudsync_opportunity_inserts.resolve_feedback_name,
       #s_crmcloudsync_opportunity_inserts.sales_stage,
       #s_crmcloudsync_opportunity_inserts.sales_stage_code,
       #s_crmcloudsync_opportunity_inserts.sales_stage_code_name,
       #s_crmcloudsync_opportunity_inserts.sales_stage_name,
       #s_crmcloudsync_opportunity_inserts.schedule_follow_up_prospect,
       #s_crmcloudsync_opportunity_inserts.schedule_follow_up_qualify,
       #s_crmcloudsync_opportunity_inserts.schedule_proposal_meeting,
       #s_crmcloudsync_opportunity_inserts.send_thank_you_note,
       #s_crmcloudsync_opportunity_inserts.send_thank_you_note_name,
       #s_crmcloudsync_opportunity_inserts.state_code,
       #s_crmcloudsync_opportunity_inserts.state_code_name,
       #s_crmcloudsync_opportunity_inserts.status_code,
       #s_crmcloudsync_opportunity_inserts.status_code_name,
       #s_crmcloudsync_opportunity_inserts.step_name,
       #s_crmcloudsync_opportunity_inserts.time_line,
       #s_crmcloudsync_opportunity_inserts.time_line_name,
       #s_crmcloudsync_opportunity_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_opportunity_inserts.total_amount,
       #s_crmcloudsync_opportunity_inserts.total_amount_base,
       #s_crmcloudsync_opportunity_inserts.total_amount_less_freight,
       #s_crmcloudsync_opportunity_inserts.total_amount_less_freight_base,
       #s_crmcloudsync_opportunity_inserts.total_discount_amount,
       #s_crmcloudsync_opportunity_inserts.total_discount_amount_base,
       #s_crmcloudsync_opportunity_inserts.total_line_item_amount,
       #s_crmcloudsync_opportunity_inserts.total_line_item_amount_base,
       #s_crmcloudsync_opportunity_inserts.total_line_item_discount_amount,
       #s_crmcloudsync_opportunity_inserts.total_line_item_discount_amount_base,
       #s_crmcloudsync_opportunity_inserts.total_tax,
       #s_crmcloudsync_opportunity_inserts.total_tax_base,
       #s_crmcloudsync_opportunity_inserts.transaction_currency_id_name,
       #s_crmcloudsync_opportunity_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_opportunity_inserts.version_number,
       #s_crmcloudsync_opportunity_inserts.inserted_date_time,
       #s_crmcloudsync_opportunity_inserts.insert_user,
       #s_crmcloudsync_opportunity_inserts.updated_date_time,
       #s_crmcloudsync_opportunity_inserts.update_user,
       #s_crmcloudsync_opportunity_inserts.ltf_profile_notes,
       #s_crmcloudsync_opportunity_inserts.ltf_programs_of_interest,
       #s_crmcloudsync_opportunity_inserts.ltf_programs_of_interest_name,
       #s_crmcloudsync_opportunity_inserts.ltf_want_to_do,
       #s_crmcloudsync_opportunity_inserts.ltf_want_to_do_name,
       #s_crmcloudsync_opportunity_inserts.ltf_why_want_to_do,
       #s_crmcloudsync_opportunity_inserts.ltf_why_want_to_do_name,
       #s_crmcloudsync_opportunity_inserts.ltf_who_met_with,
       #s_crmcloudsync_opportunity_inserts.ltf_recommended_membership,
       #s_crmcloudsync_opportunity_inserts.ltf_recommended_membership_name,
       #s_crmcloudsync_opportunity_inserts.ltf_resistance,
       #s_crmcloudsync_opportunity_inserts.ltf_resistance_name,
       #s_crmcloudsync_opportunity_inserts.traversed_path,
       #s_crmcloudsync_opportunity_inserts.ltf_web_transfer_method,
       #s_crmcloudsync_opportunity_inserts.ltf_web_transfer_method_name,
       #s_crmcloudsync_opportunity_inserts.ltf_basket_ball,
       #s_crmcloudsync_opportunity_inserts.ltf_climbing,
       #s_crmcloudsync_opportunity_inserts.ltf_commitment,
       #s_crmcloudsync_opportunity_inserts.ltf_cycling,
       #s_crmcloudsync_opportunity_inserts.ltf_guest_pass_expiration_date,
       #s_crmcloudsync_opportunity_inserts.ltf_kids_club,
       #s_crmcloudsync_opportunity_inserts.ltf_kids_club_name,
       #s_crmcloudsync_opportunity_inserts.ltf_next_follow_up,
       #s_crmcloudsync_opportunity_inserts.ltf_personal_training,
       #s_crmcloudsync_opportunity_inserts.ltf_personal_training_name,
       #s_crmcloudsync_opportunity_inserts.ltf_ready_to_join,
       #s_crmcloudsync_opportunity_inserts.ltf_ready_to_join_name,
       #s_crmcloudsync_opportunity_inserts.ltf_signature,
       #s_crmcloudsync_opportunity_inserts.ltf_swimming,
       #s_crmcloudsync_opportunity_inserts.ltf_swimming_name,
       #s_crmcloudsync_opportunity_inserts.ltf_tennis_lessons,
       #s_crmcloudsync_opportunity_inserts.ltf_tennis_lessons_name,
       #s_crmcloudsync_opportunity_inserts.ltf_yoga,
       #s_crmcloudsync_opportunity_inserts.ltf_assigned_by_app,
       #s_crmcloudsync_opportunity_inserts.ltf_assigned_by_app_name,
       #s_crmcloudsync_opportunity_inserts.ltf_assigned_to_club_date,
       #s_crmcloudsync_opportunity_inserts.ltf_ims_join_send_date,
       #s_crmcloudsync_opportunity_inserts.ltf_is_ims_join,
       #s_crmcloudsync_opportunity_inserts.ltf_is_ims_join_name,
       #s_crmcloudsync_opportunity_inserts.ltf_number_over_14_list,
       #s_crmcloudsync_opportunity_inserts.ltf_number_over_14_list_name,
       #s_crmcloudsync_opportunity_inserts.ltf_number_under_14_list,
       #s_crmcloudsync_opportunity_inserts.ltf_number_under_14_list_name,
       #s_crmcloudsync_opportunity_inserts.ltf_promo_quoted,
       #s_crmcloudsync_opportunity_inserts.ltf_selected_interest_ids,
       #s_crmcloudsync_opportunity_inserts.ltf_originating_guest_visit_name,
       #s_crmcloudsync_opportunity_inserts.ltf_assignment_request_date,
       #s_crmcloudsync_opportunity_inserts.ltf_assignment_request_id,
       #s_crmcloudsync_opportunity_inserts.ltf_line_of_business,
       #s_crmcloudsync_opportunity_inserts.ltf_channel,
       #s_crmcloudsync_opportunity_inserts.ltf_line_of_business_name,
       #s_crmcloudsync_opportunity_inserts.ltf_channel_name,
       case when s_crmcloudsync_opportunity.s_crmcloudsync_opportunity_id is null then isnull(#s_crmcloudsync_opportunity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_opportunity_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_opportunity_inserts
  left join p_crmcloudsync_opportunity
    on #s_crmcloudsync_opportunity_inserts.bk_hash = p_crmcloudsync_opportunity.bk_hash
   and p_crmcloudsync_opportunity.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_opportunity
    on p_crmcloudsync_opportunity.bk_hash = s_crmcloudsync_opportunity.bk_hash
   and p_crmcloudsync_opportunity.s_crmcloudsync_opportunity_id = s_crmcloudsync_opportunity.s_crmcloudsync_opportunity_id
 where s_crmcloudsync_opportunity.s_crmcloudsync_opportunity_id is null
    or (s_crmcloudsync_opportunity.s_crmcloudsync_opportunity_id is not null
        and s_crmcloudsync_opportunity.dv_hash <> #s_crmcloudsync_opportunity_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_opportunity @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_opportunity @current_dv_batch_id

end
