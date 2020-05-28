CREATE PROC [dbo].[proc_etl_crmcloudsync_campaign] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_campaign

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_campaign (
       bk_hash,
       actualend,
       actualstart,
       budgetedcost,
       budgetedcost_base,
       campaignid,
       codename,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       description,
       entityimage,
       entityimage_timestamp,
       entityimage_url,
       entityimageid,
       exchangerate,
       expectedresponse,
       expectedrevenue,
       expectedrevenue_base,
       importsequencenumber,
       istemplate,
       istemplatename,
       ltf_clubrestrictiontype,
       ltf_clubrestrictiontypename,
       ltf_connectwitham,
       ltf_connectwithamname,
       ltf_displayonform,
       ltf_displayonformname,
       ltf_expirationdays,
       ltf_expirationtype,
       ltf_expirationtypename,
       ltf_passdays,
       ltf_targetedrecordtype,
       ltf_targetedrecordtypename,
       ltf_userdefineddates,
       ltf_userdefineddatesname,
       message,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       name,
       objective,
       othercost,
       othercost_base,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       pricelistid,
       pricelistname,
       processid,
       promotioncodename,
       proposedend,
       proposedstart,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       totalactualcost,
       totalactualcost_base,
       totalcampaignactivityactualcost,
       totalcampaignactivityactualcost_base,
       transactioncurrencyid,
       transactioncurrencyidname,
       traversedpath,
       typecode,
       typecodename,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_issuancemethod,
       ltf_jobid,
       ltf_qrcodestring,
       ltf_qrcodeurl,
       ltf_targetedprospects,
       ltf_targetedissuedate,
       ltf_sendid,
       ltf_memberreferral,
       ltf_guestpasstype,
       ltf_rewardtype,
       ltf_rewardltbucks,
       ltf_rewardwaitdays,
       ltf_rewardclub,
       ltf_restrictedbypolicy,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(campaignid,'z#@$k%&P'))),2) bk_hash,
       actualend,
       actualstart,
       budgetedcost,
       budgetedcost_base,
       campaignid,
       codename,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       description,
       entityimage,
       entityimage_timestamp,
       entityimage_url,
       entityimageid,
       exchangerate,
       expectedresponse,
       expectedrevenue,
       expectedrevenue_base,
       importsequencenumber,
       istemplate,
       istemplatename,
       ltf_clubrestrictiontype,
       ltf_clubrestrictiontypename,
       ltf_connectwitham,
       ltf_connectwithamname,
       ltf_displayonform,
       ltf_displayonformname,
       ltf_expirationdays,
       ltf_expirationtype,
       ltf_expirationtypename,
       ltf_passdays,
       ltf_targetedrecordtype,
       ltf_targetedrecordtypename,
       ltf_userdefineddates,
       ltf_userdefineddatesname,
       message,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       name,
       objective,
       othercost,
       othercost_base,
       overriddencreatedon,
       ownerid,
       owneridname,
       owneridtype,
       owneridyominame,
       owningbusinessunit,
       owningteam,
       owninguser,
       pricelistid,
       pricelistname,
       processid,
       promotioncodename,
       proposedend,
       proposedstart,
       stageid,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       totalactualcost,
       totalactualcost_base,
       totalcampaignactivityactualcost,
       totalcampaignactivityactualcost_base,
       transactioncurrencyid,
       transactioncurrencyidname,
       traversedpath,
       typecode,
       typecodename,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_issuancemethod,
       ltf_jobid,
       ltf_qrcodestring,
       ltf_qrcodeurl,
       ltf_targetedprospects,
       ltf_targetedissuedate,
       ltf_sendid,
       ltf_memberreferral,
       ltf_guestpasstype,
       ltf_rewardtype,
       ltf_rewardltbucks,
       ltf_rewardwaitdays,
       ltf_rewardclub,
       ltf_restrictedbypolicy,
       isnull(cast(stage_crmcloudsync_campaign.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_campaign
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_campaign @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_campaign (
       bk_hash,
       campaign_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_campaign.bk_hash,
       stage_hash_crmcloudsync_campaign.campaignid campaign_id,
       isnull(cast(stage_hash_crmcloudsync_campaign.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_campaign
  left join h_crmcloudsync_campaign
    on stage_hash_crmcloudsync_campaign.bk_hash = h_crmcloudsync_campaign.bk_hash
 where h_crmcloudsync_campaign_id is null
   and stage_hash_crmcloudsync_campaign.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_campaign
if object_id('tempdb..#l_crmcloudsync_campaign_inserts') is not null drop table #l_crmcloudsync_campaign_inserts
create table #l_crmcloudsync_campaign_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_campaign.bk_hash,
       stage_hash_crmcloudsync_campaign.campaignid campaign_id,
       stage_hash_crmcloudsync_campaign.createdby created_by,
       stage_hash_crmcloudsync_campaign.modifiedby modified_by,
       stage_hash_crmcloudsync_campaign.ownerid owner_id,
       stage_hash_crmcloudsync_campaign.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_campaign.owninguser owning_user,
       stage_hash_crmcloudsync_campaign.transactioncurrencyid transaction_currency_id,
       stage_hash_crmcloudsync_campaign.ltf_jobid ltf_job_id,
       isnull(cast(stage_hash_crmcloudsync_campaign.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.campaignid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.transactioncurrencyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_jobid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_campaign
 where stage_hash_crmcloudsync_campaign.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_campaign records
set @insert_date_time = getdate()
insert into l_crmcloudsync_campaign (
       bk_hash,
       campaign_id,
       created_by,
       modified_by,
       owner_id,
       owning_business_unit,
       owning_user,
       transaction_currency_id,
       ltf_job_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_campaign_inserts.bk_hash,
       #l_crmcloudsync_campaign_inserts.campaign_id,
       #l_crmcloudsync_campaign_inserts.created_by,
       #l_crmcloudsync_campaign_inserts.modified_by,
       #l_crmcloudsync_campaign_inserts.owner_id,
       #l_crmcloudsync_campaign_inserts.owning_business_unit,
       #l_crmcloudsync_campaign_inserts.owning_user,
       #l_crmcloudsync_campaign_inserts.transaction_currency_id,
       #l_crmcloudsync_campaign_inserts.ltf_job_id,
       case when l_crmcloudsync_campaign.l_crmcloudsync_campaign_id is null then isnull(#l_crmcloudsync_campaign_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_campaign_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_campaign_inserts
  left join p_crmcloudsync_campaign
    on #l_crmcloudsync_campaign_inserts.bk_hash = p_crmcloudsync_campaign.bk_hash
   and p_crmcloudsync_campaign.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_campaign
    on p_crmcloudsync_campaign.bk_hash = l_crmcloudsync_campaign.bk_hash
   and p_crmcloudsync_campaign.l_crmcloudsync_campaign_id = l_crmcloudsync_campaign.l_crmcloudsync_campaign_id
 where l_crmcloudsync_campaign.l_crmcloudsync_campaign_id is null
    or (l_crmcloudsync_campaign.l_crmcloudsync_campaign_id is not null
        and l_crmcloudsync_campaign.dv_hash <> #l_crmcloudsync_campaign_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_campaign
if object_id('tempdb..#s_crmcloudsync_campaign_inserts') is not null drop table #s_crmcloudsync_campaign_inserts
create table #s_crmcloudsync_campaign_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_campaign.bk_hash,
       stage_hash_crmcloudsync_campaign.actualend actual_end,
       stage_hash_crmcloudsync_campaign.actualstart actual_start,
       stage_hash_crmcloudsync_campaign.budgetedcost budgeted_cost,
       stage_hash_crmcloudsync_campaign.budgetedcost_base budgeted_cost_base,
       stage_hash_crmcloudsync_campaign.campaignid campaign_id,
       stage_hash_crmcloudsync_campaign.codename code_name,
       stage_hash_crmcloudsync_campaign.createdbyname created_by_name,
       stage_hash_crmcloudsync_campaign.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_campaign.createdon created_on,
       stage_hash_crmcloudsync_campaign.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_campaign.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_campaign.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_campaign.description description,
       stage_hash_crmcloudsync_campaign.entityimage entity_image,
       stage_hash_crmcloudsync_campaign.entityimage_timestamp entity_image_timestamp,
       stage_hash_crmcloudsync_campaign.entityimage_url entity_image_url,
       stage_hash_crmcloudsync_campaign.entityimageid entity_imageid,
       stage_hash_crmcloudsync_campaign.exchangerate exchange_rate,
       stage_hash_crmcloudsync_campaign.expectedresponse expected_response,
       stage_hash_crmcloudsync_campaign.expectedrevenue expected_revenue,
       stage_hash_crmcloudsync_campaign.expectedrevenue_base expected_revenue_base,
       stage_hash_crmcloudsync_campaign.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_campaign.istemplate is_template,
       stage_hash_crmcloudsync_campaign.istemplatename is_template_name,
       stage_hash_crmcloudsync_campaign.ltf_clubrestrictiontype ltf_club_restriction_type,
       stage_hash_crmcloudsync_campaign.ltf_clubrestrictiontypename ltf_club_restriction_type_name,
       stage_hash_crmcloudsync_campaign.ltf_connectwitham ltf_connect_with_am,
       stage_hash_crmcloudsync_campaign.ltf_connectwithamname ltf_connect_with_am_name,
       stage_hash_crmcloudsync_campaign.ltf_displayonform ltf_display_on_form,
       stage_hash_crmcloudsync_campaign.ltf_displayonformname ltf_display_on_form_name,
       stage_hash_crmcloudsync_campaign.ltf_expirationdays ltf_expiration_days,
       stage_hash_crmcloudsync_campaign.ltf_expirationtype ltf_expiration_type,
       stage_hash_crmcloudsync_campaign.ltf_expirationtypename ltf_expiration_type_name,
       stage_hash_crmcloudsync_campaign.ltf_passdays ltf_pass_days,
       stage_hash_crmcloudsync_campaign.ltf_targetedrecordtype ltf_targeted_record_type,
       stage_hash_crmcloudsync_campaign.ltf_targetedrecordtypename ltf_targeted_record_type_name,
       stage_hash_crmcloudsync_campaign.ltf_userdefineddates ltf_user_defined_dates,
       stage_hash_crmcloudsync_campaign.ltf_userdefineddatesname ltf_user_defined_dates_name,
       stage_hash_crmcloudsync_campaign.message message,
       stage_hash_crmcloudsync_campaign.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_campaign.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_campaign.modifiedon modified_on,
       stage_hash_crmcloudsync_campaign.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_campaign.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_campaign.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_campaign.name name,
       stage_hash_crmcloudsync_campaign.objective objective,
       stage_hash_crmcloudsync_campaign.othercost other_cost,
       stage_hash_crmcloudsync_campaign.othercost_base other_cost_base,
       stage_hash_crmcloudsync_campaign.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_campaign.owneridname owner_id_name,
       stage_hash_crmcloudsync_campaign.owneridtype owner_id_type,
       stage_hash_crmcloudsync_campaign.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_campaign.owningteam owning_team,
       stage_hash_crmcloudsync_campaign.pricelistname price_list_name,
       stage_hash_crmcloudsync_campaign.processid process_id,
       stage_hash_crmcloudsync_campaign.promotioncodename promotion_code_name,
       stage_hash_crmcloudsync_campaign.proposedend proposed_end,
       stage_hash_crmcloudsync_campaign.proposedstart proposed_start,
       stage_hash_crmcloudsync_campaign.stageid stage_id,
       stage_hash_crmcloudsync_campaign.statecode state_code,
       stage_hash_crmcloudsync_campaign.statecodename state_code_name,
       stage_hash_crmcloudsync_campaign.statuscode status_code,
       stage_hash_crmcloudsync_campaign.statuscodename status_code_name,
       stage_hash_crmcloudsync_campaign.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_campaign.totalactualcost total_actual_cost,
       stage_hash_crmcloudsync_campaign.totalactualcost_base total_actual_cost_base,
       stage_hash_crmcloudsync_campaign.totalcampaignactivityactualcost total_campaign_activity_actual_cost,
       stage_hash_crmcloudsync_campaign.totalcampaignactivityactualcost_base total_campaign_activity_actual_cost_base,
       stage_hash_crmcloudsync_campaign.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_campaign.traversedpath traversed_path,
       stage_hash_crmcloudsync_campaign.typecode type_code,
       stage_hash_crmcloudsync_campaign.typecodename type_code_name,
       stage_hash_crmcloudsync_campaign.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_campaign.versionnumber version_number,
       stage_hash_crmcloudsync_campaign.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_campaign.InsertUser insert_user,
       stage_hash_crmcloudsync_campaign.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_campaign.UpdateUser update_user,
       stage_hash_crmcloudsync_campaign.ltf_issuancemethod ltf_issuance_method,
       stage_hash_crmcloudsync_campaign.ltf_qrcodestring ltf_qr_code_string,
       stage_hash_crmcloudsync_campaign.ltf_qrcodeurl ltf_qr_code_url,
       stage_hash_crmcloudsync_campaign.ltf_targetedprospects ltf_targeted_prospects,
       stage_hash_crmcloudsync_campaign.ltf_targetedissuedate ltf_targeted_issue_date,
       stage_hash_crmcloudsync_campaign.ltf_sendid ltf_send_id,
       stage_hash_crmcloudsync_campaign.ltf_memberreferral ltf_member_referral,
       stage_hash_crmcloudsync_campaign.ltf_guestpasstype ltf_guest_pass_type,
       stage_hash_crmcloudsync_campaign.ltf_rewardtype ltf_reward_type,
       stage_hash_crmcloudsync_campaign.ltf_rewardltbucks ltf_reward_lt_bucks,
       stage_hash_crmcloudsync_campaign.ltf_rewardwaitdays ltf_reward_wait_days,
       stage_hash_crmcloudsync_campaign.ltf_rewardclub ltf_reward_club,
       stage_hash_crmcloudsync_campaign.ltf_restrictedbypolicy ltf_restricted_by_policy,
       isnull(cast(stage_hash_crmcloudsync_campaign.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.actualend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.actualstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.budgetedcost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.budgetedcost_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.campaignid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.codename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.entityimage,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.entityimage_timestamp as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.entityimage_url,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.entityimageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.expectedresponse as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.expectedrevenue as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.expectedrevenue_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.istemplate as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.istemplatename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_clubrestrictiontype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_clubrestrictiontypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_connectwitham as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_connectwithamname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_displayonform as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_displayonformname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_expirationdays as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_expirationtype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_expirationtypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_passdays as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_targetedrecordtype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_targetedrecordtypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_userdefineddates as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_userdefineddatesname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.message,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.objective,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.othercost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.othercost_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.pricelistname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.promotioncodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.proposedend,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.proposedstart,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.totalactualcost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.totalactualcost_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.totalcampaignactivityactualcost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.totalcampaignactivityactualcost_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.traversedpath,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.typecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.typecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_issuancemethod as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_qrcodestring,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_qrcodeurl,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_targetedprospects as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_campaign.ltf_targetedissuedate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_sendid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_memberreferral as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_guestpasstype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_rewardtype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_rewardltbucks as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_rewardwaitdays as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_campaign.ltf_rewardclub,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_campaign.ltf_restrictedbypolicy as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_campaign
 where stage_hash_crmcloudsync_campaign.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_campaign records
set @insert_date_time = getdate()
insert into s_crmcloudsync_campaign (
       bk_hash,
       actual_end,
       actual_start,
       budgeted_cost,
       budgeted_cost_base,
       campaign_id,
       code_name,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       description,
       entity_image,
       entity_image_timestamp,
       entity_image_url,
       entity_imageid,
       exchange_rate,
       expected_response,
       expected_revenue,
       expected_revenue_base,
       import_sequence_number,
       is_template,
       is_template_name,
       ltf_club_restriction_type,
       ltf_club_restriction_type_name,
       ltf_connect_with_am,
       ltf_connect_with_am_name,
       ltf_display_on_form,
       ltf_display_on_form_name,
       ltf_expiration_days,
       ltf_expiration_type,
       ltf_expiration_type_name,
       ltf_pass_days,
       ltf_targeted_record_type,
       ltf_targeted_record_type_name,
       ltf_user_defined_dates,
       ltf_user_defined_dates_name,
       message,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       name,
       objective,
       other_cost,
       other_cost_base,
       overridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       owning_team,
       price_list_name,
       process_id,
       promotion_code_name,
       proposed_end,
       proposed_start,
       stage_id,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       time_zone_rule_version_number,
       total_actual_cost,
       total_actual_cost_base,
       total_campaign_activity_actual_cost,
       total_campaign_activity_actual_cost_base,
       transaction_currency_id_name,
       traversed_path,
       type_code,
       type_code_name,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       ltf_issuance_method,
       ltf_qr_code_string,
       ltf_qr_code_url,
       ltf_targeted_prospects,
       ltf_targeted_issue_date,
       ltf_send_id,
       ltf_member_referral,
       ltf_guest_pass_type,
       ltf_reward_type,
       ltf_reward_lt_bucks,
       ltf_reward_wait_days,
       ltf_reward_club,
       ltf_restricted_by_policy,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_campaign_inserts.bk_hash,
       #s_crmcloudsync_campaign_inserts.actual_end,
       #s_crmcloudsync_campaign_inserts.actual_start,
       #s_crmcloudsync_campaign_inserts.budgeted_cost,
       #s_crmcloudsync_campaign_inserts.budgeted_cost_base,
       #s_crmcloudsync_campaign_inserts.campaign_id,
       #s_crmcloudsync_campaign_inserts.code_name,
       #s_crmcloudsync_campaign_inserts.created_by_name,
       #s_crmcloudsync_campaign_inserts.created_by_yomi_name,
       #s_crmcloudsync_campaign_inserts.created_on,
       #s_crmcloudsync_campaign_inserts.created_on_behalf_by,
       #s_crmcloudsync_campaign_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_campaign_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_campaign_inserts.description,
       #s_crmcloudsync_campaign_inserts.entity_image,
       #s_crmcloudsync_campaign_inserts.entity_image_timestamp,
       #s_crmcloudsync_campaign_inserts.entity_image_url,
       #s_crmcloudsync_campaign_inserts.entity_imageid,
       #s_crmcloudsync_campaign_inserts.exchange_rate,
       #s_crmcloudsync_campaign_inserts.expected_response,
       #s_crmcloudsync_campaign_inserts.expected_revenue,
       #s_crmcloudsync_campaign_inserts.expected_revenue_base,
       #s_crmcloudsync_campaign_inserts.import_sequence_number,
       #s_crmcloudsync_campaign_inserts.is_template,
       #s_crmcloudsync_campaign_inserts.is_template_name,
       #s_crmcloudsync_campaign_inserts.ltf_club_restriction_type,
       #s_crmcloudsync_campaign_inserts.ltf_club_restriction_type_name,
       #s_crmcloudsync_campaign_inserts.ltf_connect_with_am,
       #s_crmcloudsync_campaign_inserts.ltf_connect_with_am_name,
       #s_crmcloudsync_campaign_inserts.ltf_display_on_form,
       #s_crmcloudsync_campaign_inserts.ltf_display_on_form_name,
       #s_crmcloudsync_campaign_inserts.ltf_expiration_days,
       #s_crmcloudsync_campaign_inserts.ltf_expiration_type,
       #s_crmcloudsync_campaign_inserts.ltf_expiration_type_name,
       #s_crmcloudsync_campaign_inserts.ltf_pass_days,
       #s_crmcloudsync_campaign_inserts.ltf_targeted_record_type,
       #s_crmcloudsync_campaign_inserts.ltf_targeted_record_type_name,
       #s_crmcloudsync_campaign_inserts.ltf_user_defined_dates,
       #s_crmcloudsync_campaign_inserts.ltf_user_defined_dates_name,
       #s_crmcloudsync_campaign_inserts.message,
       #s_crmcloudsync_campaign_inserts.modified_by_name,
       #s_crmcloudsync_campaign_inserts.modified_by_yomi_name,
       #s_crmcloudsync_campaign_inserts.modified_on,
       #s_crmcloudsync_campaign_inserts.modified_on_behalf_by,
       #s_crmcloudsync_campaign_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_campaign_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_campaign_inserts.name,
       #s_crmcloudsync_campaign_inserts.objective,
       #s_crmcloudsync_campaign_inserts.other_cost,
       #s_crmcloudsync_campaign_inserts.other_cost_base,
       #s_crmcloudsync_campaign_inserts.overridden_created_on,
       #s_crmcloudsync_campaign_inserts.owner_id_name,
       #s_crmcloudsync_campaign_inserts.owner_id_type,
       #s_crmcloudsync_campaign_inserts.owner_id_yomi_name,
       #s_crmcloudsync_campaign_inserts.owning_team,
       #s_crmcloudsync_campaign_inserts.price_list_name,
       #s_crmcloudsync_campaign_inserts.process_id,
       #s_crmcloudsync_campaign_inserts.promotion_code_name,
       #s_crmcloudsync_campaign_inserts.proposed_end,
       #s_crmcloudsync_campaign_inserts.proposed_start,
       #s_crmcloudsync_campaign_inserts.stage_id,
       #s_crmcloudsync_campaign_inserts.state_code,
       #s_crmcloudsync_campaign_inserts.state_code_name,
       #s_crmcloudsync_campaign_inserts.status_code,
       #s_crmcloudsync_campaign_inserts.status_code_name,
       #s_crmcloudsync_campaign_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_campaign_inserts.total_actual_cost,
       #s_crmcloudsync_campaign_inserts.total_actual_cost_base,
       #s_crmcloudsync_campaign_inserts.total_campaign_activity_actual_cost,
       #s_crmcloudsync_campaign_inserts.total_campaign_activity_actual_cost_base,
       #s_crmcloudsync_campaign_inserts.transaction_currency_id_name,
       #s_crmcloudsync_campaign_inserts.traversed_path,
       #s_crmcloudsync_campaign_inserts.type_code,
       #s_crmcloudsync_campaign_inserts.type_code_name,
       #s_crmcloudsync_campaign_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_campaign_inserts.version_number,
       #s_crmcloudsync_campaign_inserts.inserted_date_time,
       #s_crmcloudsync_campaign_inserts.insert_user,
       #s_crmcloudsync_campaign_inserts.updated_date_time,
       #s_crmcloudsync_campaign_inserts.update_user,
       #s_crmcloudsync_campaign_inserts.ltf_issuance_method,
       #s_crmcloudsync_campaign_inserts.ltf_qr_code_string,
       #s_crmcloudsync_campaign_inserts.ltf_qr_code_url,
       #s_crmcloudsync_campaign_inserts.ltf_targeted_prospects,
       #s_crmcloudsync_campaign_inserts.ltf_targeted_issue_date,
       #s_crmcloudsync_campaign_inserts.ltf_send_id,
       #s_crmcloudsync_campaign_inserts.ltf_member_referral,
       #s_crmcloudsync_campaign_inserts.ltf_guest_pass_type,
       #s_crmcloudsync_campaign_inserts.ltf_reward_type,
       #s_crmcloudsync_campaign_inserts.ltf_reward_lt_bucks,
       #s_crmcloudsync_campaign_inserts.ltf_reward_wait_days,
       #s_crmcloudsync_campaign_inserts.ltf_reward_club,
       #s_crmcloudsync_campaign_inserts.ltf_restricted_by_policy,
       case when s_crmcloudsync_campaign.s_crmcloudsync_campaign_id is null then isnull(#s_crmcloudsync_campaign_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_campaign_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_campaign_inserts
  left join p_crmcloudsync_campaign
    on #s_crmcloudsync_campaign_inserts.bk_hash = p_crmcloudsync_campaign.bk_hash
   and p_crmcloudsync_campaign.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_campaign
    on p_crmcloudsync_campaign.bk_hash = s_crmcloudsync_campaign.bk_hash
   and p_crmcloudsync_campaign.s_crmcloudsync_campaign_id = s_crmcloudsync_campaign.s_crmcloudsync_campaign_id
 where s_crmcloudsync_campaign.s_crmcloudsync_campaign_id is null
    or (s_crmcloudsync_campaign.s_crmcloudsync_campaign_id is not null
        and s_crmcloudsync_campaign.dv_hash <> #s_crmcloudsync_campaign_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_campaign @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_campaign @current_dv_batch_id

end
