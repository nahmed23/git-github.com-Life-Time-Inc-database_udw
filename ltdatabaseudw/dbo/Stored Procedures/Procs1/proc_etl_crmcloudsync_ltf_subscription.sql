CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_subscription] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_Subscription

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_Subscription (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       exchangerate,
       importsequencenumber,
       ltf_accountid,
       ltf_accountidname,
       ltf_accountidyominame,
       ltf_activationdate,
       ltf_cancellationdate,
       ltf_clubid,
       ltf_clubidname,
       ltf_cost,
       ltf_cost_base,
       ltf_customercompanycode,
       ltf_productid,
       ltf_productidname,
       ltf_referringcontactid,
       ltf_referringcontactidname,
       ltf_referringcontactidyominame,
       ltf_referringmemberid,
       ltf_subscriptionid,
       ltf_subscriptionnumber,
       ltf_terminationdate,
       ltf_terminationreason,
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
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_clubportfoliostaffingid,
       ltf_clubportfoliostaffingidname,
       ltf_lthealthreactivationdate,
       ltf_monthlycostofmembership,
       ltf_attritionexclusion,
       ltf_attritionexclusionname,
       ltf_accounthousehold,
       ltf_accounthouseholdname,
       ltf_accounthouseholyomidname,
       ltf_monthlycostofmembership_base,
       ltf_revenueunit,
       ltf_revenueunitname,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_subscriptionid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       exchangerate,
       importsequencenumber,
       ltf_accountid,
       ltf_accountidname,
       ltf_accountidyominame,
       ltf_activationdate,
       ltf_cancellationdate,
       ltf_clubid,
       ltf_clubidname,
       ltf_cost,
       ltf_cost_base,
       ltf_customercompanycode,
       ltf_productid,
       ltf_productidname,
       ltf_referringcontactid,
       ltf_referringcontactidname,
       ltf_referringcontactidyominame,
       ltf_referringmemberid,
       ltf_subscriptionid,
       ltf_subscriptionnumber,
       ltf_terminationdate,
       ltf_terminationreason,
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
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       transactioncurrencyid,
       transactioncurrencyidname,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_clubportfoliostaffingid,
       ltf_clubportfoliostaffingidname,
       ltf_lthealthreactivationdate,
       ltf_monthlycostofmembership,
       ltf_attritionexclusion,
       ltf_attritionexclusionname,
       ltf_accounthousehold,
       ltf_accounthouseholdname,
       ltf_accounthouseholyomidname,
       ltf_monthlycostofmembership_base,
       ltf_revenueunit,
       ltf_revenueunitname,
       isnull(cast(stage_crmcloudsync_LTF_Subscription.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_Subscription
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_subscription @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_subscription (
       bk_hash,
       ltf_subscription_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_Subscription.bk_hash,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_subscriptionid ltf_subscription_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_Subscription
  left join h_crmcloudsync_ltf_subscription
    on stage_hash_crmcloudsync_LTF_Subscription.bk_hash = h_crmcloudsync_ltf_subscription.bk_hash
 where h_crmcloudsync_ltf_subscription_id is null
   and stage_hash_crmcloudsync_LTF_Subscription.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_subscription
if object_id('tempdb..#l_crmcloudsync_ltf_subscription_inserts') is not null drop table #l_crmcloudsync_ltf_subscription_inserts
create table #l_crmcloudsync_ltf_subscription_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Subscription.bk_hash,
       stage_hash_crmcloudsync_LTF_Subscription.createdby created_by,
       stage_hash_crmcloudsync_LTF_Subscription.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_accountid ltf_account_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_productid ltf_product_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_referringcontactid ltf_referring_contact_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_referringmemberid ltf_referring_member_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_subscriptionid ltf_subscription_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_udwid ltf_udw_id,
       stage_hash_crmcloudsync_LTF_Subscription.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_Subscription.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Subscription.ownerid owner_id,
       stage_hash_crmcloudsync_LTF_Subscription.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_LTF_Subscription.owningteam owning_team,
       stage_hash_crmcloudsync_LTF_Subscription.owninguser owning_user,
       stage_hash_crmcloudsync_LTF_Subscription.transactioncurrencyid transaction_currency_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_clubportfoliostaffingid ltf_club_portfolio_staffing_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_accounthousehold ltf_account_household,
       isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_accountid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_productid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_referringcontactid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_referringmemberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_subscriptionid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_udwid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.transactioncurrencyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_clubportfoliostaffingid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_accounthousehold,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Subscription
 where stage_hash_crmcloudsync_LTF_Subscription.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_subscription records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_subscription (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_account_id,
       ltf_club_id,
       ltf_product_id,
       ltf_referring_contact_id,
       ltf_referring_member_id,
       ltf_subscription_id,
       ltf_udw_id,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       transaction_currency_id,
       ltf_club_portfolio_staffing_id,
       ltf_account_household,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_subscription_inserts.bk_hash,
       #l_crmcloudsync_ltf_subscription_inserts.created_by,
       #l_crmcloudsync_ltf_subscription_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_account_id,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_club_id,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_product_id,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_referring_contact_id,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_referring_member_id,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_subscription_id,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_udw_id,
       #l_crmcloudsync_ltf_subscription_inserts.modified_by,
       #l_crmcloudsync_ltf_subscription_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_subscription_inserts.owner_id,
       #l_crmcloudsync_ltf_subscription_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_subscription_inserts.owning_team,
       #l_crmcloudsync_ltf_subscription_inserts.owning_user,
       #l_crmcloudsync_ltf_subscription_inserts.transaction_currency_id,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_club_portfolio_staffing_id,
       #l_crmcloudsync_ltf_subscription_inserts.ltf_account_household,
       case when l_crmcloudsync_ltf_subscription.l_crmcloudsync_ltf_subscription_id is null then isnull(#l_crmcloudsync_ltf_subscription_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_subscription_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_subscription_inserts
  left join p_crmcloudsync_ltf_subscription
    on #l_crmcloudsync_ltf_subscription_inserts.bk_hash = p_crmcloudsync_ltf_subscription.bk_hash
   and p_crmcloudsync_ltf_subscription.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_subscription
    on p_crmcloudsync_ltf_subscription.bk_hash = l_crmcloudsync_ltf_subscription.bk_hash
   and p_crmcloudsync_ltf_subscription.l_crmcloudsync_ltf_subscription_id = l_crmcloudsync_ltf_subscription.l_crmcloudsync_ltf_subscription_id
 where l_crmcloudsync_ltf_subscription.l_crmcloudsync_ltf_subscription_id is null
    or (l_crmcloudsync_ltf_subscription.l_crmcloudsync_ltf_subscription_id is not null
        and l_crmcloudsync_ltf_subscription.dv_hash <> #l_crmcloudsync_ltf_subscription_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_subscription
if object_id('tempdb..#s_crmcloudsync_ltf_subscription_inserts') is not null drop table #s_crmcloudsync_ltf_subscription_inserts
create table #s_crmcloudsync_ltf_subscription_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Subscription.bk_hash,
       stage_hash_crmcloudsync_LTF_Subscription.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_Subscription.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscription.createdon created_on,
       stage_hash_crmcloudsync_LTF_Subscription.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Subscription.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscription.exchangerate exchange_rate,
       stage_hash_crmcloudsync_LTF_Subscription.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_accountidname ltf_account_id_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_accountidyominame ltf_account_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_activationdate ltf_activation_date,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_cancellationdate ltf_cancellation_date,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_clubidname ltf_club_id_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_cost ltf_cost,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_cost_base ltf_cost_base,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_customercompanycode ltf_customer_company_code,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_productidname ltf_product_id_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_referringcontactidname ltf_referring_contact_id_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_referringcontactidyominame ltf_referring_contact_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_subscriptionid ltf_subscription_id,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_subscriptionnumber ltf_subscription_number,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_terminationdate ltf_termination_date,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_terminationreason ltf_termination_reason,
       stage_hash_crmcloudsync_LTF_Subscription.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_Subscription.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscription.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_Subscription.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Subscription.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscription.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_Subscription.owneridname owner_id_name,
       stage_hash_crmcloudsync_LTF_Subscription.owneridtype owner_id_type,
       stage_hash_crmcloudsync_LTF_Subscription.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscription.statecode state_code,
       stage_hash_crmcloudsync_LTF_Subscription.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_Subscription.statuscode status_code,
       stage_hash_crmcloudsync_LTF_Subscription.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_Subscription.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_Subscription.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_LTF_Subscription.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_Subscription.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_Subscription.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_Subscription.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_Subscription.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_Subscription.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_clubportfoliostaffingidname ltf_club_portfolio_staffing_id_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_lthealthreactivationdate ltf_lt_health_reactivation_date,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_monthlycostofmembership ltf_monthly_cost_of_membership,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_attritionexclusion ltf_attrition_exclusion,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_attritionexclusionname ltf_attrition_exclusion_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_accounthouseholdname ltf_account_household_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_accounthouseholyomidname ltf_account_household_yomi_id_name,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_monthlycostofmembership_base ltf_monthly_cost_of_membership_base,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_revenueunit ltf_revenue_unit,
       stage_hash_crmcloudsync_LTF_Subscription.ltf_revenueunitname ltf_revenue_unit_name,
       isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_accountidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_accountidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.ltf_activationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.ltf_cancellationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_clubidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.ltf_cost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.ltf_cost_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_customercompanycode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_productidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_referringcontactidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_referringcontactidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_subscriptionid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_subscriptionnumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.ltf_terminationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_terminationreason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_clubportfoliostaffingidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscription.ltf_lthealthreactivationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.ltf_monthlycostofmembership as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.ltf_attritionexclusion as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_attritionexclusionname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_accounthouseholdname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_accounthouseholyomidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.ltf_monthlycostofmembership_base as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscription.ltf_revenueunit as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscription.ltf_revenueunitname,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Subscription
 where stage_hash_crmcloudsync_LTF_Subscription.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_subscription records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_subscription (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       exchange_rate,
       import_sequence_number,
       ltf_account_id_name,
       ltf_account_id_yomi_name,
       ltf_activation_date,
       ltf_cancellation_date,
       ltf_club_id_name,
       ltf_cost,
       ltf_cost_base,
       ltf_customer_company_code,
       ltf_product_id_name,
       ltf_referring_contact_id_name,
       ltf_referring_contact_id_yomi_name,
       ltf_subscription_id,
       ltf_subscription_number,
       ltf_termination_date,
       ltf_termination_reason,
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
       transaction_currency_id_name,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       ltf_club_portfolio_staffing_id_name,
       ltf_lt_health_reactivation_date,
       ltf_monthly_cost_of_membership,
       ltf_attrition_exclusion,
       ltf_attrition_exclusion_name,
       ltf_account_household_name,
       ltf_account_household_yomi_id_name,
       ltf_monthly_cost_of_membership_base,
       ltf_revenue_unit,
       ltf_revenue_unit_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_subscription_inserts.bk_hash,
       #s_crmcloudsync_ltf_subscription_inserts.created_by_name,
       #s_crmcloudsync_ltf_subscription_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_subscription_inserts.created_on,
       #s_crmcloudsync_ltf_subscription_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_subscription_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_subscription_inserts.exchange_rate,
       #s_crmcloudsync_ltf_subscription_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_account_id_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_account_id_yomi_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_activation_date,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_cancellation_date,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_club_id_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_cost,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_cost_base,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_customer_company_code,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_product_id_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_referring_contact_id_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_referring_contact_id_yomi_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_subscription_id,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_subscription_number,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_termination_date,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_termination_reason,
       #s_crmcloudsync_ltf_subscription_inserts.modified_by_name,
       #s_crmcloudsync_ltf_subscription_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_subscription_inserts.modified_on,
       #s_crmcloudsync_ltf_subscription_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_subscription_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_subscription_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_subscription_inserts.owner_id_name,
       #s_crmcloudsync_ltf_subscription_inserts.owner_id_type,
       #s_crmcloudsync_ltf_subscription_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_subscription_inserts.state_code,
       #s_crmcloudsync_ltf_subscription_inserts.state_code_name,
       #s_crmcloudsync_ltf_subscription_inserts.status_code,
       #s_crmcloudsync_ltf_subscription_inserts.status_code_name,
       #s_crmcloudsync_ltf_subscription_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_subscription_inserts.transaction_currency_id_name,
       #s_crmcloudsync_ltf_subscription_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_subscription_inserts.version_number,
       #s_crmcloudsync_ltf_subscription_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_subscription_inserts.insert_user,
       #s_crmcloudsync_ltf_subscription_inserts.updated_date_time,
       #s_crmcloudsync_ltf_subscription_inserts.update_user,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_club_portfolio_staffing_id_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_lt_health_reactivation_date,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_monthly_cost_of_membership,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_attrition_exclusion,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_attrition_exclusion_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_account_household_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_account_household_yomi_id_name,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_monthly_cost_of_membership_base,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_revenue_unit,
       #s_crmcloudsync_ltf_subscription_inserts.ltf_revenue_unit_name,
       case when s_crmcloudsync_ltf_subscription.s_crmcloudsync_ltf_subscription_id is null then isnull(#s_crmcloudsync_ltf_subscription_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_subscription_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_subscription_inserts
  left join p_crmcloudsync_ltf_subscription
    on #s_crmcloudsync_ltf_subscription_inserts.bk_hash = p_crmcloudsync_ltf_subscription.bk_hash
   and p_crmcloudsync_ltf_subscription.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_subscription
    on p_crmcloudsync_ltf_subscription.bk_hash = s_crmcloudsync_ltf_subscription.bk_hash
   and p_crmcloudsync_ltf_subscription.s_crmcloudsync_ltf_subscription_id = s_crmcloudsync_ltf_subscription.s_crmcloudsync_ltf_subscription_id
 where s_crmcloudsync_ltf_subscription.s_crmcloudsync_ltf_subscription_id is null
    or (s_crmcloudsync_ltf_subscription.s_crmcloudsync_ltf_subscription_id is not null
        and s_crmcloudsync_ltf_subscription.dv_hash <> #s_crmcloudsync_ltf_subscription_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_subscription @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_subscription @current_dv_batch_id

end
