CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_subscriber] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_Subscriber

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_Subscriber (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_contactid,
       ltf_contactidname,
       ltf_contactidyominame,
       ltf_joindate,
       ltf_name,
       ltf_role,
       ltf_rolename,
       ltf_subscriberid,
       ltf_subscriptionid,
       ltf_subscriptionidname,
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
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_originatingopportunityid,
       ltf_originatingopportunityidname,
       ltf_connectionpreference,
       ltf_connectionprefsource,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_subscriberid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_contactid,
       ltf_contactidname,
       ltf_contactidyominame,
       ltf_joindate,
       ltf_name,
       ltf_role,
       ltf_rolename,
       ltf_subscriberid,
       ltf_subscriptionid,
       ltf_subscriptionidname,
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
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_originatingopportunityid,
       ltf_originatingopportunityidname,
       ltf_connectionpreference,
       ltf_connectionprefsource,
       isnull(cast(stage_crmcloudsync_LTF_Subscriber.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_Subscriber
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_subscriber @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_subscriber (
       bk_hash,
       ltf_subscriber_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_Subscriber.bk_hash,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriberid ltf_subscriber_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_Subscriber
  left join h_crmcloudsync_ltf_subscriber
    on stage_hash_crmcloudsync_LTF_Subscriber.bk_hash = h_crmcloudsync_ltf_subscriber.bk_hash
 where h_crmcloudsync_ltf_subscriber_id is null
   and stage_hash_crmcloudsync_LTF_Subscriber.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_subscriber
if object_id('tempdb..#l_crmcloudsync_ltf_subscriber_inserts') is not null drop table #l_crmcloudsync_ltf_subscriber_inserts
create table #l_crmcloudsync_ltf_subscriber_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Subscriber.bk_hash,
       stage_hash_crmcloudsync_LTF_Subscriber.createdby created_by,
       stage_hash_crmcloudsync_LTF_Subscriber.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_contactid ltf_contact_id,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriberid ltf_subscriber_id,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriptionid ltf_subscription_id,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_udwid ltf_udw_id,
       stage_hash_crmcloudsync_LTF_Subscriber.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_Subscriber.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Subscriber.ownerid owner_id,
       stage_hash_crmcloudsync_LTF_Subscriber.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_LTF_Subscriber.owningteam owning_team,
       stage_hash_crmcloudsync_LTF_Subscriber.owninguser owning_user,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_originatingopportunityid ltf_originating_opportunity_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_contactid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriptionid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_udwid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_originatingopportunityid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Subscriber
 where stage_hash_crmcloudsync_LTF_Subscriber.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_subscriber records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_subscriber (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_contact_id,
       ltf_subscriber_id,
       ltf_subscription_id,
       ltf_udw_id,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       ltf_originating_opportunity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_subscriber_inserts.bk_hash,
       #l_crmcloudsync_ltf_subscriber_inserts.created_by,
       #l_crmcloudsync_ltf_subscriber_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_subscriber_inserts.ltf_contact_id,
       #l_crmcloudsync_ltf_subscriber_inserts.ltf_subscriber_id,
       #l_crmcloudsync_ltf_subscriber_inserts.ltf_subscription_id,
       #l_crmcloudsync_ltf_subscriber_inserts.ltf_udw_id,
       #l_crmcloudsync_ltf_subscriber_inserts.modified_by,
       #l_crmcloudsync_ltf_subscriber_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_subscriber_inserts.owner_id,
       #l_crmcloudsync_ltf_subscriber_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_subscriber_inserts.owning_team,
       #l_crmcloudsync_ltf_subscriber_inserts.owning_user,
       #l_crmcloudsync_ltf_subscriber_inserts.ltf_originating_opportunity_id,
       case when l_crmcloudsync_ltf_subscriber.l_crmcloudsync_ltf_subscriber_id is null then isnull(#l_crmcloudsync_ltf_subscriber_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_subscriber_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_subscriber_inserts
  left join p_crmcloudsync_ltf_subscriber
    on #l_crmcloudsync_ltf_subscriber_inserts.bk_hash = p_crmcloudsync_ltf_subscriber.bk_hash
   and p_crmcloudsync_ltf_subscriber.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_subscriber
    on p_crmcloudsync_ltf_subscriber.bk_hash = l_crmcloudsync_ltf_subscriber.bk_hash
   and p_crmcloudsync_ltf_subscriber.l_crmcloudsync_ltf_subscriber_id = l_crmcloudsync_ltf_subscriber.l_crmcloudsync_ltf_subscriber_id
 where l_crmcloudsync_ltf_subscriber.l_crmcloudsync_ltf_subscriber_id is null
    or (l_crmcloudsync_ltf_subscriber.l_crmcloudsync_ltf_subscriber_id is not null
        and l_crmcloudsync_ltf_subscriber.dv_hash <> #l_crmcloudsync_ltf_subscriber_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_subscriber
if object_id('tempdb..#s_crmcloudsync_ltf_subscriber_inserts') is not null drop table #s_crmcloudsync_ltf_subscriber_inserts
create table #s_crmcloudsync_ltf_subscriber_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Subscriber.bk_hash,
       stage_hash_crmcloudsync_LTF_Subscriber.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_Subscriber.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscriber.createdon created_on,
       stage_hash_crmcloudsync_LTF_Subscriber.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Subscriber.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscriber.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_contactidname ltf_contact_id_name,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_contactidyominame ltf_contact_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_joindate ltf_join_date,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_name ltf_name,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_role ltf_role,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_rolename ltf_role_name,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriberid ltf_subscriber_id,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriptionidname ltf_subscription_id_name,
       stage_hash_crmcloudsync_LTF_Subscriber.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_Subscriber.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscriber.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_Subscriber.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Subscriber.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscriber.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_Subscriber.owneridname owner_id_name,
       stage_hash_crmcloudsync_LTF_Subscriber.owneridtype owner_id_type,
       stage_hash_crmcloudsync_LTF_Subscriber.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Subscriber.statecode state_code,
       stage_hash_crmcloudsync_LTF_Subscriber.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_Subscriber.statuscode status_code,
       stage_hash_crmcloudsync_LTF_Subscriber.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_Subscriber.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_Subscriber.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_Subscriber.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_Subscriber.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_Subscriber.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_Subscriber.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_Subscriber.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_originatingopportunityidname ltf_originating_opportunity_id_name,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_connectionpreference ltf_connection_preference,
       stage_hash_crmcloudsync_LTF_Subscriber.ltf_connectionprefsource ltf_connection_pref_source,
       isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscriber.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_contactidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_contactidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscriber.ltf_joindate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.ltf_role as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_rolename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_subscriptionidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscriber.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscriber.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscriber.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Subscriber.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Subscriber.ltf_originatingopportunityidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.ltf_connectionpreference as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Subscriber.ltf_connectionprefsource as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Subscriber
 where stage_hash_crmcloudsync_LTF_Subscriber.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_subscriber records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_subscriber (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_contact_id_name,
       ltf_contact_id_yomi_name,
       ltf_join_date,
       ltf_name,
       ltf_role,
       ltf_role_name,
       ltf_subscriber_id,
       ltf_subscription_id_name,
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
       ltf_originating_opportunity_id_name,
       ltf_connection_preference,
       ltf_connection_pref_source,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_subscriber_inserts.bk_hash,
       #s_crmcloudsync_ltf_subscriber_inserts.created_by_name,
       #s_crmcloudsync_ltf_subscriber_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_subscriber_inserts.created_on,
       #s_crmcloudsync_ltf_subscriber_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_subscriber_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_subscriber_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_contact_id_name,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_contact_id_yomi_name,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_join_date,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_name,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_role,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_role_name,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_subscriber_id,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_subscription_id_name,
       #s_crmcloudsync_ltf_subscriber_inserts.modified_by_name,
       #s_crmcloudsync_ltf_subscriber_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_subscriber_inserts.modified_on,
       #s_crmcloudsync_ltf_subscriber_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_subscriber_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_subscriber_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_subscriber_inserts.owner_id_name,
       #s_crmcloudsync_ltf_subscriber_inserts.owner_id_type,
       #s_crmcloudsync_ltf_subscriber_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_subscriber_inserts.state_code,
       #s_crmcloudsync_ltf_subscriber_inserts.state_code_name,
       #s_crmcloudsync_ltf_subscriber_inserts.status_code,
       #s_crmcloudsync_ltf_subscriber_inserts.status_code_name,
       #s_crmcloudsync_ltf_subscriber_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_subscriber_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_subscriber_inserts.version_number,
       #s_crmcloudsync_ltf_subscriber_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_subscriber_inserts.insert_user,
       #s_crmcloudsync_ltf_subscriber_inserts.updated_date_time,
       #s_crmcloudsync_ltf_subscriber_inserts.update_user,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_originating_opportunity_id_name,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_connection_preference,
       #s_crmcloudsync_ltf_subscriber_inserts.ltf_connection_pref_source,
       case when s_crmcloudsync_ltf_subscriber.s_crmcloudsync_ltf_subscriber_id is null then isnull(#s_crmcloudsync_ltf_subscriber_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_subscriber_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_subscriber_inserts
  left join p_crmcloudsync_ltf_subscriber
    on #s_crmcloudsync_ltf_subscriber_inserts.bk_hash = p_crmcloudsync_ltf_subscriber.bk_hash
   and p_crmcloudsync_ltf_subscriber.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_subscriber
    on p_crmcloudsync_ltf_subscriber.bk_hash = s_crmcloudsync_ltf_subscriber.bk_hash
   and p_crmcloudsync_ltf_subscriber.s_crmcloudsync_ltf_subscriber_id = s_crmcloudsync_ltf_subscriber.s_crmcloudsync_ltf_subscriber_id
 where s_crmcloudsync_ltf_subscriber.s_crmcloudsync_ltf_subscriber_id is null
    or (s_crmcloudsync_ltf_subscriber.s_crmcloudsync_ltf_subscriber_id is not null
        and s_crmcloudsync_ltf_subscriber.dv_hash <> #s_crmcloudsync_ltf_subscriber_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_subscriber @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_subscriber @current_dv_batch_id

end
