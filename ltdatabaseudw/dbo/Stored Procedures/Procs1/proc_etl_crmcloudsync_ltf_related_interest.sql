CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_related_interest] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_RelatedInterest

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_RelatedInterest (
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
       ltf_interestid,
       ltf_interestidname,
       ltf_leadid,
       ltf_leadidname,
       ltf_leadidyominame,
       ltf_name,
       ltf_opportunityid,
       ltf_opportunityidname,
       ltf_relatedinterestid,
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
       ltf_adddate,
       ltf_addby,
       ltf_addsource,
       ltf_removedate,
       ltf_removeby,
       ltf_removesource,
       ltf_primaryinterest,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_relatedinterestid,'z#@$k%&P'))),2) bk_hash,
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
       ltf_interestid,
       ltf_interestidname,
       ltf_leadid,
       ltf_leadidname,
       ltf_leadidyominame,
       ltf_name,
       ltf_opportunityid,
       ltf_opportunityidname,
       ltf_relatedinterestid,
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
       ltf_adddate,
       ltf_addby,
       ltf_addsource,
       ltf_removedate,
       ltf_removeby,
       ltf_removesource,
       ltf_primaryinterest,
       isnull(cast(stage_crmcloudsync_LTF_RelatedInterest.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_RelatedInterest
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_related_interest @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_related_interest (
       bk_hash,
       ltf_related_interest_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_RelatedInterest.bk_hash,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_relatedinterestid ltf_related_interest_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_RelatedInterest
  left join h_crmcloudsync_ltf_related_interest
    on stage_hash_crmcloudsync_LTF_RelatedInterest.bk_hash = h_crmcloudsync_ltf_related_interest.bk_hash
 where h_crmcloudsync_ltf_related_interest_id is null
   and stage_hash_crmcloudsync_LTF_RelatedInterest.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_related_interest
if object_id('tempdb..#l_crmcloudsync_ltf_related_interest_inserts') is not null drop table #l_crmcloudsync_ltf_related_interest_inserts
create table #l_crmcloudsync_ltf_related_interest_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_RelatedInterest.bk_hash,
       stage_hash_crmcloudsync_LTF_RelatedInterest.createdby created_by,
       stage_hash_crmcloudsync_LTF_RelatedInterest.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_contactid ltf_contact_id,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_interestid ltf_interest_id,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_leadid ltf_lead_id,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_opportunityid ltf_opportunity_id,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_relatedinterestid ltf_related_interest_id,
       stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ownerid owner_id,
       stage_hash_crmcloudsync_LTF_RelatedInterest.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_LTF_RelatedInterest.owningteam owning_team,
       stage_hash_crmcloudsync_LTF_RelatedInterest.owninguser owning_user,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_addby ltf_add_by,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_addsource ltf_add_source,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_removeby ltf_remove_by,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_removesource ltf_remove_source,
       isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_contactid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_interestid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_leadid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_opportunityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_relatedinterestid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_addby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_addsource as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_removeby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_removesource as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_RelatedInterest
 where stage_hash_crmcloudsync_LTF_RelatedInterest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_related_interest records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_related_interest (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_contact_id,
       ltf_interest_id,
       ltf_lead_id,
       ltf_opportunity_id,
       ltf_related_interest_id,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       ltf_add_by,
       ltf_add_source,
       ltf_remove_by,
       ltf_remove_source,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_related_interest_inserts.bk_hash,
       #l_crmcloudsync_ltf_related_interest_inserts.created_by,
       #l_crmcloudsync_ltf_related_interest_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_contact_id,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_interest_id,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_lead_id,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_opportunity_id,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_related_interest_id,
       #l_crmcloudsync_ltf_related_interest_inserts.modified_by,
       #l_crmcloudsync_ltf_related_interest_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_related_interest_inserts.owner_id,
       #l_crmcloudsync_ltf_related_interest_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_related_interest_inserts.owning_team,
       #l_crmcloudsync_ltf_related_interest_inserts.owning_user,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_add_by,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_add_source,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_remove_by,
       #l_crmcloudsync_ltf_related_interest_inserts.ltf_remove_source,
       case when l_crmcloudsync_ltf_related_interest.l_crmcloudsync_ltf_related_interest_id is null then isnull(#l_crmcloudsync_ltf_related_interest_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_related_interest_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_related_interest_inserts
  left join p_crmcloudsync_ltf_related_interest
    on #l_crmcloudsync_ltf_related_interest_inserts.bk_hash = p_crmcloudsync_ltf_related_interest.bk_hash
   and p_crmcloudsync_ltf_related_interest.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_related_interest
    on p_crmcloudsync_ltf_related_interest.bk_hash = l_crmcloudsync_ltf_related_interest.bk_hash
   and p_crmcloudsync_ltf_related_interest.l_crmcloudsync_ltf_related_interest_id = l_crmcloudsync_ltf_related_interest.l_crmcloudsync_ltf_related_interest_id
 where l_crmcloudsync_ltf_related_interest.l_crmcloudsync_ltf_related_interest_id is null
    or (l_crmcloudsync_ltf_related_interest.l_crmcloudsync_ltf_related_interest_id is not null
        and l_crmcloudsync_ltf_related_interest.dv_hash <> #l_crmcloudsync_ltf_related_interest_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_related_interest
if object_id('tempdb..#s_crmcloudsync_ltf_related_interest_inserts') is not null drop table #s_crmcloudsync_ltf_related_interest_inserts
create table #s_crmcloudsync_ltf_related_interest_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_RelatedInterest.bk_hash,
       stage_hash_crmcloudsync_LTF_RelatedInterest.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.createdon created_on,
       stage_hash_crmcloudsync_LTF_RelatedInterest.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_contactidname ltf_contact_id_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_contactidyominame ltf_contact_id_yomi_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_interestidname ltf_interest_id_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_leadidname ltf_lead_id_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_leadidyominame ltf_lead_id_yomi_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_name ltf_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_opportunityidname ltf_opportunity_id_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_relatedinterestid ltf_related_interest_id,
       stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_RelatedInterest.owneridname owner_id_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.owneridtype owner_id_type,
       stage_hash_crmcloudsync_LTF_RelatedInterest.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.statecode state_code,
       stage_hash_crmcloudsync_LTF_RelatedInterest.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.statuscode status_code,
       stage_hash_crmcloudsync_LTF_RelatedInterest.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_RelatedInterest.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_RelatedInterest.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_RelatedInterest.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_RelatedInterest.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_RelatedInterest.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_RelatedInterest.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_RelatedInterest.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_adddate ltf_add_date,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_removedate ltf_remove_date,
       stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_primaryinterest ltf_primary_interest,
       isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_RelatedInterest.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_contactidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_contactidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_interestidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_leadidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_leadidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_opportunityidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_relatedinterestid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_RelatedInterest.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_RelatedInterest.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_RelatedInterest.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_RelatedInterest.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_adddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_removedate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_RelatedInterest.ltf_primaryinterest as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_RelatedInterest
 where stage_hash_crmcloudsync_LTF_RelatedInterest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_related_interest records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_related_interest (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_contact_id_name,
       ltf_contact_id_yomi_name,
       ltf_interest_id_name,
       ltf_lead_id_name,
       ltf_lead_id_yomi_name,
       ltf_name,
       ltf_opportunity_id_name,
       ltf_related_interest_id,
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
       ltf_add_date,
       ltf_remove_date,
       ltf_primary_interest,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_related_interest_inserts.bk_hash,
       #s_crmcloudsync_ltf_related_interest_inserts.created_by_name,
       #s_crmcloudsync_ltf_related_interest_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_related_interest_inserts.created_on,
       #s_crmcloudsync_ltf_related_interest_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_related_interest_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_related_interest_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_contact_id_name,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_contact_id_yomi_name,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_interest_id_name,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_lead_id_name,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_lead_id_yomi_name,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_name,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_opportunity_id_name,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_related_interest_id,
       #s_crmcloudsync_ltf_related_interest_inserts.modified_by_name,
       #s_crmcloudsync_ltf_related_interest_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_related_interest_inserts.modified_on,
       #s_crmcloudsync_ltf_related_interest_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_related_interest_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_related_interest_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_related_interest_inserts.owner_id_name,
       #s_crmcloudsync_ltf_related_interest_inserts.owner_id_type,
       #s_crmcloudsync_ltf_related_interest_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_related_interest_inserts.state_code,
       #s_crmcloudsync_ltf_related_interest_inserts.state_code_name,
       #s_crmcloudsync_ltf_related_interest_inserts.status_code,
       #s_crmcloudsync_ltf_related_interest_inserts.status_code_name,
       #s_crmcloudsync_ltf_related_interest_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_related_interest_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_related_interest_inserts.version_number,
       #s_crmcloudsync_ltf_related_interest_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_related_interest_inserts.insert_user,
       #s_crmcloudsync_ltf_related_interest_inserts.updated_date_time,
       #s_crmcloudsync_ltf_related_interest_inserts.update_user,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_add_date,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_remove_date,
       #s_crmcloudsync_ltf_related_interest_inserts.ltf_primary_interest,
       case when s_crmcloudsync_ltf_related_interest.s_crmcloudsync_ltf_related_interest_id is null then isnull(#s_crmcloudsync_ltf_related_interest_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_related_interest_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_related_interest_inserts
  left join p_crmcloudsync_ltf_related_interest
    on #s_crmcloudsync_ltf_related_interest_inserts.bk_hash = p_crmcloudsync_ltf_related_interest.bk_hash
   and p_crmcloudsync_ltf_related_interest.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_related_interest
    on p_crmcloudsync_ltf_related_interest.bk_hash = s_crmcloudsync_ltf_related_interest.bk_hash
   and p_crmcloudsync_ltf_related_interest.s_crmcloudsync_ltf_related_interest_id = s_crmcloudsync_ltf_related_interest.s_crmcloudsync_ltf_related_interest_id
 where s_crmcloudsync_ltf_related_interest.s_crmcloudsync_ltf_related_interest_id is null
    or (s_crmcloudsync_ltf_related_interest.s_crmcloudsync_ltf_related_interest_id is not null
        and s_crmcloudsync_ltf_related_interest.dv_hash <> #s_crmcloudsync_ltf_related_interest_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_related_interest @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_related_interest @current_dv_batch_id

end
