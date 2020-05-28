CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_connect_member] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_ConnectMember

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_ConnectMember (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_connectmemberid,
       ltf_connectnotes,
       ltf_linkdescription,
       ltf_moveitscheduleddate,
       ltf_moveitscheduledwith,
       ltf_opportunityid,
       ltf_opportunityidname,
       ltf_profilenotes,
       ltf_programsofinterest,
       ltf_programsofinterestname,
       ltf_subscriberid,
       ltf_subscriberidname,
       ltf_wanttodo,
       ltf_wanttodoname,
       ltf_whometwith,
       ltf_whywanttodo,
       ltf_whywanttodoname,
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
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_connectmemberid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_connectmemberid,
       ltf_connectnotes,
       ltf_linkdescription,
       ltf_moveitscheduleddate,
       ltf_moveitscheduledwith,
       ltf_opportunityid,
       ltf_opportunityidname,
       ltf_profilenotes,
       ltf_programsofinterest,
       ltf_programsofinterestname,
       ltf_subscriberid,
       ltf_subscriberidname,
       ltf_wanttodo,
       ltf_wanttodoname,
       ltf_whometwith,
       ltf_whywanttodo,
       ltf_whywanttodoname,
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
       isnull(cast(stage_crmcloudsync_LTF_ConnectMember.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_ConnectMember
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_connect_member @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_connect_member (
       bk_hash,
       ltf_connect_member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_ConnectMember.bk_hash,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_connectmemberid ltf_connect_member_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_ConnectMember
  left join h_crmcloudsync_ltf_connect_member
    on stage_hash_crmcloudsync_LTF_ConnectMember.bk_hash = h_crmcloudsync_ltf_connect_member.bk_hash
 where h_crmcloudsync_ltf_connect_member_id is null
   and stage_hash_crmcloudsync_LTF_ConnectMember.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_connect_member
if object_id('tempdb..#l_crmcloudsync_ltf_connect_member_inserts') is not null drop table #l_crmcloudsync_ltf_connect_member_inserts
create table #l_crmcloudsync_ltf_connect_member_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_ConnectMember.bk_hash,
       stage_hash_crmcloudsync_LTF_ConnectMember.createdby created_by,
       stage_hash_crmcloudsync_LTF_ConnectMember.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_connectmemberid ltf_connect_member_id,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_opportunityid ltf_opportunity_id,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_subscriberid ltf_subscriber_id,
       stage_hash_crmcloudsync_LTF_ConnectMember.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_ConnectMember.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_ConnectMember.ownerid owner_id,
       stage_hash_crmcloudsync_LTF_ConnectMember.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_LTF_ConnectMember.owningteam owning_team,
       stage_hash_crmcloudsync_LTF_ConnectMember.owninguser owning_user,
       isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_connectmemberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_opportunityid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_subscriberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.owninguser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_ConnectMember
 where stage_hash_crmcloudsync_LTF_ConnectMember.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_connect_member records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_connect_member (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_connect_member_id,
       ltf_opportunity_id,
       ltf_subscriber_id,
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
select #l_crmcloudsync_ltf_connect_member_inserts.bk_hash,
       #l_crmcloudsync_ltf_connect_member_inserts.created_by,
       #l_crmcloudsync_ltf_connect_member_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_connect_member_inserts.ltf_connect_member_id,
       #l_crmcloudsync_ltf_connect_member_inserts.ltf_opportunity_id,
       #l_crmcloudsync_ltf_connect_member_inserts.ltf_subscriber_id,
       #l_crmcloudsync_ltf_connect_member_inserts.modified_by,
       #l_crmcloudsync_ltf_connect_member_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_connect_member_inserts.owner_id,
       #l_crmcloudsync_ltf_connect_member_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_connect_member_inserts.owning_team,
       #l_crmcloudsync_ltf_connect_member_inserts.owning_user,
       case when l_crmcloudsync_ltf_connect_member.l_crmcloudsync_ltf_connect_member_id is null then isnull(#l_crmcloudsync_ltf_connect_member_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_connect_member_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_connect_member_inserts
  left join p_crmcloudsync_ltf_connect_member
    on #l_crmcloudsync_ltf_connect_member_inserts.bk_hash = p_crmcloudsync_ltf_connect_member.bk_hash
   and p_crmcloudsync_ltf_connect_member.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_connect_member
    on p_crmcloudsync_ltf_connect_member.bk_hash = l_crmcloudsync_ltf_connect_member.bk_hash
   and p_crmcloudsync_ltf_connect_member.l_crmcloudsync_ltf_connect_member_id = l_crmcloudsync_ltf_connect_member.l_crmcloudsync_ltf_connect_member_id
 where l_crmcloudsync_ltf_connect_member.l_crmcloudsync_ltf_connect_member_id is null
    or (l_crmcloudsync_ltf_connect_member.l_crmcloudsync_ltf_connect_member_id is not null
        and l_crmcloudsync_ltf_connect_member.dv_hash <> #l_crmcloudsync_ltf_connect_member_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_connect_member
if object_id('tempdb..#s_crmcloudsync_ltf_connect_member_inserts') is not null drop table #s_crmcloudsync_ltf_connect_member_inserts
create table #s_crmcloudsync_ltf_connect_member_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_ConnectMember.bk_hash,
       stage_hash_crmcloudsync_LTF_ConnectMember.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.createdon created_on,
       stage_hash_crmcloudsync_LTF_ConnectMember.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_connectmemberid ltf_connect_member_id,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_connectnotes ltf_connect_notes,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_linkdescription ltf_link_description,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_moveitscheduleddate ltf_move_it_scheduled_date,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_moveitscheduledwith ltf_move_it_scheduled_with,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_opportunityidname ltf_opportunity_id_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_profilenotes ltf_profile_notes,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_programsofinterest ltf_programs_of_interest,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_programsofinterestname ltf_programs_of_interest_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_subscriberidname ltf_subscriber_id_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_wanttodo ltf_want_to_do,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_wanttodoname ltf_want_to_do_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_whometwith ltf_who_met_with,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_whywanttodo ltf_why_want_to_do,
       stage_hash_crmcloudsync_LTF_ConnectMember.ltf_whywanttodoname ltf_why_want_to_do_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_ConnectMember.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_ConnectMember.owneridname owner_id_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.owneridtype owner_id_type,
       stage_hash_crmcloudsync_LTF_ConnectMember.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.statecode state_code,
       stage_hash_crmcloudsync_LTF_ConnectMember.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.statuscode status_code,
       stage_hash_crmcloudsync_LTF_ConnectMember.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_ConnectMember.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_ConnectMember.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_ConnectMember.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_ConnectMember.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_ConnectMember.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_ConnectMember.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_ConnectMember.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ConnectMember.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_connectmemberid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_connectnotes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_linkdescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ConnectMember.ltf_moveitscheduleddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_moveitscheduledwith,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_opportunityidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_profilenotes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_programsofinterest as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_programsofinterestname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_subscriberidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_wanttodo as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_wanttodoname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_whometwith,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_whywanttodo as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.ltf_whywanttodoname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ConnectMember.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ConnectMember.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ConnectMember.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ConnectMember.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ConnectMember.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ConnectMember.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_ConnectMember
 where stage_hash_crmcloudsync_LTF_ConnectMember.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_connect_member records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_connect_member (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_connect_member_id,
       ltf_connect_notes,
       ltf_link_description,
       ltf_move_it_scheduled_date,
       ltf_move_it_scheduled_with,
       ltf_opportunity_id_name,
       ltf_profile_notes,
       ltf_programs_of_interest,
       ltf_programs_of_interest_name,
       ltf_subscriber_id_name,
       ltf_want_to_do,
       ltf_want_to_do_name,
       ltf_who_met_with,
       ltf_why_want_to_do,
       ltf_why_want_to_do_name,
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
select #s_crmcloudsync_ltf_connect_member_inserts.bk_hash,
       #s_crmcloudsync_ltf_connect_member_inserts.created_by_name,
       #s_crmcloudsync_ltf_connect_member_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_connect_member_inserts.created_on,
       #s_crmcloudsync_ltf_connect_member_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_connect_member_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_connect_member_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_connect_member_id,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_connect_notes,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_link_description,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_move_it_scheduled_date,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_move_it_scheduled_with,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_opportunity_id_name,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_profile_notes,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_programs_of_interest,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_programs_of_interest_name,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_subscriber_id_name,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_want_to_do,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_want_to_do_name,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_who_met_with,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_why_want_to_do,
       #s_crmcloudsync_ltf_connect_member_inserts.ltf_why_want_to_do_name,
       #s_crmcloudsync_ltf_connect_member_inserts.modified_by_name,
       #s_crmcloudsync_ltf_connect_member_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_connect_member_inserts.modified_on,
       #s_crmcloudsync_ltf_connect_member_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_connect_member_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_connect_member_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_connect_member_inserts.owner_id_name,
       #s_crmcloudsync_ltf_connect_member_inserts.owner_id_type,
       #s_crmcloudsync_ltf_connect_member_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_connect_member_inserts.state_code,
       #s_crmcloudsync_ltf_connect_member_inserts.state_code_name,
       #s_crmcloudsync_ltf_connect_member_inserts.status_code,
       #s_crmcloudsync_ltf_connect_member_inserts.status_code_name,
       #s_crmcloudsync_ltf_connect_member_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_connect_member_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_connect_member_inserts.version_number,
       #s_crmcloudsync_ltf_connect_member_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_connect_member_inserts.insert_user,
       #s_crmcloudsync_ltf_connect_member_inserts.updated_date_time,
       #s_crmcloudsync_ltf_connect_member_inserts.update_user,
       case when s_crmcloudsync_ltf_connect_member.s_crmcloudsync_ltf_connect_member_id is null then isnull(#s_crmcloudsync_ltf_connect_member_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_connect_member_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_connect_member_inserts
  left join p_crmcloudsync_ltf_connect_member
    on #s_crmcloudsync_ltf_connect_member_inserts.bk_hash = p_crmcloudsync_ltf_connect_member.bk_hash
   and p_crmcloudsync_ltf_connect_member.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_connect_member
    on p_crmcloudsync_ltf_connect_member.bk_hash = s_crmcloudsync_ltf_connect_member.bk_hash
   and p_crmcloudsync_ltf_connect_member.s_crmcloudsync_ltf_connect_member_id = s_crmcloudsync_ltf_connect_member.s_crmcloudsync_ltf_connect_member_id
 where s_crmcloudsync_ltf_connect_member.s_crmcloudsync_ltf_connect_member_id is null
    or (s_crmcloudsync_ltf_connect_member.s_crmcloudsync_ltf_connect_member_id is not null
        and s_crmcloudsync_ltf_connect_member.dv_hash <> #s_crmcloudsync_ltf_connect_member_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_connect_member @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_connect_member @current_dv_batch_id

end
