CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_outreach_sub_segment] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_OutreachSubSegment

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_OutreachSubSegment (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_attributeindex,
       ltf_description,
       ltf_outreachsubsegmentid,
       ltf_subsegment,
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
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_outreachsubsegmentid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_attributeindex,
       ltf_description,
       ltf_outreachsubsegmentid,
       ltf_subsegment,
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
       isnull(cast(stage_crmcloudsync_LTF_OutreachSubSegment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_OutreachSubSegment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_outreach_sub_segment @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_outreach_sub_segment (
       bk_hash,
       ltf_outreach_sub_segment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_OutreachSubSegment.bk_hash,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_outreachsubsegmentid ltf_outreach_sub_segment_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_OutreachSubSegment
  left join h_crmcloudsync_ltf_outreach_sub_segment
    on stage_hash_crmcloudsync_LTF_OutreachSubSegment.bk_hash = h_crmcloudsync_ltf_outreach_sub_segment.bk_hash
 where h_crmcloudsync_ltf_outreach_sub_segment_id is null
   and stage_hash_crmcloudsync_LTF_OutreachSubSegment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_outreach_sub_segment
if object_id('tempdb..#l_crmcloudsync_ltf_outreach_sub_segment_inserts') is not null drop table #l_crmcloudsync_ltf_outreach_sub_segment_inserts
create table #l_crmcloudsync_ltf_outreach_sub_segment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_OutreachSubSegment.bk_hash,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdby created_by,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_outreachsubsegmentid ltf_outreach_sub_segment_id,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.ownerid owner_id,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.owningteam owning_team,
       isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_outreachsubsegmentid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.owningteam,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_OutreachSubSegment
 where stage_hash_crmcloudsync_LTF_OutreachSubSegment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_outreach_sub_segment records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_outreach_sub_segment (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_outreach_sub_segment_id,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_business_unit,
       owning_team,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_outreach_sub_segment_inserts.bk_hash,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.created_by,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.ltf_outreach_sub_segment_id,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.modified_by,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.owner_id,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.owning_team,
       case when l_crmcloudsync_ltf_outreach_sub_segment.l_crmcloudsync_ltf_outreach_sub_segment_id is null then isnull(#l_crmcloudsync_ltf_outreach_sub_segment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_outreach_sub_segment_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_outreach_sub_segment_inserts
  left join p_crmcloudsync_ltf_outreach_sub_segment
    on #l_crmcloudsync_ltf_outreach_sub_segment_inserts.bk_hash = p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
   and p_crmcloudsync_ltf_outreach_sub_segment.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_outreach_sub_segment
    on p_crmcloudsync_ltf_outreach_sub_segment.bk_hash = l_crmcloudsync_ltf_outreach_sub_segment.bk_hash
   and p_crmcloudsync_ltf_outreach_sub_segment.l_crmcloudsync_ltf_outreach_sub_segment_id = l_crmcloudsync_ltf_outreach_sub_segment.l_crmcloudsync_ltf_outreach_sub_segment_id
 where l_crmcloudsync_ltf_outreach_sub_segment.l_crmcloudsync_ltf_outreach_sub_segment_id is null
    or (l_crmcloudsync_ltf_outreach_sub_segment.l_crmcloudsync_ltf_outreach_sub_segment_id is not null
        and l_crmcloudsync_ltf_outreach_sub_segment.dv_hash <> #l_crmcloudsync_ltf_outreach_sub_segment_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_outreach_sub_segment
if object_id('tempdb..#s_crmcloudsync_ltf_outreach_sub_segment_inserts') is not null drop table #s_crmcloudsync_ltf_outreach_sub_segment_inserts
create table #s_crmcloudsync_ltf_outreach_sub_segment_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_OutreachSubSegment.bk_hash,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdon created_on,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_attributeindex ltf_attribute_index,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_description ltf_description,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_outreachsubsegmentid ltf_outreach_sub_segment_id,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_subsegment ltf_subsegment,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.owneridname owner_id_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.owneridtype owner_id_type,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.statecode state_code,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.statuscode status_code,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_OutreachSubSegment.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_attributeindex as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_outreachsubsegmentid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.ltf_subsegment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_OutreachSubSegment.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_OutreachSubSegment.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_OutreachSubSegment.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_OutreachSubSegment.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_OutreachSubSegment.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_OutreachSubSegment
 where stage_hash_crmcloudsync_LTF_OutreachSubSegment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_outreach_sub_segment records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_outreach_sub_segment (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_attribute_index,
       ltf_description,
       ltf_outreach_sub_segment_id,
       ltf_subsegment,
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
select #s_crmcloudsync_ltf_outreach_sub_segment_inserts.bk_hash,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.created_by_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.created_on,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.ltf_attribute_index,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.ltf_description,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.ltf_outreach_sub_segment_id,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.ltf_subsegment,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.modified_by_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.modified_on,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.owner_id_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.owner_id_type,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.state_code,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.state_code_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.status_code,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.status_code_name,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.version_number,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.insert_user,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.updated_date_time,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.update_user,
       case when s_crmcloudsync_ltf_outreach_sub_segment.s_crmcloudsync_ltf_outreach_sub_segment_id is null then isnull(#s_crmcloudsync_ltf_outreach_sub_segment_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_outreach_sub_segment_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_outreach_sub_segment_inserts
  left join p_crmcloudsync_ltf_outreach_sub_segment
    on #s_crmcloudsync_ltf_outreach_sub_segment_inserts.bk_hash = p_crmcloudsync_ltf_outreach_sub_segment.bk_hash
   and p_crmcloudsync_ltf_outreach_sub_segment.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_outreach_sub_segment
    on p_crmcloudsync_ltf_outreach_sub_segment.bk_hash = s_crmcloudsync_ltf_outreach_sub_segment.bk_hash
   and p_crmcloudsync_ltf_outreach_sub_segment.s_crmcloudsync_ltf_outreach_sub_segment_id = s_crmcloudsync_ltf_outreach_sub_segment.s_crmcloudsync_ltf_outreach_sub_segment_id
 where s_crmcloudsync_ltf_outreach_sub_segment.s_crmcloudsync_ltf_outreach_sub_segment_id is null
    or (s_crmcloudsync_ltf_outreach_sub_segment.s_crmcloudsync_ltf_outreach_sub_segment_id is not null
        and s_crmcloudsync_ltf_outreach_sub_segment.dv_hash <> #s_crmcloudsync_ltf_outreach_sub_segment_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_outreach_sub_segment @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_outreach_sub_segment @current_dv_batch_id

end
