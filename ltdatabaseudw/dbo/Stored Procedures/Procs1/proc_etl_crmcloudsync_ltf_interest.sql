CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_interest] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_Interest

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_Interest (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_interestid,
       ltf_mmsid,
       ltf_name,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       organizationid,
       organizationidname,
       overriddencreatedon,
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
       ltf_shortlist,
       ltf_interestgroup,
       ltf_clubelement1,
       ltf_clubelement2,
       ltf_clubelement3,
       ltf_juniorsonly,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_interestid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_interestid,
       ltf_mmsid,
       ltf_name,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       organizationid,
       organizationidname,
       overriddencreatedon,
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
       ltf_shortlist,
       ltf_interestgroup,
       ltf_clubelement1,
       ltf_clubelement2,
       ltf_clubelement3,
       ltf_juniorsonly,
       isnull(cast(stage_crmcloudsync_LTF_Interest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_Interest
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_interest @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_interest (
       bk_hash,
       ltf_interest_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_Interest.bk_hash,
       stage_hash_crmcloudsync_LTF_Interest.ltf_interestid ltf_interest_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_Interest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_Interest
  left join h_crmcloudsync_ltf_interest
    on stage_hash_crmcloudsync_LTF_Interest.bk_hash = h_crmcloudsync_ltf_interest.bk_hash
 where h_crmcloudsync_ltf_interest_id is null
   and stage_hash_crmcloudsync_LTF_Interest.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_interest
if object_id('tempdb..#l_crmcloudsync_ltf_interest_inserts') is not null drop table #l_crmcloudsync_ltf_interest_inserts
create table #l_crmcloudsync_ltf_interest_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Interest.bk_hash,
       stage_hash_crmcloudsync_LTF_Interest.createdby created_by,
       stage_hash_crmcloudsync_LTF_Interest.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Interest.ltf_interestid ltf_interest_id,
       stage_hash_crmcloudsync_LTF_Interest.ltf_mmsid ltf_mms_id,
       stage_hash_crmcloudsync_LTF_Interest.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_Interest.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Interest.organizationid organization_id,
       stage_hash_crmcloudsync_LTF_Interest.statecode state_code,
       stage_hash_crmcloudsync_LTF_Interest.statuscode status_code,
       stage_hash_crmcloudsync_LTF_Interest.versionnumber version_number,
       isnull(cast(stage_hash_crmcloudsync_LTF_Interest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.ltf_interestid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.ltf_mmsid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.organizationid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.versionnumber as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Interest
 where stage_hash_crmcloudsync_LTF_Interest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_interest records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_interest (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_interest_id,
       ltf_mms_id,
       modified_by,
       modified_on_behalf_by,
       organization_id,
       state_code,
       status_code,
       version_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_interest_inserts.bk_hash,
       #l_crmcloudsync_ltf_interest_inserts.created_by,
       #l_crmcloudsync_ltf_interest_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_interest_inserts.ltf_interest_id,
       #l_crmcloudsync_ltf_interest_inserts.ltf_mms_id,
       #l_crmcloudsync_ltf_interest_inserts.modified_by,
       #l_crmcloudsync_ltf_interest_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_interest_inserts.organization_id,
       #l_crmcloudsync_ltf_interest_inserts.state_code,
       #l_crmcloudsync_ltf_interest_inserts.status_code,
       #l_crmcloudsync_ltf_interest_inserts.version_number,
       case when l_crmcloudsync_ltf_interest.l_crmcloudsync_ltf_interest_id is null then isnull(#l_crmcloudsync_ltf_interest_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_interest_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_interest_inserts
  left join p_crmcloudsync_ltf_interest
    on #l_crmcloudsync_ltf_interest_inserts.bk_hash = p_crmcloudsync_ltf_interest.bk_hash
   and p_crmcloudsync_ltf_interest.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_interest
    on p_crmcloudsync_ltf_interest.bk_hash = l_crmcloudsync_ltf_interest.bk_hash
   and p_crmcloudsync_ltf_interest.l_crmcloudsync_ltf_interest_id = l_crmcloudsync_ltf_interest.l_crmcloudsync_ltf_interest_id
 where l_crmcloudsync_ltf_interest.l_crmcloudsync_ltf_interest_id is null
    or (l_crmcloudsync_ltf_interest.l_crmcloudsync_ltf_interest_id is not null
        and l_crmcloudsync_ltf_interest.dv_hash <> #l_crmcloudsync_ltf_interest_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_interest
if object_id('tempdb..#s_crmcloudsync_ltf_interest_inserts') is not null drop table #s_crmcloudsync_ltf_interest_inserts
create table #s_crmcloudsync_ltf_interest_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Interest.bk_hash,
       stage_hash_crmcloudsync_LTF_Interest.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_Interest.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Interest.createdon created_on,
       stage_hash_crmcloudsync_LTF_Interest.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Interest.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Interest.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_Interest.ltf_interestid ltf_interest_id,
       stage_hash_crmcloudsync_LTF_Interest.ltf_name ltf_name,
       stage_hash_crmcloudsync_LTF_Interest.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_Interest.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Interest.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_Interest.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Interest.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Interest.organizationidname organization_id_name,
       stage_hash_crmcloudsync_LTF_Interest.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_Interest.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_Interest.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_Interest.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_Interest.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_Interest.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_Interest.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_Interest.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_Interest.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_Interest.ltf_shortlist ltf_shortlist,
       stage_hash_crmcloudsync_LTF_Interest.ltf_interestgroup ltf_interest_group,
       stage_hash_crmcloudsync_LTF_Interest.ltf_clubelement1 ltf_club_element_1,
       stage_hash_crmcloudsync_LTF_Interest.ltf_clubelement2 ltf_club_element_2,
       stage_hash_crmcloudsync_LTF_Interest.ltf_clubelement3 ltf_club_element_3,
       stage_hash_crmcloudsync_LTF_Interest.ltf_juniorsonly ltf_juniors_only,
       isnull(cast(stage_hash_crmcloudsync_LTF_Interest.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Interest.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.ltf_interestid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.ltf_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Interest.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.organizationidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Interest.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Interest.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Interest.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.ltf_shortlist as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.ltf_interestgroup as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.ltf_clubelement1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.ltf_clubelement2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Interest.ltf_clubelement3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Interest.ltf_juniorsonly as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Interest
 where stage_hash_crmcloudsync_LTF_Interest.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_interest records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_interest (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_interest_id,
       ltf_name,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       organization_id_name,
       overridden_created_on,
       state_code_name,
       status_code_name,
       time_zone_rule_version_number,
       utc_conversion_time_zone_code,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       ltf_shortlist,
       ltf_interest_group,
       ltf_club_element_1,
       ltf_club_element_2,
       ltf_club_element_3,
       ltf_juniors_only,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_interest_inserts.bk_hash,
       #s_crmcloudsync_ltf_interest_inserts.created_by_name,
       #s_crmcloudsync_ltf_interest_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_interest_inserts.created_on,
       #s_crmcloudsync_ltf_interest_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_interest_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_interest_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_interest_inserts.ltf_interest_id,
       #s_crmcloudsync_ltf_interest_inserts.ltf_name,
       #s_crmcloudsync_ltf_interest_inserts.modified_by_name,
       #s_crmcloudsync_ltf_interest_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_interest_inserts.modified_on,
       #s_crmcloudsync_ltf_interest_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_interest_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_interest_inserts.organization_id_name,
       #s_crmcloudsync_ltf_interest_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_interest_inserts.state_code_name,
       #s_crmcloudsync_ltf_interest_inserts.status_code_name,
       #s_crmcloudsync_ltf_interest_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_interest_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_interest_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_interest_inserts.insert_user,
       #s_crmcloudsync_ltf_interest_inserts.updated_date_time,
       #s_crmcloudsync_ltf_interest_inserts.update_user,
       #s_crmcloudsync_ltf_interest_inserts.ltf_shortlist,
       #s_crmcloudsync_ltf_interest_inserts.ltf_interest_group,
       #s_crmcloudsync_ltf_interest_inserts.ltf_club_element_1,
       #s_crmcloudsync_ltf_interest_inserts.ltf_club_element_2,
       #s_crmcloudsync_ltf_interest_inserts.ltf_club_element_3,
       #s_crmcloudsync_ltf_interest_inserts.ltf_juniors_only,
       case when s_crmcloudsync_ltf_interest.s_crmcloudsync_ltf_interest_id is null then isnull(#s_crmcloudsync_ltf_interest_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_interest_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_interest_inserts
  left join p_crmcloudsync_ltf_interest
    on #s_crmcloudsync_ltf_interest_inserts.bk_hash = p_crmcloudsync_ltf_interest.bk_hash
   and p_crmcloudsync_ltf_interest.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_interest
    on p_crmcloudsync_ltf_interest.bk_hash = s_crmcloudsync_ltf_interest.bk_hash
   and p_crmcloudsync_ltf_interest.s_crmcloudsync_ltf_interest_id = s_crmcloudsync_ltf_interest.s_crmcloudsync_ltf_interest_id
 where s_crmcloudsync_ltf_interest.s_crmcloudsync_ltf_interest_id is null
    or (s_crmcloudsync_ltf_interest.s_crmcloudsync_ltf_interest_id is not null
        and s_crmcloudsync_ltf_interest.dv_hash <> #s_crmcloudsync_ltf_interest_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_interest @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_interest @current_dv_batch_id

end
