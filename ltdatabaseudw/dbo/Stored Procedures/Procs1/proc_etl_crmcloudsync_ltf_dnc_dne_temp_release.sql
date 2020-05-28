CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_dnc_dne_temp_release] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_DNCDNETempRelease

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_DNCDNETempRelease (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_dncdnetempreleaseid,
       ltf_expirationdate,
       ltf_value,
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
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_dncdnetempreleaseid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_dncdnetempreleaseid,
       ltf_expirationdate,
       ltf_value,
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
       isnull(cast(stage_crmcloudsync_LTF_DNCDNETempRelease.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_crmcloudsync_LTF_DNCDNETempRelease
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_dnc_dne_temp_release @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_dnc_dne_temp_release (
       bk_hash,
       ltf_dnc_dne_temp_release_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_crmcloudsync_LTF_DNCDNETempRelease.bk_hash,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_dncdnetempreleaseid ltf_dnc_dne_temp_release_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_DNCDNETempRelease
  left join h_crmcloudsync_ltf_dnc_dne_temp_release
    on stage_hash_crmcloudsync_LTF_DNCDNETempRelease.bk_hash = h_crmcloudsync_ltf_dnc_dne_temp_release.bk_hash
 where h_crmcloudsync_ltf_dnc_dne_temp_release_id is null
   and stage_hash_crmcloudsync_LTF_DNCDNETempRelease.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_dnc_dne_temp_release
if object_id('tempdb..#l_crmcloudsync_ltf_dnc_dne_temp_release_inserts') is not null drop table #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts
create table #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_DNCDNETempRelease.bk_hash,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdby created_by,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_dncdnetempreleaseid ltf_dnc_dne_temp_release_id,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.organizationid organization_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdby,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdonbehalfby,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_dncdnetempreleaseid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedby,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedonbehalfby,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.organizationid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_DNCDNETempRelease
 where stage_hash_crmcloudsync_LTF_DNCDNETempRelease.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_dnc_dne_temp_release records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_dnc_dne_temp_release (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_dnc_dne_temp_release_id,
       modified_by,
       modified_on_behalf_by,
       organization_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.bk_hash,
       #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.created_by,
       #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.ltf_dnc_dne_temp_release_id,
       #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.modified_by,
       #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.organization_id,
       case when l_crmcloudsync_ltf_dnc_dne_temp_release.l_crmcloudsync_ltf_dnc_dne_temp_release_id is null then isnull(#l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts
  left join p_crmcloudsync_ltf_dnc_dne_temp_release
    on #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.bk_hash = p_crmcloudsync_ltf_dnc_dne_temp_release.bk_hash
   and p_crmcloudsync_ltf_dnc_dne_temp_release.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_dnc_dne_temp_release
    on p_crmcloudsync_ltf_dnc_dne_temp_release.bk_hash = l_crmcloudsync_ltf_dnc_dne_temp_release.bk_hash
   and p_crmcloudsync_ltf_dnc_dne_temp_release.l_crmcloudsync_ltf_dnc_dne_temp_release_id = l_crmcloudsync_ltf_dnc_dne_temp_release.l_crmcloudsync_ltf_dnc_dne_temp_release_id
 where l_crmcloudsync_ltf_dnc_dne_temp_release.l_crmcloudsync_ltf_dnc_dne_temp_release_id is null
    or (l_crmcloudsync_ltf_dnc_dne_temp_release.l_crmcloudsync_ltf_dnc_dne_temp_release_id is not null
        and l_crmcloudsync_ltf_dnc_dne_temp_release.dv_hash <> #l_crmcloudsync_ltf_dnc_dne_temp_release_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_dnc_dne_temp_release
if object_id('tempdb..#s_crmcloudsync_ltf_dnc_dne_temp_release_inserts') is not null drop table #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts
create table #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_DNCDNETempRelease.bk_hash,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdon created_on,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_dncdnetempreleaseid ltf_dnc_dne_temp_release_id,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_expirationdate ltf_expiration_date,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_value ltf_value,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.organizationidname organization_id_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.statecode state_code,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.statuscode status_code,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_DNCDNETempRelease.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdbyname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdbyyominame,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdon,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdonbehalfbyname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.createdonbehalfbyyominame,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.importsequencenumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_dncdnetempreleaseid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_expirationdate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.ltf_value,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedbyname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedbyyominame,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedon,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedonbehalfbyname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.modifiedonbehalfbyyominame,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.organizationidname,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_DNCDNETempRelease.overriddencreatedon,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.statecode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.statecodename,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.statuscode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.statuscodename,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.versionnumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_DNCDNETempRelease.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.InsertUser,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_DNCDNETempRelease.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_DNCDNETempRelease.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_DNCDNETempRelease
 where stage_hash_crmcloudsync_LTF_DNCDNETempRelease.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_dnc_dne_temp_release records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_dnc_dne_temp_release (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_dnc_dne_temp_release_id,
       ltf_expiration_date,
       ltf_value,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       organization_id_name,
       overridden_created_on,
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
select #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.bk_hash,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.created_by_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.created_on,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.ltf_dnc_dne_temp_release_id,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.ltf_expiration_date,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.ltf_value,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.modified_by_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.modified_on,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.organization_id_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.state_code,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.state_code_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.status_code,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.status_code_name,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.version_number,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.insert_user,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.updated_date_time,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.update_user,
       case when s_crmcloudsync_ltf_dnc_dne_temp_release.s_crmcloudsync_ltf_dnc_dne_temp_release_id is null then isnull(#s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts
  left join p_crmcloudsync_ltf_dnc_dne_temp_release
    on #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.bk_hash = p_crmcloudsync_ltf_dnc_dne_temp_release.bk_hash
   and p_crmcloudsync_ltf_dnc_dne_temp_release.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_dnc_dne_temp_release
    on p_crmcloudsync_ltf_dnc_dne_temp_release.bk_hash = s_crmcloudsync_ltf_dnc_dne_temp_release.bk_hash
   and p_crmcloudsync_ltf_dnc_dne_temp_release.s_crmcloudsync_ltf_dnc_dne_temp_release_id = s_crmcloudsync_ltf_dnc_dne_temp_release.s_crmcloudsync_ltf_dnc_dne_temp_release_id
 where s_crmcloudsync_ltf_dnc_dne_temp_release.s_crmcloudsync_ltf_dnc_dne_temp_release_id is null
    or (s_crmcloudsync_ltf_dnc_dne_temp_release.s_crmcloudsync_ltf_dnc_dne_temp_release_id is not null
        and s_crmcloudsync_ltf_dnc_dne_temp_release.dv_hash <> #s_crmcloudsync_ltf_dnc_dne_temp_release_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_dnc_dne_temp_release @current_dv_batch_id

end
