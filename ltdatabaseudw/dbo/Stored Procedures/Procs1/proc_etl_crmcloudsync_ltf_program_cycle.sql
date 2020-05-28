CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_program_cycle] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_ProgramCycle

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_ProgramCycle (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_cancelreason,
       ltf_cyclebegindate,
       ltf_cycleenddate,
       ltf_cyclename,
       ltf_program,
       ltf_programcycleid,
       ltf_programname,
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
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_programcycleid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_cancelreason,
       ltf_cyclebegindate,
       ltf_cycleenddate,
       ltf_cyclename,
       ltf_program,
       ltf_programcycleid,
       ltf_programname,
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
       isnull(cast(stage_crmcloudsync_LTF_ProgramCycle.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_ProgramCycle
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_program_cycle @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_program_cycle (
       bk_hash,
       ltf_program_cycle_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_ProgramCycle.bk_hash,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_programcycleid ltf_program_cycle_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_ProgramCycle
  left join h_crmcloudsync_ltf_program_cycle
    on stage_hash_crmcloudsync_LTF_ProgramCycle.bk_hash = h_crmcloudsync_ltf_program_cycle.bk_hash
 where h_crmcloudsync_ltf_program_cycle_id is null
   and stage_hash_crmcloudsync_LTF_ProgramCycle.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_program_cycle
if object_id('tempdb..#l_crmcloudsync_ltf_program_cycle_inserts') is not null drop table #l_crmcloudsync_ltf_program_cycle_inserts
create table #l_crmcloudsync_ltf_program_cycle_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_ProgramCycle.bk_hash,
       stage_hash_crmcloudsync_LTF_ProgramCycle.createdby created_by,
       stage_hash_crmcloudsync_LTF_ProgramCycle.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_ProgramCycle.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_program ltf_program,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_programcycleid ltf_program_cycle_id,
       stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ownerid owner_id,
       stage_hash_crmcloudsync_LTF_ProgramCycle.owningbusinessunit owning_business_unit,
       stage_hash_crmcloudsync_LTF_ProgramCycle.owningteam owning_team,
       stage_hash_crmcloudsync_LTF_ProgramCycle.owninguser owning_user,
       stage_hash_crmcloudsync_LTF_ProgramCycle.statecode state_code,
       stage_hash_crmcloudsync_LTF_ProgramCycle.statuscode status_code,
       stage_hash_crmcloudsync_LTF_ProgramCycle.versionnumber version_number,
       isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_program as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_programcycleid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.ownerid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.owningbusinessunit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.owningteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.owninguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.versionnumber as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_ProgramCycle
 where stage_hash_crmcloudsync_LTF_ProgramCycle.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_program_cycle records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_program_cycle (
       bk_hash,
       created_by,
       created_on_behalf_by,
       import_sequence_number,
       ltf_program,
       ltf_program_cycle_id,
       modified_by,
       modified_on_behalf_by,
       owner_id,
       owning_business_unit,
       owning_team,
       owning_user,
       state_code,
       status_code,
       version_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_program_cycle_inserts.bk_hash,
       #l_crmcloudsync_ltf_program_cycle_inserts.created_by,
       #l_crmcloudsync_ltf_program_cycle_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_program_cycle_inserts.import_sequence_number,
       #l_crmcloudsync_ltf_program_cycle_inserts.ltf_program,
       #l_crmcloudsync_ltf_program_cycle_inserts.ltf_program_cycle_id,
       #l_crmcloudsync_ltf_program_cycle_inserts.modified_by,
       #l_crmcloudsync_ltf_program_cycle_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_program_cycle_inserts.owner_id,
       #l_crmcloudsync_ltf_program_cycle_inserts.owning_business_unit,
       #l_crmcloudsync_ltf_program_cycle_inserts.owning_team,
       #l_crmcloudsync_ltf_program_cycle_inserts.owning_user,
       #l_crmcloudsync_ltf_program_cycle_inserts.state_code,
       #l_crmcloudsync_ltf_program_cycle_inserts.status_code,
       #l_crmcloudsync_ltf_program_cycle_inserts.version_number,
       case when l_crmcloudsync_ltf_program_cycle.l_crmcloudsync_ltf_program_cycle_id is null then isnull(#l_crmcloudsync_ltf_program_cycle_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_program_cycle_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_program_cycle_inserts
  left join p_crmcloudsync_ltf_program_cycle
    on #l_crmcloudsync_ltf_program_cycle_inserts.bk_hash = p_crmcloudsync_ltf_program_cycle.bk_hash
   and p_crmcloudsync_ltf_program_cycle.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_program_cycle
    on p_crmcloudsync_ltf_program_cycle.bk_hash = l_crmcloudsync_ltf_program_cycle.bk_hash
   and p_crmcloudsync_ltf_program_cycle.l_crmcloudsync_ltf_program_cycle_id = l_crmcloudsync_ltf_program_cycle.l_crmcloudsync_ltf_program_cycle_id
 where l_crmcloudsync_ltf_program_cycle.l_crmcloudsync_ltf_program_cycle_id is null
    or (l_crmcloudsync_ltf_program_cycle.l_crmcloudsync_ltf_program_cycle_id is not null
        and l_crmcloudsync_ltf_program_cycle.dv_hash <> #l_crmcloudsync_ltf_program_cycle_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_program_cycle
if object_id('tempdb..#s_crmcloudsync_ltf_program_cycle_inserts') is not null drop table #s_crmcloudsync_ltf_program_cycle_inserts
create table #s_crmcloudsync_ltf_program_cycle_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_ProgramCycle.bk_hash,
       stage_hash_crmcloudsync_LTF_ProgramCycle.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.createdon created_on,
       stage_hash_crmcloudsync_LTF_ProgramCycle.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_cancelreason ltf_cancel_reason,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_cyclebegindate ltf_cycle_begin_date,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_cycleenddate ltf_cycle_end_date,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_cyclename ltf_cycle_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_programcycleid ltf_program_cycle_id,
       stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_programname ltf_program_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_ProgramCycle.owneridname owner_id_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.owneridtype owner_id_type,
       stage_hash_crmcloudsync_LTF_ProgramCycle.owneridyominame owner_id_yomi_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_ProgramCycle.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_ProgramCycle.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_ProgramCycle.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_ProgramCycle.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_ProgramCycle.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_ProgramCycle.UpdateUser update_user,
       isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ProgramCycle.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_cancelreason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_cyclebegindate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_cycleenddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_cyclename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_programcycleid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.ltf_programname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ProgramCycle.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.owneridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.owneridtype,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.owneridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_ProgramCycle.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ProgramCycle.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_ProgramCycle.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_ProgramCycle.UpdateUser,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_ProgramCycle
 where stage_hash_crmcloudsync_LTF_ProgramCycle.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_program_cycle records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_program_cycle (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       ltf_cancel_reason,
       ltf_cycle_begin_date,
       ltf_cycle_end_date,
       ltf_cycle_name,
       ltf_program_cycle_id,
       ltf_program_name,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       overridden_created_on,
       owner_id_name,
       owner_id_type,
       owner_id_yomi_name,
       state_code_name,
       status_code_name,
       time_zone_rule_version_number,
       utc_conversion_time_zone_code,
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
select #s_crmcloudsync_ltf_program_cycle_inserts.bk_hash,
       #s_crmcloudsync_ltf_program_cycle_inserts.created_by_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.created_on,
       #s_crmcloudsync_ltf_program_cycle_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.ltf_cancel_reason,
       #s_crmcloudsync_ltf_program_cycle_inserts.ltf_cycle_begin_date,
       #s_crmcloudsync_ltf_program_cycle_inserts.ltf_cycle_end_date,
       #s_crmcloudsync_ltf_program_cycle_inserts.ltf_cycle_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.ltf_program_cycle_id,
       #s_crmcloudsync_ltf_program_cycle_inserts.ltf_program_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.modified_by_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.modified_by_yomi_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.modified_on,
       #s_crmcloudsync_ltf_program_cycle_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_program_cycle_inserts.owner_id_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.owner_id_type,
       #s_crmcloudsync_ltf_program_cycle_inserts.owner_id_yomi_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.state_code_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.status_code_name,
       #s_crmcloudsync_ltf_program_cycle_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_program_cycle_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_program_cycle_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_program_cycle_inserts.insert_user,
       #s_crmcloudsync_ltf_program_cycle_inserts.updated_date_time,
       #s_crmcloudsync_ltf_program_cycle_inserts.update_user,
       case when s_crmcloudsync_ltf_program_cycle.s_crmcloudsync_ltf_program_cycle_id is null then isnull(#s_crmcloudsync_ltf_program_cycle_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_program_cycle_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_program_cycle_inserts
  left join p_crmcloudsync_ltf_program_cycle
    on #s_crmcloudsync_ltf_program_cycle_inserts.bk_hash = p_crmcloudsync_ltf_program_cycle.bk_hash
   and p_crmcloudsync_ltf_program_cycle.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_program_cycle
    on p_crmcloudsync_ltf_program_cycle.bk_hash = s_crmcloudsync_ltf_program_cycle.bk_hash
   and p_crmcloudsync_ltf_program_cycle.s_crmcloudsync_ltf_program_cycle_id = s_crmcloudsync_ltf_program_cycle.s_crmcloudsync_ltf_program_cycle_id
 where s_crmcloudsync_ltf_program_cycle.s_crmcloudsync_ltf_program_cycle_id is null
    or (s_crmcloudsync_ltf_program_cycle.s_crmcloudsync_ltf_program_cycle_id is not null
        and s_crmcloudsync_ltf_program_cycle.dv_hash <> #s_crmcloudsync_ltf_program_cycle_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_program_cycle @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_program_cycle @current_dv_batch_id

end
