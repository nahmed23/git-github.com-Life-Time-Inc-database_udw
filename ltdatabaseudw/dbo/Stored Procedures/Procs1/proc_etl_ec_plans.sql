CREATE PROC [dbo].[proc_etl_ec_plans] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_Plans

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_Plans (
       bk_hash,
       PlanId,
       ProgramId,
       PartyId,
       Name,
       Duration,
       DurationType,
       StartDate,
       EndDate,
       CoachPartyId,
       SourceId,
       SourceType,
       CreatedDate,
       UpdatedDate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PlanId as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PlanId,
       ProgramId,
       PartyId,
       Name,
       Duration,
       DurationType,
       StartDate,
       EndDate,
       CoachPartyId,
       SourceId,
       SourceType,
       CreatedDate,
       UpdatedDate,
       isnull(cast(stage_ec_Plans.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_Plans
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_plans @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_plans (
       bk_hash,
       plan_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_Plans.bk_hash,
       stage_hash_ec_Plans.PlanId plan_id,
       isnull(cast(stage_hash_ec_Plans.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_Plans
  left join h_ec_plans
    on stage_hash_ec_Plans.bk_hash = h_ec_plans.bk_hash
 where h_ec_plans_id is null
   and stage_hash_ec_Plans.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ec_plans
if object_id('tempdb..#l_ec_plans_inserts') is not null drop table #l_ec_plans_inserts
create table #l_ec_plans_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Plans.bk_hash,
       stage_hash_ec_Plans.PlanId plan_id,
       stage_hash_ec_Plans.ProgramId program_id,
       stage_hash_ec_Plans.PartyId party_id,
       stage_hash_ec_Plans.CoachPartyId coach_party_id,
       stage_hash_ec_Plans.SourceId source_id,
       isnull(cast(stage_hash_ec_Plans.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_Plans.PlanId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Plans.ProgramId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Plans.PartyId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Plans.CoachPartyId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Plans.SourceId,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Plans
 where stage_hash_ec_Plans.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ec_plans records
set @insert_date_time = getdate()
insert into l_ec_plans (
       bk_hash,
       plan_id,
       program_id,
       party_id,
       coach_party_id,
       source_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ec_plans_inserts.bk_hash,
       #l_ec_plans_inserts.plan_id,
       #l_ec_plans_inserts.program_id,
       #l_ec_plans_inserts.party_id,
       #l_ec_plans_inserts.coach_party_id,
       #l_ec_plans_inserts.source_id,
       case when l_ec_plans.l_ec_plans_id is null then isnull(#l_ec_plans_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #l_ec_plans_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ec_plans_inserts
  left join p_ec_plans
    on #l_ec_plans_inserts.bk_hash = p_ec_plans.bk_hash
   and p_ec_plans.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ec_plans
    on p_ec_plans.bk_hash = l_ec_plans.bk_hash
   and p_ec_plans.l_ec_plans_id = l_ec_plans.l_ec_plans_id
 where l_ec_plans.l_ec_plans_id is null
    or (l_ec_plans.l_ec_plans_id is not null
        and l_ec_plans.dv_hash <> #l_ec_plans_inserts.source_hash)

--calculate hash and lookup to current s_ec_plans
if object_id('tempdb..#s_ec_plans_inserts') is not null drop table #s_ec_plans_inserts
create table #s_ec_plans_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Plans.bk_hash,
       stage_hash_ec_Plans.PlanId plan_id,
       stage_hash_ec_Plans.Name name,
       stage_hash_ec_Plans.Duration duration,
       stage_hash_ec_Plans.DurationType duration_type,
       stage_hash_ec_Plans.StartDate start_date,
       stage_hash_ec_Plans.EndDate end_date,
       stage_hash_ec_Plans.SourceType source_type,
       stage_hash_ec_Plans.CreatedDate created_date,
       stage_hash_ec_Plans.UpdatedDate updated_date,
       isnull(cast(stage_hash_ec_Plans.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_Plans.PlanId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Plans.Name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Plans.Duration,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Plans.DurationType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Plans.StartDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Plans.EndDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Plans.SourceType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Plans.CreatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Plans.UpdatedDate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Plans
 where stage_hash_ec_Plans.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_plans records
set @insert_date_time = getdate()
insert into s_ec_plans (
       bk_hash,
       plan_id,
       name,
       duration,
       duration_type,
       start_date,
       end_date,
       source_type,
       created_date,
       updated_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_plans_inserts.bk_hash,
       #s_ec_plans_inserts.plan_id,
       #s_ec_plans_inserts.name,
       #s_ec_plans_inserts.duration,
       #s_ec_plans_inserts.duration_type,
       #s_ec_plans_inserts.start_date,
       #s_ec_plans_inserts.end_date,
       #s_ec_plans_inserts.source_type,
       #s_ec_plans_inserts.created_date,
       #s_ec_plans_inserts.updated_date,
       case when s_ec_plans.s_ec_plans_id is null then isnull(#s_ec_plans_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_plans_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_plans_inserts
  left join p_ec_plans
    on #s_ec_plans_inserts.bk_hash = p_ec_plans.bk_hash
   and p_ec_plans.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_plans
    on p_ec_plans.bk_hash = s_ec_plans.bk_hash
   and p_ec_plans.s_ec_plans_id = s_ec_plans.s_ec_plans_id
 where s_ec_plans.s_ec_plans_id is null
    or (s_ec_plans.s_ec_plans_id is not null
        and s_ec_plans.dv_hash <> #s_ec_plans_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_plans @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_plans @current_dv_batch_id

end
