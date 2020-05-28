CREATE PROC [dbo].[proc_etl_ec_workouts] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_Workouts

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_Workouts (
       bk_hash,
       [Id],
       Name,
       Description,
       CreatedDate,
       ModifiedDate,
       InactiveDate,
       Tags,
       PartyId,
       Discriminator,
       Type,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([Id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [Id],
       Name,
       Description,
       CreatedDate,
       ModifiedDate,
       InactiveDate,
       Tags,
       PartyId,
       Discriminator,
       Type,
       isnull(cast(stage_ec_Workouts.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_Workouts
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_workouts @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_workouts (
       bk_hash,
       workouts_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_Workouts.bk_hash,
       stage_hash_ec_Workouts.[Id] workouts_id,
       isnull(cast(stage_hash_ec_Workouts.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_Workouts
  left join h_ec_workouts
    on stage_hash_ec_Workouts.bk_hash = h_ec_workouts.bk_hash
 where h_ec_workouts_id is null
   and stage_hash_ec_Workouts.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ec_workouts
if object_id('tempdb..#l_ec_workouts_inserts') is not null drop table #l_ec_workouts_inserts
create table #l_ec_workouts_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Workouts.bk_hash,
       stage_hash_ec_Workouts.[Id] workouts_id,
       stage_hash_ec_Workouts.PartyId party_id,
       isnull(cast(stage_hash_ec_Workouts.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_Workouts.[Id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Workouts.PartyId as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Workouts
 where stage_hash_ec_Workouts.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ec_workouts records
set @insert_date_time = getdate()
insert into l_ec_workouts (
       bk_hash,
       workouts_id,
       party_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ec_workouts_inserts.bk_hash,
       #l_ec_workouts_inserts.workouts_id,
       #l_ec_workouts_inserts.party_id,
       case when l_ec_workouts.l_ec_workouts_id is null then isnull(#l_ec_workouts_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #l_ec_workouts_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ec_workouts_inserts
  left join p_ec_workouts
    on #l_ec_workouts_inserts.bk_hash = p_ec_workouts.bk_hash
   and p_ec_workouts.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ec_workouts
    on p_ec_workouts.bk_hash = l_ec_workouts.bk_hash
   and p_ec_workouts.l_ec_workouts_id = l_ec_workouts.l_ec_workouts_id
 where l_ec_workouts.l_ec_workouts_id is null
    or (l_ec_workouts.l_ec_workouts_id is not null
        and l_ec_workouts.dv_hash <> #l_ec_workouts_inserts.source_hash)

--calculate hash and lookup to current s_ec_workouts
if object_id('tempdb..#s_ec_workouts_inserts') is not null drop table #s_ec_workouts_inserts
create table #s_ec_workouts_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Workouts.bk_hash,
       stage_hash_ec_Workouts.[Id] workouts_id,
       stage_hash_ec_Workouts.Name name,
       stage_hash_ec_Workouts.Description description,
       stage_hash_ec_Workouts.CreatedDate created_date,
       stage_hash_ec_Workouts.ModifiedDate modified_date,
       stage_hash_ec_Workouts.InactiveDate inactive_date,
       stage_hash_ec_Workouts.Tags tags,
       stage_hash_ec_Workouts.Type type,
       stage_hash_ec_Workouts.Discriminator discriminator,
       isnull(cast(stage_hash_ec_Workouts.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_Workouts.[Id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Workouts.Name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Workouts.Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Workouts.CreatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Workouts.ModifiedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Workouts.InactiveDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Workouts.Tags,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Workouts.Type as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Workouts.Discriminator,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Workouts
 where stage_hash_ec_Workouts.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_workouts records
set @insert_date_time = getdate()
insert into s_ec_workouts (
       bk_hash,
       workouts_id,
       name,
       description,
       created_date,
       modified_date,
       inactive_date,
       tags,
       type,
       discriminator,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_workouts_inserts.bk_hash,
       #s_ec_workouts_inserts.workouts_id,
       #s_ec_workouts_inserts.name,
       #s_ec_workouts_inserts.description,
       #s_ec_workouts_inserts.created_date,
       #s_ec_workouts_inserts.modified_date,
       #s_ec_workouts_inserts.inactive_date,
       #s_ec_workouts_inserts.tags,
       #s_ec_workouts_inserts.type,
       #s_ec_workouts_inserts.discriminator,
       case when s_ec_workouts.s_ec_workouts_id is null then isnull(#s_ec_workouts_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_workouts_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_workouts_inserts
  left join p_ec_workouts
    on #s_ec_workouts_inserts.bk_hash = p_ec_workouts.bk_hash
   and p_ec_workouts.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_workouts
    on p_ec_workouts.bk_hash = s_ec_workouts.bk_hash
   and p_ec_workouts.s_ec_workouts_id = s_ec_workouts.s_ec_workouts_id
 where s_ec_workouts.s_ec_workouts_id is null
    or (s_ec_workouts.s_ec_workouts_id is not null
        and s_ec_workouts.dv_hash <> #s_ec_workouts_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_workouts @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_workouts @current_dv_batch_id

end
