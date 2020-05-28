CREATE PROC [dbo].[proc_etl_boss_participation] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_participation

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_participation (
       bk_hash,
       reservation,
       participation_date,
       no_participants,
       comment,
       no_non_mbr,
       updated_at,
       created_at,
       [id],
       system_count,
       MOD_count,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       reservation,
       participation_date,
       no_participants,
       comment,
       no_non_mbr,
       updated_at,
       created_at,
       [id],
       system_count,
       MOD_count,
       isnull(cast(stage_boss_participation.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_participation
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_participation @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_participation (
       bk_hash,
       participation_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_participation.bk_hash,
       stage_hash_boss_participation.[id] participation_id,
       isnull(cast(stage_hash_boss_participation.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_participation
  left join h_boss_participation
    on stage_hash_boss_participation.bk_hash = h_boss_participation.bk_hash
 where h_boss_participation_id is null
   and stage_hash_boss_participation.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_participation
if object_id('tempdb..#l_boss_participation_inserts') is not null drop table #l_boss_participation_inserts
create table #l_boss_participation_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_participation.bk_hash,
       stage_hash_boss_participation.reservation reservation,
       stage_hash_boss_participation.[id] participation_id,
       isnull(cast(stage_hash_boss_participation.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_participation.reservation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_participation.[id] as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_participation
 where stage_hash_boss_participation.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_participation records
set @insert_date_time = getdate()
insert into l_boss_participation (
       bk_hash,
       reservation,
       participation_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_participation_inserts.bk_hash,
       #l_boss_participation_inserts.reservation,
       #l_boss_participation_inserts.participation_id,
       case when l_boss_participation.l_boss_participation_id is null then isnull(#l_boss_participation_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_participation_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_participation_inserts
  left join p_boss_participation
    on #l_boss_participation_inserts.bk_hash = p_boss_participation.bk_hash
   and p_boss_participation.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_participation
    on p_boss_participation.bk_hash = l_boss_participation.bk_hash
   and p_boss_participation.l_boss_participation_id = l_boss_participation.l_boss_participation_id
 where l_boss_participation.l_boss_participation_id is null
    or (l_boss_participation.l_boss_participation_id is not null
        and l_boss_participation.dv_hash <> #l_boss_participation_inserts.source_hash)

--calculate hash and lookup to current s_boss_participation
if object_id('tempdb..#s_boss_participation_inserts') is not null drop table #s_boss_participation_inserts
create table #s_boss_participation_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_participation.bk_hash,
       stage_hash_boss_participation.participation_date participation_date,
       stage_hash_boss_participation.no_participants no_participants,
       stage_hash_boss_participation.comment comment,
       stage_hash_boss_participation.no_non_mbr no_non_mbr,
       stage_hash_boss_participation.updated_at updated_at,
       stage_hash_boss_participation.created_at created_at,
       stage_hash_boss_participation.[id] participation_id,
       stage_hash_boss_participation.system_count system_count,
       stage_hash_boss_participation.MOD_count mod_count,
       isnull(cast(stage_hash_boss_participation.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_participation.participation_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_participation.no_participants as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_participation.comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_participation.no_non_mbr as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_participation.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_participation.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_participation.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_participation.system_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_participation.MOD_count as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_participation
 where stage_hash_boss_participation.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_participation records
set @insert_date_time = getdate()
insert into s_boss_participation (
       bk_hash,
       participation_date,
       no_participants,
       comment,
       no_non_mbr,
       updated_at,
       created_at,
       participation_id,
       system_count,
       mod_count,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_participation_inserts.bk_hash,
       #s_boss_participation_inserts.participation_date,
       #s_boss_participation_inserts.no_participants,
       #s_boss_participation_inserts.comment,
       #s_boss_participation_inserts.no_non_mbr,
       #s_boss_participation_inserts.updated_at,
       #s_boss_participation_inserts.created_at,
       #s_boss_participation_inserts.participation_id,
       #s_boss_participation_inserts.system_count,
       #s_boss_participation_inserts.mod_count,
       case when s_boss_participation.s_boss_participation_id is null then isnull(#s_boss_participation_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_participation_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_participation_inserts
  left join p_boss_participation
    on #s_boss_participation_inserts.bk_hash = p_boss_participation.bk_hash
   and p_boss_participation.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_participation
    on p_boss_participation.bk_hash = s_boss_participation.bk_hash
   and p_boss_participation.s_boss_participation_id = s_boss_participation.s_boss_participation_id
 where s_boss_participation.s_boss_participation_id is null
    or (s_boss_participation.s_boss_participation_id is not null
        and s_boss_participation.dv_hash <> #s_boss_participation_inserts.source_hash)

--Run the dv_deleted proc
exec dbo.proc_dv_deleted_boss_participation @current_dv_batch_id, @job_start_date_time_varchar

--Run the PIT proc
exec dbo.proc_p_boss_participation @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_participation @current_dv_batch_id

end
