CREATE PROC [dbo].[proc_etl_boss_evt_registration_processes] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_evt_registration_processes

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_evt_registration_processes (
       bk_hash,
       [id],
       member_id,
       reservation_id,
       state,
       created_at,
       updated_at,
       user_id,
       roster_id,
       expires_at,
       roster_only,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       member_id,
       reservation_id,
       state,
       created_at,
       updated_at,
       user_id,
       roster_id,
       expires_at,
       roster_only,
       isnull(cast(stage_boss_evt_registration_processes.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_evt_registration_processes
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_evt_registration_processes @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_evt_registration_processes (
       bk_hash,
       evt_registration_processes_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_evt_registration_processes.bk_hash,
       stage_hash_boss_evt_registration_processes.[id] evt_registration_processes_id,
       isnull(cast(stage_hash_boss_evt_registration_processes.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_evt_registration_processes
  left join h_boss_evt_registration_processes
    on stage_hash_boss_evt_registration_processes.bk_hash = h_boss_evt_registration_processes.bk_hash
 where h_boss_evt_registration_processes_id is null
   and stage_hash_boss_evt_registration_processes.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_evt_registration_processes
if object_id('tempdb..#l_boss_evt_registration_processes_inserts') is not null drop table #l_boss_evt_registration_processes_inserts
create table #l_boss_evt_registration_processes_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_evt_registration_processes.bk_hash,
       stage_hash_boss_evt_registration_processes.[id] evt_registration_processes_id,
       stage_hash_boss_evt_registration_processes.member_id member_id,
       stage_hash_boss_evt_registration_processes.reservation_id reservation_id,
       stage_hash_boss_evt_registration_processes.user_id user_id,
       stage_hash_boss_evt_registration_processes.roster_id roster_id,
       isnull(cast(stage_hash_boss_evt_registration_processes.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_evt_registration_processes.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_evt_registration_processes.member_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_evt_registration_processes.reservation_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_evt_registration_processes.user_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_evt_registration_processes.roster_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_evt_registration_processes
 where stage_hash_boss_evt_registration_processes.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_evt_registration_processes records
set @insert_date_time = getdate()
insert into l_boss_evt_registration_processes (
       bk_hash,
       evt_registration_processes_id,
       member_id,
       reservation_id,
       user_id,
       roster_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_evt_registration_processes_inserts.bk_hash,
       #l_boss_evt_registration_processes_inserts.evt_registration_processes_id,
       #l_boss_evt_registration_processes_inserts.member_id,
       #l_boss_evt_registration_processes_inserts.reservation_id,
       #l_boss_evt_registration_processes_inserts.user_id,
       #l_boss_evt_registration_processes_inserts.roster_id,
       case when l_boss_evt_registration_processes.l_boss_evt_registration_processes_id is null then isnull(#l_boss_evt_registration_processes_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_evt_registration_processes_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_evt_registration_processes_inserts
  left join p_boss_evt_registration_processes
    on #l_boss_evt_registration_processes_inserts.bk_hash = p_boss_evt_registration_processes.bk_hash
   and p_boss_evt_registration_processes.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_evt_registration_processes
    on p_boss_evt_registration_processes.bk_hash = l_boss_evt_registration_processes.bk_hash
   and p_boss_evt_registration_processes.l_boss_evt_registration_processes_id = l_boss_evt_registration_processes.l_boss_evt_registration_processes_id
 where l_boss_evt_registration_processes.l_boss_evt_registration_processes_id is null
    or (l_boss_evt_registration_processes.l_boss_evt_registration_processes_id is not null
        and l_boss_evt_registration_processes.dv_hash <> #l_boss_evt_registration_processes_inserts.source_hash)

--calculate hash and lookup to current s_boss_evt_registration_processes
if object_id('tempdb..#s_boss_evt_registration_processes_inserts') is not null drop table #s_boss_evt_registration_processes_inserts
create table #s_boss_evt_registration_processes_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_evt_registration_processes.bk_hash,
       stage_hash_boss_evt_registration_processes.[id] evt_registration_processes_id,
       stage_hash_boss_evt_registration_processes.state state,
       stage_hash_boss_evt_registration_processes.created_at created_at,
       stage_hash_boss_evt_registration_processes.updated_at updated_at,
       stage_hash_boss_evt_registration_processes.expires_at expires_at,
       stage_hash_boss_evt_registration_processes.roster_only roster_only,
       isnull(cast(stage_hash_boss_evt_registration_processes.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_evt_registration_processes.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_evt_registration_processes.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_evt_registration_processes.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_evt_registration_processes.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_evt_registration_processes.expires_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_evt_registration_processes.roster_only as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_evt_registration_processes
 where stage_hash_boss_evt_registration_processes.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_evt_registration_processes records
set @insert_date_time = getdate()
insert into s_boss_evt_registration_processes (
       bk_hash,
       evt_registration_processes_id,
       state,
       created_at,
       updated_at,
       expires_at,
       roster_only,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_evt_registration_processes_inserts.bk_hash,
       #s_boss_evt_registration_processes_inserts.evt_registration_processes_id,
       #s_boss_evt_registration_processes_inserts.state,
       #s_boss_evt_registration_processes_inserts.created_at,
       #s_boss_evt_registration_processes_inserts.updated_at,
       #s_boss_evt_registration_processes_inserts.expires_at,
       #s_boss_evt_registration_processes_inserts.roster_only,
       case when s_boss_evt_registration_processes.s_boss_evt_registration_processes_id is null then isnull(#s_boss_evt_registration_processes_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_evt_registration_processes_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_evt_registration_processes_inserts
  left join p_boss_evt_registration_processes
    on #s_boss_evt_registration_processes_inserts.bk_hash = p_boss_evt_registration_processes.bk_hash
   and p_boss_evt_registration_processes.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_evt_registration_processes
    on p_boss_evt_registration_processes.bk_hash = s_boss_evt_registration_processes.bk_hash
   and p_boss_evt_registration_processes.s_boss_evt_registration_processes_id = s_boss_evt_registration_processes.s_boss_evt_registration_processes_id
 where s_boss_evt_registration_processes.s_boss_evt_registration_processes_id is null
    or (s_boss_evt_registration_processes.s_boss_evt_registration_processes_id is not null
        and s_boss_evt_registration_processes.dv_hash <> #s_boss_evt_registration_processes_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_evt_registration_processes @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_evt_registration_processes @current_dv_batch_id

end
