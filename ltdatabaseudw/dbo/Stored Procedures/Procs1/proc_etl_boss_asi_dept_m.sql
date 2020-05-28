CREATE PROC [dbo].[proc_etl_boss_asi_dept_m] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asideptm

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asideptm (
       bk_hash,
       deptm_code,
       deptm_desc,
       deptm_has_res,
       deptm_legacy_code,
       deptm_created_at,
       deptm_updated_at,
       deptm_id,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(deptm_code as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       deptm_code,
       deptm_desc,
       deptm_has_res,
       deptm_legacy_code,
       deptm_created_at,
       deptm_updated_at,
       deptm_id,
       isnull(cast(stage_boss_asideptm.deptm_created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_asideptm
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_dept_m @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_dept_m (
       bk_hash,
       dept_m_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_asideptm.bk_hash,
       stage_hash_boss_asideptm.deptm_code dept_m_code,
       isnull(cast(stage_hash_boss_asideptm.deptm_created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asideptm
  left join h_boss_asi_dept_m
    on stage_hash_boss_asideptm.bk_hash = h_boss_asi_dept_m.bk_hash
 where h_boss_asi_dept_m_id is null
   and stage_hash_boss_asideptm.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_dept_m
if object_id('tempdb..#l_boss_asi_dept_m_inserts') is not null drop table #l_boss_asi_dept_m_inserts
create table #l_boss_asi_dept_m_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asideptm.bk_hash,
       stage_hash_boss_asideptm.deptm_code dept_m_code,
       stage_hash_boss_asideptm.deptm_legacy_code dept_m_legacy_code,
       stage_hash_boss_asideptm.deptm_id dept_m_id,
       isnull(cast(stage_hash_boss_asideptm.deptm_created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asideptm.deptm_code as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asideptm.deptm_legacy_code as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_boss_asideptm.deptm_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asideptm
 where stage_hash_boss_asideptm.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_dept_m records
set @insert_date_time = getdate()
insert into l_boss_asi_dept_m (
       bk_hash,
       dept_m_code,
       dept_m_legacy_code,
       dept_m_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_dept_m_inserts.bk_hash,
       #l_boss_asi_dept_m_inserts.dept_m_code,
       #l_boss_asi_dept_m_inserts.dept_m_legacy_code,
       #l_boss_asi_dept_m_inserts.dept_m_id,
       case when l_boss_asi_dept_m.l_boss_asi_dept_m_id is null then isnull(#l_boss_asi_dept_m_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_dept_m_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_dept_m_inserts
  left join p_boss_asi_dept_m
    on #l_boss_asi_dept_m_inserts.bk_hash = p_boss_asi_dept_m.bk_hash
   and p_boss_asi_dept_m.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_dept_m
    on p_boss_asi_dept_m.bk_hash = l_boss_asi_dept_m.bk_hash
   and p_boss_asi_dept_m.l_boss_asi_dept_m_id = l_boss_asi_dept_m.l_boss_asi_dept_m_id
 where l_boss_asi_dept_m.l_boss_asi_dept_m_id is null
    or (l_boss_asi_dept_m.l_boss_asi_dept_m_id is not null
        and l_boss_asi_dept_m.dv_hash <> #l_boss_asi_dept_m_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_dept_m
if object_id('tempdb..#s_boss_asi_dept_m_inserts') is not null drop table #s_boss_asi_dept_m_inserts
create table #s_boss_asi_dept_m_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asideptm.bk_hash,
       stage_hash_boss_asideptm.deptm_code dept_m_code,
       stage_hash_boss_asideptm.deptm_desc dept_m_desc,
       stage_hash_boss_asideptm.deptm_has_res dept_m_has_res,
       stage_hash_boss_asideptm.deptm_created_at dept_m_created_at,
       stage_hash_boss_asideptm.deptm_updated_at dept_m_updated_at,
       isnull(cast(stage_hash_boss_asideptm.deptm_created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asideptm.deptm_code as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_boss_asideptm.deptm_desc,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_boss_asideptm.deptm_has_res,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asideptm.deptm_created_at,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asideptm.deptm_updated_at,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asideptm
 where stage_hash_boss_asideptm.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_dept_m records
set @insert_date_time = getdate()
insert into s_boss_asi_dept_m (
       bk_hash,
       dept_m_code,
       dept_m_desc,
       dept_m_has_res,
       dept_m_created_at,
       dept_m_updated_at,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_dept_m_inserts.bk_hash,
       #s_boss_asi_dept_m_inserts.dept_m_code,
       #s_boss_asi_dept_m_inserts.dept_m_desc,
       #s_boss_asi_dept_m_inserts.dept_m_has_res,
       #s_boss_asi_dept_m_inserts.dept_m_created_at,
       #s_boss_asi_dept_m_inserts.dept_m_updated_at,
       case when s_boss_asi_dept_m.s_boss_asi_dept_m_id is null then isnull(#s_boss_asi_dept_m_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_dept_m_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_dept_m_inserts
  left join p_boss_asi_dept_m
    on #s_boss_asi_dept_m_inserts.bk_hash = p_boss_asi_dept_m.bk_hash
   and p_boss_asi_dept_m.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_dept_m
    on p_boss_asi_dept_m.bk_hash = s_boss_asi_dept_m.bk_hash
   and p_boss_asi_dept_m.s_boss_asi_dept_m_id = s_boss_asi_dept_m.s_boss_asi_dept_m_id
 where s_boss_asi_dept_m.s_boss_asi_dept_m_id is null
    or (s_boss_asi_dept_m.s_boss_asi_dept_m_id is not null
        and s_boss_asi_dept_m.dv_hash <> #s_boss_asi_dept_m_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_asi_dept_m @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_dept_m @current_dv_batch_id

end
