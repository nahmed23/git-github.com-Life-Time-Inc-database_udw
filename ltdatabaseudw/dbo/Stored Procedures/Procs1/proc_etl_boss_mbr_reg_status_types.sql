CREATE PROC [dbo].[proc_etl_boss_mbr_reg_status_types] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_mbr_reg_status_types

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_mbr_reg_status_types (
       bk_hash,
       [id],
       name,
       description,
       position,
       term_type,
       active,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [id],
       name,
       description,
       position,
       term_type,
       active,
       created_at,
       updated_at,
       isnull(cast(stage_boss_mbr_reg_status_types.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_mbr_reg_status_types
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_mbr_reg_status_types @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_mbr_reg_status_types (
       bk_hash,
       mbr_reg_status_types_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_mbr_reg_status_types.bk_hash,
       stage_hash_boss_mbr_reg_status_types.[id] mbr_reg_status_types_id,
       isnull(cast(stage_hash_boss_mbr_reg_status_types.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_mbr_reg_status_types
  left join h_boss_mbr_reg_status_types
    on stage_hash_boss_mbr_reg_status_types.bk_hash = h_boss_mbr_reg_status_types.bk_hash
 where h_boss_mbr_reg_status_types_id is null
   and stage_hash_boss_mbr_reg_status_types.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_boss_mbr_reg_status_types
if object_id('tempdb..#s_boss_mbr_reg_status_types_inserts') is not null drop table #s_boss_mbr_reg_status_types_inserts
create table #s_boss_mbr_reg_status_types_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_mbr_reg_status_types.bk_hash,
       stage_hash_boss_mbr_reg_status_types.[id] mbr_reg_status_types_id,
       stage_hash_boss_mbr_reg_status_types.name name,
       stage_hash_boss_mbr_reg_status_types.description description,
       stage_hash_boss_mbr_reg_status_types.position position,
       stage_hash_boss_mbr_reg_status_types.term_type term_type,
       stage_hash_boss_mbr_reg_status_types.active active,
       stage_hash_boss_mbr_reg_status_types.created_at created_at,
       stage_hash_boss_mbr_reg_status_types.updated_at updated_at,
       isnull(cast(stage_hash_boss_mbr_reg_status_types.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_reg_status_types.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_reg_status_types.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_reg_status_types.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_mbr_reg_status_types.position as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_reg_status_types.term_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_mbr_reg_status_types.active,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_reg_status_types.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_mbr_reg_status_types.updated_at,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_mbr_reg_status_types
 where stage_hash_boss_mbr_reg_status_types.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_mbr_reg_status_types records
set @insert_date_time = getdate()
insert into s_boss_mbr_reg_status_types (
       bk_hash,
       mbr_reg_status_types_id,
       name,
       description,
       position,
       term_type,
       active,
       created_at,
       updated_at,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_mbr_reg_status_types_inserts.bk_hash,
       #s_boss_mbr_reg_status_types_inserts.mbr_reg_status_types_id,
       #s_boss_mbr_reg_status_types_inserts.name,
       #s_boss_mbr_reg_status_types_inserts.description,
       #s_boss_mbr_reg_status_types_inserts.position,
       #s_boss_mbr_reg_status_types_inserts.term_type,
       #s_boss_mbr_reg_status_types_inserts.active,
       #s_boss_mbr_reg_status_types_inserts.created_at,
       #s_boss_mbr_reg_status_types_inserts.updated_at,
       case when s_boss_mbr_reg_status_types.s_boss_mbr_reg_status_types_id is null then isnull(#s_boss_mbr_reg_status_types_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_mbr_reg_status_types_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_mbr_reg_status_types_inserts
  left join p_boss_mbr_reg_status_types
    on #s_boss_mbr_reg_status_types_inserts.bk_hash = p_boss_mbr_reg_status_types.bk_hash
   and p_boss_mbr_reg_status_types.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_mbr_reg_status_types
    on p_boss_mbr_reg_status_types.bk_hash = s_boss_mbr_reg_status_types.bk_hash
   and p_boss_mbr_reg_status_types.s_boss_mbr_reg_status_types_id = s_boss_mbr_reg_status_types.s_boss_mbr_reg_status_types_id
 where s_boss_mbr_reg_status_types.s_boss_mbr_reg_status_types_id is null
    or (s_boss_mbr_reg_status_types.s_boss_mbr_reg_status_types_id is not null
        and s_boss_mbr_reg_status_types.dv_hash <> #s_boss_mbr_reg_status_types_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_mbr_reg_status_types @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_mbr_reg_status_types @current_dv_batch_id

end
