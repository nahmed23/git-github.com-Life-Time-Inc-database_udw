CREATE PROC [dbo].[proc_etl_boss_asi_res_inst] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_asiresinst

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_asiresinst (
       bk_hash,
       reservation,
       instructor_id,
       start_date,
       end_date,
       name,
       comment,
       cost,
       substitute,
       sub_for,
       [id],
       employee_id,
       updated_at,
       created_at,
       res_color,
       use_for_LTBucks,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([id] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       reservation,
       instructor_id,
       start_date,
       end_date,
       name,
       comment,
       cost,
       substitute,
       sub_for,
       [id],
       employee_id,
       updated_at,
       created_at,
       res_color,
       use_for_LTBucks,
       isnull(cast(stage_boss_asiresinst.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_boss_asiresinst
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_asi_res_inst @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_asi_res_inst (
       bk_hash,
       asi_res_inst_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_boss_asiresinst.bk_hash,
       stage_hash_boss_asiresinst.[id] asi_res_inst_id,
       isnull(cast(stage_hash_boss_asiresinst.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_asiresinst
  left join h_boss_asi_res_inst
    on stage_hash_boss_asiresinst.bk_hash = h_boss_asi_res_inst.bk_hash
 where h_boss_asi_res_inst_id is null
   and stage_hash_boss_asiresinst.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_asi_res_inst
if object_id('tempdb..#l_boss_asi_res_inst_inserts') is not null drop table #l_boss_asi_res_inst_inserts
create table #l_boss_asi_res_inst_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiresinst.bk_hash,
       stage_hash_boss_asiresinst.reservation reservation,
       stage_hash_boss_asiresinst.instructor_id instructor_id,
       stage_hash_boss_asiresinst.[id] asi_res_inst_id,
       stage_hash_boss_asiresinst.employee_id employee_id,
       isnull(cast(stage_hash_boss_asiresinst.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_asiresinst.reservation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresinst.instructor_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresinst.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresinst.employee_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiresinst
 where stage_hash_boss_asiresinst.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_boss_asi_res_inst records
set @insert_date_time = getdate()
insert into l_boss_asi_res_inst (
       bk_hash,
       reservation,
       instructor_id,
       asi_res_inst_id,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_boss_asi_res_inst_inserts.bk_hash,
       #l_boss_asi_res_inst_inserts.reservation,
       #l_boss_asi_res_inst_inserts.instructor_id,
       #l_boss_asi_res_inst_inserts.asi_res_inst_id,
       #l_boss_asi_res_inst_inserts.employee_id,
       case when l_boss_asi_res_inst.l_boss_asi_res_inst_id is null then isnull(#l_boss_asi_res_inst_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_asi_res_inst_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_asi_res_inst_inserts
  left join p_boss_asi_res_inst
    on #l_boss_asi_res_inst_inserts.bk_hash = p_boss_asi_res_inst.bk_hash
   and p_boss_asi_res_inst.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_asi_res_inst
    on p_boss_asi_res_inst.bk_hash = l_boss_asi_res_inst.bk_hash
   and p_boss_asi_res_inst.l_boss_asi_res_inst_id = l_boss_asi_res_inst.l_boss_asi_res_inst_id
 where l_boss_asi_res_inst.l_boss_asi_res_inst_id is null
    or (l_boss_asi_res_inst.l_boss_asi_res_inst_id is not null
        and l_boss_asi_res_inst.dv_hash <> #l_boss_asi_res_inst_inserts.source_hash)

--calculate hash and lookup to current s_boss_asi_res_inst
if object_id('tempdb..#s_boss_asi_res_inst_inserts') is not null drop table #s_boss_asi_res_inst_inserts
create table #s_boss_asi_res_inst_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_asiresinst.bk_hash,
       stage_hash_boss_asiresinst.start_date start_date,
       stage_hash_boss_asiresinst.end_date end_date,
       stage_hash_boss_asiresinst.name name,
       stage_hash_boss_asiresinst.comment comment,
       stage_hash_boss_asiresinst.cost cost,
       stage_hash_boss_asiresinst.substitute substitute,
       stage_hash_boss_asiresinst.sub_for sub_for,
       stage_hash_boss_asiresinst.[id] asi_res_inst_id,
       stage_hash_boss_asiresinst.updated_at updated_at,
       stage_hash_boss_asiresinst.created_at created_at,
       stage_hash_boss_asiresinst.res_color res_color,
       stage_hash_boss_asiresinst.use_for_LTBucks use_for_lt_bucks,
       isnull(cast(stage_hash_boss_asiresinst.created_at as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiresinst.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiresinst.end_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiresinst.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiresinst.comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresinst.cost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiresinst.substitute,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiresinst.sub_for,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresinst.[id] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiresinst.updated_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_asiresinst.created_at,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_boss_asiresinst.res_color as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_asiresinst.use_for_LTBucks,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_asiresinst
 where stage_hash_boss_asiresinst.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_boss_asi_res_inst records
set @insert_date_time = getdate()
insert into s_boss_asi_res_inst (
       bk_hash,
       start_date,
       end_date,
       name,
       comment,
       cost,
       substitute,
       sub_for,
       asi_res_inst_id,
       updated_at,
       created_at,
       res_color,
       use_for_lt_bucks,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_boss_asi_res_inst_inserts.bk_hash,
       #s_boss_asi_res_inst_inserts.start_date,
       #s_boss_asi_res_inst_inserts.end_date,
       #s_boss_asi_res_inst_inserts.name,
       #s_boss_asi_res_inst_inserts.comment,
       #s_boss_asi_res_inst_inserts.cost,
       #s_boss_asi_res_inst_inserts.substitute,
       #s_boss_asi_res_inst_inserts.sub_for,
       #s_boss_asi_res_inst_inserts.asi_res_inst_id,
       #s_boss_asi_res_inst_inserts.updated_at,
       #s_boss_asi_res_inst_inserts.created_at,
       #s_boss_asi_res_inst_inserts.res_color,
       #s_boss_asi_res_inst_inserts.use_for_lt_bucks,
       case when s_boss_asi_res_inst.s_boss_asi_res_inst_id is null then isnull(#s_boss_asi_res_inst_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_asi_res_inst_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_asi_res_inst_inserts
  left join p_boss_asi_res_inst
    on #s_boss_asi_res_inst_inserts.bk_hash = p_boss_asi_res_inst.bk_hash
   and p_boss_asi_res_inst.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_asi_res_inst
    on p_boss_asi_res_inst.bk_hash = s_boss_asi_res_inst.bk_hash
   and p_boss_asi_res_inst.s_boss_asi_res_inst_id = s_boss_asi_res_inst.s_boss_asi_res_inst_id
 where s_boss_asi_res_inst.s_boss_asi_res_inst_id is null
    or (s_boss_asi_res_inst.s_boss_asi_res_inst_id is not null
        and s_boss_asi_res_inst.dv_hash <> #s_boss_asi_res_inst_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_boss_asi_res_inst @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_asi_res_inst @current_dv_batch_id

end
