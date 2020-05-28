CREATE PROC [dbo].[proc_etl_humanity_tasks] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_humanity_tasks

set @insert_date_time = getdate()
insert into dbo.stage_hash_humanity_tasks (
       bk_hash,
       task_id,
       shift_id,
       company_id,
       task_name,
       created_at,
       created_by,
       deleted,
       load_dttm,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(task_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(shift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(company_id,'z#@$k%&P')+'P%#&z$@k'+isnull(task_name,'z#@$k%&P')+'P%#&z$@k'+isnull(created_at,'z#@$k%&P')+'P%#&z$@k'+isnull(created_by,'z#@$k%&P')+'P%#&z$@k'+isnull(deleted,'z#@$k%&P')+'P%#&z$@k'+isnull(load_dttm,'z#@$k%&P')+'P%#&z$@k'+isnull(ltf_file_name,'z#@$k%&P'))),2) bk_hash,
       task_id,
       shift_id,
       company_id,
       task_name,
       created_at,
       created_by,
       deleted,
       load_dttm,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       isnull(cast(stage_humanity_tasks.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_humanity_tasks
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_humanity_tasks @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_humanity_tasks (
       bk_hash,
       task_id,
       shift_id,
       company_id,
       task_name,
       created_at,
       created_by,
       deleted,
       load_dttm,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_humanity_tasks.bk_hash,
       stage_hash_humanity_tasks.task_id task_id,
       stage_hash_humanity_tasks.shift_id shift_id,
       stage_hash_humanity_tasks.company_id company_id,
       stage_hash_humanity_tasks.task_name task_name,
       stage_hash_humanity_tasks.created_at created_at,
       stage_hash_humanity_tasks.created_by created_by,
       stage_hash_humanity_tasks.deleted deleted,
       stage_hash_humanity_tasks.load_dttm load_dttm,
       stage_hash_humanity_tasks.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_humanity_tasks.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       47,
       @insert_date_time,
       @user
  from stage_hash_humanity_tasks
  left join h_humanity_tasks
    on stage_hash_humanity_tasks.bk_hash = h_humanity_tasks.bk_hash
 where h_humanity_tasks_id is null
   and stage_hash_humanity_tasks.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_humanity_tasks
if object_id('tempdb..#l_humanity_tasks_inserts') is not null drop table #l_humanity_tasks_inserts
create table #l_humanity_tasks_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_tasks.bk_hash,
       stage_hash_humanity_tasks.task_id task_id,
       stage_hash_humanity_tasks.shift_id shift_id,
       stage_hash_humanity_tasks.company_id company_id,
       stage_hash_humanity_tasks.task_name task_name,
       stage_hash_humanity_tasks.created_at created_at,
       stage_hash_humanity_tasks.created_by created_by,
       stage_hash_humanity_tasks.deleted deleted,
       stage_hash_humanity_tasks.load_dttm load_dttm,
       stage_hash_humanity_tasks.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_humanity_tasks.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_tasks.task_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_humanity_tasks.shift_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.task_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.created_at,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.created_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.deleted,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.load_dttm,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.ltf_file_name,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_tasks
 where stage_hash_humanity_tasks.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_humanity_tasks records
set @insert_date_time = getdate()
insert into l_humanity_tasks (
       bk_hash,
       task_id,
       shift_id,
       company_id,
       task_name,
       created_at,
       created_by,
       deleted,
       load_dttm,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_humanity_tasks_inserts.bk_hash,
       #l_humanity_tasks_inserts.task_id,
       #l_humanity_tasks_inserts.shift_id,
       #l_humanity_tasks_inserts.company_id,
       #l_humanity_tasks_inserts.task_name,
       #l_humanity_tasks_inserts.created_at,
       #l_humanity_tasks_inserts.created_by,
       #l_humanity_tasks_inserts.deleted,
       #l_humanity_tasks_inserts.load_dttm,
       #l_humanity_tasks_inserts.ltf_file_name,
       case when l_humanity_tasks.l_humanity_tasks_id is null then isnull(#l_humanity_tasks_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #l_humanity_tasks_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_humanity_tasks_inserts
  left join p_humanity_tasks
    on #l_humanity_tasks_inserts.bk_hash = p_humanity_tasks.bk_hash
   and p_humanity_tasks.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_humanity_tasks
    on p_humanity_tasks.bk_hash = l_humanity_tasks.bk_hash
   and p_humanity_tasks.l_humanity_tasks_id = l_humanity_tasks.l_humanity_tasks_id
 where l_humanity_tasks.l_humanity_tasks_id is null
    or (l_humanity_tasks.l_humanity_tasks_id is not null
        and l_humanity_tasks.dv_hash <> #l_humanity_tasks_inserts.source_hash)

--calculate hash and lookup to current s_humanity_tasks
if object_id('tempdb..#s_humanity_tasks_inserts') is not null drop table #s_humanity_tasks_inserts
create table #s_humanity_tasks_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_tasks.bk_hash,
       stage_hash_humanity_tasks.task_id task_id,
       stage_hash_humanity_tasks.shift_id shift_id,
       stage_hash_humanity_tasks.company_id company_id,
       stage_hash_humanity_tasks.task_name task_name,
       stage_hash_humanity_tasks.created_at created_at,
       stage_hash_humanity_tasks.created_by created_by,
       stage_hash_humanity_tasks.deleted deleted,
       stage_hash_humanity_tasks.load_dttm load_dttm,
       stage_hash_humanity_tasks.ltf_file_name ltf_file_name,
       stage_hash_humanity_tasks.file_arrive_date file_arrive_date,
       stage_hash_humanity_tasks.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_humanity_tasks.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_tasks.task_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_humanity_tasks.shift_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.task_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.created_at,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.created_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.deleted,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.load_dttm,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.ltf_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_tasks.file_arrive_date,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_tasks
 where stage_hash_humanity_tasks.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_humanity_tasks records
set @insert_date_time = getdate()
insert into s_humanity_tasks (
       bk_hash,
       task_id,
       shift_id,
       company_id,
       task_name,
       created_at,
       created_by,
       deleted,
       load_dttm,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_humanity_tasks_inserts.bk_hash,
       #s_humanity_tasks_inserts.task_id,
       #s_humanity_tasks_inserts.shift_id,
       #s_humanity_tasks_inserts.company_id,
       #s_humanity_tasks_inserts.task_name,
       #s_humanity_tasks_inserts.created_at,
       #s_humanity_tasks_inserts.created_by,
       #s_humanity_tasks_inserts.deleted,
       #s_humanity_tasks_inserts.load_dttm,
       #s_humanity_tasks_inserts.ltf_file_name,
       #s_humanity_tasks_inserts.file_arrive_date,
       #s_humanity_tasks_inserts.dummy_modified_date_time,
       case when s_humanity_tasks.s_humanity_tasks_id is null then isnull(#s_humanity_tasks_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #s_humanity_tasks_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_humanity_tasks_inserts
  left join p_humanity_tasks
    on #s_humanity_tasks_inserts.bk_hash = p_humanity_tasks.bk_hash
   and p_humanity_tasks.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_humanity_tasks
    on p_humanity_tasks.bk_hash = s_humanity_tasks.bk_hash
   and p_humanity_tasks.s_humanity_tasks_id = s_humanity_tasks.s_humanity_tasks_id
 where s_humanity_tasks.s_humanity_tasks_id is null
    or (s_humanity_tasks.s_humanity_tasks_id is not null
        and s_humanity_tasks.dv_hash <> #s_humanity_tasks_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_humanity_tasks @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_humanity_tasks @current_dv_batch_id

end
