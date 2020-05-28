CREATE PROC [dbo].[proc_etl_ig_it_cfg_emp_master] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_cfg_Emp_Master

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_cfg_Emp_Master (
       bk_hash,
       emp_id,
       emp_pos_name,
       emp_first_name,
       emp_last_name,
       supervisor_emp_id,
       emp_hire_dt,
       emp_terminate_dt,
       emp_card_no,
       store_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(emp_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       emp_id,
       emp_pos_name,
       emp_first_name,
       emp_last_name,
       supervisor_emp_id,
       emp_hire_dt,
       emp_terminate_dt,
       emp_card_no,
       store_id,
       dummy_modified_date_time,
       isnull(cast(stage_ig_it_cfg_Emp_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_it_cfg_Emp_Master
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_cfg_emp_master @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_cfg_emp_master (
       bk_hash,
       emp_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_it_cfg_Emp_Master.bk_hash,
       stage_hash_ig_it_cfg_Emp_Master.emp_id emp_id,
       isnull(cast(stage_hash_ig_it_cfg_Emp_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       16,
       @insert_date_time,
       @user
  from stage_hash_ig_it_cfg_Emp_Master
  left join h_ig_it_cfg_emp_master
    on stage_hash_ig_it_cfg_Emp_Master.bk_hash = h_ig_it_cfg_emp_master.bk_hash
 where h_ig_it_cfg_emp_master_id is null
   and stage_hash_ig_it_cfg_Emp_Master.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_cfg_emp_master
if object_id('tempdb..#l_ig_it_cfg_emp_master_inserts') is not null drop table #l_ig_it_cfg_emp_master_inserts
create table #l_ig_it_cfg_emp_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Emp_Master.bk_hash,
       stage_hash_ig_it_cfg_Emp_Master.emp_id emp_id,
       stage_hash_ig_it_cfg_Emp_Master.supervisor_emp_id supervisor_emp_id,
       stage_hash_ig_it_cfg_Emp_Master.store_id store_id,
       isnull(cast(stage_hash_ig_it_cfg_Emp_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Emp_Master.emp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Emp_Master.supervisor_emp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Emp_Master.store_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Emp_Master
 where stage_hash_ig_it_cfg_Emp_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_cfg_emp_master records
set @insert_date_time = getdate()
insert into l_ig_it_cfg_emp_master (
       bk_hash,
       emp_id,
       supervisor_emp_id,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_cfg_emp_master_inserts.bk_hash,
       #l_ig_it_cfg_emp_master_inserts.emp_id,
       #l_ig_it_cfg_emp_master_inserts.supervisor_emp_id,
       #l_ig_it_cfg_emp_master_inserts.store_id,
       case when l_ig_it_cfg_emp_master.l_ig_it_cfg_emp_master_id is null then isnull(#l_ig_it_cfg_emp_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #l_ig_it_cfg_emp_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_cfg_emp_master_inserts
  left join p_ig_it_cfg_emp_master
    on #l_ig_it_cfg_emp_master_inserts.bk_hash = p_ig_it_cfg_emp_master.bk_hash
   and p_ig_it_cfg_emp_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_cfg_emp_master
    on p_ig_it_cfg_emp_master.bk_hash = l_ig_it_cfg_emp_master.bk_hash
   and p_ig_it_cfg_emp_master.l_ig_it_cfg_emp_master_id = l_ig_it_cfg_emp_master.l_ig_it_cfg_emp_master_id
 where l_ig_it_cfg_emp_master.l_ig_it_cfg_emp_master_id is null
    or (l_ig_it_cfg_emp_master.l_ig_it_cfg_emp_master_id is not null
        and l_ig_it_cfg_emp_master.dv_hash <> #l_ig_it_cfg_emp_master_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_cfg_emp_master
if object_id('tempdb..#s_ig_it_cfg_emp_master_inserts') is not null drop table #s_ig_it_cfg_emp_master_inserts
create table #s_ig_it_cfg_emp_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Emp_Master.bk_hash,
       stage_hash_ig_it_cfg_Emp_Master.emp_id emp_id,
       stage_hash_ig_it_cfg_Emp_Master.emp_pos_name emp_pos_name,
       stage_hash_ig_it_cfg_Emp_Master.emp_first_name emp_first_name,
       stage_hash_ig_it_cfg_Emp_Master.emp_last_name emp_last_name,
       stage_hash_ig_it_cfg_Emp_Master.emp_hire_dt emp_hire_dt,
       stage_hash_ig_it_cfg_Emp_Master.emp_terminate_dt emp_terminate_dt,
       stage_hash_ig_it_cfg_Emp_Master.emp_card_no emp_card_no,
       stage_hash_ig_it_cfg_Emp_Master.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_ig_it_cfg_Emp_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Emp_Master.emp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Emp_Master.emp_pos_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Emp_Master.emp_first_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Emp_Master.emp_last_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Emp_Master.emp_hire_dt,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Emp_Master.emp_terminate_dt,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Emp_Master.emp_card_no as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Emp_Master
 where stage_hash_ig_it_cfg_Emp_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_emp_master records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_emp_master (
       bk_hash,
       emp_id,
       emp_pos_name,
       emp_first_name,
       emp_last_name,
       emp_hire_dt,
       emp_terminate_dt,
       emp_card_no,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_emp_master_inserts.bk_hash,
       #s_ig_it_cfg_emp_master_inserts.emp_id,
       #s_ig_it_cfg_emp_master_inserts.emp_pos_name,
       #s_ig_it_cfg_emp_master_inserts.emp_first_name,
       #s_ig_it_cfg_emp_master_inserts.emp_last_name,
       #s_ig_it_cfg_emp_master_inserts.emp_hire_dt,
       #s_ig_it_cfg_emp_master_inserts.emp_terminate_dt,
       #s_ig_it_cfg_emp_master_inserts.emp_card_no,
       #s_ig_it_cfg_emp_master_inserts.dummy_modified_date_time,
       case when s_ig_it_cfg_emp_master.s_ig_it_cfg_emp_master_id is null then isnull(#s_ig_it_cfg_emp_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_emp_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_emp_master_inserts
  left join p_ig_it_cfg_emp_master
    on #s_ig_it_cfg_emp_master_inserts.bk_hash = p_ig_it_cfg_emp_master.bk_hash
   and p_ig_it_cfg_emp_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_emp_master
    on p_ig_it_cfg_emp_master.bk_hash = s_ig_it_cfg_emp_master.bk_hash
   and p_ig_it_cfg_emp_master.s_ig_it_cfg_emp_master_id = s_ig_it_cfg_emp_master.s_ig_it_cfg_emp_master_id
 where s_ig_it_cfg_emp_master.s_ig_it_cfg_emp_master_id is null
    or (s_ig_it_cfg_emp_master.s_ig_it_cfg_emp_master_id is not null
        and s_ig_it_cfg_emp_master.dv_hash <> #s_ig_it_cfg_emp_master_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_cfg_emp_master @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_cfg_emp_master @current_dv_batch_id

end
