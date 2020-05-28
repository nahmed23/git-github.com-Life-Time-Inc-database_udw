CREATE PROC [dbo].[proc_etl_ig_it_trn_emp_check_cum_BD] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_trn_Emp_Check_Cum_BD

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_trn_Emp_Check_Cum_BD (
       bk_hash,
       bus_day_id,
       check_type_id,
       meal_period_id,
       num_checks,
       num_covers,
       profit_center_id,
       server_emp_id,
       void_type_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(bus_day_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(check_type_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(meal_period_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(profit_center_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(server_emp_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(void_type_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       bus_day_id,
       check_type_id,
       meal_period_id,
       num_checks,
       num_covers,
       profit_center_id,
       server_emp_id,
       void_type_id,
       dummy_modified_date_time,
       isnull(cast(stage_ig_it_trn_Emp_Check_Cum_BD.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_it_trn_Emp_Check_Cum_BD
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_trn_emp_check_cum_BD @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_trn_emp_check_cum_BD (
       bk_hash,
       bus_day_id,
       check_type_id,
       meal_period_id,
       profit_center_id,
       server_emp_id,
       void_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_it_trn_Emp_Check_Cum_BD.bk_hash,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.bus_day_id bus_day_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.check_type_id check_type_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.meal_period_id meal_period_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.profit_center_id profit_center_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.server_emp_id server_emp_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.void_type_id void_type_id,
       isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       15,
       @insert_date_time,
       @user
  from stage_hash_ig_it_trn_Emp_Check_Cum_BD
  left join h_ig_it_trn_emp_check_cum_BD
    on stage_hash_ig_it_trn_Emp_Check_Cum_BD.bk_hash = h_ig_it_trn_emp_check_cum_BD.bk_hash
 where h_ig_it_trn_emp_check_cum_BD_id is null
   and stage_hash_ig_it_trn_Emp_Check_Cum_BD.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ig_it_trn_emp_check_cum_BD
if object_id('tempdb..#s_ig_it_trn_emp_check_cum_BD_inserts') is not null drop table #s_ig_it_trn_emp_check_cum_BD_inserts
create table #s_ig_it_trn_emp_check_cum_BD_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Emp_Check_Cum_BD.bk_hash,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.bus_day_id bus_day_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.check_type_id check_type_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.meal_period_id meal_period_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.num_checks num_checks,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.num_covers num_covers,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.profit_center_id profit_center_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.server_emp_id server_emp_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.void_type_id void_type_id,
       stage_hash_ig_it_trn_Emp_Check_Cum_BD.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.bus_day_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.check_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.meal_period_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.num_checks as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.num_covers as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.profit_center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.server_emp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Emp_Check_Cum_BD.void_type_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Emp_Check_Cum_BD
 where stage_hash_ig_it_trn_Emp_Check_Cum_BD.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_trn_emp_check_cum_BD records
set @insert_date_time = getdate()
insert into s_ig_it_trn_emp_check_cum_BD (
       bk_hash,
       bus_day_id,
       check_type_id,
       meal_period_id,
       num_checks,
       num_covers,
       profit_center_id,
       server_emp_id,
       void_type_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_trn_emp_check_cum_BD_inserts.bk_hash,
       #s_ig_it_trn_emp_check_cum_BD_inserts.bus_day_id,
       #s_ig_it_trn_emp_check_cum_BD_inserts.check_type_id,
       #s_ig_it_trn_emp_check_cum_BD_inserts.meal_period_id,
       #s_ig_it_trn_emp_check_cum_BD_inserts.num_checks,
       #s_ig_it_trn_emp_check_cum_BD_inserts.num_covers,
       #s_ig_it_trn_emp_check_cum_BD_inserts.profit_center_id,
       #s_ig_it_trn_emp_check_cum_BD_inserts.server_emp_id,
       #s_ig_it_trn_emp_check_cum_BD_inserts.void_type_id,
       #s_ig_it_trn_emp_check_cum_BD_inserts.dummy_modified_date_time,
       case when s_ig_it_trn_emp_check_cum_BD.s_ig_it_trn_emp_check_cum_BD_id is null then isnull(#s_ig_it_trn_emp_check_cum_BD_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #s_ig_it_trn_emp_check_cum_BD_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_trn_emp_check_cum_BD_inserts
  left join p_ig_it_trn_emp_check_cum_BD
    on #s_ig_it_trn_emp_check_cum_BD_inserts.bk_hash = p_ig_it_trn_emp_check_cum_BD.bk_hash
   and p_ig_it_trn_emp_check_cum_BD.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_trn_emp_check_cum_BD
    on p_ig_it_trn_emp_check_cum_BD.bk_hash = s_ig_it_trn_emp_check_cum_BD.bk_hash
   and p_ig_it_trn_emp_check_cum_BD.s_ig_it_trn_emp_check_cum_BD_id = s_ig_it_trn_emp_check_cum_BD.s_ig_it_trn_emp_check_cum_BD_id
 where s_ig_it_trn_emp_check_cum_BD.s_ig_it_trn_emp_check_cum_BD_id is null
    or (s_ig_it_trn_emp_check_cum_BD.s_ig_it_trn_emp_check_cum_BD_id is not null
        and s_ig_it_trn_emp_check_cum_BD.dv_hash <> #s_ig_it_trn_emp_check_cum_BD_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_trn_emp_check_cum_BD @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_trn_emp_check_cum_BD @current_dv_batch_id

end
