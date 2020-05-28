CREATE PROC [dbo].[proc_etl_ig_it_cfg_profit_center_group_master] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_cfg_Profit_Center_Group_Master

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_cfg_Profit_Center_Group_Master (
       bk_hash,
       ent_id,
       profit_ctr_grp_id,
       profit_ctr_grp_name,
       profit_ctr_grp_abbr1,
       profit_ctr_grp_abbr2,
       profit_ctr_auto_update_flag,
       charging_pattern,
       data_control_group_id,
       store_id,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ent_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(profit_ctr_grp_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ent_id,
       profit_ctr_grp_id,
       profit_ctr_grp_name,
       profit_ctr_grp_abbr1,
       profit_ctr_grp_abbr2,
       profit_ctr_auto_update_flag,
       charging_pattern,
       data_control_group_id,
       store_id,
       jan_one,
       isnull(cast(stage_ig_it_cfg_Profit_Center_Group_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_it_cfg_Profit_Center_Group_Master
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_cfg_profit_center_group_master @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_cfg_profit_center_group_master (
       bk_hash,
       ent_id,
       profit_ctr_grp_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_it_cfg_Profit_Center_Group_Master.bk_hash,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_id profit_ctr_grp_id,
       isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       16,
       @insert_date_time,
       @user
  from stage_hash_ig_it_cfg_Profit_Center_Group_Master
  left join h_ig_it_cfg_profit_center_group_master
    on stage_hash_ig_it_cfg_Profit_Center_Group_Master.bk_hash = h_ig_it_cfg_profit_center_group_master.bk_hash
 where h_ig_it_cfg_profit_center_group_master_id is null
   and stage_hash_ig_it_cfg_Profit_Center_Group_Master.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_cfg_profit_center_group_master
if object_id('tempdb..#l_ig_it_cfg_profit_center_group_master_inserts') is not null drop table #l_ig_it_cfg_profit_center_group_master_inserts
create table #l_ig_it_cfg_profit_center_group_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Profit_Center_Group_Master.bk_hash,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_id profit_ctr_grp_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.data_control_group_id data_control_group_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.store_id store_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Master.data_control_group_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Master.store_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Profit_Center_Group_Master
 where stage_hash_ig_it_cfg_Profit_Center_Group_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_cfg_profit_center_group_master records
set @insert_date_time = getdate()
insert into l_ig_it_cfg_profit_center_group_master (
       bk_hash,
       ent_id,
       profit_ctr_grp_id,
       data_control_group_id,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_cfg_profit_center_group_master_inserts.bk_hash,
       #l_ig_it_cfg_profit_center_group_master_inserts.ent_id,
       #l_ig_it_cfg_profit_center_group_master_inserts.profit_ctr_grp_id,
       #l_ig_it_cfg_profit_center_group_master_inserts.data_control_group_id,
       #l_ig_it_cfg_profit_center_group_master_inserts.store_id,
       case when l_ig_it_cfg_profit_center_group_master.l_ig_it_cfg_profit_center_group_master_id is null then isnull(#l_ig_it_cfg_profit_center_group_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #l_ig_it_cfg_profit_center_group_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_cfg_profit_center_group_master_inserts
  left join p_ig_it_cfg_profit_center_group_master
    on #l_ig_it_cfg_profit_center_group_master_inserts.bk_hash = p_ig_it_cfg_profit_center_group_master.bk_hash
   and p_ig_it_cfg_profit_center_group_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_cfg_profit_center_group_master
    on p_ig_it_cfg_profit_center_group_master.bk_hash = l_ig_it_cfg_profit_center_group_master.bk_hash
   and p_ig_it_cfg_profit_center_group_master.l_ig_it_cfg_profit_center_group_master_id = l_ig_it_cfg_profit_center_group_master.l_ig_it_cfg_profit_center_group_master_id
 where l_ig_it_cfg_profit_center_group_master.l_ig_it_cfg_profit_center_group_master_id is null
    or (l_ig_it_cfg_profit_center_group_master.l_ig_it_cfg_profit_center_group_master_id is not null
        and l_ig_it_cfg_profit_center_group_master.dv_hash <> #l_ig_it_cfg_profit_center_group_master_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_cfg_profit_center_group_master
if object_id('tempdb..#s_ig_it_cfg_profit_center_group_master_inserts') is not null drop table #s_ig_it_cfg_profit_center_group_master_inserts
create table #s_ig_it_cfg_profit_center_group_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Profit_Center_Group_Master.bk_hash,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_id profit_ctr_grp_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_name profit_ctr_grp_name,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_abbr1 profit_ctr_grp_abbr1,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_abbr2 profit_ctr_grp_abbr2,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_auto_update_flag profit_ctr_auto_update_flag,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.charging_pattern charging_pattern,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.jan_one jan_one,
       stage_hash_ig_it_cfg_Profit_Center_Group_Master.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_abbr1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_grp_abbr2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Master.profit_ctr_auto_update_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Group_Master.charging_pattern,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Profit_Center_Group_Master.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Profit_Center_Group_Master
 where stage_hash_ig_it_cfg_Profit_Center_Group_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_profit_center_group_master records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_profit_center_group_master (
       bk_hash,
       ent_id,
       profit_ctr_grp_id,
       profit_ctr_grp_name,
       profit_ctr_grp_abbr1,
       profit_ctr_grp_abbr2,
       profit_ctr_auto_update_flag,
       charging_pattern,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_profit_center_group_master_inserts.bk_hash,
       #s_ig_it_cfg_profit_center_group_master_inserts.ent_id,
       #s_ig_it_cfg_profit_center_group_master_inserts.profit_ctr_grp_id,
       #s_ig_it_cfg_profit_center_group_master_inserts.profit_ctr_grp_name,
       #s_ig_it_cfg_profit_center_group_master_inserts.profit_ctr_grp_abbr1,
       #s_ig_it_cfg_profit_center_group_master_inserts.profit_ctr_grp_abbr2,
       #s_ig_it_cfg_profit_center_group_master_inserts.profit_ctr_auto_update_flag,
       #s_ig_it_cfg_profit_center_group_master_inserts.charging_pattern,
       #s_ig_it_cfg_profit_center_group_master_inserts.jan_one,
       case when s_ig_it_cfg_profit_center_group_master.s_ig_it_cfg_profit_center_group_master_id is null then isnull(#s_ig_it_cfg_profit_center_group_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_profit_center_group_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_profit_center_group_master_inserts
  left join p_ig_it_cfg_profit_center_group_master
    on #s_ig_it_cfg_profit_center_group_master_inserts.bk_hash = p_ig_it_cfg_profit_center_group_master.bk_hash
   and p_ig_it_cfg_profit_center_group_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_profit_center_group_master
    on p_ig_it_cfg_profit_center_group_master.bk_hash = s_ig_it_cfg_profit_center_group_master.bk_hash
   and p_ig_it_cfg_profit_center_group_master.s_ig_it_cfg_profit_center_group_master_id = s_ig_it_cfg_profit_center_group_master.s_ig_it_cfg_profit_center_group_master_id
 where s_ig_it_cfg_profit_center_group_master.s_ig_it_cfg_profit_center_group_master_id is null
    or (s_ig_it_cfg_profit_center_group_master.s_ig_it_cfg_profit_center_group_master_id is not null
        and s_ig_it_cfg_profit_center_group_master.dv_hash <> #s_ig_it_cfg_profit_center_group_master_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_cfg_profit_center_group_master @current_dv_batch_id

end
