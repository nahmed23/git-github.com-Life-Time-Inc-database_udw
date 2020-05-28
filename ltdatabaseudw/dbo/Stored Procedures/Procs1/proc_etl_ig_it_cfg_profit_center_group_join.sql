CREATE PROC [dbo].[proc_etl_ig_it_cfg_profit_center_group_join] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_cfg_Profit_Center_Group_Join

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_cfg_Profit_Center_Group_Join (
       bk_hash,
       ent_id,
       profit_ctr_grp_id,
       profit_center_id,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ent_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(profit_ctr_grp_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(profit_center_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ent_id,
       profit_ctr_grp_id,
       profit_center_id,
       jan_one,
       isnull(cast(stage_ig_it_cfg_Profit_Center_Group_Join.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_it_cfg_Profit_Center_Group_Join
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_cfg_profit_center_group_join @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_cfg_profit_center_group_join (
       bk_hash,
       ent_id,
       profit_ctr_grp_id,
       profit_center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_it_cfg_Profit_Center_Group_Join.bk_hash,
       stage_hash_ig_it_cfg_Profit_Center_Group_Join.ent_id ent_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Join.profit_ctr_grp_id profit_ctr_grp_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Join.profit_center_id profit_center_id,
       isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Join.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       16,
       @insert_date_time,
       @user
  from stage_hash_ig_it_cfg_Profit_Center_Group_Join
  left join h_ig_it_cfg_profit_center_group_join
    on stage_hash_ig_it_cfg_Profit_Center_Group_Join.bk_hash = h_ig_it_cfg_profit_center_group_join.bk_hash
 where h_ig_it_cfg_profit_center_group_join_id is null
   and stage_hash_ig_it_cfg_Profit_Center_Group_Join.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ig_it_cfg_profit_center_group_join
if object_id('tempdb..#s_ig_it_cfg_profit_center_group_join_inserts') is not null drop table #s_ig_it_cfg_profit_center_group_join_inserts
create table #s_ig_it_cfg_profit_center_group_join_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Profit_Center_Group_Join.bk_hash,
       stage_hash_ig_it_cfg_Profit_Center_Group_Join.ent_id ent_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Join.profit_ctr_grp_id profit_ctr_grp_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Join.profit_center_id profit_center_id,
       stage_hash_ig_it_cfg_Profit_Center_Group_Join.jan_one jan_one,
       isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Join.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Join.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Join.profit_ctr_grp_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Group_Join.profit_center_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Profit_Center_Group_Join.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Profit_Center_Group_Join
 where stage_hash_ig_it_cfg_Profit_Center_Group_Join.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_profit_center_group_join records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_profit_center_group_join (
       bk_hash,
       ent_id,
       profit_ctr_grp_id,
       profit_center_id,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_profit_center_group_join_inserts.bk_hash,
       #s_ig_it_cfg_profit_center_group_join_inserts.ent_id,
       #s_ig_it_cfg_profit_center_group_join_inserts.profit_ctr_grp_id,
       #s_ig_it_cfg_profit_center_group_join_inserts.profit_center_id,
       #s_ig_it_cfg_profit_center_group_join_inserts.jan_one,
       case when s_ig_it_cfg_profit_center_group_join.s_ig_it_cfg_profit_center_group_join_id is null then isnull(#s_ig_it_cfg_profit_center_group_join_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_profit_center_group_join_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_profit_center_group_join_inserts
  left join p_ig_it_cfg_profit_center_group_join
    on #s_ig_it_cfg_profit_center_group_join_inserts.bk_hash = p_ig_it_cfg_profit_center_group_join.bk_hash
   and p_ig_it_cfg_profit_center_group_join.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_profit_center_group_join
    on p_ig_it_cfg_profit_center_group_join.bk_hash = s_ig_it_cfg_profit_center_group_join.bk_hash
   and p_ig_it_cfg_profit_center_group_join.s_ig_it_cfg_profit_center_group_join_id = s_ig_it_cfg_profit_center_group_join.s_ig_it_cfg_profit_center_group_join_id
 where s_ig_it_cfg_profit_center_group_join.s_ig_it_cfg_profit_center_group_join_id is null
    or (s_ig_it_cfg_profit_center_group_join.s_ig_it_cfg_profit_center_group_join_id is not null
        and s_ig_it_cfg_profit_center_group_join.dv_hash <> #s_ig_it_cfg_profit_center_group_join_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_cfg_profit_center_group_join @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_cfg_profit_center_group_join @current_dv_batch_id

end
