CREATE PROC [dbo].[proc_etl_ig_it_cfg_discoup_master] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_cfg_Discoup_Master

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_cfg_Discoup_Master (
       bk_hash,
       ent_id,
       discoup_id,
       discoup_name,
       discoup_abbr1,
       discoup_abbr2,
       discoup_type_code_id,
       discoup_item_level_code_id,
       discoup_open_code_id,
       discoup_pct_amt_code_id,
       discoup_percent,
       discoup_max_percent,
       discoup_amt,
       discoup_max_amt,
       post_acct_no,
       prompt_extra_data_flag,
       threshhold_amt,
       profit_ctr_grp_id,
       round_basis,
       round_type_id,
       store_id,
       assoc_tender_id,
       food_rev_class_flag,
       bev_rev_class_flag,
       soda_rev_class_flag,
       other_rev_class_flag,
       exclusive_flag,
       discount_extra_prompt_code,
       row_version,
       security_id,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ent_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(discoup_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ent_id,
       discoup_id,
       discoup_name,
       discoup_abbr1,
       discoup_abbr2,
       discoup_type_code_id,
       discoup_item_level_code_id,
       discoup_open_code_id,
       discoup_pct_amt_code_id,
       discoup_percent,
       discoup_max_percent,
       discoup_amt,
       discoup_max_amt,
       post_acct_no,
       prompt_extra_data_flag,
       threshhold_amt,
       profit_ctr_grp_id,
       round_basis,
       round_type_id,
       store_id,
       assoc_tender_id,
       food_rev_class_flag,
       bev_rev_class_flag,
       soda_rev_class_flag,
       other_rev_class_flag,
       exclusive_flag,
       discount_extra_prompt_code,
       row_version,
       security_id,
       jan_one,
       isnull(cast(stage_ig_it_cfg_Discoup_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_it_cfg_Discoup_Master
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_cfg_discoup_master @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_cfg_discoup_master (
       bk_hash,
       ent_id,
       discoup_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_it_cfg_Discoup_Master.bk_hash,
       stage_hash_ig_it_cfg_Discoup_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_id discoup_id,
       isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       16,
       @insert_date_time,
       @user
  from stage_hash_ig_it_cfg_Discoup_Master
  left join h_ig_it_cfg_discoup_master
    on stage_hash_ig_it_cfg_Discoup_Master.bk_hash = h_ig_it_cfg_discoup_master.bk_hash
 where h_ig_it_cfg_discoup_master_id is null
   and stage_hash_ig_it_cfg_Discoup_Master.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_cfg_discoup_master
if object_id('tempdb..#l_ig_it_cfg_discoup_master_inserts') is not null drop table #l_ig_it_cfg_discoup_master_inserts
create table #l_ig_it_cfg_discoup_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Discoup_Master.bk_hash,
       stage_hash_ig_it_cfg_Discoup_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_id discoup_id,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_type_code_id discoup_type_code_id,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_item_level_code_id discoup_item_level_code_id,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_open_code_id discoup_open_code_id,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_pct_amt_code_id discoup_pct_amt_code_id,
       stage_hash_ig_it_cfg_Discoup_Master.profit_ctr_grp_id profit_ctr_grp_id,
       stage_hash_ig_it_cfg_Discoup_Master.round_type_id round_type_id,
       stage_hash_ig_it_cfg_Discoup_Master.store_id store_id,
       stage_hash_ig_it_cfg_Discoup_Master.assoc_tender_id assoc_tender_id,
       stage_hash_ig_it_cfg_Discoup_Master.security_id security_id,
       isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_type_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_item_level_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_open_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_pct_amt_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.profit_ctr_grp_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.round_type_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.store_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.assoc_tender_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.security_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Discoup_Master
 where stage_hash_ig_it_cfg_Discoup_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_cfg_discoup_master records
set @insert_date_time = getdate()
insert into l_ig_it_cfg_discoup_master (
       bk_hash,
       ent_id,
       discoup_id,
       discoup_type_code_id,
       discoup_item_level_code_id,
       discoup_open_code_id,
       discoup_pct_amt_code_id,
       profit_ctr_grp_id,
       round_type_id,
       store_id,
       assoc_tender_id,
       security_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_cfg_discoup_master_inserts.bk_hash,
       #l_ig_it_cfg_discoup_master_inserts.ent_id,
       #l_ig_it_cfg_discoup_master_inserts.discoup_id,
       #l_ig_it_cfg_discoup_master_inserts.discoup_type_code_id,
       #l_ig_it_cfg_discoup_master_inserts.discoup_item_level_code_id,
       #l_ig_it_cfg_discoup_master_inserts.discoup_open_code_id,
       #l_ig_it_cfg_discoup_master_inserts.discoup_pct_amt_code_id,
       #l_ig_it_cfg_discoup_master_inserts.profit_ctr_grp_id,
       #l_ig_it_cfg_discoup_master_inserts.round_type_id,
       #l_ig_it_cfg_discoup_master_inserts.store_id,
       #l_ig_it_cfg_discoup_master_inserts.assoc_tender_id,
       #l_ig_it_cfg_discoup_master_inserts.security_id,
       case when l_ig_it_cfg_discoup_master.l_ig_it_cfg_discoup_master_id is null then isnull(#l_ig_it_cfg_discoup_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #l_ig_it_cfg_discoup_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_cfg_discoup_master_inserts
  left join p_ig_it_cfg_discoup_master
    on #l_ig_it_cfg_discoup_master_inserts.bk_hash = p_ig_it_cfg_discoup_master.bk_hash
   and p_ig_it_cfg_discoup_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_cfg_discoup_master
    on p_ig_it_cfg_discoup_master.bk_hash = l_ig_it_cfg_discoup_master.bk_hash
   and p_ig_it_cfg_discoup_master.l_ig_it_cfg_discoup_master_id = l_ig_it_cfg_discoup_master.l_ig_it_cfg_discoup_master_id
 where l_ig_it_cfg_discoup_master.l_ig_it_cfg_discoup_master_id is null
    or (l_ig_it_cfg_discoup_master.l_ig_it_cfg_discoup_master_id is not null
        and l_ig_it_cfg_discoup_master.dv_hash <> #l_ig_it_cfg_discoup_master_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_cfg_discoup_master
if object_id('tempdb..#s_ig_it_cfg_discoup_master_inserts') is not null drop table #s_ig_it_cfg_discoup_master_inserts
create table #s_ig_it_cfg_discoup_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Discoup_Master.bk_hash,
       stage_hash_ig_it_cfg_Discoup_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_id discoup_id,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_name discoup_name,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_abbr1 discoup_abbr1,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_abbr2 discoup_abbr2,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_percent discoup_percent,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_max_percent discoup_max_percent,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_amt discoup_amt,
       stage_hash_ig_it_cfg_Discoup_Master.discoup_max_amt discoup_max_amt,
       stage_hash_ig_it_cfg_Discoup_Master.post_acct_no post_acct_no,
       stage_hash_ig_it_cfg_Discoup_Master.prompt_extra_data_flag prompt_extra_data_flag,
       stage_hash_ig_it_cfg_Discoup_Master.threshhold_amt threshhold_amt,
       stage_hash_ig_it_cfg_Discoup_Master.round_basis round_basis,
       stage_hash_ig_it_cfg_Discoup_Master.food_rev_class_flag food_rev_class_flag,
       stage_hash_ig_it_cfg_Discoup_Master.bev_rev_class_flag bev_rev_class_flag,
       stage_hash_ig_it_cfg_Discoup_Master.soda_rev_class_flag soda_rev_class_flag,
       stage_hash_ig_it_cfg_Discoup_Master.other_rev_class_flag other_rev_class_flag,
       stage_hash_ig_it_cfg_Discoup_Master.exclusive_flag exclusive_flag,
       stage_hash_ig_it_cfg_Discoup_Master.discount_extra_prompt_code discount_extra_prompt_code,
       stage_hash_ig_it_cfg_Discoup_Master.row_version row_version,
       stage_hash_ig_it_cfg_Discoup_Master.jan_one jan_one,
       isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.discoup_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.discoup_abbr1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.discoup_abbr2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_percent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_max_percent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discoup_max_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.post_acct_no,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.prompt_extra_data_flag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.threshhold_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.round_basis as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.food_rev_class_flag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.bev_rev_class_flag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.soda_rev_class_flag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.other_rev_class_flag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Discoup_Master.exclusive_flag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Discoup_Master.discount_extra_prompt_code as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_ig_it_cfg_Discoup_Master.row_version, 2),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Discoup_Master.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Discoup_Master
 where stage_hash_ig_it_cfg_Discoup_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_discoup_master records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_discoup_master (
       bk_hash,
       ent_id,
       discoup_id,
       discoup_name,
       discoup_abbr1,
       discoup_abbr2,
       discoup_percent,
       discoup_max_percent,
       discoup_amt,
       discoup_max_amt,
       post_acct_no,
       prompt_extra_data_flag,
       threshhold_amt,
       round_basis,
       food_rev_class_flag,
       bev_rev_class_flag,
       soda_rev_class_flag,
       other_rev_class_flag,
       exclusive_flag,
       discount_extra_prompt_code,
       row_version,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_discoup_master_inserts.bk_hash,
       #s_ig_it_cfg_discoup_master_inserts.ent_id,
       #s_ig_it_cfg_discoup_master_inserts.discoup_id,
       #s_ig_it_cfg_discoup_master_inserts.discoup_name,
       #s_ig_it_cfg_discoup_master_inserts.discoup_abbr1,
       #s_ig_it_cfg_discoup_master_inserts.discoup_abbr2,
       #s_ig_it_cfg_discoup_master_inserts.discoup_percent,
       #s_ig_it_cfg_discoup_master_inserts.discoup_max_percent,
       #s_ig_it_cfg_discoup_master_inserts.discoup_amt,
       #s_ig_it_cfg_discoup_master_inserts.discoup_max_amt,
       #s_ig_it_cfg_discoup_master_inserts.post_acct_no,
       #s_ig_it_cfg_discoup_master_inserts.prompt_extra_data_flag,
       #s_ig_it_cfg_discoup_master_inserts.threshhold_amt,
       #s_ig_it_cfg_discoup_master_inserts.round_basis,
       #s_ig_it_cfg_discoup_master_inserts.food_rev_class_flag,
       #s_ig_it_cfg_discoup_master_inserts.bev_rev_class_flag,
       #s_ig_it_cfg_discoup_master_inserts.soda_rev_class_flag,
       #s_ig_it_cfg_discoup_master_inserts.other_rev_class_flag,
       #s_ig_it_cfg_discoup_master_inserts.exclusive_flag,
       #s_ig_it_cfg_discoup_master_inserts.discount_extra_prompt_code,
       #s_ig_it_cfg_discoup_master_inserts.row_version,
       #s_ig_it_cfg_discoup_master_inserts.jan_one,
       case when s_ig_it_cfg_discoup_master.s_ig_it_cfg_discoup_master_id is null then isnull(#s_ig_it_cfg_discoup_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_discoup_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_discoup_master_inserts
  left join p_ig_it_cfg_discoup_master
    on #s_ig_it_cfg_discoup_master_inserts.bk_hash = p_ig_it_cfg_discoup_master.bk_hash
   and p_ig_it_cfg_discoup_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_discoup_master
    on p_ig_it_cfg_discoup_master.bk_hash = s_ig_it_cfg_discoup_master.bk_hash
   and p_ig_it_cfg_discoup_master.s_ig_it_cfg_discoup_master_id = s_ig_it_cfg_discoup_master.s_ig_it_cfg_discoup_master_id
 where s_ig_it_cfg_discoup_master.s_ig_it_cfg_discoup_master_id is null
    or (s_ig_it_cfg_discoup_master.s_ig_it_cfg_discoup_master_id is not null
        and s_ig_it_cfg_discoup_master.dv_hash <> #s_ig_it_cfg_discoup_master_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_cfg_discoup_master @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_cfg_discoup_master @current_dv_batch_id

end
