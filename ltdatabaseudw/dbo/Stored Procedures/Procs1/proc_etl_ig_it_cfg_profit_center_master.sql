CREATE PROC [dbo].[proc_etl_ig_it_cfg_profit_center_master] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_cfg_Profit_Center_Master

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_cfg_Profit_Center_Master (
       bk_hash,
       profit_center_id,
       profit_center_name,
       profit_ctr_abbr1,
       profit_ctr_abbr2,
       chk_hdr_line1,
       chk_hdr_line2,
       chk_hdr_line3,
       chk_ftr_line1,
       chk_ftr_line2,
       chk_ftr_line3,
       doc_lines_advance,
       max_doc_lines_page,
       min_rcpt_lines_page,
       sales_tippable_flag,
       print_by_rev_cat_flag,
       data_control_group_id,
       ent_id,
       store_id,
       default_table_layout_id,
       merchant_id,
       bypass_CC_agency_threshold_amount,
       bypass_CC_voice_auth_threshold_amount,
       bypass_CC_printing_threshold_amount,
       primary_language_id,
       secondary_language_id,
       tip_max_percent,
       tip_enforcement_code_id,
       profit_center_desc,
       source_property_code,
       pole_display_open,
       pole_display_closed,
       row_version,
       track_id,
       track_action,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(profit_center_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       profit_center_id,
       profit_center_name,
       profit_ctr_abbr1,
       profit_ctr_abbr2,
       chk_hdr_line1,
       chk_hdr_line2,
       chk_hdr_line3,
       chk_ftr_line1,
       chk_ftr_line2,
       chk_ftr_line3,
       doc_lines_advance,
       max_doc_lines_page,
       min_rcpt_lines_page,
       sales_tippable_flag,
       print_by_rev_cat_flag,
       data_control_group_id,
       ent_id,
       store_id,
       default_table_layout_id,
       merchant_id,
       bypass_CC_agency_threshold_amount,
       bypass_CC_voice_auth_threshold_amount,
       bypass_CC_printing_threshold_amount,
       primary_language_id,
       secondary_language_id,
       tip_max_percent,
       tip_enforcement_code_id,
       profit_center_desc,
       source_property_code,
       pole_display_open,
       pole_display_closed,
       row_version,
       track_id,
       track_action,
       inserted_date_time,
       updated_date_time,
       isnull(cast(stage_ig_it_cfg_Profit_Center_Master.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_it_cfg_Profit_Center_Master
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_cfg_profit_center_master @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_cfg_profit_center_master (
       bk_hash,
       profit_center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_it_cfg_Profit_Center_Master.bk_hash,
       stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_id profit_center_id,
       isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       16,
       @insert_date_time,
       @user
  from stage_hash_ig_it_cfg_Profit_Center_Master
  left join h_ig_it_cfg_profit_center_master
    on stage_hash_ig_it_cfg_Profit_Center_Master.bk_hash = h_ig_it_cfg_profit_center_master.bk_hash
 where h_ig_it_cfg_profit_center_master_id is null
   and stage_hash_ig_it_cfg_Profit_Center_Master.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_cfg_profit_center_master
if object_id('tempdb..#l_ig_it_cfg_profit_center_master_inserts') is not null drop table #l_ig_it_cfg_profit_center_master_inserts
create table #l_ig_it_cfg_profit_center_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Profit_Center_Master.bk_hash,
       stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_id profit_center_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.data_control_group_id data_control_group_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.store_id store_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.default_table_layout_id default_table_layout_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.merchant_id merchant_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.primary_language_id primary_language_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.secondary_language_id secondary_language_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.tip_enforcement_code_id tip_enforcement_code_id,
       isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.data_control_group_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.store_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.default_table_layout_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.merchant_id,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.primary_language_id,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.secondary_language_id,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.tip_enforcement_code_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Profit_Center_Master
 where stage_hash_ig_it_cfg_Profit_Center_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_cfg_profit_center_master records
set @insert_date_time = getdate()
insert into l_ig_it_cfg_profit_center_master (
       bk_hash,
       profit_center_id,
       data_control_group_id,
       ent_id,
       store_id,
       default_table_layout_id,
       merchant_id,
       primary_language_id,
       secondary_language_id,
       tip_enforcement_code_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_cfg_profit_center_master_inserts.bk_hash,
       #l_ig_it_cfg_profit_center_master_inserts.profit_center_id,
       #l_ig_it_cfg_profit_center_master_inserts.data_control_group_id,
       #l_ig_it_cfg_profit_center_master_inserts.ent_id,
       #l_ig_it_cfg_profit_center_master_inserts.store_id,
       #l_ig_it_cfg_profit_center_master_inserts.default_table_layout_id,
       #l_ig_it_cfg_profit_center_master_inserts.merchant_id,
       #l_ig_it_cfg_profit_center_master_inserts.primary_language_id,
       #l_ig_it_cfg_profit_center_master_inserts.secondary_language_id,
       #l_ig_it_cfg_profit_center_master_inserts.tip_enforcement_code_id,
       case when l_ig_it_cfg_profit_center_master.l_ig_it_cfg_profit_center_master_id is null then isnull(#l_ig_it_cfg_profit_center_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #l_ig_it_cfg_profit_center_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_cfg_profit_center_master_inserts
  left join p_ig_it_cfg_profit_center_master
    on #l_ig_it_cfg_profit_center_master_inserts.bk_hash = p_ig_it_cfg_profit_center_master.bk_hash
   and p_ig_it_cfg_profit_center_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_cfg_profit_center_master
    on p_ig_it_cfg_profit_center_master.bk_hash = l_ig_it_cfg_profit_center_master.bk_hash
   and p_ig_it_cfg_profit_center_master.l_ig_it_cfg_profit_center_master_id = l_ig_it_cfg_profit_center_master.l_ig_it_cfg_profit_center_master_id
 where l_ig_it_cfg_profit_center_master.l_ig_it_cfg_profit_center_master_id is null
    or (l_ig_it_cfg_profit_center_master.l_ig_it_cfg_profit_center_master_id is not null
        and l_ig_it_cfg_profit_center_master.dv_hash <> #l_ig_it_cfg_profit_center_master_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_cfg_profit_center_master
if object_id('tempdb..#s_ig_it_cfg_profit_center_master_inserts') is not null drop table #s_ig_it_cfg_profit_center_master_inserts
create table #s_ig_it_cfg_profit_center_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Profit_Center_Master.bk_hash,
       stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_id profit_center_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_name profit_center_name,
       stage_hash_ig_it_cfg_Profit_Center_Master.profit_ctr_abbr1 profit_ctr_abbr1,
       stage_hash_ig_it_cfg_Profit_Center_Master.profit_ctr_abbr2 profit_ctr_abbr2,
       stage_hash_ig_it_cfg_Profit_Center_Master.chk_hdr_line1 chk_hdr_line1,
       stage_hash_ig_it_cfg_Profit_Center_Master.chk_hdr_line2 chk_hdr_line2,
       stage_hash_ig_it_cfg_Profit_Center_Master.chk_hdr_line3 chk_hdr_line3,
       stage_hash_ig_it_cfg_Profit_Center_Master.chk_ftr_line1 chk_ftr_line1,
       stage_hash_ig_it_cfg_Profit_Center_Master.chk_ftr_line2 chk_ftr_line2,
       stage_hash_ig_it_cfg_Profit_Center_Master.chk_ftr_line3 chk_ftr_line3,
       stage_hash_ig_it_cfg_Profit_Center_Master.doc_lines_advance doc_lines_advance,
       stage_hash_ig_it_cfg_Profit_Center_Master.max_doc_lines_page max_doc_lines_page,
       stage_hash_ig_it_cfg_Profit_Center_Master.min_rcpt_lines_page min_rcpt_lines_page,
       stage_hash_ig_it_cfg_Profit_Center_Master.sales_tippable_flag sales_tippable_flag,
       stage_hash_ig_it_cfg_Profit_Center_Master.print_by_rev_cat_flag print_by_rev_cat_flag,
       stage_hash_ig_it_cfg_Profit_Center_Master.bypass_CC_agency_threshold_amount bypass_cc_agency_threshold_amount,
       stage_hash_ig_it_cfg_Profit_Center_Master.bypass_CC_voice_auth_threshold_amount bypass_cc_voice_auth_threshold_amount,
       stage_hash_ig_it_cfg_Profit_Center_Master.bypass_CC_printing_threshold_amount bypass_cc_printing_threshold_amount,
       stage_hash_ig_it_cfg_Profit_Center_Master.tip_max_percent tip_max_percent,
       stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_desc profit_center_desc,
       stage_hash_ig_it_cfg_Profit_Center_Master.source_property_code source_property_code,
       stage_hash_ig_it_cfg_Profit_Center_Master.pole_display_open pole_display_open,
       stage_hash_ig_it_cfg_Profit_Center_Master.pole_display_closed pole_display_closed,
       stage_hash_ig_it_cfg_Profit_Center_Master.row_version row_version,
       stage_hash_ig_it_cfg_Profit_Center_Master.track_id track_id,
       stage_hash_ig_it_cfg_Profit_Center_Master.track_action track_action,
       stage_hash_ig_it_cfg_Profit_Center_Master.inserted_date_time inserted_date_time,
       stage_hash_ig_it_cfg_Profit_Center_Master.updated_date_time updated_date_time,
       isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.inserted_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.profit_ctr_abbr1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.profit_ctr_abbr2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.chk_hdr_line1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.chk_hdr_line2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.chk_hdr_line3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.chk_ftr_line1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.chk_ftr_line2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.chk_ftr_line3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.doc_lines_advance as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.max_doc_lines_page as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.min_rcpt_lines_page as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.sales_tippable_flag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.print_by_rev_cat_flag,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.bypass_CC_agency_threshold_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.bypass_CC_voice_auth_threshold_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.bypass_CC_printing_threshold_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.tip_max_percent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.profit_center_desc,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.source_property_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.pole_display_open,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.pole_display_closed,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_ig_it_cfg_Profit_Center_Master.row_version, 2),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Profit_Center_Master.track_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Profit_Center_Master.track_action,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Profit_Center_Master.inserted_date_time,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Profit_Center_Master.updated_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Profit_Center_Master
 where stage_hash_ig_it_cfg_Profit_Center_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_profit_center_master records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_profit_center_master (
       bk_hash,
       profit_center_id,
       profit_center_name,
       profit_ctr_abbr1,
       profit_ctr_abbr2,
       chk_hdr_line1,
       chk_hdr_line2,
       chk_hdr_line3,
       chk_ftr_line1,
       chk_ftr_line2,
       chk_ftr_line3,
       doc_lines_advance,
       max_doc_lines_page,
       min_rcpt_lines_page,
       sales_tippable_flag,
       print_by_rev_cat_flag,
       bypass_cc_agency_threshold_amount,
       bypass_cc_voice_auth_threshold_amount,
       bypass_cc_printing_threshold_amount,
       tip_max_percent,
       profit_center_desc,
       source_property_code,
       pole_display_open,
       pole_display_closed,
       row_version,
       track_id,
       track_action,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_profit_center_master_inserts.bk_hash,
       #s_ig_it_cfg_profit_center_master_inserts.profit_center_id,
       #s_ig_it_cfg_profit_center_master_inserts.profit_center_name,
       #s_ig_it_cfg_profit_center_master_inserts.profit_ctr_abbr1,
       #s_ig_it_cfg_profit_center_master_inserts.profit_ctr_abbr2,
       #s_ig_it_cfg_profit_center_master_inserts.chk_hdr_line1,
       #s_ig_it_cfg_profit_center_master_inserts.chk_hdr_line2,
       #s_ig_it_cfg_profit_center_master_inserts.chk_hdr_line3,
       #s_ig_it_cfg_profit_center_master_inserts.chk_ftr_line1,
       #s_ig_it_cfg_profit_center_master_inserts.chk_ftr_line2,
       #s_ig_it_cfg_profit_center_master_inserts.chk_ftr_line3,
       #s_ig_it_cfg_profit_center_master_inserts.doc_lines_advance,
       #s_ig_it_cfg_profit_center_master_inserts.max_doc_lines_page,
       #s_ig_it_cfg_profit_center_master_inserts.min_rcpt_lines_page,
       #s_ig_it_cfg_profit_center_master_inserts.sales_tippable_flag,
       #s_ig_it_cfg_profit_center_master_inserts.print_by_rev_cat_flag,
       #s_ig_it_cfg_profit_center_master_inserts.bypass_cc_agency_threshold_amount,
       #s_ig_it_cfg_profit_center_master_inserts.bypass_cc_voice_auth_threshold_amount,
       #s_ig_it_cfg_profit_center_master_inserts.bypass_cc_printing_threshold_amount,
       #s_ig_it_cfg_profit_center_master_inserts.tip_max_percent,
       #s_ig_it_cfg_profit_center_master_inserts.profit_center_desc,
       #s_ig_it_cfg_profit_center_master_inserts.source_property_code,
       #s_ig_it_cfg_profit_center_master_inserts.pole_display_open,
       #s_ig_it_cfg_profit_center_master_inserts.pole_display_closed,
       #s_ig_it_cfg_profit_center_master_inserts.row_version,
       #s_ig_it_cfg_profit_center_master_inserts.track_id,
       #s_ig_it_cfg_profit_center_master_inserts.track_action,
       #s_ig_it_cfg_profit_center_master_inserts.inserted_date_time,
       #s_ig_it_cfg_profit_center_master_inserts.updated_date_time,
       case when s_ig_it_cfg_profit_center_master.s_ig_it_cfg_profit_center_master_id is null then isnull(#s_ig_it_cfg_profit_center_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_profit_center_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_profit_center_master_inserts
  left join p_ig_it_cfg_profit_center_master
    on #s_ig_it_cfg_profit_center_master_inserts.bk_hash = p_ig_it_cfg_profit_center_master.bk_hash
   and p_ig_it_cfg_profit_center_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_profit_center_master
    on p_ig_it_cfg_profit_center_master.bk_hash = s_ig_it_cfg_profit_center_master.bk_hash
   and p_ig_it_cfg_profit_center_master.s_ig_it_cfg_profit_center_master_id = s_ig_it_cfg_profit_center_master.s_ig_it_cfg_profit_center_master_id
 where s_ig_it_cfg_profit_center_master.s_ig_it_cfg_profit_center_master_id is null
    or (s_ig_it_cfg_profit_center_master.s_ig_it_cfg_profit_center_master_id is not null
        and s_ig_it_cfg_profit_center_master.dv_hash <> #s_ig_it_cfg_profit_center_master_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_cfg_profit_center_master @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_cfg_profit_center_master @current_dv_batch_id

end
