CREATE PROC [dbo].[proc_etl_ig_it_cfg_terminal_master] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_cfg_Terminal_Master

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_cfg_Terminal_Master (
       bk_hash,
       term_id,
       term_name,
       IP_address,
       term_grp_id,
       term_printer_grp_id,
       term_service_grp_id,
       term_option_grp_id,
       primary_profit_center_id,
       term_active_flag,
       first_table_no,
       num_tables,
       alt_rcpt_term_id,
       GA_file_load_flag,
       FOL_file_load_flag,
       RMS_file_load_flag,
       current_version,
       default_table_layout_id,
       profile_id,
       alt_bargun_term_id,
       virtual_term_flag,
       bargun_term_flag,
       bargun_bump_override_flag,
       bargun_print_drinks_on_bump_flag,
       ped_value,
       term_receipt_info,
       row_version,
       receipt_printer_type,
       static_receipt_printer_id,
       last_ping_utc_date_time,
       payment_device_id,
       is_default_api_terminal,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(term_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       term_id,
       term_name,
       IP_address,
       term_grp_id,
       term_printer_grp_id,
       term_service_grp_id,
       term_option_grp_id,
       primary_profit_center_id,
       term_active_flag,
       first_table_no,
       num_tables,
       alt_rcpt_term_id,
       GA_file_load_flag,
       FOL_file_load_flag,
       RMS_file_load_flag,
       current_version,
       default_table_layout_id,
       profile_id,
       alt_bargun_term_id,
       virtual_term_flag,
       bargun_term_flag,
       bargun_bump_override_flag,
       bargun_print_drinks_on_bump_flag,
       ped_value,
       term_receipt_info,
       row_version,
       receipt_printer_type,
       static_receipt_printer_id,
       last_ping_utc_date_time,
       payment_device_id,
       is_default_api_terminal,
       dummy_modified_date_time,
       isnull(cast(stage_ig_it_cfg_Terminal_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_it_cfg_Terminal_Master
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_cfg_terminal_master @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_cfg_terminal_master (
       bk_hash,
       term_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_it_cfg_Terminal_Master.bk_hash,
       stage_hash_ig_it_cfg_Terminal_Master.term_id term_id,
       isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       16,
       @insert_date_time,
       @user
  from stage_hash_ig_it_cfg_Terminal_Master
  left join h_ig_it_cfg_terminal_master
    on stage_hash_ig_it_cfg_Terminal_Master.bk_hash = h_ig_it_cfg_terminal_master.bk_hash
 where h_ig_it_cfg_terminal_master_id is null
   and stage_hash_ig_it_cfg_Terminal_Master.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_cfg_terminal_master
if object_id('tempdb..#l_ig_it_cfg_terminal_master_inserts') is not null drop table #l_ig_it_cfg_terminal_master_inserts
create table #l_ig_it_cfg_terminal_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Terminal_Master.bk_hash,
       stage_hash_ig_it_cfg_Terminal_Master.term_id term_id,
       stage_hash_ig_it_cfg_Terminal_Master.term_grp_id term_grp_id,
       stage_hash_ig_it_cfg_Terminal_Master.term_printer_grp_id term_printer_grp_id,
       stage_hash_ig_it_cfg_Terminal_Master.term_service_grp_id term_service_grp_id,
       stage_hash_ig_it_cfg_Terminal_Master.term_option_grp_id term_option_grp_id,
       stage_hash_ig_it_cfg_Terminal_Master.primary_profit_center_id primary_profit_center_id,
       stage_hash_ig_it_cfg_Terminal_Master.alt_rcpt_term_id alt_rcpt_term_id,
       stage_hash_ig_it_cfg_Terminal_Master.profile_id profile_id,
       stage_hash_ig_it_cfg_Terminal_Master.alt_bargun_term_id alt_bargun_term_id,
       isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.term_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.term_grp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.term_printer_grp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.term_service_grp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.term_option_grp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.primary_profit_center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.alt_rcpt_term_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.profile_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.alt_bargun_term_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Terminal_Master
 where stage_hash_ig_it_cfg_Terminal_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_cfg_terminal_master records
set @insert_date_time = getdate()
insert into l_ig_it_cfg_terminal_master (
       bk_hash,
       term_id,
       term_grp_id,
       term_printer_grp_id,
       term_service_grp_id,
       term_option_grp_id,
       primary_profit_center_id,
       alt_rcpt_term_id,
       profile_id,
       alt_bargun_term_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_cfg_terminal_master_inserts.bk_hash,
       #l_ig_it_cfg_terminal_master_inserts.term_id,
       #l_ig_it_cfg_terminal_master_inserts.term_grp_id,
       #l_ig_it_cfg_terminal_master_inserts.term_printer_grp_id,
       #l_ig_it_cfg_terminal_master_inserts.term_service_grp_id,
       #l_ig_it_cfg_terminal_master_inserts.term_option_grp_id,
       #l_ig_it_cfg_terminal_master_inserts.primary_profit_center_id,
       #l_ig_it_cfg_terminal_master_inserts.alt_rcpt_term_id,
       #l_ig_it_cfg_terminal_master_inserts.profile_id,
       #l_ig_it_cfg_terminal_master_inserts.alt_bargun_term_id,
       case when l_ig_it_cfg_terminal_master.l_ig_it_cfg_terminal_master_id is null then isnull(#l_ig_it_cfg_terminal_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #l_ig_it_cfg_terminal_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_cfg_terminal_master_inserts
  left join p_ig_it_cfg_terminal_master
    on #l_ig_it_cfg_terminal_master_inserts.bk_hash = p_ig_it_cfg_terminal_master.bk_hash
   and p_ig_it_cfg_terminal_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_cfg_terminal_master
    on p_ig_it_cfg_terminal_master.bk_hash = l_ig_it_cfg_terminal_master.bk_hash
   and p_ig_it_cfg_terminal_master.l_ig_it_cfg_terminal_master_id = l_ig_it_cfg_terminal_master.l_ig_it_cfg_terminal_master_id
 where l_ig_it_cfg_terminal_master.l_ig_it_cfg_terminal_master_id is null
    or (l_ig_it_cfg_terminal_master.l_ig_it_cfg_terminal_master_id is not null
        and l_ig_it_cfg_terminal_master.dv_hash <> #l_ig_it_cfg_terminal_master_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_cfg_terminal_master
if object_id('tempdb..#s_ig_it_cfg_terminal_master_inserts') is not null drop table #s_ig_it_cfg_terminal_master_inserts
create table #s_ig_it_cfg_terminal_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Terminal_Master.bk_hash,
       stage_hash_ig_it_cfg_Terminal_Master.term_id term_id,
       stage_hash_ig_it_cfg_Terminal_Master.term_name term_name,
       stage_hash_ig_it_cfg_Terminal_Master.IP_address ip_address,
       stage_hash_ig_it_cfg_Terminal_Master.term_active_flag term_active_flag,
       stage_hash_ig_it_cfg_Terminal_Master.first_table_no first_table_no,
       stage_hash_ig_it_cfg_Terminal_Master.num_tables num_tables,
       stage_hash_ig_it_cfg_Terminal_Master.GA_file_load_flag ga_file_load_flag,
       stage_hash_ig_it_cfg_Terminal_Master.FOL_file_load_flag fol_file_load_flag,
       stage_hash_ig_it_cfg_Terminal_Master.RMS_file_load_flag rms_file_load_flag,
       stage_hash_ig_it_cfg_Terminal_Master.current_version current_version,
       stage_hash_ig_it_cfg_Terminal_Master.default_table_layout_id default_table_layout_id,
       stage_hash_ig_it_cfg_Terminal_Master.virtual_term_flag virtual_term_flag,
       stage_hash_ig_it_cfg_Terminal_Master.bargun_term_flag bargun_term_flag,
       stage_hash_ig_it_cfg_Terminal_Master.bargun_bump_override_flag bargun_bump_override_flag,
       stage_hash_ig_it_cfg_Terminal_Master.bargun_print_drinks_on_bump_flag bargun_print_drinks_on_bump_flag,
       stage_hash_ig_it_cfg_Terminal_Master.ped_value ped_value,
       stage_hash_ig_it_cfg_Terminal_Master.term_receipt_info term_receipt_info,
       stage_hash_ig_it_cfg_Terminal_Master.receipt_printer_type receipt_printer_type,
       stage_hash_ig_it_cfg_Terminal_Master.static_receipt_printer_id static_receipt_printer_id,
       stage_hash_ig_it_cfg_Terminal_Master.payment_device_id payment_device_id,
       stage_hash_ig_it_cfg_Terminal_Master.is_default_api_terminal is_default_api_terminal,
       stage_hash_ig_it_cfg_Terminal_Master.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.term_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Terminal_Master.term_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Terminal_Master.IP_address,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.term_active_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.first_table_no as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.num_tables as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.GA_file_load_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.FOL_file_load_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.RMS_file_load_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Terminal_Master.current_version,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.default_table_layout_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.virtual_term_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.bargun_term_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.bargun_bump_override_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.bargun_print_drinks_on_bump_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Terminal_Master.ped_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Terminal_Master.term_receipt_info,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.receipt_printer_type as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.static_receipt_printer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.payment_device_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.is_default_api_terminal as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Terminal_Master
 where stage_hash_ig_it_cfg_Terminal_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_terminal_master records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_terminal_master (
       bk_hash,
       term_id,
       term_name,
       ip_address,
       term_active_flag,
       first_table_no,
       num_tables,
       ga_file_load_flag,
       fol_file_load_flag,
       rms_file_load_flag,
       current_version,
       default_table_layout_id,
       virtual_term_flag,
       bargun_term_flag,
       bargun_bump_override_flag,
       bargun_print_drinks_on_bump_flag,
       ped_value,
       term_receipt_info,
       receipt_printer_type,
       static_receipt_printer_id,
       payment_device_id,
       is_default_api_terminal,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_terminal_master_inserts.bk_hash,
       #s_ig_it_cfg_terminal_master_inserts.term_id,
       #s_ig_it_cfg_terminal_master_inserts.term_name,
       #s_ig_it_cfg_terminal_master_inserts.ip_address,
       #s_ig_it_cfg_terminal_master_inserts.term_active_flag,
       #s_ig_it_cfg_terminal_master_inserts.first_table_no,
       #s_ig_it_cfg_terminal_master_inserts.num_tables,
       #s_ig_it_cfg_terminal_master_inserts.ga_file_load_flag,
       #s_ig_it_cfg_terminal_master_inserts.fol_file_load_flag,
       #s_ig_it_cfg_terminal_master_inserts.rms_file_load_flag,
       #s_ig_it_cfg_terminal_master_inserts.current_version,
       #s_ig_it_cfg_terminal_master_inserts.default_table_layout_id,
       #s_ig_it_cfg_terminal_master_inserts.virtual_term_flag,
       #s_ig_it_cfg_terminal_master_inserts.bargun_term_flag,
       #s_ig_it_cfg_terminal_master_inserts.bargun_bump_override_flag,
       #s_ig_it_cfg_terminal_master_inserts.bargun_print_drinks_on_bump_flag,
       #s_ig_it_cfg_terminal_master_inserts.ped_value,
       #s_ig_it_cfg_terminal_master_inserts.term_receipt_info,
       #s_ig_it_cfg_terminal_master_inserts.receipt_printer_type,
       #s_ig_it_cfg_terminal_master_inserts.static_receipt_printer_id,
       #s_ig_it_cfg_terminal_master_inserts.payment_device_id,
       #s_ig_it_cfg_terminal_master_inserts.is_default_api_terminal,
       #s_ig_it_cfg_terminal_master_inserts.dummy_modified_date_time,
       case when s_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_id is null then isnull(#s_ig_it_cfg_terminal_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_terminal_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_terminal_master_inserts
  left join p_ig_it_cfg_terminal_master
    on #s_ig_it_cfg_terminal_master_inserts.bk_hash = p_ig_it_cfg_terminal_master.bk_hash
   and p_ig_it_cfg_terminal_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_terminal_master
    on p_ig_it_cfg_terminal_master.bk_hash = s_ig_it_cfg_terminal_master.bk_hash
   and p_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_id = s_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_id
 where s_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_id is null
    or (s_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_id is not null
        and s_ig_it_cfg_terminal_master.dv_hash <> #s_ig_it_cfg_terminal_master_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_cfg_terminal_master_1
if object_id('tempdb..#s_ig_it_cfg_terminal_master_1_inserts') is not null drop table #s_ig_it_cfg_terminal_master_1_inserts
create table #s_ig_it_cfg_terminal_master_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Terminal_Master.bk_hash,
       stage_hash_ig_it_cfg_Terminal_Master.term_id term_id,
       stage_hash_ig_it_cfg_Terminal_Master.row_version row_version,
       stage_hash_ig_it_cfg_Terminal_Master.last_ping_utc_date_time last_ping_utc_date_time,
       stage_hash_ig_it_cfg_Terminal_Master.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Terminal_Master.term_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_ig_it_cfg_Terminal_Master.row_version, 2),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Terminal_Master.last_ping_utc_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Terminal_Master
 where stage_hash_ig_it_cfg_Terminal_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_terminal_master_1 records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_terminal_master_1 (
       bk_hash,
       term_id,
       row_version,
       last_ping_utc_date_time,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_terminal_master_1_inserts.bk_hash,
       #s_ig_it_cfg_terminal_master_1_inserts.term_id,
       #s_ig_it_cfg_terminal_master_1_inserts.row_version,
       #s_ig_it_cfg_terminal_master_1_inserts.last_ping_utc_date_time,
       #s_ig_it_cfg_terminal_master_1_inserts.dummy_modified_date_time,
       case when s_ig_it_cfg_terminal_master_1.s_ig_it_cfg_terminal_master_1_id is null then isnull(#s_ig_it_cfg_terminal_master_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_terminal_master_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_terminal_master_1_inserts
  left join p_ig_it_cfg_terminal_master
    on #s_ig_it_cfg_terminal_master_1_inserts.bk_hash = p_ig_it_cfg_terminal_master.bk_hash
   and p_ig_it_cfg_terminal_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_terminal_master_1
    on p_ig_it_cfg_terminal_master.bk_hash = s_ig_it_cfg_terminal_master_1.bk_hash
   and p_ig_it_cfg_terminal_master.s_ig_it_cfg_terminal_master_1_id = s_ig_it_cfg_terminal_master_1.s_ig_it_cfg_terminal_master_1_id
 where s_ig_it_cfg_terminal_master_1.s_ig_it_cfg_terminal_master_1_id is null
    or (s_ig_it_cfg_terminal_master_1.s_ig_it_cfg_terminal_master_1_id is not null
        and s_ig_it_cfg_terminal_master_1.dv_hash <> #s_ig_it_cfg_terminal_master_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_cfg_terminal_master @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_cfg_terminal_master @current_dv_batch_id

end
