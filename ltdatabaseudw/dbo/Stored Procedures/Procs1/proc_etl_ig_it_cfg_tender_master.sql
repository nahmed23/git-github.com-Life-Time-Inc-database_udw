CREATE PROC [dbo].[proc_etl_ig_it_cfg_tender_master] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_cfg_Tender_Master

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_cfg_Tender_Master (
       bk_hash,
       ent_id,
       tender_id,
       tender_name,
       tender_abbr1,
       tender_abbr2,
       verification_code_id,
       sales_tippable_flag,
       tender_limit,
       franking_code_id,
       security_id,
       overtender_code_id,
       open_cashdrwr_code_id,
       require_amt_flag,
       check_type_id,
       price_level_id,
       restricted_flag,
       first_tender_flag,
       last_tender_flag,
       num_receipts_print,
       auto_remove_tax_flag,
       enter_tip_prompt,
       discoup_id,
       post_acct_no,
       post_system1_flag,
       post_system2_flag,
       post_system3_flag,
       post_system4_flag,
       post_system5_flag,
       post_system6_flag,
       post_system7_flag,
       post_system8_flag,
       prompt_extra_data_flag,
       post_site_id,
       icc_rate,
       icc_decimal_places,
       prompt_extra_alpha_flag,
       tender_class_id,
       store_id,
       comp_tender_flag,
       prompt_cvv_flag,
       prompt_zipcode_flag,
       use_sigcap_flag,
       use_archive_flag,
       verification_manual_entry_code_id,
       additional_checkid_code_id,
       destination_property_code,
       emv_card_type_code,
       row_version,
       loyalty_earn_eligible_flag,
       tax_comp_code,
       bypass_pds_flag,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ent_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(tender_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ent_id,
       tender_id,
       tender_name,
       tender_abbr1,
       tender_abbr2,
       verification_code_id,
       sales_tippable_flag,
       tender_limit,
       franking_code_id,
       security_id,
       overtender_code_id,
       open_cashdrwr_code_id,
       require_amt_flag,
       check_type_id,
       price_level_id,
       restricted_flag,
       first_tender_flag,
       last_tender_flag,
       num_receipts_print,
       auto_remove_tax_flag,
       enter_tip_prompt,
       discoup_id,
       post_acct_no,
       post_system1_flag,
       post_system2_flag,
       post_system3_flag,
       post_system4_flag,
       post_system5_flag,
       post_system6_flag,
       post_system7_flag,
       post_system8_flag,
       prompt_extra_data_flag,
       post_site_id,
       icc_rate,
       icc_decimal_places,
       prompt_extra_alpha_flag,
       tender_class_id,
       store_id,
       comp_tender_flag,
       prompt_cvv_flag,
       prompt_zipcode_flag,
       use_sigcap_flag,
       use_archive_flag,
       verification_manual_entry_code_id,
       additional_checkid_code_id,
       destination_property_code,
       emv_card_type_code,
       row_version,
       loyalty_earn_eligible_flag,
       tax_comp_code,
       bypass_pds_flag,
       jan_one,
       isnull(cast(stage_ig_it_cfg_Tender_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_it_cfg_Tender_Master
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_cfg_tender_master @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_cfg_tender_master (
       bk_hash,
       ent_id,
       tender_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_it_cfg_Tender_Master.bk_hash,
       stage_hash_ig_it_cfg_Tender_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Tender_Master.tender_id tender_id,
       isnull(cast(stage_hash_ig_it_cfg_Tender_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       16,
       @insert_date_time,
       @user
  from stage_hash_ig_it_cfg_Tender_Master
  left join h_ig_it_cfg_tender_master
    on stage_hash_ig_it_cfg_Tender_Master.bk_hash = h_ig_it_cfg_tender_master.bk_hash
 where h_ig_it_cfg_tender_master_id is null
   and stage_hash_ig_it_cfg_Tender_Master.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_cfg_tender_master
if object_id('tempdb..#l_ig_it_cfg_tender_master_inserts') is not null drop table #l_ig_it_cfg_tender_master_inserts
create table #l_ig_it_cfg_tender_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Tender_Master.bk_hash,
       stage_hash_ig_it_cfg_Tender_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Tender_Master.tender_id tender_id,
       stage_hash_ig_it_cfg_Tender_Master.verification_code_id verification_code_id,
       stage_hash_ig_it_cfg_Tender_Master.franking_code_id franking_code_id,
       stage_hash_ig_it_cfg_Tender_Master.security_id security_id,
       stage_hash_ig_it_cfg_Tender_Master.overtender_code_id over_tender_code_id,
       stage_hash_ig_it_cfg_Tender_Master.open_cashdrwr_code_id open_cashdrwr_code_id,
       stage_hash_ig_it_cfg_Tender_Master.check_type_id check_type_id,
       stage_hash_ig_it_cfg_Tender_Master.price_level_id price_level_id,
       stage_hash_ig_it_cfg_Tender_Master.discoup_id discoup_id,
       stage_hash_ig_it_cfg_Tender_Master.post_site_id post_site_id,
       stage_hash_ig_it_cfg_Tender_Master.tender_class_id tender_class_id,
       stage_hash_ig_it_cfg_Tender_Master.store_id store_id,
       stage_hash_ig_it_cfg_Tender_Master.additional_checkid_code_id additional_check_id_code_id,
       isnull(cast(stage_hash_ig_it_cfg_Tender_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.tender_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.verification_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.franking_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.security_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.overtender_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.open_cashdrwr_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.check_type_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.price_level_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.discoup_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_site_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.tender_class_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.store_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.additional_checkid_code_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Tender_Master
 where stage_hash_ig_it_cfg_Tender_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_cfg_tender_master records
set @insert_date_time = getdate()
insert into l_ig_it_cfg_tender_master (
       bk_hash,
       ent_id,
       tender_id,
       verification_code_id,
       franking_code_id,
       security_id,
       over_tender_code_id,
       open_cashdrwr_code_id,
       check_type_id,
       price_level_id,
       discoup_id,
       post_site_id,
       tender_class_id,
       store_id,
       additional_check_id_code_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_cfg_tender_master_inserts.bk_hash,
       #l_ig_it_cfg_tender_master_inserts.ent_id,
       #l_ig_it_cfg_tender_master_inserts.tender_id,
       #l_ig_it_cfg_tender_master_inserts.verification_code_id,
       #l_ig_it_cfg_tender_master_inserts.franking_code_id,
       #l_ig_it_cfg_tender_master_inserts.security_id,
       #l_ig_it_cfg_tender_master_inserts.over_tender_code_id,
       #l_ig_it_cfg_tender_master_inserts.open_cashdrwr_code_id,
       #l_ig_it_cfg_tender_master_inserts.check_type_id,
       #l_ig_it_cfg_tender_master_inserts.price_level_id,
       #l_ig_it_cfg_tender_master_inserts.discoup_id,
       #l_ig_it_cfg_tender_master_inserts.post_site_id,
       #l_ig_it_cfg_tender_master_inserts.tender_class_id,
       #l_ig_it_cfg_tender_master_inserts.store_id,
       #l_ig_it_cfg_tender_master_inserts.additional_check_id_code_id,
       case when l_ig_it_cfg_tender_master.l_ig_it_cfg_tender_master_id is null then isnull(#l_ig_it_cfg_tender_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #l_ig_it_cfg_tender_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_cfg_tender_master_inserts
  left join p_ig_it_cfg_tender_master
    on #l_ig_it_cfg_tender_master_inserts.bk_hash = p_ig_it_cfg_tender_master.bk_hash
   and p_ig_it_cfg_tender_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_cfg_tender_master
    on p_ig_it_cfg_tender_master.bk_hash = l_ig_it_cfg_tender_master.bk_hash
   and p_ig_it_cfg_tender_master.l_ig_it_cfg_tender_master_id = l_ig_it_cfg_tender_master.l_ig_it_cfg_tender_master_id
 where l_ig_it_cfg_tender_master.l_ig_it_cfg_tender_master_id is null
    or (l_ig_it_cfg_tender_master.l_ig_it_cfg_tender_master_id is not null
        and l_ig_it_cfg_tender_master.dv_hash <> #l_ig_it_cfg_tender_master_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_cfg_tender_master
if object_id('tempdb..#s_ig_it_cfg_tender_master_inserts') is not null drop table #s_ig_it_cfg_tender_master_inserts
create table #s_ig_it_cfg_tender_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_cfg_Tender_Master.bk_hash,
       stage_hash_ig_it_cfg_Tender_Master.ent_id ent_id,
       stage_hash_ig_it_cfg_Tender_Master.tender_id tender_id,
       stage_hash_ig_it_cfg_Tender_Master.tender_name tender_name,
       stage_hash_ig_it_cfg_Tender_Master.tender_abbr1 tender_abbr1,
       stage_hash_ig_it_cfg_Tender_Master.tender_abbr2 tender_abbr2,
       stage_hash_ig_it_cfg_Tender_Master.sales_tippable_flag sales_tippable_flag,
       stage_hash_ig_it_cfg_Tender_Master.tender_limit tender_limit,
       stage_hash_ig_it_cfg_Tender_Master.require_amt_flag require_amt_flag,
       stage_hash_ig_it_cfg_Tender_Master.restricted_flag restricted_flag,
       stage_hash_ig_it_cfg_Tender_Master.first_tender_flag first_tender_flag,
       stage_hash_ig_it_cfg_Tender_Master.last_tender_flag last_tender_flag,
       stage_hash_ig_it_cfg_Tender_Master.num_receipts_print num_receipts_print,
       stage_hash_ig_it_cfg_Tender_Master.auto_remove_tax_flag auto_remove_tax_flag,
       stage_hash_ig_it_cfg_Tender_Master.enter_tip_prompt enter_tip_prompt,
       stage_hash_ig_it_cfg_Tender_Master.post_acct_no post_acct_no,
       stage_hash_ig_it_cfg_Tender_Master.post_system1_flag post_system1_flag,
       stage_hash_ig_it_cfg_Tender_Master.post_system2_flag post_system2_flag,
       stage_hash_ig_it_cfg_Tender_Master.post_system3_flag post_system3_flag,
       stage_hash_ig_it_cfg_Tender_Master.post_system4_flag post_system4_flag,
       stage_hash_ig_it_cfg_Tender_Master.post_system5_flag post_system5_flag,
       stage_hash_ig_it_cfg_Tender_Master.post_system6_flag post_system6_flag,
       stage_hash_ig_it_cfg_Tender_Master.post_system7_flag post_system7_flag,
       stage_hash_ig_it_cfg_Tender_Master.post_system8_flag post_system8_flag,
       stage_hash_ig_it_cfg_Tender_Master.prompt_extra_data_flag prompt_extra_data_flag,
       stage_hash_ig_it_cfg_Tender_Master.icc_rate icc_rate,
       stage_hash_ig_it_cfg_Tender_Master.icc_decimal_places icc_decimal_places,
       stage_hash_ig_it_cfg_Tender_Master.prompt_extra_alpha_flag prompt_extra_alpha_flag,
       stage_hash_ig_it_cfg_Tender_Master.comp_tender_flag comp_tender_flag,
       stage_hash_ig_it_cfg_Tender_Master.prompt_cvv_flag prompt_cvv_flag,
       stage_hash_ig_it_cfg_Tender_Master.prompt_zipcode_flag prompt_zipcode_flag,
       stage_hash_ig_it_cfg_Tender_Master.use_sigcap_flag use_sigcap_flag,
       stage_hash_ig_it_cfg_Tender_Master.use_archive_flag use_archive_flag,
       stage_hash_ig_it_cfg_Tender_Master.verification_manual_entry_code_id verification_manual_entry_code_id,
       stage_hash_ig_it_cfg_Tender_Master.destination_property_code destination_property_code,
       stage_hash_ig_it_cfg_Tender_Master.emv_card_type_code emv_card_type_code,
       stage_hash_ig_it_cfg_Tender_Master.row_version row_version,
       stage_hash_ig_it_cfg_Tender_Master.loyalty_earn_eligible_flag loyalty_earn_eligible_flag,
       stage_hash_ig_it_cfg_Tender_Master.tax_comp_code tax_comp_code,
       stage_hash_ig_it_cfg_Tender_Master.bypass_pds_flag bypass_pds_flag,
       stage_hash_ig_it_cfg_Tender_Master.jan_one jan_one,
       isnull(cast(stage_hash_ig_it_cfg_Tender_Master.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.ent_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.tender_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Tender_Master.tender_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Tender_Master.tender_abbr1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Tender_Master.tender_abbr2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.sales_tippable_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.tender_limit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.require_amt_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.restricted_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.first_tender_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.last_tender_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.num_receipts_print as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.auto_remove_tax_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.enter_tip_prompt as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Tender_Master.post_acct_no,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_system1_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_system2_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_system3_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_system4_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_system5_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_system6_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_system7_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.post_system8_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.prompt_extra_data_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.icc_rate as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.icc_decimal_places as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.prompt_extra_alpha_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.comp_tender_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.prompt_cvv_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.prompt_zipcode_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.use_sigcap_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.use_archive_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.verification_manual_entry_code_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Tender_Master.destination_property_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_cfg_Tender_Master.emv_card_type_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar(500), stage_hash_ig_it_cfg_Tender_Master.row_version, 2),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.loyalty_earn_eligible_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.tax_comp_code as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_cfg_Tender_Master.bypass_pds_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_cfg_Tender_Master.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_cfg_Tender_Master
 where stage_hash_ig_it_cfg_Tender_Master.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_cfg_tender_master records
set @insert_date_time = getdate()
insert into s_ig_it_cfg_tender_master (
       bk_hash,
       ent_id,
       tender_id,
       tender_name,
       tender_abbr1,
       tender_abbr2,
       sales_tippable_flag,
       tender_limit,
       require_amt_flag,
       restricted_flag,
       first_tender_flag,
       last_tender_flag,
       num_receipts_print,
       auto_remove_tax_flag,
       enter_tip_prompt,
       post_acct_no,
       post_system1_flag,
       post_system2_flag,
       post_system3_flag,
       post_system4_flag,
       post_system5_flag,
       post_system6_flag,
       post_system7_flag,
       post_system8_flag,
       prompt_extra_data_flag,
       icc_rate,
       icc_decimal_places,
       prompt_extra_alpha_flag,
       comp_tender_flag,
       prompt_cvv_flag,
       prompt_zipcode_flag,
       use_sigcap_flag,
       use_archive_flag,
       verification_manual_entry_code_id,
       destination_property_code,
       emv_card_type_code,
       row_version,
       loyalty_earn_eligible_flag,
       tax_comp_code,
       bypass_pds_flag,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_cfg_tender_master_inserts.bk_hash,
       #s_ig_it_cfg_tender_master_inserts.ent_id,
       #s_ig_it_cfg_tender_master_inserts.tender_id,
       #s_ig_it_cfg_tender_master_inserts.tender_name,
       #s_ig_it_cfg_tender_master_inserts.tender_abbr1,
       #s_ig_it_cfg_tender_master_inserts.tender_abbr2,
       #s_ig_it_cfg_tender_master_inserts.sales_tippable_flag,
       #s_ig_it_cfg_tender_master_inserts.tender_limit,
       #s_ig_it_cfg_tender_master_inserts.require_amt_flag,
       #s_ig_it_cfg_tender_master_inserts.restricted_flag,
       #s_ig_it_cfg_tender_master_inserts.first_tender_flag,
       #s_ig_it_cfg_tender_master_inserts.last_tender_flag,
       #s_ig_it_cfg_tender_master_inserts.num_receipts_print,
       #s_ig_it_cfg_tender_master_inserts.auto_remove_tax_flag,
       #s_ig_it_cfg_tender_master_inserts.enter_tip_prompt,
       #s_ig_it_cfg_tender_master_inserts.post_acct_no,
       #s_ig_it_cfg_tender_master_inserts.post_system1_flag,
       #s_ig_it_cfg_tender_master_inserts.post_system2_flag,
       #s_ig_it_cfg_tender_master_inserts.post_system3_flag,
       #s_ig_it_cfg_tender_master_inserts.post_system4_flag,
       #s_ig_it_cfg_tender_master_inserts.post_system5_flag,
       #s_ig_it_cfg_tender_master_inserts.post_system6_flag,
       #s_ig_it_cfg_tender_master_inserts.post_system7_flag,
       #s_ig_it_cfg_tender_master_inserts.post_system8_flag,
       #s_ig_it_cfg_tender_master_inserts.prompt_extra_data_flag,
       #s_ig_it_cfg_tender_master_inserts.icc_rate,
       #s_ig_it_cfg_tender_master_inserts.icc_decimal_places,
       #s_ig_it_cfg_tender_master_inserts.prompt_extra_alpha_flag,
       #s_ig_it_cfg_tender_master_inserts.comp_tender_flag,
       #s_ig_it_cfg_tender_master_inserts.prompt_cvv_flag,
       #s_ig_it_cfg_tender_master_inserts.prompt_zipcode_flag,
       #s_ig_it_cfg_tender_master_inserts.use_sigcap_flag,
       #s_ig_it_cfg_tender_master_inserts.use_archive_flag,
       #s_ig_it_cfg_tender_master_inserts.verification_manual_entry_code_id,
       #s_ig_it_cfg_tender_master_inserts.destination_property_code,
       #s_ig_it_cfg_tender_master_inserts.emv_card_type_code,
       #s_ig_it_cfg_tender_master_inserts.row_version,
       #s_ig_it_cfg_tender_master_inserts.loyalty_earn_eligible_flag,
       #s_ig_it_cfg_tender_master_inserts.tax_comp_code,
       #s_ig_it_cfg_tender_master_inserts.bypass_pds_flag,
       #s_ig_it_cfg_tender_master_inserts.jan_one,
       case when s_ig_it_cfg_tender_master.s_ig_it_cfg_tender_master_id is null then isnull(#s_ig_it_cfg_tender_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       16,
       #s_ig_it_cfg_tender_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_cfg_tender_master_inserts
  left join p_ig_it_cfg_tender_master
    on #s_ig_it_cfg_tender_master_inserts.bk_hash = p_ig_it_cfg_tender_master.bk_hash
   and p_ig_it_cfg_tender_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_cfg_tender_master
    on p_ig_it_cfg_tender_master.bk_hash = s_ig_it_cfg_tender_master.bk_hash
   and p_ig_it_cfg_tender_master.s_ig_it_cfg_tender_master_id = s_ig_it_cfg_tender_master.s_ig_it_cfg_tender_master_id
 where s_ig_it_cfg_tender_master.s_ig_it_cfg_tender_master_id is null
    or (s_ig_it_cfg_tender_master.s_ig_it_cfg_tender_master_id is not null
        and s_ig_it_cfg_tender_master.dv_hash <> #s_ig_it_cfg_tender_master_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_cfg_tender_master @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_cfg_tender_master @current_dv_batch_id

end
