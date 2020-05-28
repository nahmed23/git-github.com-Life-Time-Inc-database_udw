CREATE PROC [dbo].[proc_etl_ig_it_trn_order_tender] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_trn_Order_Tender

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_trn_Order_Tender (
       bk_hash,
       adtl_info,
       auth_acct_no,
       breakage_amt,
       change_amt,
       change_tender_id,
       charges_to_date_amt,
       curr_dec_places,
       customer_name,
       exchange_rate,
       order_hdr_id,
       PMS_post_flag,
       post_acct_no,
       post_system1_flag,
       post_system2_flag,
       post_system3_flag,
       post_system4_flag,
       post_system5_flag,
       post_system6_flag,
       post_system7_flag,
       post_system8_flag,
       prorata_discount_amt,
       prorata_grat_amt,
       prorata_sales_amt_gross,
       prorata_svc_chg_amt,
       prorata_tax_amt,
       received_curr_amt,
       remaining_balance_amt,
       sales_tippable_flag,
       subtender_id,
       subtender_qty,
       tax_removed_code,
       tender_amt,
       tender_id,
       tender_seq,
       tender_type_id,
       tip_amt,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(order_hdr_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(tender_seq as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       adtl_info,
       auth_acct_no,
       breakage_amt,
       change_amt,
       change_tender_id,
       charges_to_date_amt,
       curr_dec_places,
       customer_name,
       exchange_rate,
       order_hdr_id,
       PMS_post_flag,
       post_acct_no,
       post_system1_flag,
       post_system2_flag,
       post_system3_flag,
       post_system4_flag,
       post_system5_flag,
       post_system6_flag,
       post_system7_flag,
       post_system8_flag,
       prorata_discount_amt,
       prorata_grat_amt,
       prorata_sales_amt_gross,
       prorata_svc_chg_amt,
       prorata_tax_amt,
       received_curr_amt,
       remaining_balance_amt,
       sales_tippable_flag,
       subtender_id,
       subtender_qty,
       tax_removed_code,
       tender_amt,
       tender_id,
       tender_seq,
       tender_type_id,
       tip_amt,
       jan_one,
       isnull(cast(stage_ig_it_trn_Order_Tender.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_it_trn_Order_Tender
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_trn_order_tender @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_trn_order_tender (
       bk_hash,
       order_hdr_id,
       tender_seq,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_it_trn_Order_Tender.bk_hash,
       stage_hash_ig_it_trn_Order_Tender.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Tender.tender_seq tender_seq,
       isnull(cast(stage_hash_ig_it_trn_Order_Tender.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       15,
       @insert_date_time,
       @user
  from stage_hash_ig_it_trn_Order_Tender
  left join h_ig_it_trn_order_tender
    on stage_hash_ig_it_trn_Order_Tender.bk_hash = h_ig_it_trn_order_tender.bk_hash
 where h_ig_it_trn_order_tender_id is null
   and stage_hash_ig_it_trn_Order_Tender.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_trn_order_tender
if object_id('tempdb..#l_ig_it_trn_order_tender_inserts') is not null drop table #l_ig_it_trn_order_tender_inserts
create table #l_ig_it_trn_order_tender_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Order_Tender.bk_hash,
       stage_hash_ig_it_trn_Order_Tender.change_tender_id change_tender_id,
       stage_hash_ig_it_trn_Order_Tender.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Tender.post_acct_no post_acct_no,
       stage_hash_ig_it_trn_Order_Tender.subtender_id sub_tender_id,
       stage_hash_ig_it_trn_Order_Tender.tender_id tender_id,
       stage_hash_ig_it_trn_Order_Tender.tender_seq tender_seq,
       stage_hash_ig_it_trn_Order_Tender.tender_type_id tender_type_id,
       isnull(cast(stage_hash_ig_it_trn_Order_Tender.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.change_tender_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.order_hdr_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_trn_Order_Tender.post_acct_no,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.subtender_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.tender_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.tender_seq as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.tender_type_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Order_Tender
 where stage_hash_ig_it_trn_Order_Tender.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_trn_order_tender records
set @insert_date_time = getdate()
insert into l_ig_it_trn_order_tender (
       bk_hash,
       change_tender_id,
       order_hdr_id,
       post_acct_no,
       sub_tender_id,
       tender_id,
       tender_seq,
       tender_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_trn_order_tender_inserts.bk_hash,
       #l_ig_it_trn_order_tender_inserts.change_tender_id,
       #l_ig_it_trn_order_tender_inserts.order_hdr_id,
       #l_ig_it_trn_order_tender_inserts.post_acct_no,
       #l_ig_it_trn_order_tender_inserts.sub_tender_id,
       #l_ig_it_trn_order_tender_inserts.tender_id,
       #l_ig_it_trn_order_tender_inserts.tender_seq,
       #l_ig_it_trn_order_tender_inserts.tender_type_id,
       case when l_ig_it_trn_order_tender.l_ig_it_trn_order_tender_id is null then isnull(#l_ig_it_trn_order_tender_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #l_ig_it_trn_order_tender_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_trn_order_tender_inserts
  left join p_ig_it_trn_order_tender
    on #l_ig_it_trn_order_tender_inserts.bk_hash = p_ig_it_trn_order_tender.bk_hash
   and p_ig_it_trn_order_tender.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_trn_order_tender
    on p_ig_it_trn_order_tender.bk_hash = l_ig_it_trn_order_tender.bk_hash
   and p_ig_it_trn_order_tender.l_ig_it_trn_order_tender_id = l_ig_it_trn_order_tender.l_ig_it_trn_order_tender_id
 where l_ig_it_trn_order_tender.l_ig_it_trn_order_tender_id is null
    or (l_ig_it_trn_order_tender.l_ig_it_trn_order_tender_id is not null
        and l_ig_it_trn_order_tender.dv_hash <> #l_ig_it_trn_order_tender_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_trn_order_tender
if object_id('tempdb..#s_ig_it_trn_order_tender_inserts') is not null drop table #s_ig_it_trn_order_tender_inserts
create table #s_ig_it_trn_order_tender_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Order_Tender.bk_hash,
       stage_hash_ig_it_trn_Order_Tender.adtl_info adtl_info,
       stage_hash_ig_it_trn_Order_Tender.auth_acct_no auth_acct_no,
       stage_hash_ig_it_trn_Order_Tender.breakage_amt breakage_amt,
       stage_hash_ig_it_trn_Order_Tender.change_amt change_amt,
       stage_hash_ig_it_trn_Order_Tender.charges_to_date_amt charges_to_date_amt,
       stage_hash_ig_it_trn_Order_Tender.curr_dec_places curr_dec_places,
       stage_hash_ig_it_trn_Order_Tender.customer_name customer_name,
       stage_hash_ig_it_trn_Order_Tender.exchange_rate exchange_rate,
       stage_hash_ig_it_trn_Order_Tender.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Tender.PMS_post_flag pms_post_flag,
       stage_hash_ig_it_trn_Order_Tender.post_system1_flag post_system_1_flag,
       stage_hash_ig_it_trn_Order_Tender.post_system2_flag post_system_2_flag,
       stage_hash_ig_it_trn_Order_Tender.post_system3_flag post_system_3_flag,
       stage_hash_ig_it_trn_Order_Tender.post_system4_flag post_system_4_flag,
       stage_hash_ig_it_trn_Order_Tender.post_system5_flag post_system_5_flag,
       stage_hash_ig_it_trn_Order_Tender.post_system6_flag post_system_6_flag,
       stage_hash_ig_it_trn_Order_Tender.post_system7_flag post_system_7_flag,
       stage_hash_ig_it_trn_Order_Tender.post_system8_flag post_system_8_flag,
       stage_hash_ig_it_trn_Order_Tender.prorata_discount_amt pro_rata_discount_amt,
       stage_hash_ig_it_trn_Order_Tender.prorata_grat_amt pro_rata_grat_amt,
       stage_hash_ig_it_trn_Order_Tender.prorata_sales_amt_gross pro_rata_sales_amt_gross,
       stage_hash_ig_it_trn_Order_Tender.prorata_svc_chg_amt pro_rata_svc_chg_amt,
       stage_hash_ig_it_trn_Order_Tender.prorata_tax_amt pro_rata_tax_amt,
       stage_hash_ig_it_trn_Order_Tender.received_curr_amt received_curr_amt,
       stage_hash_ig_it_trn_Order_Tender.remaining_balance_amt remaining_balance_amt,
       stage_hash_ig_it_trn_Order_Tender.sales_tippable_flag sales_tippable_flag,
       stage_hash_ig_it_trn_Order_Tender.subtender_qty sub_tender_qty,
       stage_hash_ig_it_trn_Order_Tender.tax_removed_code tax_removed_code,
       stage_hash_ig_it_trn_Order_Tender.tender_amt tender_amt,
       stage_hash_ig_it_trn_Order_Tender.tender_seq tender_seq,
       stage_hash_ig_it_trn_Order_Tender.tip_amt tip_amt,
       stage_hash_ig_it_trn_Order_Tender.jan_one jan_one,
       isnull(cast(stage_hash_ig_it_trn_Order_Tender.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_ig_it_trn_Order_Tender.adtl_info,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_trn_Order_Tender.auth_acct_no,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.breakage_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.change_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.charges_to_date_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.curr_dec_places as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_ig_it_trn_Order_Tender.customer_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.exchange_rate as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.order_hdr_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.PMS_post_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.post_system1_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.post_system2_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.post_system3_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.post_system4_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.post_system5_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.post_system6_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.post_system7_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.post_system8_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.prorata_discount_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.prorata_grat_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.prorata_sales_amt_gross as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.prorata_svc_chg_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.prorata_tax_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.received_curr_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.remaining_balance_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.sales_tippable_flag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.subtender_qty as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.tax_removed_code as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.tender_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.tender_seq as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Tender.tip_amt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Order_Tender.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Order_Tender
 where stage_hash_ig_it_trn_Order_Tender.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_trn_order_tender records
set @insert_date_time = getdate()
insert into s_ig_it_trn_order_tender (
       bk_hash,
       adtl_info,
       auth_acct_no,
       breakage_amt,
       change_amt,
       charges_to_date_amt,
       curr_dec_places,
       customer_name,
       exchange_rate,
       order_hdr_id,
       pms_post_flag,
       post_system_1_flag,
       post_system_2_flag,
       post_system_3_flag,
       post_system_4_flag,
       post_system_5_flag,
       post_system_6_flag,
       post_system_7_flag,
       post_system_8_flag,
       pro_rata_discount_amt,
       pro_rata_grat_amt,
       pro_rata_sales_amt_gross,
       pro_rata_svc_chg_amt,
       pro_rata_tax_amt,
       received_curr_amt,
       remaining_balance_amt,
       sales_tippable_flag,
       sub_tender_qty,
       tax_removed_code,
       tender_amt,
       tender_seq,
       tip_amt,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_trn_order_tender_inserts.bk_hash,
       #s_ig_it_trn_order_tender_inserts.adtl_info,
       #s_ig_it_trn_order_tender_inserts.auth_acct_no,
       #s_ig_it_trn_order_tender_inserts.breakage_amt,
       #s_ig_it_trn_order_tender_inserts.change_amt,
       #s_ig_it_trn_order_tender_inserts.charges_to_date_amt,
       #s_ig_it_trn_order_tender_inserts.curr_dec_places,
       #s_ig_it_trn_order_tender_inserts.customer_name,
       #s_ig_it_trn_order_tender_inserts.exchange_rate,
       #s_ig_it_trn_order_tender_inserts.order_hdr_id,
       #s_ig_it_trn_order_tender_inserts.pms_post_flag,
       #s_ig_it_trn_order_tender_inserts.post_system_1_flag,
       #s_ig_it_trn_order_tender_inserts.post_system_2_flag,
       #s_ig_it_trn_order_tender_inserts.post_system_3_flag,
       #s_ig_it_trn_order_tender_inserts.post_system_4_flag,
       #s_ig_it_trn_order_tender_inserts.post_system_5_flag,
       #s_ig_it_trn_order_tender_inserts.post_system_6_flag,
       #s_ig_it_trn_order_tender_inserts.post_system_7_flag,
       #s_ig_it_trn_order_tender_inserts.post_system_8_flag,
       #s_ig_it_trn_order_tender_inserts.pro_rata_discount_amt,
       #s_ig_it_trn_order_tender_inserts.pro_rata_grat_amt,
       #s_ig_it_trn_order_tender_inserts.pro_rata_sales_amt_gross,
       #s_ig_it_trn_order_tender_inserts.pro_rata_svc_chg_amt,
       #s_ig_it_trn_order_tender_inserts.pro_rata_tax_amt,
       #s_ig_it_trn_order_tender_inserts.received_curr_amt,
       #s_ig_it_trn_order_tender_inserts.remaining_balance_amt,
       #s_ig_it_trn_order_tender_inserts.sales_tippable_flag,
       #s_ig_it_trn_order_tender_inserts.sub_tender_qty,
       #s_ig_it_trn_order_tender_inserts.tax_removed_code,
       #s_ig_it_trn_order_tender_inserts.tender_amt,
       #s_ig_it_trn_order_tender_inserts.tender_seq,
       #s_ig_it_trn_order_tender_inserts.tip_amt,
       #s_ig_it_trn_order_tender_inserts.jan_one,
       case when s_ig_it_trn_order_tender.s_ig_it_trn_order_tender_id is null then isnull(#s_ig_it_trn_order_tender_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #s_ig_it_trn_order_tender_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_trn_order_tender_inserts
  left join p_ig_it_trn_order_tender
    on #s_ig_it_trn_order_tender_inserts.bk_hash = p_ig_it_trn_order_tender.bk_hash
   and p_ig_it_trn_order_tender.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_trn_order_tender
    on p_ig_it_trn_order_tender.bk_hash = s_ig_it_trn_order_tender.bk_hash
   and p_ig_it_trn_order_tender.s_ig_it_trn_order_tender_id = s_ig_it_trn_order_tender.s_ig_it_trn_order_tender_id
 where s_ig_it_trn_order_tender.s_ig_it_trn_order_tender_id is null
    or (s_ig_it_trn_order_tender.s_ig_it_trn_order_tender_id is not null
        and s_ig_it_trn_order_tender.dv_hash <> #s_ig_it_trn_order_tender_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_trn_order_tender @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_trn_order_tender @current_dv_batch_id

end
