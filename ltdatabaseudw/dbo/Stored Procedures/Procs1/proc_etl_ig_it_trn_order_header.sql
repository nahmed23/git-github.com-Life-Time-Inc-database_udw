CREATE PROC [dbo].[proc_etl_ig_it_trn_order_header] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_trn_Order_Header

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_trn_Order_Header (
       bk_hash,
       assoc_check_no,
       bus_day_id,
       cashier_emp_id,
       check_no,
       check_status,
       check_type_id,
       close_dttime,
       close_term_id,
       discount_amt,
       drawer_no,
       grat_amt,
       meal_period_id,
       num_covers,
       open_dttime,
       open_term_id,
       order_hdr_id,
       order_process_dttime,
       pretender_flag,
       print_count,
       profit_center_id,
       refund_flag,
       sales_amt_gross,
       sales_tippable_flag,
       server_emp_id,
       service_charge_amt,
       table_alpha_no,
       table_no,
       tax_amt,
       tax_removd_flag,
       tender_amt_gross,
       tender_id,
       tip_amt,
       tran_data_tag_text,
       void_reason_id,
       jan_one,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(order_hdr_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       assoc_check_no,
       bus_day_id,
       cashier_emp_id,
       check_no,
       check_status,
       check_type_id,
       close_dttime,
       close_term_id,
       discount_amt,
       drawer_no,
       grat_amt,
       meal_period_id,
       num_covers,
       open_dttime,
       open_term_id,
       order_hdr_id,
       order_process_dttime,
       pretender_flag,
       print_count,
       profit_center_id,
       refund_flag,
       sales_amt_gross,
       sales_tippable_flag,
       server_emp_id,
       service_charge_amt,
       table_alpha_no,
       table_no,
       tax_amt,
       tax_removd_flag,
       tender_amt_gross,
       tender_id,
       tip_amt,
       tran_data_tag_text,
       void_reason_id,
       jan_one,
       isnull(cast(stage_ig_it_trn_Order_Header.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_it_trn_Order_Header
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_trn_order_header @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_trn_order_header (
       bk_hash,
       order_hdr_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_it_trn_Order_Header.bk_hash,
       stage_hash_ig_it_trn_Order_Header.order_hdr_id order_hdr_id,
       isnull(cast(stage_hash_ig_it_trn_Order_Header.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       15,
       @insert_date_time,
       @user
  from stage_hash_ig_it_trn_Order_Header
  left join h_ig_it_trn_order_header
    on stage_hash_ig_it_trn_Order_Header.bk_hash = h_ig_it_trn_order_header.bk_hash
 where h_ig_it_trn_order_header_id is null
   and stage_hash_ig_it_trn_Order_Header.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_it_trn_order_header
if object_id('tempdb..#l_ig_it_trn_order_header_inserts') is not null drop table #l_ig_it_trn_order_header_inserts
create table #l_ig_it_trn_order_header_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Order_Header.bk_hash,
       stage_hash_ig_it_trn_Order_Header.bus_day_id bus_day_id,
       stage_hash_ig_it_trn_Order_Header.cashier_emp_id cashier_emp_id,
       stage_hash_ig_it_trn_Order_Header.check_type_id check_type_id,
       stage_hash_ig_it_trn_Order_Header.close_term_id close_term_id,
       stage_hash_ig_it_trn_Order_Header.meal_period_id meal_period_id,
       stage_hash_ig_it_trn_Order_Header.open_term_id open_term_id,
       stage_hash_ig_it_trn_Order_Header.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Header.profit_center_id profit_center_id,
       stage_hash_ig_it_trn_Order_Header.server_emp_id server_emp_id,
       stage_hash_ig_it_trn_Order_Header.tender_id tender_id,
       stage_hash_ig_it_trn_Order_Header.void_reason_id void_reason_id,
       isnull(cast(stage_hash_ig_it_trn_Order_Header.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.bus_day_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.cashier_emp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.check_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.close_term_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.meal_period_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.open_term_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.order_hdr_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.profit_center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.server_emp_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.tender_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.void_reason_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Order_Header
 where stage_hash_ig_it_trn_Order_Header.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_it_trn_order_header records
set @insert_date_time = getdate()
insert into l_ig_it_trn_order_header (
       bk_hash,
       bus_day_id,
       cashier_emp_id,
       check_type_id,
       close_term_id,
       meal_period_id,
       open_term_id,
       order_hdr_id,
       profit_center_id,
       server_emp_id,
       tender_id,
       void_reason_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_it_trn_order_header_inserts.bk_hash,
       #l_ig_it_trn_order_header_inserts.bus_day_id,
       #l_ig_it_trn_order_header_inserts.cashier_emp_id,
       #l_ig_it_trn_order_header_inserts.check_type_id,
       #l_ig_it_trn_order_header_inserts.close_term_id,
       #l_ig_it_trn_order_header_inserts.meal_period_id,
       #l_ig_it_trn_order_header_inserts.open_term_id,
       #l_ig_it_trn_order_header_inserts.order_hdr_id,
       #l_ig_it_trn_order_header_inserts.profit_center_id,
       #l_ig_it_trn_order_header_inserts.server_emp_id,
       #l_ig_it_trn_order_header_inserts.tender_id,
       #l_ig_it_trn_order_header_inserts.void_reason_id,
       case when l_ig_it_trn_order_header.l_ig_it_trn_order_header_id is null then isnull(#l_ig_it_trn_order_header_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #l_ig_it_trn_order_header_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_it_trn_order_header_inserts
  left join p_ig_it_trn_order_header
    on #l_ig_it_trn_order_header_inserts.bk_hash = p_ig_it_trn_order_header.bk_hash
   and p_ig_it_trn_order_header.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_it_trn_order_header
    on p_ig_it_trn_order_header.bk_hash = l_ig_it_trn_order_header.bk_hash
   and p_ig_it_trn_order_header.l_ig_it_trn_order_header_id = l_ig_it_trn_order_header.l_ig_it_trn_order_header_id
 where l_ig_it_trn_order_header.l_ig_it_trn_order_header_id is null
    or (l_ig_it_trn_order_header.l_ig_it_trn_order_header_id is not null
        and l_ig_it_trn_order_header.dv_hash <> #l_ig_it_trn_order_header_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_trn_order_header
if object_id('tempdb..#s_ig_it_trn_order_header_inserts') is not null drop table #s_ig_it_trn_order_header_inserts
create table #s_ig_it_trn_order_header_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Order_Header.bk_hash,
       stage_hash_ig_it_trn_Order_Header.assoc_check_no assoc_check_no,
       stage_hash_ig_it_trn_Order_Header.check_no check_no,
       stage_hash_ig_it_trn_Order_Header.check_status check_status,
       stage_hash_ig_it_trn_Order_Header.close_dttime close_dttime,
       stage_hash_ig_it_trn_Order_Header.discount_amt discount_amt,
       stage_hash_ig_it_trn_Order_Header.drawer_no drawer_no,
       stage_hash_ig_it_trn_Order_Header.grat_amt grat_amt,
       stage_hash_ig_it_trn_Order_Header.num_covers num_covers,
       stage_hash_ig_it_trn_Order_Header.open_dttime open_dttime,
       stage_hash_ig_it_trn_Order_Header.order_hdr_id order_hdr_id,
       stage_hash_ig_it_trn_Order_Header.order_process_dttime order_process_dttime,
       stage_hash_ig_it_trn_Order_Header.pretender_flag pretender_flag,
       stage_hash_ig_it_trn_Order_Header.print_count print_count,
       stage_hash_ig_it_trn_Order_Header.refund_flag refund_flag,
       stage_hash_ig_it_trn_Order_Header.sales_amt_gross sales_amt_gross,
       stage_hash_ig_it_trn_Order_Header.sales_tippable_flag sales_tippable_flag,
       stage_hash_ig_it_trn_Order_Header.service_charge_amt service_charge_amt,
       stage_hash_ig_it_trn_Order_Header.table_alpha_no table_alpha_no,
       stage_hash_ig_it_trn_Order_Header.table_no table_no,
       stage_hash_ig_it_trn_Order_Header.tax_amt tax_amt,
       stage_hash_ig_it_trn_Order_Header.tax_removd_flag tax_removd_flag,
       stage_hash_ig_it_trn_Order_Header.tender_amt_gross tender_amt_gross,
       stage_hash_ig_it_trn_Order_Header.tip_amt tip_amt,
       stage_hash_ig_it_trn_Order_Header.tran_data_tag_text tran_data_tag_text,
       stage_hash_ig_it_trn_Order_Header.jan_one jan_one,
       isnull(cast(stage_hash_ig_it_trn_Order_Header.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.assoc_check_no as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.check_no as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.check_status as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Order_Header.close_dttime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.discount_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.drawer_no as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.grat_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.num_covers as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Order_Header.open_dttime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.order_hdr_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Order_Header.order_process_dttime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.pretender_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.print_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.refund_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.sales_amt_gross as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.sales_tippable_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.service_charge_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_trn_Order_Header.table_alpha_no,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.table_no as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.tax_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.tax_removd_flag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.tender_amt_gross as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Order_Header.tip_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_it_trn_Order_Header.tran_data_tag_text,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Order_Header.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Order_Header
 where stage_hash_ig_it_trn_Order_Header.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_trn_order_header records
set @insert_date_time = getdate()
insert into s_ig_it_trn_order_header (
       bk_hash,
       assoc_check_no,
       check_no,
       check_status,
       close_dttime,
       discount_amt,
       drawer_no,
       grat_amt,
       num_covers,
       open_dttime,
       order_hdr_id,
       order_process_dttime,
       pretender_flag,
       print_count,
       refund_flag,
       sales_amt_gross,
       sales_tippable_flag,
       service_charge_amt,
       table_alpha_no,
       table_no,
       tax_amt,
       tax_removd_flag,
       tender_amt_gross,
       tip_amt,
       tran_data_tag_text,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_trn_order_header_inserts.bk_hash,
       #s_ig_it_trn_order_header_inserts.assoc_check_no,
       #s_ig_it_trn_order_header_inserts.check_no,
       #s_ig_it_trn_order_header_inserts.check_status,
       #s_ig_it_trn_order_header_inserts.close_dttime,
       #s_ig_it_trn_order_header_inserts.discount_amt,
       #s_ig_it_trn_order_header_inserts.drawer_no,
       #s_ig_it_trn_order_header_inserts.grat_amt,
       #s_ig_it_trn_order_header_inserts.num_covers,
       #s_ig_it_trn_order_header_inserts.open_dttime,
       #s_ig_it_trn_order_header_inserts.order_hdr_id,
       #s_ig_it_trn_order_header_inserts.order_process_dttime,
       #s_ig_it_trn_order_header_inserts.pretender_flag,
       #s_ig_it_trn_order_header_inserts.print_count,
       #s_ig_it_trn_order_header_inserts.refund_flag,
       #s_ig_it_trn_order_header_inserts.sales_amt_gross,
       #s_ig_it_trn_order_header_inserts.sales_tippable_flag,
       #s_ig_it_trn_order_header_inserts.service_charge_amt,
       #s_ig_it_trn_order_header_inserts.table_alpha_no,
       #s_ig_it_trn_order_header_inserts.table_no,
       #s_ig_it_trn_order_header_inserts.tax_amt,
       #s_ig_it_trn_order_header_inserts.tax_removd_flag,
       #s_ig_it_trn_order_header_inserts.tender_amt_gross,
       #s_ig_it_trn_order_header_inserts.tip_amt,
       #s_ig_it_trn_order_header_inserts.tran_data_tag_text,
       #s_ig_it_trn_order_header_inserts.jan_one,
       case when s_ig_it_trn_order_header.s_ig_it_trn_order_header_id is null then isnull(#s_ig_it_trn_order_header_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #s_ig_it_trn_order_header_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_trn_order_header_inserts
  left join p_ig_it_trn_order_header
    on #s_ig_it_trn_order_header_inserts.bk_hash = p_ig_it_trn_order_header.bk_hash
   and p_ig_it_trn_order_header.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_trn_order_header
    on p_ig_it_trn_order_header.bk_hash = s_ig_it_trn_order_header.bk_hash
   and p_ig_it_trn_order_header.s_ig_it_trn_order_header_id = s_ig_it_trn_order_header.s_ig_it_trn_order_header_id
 where s_ig_it_trn_order_header.s_ig_it_trn_order_header_id is null
    or (s_ig_it_trn_order_header.s_ig_it_trn_order_header_id is not null
        and s_ig_it_trn_order_header.dv_hash <> #s_ig_it_trn_order_header_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_trn_order_header @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_trn_order_header @current_dv_batch_id

end
