CREATE PROC [dbo].[proc_etl_ig_it_trn_terminal_shift_cash_BD] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_it_trn_Terminal_Shift_Cash_BD

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_it_trn_Terminal_Shift_Cash_BD (
       bk_hash,
       breakage_amt,
       bus_day_id,
       cash_drop_amt,
       cash_shift_id,
       change_amt,
       loan_amt,
       num_tendered_checks,
       paidout_amt,
       received_curr_amt,
       tender_amt,
       tender_id,
       tender_qty,
       term_id,
       withdrawal_amt,
       BD_start_dttime,
       BD_end_dttime,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(bus_day_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cash_shift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(tender_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(term_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       breakage_amt,
       bus_day_id,
       cash_drop_amt,
       cash_shift_id,
       change_amt,
       loan_amt,
       num_tendered_checks,
       paidout_amt,
       received_curr_amt,
       tender_amt,
       tender_id,
       tender_qty,
       term_id,
       withdrawal_amt,
       BD_start_dttime,
       BD_end_dttime,
       dummy_modified_date_time,
       isnull(cast(stage_ig_it_trn_Terminal_Shift_Cash_BD.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ig_it_trn_Terminal_Shift_Cash_BD
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_it_trn_terminal_shift_cash_BD @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_it_trn_terminal_shift_cash_BD (
       bk_hash,
       bus_day_id,
       cash_shift_id,
       tender_id,
       term_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bk_hash,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bus_day_id bus_day_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.cash_shift_id cash_shift_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_id tender_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.term_id term_id,
       isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       15,
       @insert_date_time,
       @user
  from stage_hash_ig_it_trn_Terminal_Shift_Cash_BD
  left join h_ig_it_trn_terminal_shift_cash_BD
    on stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bk_hash = h_ig_it_trn_terminal_shift_cash_BD.bk_hash
 where h_ig_it_trn_terminal_shift_cash_BD_id is null
   and stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ig_it_trn_terminal_shift_cash_BD
if object_id('tempdb..#s_ig_it_trn_terminal_shift_cash_BD_inserts') is not null drop table #s_ig_it_trn_terminal_shift_cash_BD_inserts
create table #s_ig_it_trn_terminal_shift_cash_BD_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bk_hash,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.breakage_amt breakage_amt,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bus_day_id bus_day_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.cash_drop_amt cash_drop_amt,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.cash_shift_id cash_shift_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.change_amt change_amt,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.loan_amt loan_amt,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.num_tendered_checks num_tendered_checks,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.paidout_amt paidout_amt,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.received_curr_amt received_curr_amt,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_amt tender_amt,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_id tender_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_qty tender_qty,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.term_id term_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.withdrawal_amt withdrawal_amt,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.breakage_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bus_day_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.cash_drop_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.cash_shift_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.change_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.loan_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.num_tendered_checks as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.paidout_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.received_curr_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_amt as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.term_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.withdrawal_amt as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Terminal_Shift_Cash_BD
 where stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_trn_terminal_shift_cash_BD records
set @insert_date_time = getdate()
insert into s_ig_it_trn_terminal_shift_cash_BD (
       bk_hash,
       breakage_amt,
       bus_day_id,
       cash_drop_amt,
       cash_shift_id,
       change_amt,
       loan_amt,
       num_tendered_checks,
       paidout_amt,
       received_curr_amt,
       tender_amt,
       tender_id,
       tender_qty,
       term_id,
       withdrawal_amt,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_trn_terminal_shift_cash_BD_inserts.bk_hash,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.breakage_amt,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.bus_day_id,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.cash_drop_amt,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.cash_shift_id,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.change_amt,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.loan_amt,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.num_tendered_checks,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.paidout_amt,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.received_curr_amt,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.tender_amt,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.tender_id,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.tender_qty,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.term_id,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.withdrawal_amt,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.dummy_modified_date_time,
       case when s_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_id is null then isnull(#s_ig_it_trn_terminal_shift_cash_BD_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #s_ig_it_trn_terminal_shift_cash_BD_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_trn_terminal_shift_cash_BD_inserts
  left join p_ig_it_trn_terminal_shift_cash_BD
    on #s_ig_it_trn_terminal_shift_cash_BD_inserts.bk_hash = p_ig_it_trn_terminal_shift_cash_BD.bk_hash
   and p_ig_it_trn_terminal_shift_cash_BD.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_trn_terminal_shift_cash_BD
    on p_ig_it_trn_terminal_shift_cash_BD.bk_hash = s_ig_it_trn_terminal_shift_cash_BD.bk_hash
   and p_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_id = s_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_id
 where s_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_id is null
    or (s_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_id is not null
        and s_ig_it_trn_terminal_shift_cash_BD.dv_hash <> #s_ig_it_trn_terminal_shift_cash_BD_inserts.source_hash)

--calculate hash and lookup to current s_ig_it_trn_terminal_shift_cash_BD_1
if object_id('tempdb..#s_ig_it_trn_terminal_shift_cash_BD_1_inserts') is not null drop table #s_ig_it_trn_terminal_shift_cash_BD_1_inserts
create table #s_ig_it_trn_terminal_shift_cash_BD_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bk_hash,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bus_day_id bus_day_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.cash_shift_id cash_shift_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_id tender_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.term_id term_id,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.BD_start_dttime bd_start_dt_time,
       stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.BD_end_dttime bd_end_dt_time,
       isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.bus_day_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.cash_shift_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.tender_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.term_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.BD_start_dttime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.BD_end_dttime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_it_trn_Terminal_Shift_Cash_BD
 where stage_hash_ig_it_trn_Terminal_Shift_Cash_BD.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_it_trn_terminal_shift_cash_BD_1 records
set @insert_date_time = getdate()
insert into s_ig_it_trn_terminal_shift_cash_BD_1 (
       bk_hash,
       bus_day_id,
       cash_shift_id,
       tender_id,
       term_id,
       bd_start_dt_time,
       bd_end_dt_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.bk_hash,
       #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.bus_day_id,
       #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.cash_shift_id,
       #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.tender_id,
       #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.term_id,
       #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.bd_start_dt_time,
       #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.bd_end_dt_time,
       case when s_ig_it_trn_terminal_shift_cash_BD_1.s_ig_it_trn_terminal_shift_cash_BD_1_id is null then isnull(#s_ig_it_trn_terminal_shift_cash_BD_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       15,
       #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_it_trn_terminal_shift_cash_BD_1_inserts
  left join p_ig_it_trn_terminal_shift_cash_BD
    on #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.bk_hash = p_ig_it_trn_terminal_shift_cash_BD.bk_hash
   and p_ig_it_trn_terminal_shift_cash_BD.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_it_trn_terminal_shift_cash_BD_1
    on p_ig_it_trn_terminal_shift_cash_BD.bk_hash = s_ig_it_trn_terminal_shift_cash_BD_1.bk_hash
   and p_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_1_id = s_ig_it_trn_terminal_shift_cash_BD_1.s_ig_it_trn_terminal_shift_cash_BD_1_id
 where s_ig_it_trn_terminal_shift_cash_BD_1.s_ig_it_trn_terminal_shift_cash_BD_1_id is null
    or (s_ig_it_trn_terminal_shift_cash_BD_1.s_ig_it_trn_terminal_shift_cash_BD_1_id is not null
        and s_ig_it_trn_terminal_shift_cash_BD_1.dv_hash <> #s_ig_it_trn_terminal_shift_cash_BD_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_it_trn_terminal_shift_cash_BD @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_it_trn_terminal_shift_cash_BD @current_dv_batch_id

end
