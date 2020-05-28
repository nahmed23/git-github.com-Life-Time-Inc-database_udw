CREATE PROC [dbo].[proc_etl_ig_ig_business_sum_tips_BP_PC_MP_SE] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE (
       bk_hash,
       tendered_business_period_dim_id,
       posted_business_period_dim_id,
       event_dim_id,
       profit_center_dim_id,
       meal_period_dim_id,
       server_emp_dim_id,
       gross_sales_amount,
       discount_amount,
       irs_allocable_sales_amount,
       charged_tip_amount,
       declared_cash_tip_amount,
       charged_gratuity_amount,
       tip_transfer_in_amount,
       tip_transfer_out_amount,
       charged_tip_grat_sales_amount,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(tendered_business_period_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(posted_business_period_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(event_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(profit_center_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(meal_period_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(server_emp_dim_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       tendered_business_period_dim_id,
       posted_business_period_dim_id,
       event_dim_id,
       profit_center_dim_id,
       meal_period_dim_id,
       server_emp_dim_id,
       gross_sales_amount,
       discount_amount,
       irs_allocable_sales_amount,
       charged_tip_amount,
       declared_cash_tip_amount,
       charged_gratuity_amount,
       tip_transfer_in_amount,
       tip_transfer_out_amount,
       charged_tip_grat_sales_amount,
       jan_one,
       isnull(cast(stage_ig_ig_business_Sum_Tips_BP_PC_MP_SE.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_ig_business_Sum_Tips_BP_PC_MP_SE
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_ig_business_sum_tips_BP_PC_MP_SE @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_ig_business_sum_tips_BP_PC_MP_SE (
       bk_hash,
       tendered_business_period_dim_id,
       posted_business_period_dim_id,
       event_dim_id,
       profit_center_dim_id,
       meal_period_dim_id,
       server_emp_dim_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.bk_hash,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.tendered_business_period_dim_id tendered_business_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.posted_business_period_dim_id posted_business_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.event_dim_id event_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.profit_center_dim_id profit_center_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.meal_period_dim_id meal_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.server_emp_dim_id server_emp_dim_id,
       isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       14,
       @insert_date_time,
       @user
  from stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE
  left join h_ig_ig_business_sum_tips_BP_PC_MP_SE
    on stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.bk_hash = h_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash
 where h_ig_ig_business_sum_tips_BP_PC_MP_SE_id is null
   and stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ig_ig_business_sum_tips_BP_PC_MP_SE
if object_id('tempdb..#s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts') is not null drop table #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts
create table #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.bk_hash,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.tendered_business_period_dim_id tendered_business_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.posted_business_period_dim_id posted_business_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.event_dim_id event_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.profit_center_dim_id profit_center_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.meal_period_dim_id meal_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.server_emp_dim_id server_emp_dim_id,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.gross_sales_amount gross_sales_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.discount_amount discount_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.irs_allocable_sales_amount irs_allocable_sales_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.charged_tip_amount charged_tip_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.declared_cash_tip_amount declared_cash_tip_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.charged_gratuity_amount charged_gratuity_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.tip_transfer_in_amount tip_transfer_in_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.tip_transfer_out_amount tip_transfer_out_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.charged_tip_grat_sales_amount charged_tip_grat_sales_amount,
       stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.jan_one jan_one,
       isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.tendered_business_period_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.posted_business_period_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.event_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.profit_center_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.meal_period_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.server_emp_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.gross_sales_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.discount_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.irs_allocable_sales_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.charged_tip_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.declared_cash_tip_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.charged_gratuity_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.tip_transfer_in_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.tip_transfer_out_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.charged_tip_grat_sales_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE
 where stage_hash_ig_ig_business_Sum_Tips_BP_PC_MP_SE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_ig_business_sum_tips_BP_PC_MP_SE records
set @insert_date_time = getdate()
insert into s_ig_ig_business_sum_tips_BP_PC_MP_SE (
       bk_hash,
       tendered_business_period_dim_id,
       posted_business_period_dim_id,
       event_dim_id,
       profit_center_dim_id,
       meal_period_dim_id,
       server_emp_dim_id,
       gross_sales_amount,
       discount_amount,
       irs_allocable_sales_amount,
       charged_tip_amount,
       declared_cash_tip_amount,
       charged_gratuity_amount,
       tip_transfer_in_amount,
       tip_transfer_out_amount,
       charged_tip_grat_sales_amount,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.bk_hash,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.tendered_business_period_dim_id,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.posted_business_period_dim_id,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.event_dim_id,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.profit_center_dim_id,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.meal_period_dim_id,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.server_emp_dim_id,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.gross_sales_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.discount_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.irs_allocable_sales_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.charged_tip_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.declared_cash_tip_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.charged_gratuity_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.tip_transfer_in_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.tip_transfer_out_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.charged_tip_grat_sales_amount,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.jan_one,
       case when s_ig_ig_business_sum_tips_BP_PC_MP_SE.s_ig_ig_business_sum_tips_BP_PC_MP_SE_id is null then isnull(#s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       14,
       #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts
  left join p_ig_ig_business_sum_tips_BP_PC_MP_SE
    on #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.bk_hash = p_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash
   and p_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_ig_business_sum_tips_BP_PC_MP_SE
    on p_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash = s_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash
   and p_ig_ig_business_sum_tips_BP_PC_MP_SE.s_ig_ig_business_sum_tips_BP_PC_MP_SE_id = s_ig_ig_business_sum_tips_BP_PC_MP_SE.s_ig_ig_business_sum_tips_BP_PC_MP_SE_id
 where s_ig_ig_business_sum_tips_BP_PC_MP_SE.s_ig_ig_business_sum_tips_BP_PC_MP_SE_id is null
    or (s_ig_ig_business_sum_tips_BP_PC_MP_SE.s_ig_ig_business_sum_tips_BP_PC_MP_SE_id is not null
        and s_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_hash <> #s_ig_ig_business_sum_tips_BP_PC_MP_SE_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_ig_business_sum_tips_BP_PC_MP_SE @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_ig_business_sum_tips_BP_PC_MP_SE @current_dv_batch_id

end
