CREATE PROC [dbo].[proc_etl_ig_ig_business_sum_tender_BP_PC_MP_CT] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT (
       bk_hash,
       tendered_business_period_dim_id,
       posted_business_period_dim_id,
       event_dim_id,
       profit_center_dim_id,
       meal_period_dim_id,
       check_type_dim_id,
       tender_dim_id,
       credit_type_id,
       tender_amount,
       change_amount,
       received_amount,
       breakage_amount,
       tip_amount,
       tender_count,
       tender_quantity,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(tendered_business_period_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(posted_business_period_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(event_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(profit_center_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(meal_period_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(check_type_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(tender_dim_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(credit_type_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       tendered_business_period_dim_id,
       posted_business_period_dim_id,
       event_dim_id,
       profit_center_dim_id,
       meal_period_dim_id,
       check_type_dim_id,
       tender_dim_id,
       credit_type_id,
       tender_amount,
       change_amount,
       received_amount,
       breakage_amount,
       tip_amount,
       tender_count,
       tender_quantity,
       jan_one,
       isnull(cast(stage_ig_ig_business_Sum_Tender_BP_PC_MP_CT.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_ig_business_Sum_Tender_BP_PC_MP_CT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_ig_business_sum_tender_BP_PC_MP_CT @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_ig_business_sum_tender_BP_PC_MP_CT (
       bk_hash,
       tendered_business_period_dim_id,
       posted_business_period_dim_id,
       event_dim_id,
       profit_center_dim_id,
       meal_period_dim_id,
       check_type_dim_id,
       tender_dim_id,
       credit_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.bk_hash,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tendered_business_period_dim_id tendered_business_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.posted_business_period_dim_id posted_business_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.event_dim_id event_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.profit_center_dim_id profit_center_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.meal_period_dim_id meal_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.check_type_dim_id check_type_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_dim_id tender_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.credit_type_id credit_type_id,
       isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       14,
       @insert_date_time,
       @user
  from stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT
  left join h_ig_ig_business_sum_tender_BP_PC_MP_CT
    on stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.bk_hash = h_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
 where h_ig_ig_business_sum_tender_BP_PC_MP_CT_id is null
   and stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ig_ig_business_sum_tender_BP_PC_MP_CT
if object_id('tempdb..#s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts') is not null drop table #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts
create table #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.bk_hash,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tendered_business_period_dim_id tendered_business_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.posted_business_period_dim_id posted_business_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.event_dim_id event_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.profit_center_dim_id profit_center_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.meal_period_dim_id meal_period_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.check_type_dim_id check_type_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_dim_id tender_dim_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.credit_type_id credit_type_id,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_amount tender_amount,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.change_amount change_amount,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.received_amount received_amount,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.breakage_amount breakage_amount,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tip_amount tip_amount,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_count tender_count,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_quantity tender_quantity,
       stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.jan_one jan_one,
       isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tendered_business_period_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.posted_business_period_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.event_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.profit_center_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.meal_period_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.check_type_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_dim_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.credit_type_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.change_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.received_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.breakage_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tip_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_count as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.tender_quantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT
 where stage_hash_ig_ig_business_Sum_Tender_BP_PC_MP_CT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_ig_business_sum_tender_BP_PC_MP_CT records
set @insert_date_time = getdate()
insert into s_ig_ig_business_sum_tender_BP_PC_MP_CT (
       bk_hash,
       tendered_business_period_dim_id,
       posted_business_period_dim_id,
       event_dim_id,
       profit_center_dim_id,
       meal_period_dim_id,
       check_type_dim_id,
       tender_dim_id,
       credit_type_id,
       tender_amount,
       change_amount,
       received_amount,
       breakage_amount,
       tip_amount,
       tender_count,
       tender_quantity,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.bk_hash,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.tendered_business_period_dim_id,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.posted_business_period_dim_id,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.event_dim_id,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.profit_center_dim_id,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.meal_period_dim_id,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.check_type_dim_id,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.tender_dim_id,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.credit_type_id,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.tender_amount,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.change_amount,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.received_amount,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.breakage_amount,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.tip_amount,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.tender_count,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.tender_quantity,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.jan_one,
       case when s_ig_ig_business_sum_tender_BP_PC_MP_CT.s_ig_ig_business_sum_tender_BP_PC_MP_CT_id is null then isnull(#s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       14,
       #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts
  left join p_ig_ig_business_sum_tender_BP_PC_MP_CT
    on #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.bk_hash = p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
   and p_ig_ig_business_sum_tender_BP_PC_MP_CT.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_ig_business_sum_tender_BP_PC_MP_CT
    on p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash = s_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
   and p_ig_ig_business_sum_tender_BP_PC_MP_CT.s_ig_ig_business_sum_tender_BP_PC_MP_CT_id = s_ig_ig_business_sum_tender_BP_PC_MP_CT.s_ig_ig_business_sum_tender_BP_PC_MP_CT_id
 where s_ig_ig_business_sum_tender_BP_PC_MP_CT.s_ig_ig_business_sum_tender_BP_PC_MP_CT_id is null
    or (s_ig_ig_business_sum_tender_BP_PC_MP_CT.s_ig_ig_business_sum_tender_BP_PC_MP_CT_id is not null
        and s_ig_ig_business_sum_tender_BP_PC_MP_CT.dv_hash <> #s_ig_ig_business_sum_tender_BP_PC_MP_CT_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_ig_business_sum_tender_BP_PC_MP_CT @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_ig_business_sum_tender_BP_PC_MP_CT @current_dv_batch_id

end
