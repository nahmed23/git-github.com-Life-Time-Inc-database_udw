CREATE PROC [dbo].[proc_fact_cafe_tips_summary] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_cafe_tips_summary)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_cafe_tips_summary_key), location=user_db) as  
select
      d_ig_ig_business_sum_tips_bp_pc_mp_se.dim_ig_ig_business_sum_tips_bp_pc_mp_se_key as fact_cafe_tips_summary_key,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.tendered_business_period_dim_id,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.posted_business_period_dim_id,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.event_dim_id,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.profit_center_dim_id,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.meal_period_dim_id,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.server_emp_dim_id as server_employee_dim_id,
      ig_ig_dimension_profit_center_dimension.store_id,
      ig_ig_dimension_profit_center_dimension.dim_cafe_profit_center_key,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.charged_tip_amount,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.charged_gratuity_amount,
      tendered_ig_ig_dimension_business_period_dimension.business_period_start_dim_date_key tendered_business_period_start_dim_date_key,
      tendered_ig_ig_dimension_business_period_dimension.business_period_start_dim_date_key tendered_business_period_end_dim_date_key,
       tendered_ig_ig_dimension_business_period_dimension.month_ending_dim_date_key,
      posted_ig_ig_dimension_business_period_dimension.business_period_start_dim_date_key posted_business_period_start_dim_date_key,
      posted_ig_ig_dimension_business_period_dimension.business_period_start_dim_date_key posted_business_period_end_dim_date_key,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.dv_batch_id,
      d_ig_ig_business_sum_tips_bp_pc_mp_se.dv_load_date_time 
  from d_ig_ig_business_sum_tips_bp_pc_mp_se
    join d_ig_ig_dimension_profit_center_dimension ig_ig_dimension_profit_center_dimension
      on d_ig_ig_business_sum_tips_bp_pc_mp_se.profit_center_dim_id = 
         ig_ig_dimension_profit_center_dimension.profit_center_dim_id
    join d_ig_ig_dimension_business_period_dimension   tendered_ig_ig_dimension_business_period_dimension
      on d_ig_ig_business_sum_tips_bp_pc_mp_se.tendered_business_period_dim_id = 
         tendered_ig_ig_dimension_business_period_dimension.business_period_dim_id
    join d_ig_ig_dimension_business_period_dimension   posted_ig_ig_dimension_business_period_dimension
      on d_ig_ig_business_sum_tips_bp_pc_mp_se.posted_business_period_dim_id = 
         posted_ig_ig_dimension_business_period_dimension.business_period_dim_id
    where ig_ig_dimension_profit_center_dimension.store_id not in (2,45)
      and d_ig_ig_business_sum_tips_bp_pc_mp_se.dv_batch_id >= @load_dv_batch_id


 if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_cafe_tips_summary_key), location=user_db) as  
select  
      #etl_step_1.fact_cafe_tips_summary_key,
      #etl_step_1.tendered_business_period_dim_id,
      #etl_step_1.posted_business_period_dim_id,
      #etl_step_1.event_dim_id,
      #etl_step_1.profit_center_dim_id,
      #etl_step_1.meal_period_dim_id,
      #etl_step_1.server_employee_dim_id,
       club.dim_club_key,
      #etl_step_1.dim_cafe_profit_center_key,
      #etl_step_1.charged_tip_amount,
      #etl_step_1.charged_gratuity_amount,
       #etl_step_1.tendered_business_period_start_dim_date_key,
      #etl_step_1.tendered_business_period_end_dim_date_key,
      #etl_step_1.posted_business_period_start_dim_date_key,
      #etl_step_1.posted_business_period_end_dim_date_key,
       case when #etl_step_1.fact_cafe_tips_summary_key in ('-997','-998','-999') then #etl_step_1.fact_cafe_tips_summary_key
       when club_merchant_number.club_merchant_number_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(club_merchant_number.club_merchant_number_id as bigint) as varchar(500)),'z#@$k%&P'))),2) end as dim_merchant_number_key,
       isnull(club.local_currency_code,'USD') as original_currency_code,
       case when #etl_step_1.fact_cafe_tips_summary_key in ('-997', '-998', '-999') then #etl_step_1.fact_cafe_tips_summary_key
       when #etl_step_1.month_ending_dim_date_key is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_1.month_ending_dim_date_key),'z#@$k%&P')+
                                        'P%#&z$@k'+isnull(isnull(club.local_currency_code,'USD'),'z#@$k%&P')+
                                        'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                        'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
       end as usd_monthly_average_dim_exchange_rate_key,
       case when #etl_step_1.fact_cafe_tips_summary_key in ('-997', '-998', '-999') then #etl_step_1.fact_cafe_tips_summary_key
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(isnull(club.local_currency_code,'USD'),'z#@$k%&P')+
                                               'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
       end as usd_dim_plan_exchange_rate_key,
      #etl_step_1.dv_batch_id,
      #etl_step_1.dv_load_date_time 
  from #etl_step_1 
  left join dim_club club on 
  #etl_step_1.store_id = club.info_genesis_store_id
  left join d_mms_club_merchant_number club_merchant_number on 
  club.dim_club_key = club_merchant_number.dim_club_key
  
     /*   Delete records from the table that exist*/
     /*   Insert records from temp table for current and missing batches*/
     
begin tran
     
delete dbo.fact_cafe_tips_summary
    where fact_cafe_tips_summary_key in (select fact_cafe_tips_summary_key from dbo.#etl_step_2) 


        insert into fact_cafe_tips_summary
          (      fact_cafe_tips_summary_key,
                 tendered_business_period_dim_id,
                 posted_business_period_dim_id,
                 event_dim_id,
                 profit_center_dim_id,
                 meal_period_dim_id,
                 server_employee_dim_id,
                 dim_club_key,
                 dim_cafe_profit_center_key,
                 charged_tip_amount,
                 charged_gratuity_amount,
                 tendered_business_period_start_dim_date_key,
                 tendered_business_period_end_dim_date_key,
                 posted_business_period_start_dim_date_key,
                 posted_business_period_end_dim_date_key,
                 dim_merchant_number_key,
                 original_currency_code,
                 usd_monthly_average_dim_exchange_rate_key,
                 usd_dim_plan_exchange_rate_key,
                 dv_batch_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_inserted_date_time,
                 dv_insert_user )
                     select 
                 fact_cafe_tips_summary_key,
                 tendered_business_period_dim_id,
                 posted_business_period_dim_id,
                 event_dim_id,
                 profit_center_dim_id,
                 meal_period_dim_id,
                 server_employee_dim_id,
                 dim_club_key,
                 dim_cafe_profit_center_key,
                 charged_tip_amount,
                 charged_gratuity_amount,
                 tendered_business_period_start_dim_date_key,
                 tendered_business_period_end_dim_date_key,
                 posted_business_period_start_dim_date_key,
                 posted_business_period_end_dim_date_key,
                 dim_merchant_number_key,
                 original_currency_code,
                 usd_monthly_average_dim_exchange_rate_key,
                 usd_dim_plan_exchange_rate_key,
                 dv_batch_id,
                 dv_load_date_time,
                 'dec 31, 9999',
                 getdate(),
                 suser_sname()
        from #etl_step_2

    commit tran

   end
