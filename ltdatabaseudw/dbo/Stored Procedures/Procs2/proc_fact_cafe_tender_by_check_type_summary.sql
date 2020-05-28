CREATE PROC [dbo].[proc_fact_cafe_tender_by_check_type_summary] @dv_batch_id [varchar](500),@InfoGenesisExcludedStoreIDList [nvarchar](500),@dropTables [nvarchar](2000) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_cafe_tender_by_check_type_summary)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

--Create #temp tablesbases for in-paramters 
execute  sp_executesql @InfoGenesisExcludedStoreIDList


if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(fact_cafe_tender_by_check_type_summary_key)) as
select      d_ig_ig_business_sum_tender_bp_pc_mp_ct.fact_cafe_tender_by_check_type_summary_key,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.tendered_business_period_dim_id,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.posted_business_period_dim_id,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.event_dim_id,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.profit_center_dim_id,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.meal_period_dim_id,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.check_type_dim_id,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.tender_dim_id,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.credit_type_id,
			igigdimensiontenderdimension.dim_cafe_payment_type_key,
			igigdimensionprofitcenterdimension.store_id,
			'r_mms_val_business_area_' + convert(char(32),hashbytes('md5',('P%#&z$@k'+'3')),2) club_business_area_dim_description_key,
			'r_mms_val_business_area_' + convert(char(32),hashbytes('md5',('P%#&z$@k'+'4')),2) corporate_business_area_dim_description_key,
			dclub.club_id,
			dclub.club_type as club_type_description,
            case when d_ig_ig_business_sum_tender_bp_pc_mp_ct.fact_cafe_tender_by_check_type_summary_key in ('-997','-998','-999') then d_ig_ig_business_sum_tender_bp_pc_mp_ct.fact_cafe_tender_by_check_type_summary_key       
                 when  dclub.dim_club_key is null then '-998'   
				      else dclub.dim_club_key
            end dim_club_key,
            case when d_ig_ig_business_sum_tender_bp_pc_mp_ct.fact_cafe_tender_by_check_type_summary_key in ('-997','-998','-999') then d_ig_ig_business_sum_tender_bp_pc_mp_ct.fact_cafe_tender_by_check_type_summary_key       
                 when igigdimensionprofitcenterdimension.store_id is null then '-998'       
				      else igigdimensionprofitcenterdimension.dim_cafe_profit_center_key
            end dim_cafe_profit_center_key,
            case when igigdimensiontenderdimension.tender_id = 1 then d_ig_ig_business_sum_tender_bp_pc_mp_ct.tender_net_amount
				      else d_ig_ig_business_sum_tender_bp_pc_mp_ct.new_tender_amount
            end tender_net_amount,
            tenderedigigdimensionbusinessperioddimension.business_period_start_dim_date_key tendered_business_period_start_dim_date_key,
            tenderedigigdimensionbusinessperioddimension.business_period_end_dim_date_key tendered_business_period_end_dim_date_key,
            postedigigdimensionbusinessperioddimension.business_period_start_dim_date_key posted_business_period_start_dim_date_key,
            postedigigdimensionbusinessperioddimension.business_period_end_dim_date_key posted_business_period_end_dim_date_key,
            isnull(dclub.local_currency_code,'USD') as original_currency_code,
            postedigigdimensionbusinessperioddimension.month_ending_dim_date_key,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.dv_load_date_time d_ig_ig_business_sum_tender_bp_pc_mp_ct_dv_load_date_time,
            d_ig_ig_business_sum_tender_bp_pc_mp_ct.dv_batch_id d_ig_ig_business_sum_tender_bp_pc_mp_ct_dv_batch_id,
			'dec 31, 9999' dv_load_end_date_time
            from d_ig_ig_business_sum_tender_bp_pc_mp_ct
    join d_ig_ig_dimension_profit_center_dimension igigdimensionprofitcenterdimension
          on d_ig_ig_business_sum_tender_bp_pc_mp_ct.profit_center_dim_id = 
                  igigdimensionprofitcenterdimension.profit_center_dim_id
    join d_ig_ig_dimension_business_period_dimension          
                  tenderedigigdimensionbusinessperioddimension
           on d_ig_ig_business_sum_tender_bp_pc_mp_ct.tendered_business_period_dim_id = 
         tenderedigigdimensionbusinessperioddimension.business_period_dim_id
    join d_ig_ig_dimension_business_period_dimension   postedigigdimensionbusinessperioddimension
           on d_ig_ig_business_sum_tender_bp_pc_mp_ct.posted_business_period_dim_id = 
         postedigigdimensionbusinessperioddimension.business_period_dim_id
    join d_ig_ig_dimension_tender_dimension igigdimensiontenderdimension
           on d_ig_ig_business_sum_tender_bp_pc_mp_ct.tender_dim_id = 
         igigdimensiontenderdimension.tender_dim_id
    left join dim_club dclub
           on dclub.info_genesis_store_id = igigdimensionprofitcenterdimension.store_id
    where igigdimensionprofitcenterdimension.store_id not in (select store_id_list from #InfoGenesisExcludedStoreIDList)
	and   d_ig_ig_business_sum_tender_bp_pc_mp_ct.dv_batch_id >= @load_dv_batch_id

----------------------------------------------------------------------------------------------------

--Get the latest record from dim_club_key, business_area_dim_description_key, currency_code combination
if object_id('tempdb..#etl_step1_2') is not null drop table #etl_step1_2
create table dbo.#etl_step1_2 with(distribution=hash(dim_club_key)) as
select 
    dim_mms_merchant_number_key,
    dim_club_key,
    business_area_dim_description_key,
    currency_code,
    auto_reconcile_flag,
    row_number() over(partition by dim_club_key, business_area_dim_description_key, currency_code order by club_merchant_number_id desc) r
  from marketing.v_dim_mms_merchant_number		   

----------------------------------------------------------------------------------------------------

if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution=hash(fact_cafe_tender_by_check_type_summary_key)) as
select      #etl_step1.fact_cafe_tender_by_check_type_summary_key,
            #etl_step1.tendered_business_period_dim_id,
            #etl_step1.posted_business_period_dim_id,
            #etl_step1.event_dim_id,
            #etl_step1.profit_center_dim_id,
            #etl_step1.meal_period_dim_id,
            #etl_step1.check_type_dim_id,
            #etl_step1.tender_dim_id,
            #etl_step1.credit_type_id,
			#etl_step1.dim_cafe_payment_type_key,
			#etl_step1.dim_club_key,
			#etl_step1.dim_cafe_profit_center_key,
			#etl_step1.tender_net_amount,
			#etl_step1.tendered_business_period_start_dim_date_key,
			#etl_step1.tendered_business_period_end_dim_date_key,
			#etl_step1.posted_business_period_start_dim_date_key,
			#etl_step1.posted_business_period_end_dim_date_key,
			#etl_step1.original_currency_code,
			case when #etl_step1.fact_cafe_tender_by_check_type_summary_key in ('-997', '-998', '-999') then #etl_step1.fact_cafe_tender_by_check_type_summary_key
                 when #etl_step1.club_type_description = 'MMS Non-Club Location' then '-997'
				 when #etl_step1.store_id is null then '-998'
                 when club_d_mms_club_merchant_number.dim_mms_merchant_number_key is null then '-998' -- This should be changed to dim_mms_club_merchant_number_key
                      else club_d_mms_club_merchant_number.dim_mms_merchant_number_key
                 end dim_merchant_number_key,
			case when #etl_step1.fact_cafe_tender_by_check_type_summary_key in ('-997', '-998', '-999') then #etl_step1.fact_cafe_tender_by_check_type_summary_key
                      else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step1.month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(#etl_step1.original_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
                 end usd_monthly_average_dim_exchange_rate_key,
			case when #etl_step1.fact_cafe_tender_by_check_type_summary_key in ('-997', '-998', '-999') then #etl_step1.fact_cafe_tender_by_check_type_summary_key
                      else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step1.original_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
                 end usd_dim_plan_exchange_rate_key,  
			case when #etl_step1.fact_cafe_tender_by_check_type_summary_key in ('-997', '-998', '-999') then #etl_step1.fact_cafe_tender_by_check_type_summary_key
                 when dclubcur.dim_club_currency_code_key is null then '-998'
                      else dclubcur.dim_club_currency_code_key
                 end dim_club_currency_code_key,
		--	case when #etl_step1.fact_cafe_tender_by_check_type_summary_key in ('-997', '-998', '-999') then #etl_step1.fact_cafe_tender_by_check_type_summary_key
		--	     when #etl_step1.dim_club_key = '-998' then  '-998' 
         --             else  convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step1.dim_club_key,'z#@$k%&P')+
        --                                        'P%#&z$@k'+isnull(#etl_step1.original_currency_code,'z#@$k%&P'))),2)
       --          end dim_club_currency_code_key,
			#etl_step1.d_ig_ig_business_sum_tender_bp_pc_mp_ct_dv_load_date_time,
			#etl_step1.d_ig_ig_business_sum_tender_bp_pc_mp_ct_dv_batch_id,
			#etl_step1.dv_load_end_date_time,
			getdate() dv_inserted_date_time,
			suser_sname() dv_insert_user
			from #etl_step1 
	     left join dim_club_currency_code dclubcur 
		 on #etl_step1.club_id = dclubcur.club_id
         and  #etl_step1.original_currency_code = dclubcur.currency_code
		 left join #etl_step1_2 club_d_mms_club_merchant_number 
		 on #etl_step1.club_business_area_dim_description_key = club_d_mms_club_merchant_number.business_area_dim_description_key
		 and #etl_step1.dim_club_key = club_d_mms_club_merchant_number.dim_club_key
		 and #etl_step1.original_currency_code = club_d_mms_club_merchant_number.currency_code
		 and club_d_mms_club_merchant_number.auto_reconcile_flag = 'Y'
		 and club_d_mms_club_merchant_number.r = 1
		 left join #etl_step1_2 corporate_d_mms_club_merchant_number -- This should be changed to d_mms_club_merchant_number
		 on #etl_step1.club_business_area_dim_description_key = corporate_d_mms_club_merchant_number.business_area_dim_description_key
		 and #etl_step1.dim_club_key = corporate_d_mms_club_merchant_number.dim_club_key
		 and #etl_step1.original_currency_code = corporate_d_mms_club_merchant_number.currency_code
		 and corporate_d_mms_club_merchant_number.auto_reconcile_flag = 'Y'
		 and corporate_d_mms_club_merchant_number.r = 1


-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.fact_cafe_tender_by_check_type_summary
   where fact_cafe_tender_by_check_type_summary_key in (select fact_cafe_tender_by_check_type_summary_key from dbo.#etl_step2) 
   
   
     insert into dbo.fact_cafe_tender_by_check_type_summary
    (   
	    fact_cafe_tender_by_check_type_summary_key,
   		tendered_business_period_dim_id,
        posted_business_period_dim_id,
        event_dim_id,
        profit_center_dim_id,
        meal_period_dim_id,
        check_type_dim_id,
        tender_dim_id,
        credit_type_id,
		dim_cafe_payment_type_key,
        dim_club_key,
        dim_cafe_profit_center_key,
        tender_net_amount,
        tendered_business_period_start_dim_date_key,
        tendered_business_period_end_dim_date_key,
        posted_business_period_start_dim_date_key,
        posted_business_period_end_dim_date_key,
        original_currency_code,
		dim_merchant_number_key,
		usd_monthly_average_dim_exchange_rate_key,
		usd_dim_plan_exchange_rate_key,
		dim_club_currency_code_key,
		dv_load_date_time,
		dv_batch_id,
		dv_load_end_date_time,
		dv_inserted_date_time,
		dv_insert_user
		)
		select 
		#etl_step2.fact_cafe_tender_by_check_type_summary_key,
   		#etl_step2.tendered_business_period_dim_id,
        #etl_step2.posted_business_period_dim_id,
        #etl_step2.event_dim_id,
        #etl_step2.profit_center_dim_id,
        #etl_step2.meal_period_dim_id,
        #etl_step2.check_type_dim_id,
        #etl_step2.tender_dim_id,
        #etl_step2.credit_type_id,
		#etl_step2.dim_cafe_payment_type_key,
        #etl_step2.dim_club_key,
        #etl_step2.dim_cafe_profit_center_key,
        #etl_step2.tender_net_amount,
        #etl_step2.tendered_business_period_start_dim_date_key,
        #etl_step2.tendered_business_period_end_dim_date_key,
        #etl_step2.posted_business_period_start_dim_date_key,
        #etl_step2.posted_business_period_end_dim_date_key,
        #etl_step2.original_currency_code,
		#etl_step2.dim_merchant_number_key,
		#etl_step2.usd_monthly_average_dim_exchange_rate_key,
		#etl_step2.usd_dim_plan_exchange_rate_key,
		#etl_step2.dim_club_currency_code_key,
		#etl_step2.d_ig_ig_business_sum_tender_bp_pc_mp_ct_dv_load_date_time,
		#etl_step2.d_ig_ig_business_sum_tender_bp_pc_mp_ct_dv_batch_id,
		#etl_step2.dv_load_end_date_time,
		#etl_step2.dv_inserted_date_time,
		#etl_step2.dv_insert_user
		from #etl_step2
				
commit tran

execute  sp_executesql @dropTables

end
