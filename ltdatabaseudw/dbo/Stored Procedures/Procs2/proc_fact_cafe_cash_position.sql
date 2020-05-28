CREATE PROC [dbo].[proc_fact_cafe_cash_position] @begin_extract_date_time [datetime] AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

declare @begin_extract_dim_date_key char(8) = (convert(char(8), convert(datetime, @begin_extract_date_time, 120), 112));

IF object_id('tempdb..#etl_step1') IS NOT NULL
	DROP TABLE #etl_step1
if object_id('tempdb.dbo.#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution = hash(dim_cafe_business_day_dates_key), location=user_db) as
	
with summary_cafe_cumlative_tender_cash_only (dim_cafe_business_day_dates_key, cashier_dim_cafe_employee_key,dim_cafe_meal_period_key,dim_cafe_profit_center_key,net_tender_amount,
dv_load_date_time,dv_batch_id) as
  (select   d_ig_it_trn_tender_cum_bd.dim_cafe_business_day_dates_key,
            d_ig_it_trn_tender_cum_bd.cashier_dim_cafe_employee_key,
            d_ig_it_trn_tender_cum_bd.dim_cafe_meal_period_key,
            d_ig_it_trn_tender_cum_bd.dim_cafe_profit_center_key,
            sum(d_ig_it_trn_tender_cum_bd.net_tender_amount) net_cash_tender_amount,
			max(d_ig_it_trn_tender_cum_bd.dv_load_date_time) dv_load_date_time,
			max(d_ig_it_trn_tender_cum_bd.dv_batch_id) dv_batch_id
       from d_ig_it_trn_tender_cum_bd
       join d_ig_it_trn_business_day_dates
         on d_ig_it_trn_tender_cum_bd.dim_cafe_business_day_dates_key = d_ig_it_trn_business_day_dates.dim_cafe_business_day_dates_key
      where d_ig_it_trn_business_day_dates.business_day_start_dim_date_key >= @begin_extract_dim_date_key
        and d_ig_it_trn_tender_cum_bd.tender_id = 1
      group by d_ig_it_trn_tender_cum_bd.dim_cafe_business_day_dates_key,
            cashier_dim_cafe_employee_key,
            dim_cafe_meal_period_key,
            dim_cafe_profit_center_key
    )

select  isnull(d_ig_it_trn_emp_cash_bd.dim_cafe_business_day_dates_key,     
                summary_cafe_cumlative_tender_cash_only.dim_cafe_business_day_dates_key) as dim_cafe_business_day_dates_key,
        isnull(d_ig_it_trn_emp_cash_bd.cashier_dim_cafe_employee_key,
                summary_cafe_cumlative_tender_cash_only.cashier_dim_cafe_employee_key) as cashier_dim_cafe_employee_key,
        isnull(d_ig_it_trn_emp_cash_bd.dim_cafe_meal_period_key,
                summary_cafe_cumlative_tender_cash_only.dim_cafe_meal_period_key) as dim_cafe_meal_period_key,
        isnull(d_ig_it_trn_emp_cash_bd.dim_cafe_profit_center_key,
                summary_cafe_cumlative_tender_cash_only.dim_cafe_profit_center_key) as dim_cafe_profit_center_key,
        isnull(d_ig_it_trn_emp_cash_bd.loan_amount, 0) as loan_amount,
        isnull(d_ig_it_trn_emp_cash_bd.withdrawal_amount, 0) as withdrawal_amount,
        isnull(summary_cafe_cumlative_tender_cash_only.net_tender_amount, 0) as net_cash_tender_amount,
		isnull(d_ig_it_trn_emp_cash_BD.paid_out_amount, 0) as paid_tips,		
/*		isnull(summary_cafe_cumlative_tender_cash_only.tip_amount, 0) as paid_tips,*/
        isnull(summary_cafe_cumlative_tender_cash_only.net_tender_amount, 0)
               + isnull(d_ig_it_trn_emp_cash_bd.loan_amount, 0)
                 - isnull(d_ig_it_trn_emp_cash_bd.withdrawal_amount, 0)
				   - isnull(d_ig_it_trn_emp_cash_BD.paid_out_amount, 0) as accountable_cash,
 /*                  - isnull(summary_cafe_cumlative_tender_cash_only.tip_amount, 0) as accountable_cash,*/
        isnull(d_ig_it_trn_emp_cash_bd.cash_drop_amount, 0) as cash_drop_amount,
        isnull(d_ig_it_trn_emp_cash_bd.cash_drop_amount, 0)
                -  isnull(summary_cafe_cumlative_tender_cash_only.net_tender_amount, 0)
                  - isnull(d_ig_it_trn_emp_cash_bd.loan_amount, 0)
                    + isnull(d_ig_it_trn_emp_cash_bd.withdrawal_amount, 0)
                      + isnull(d_ig_it_trn_emp_cash_BD.paid_out_amount, 0) as over_short_amount,					
 /*                     + isnull(summary_cafe_cumlative_tender_cash_only.tip_amount, 0) as over_short_amount,*/
        case 
	    	when isnull(d_ig_it_trn_emp_cash_bd.dv_load_date_time,'Jan 1, 1753') >= 
			        isnull(summary_cafe_cumlative_tender_cash_only.dv_load_date_time,'Jan 1, 1753') 
				then d_ig_it_trn_emp_cash_bd.dv_load_date_time
			else isnull(summary_cafe_cumlative_tender_cash_only.dv_load_date_time,'Jan 1, 1753') 
	      end dv_load_date_time
	    , case 
	    	when isnull(d_ig_it_trn_emp_cash_bd.dv_batch_id,-1) >= isnull(summary_cafe_cumlative_tender_cash_only.dv_batch_id,-1) 
					then d_ig_it_trn_emp_cash_bd.dv_batch_id
			else isnull(summary_cafe_cumlative_tender_cash_only.dv_batch_id,-1) 
	      end dv_batch_id
	    , convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	from d_ig_it_trn_emp_cash_bd
    full join summary_cafe_cumlative_tender_cash_only
         on d_ig_it_trn_emp_cash_bd.dim_cafe_business_day_dates_key = summary_cafe_cumlative_tender_cash_only.dim_cafe_business_day_dates_key
         and d_ig_it_trn_emp_cash_bd.cashier_dim_cafe_employee_key = summary_cafe_cumlative_tender_cash_only.cashier_dim_cafe_employee_key
         and d_ig_it_trn_emp_cash_bd.dim_cafe_meal_period_key = summary_cafe_cumlative_tender_cash_only.dim_cafe_meal_period_key
         and d_ig_it_trn_emp_cash_bd.dim_cafe_profit_center_key = summary_cafe_cumlative_tender_cash_only.dim_cafe_profit_center_key
    join d_ig_it_trn_business_day_dates
         on isnull(d_ig_it_trn_emp_cash_bd.dim_cafe_business_day_dates_key, summary_cafe_cumlative_tender_cash_only.dim_cafe_business_day_dates_key) = d_ig_it_trn_business_day_dates.dim_cafe_business_day_dates_key
   where d_ig_it_trn_business_day_dates.business_day_start_dim_date_key >= @begin_extract_dim_date_key


if object_id('tempdb..#etl_step2') IS NOT NULL
DROP TABLE #etl_step2

if object_id('tempdb.dbo.#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution = hash(dim_cafe_business_day_dates_key), location=user_db) as
select  convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#etl_step1.dim_cafe_business_day_dates_key as varchar(500)),'z#@$k%&P')+
        'P%#&z$@k'+isnull(cast(#etl_step1.cashier_dim_cafe_employee_key as varchar(500)),'z#@$k%&P') +
        'P%#&z$@k'+isnull(cast(#etl_step1.dim_cafe_meal_period_key as varchar(500)),'z#@$k%&P')+
        'P%#&z$@k'+isnull(cast(#etl_step1.dim_cafe_profit_center_key as varchar(500)),'z#@$k%&P'))),2) fact_cafe_cash_position_key
        ,#etl_step1.dim_cafe_business_day_dates_key
        ,#etl_step1.cashier_dim_cafe_employee_key
        ,#etl_step1.dim_cafe_meal_period_key
        ,#etl_step1.dim_cafe_profit_center_key
        ,#etl_step1.loan_amount
        ,#etl_step1.withdrawal_amount
        ,#etl_step1.net_cash_tender_amount
		,#etl_step1.paid_tips
        ,#etl_step1.accountable_cash
        ,#etl_step1.cash_drop_amount
        ,#etl_step1.over_short_amount
        ,#etl_step1.dv_load_date_time
        ,#etl_step1.dv_batch_id
		,#etl_step1.dv_load_end_date_time
 from #etl_step1
	
	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.fact_cafe_cash_position
	WHERE fact_cafe_cash_position_key IN (
			SELECT fact_cafe_cash_position_key
			FROM dbo.#etl_step2
			)

	INSERT INTO fact_cafe_cash_position(
         fact_cafe_cash_position_key
        ,dim_cafe_business_day_dates_key
        ,cashier_dim_cafe_employee_key
        ,dim_cafe_meal_period_key
        ,dim_cafe_profit_center_key
        ,loan_amount
        ,withdrawal_amount
        ,net_cash_tender_amount
		,paid_tips
        ,accountable_cash
        ,cash_drop_amount
        ,over_short_amount
        ,dv_load_date_time
        ,dv_batch_id
        ,dv_load_end_date_time
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         fact_cafe_cash_position_key
        ,dim_cafe_business_day_dates_key
        ,cashier_dim_cafe_employee_key
        ,dim_cafe_meal_period_key
        ,dim_cafe_profit_center_key
        ,loan_amount
        ,withdrawal_amount
        ,net_cash_tender_amount
		,paid_tips
        ,accountable_cash
        ,cash_drop_amount
        ,over_short_amount
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,getdate()
		,suser_sname()
	FROM #etl_step2

	COMMIT TRAN

			
END
