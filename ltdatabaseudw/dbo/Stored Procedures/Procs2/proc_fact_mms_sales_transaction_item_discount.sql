CREATE PROC [dbo].[proc_fact_mms_sales_transaction_item_discount] @dv_batch_id [varchar](500),@begin_extract_date_time [datetime] AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_sales_transaction_item_discount)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_sales_transaction_item_discount_key), location=user_db) as
 select tran_item_discount.bk_hash fact_mms_sales_transaction_item_discount_key,
        tran_item_discount.tran_item_discount_id tran_item_discount_id,
        tran_item_discount.tran_item_id tran_item_id ,
		tran_item_discount.pricing_discount_id pricing_discount_id,
		tran_item_discount.applied_discount_amount applied_discount_amount,
        tran_item_discount.dv_batch_id,
        tran_item_discount.dv_load_date_time
 from d_mms_tran_item_discount tran_item_discount 
 where tran_item_discount.dv_batch_id >= @load_dv_batch_id
 
 -- Find the month_starting_dim_date_key for the begin_extract_date_time
 declare @factSalesTransaction_3_month char(8)
 select @factSalesTransaction_3_month = month_starting_dim_date_key
  from dim_date
 where  dim_date.calendar_date =dateadd(mm,-3,convert(varchar,case when  @begin_extract_date_time = '1753-01-01 00:00:00' then '1753-04-01 00:00:00'
                                                              else @begin_extract_date_time end, 112)) 

 


 
 if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_mms_sales_transaction_item_key), location=user_db) as
 select fact_mms_sales_transaction_item.fact_mms_sales_transaction_item_key,
        fact_mms_sales_transaction_item.tran_item_id tran_item_id,
        fact_mms_sales_transaction_item.udw_inserted_dim_date_key udw_inserted_dim_date_key,
		fact_mms_sales_transaction_item.original_currency_code original_currency_code,
		fact_mms_sales_transaction_item.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
		fact_mms_sales_transaction_item.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key
 from fact_mms_sales_transaction_item fact_mms_sales_transaction_item
 join dim_date
   on fact_mms_sales_transaction_item.post_dim_date_key> dim_date.dim_date_key
   and dim_date.calendar_date = @factSalesTransaction_3_month
	
	
if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(fact_mms_sales_transaction_item_discount_key), location=user_db) as
 select #etl_step_1.fact_mms_sales_transaction_item_discount_key fact_mms_sales_transaction_item_discount_key,
        #etl_step_2.tran_item_id tran_item_id,
        #etl_step_1.tran_item_discount_id tran_item_discount_id,
        #etl_step_2.udw_inserted_dim_date_key udw_inserted_dim_date_key,
		#etl_step_2.original_currency_code original_currency_code,
		#etl_step_2.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
		#etl_step_2.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
		#etl_step_1.applied_discount_amount applied_discount_amount,
		dim_mms_pricing_discount.dim_mms_pricing_discount_key dim_mms_pricing_discount_key,
		#etl_step_1.dv_batch_id dv_batch_id,
		#etl_step_1.dv_load_date_time dv_load_date_time
 from #etl_step_1
 join #etl_step_2
   on #etl_step_1.tran_item_id = #etl_step_2.tran_item_id
 left join dim_mms_pricing_discount dim_mms_pricing_discount
 on #etl_step_1.pricing_discount_id = dim_mms_pricing_discount.pricing_discount_id
   
	
if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(fact_mms_sales_transaction_item_discount_key), location=user_db) as
 select #etl_step_1.fact_mms_sales_transaction_item_discount_key fact_mms_sales_transaction_item_discount_key,
        fact_mms_sales_transaction_item.tran_item_id tran_item_id,
        #etl_step_1.tran_item_discount_id tran_item_discount_id,
        fact_mms_sales_transaction_item.udw_inserted_dim_date_key udw_inserted_dim_date_key,
		fact_mms_sales_transaction_item.original_currency_code original_currency_code,
		fact_mms_sales_transaction_item.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
		fact_mms_sales_transaction_item.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
		#etl_step_1.applied_discount_amount applied_discount_amount,
		dim_mms_pricing_discount.dim_mms_pricing_discount_key dim_mms_pricing_discount_key,
		#etl_step_1.dv_batch_id dv_batch_id,
		#etl_step_1.dv_load_date_time dv_load_date_time
 from #etl_step_1
 join fact_mms_sales_transaction_item fact_mms_sales_transaction_item 
 on #etl_step_1.tran_item_id = fact_mms_sales_transaction_item.tran_item_id
 left join dim_mms_pricing_discount dim_mms_pricing_discount
 on (#etl_step_1.pricing_discount_id = dim_mms_pricing_discount.pricing_discount_id)

if object_id('tempdb..#etl_step_5') is not null drop table #etl_step_5
create table dbo.#etl_step_5 with(distribution=hash(fact_mms_sales_transaction_item_discount_key), location=user_db) as

select #etl_step_3.fact_mms_sales_transaction_item_discount_key,
       #etl_step_3.tran_item_id tran_item_id,
       #etl_step_3.tran_item_discount_id tran_item_discount_id,
       #etl_step_3.udw_inserted_dim_date_key fact_mms_sales_transaction_edw_inserted_dim_date_key,
	   #etl_step_3.original_currency_code original_currency_code,
	   #etl_step_3.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
	   #etl_step_3.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
	   #etl_step_3.applied_discount_amount discount_amount,
	   #etl_step_3.dim_mms_pricing_discount_key dim_mms_pricing_discount_key,
	   #etl_step_3.dv_batch_id dv_batch_id,
	   #etl_step_3.dv_load_date_time dv_load_date_time
 from #etl_step_3
union
 
select #etl_step_4.fact_mms_sales_transaction_item_discount_key,
       #etl_step_4.tran_item_id tran_item_id,
       #etl_step_4.tran_item_discount_id tran_item_discount_id,
       #etl_step_4.udw_inserted_dim_date_key fact_mms_sales_transaction_edw_inserted_dim_date_key,
	   #etl_step_4.original_currency_code original_currency_code,
	   #etl_step_4.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
	   #etl_step_4.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
	   #etl_step_4.applied_discount_amount discount_amount,
	   #etl_step_4.dim_mms_pricing_discount_key dim_mms_pricing_discount_key,
	   #etl_step_4.dv_batch_id dv_batch_id,
	   #etl_step_4.dv_load_date_time dv_load_date_time
  from #etl_step_4

 -- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

  begin tran

  delete dbo.fact_mms_sales_transaction_item_discount
   where fact_mms_sales_transaction_item_discount_key in (select fact_mms_sales_transaction_item_discount_key from dbo.#etl_step_5) 

   insert into  fact_mms_sales_transaction_item_discount
          (fact_mms_sales_transaction_item_discount_key 
          ,tran_item_id 
          ,tran_item_discount_id 
          ,fact_mms_sales_transaction_edw_inserted_dim_date_key 
          ,original_currency_code 
          ,usd_monthly_average_dim_exchange_rate_key 
          ,usd_dim_plan_exchange_rate_key
          ,discount_amount
		  ,dim_mms_pricing_discount_key
    	  ,dv_batch_id
		  ,dv_load_date_time
          , dv_load_end_date_time	 
		  ,dv_inserted_date_time
          ,dv_insert_user)
		  
  select fact_mms_sales_transaction_item_discount_key 
          ,tran_item_id 
          ,tran_item_discount_id 
          ,fact_mms_sales_transaction_edw_inserted_dim_date_key 
          ,original_currency_code 
          ,usd_monthly_average_dim_exchange_rate_key 
          ,usd_dim_plan_exchange_rate_key
          ,discount_amount
		  ,dim_mms_pricing_discount_key
    	  ,dv_batch_id
		  ,dv_load_date_time
          ,'dec 31, 9999'
		  ,getdate() 
          ,suser_sname()
   from #etl_step_5

  
    commit tran
	end

