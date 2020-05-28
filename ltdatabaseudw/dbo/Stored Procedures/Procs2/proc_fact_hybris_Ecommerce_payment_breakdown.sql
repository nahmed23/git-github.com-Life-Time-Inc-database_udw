CREATE PROC [dbo].[proc_fact_hybris_Ecommerce_payment_breakdown] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_hybris_Ecommerce_payment_breakdown)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_hybris_Ecommerce_payment_breakdown_key), location=user_db) as 
select 
	MAX(Ecommerce_payment_breakdown.fact_hybris_payment_breakdown_key) fact_hybris_Ecommerce_payment_breakdown_key,
	CAST(Ecommerce_payment_breakdown.transaction_date AS DATE) transaction_date,
	CAST(Ecommerce_payment_breakdown.transaction_date AS DATE) posted_date,
	Ecommerce_payment_breakdown.tender_type_id,
	max(Ecommerce_payment_breakdown.dv_batch_id) dv_batch_id,
	max(Ecommerce_payment_breakdown.dv_load_date_time) dv_load_date_time,
	'Hybris' transaction_memo,
	'LTFOPCO' company_id,
	'USD' currency_id,
	'Ecommerce Online' transaction_id,
	'1900' as region_id,
	'Ecommerce Online' as cost_center_id, 
	'Ecommerce Online' transaction_line_category_id,
	'dec 31, 9999'  dv_load_end_date_time ,
	getdate()  dv_inserted_date_time,
	suser_sname()  dv_insert_user,
	SUM(Ecommerce_payment_breakdown.transaction_amount) transaction_amount, 
	SUM(Ecommerce_payment_breakdown.transaction_line_amount) transaction_line_amount
	from dbo.d_hybris_Ecommerce_payment_breakdown Ecommerce_payment_breakdown
	where Ecommerce_payment_breakdown.tender_type_id <> 'LTBUCKS'
	GROUP BY 
	Ecommerce_payment_breakdown.tender_type_id,
	Ecommerce_payment_breakdown.dv_batch_id,
	CAST(Ecommerce_payment_breakdown.transaction_date AS DATE)  

truncate table dbo.fact_hybris_Ecommerce_payment_breakdown

begin tran
	
	insert into fact_hybris_Ecommerce_payment_breakdown
   (fact_hybris_Ecommerce_payment_breakdown_key,
   transaction_date,
   posted_date,
   tender_type_id,
   dv_batch_id,
   dv_load_date_time,
   transaction_memo,
   company_id,
   currency_id,
   transaction_id,
   region_id,
   cost_center_id,
   transaction_line_category_id,
   dv_load_end_date_time,
   dv_inserted_date_time,
   dv_insert_user,
   transaction_amount,
   transaction_line_amount
   )
   
   select fact_hybris_Ecommerce_payment_breakdown_key,
   transaction_date,
   posted_date,
   tender_type_id,
   dv_batch_id,
   dv_load_date_time,
   transaction_memo,
   company_id,
   currency_id,
   transaction_id,
   region_id,
   cost_center_id,
   transaction_line_category_id,
   dv_load_end_date_time,
   dv_inserted_date_time,
   dv_insert_user,
   transaction_amount,
   transaction_line_amount
   from #etl_step_1
   
  commit tran
    
 end
---go	
  
--  -- Populate the table
---truncate table dbo.fact_hybris_Ecommerce_payment_breakdown
----exec dbo.proc_fact_hybris_Ecommerce_payment_breakdown -1 