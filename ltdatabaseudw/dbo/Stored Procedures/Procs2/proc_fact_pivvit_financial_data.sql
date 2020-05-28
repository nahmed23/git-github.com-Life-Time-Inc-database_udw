CREATE PROC [dbo].[proc_fact_pivvit_financial_data] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_pivvit_financial_data)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_0') is not null drop table #etl_step_0
create table dbo.#etl_step_0 with(distribution=hash(fact_pivvit_financial_data_key), location=user_db) as 
select 
	MAX(Pivvit_financial_data.fact_pivvit_financial_data_key) fact_pivvit_financial_data_key,	
	CAST(Pivvit_financial_data.transaction_date AS DATE) posted_date,	
	Pivvit_financial_data.offering_id,
	Pivvit_financial_data.tender_type_id,
    --Pivvit_financial_data.sign_flag,
	SUM(Pivvit_financial_data.discount_amount) discount_amount,
	SUM(Pivvit_financial_data.transaction_amount) transaction_amount, 
	/*case when Pivvit_financial_data.sign_flag='POS' then SUM(Pivvit_financial_data.transaction_amount) else 0 end  transaction_amount, 
    case when Pivvit_financial_data.sign_flag='NEG' then SUM(Pivvit_financial_data.transaction_amount) else 0 end refund_amount,*/
	SUM(Pivvit_financial_data.transaction_lineamount) transaction_lineamount,
	SUM(Pivvit_financial_data.tax_amount) tax_amount,
	Pivvit_financial_data.transaction_date,
	'Pivvit ' + cast(Pivvit_financial_data.batch_id as varchar)+' '+Pivvit_financial_data.tender_type_id transaction_id,
	Pivvit_financial_data.transaction_memo,
	'Pivvit '+Pivvit_financial_data.tender_type_id transaction_line_memo,
	max(Pivvit_financial_data.dv_batch_id) dv_batch_id,
	max(Pivvit_financial_data.dv_load_date_time) dv_load_date_time	
	from dbo.d_pivvit_financial_data Pivvit_financial_data	
	GROUP BY 
	Pivvit_financial_data.tender_type_id,
	Pivvit_financial_data.batch_id,
	Pivvit_financial_data.dv_batch_id,	
	Pivvit_financial_data.offering_id,
	Pivvit_financial_data.transaction_date,	
	Pivvit_financial_data.transaction_memo
	--,Pivvit_financial_data.sign_flag

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_pivvit_financial_data_key), location=user_db) as 
select 
	MAX(Pivvit_financial_data_temp.fact_pivvit_financial_data_key) fact_pivvit_financial_data_key,
	'LTFOPCO' company_id,
	'Pivvit' cost_center_id,
	'USD' currency_id,
	'1900' club_id,
	CAST(Pivvit_financial_data_temp.transaction_date AS DATE) posted_date,
	'1900' as region_id,
	Pivvit_financial_data_temp.offering_id,
	Pivvit_financial_data_temp.tender_type_id,    
	MAX(Pivvit_financial_data_temp.discount_amount) discount_amount,
	SUM(Pivvit_financial_data_temp.transaction_amount) transaction_amount, 
    --SUM(Pivvit_financial_data_temp.refund_amount) refund_amount,
	MAX(Pivvit_financial_data_temp.transaction_lineamount) transaction_lineamount,
	MAX(Pivvit_financial_data_temp.tax_amount) tax_amount,
	Pivvit_financial_data_temp.transaction_date,
	Pivvit_financial_data_temp.transaction_id,
	Pivvit_financial_data_temp.transaction_memo,
	Pivvit_financial_data_temp.transaction_line_memo,
	max(Pivvit_financial_data_temp.dv_batch_id) dv_batch_id,
	max(Pivvit_financial_data_temp.dv_load_date_time) dv_load_date_time,	
	'dec 31, 9999'  dv_load_end_date_time ,
	getdate()  dv_inserted_date_time,
	suser_sname()  dv_insert_user
	from #etl_step_0 Pivvit_financial_data_temp	
	GROUP BY 
	Pivvit_financial_data_temp.tender_type_id,
	Pivvit_financial_data_temp.dv_batch_id,	
	Pivvit_financial_data_temp.offering_id,
	Pivvit_financial_data_temp.transaction_date,	
	Pivvit_financial_data_temp.transaction_memo,
	Pivvit_financial_data_temp.transaction_id,
	Pivvit_financial_data_temp.transaction_line_memo

truncate table dbo.fact_pivvit_financial_data

begin tran
	
	insert into fact_pivvit_financial_data
   (fact_pivvit_financial_data_key,
   company_id,
   cost_center_id,
   currency_id,
   club_id,
   posted_date,
   region_id,
   offering_id,
   tender_type_id,
   discount_amount,
   transaction_amount,
   --refund_amount,
   transaction_lineamount,
   tax_amount,
   transaction_date,
   transaction_id,
   transaction_memo,
   transaction_line_memo,
   dv_batch_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_inserted_date_time,
   dv_insert_user
   )
   
   select fact_pivvit_financial_data_key,
   company_id,
   cost_center_id,
   currency_id,
   club_id,
   posted_date,
   region_id,
   offering_id,
   tender_type_id,
   discount_amount,
   transaction_amount,
   --refund_amount,
   transaction_lineamount,
   tax_amount,
   transaction_date,
   transaction_id,
   transaction_memo,
   transaction_line_memo,
   dv_batch_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_inserted_date_time,
   dv_insert_user
   from #etl_step_1

   
  commit tran
    
 end
-- go	
  
--  -- Populate the table
---truncate table dbo.fact_pivvit_financial_data
----exec dbo.proc_fact_pivvit_financial_data -1 

