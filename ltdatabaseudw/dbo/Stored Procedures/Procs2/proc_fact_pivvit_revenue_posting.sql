CREATE PROC [dbo].[proc_fact_pivvit_revenue_posting] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_pivvit_revenue_posting)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_pivvit_revenue_posting_key), location=user_db) as 
select 
	MAX(pivvit_revenue_posting.fact_pivvit_revenue_posting_key) fact_pivvit_revenue_posting_key,
	pivvit_revenue_posting.company_id,
	pivvit_revenue_posting.cost_center_id,
	pivvit_revenue_posting.currency_id,
	pivvit_revenue_posting.club_id,
	CAST(pivvit_revenue_posting.transaction_date AS DATE) posted_date,
	pivvit_revenue_posting.club_id as region_id,
	pivvit_revenue_posting.tender_type_id,	
	pivvit_revenue_posting.sign_flag,
	SUM(pivvit_revenue_posting.discount_amount) discount_amount,
	SUM(pivvit_revenue_posting.transaction_amount) transaction_amount, 
	SUM(pivvit_revenue_posting.transaction_lineamount) transaction_lineamount,
	SUM(pivvit_revenue_posting.tax_amount) tax_amount,
	pivvit_revenue_posting.transaction_date,
	'Pivvit ' + cast(pivvit_revenue_posting.dv_batch_id as varchar)+' '+pivvit_revenue_posting.tender_type_id transaction_id,
	pivvit_revenue_posting.transaction_memo,
	'Pivvit '+pivvit_revenue_posting.tender_type_id transaction_line_memo,
	pivvit_revenue_posting.mms_product_code,	
	max(pivvit_revenue_posting.dv_batch_id) dv_batch_id,
	max(pivvit_revenue_posting.dv_load_date_time) dv_load_date_time,	
	'dec 31, 9999'  dv_load_end_date_time ,
	getdate()  dv_inserted_date_time,
	suser_sname()  dv_insert_user
	from dbo.d_pivvit_revenue_posting pivvit_revenue_posting	
	GROUP BY 
	pivvit_revenue_posting.company_id,
	pivvit_revenue_posting.cost_center_id,
	pivvit_revenue_posting.currency_id,
	pivvit_revenue_posting.tender_type_id,
	pivvit_revenue_posting.mms_product_code,	
	pivvit_revenue_posting.dv_batch_id,
	pivvit_revenue_posting.club_id,
	pivvit_revenue_posting.transaction_date,	
	pivvit_revenue_posting.transaction_memo,
	pivvit_revenue_posting.sign_flag

truncate table dbo.fact_pivvit_revenue_posting

begin tran
	
	insert into fact_pivvit_revenue_posting
   (fact_pivvit_revenue_posting_key,
   company_id,
   cost_center_id,
   currency_id,
   club_id,
   posted_date,
   region_id,
   tender_type_id,
   discount_amount,
   transaction_amount,
   transaction_lineamount,
   tax_amount,
   transaction_date,
   transaction_id,
   transaction_memo,
   transaction_line_memo,
   mms_product_code,   
   dv_batch_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_inserted_date_time,
   dv_insert_user
   )
   
   select fact_pivvit_revenue_posting_key,
   company_id,
   cost_center_id,
   currency_id,
   club_id,
   posted_date,
   region_id,
   tender_type_id,
   discount_amount,
   transaction_amount,
   transaction_lineamount,
   tax_amount,
   transaction_date,
   transaction_id,
   transaction_memo,
   transaction_line_memo,
   mms_product_code,   
   dv_batch_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_inserted_date_time,
   dv_insert_user
   from #etl_step_1
   
  commit tran
    
 end
---go	
  
--  -- Populate the table
---truncate table dbo.fact_pivvit_revenue_posting
----exec dbo.proc_fact_pivvit_revenue_posting -1 