CREATE PROC [dbo].[proc_fact_olo_order_detail] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_olo_order_detail)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_olo_order_detail_key), location=user_db) as 
  select 
     olo_order_detail.bk_hash fact_olo_order_detail_key,
	 olo_order_detail.transaction_date transaction_date,
	 'Olo-Online POS' as transaction_memo,
	 'LTFOPCO' as company_id,
	 'USD' currency_id,
	 store_number,
	 store_number + ' Olo-Online POS' transaction_id,
	 olo_order_detail.transaction_amount transaction_amount,
	 olo_order_detail.transaction_date posted_date,
	 'Olo-Online-POS-'+payment_description as transaction_line_memo,
	'Restaurant' as cost_center_id,
	 payment_description as tender_type_id,
	 'dec 31, 9999'  dv_load_date_time,
	 'dec 31, 9999'  dv_load_end_date_time ,
	 -1 dv_batch_id	
	 from d_olo_order_detail olo_order_detail

	if object_id('tempdb..#etl_step_sum') is not null drop table #etl_step_sum
create table dbo.#etl_step_sum with(distribution=hash(store_number), location=user_db) as 
  select  store_number,
          transaction_date,
		  transaction_memo,
		  company_id,
		  currency_id,
		  transaction_id,
		  sum(#etl_step_1.transaction_amount) transaction_amount,
		  posted_date,
		  transaction_line_memo,
		  cost_center_id,
		  tender_type_id,
		  dv_load_date_time,
		  dv_load_end_date_time,
		  dv_batch_id
  from #etl_step_1
  group by store_number,
          transaction_date,
		  transaction_memo,
		  company_id,
		  currency_id,
		  transaction_id,
		  posted_date,
		  transaction_line_memo,
		  cost_center_id,
		  tender_type_id,
		  dv_load_date_time,
		  dv_load_end_date_time,
		  dv_batch_id
 

truncate table fact_olo_order_detail
begin tran


   insert into fact_olo_order_detail
   (
   transaction_date,
   transaction_memo,
   deposit,
   withdrawal,
   company_id,
   currency_id,
   batch_id,
   transaction_id,
   drawer_id,
   transaction_amount,
   shipping_amount,
   discount_amount,
   drawer_close_comments,
   posted_date,
   transaction_line_amount,
   transaction_line_tax_amount,
   transaction_line_memo,
   region_id,
   cost_center_id,
   tender_type_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user)
   
   select
   transaction_date,
   transaction_memo,
   case when transaction_amount>= 0.00 then 'true'
   else 'false'
   end deposit,
      case when transaction_amount>= 0.00 then 'false'
   else 'true'
   end withdrawal,
   company_id,
   currency_id,
   null as batch_id,
   transaction_id,
   null as drawer_id,
   transaction_amount,
   null as shipping_amount,
   null as discount_amount,
   null as drawer_close_comments,
   posted_date,
   transaction_amount,
   null as transaction_line_tax_amount,
   transaction_line_memo,
   store_number as region_id,
   cost_center_id,
   tender_type_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
    getdate()  ,
   suser_sname()
   from #etl_step_sum
   
   
  commit tran
  
 end

