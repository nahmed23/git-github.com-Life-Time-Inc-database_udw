CREATE PROC [dbo].[proc_fact_orbital_transaction_data_report] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_orbital_transaction_data_report)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_orbital_transaction_data_report_key), location=user_db) as 
select 
		MAX(Orbital_transaction_data_report.fact_orbital_transaction_data_report_key) fact_orbital_transaction_data_report_key,
		'LTFCORP' company_id,
		'Orbital Refunds' cost_center_id,
		'Orbital Refunds' category_id,
		'USD' currency_id,	
		Orbital_transaction_data_report.transaction_date posted_date,
		'9001' as region_id,	
		Orbital_transaction_data_report.tender_type_id,
		case when Orbital_transaction_data_report.deposit_flag='Y' then 'true' else 'false' end deposit,
		case when Orbital_transaction_data_report.deposit_flag='Y' then 'false' else 'true' end withdrawl,	
		SUM(Orbital_transaction_data_report.amount) transaction_amount, 	
		convert(date,'20'+substring(Orbital_transaction_data_report.transaction_date,5,2)+'-'+
		substring(Orbital_transaction_data_report.transaction_date,1,2)+'-'+substring(Orbital_transaction_data_report.transaction_date,3,2)) transaction_date,
		--convert(date,Orbital_transaction_data_report.transaction_date) transaction_date,
		'Orbital Refunds'+' - '+convert(varchar,Orbital_transaction_data_report.dv_batch_id)+' - '+trim(convert(varchar,Orbital_transaction_data_report.Batch_Number)) transaction_id,	
		max(Orbital_transaction_data_report.dv_batch_id) dv_batch_id,
		max(Orbital_transaction_data_report.dv_load_date_time) dv_load_date_time,	
		'dec 31, 9999'  dv_load_end_date_time ,
		getdate()  dv_inserted_date_time,
		suser_sname()  dv_insert_user
		from dbo.d_orbital_transaction_data_report Orbital_transaction_data_report	
		GROUP BY 
		Orbital_transaction_data_report.tender_type_id,
		Orbital_transaction_data_report.deposit_flag,	
		Orbital_transaction_data_report.dv_batch_id,	
        Orbital_transaction_data_report.Batch_Number,
		Orbital_transaction_data_report.transaction_date
		--ORDER BY Orbital_transaction_data_report.transaction_date
	
truncate table dbo.fact_orbital_transaction_data_report

begin tran
	
	insert into fact_orbital_transaction_data_report
   (fact_orbital_transaction_data_report_key,
   company_id,
   cost_center_id,
   category_id,
   currency_id,   
   posted_date,
   region_id,   
   tender_type_id,   
   deposit,
   withdrawl,
   amount,   
   transaction_date,
   transaction_id,   
   dv_batch_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_inserted_date_time,
   dv_insert_user
   )
   
   select fact_orbital_transaction_data_report_key,
   company_id,
   cost_center_id,
   category_id,
   currency_id,   
   posted_date,
   region_id,   
   tender_type_id, 
   deposit,
   withdrawl,   
   transaction_amount,   
   transaction_date,
   transaction_id,   
   dv_batch_id,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_inserted_date_time,
   dv_insert_user
   from #etl_step_1
   
  commit tran
    
 end
