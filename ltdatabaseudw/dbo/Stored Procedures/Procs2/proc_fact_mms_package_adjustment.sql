CREATE PROC [dbo].[proc_fact_mms_package_adjustment] @dv_batch_id [varchar](500),@begin_extract_date_time [datetime] AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_package_adjustment)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

/*---Get MMSPackageAdjustment data in the current batch---------------------------------------------------*/
if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_package_adjustment_key), location=user_db) as
select 	d_mms_package_adjustment.fact_mms_package_adjustment_key,
		d_mms_package_adjustment.package_adjustment_id,
		d_mms_package_adjustment.adjusted_date_time,
		d_mms_package_adjustment.adjusted_dim_date_key,
		d_mms_package_adjustment.adjusted_dim_time_key,
		d_mms_package_adjustment.adjustment_comment,
		d_mms_package_adjustment.adjustment_dim_employee_key,
		d_mms_package_adjustment.adjustment_mms_tran_id,
		d_mms_package_adjustment.adjustment_type_dim_description_key,
		d_mms_package_adjustment.fact_mms_package_key,
		d_mms_package_adjustment.number_of_sessions_adjusted,
		d_mms_package_adjustment.package_adjustment_amount,
		d_mms_package_adjustment.dv_load_date_time,
		d_mms_package_adjustment.dv_batch_id
  from d_mms_package_adjustment
  where d_mms_package_adjustment.dv_batch_id >=@load_dv_batch_id
 
/*---Join  #etl_step_1 with fact_mms_package table on fact_mms_package_key column----------------------*/
if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_mms_package_adjustment_key), location=user_db) as  
select 	#etl_step_1.fact_mms_package_adjustment_key fact_mms_package_adjustment_key,
		#etl_step_1.package_adjustment_id package_adjustment_id,
		#etl_step_1.adjusted_date_time adjusted_date_time,
		#etl_step_1.adjusted_dim_date_key adjusted_dim_date_key,
		#etl_step_1.adjusted_dim_time_key adjusted_dim_time_key,
		#etl_step_1.adjustment_dim_employee_key adjustment_dim_employee_key,
		#etl_step_1.adjustment_mms_tran_id adjustment_mms_tran_id,
		#etl_step_1.adjustment_type_dim_description_key adjustment_type_dim_description_key,
		isnull(fact_mms_package.fact_mms_package_key, '-998') fact_mms_package_key,
		isnull(fact_mms_package.fact_mms_sales_transaction_key, '-998') fact_mms_sales_transaction_key,
		isnull(fact_mms_package.created_dim_date_key, '-998') package_entered_dim_date_key,
		isnull(fact_mms_package.created_dim_time_key, '-998') package_entered_dim_time_key,
		isnull(fact_mms_package.dim_club_key, '-998') package_entered_dim_club_key,
		isnull(fact_mms_package.dim_mms_member_key, '-998') dim_mms_member_key,
		isnull(fact_mms_package.dim_mms_product_key, '-998') dim_mms_product_key,
		#etl_step_1.number_of_sessions_adjusted number_of_sessions_adjusted,
		#etl_step_1.package_adjustment_amount package_adjustment_amount,
		#etl_step_1.adjustment_comment adjustment_comment,
		#etl_step_1.dv_load_date_time dv_load_date_time,
		'dec 31, 9999' dv_load_end_date_time,
		#etl_step_1.dv_batch_id dv_batch_id,
		getdate() dv_inserted_date_time,
		suser_sname() dv_insert_user
  from #etl_step_1
  join dbo.fact_mms_package 
  on #etl_step_1.fact_mms_package_key = fact_mms_package.fact_mms_package_key
 
/*----Final data set to insert into fact_mms_package_adjustment table------------------*/
if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(fact_mms_package_adjustment_key), location=user_db) as  
select 	#etl_step_2.fact_mms_package_adjustment_key,
		#etl_step_2.package_adjustment_id,
		#etl_step_2.adjusted_date_time,
		#etl_step_2.adjusted_dim_date_key,
		#etl_step_2.adjusted_dim_time_key,
		#etl_step_2.adjustment_dim_employee_key,
		#etl_step_2.adjustment_mms_tran_id,
		#etl_step_2.adjustment_type_dim_description_key,
		#etl_step_2.fact_mms_package_key,
		#etl_step_2.fact_mms_sales_transaction_key,
		#etl_step_2.package_entered_dim_date_key,
		#etl_step_2.package_entered_dim_time_key,
		#etl_step_2.package_entered_dim_club_key,
		#etl_step_2.dim_mms_member_key,
		#etl_step_2.dim_mms_product_key,
		#etl_step_2.number_of_sessions_adjusted,
		#etl_step_2.package_adjustment_amount,
		#etl_step_2.adjustment_comment,
		#etl_step_2.dv_load_date_time,
		#etl_step_2.dv_load_end_date_time,
		#etl_step_2.dv_batch_id,
		#etl_step_2.dv_inserted_date_time,
		#etl_step_2.dv_insert_user
  from #etl_step_2
  
  
/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/

begin tran

  delete dbo.fact_mms_package_adjustment
   where fact_mms_package_adjustment_key in (select fact_mms_package_adjustment_key from dbo.#etl_step_2) 
   
    insert into fact_mms_package_adjustment
	(   
		fact_mms_package_adjustment_key,
		package_adjustment_id,
		adjusted_date_time,
		adjusted_dim_date_key,
		adjusted_dim_time_key,
		adjustment_dim_employee_key,
		adjustment_mms_tran_id,
		adjustment_type_dim_description_key,
		fact_mms_package_key,
		fact_mms_sales_transaction_key,
		package_entered_dim_date_key,
		package_entered_dim_time_key,
		package_entered_dim_club_key,
		dim_mms_member_key,
		dim_mms_product_key,
		number_of_sessions_adjusted,
		package_adjustment_amount,
		adjustment_comment,
		dv_load_date_time,
		dv_load_end_date_time,
		dv_batch_id,
		dv_inserted_date_time,
		dv_insert_user
		)
	select fact_mms_package_adjustment_key,
		package_adjustment_id,
		adjusted_date_time,
		adjusted_dim_date_key,
		adjusted_dim_time_key,
		adjustment_dim_employee_key,
		adjustment_mms_tran_id,
		adjustment_type_dim_description_key,
		fact_mms_package_key,
		fact_mms_sales_transaction_key,
		package_entered_dim_date_key,
		package_entered_dim_time_key,
		package_entered_dim_club_key,
		dim_mms_member_key,
		dim_mms_product_key,
		number_of_sessions_adjusted,
		package_adjustment_amount,
		adjustment_comment,
		dv_load_date_time,
		dv_load_end_date_time,
		dv_batch_id,
		dv_inserted_date_time,
		dv_insert_user
		from #etl_step_3
commit tran

/*---------recalculate package_adjustment_amount and update in fact_mms_package_adjustment table-------------*/
if object_id('tempdb..#LTFDW_FactECommerceSalesTransaction_HybrisPackages') is not null drop table #LTFDW_FactECommerceSalesTransaction_HybrisPackages
 create table dbo.#LTFDW_FactECommerceSalesTransaction_HybrisPackages with(distribution=hash(fact_mms_package_key), location=user_db) as  
 SELECT fact_mms_package_adjustment.fact_mms_package_adjustment_key  fact_mms_package_adjustment_key,
        fact_mms_package_adjustment.fact_mms_package_key,
        case when fact_mms_package.number_of_sessions = 0 or fact_mms_package_adjustment.number_of_sessions_adjusted = 0 then 0
             else ((fact_hybris_transaction_item.transaction_amount_gross / fact_mms_package.number_of_sessions) * fact_mms_package_adjustment.number_of_sessions_adjusted) end package_adjustment_amount
  from  fact_mms_package_adjustment
  join fact_mms_package 
	on fact_mms_package_adjustment.fact_mms_package_key=fact_mms_package.fact_mms_package_key
  join fact_hybris_transaction_item
    on fact_mms_package_adjustment.fact_mms_sales_transaction_key=fact_hybris_transaction_item.fact_mms_sales_transaction_key
   and fact_hybris_transaction_item.fact_mms_sales_transaction_key <> '-998'
 Where (fact_mms_package_adjustment.dv_batch_id >= @load_dv_batch_id
         or fact_hybris_transaction_item.dv_batch_id >= @load_dv_batch_id 
         or fact_mms_package.dv_batch_id >= @load_dv_batch_id)
   and fact_mms_package_adjustment.package_adjustment_amount <> case when fact_mms_package.number_of_sessions = 0 or fact_mms_package_adjustment.number_of_sessions_adjusted = 0 then 0
                                                                     else ((fact_hybris_transaction_item.transaction_amount_gross / fact_mms_package.number_of_sessions) * fact_mms_package_adjustment.number_of_sessions_adjusted) end 

 update fact_mms_package_adjustment
 set package_adjustment_amount =  #LTFDW_FactECommerceSalesTransaction_HybrisPackages.package_adjustment_amount
 from  #LTFDW_FactECommerceSalesTransaction_HybrisPackages
 where fact_mms_package_adjustment.fact_mms_package_adjustment_key=#LTFDW_FactECommerceSalesTransaction_HybrisPackages.fact_mms_package_adjustment_key
 
 end
