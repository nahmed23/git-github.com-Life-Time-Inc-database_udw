CREATE PROC [dbo].[proc_fact_mms_package_session] @dv_batch_id [varchar](500),@begin_extract_date_time [datetime] AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_package_session)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

 /* Get the MMSPackageSession data in the currentBatch*/
 if object_id('tempdb..#mms_package_session_current_batch') is not null drop table #mms_package_session_current_batch
create table dbo.#mms_package_session_current_batch with(distribution=hash(fact_mms_package_session_key), location=user_db) as
SELECT d_mms_package_session.fact_mms_package_session_key,
       d_mms_package_session.package_session_id,
       d_mms_package_session.created_dim_date_key,
	   d_mms_package_session.created_dim_time_key,
       d_mms_package_session.delivered_dim_date_key,
	   d_mms_package_session.delivered_dim_time_key,
       d_mms_package_session.package_id,
       d_mms_package_session.package_session_club_key,
       d_mms_package_session.delivered_dim_employee_key,
       d_mms_package_session.delivered_session_price,
       d_mms_package_session.comment,
	   d_mms_package_session.dv_load_date_time,
	   d_mms_package_session.dv_batch_id
  INTO #mms_package_session_current_batch
  FROM d_mms_package_session
 WHERE d_mms_package_session.dv_batch_id >= @load_dv_batch_id

 /* Get the MMSPackageSession data associated with MMSPackage records in the current batch*/
if object_id('tempdb..#mms_package_session_from_mms_package_current_batch') is not null drop table #mms_package_session_from_mms_package_current_batch
create table dbo.#mms_package_session_from_mms_package_current_batch with(distribution=hash(fact_mms_package_session_key), location=user_db) as
SELECT d_mms_package_session.fact_mms_package_session_key,
       d_mms_package_session.package_session_id,
       d_mms_package_session.created_dim_date_key,
	   d_mms_package_session.created_dim_time_key,
       d_mms_package_session.delivered_dim_date_key,
	   d_mms_package_session.delivered_dim_time_key,
       d_mms_package_session.package_id,
       d_mms_package_session.package_session_club_key,
       d_mms_package_session.delivered_dim_employee_key,
       d_mms_package_session.delivered_session_price,
       d_mms_package_session.comment,
	   d_mms_package_session.dv_load_date_time,
	   d_mms_package_session.dv_batch_id
  INTO #mms_package_session_from_mms_package_current_batch
  FROM d_mms_package
  JOIN d_mms_package_session
    ON d_mms_package.package_id = d_mms_package_session.package_id
 WHERE d_mms_package.dv_batch_id >= @load_dv_batch_id
 
 /* Combine the two sets of MMSPackageSession data*/
/* Use Union to remove duplicates*/
if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_package_session_key), location=user_db) as
SELECT fact_mms_package_session_key,
       package_session_id,
       package_id,
       created_dim_date_key,
	   created_dim_time_key,
       delivered_dim_date_key,
	   delivered_dim_time_key,
       delivered_dim_employee_key,
       package_session_club_key,
       delivered_session_price,
       Comment,
	   dv_load_date_time,
	   dv_batch_id
  FROM #mms_package_session_current_batch
  
UNION

SELECT fact_mms_package_session_key,
       package_session_id,
       package_id,
       created_dim_date_key,
	   created_dim_time_key,
       delivered_dim_date_key,
	   delivered_dim_time_key,
       delivered_dim_employee_key,
       package_session_club_key,
       delivered_session_price,
       Comment,
	   dv_load_date_time,
	   dv_batch_id
  FROM #mms_package_session_from_mms_package_current_batch


if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_mms_package_session_key), location=user_db) as  
select #etl_step_1.fact_mms_package_session_key fact_mms_package_session_key,
       #etl_step_1.created_dim_date_key created_dim_date_key,
	   #etl_step_1.delivered_dim_date_key delivered_dim_date_key,
	   #etl_step_1.delivered_dim_time_key delivered_dim_time_key,
	   fact_mms_package.dim_mms_product_key  fact_mms_package_dim_product_key, /*/*/*-------------This is again required in next hash table to populate BodyAgeAssesmentCount*/*/*/
       #etl_step_1.package_session_club_key delivered_dim_club_key,
       fact_mms_package.dim_mms_member_key dim_mms_member_key,
       #etl_step_1.package_session_id package_session_id,
	   fact_mms_package.mms_tran_id mms_tran_id,  
	   case when #etl_step_1.delivered_dim_date_key is null or #etl_step_1.delivered_dim_date_key = '-998'
	   then 0 else 1 end session_complete_count,
	   fact_mms_package.package_status_dim_description_key package_status_dim_description_key,    /*/*/*-------------This is again required in next hash table to populate VoidedFlag*/*/*/
	   fact_mms_package.dim_club_key package_entered_dim_club_key,
	   fact_mms_package.primary_sales_dim_employee_key primary_sales_dim_employee_key,
	   fact_mms_package.secondary_sales_dim_employee_key secondary_sales_dim_employee_key,
	   #etl_step_1.delivered_dim_employee_key delivered_dim_employee_key,
	   convert(char(8),getdate(),112) edw_inserted_dim_date_key,
	   #etl_step_1.delivered_session_price delivered_session_price,
	   fact_mms_package.tran_item_id tran_item_id,
	   fact_mms_package.package_entered_dim_employee_key package_entered_dim_employee_key,
	   case when fact_mms_package.sales_discount_amount is null then 0
	   when fact_mms_package.number_of_sessions = 0 then 0
	   else fact_mms_package.sales_discount_amount/fact_mms_package.number_of_sessions
	   end delivered_session_discount_value,
	   case when isnull(fact_mms_package.item_lt_bucks_amount,0) = 0 or isnull(fact_mms_package.number_of_sessions,0) = 0 then 0 
	   else fact_mms_package.item_lt_bucks_amount / fact_mms_package.number_of_sessions end delivered_session_lt_bucks_amount, /*--added UDW-9451-----*/
	   isnull(fact_mms_package.number_of_sessions,0) number_of_sessions,
	   isnull(fact_mms_package.original_currency_code,'USD')  original_currency_code,
	   isnull(fact_mms_package.usd_monthly_average_dim_exchange_rate_key,'-998') usd_monthly_average_dim_exchange_rate_key,
	   isnull(fact_mms_package.usd_dim_plan_exchange_rate_key,'-998') usd_dim_plan_exchange_rate_key,
	   #etl_step_1.created_dim_time_key created_dim_time_key,
	   fact_mms_package.fact_mms_package_key fact_mms_package_key,
	   fact_mms_package.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
	   isnull(fact_mms_package.created_dim_date_key,'-998') package_created_dim_date_key,
	   fact_mms_package.created_dim_time_key package_created_dim_time_key,
	   fact_mms_package.sales_channel_dim_description_key sales_channel_dim_description_key,
	   case when fact_mms_package.package_edited_flag is null then 'N' else 'Y' end package_edited_flag,
	   #etl_step_1.comment session_comment,
	   fact_mms_package.reporting_dim_club_key reporting_dim_club_key,
	   fact_mms_package.reporting_local_currency_monthly_average_dim_exchange_rate_key reporting_local_currency_monthly_average_dim_exchange_rate_key,
	   fact_mms_package.reporting_local_currency_dim_plan_exchange_rate_key reporting_local_currency_dim_plan_exchange_rate_key,
	   #etl_step_1.package_id package_id,
	   #etl_step_1.dv_load_date_time,
	   #etl_step_1.dv_batch_id
	   from #etl_step_1
left join fact_mms_package fact_mms_package
on #etl_step_1.package_id = fact_mms_package.package_id

 
if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(fact_mms_package_session_key), location=user_db) as  
select #etl_step_2.fact_mms_package_session_key fact_mms_package_session_key,
       #etl_step_2.created_dim_date_key created_dim_date_key,
	   #etl_step_2.created_dim_time_key created_dim_time_key,
	   #etl_step_2.delivered_dim_date_key delivered_dim_date_key,
       #etl_step_2.delivered_dim_time_key delivered_dim_time_key,	   
       #etl_step_2.fact_mms_package_dim_product_key fact_mms_package_dim_product_key  	,
	   dim_mms_product.dim_mms_product_id dim_mms_product_id,
       #etl_step_2.delivered_dim_club_key delivered_dim_club_key,
       #etl_step_2.dim_mms_member_key dim_mms_member_key,
	   #etl_step_2.package_session_id package_session_id,
	   #etl_step_2.mms_tran_id mms_tran_id,
	   #etl_step_2.session_complete_count session_complete_count,
	   #etl_step_2.package_status_dim_description_key package_status_dim_description_key,
	   dim_description.dim_description_id dim_description_id,
	   #etl_step_2.package_entered_dim_club_key package_entered_dim_club_key,
	   #etl_step_2.primary_sales_dim_employee_key primary_sales_dim_employee_key,
	   #etl_step_2.secondary_sales_dim_employee_key secondary_sales_dim_employee_key,
	   #etl_step_2.delivered_dim_employee_key delivered_dim_employee_key,
	   #etl_step_2.edw_inserted_dim_date_key edw_inserted_dim_date_key,
	   #etl_step_2.delivered_session_price delivered_session_price,
	   #etl_step_2.tran_item_id tran_item_id,
	   #etl_step_2.package_entered_dim_employee_key package_entered_dim_employee_key,
	   #etl_step_2.delivered_session_discount_value delivered_session_discount_value,
	   #etl_step_2.delivered_session_lt_bucks_amount delivered_session_lt_bucks_amount,
	   #etl_step_2.number_of_sessions number_of_sessions_in_package,
	   #etl_step_2.original_currency_code original_currency_code,
	   #etl_step_2.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
	   #etl_step_2.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
	   #etl_step_2.fact_mms_package_key fact_mms_package_key,
	   #etl_step_2.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
	   #etl_step_2.package_created_dim_date_key package_created_dim_date_key,
	   #etl_step_2.package_created_dim_time_key package_created_dim_time_key,
	   #etl_step_2.sales_channel_dim_description_key sales_channel_dim_description_key,
	   #etl_step_2.package_edited_flag package_edited_flag,
	   #etl_step_2.session_comment session_comment,
	   #etl_step_2.reporting_dim_club_key reporting_dim_club_key,
	   #etl_step_2.reporting_local_currency_monthly_average_dim_exchange_rate_key reporting_local_currency_monthly_average_dim_exchange_rate_key,
	   #etl_step_2.reporting_local_currency_dim_plan_exchange_rate_key reporting_local_currency_dim_plan_exchange_rate_key,
	   #etl_step_2.package_id package_id,
	   #etl_step_2.dv_load_date_time,
	   #etl_step_2.dv_batch_id
	from #etl_step_2
   left  join dim_mms_product dim_mms_product
	on dim_mms_product.dim_mms_product_key  = #etl_step_2.fact_mms_package_dim_product_key
	left join dim_description dim_description
	on dim_description.dim_description_key = #etl_step_2.package_status_dim_description_key
	
if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(fact_mms_package_session_key), location=user_db) as  
select #etl_step_3.fact_mms_package_session_key	fact_mms_package_session_key,
       #etl_step_3.created_dim_date_key created_dim_date_key,
	   #etl_step_3.delivered_dim_date_key delivered_dim_date_key,
	   #etl_step_3.fact_mms_package_dim_product_key fact_mms_package_dim_product_key,
	   #etl_step_3.delivered_dim_club_key delivered_dim_club_key,
	   #etl_step_3.dim_mms_member_key dim_mms_member_key,
	   #etl_step_3.package_session_id package_session_id,
	   #etl_step_3.mms_tran_id mms_tran_id,
	   case when #etl_step_3.dim_mms_product_id in ('1482','2785') then 1 else 0
	   end body_age_assessment_count,
	   #etl_step_3.session_complete_count session_complete_count,
	   case when #etl_step_3.package_status_dim_description_key is null then 'N'
	   when #etl_step_3.package_status_dim_description_key in ('-997','-998','-999') then 'N'
       when #etl_step_3.dim_description_id	is null  then 'N'
       when #etl_step_3.dim_description_id = 4 then 'Y'
       else 'N'	 
      end voided_flag ,
	  #etl_step_3.package_entered_dim_club_key package_entered_dim_club_key,
	  #etl_step_3.primary_sales_dim_employee_key primary_sales_dim_employee_key,
	  #etl_step_3.secondary_sales_dim_employee_key secondary_sales_dim_employee_key,
	  #etl_step_3.delivered_dim_employee_key delivered_dim_employee_key,
	  #etl_step_3.edw_inserted_dim_date_key edw_inserted_dim_date_key,
	  #etl_step_3.delivered_session_price delivered_session_price,
	  #etl_step_3.tran_item_id tran_item_id,
	  #etl_step_3.package_entered_dim_employee_key package_entered_dim_employee_key,
	  #etl_step_3.delivered_session_discount_value delivered_session_discount_value,
	  #etl_step_3.delivered_session_lt_bucks_amount delivered_session_lt_bucks_amount,
	  #etl_step_3.number_of_sessions_in_package number_of_sessions_in_package,
	  #etl_step_3.original_currency_code original_currency_code,
	  #etl_step_3.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
	  #etl_step_3.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
	  #etl_step_3.created_dim_time_key created_dim_time_key,
	  #etl_step_3.delivered_dim_time_key delivered_dim_time_key,
	  #etl_step_3.fact_mms_package_key fact_mms_package_key ,
	  #etl_step_3.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
	  #etl_step_3.package_created_dim_date_key package_created_dim_date_key,
	  #etl_step_3.package_created_dim_time_key package_created_dim_time_key,
	  #etl_step_3.package_status_dim_description_key package_status_dim_description_key,
	  #etl_step_3.sales_channel_dim_description_key sales_channel_dim_description_key,
	  #etl_step_3.package_edited_flag package_edited_flag,
	  #etl_step_3.session_comment session_comment,
	  #etl_step_3.reporting_dim_club_key reporting_dim_club_key,
	  #etl_step_3.reporting_local_currency_monthly_average_dim_exchange_rate_key reporting_local_currency_monthly_average_dim_exchange_rate_key,
	  #etl_step_3.reporting_local_currency_dim_plan_exchange_rate_key reporting_local_currency_dim_plan_exchange_rate_key,
	  #etl_step_3.package_id package_id,
	  #etl_step_3.dv_load_date_time,
	  #etl_step_3.dv_batch_id
	  from #etl_step_3

/* Delete and re-insert as a single transaction*/
/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/

begin tran

  delete dbo.fact_mms_package_session
   where fact_mms_package_session_key in (select fact_mms_package_session_key from dbo.#etl_step_4) 
   
    insert into fact_mms_package_session
	(   
	    fact_mms_package_session_key
		,package_session_id
		,body_age_assessment_count
		,created_dim_date_key
		,created_dim_time_key
		,delivered_dim_club_key
		,delivered_dim_date_key
		,delivered_dim_employee_key
		,delivered_dim_time_key
		,delivered_session_discount_value
		,delivered_session_lt_bucks_amount
		,delivered_session_price
		,dim_mms_member_key
		,edw_inserted_dim_date_key
		,fact_mms_package_dim_product_key
		,fact_mms_package_key
		,fact_mms_sales_transaction_key
		,mms_tran_id
		,number_of_sessions_in_package
		,original_currency_code
		,package_created_dim_date_key
		,package_created_dim_time_key
		,package_edited_flag
		,package_entered_dim_club_key
		,package_entered_dim_employee_key
		,package_id
		,package_status_dim_description_key
		,primary_sales_dim_employee_key
		,reporting_dim_club_key
		,reporting_local_currency_dim_plan_exchange_rate_key
		,reporting_local_currency_monthly_average_dim_exchange_rate_key
		,sales_channel_dim_description_key
		,secondary_sales_dim_employee_key
		,session_comment
		,session_complete_count
		,tran_item_id
		,usd_dim_plan_exchange_rate_key
		,usd_monthly_average_dim_exchange_rate_key
		,voided_flag
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user

	)
   
   select
         fact_mms_package_session_key
        ,package_session_id
		,body_age_assessment_count
		,created_dim_date_key
		,created_dim_time_key
		,delivered_dim_club_key
		,delivered_dim_date_key
		,delivered_dim_employee_key
		,delivered_dim_time_key
		,delivered_session_discount_value
		,delivered_session_lt_bucks_amount
		,delivered_session_price
		,dim_mms_member_key
		,edw_inserted_dim_date_key
		,fact_mms_package_dim_product_key
		,fact_mms_package_key
		,fact_mms_sales_transaction_key
		,mms_tran_id
		,number_of_sessions_in_package
		,original_currency_code
		,package_created_dim_date_key
		,package_created_dim_time_key
		,package_edited_flag
		,package_entered_dim_club_key
		,package_entered_dim_employee_key
		,package_id
		,package_status_dim_description_key
		,primary_sales_dim_employee_key
		,reporting_dim_club_key
		,reporting_local_currency_dim_plan_exchange_rate_key
		,reporting_local_currency_monthly_average_dim_exchange_rate_key
		,sales_channel_dim_description_key
		,secondary_sales_dim_employee_key
		,session_comment
		,session_complete_count
		,tran_item_id
		,usd_dim_plan_exchange_rate_key
		,usd_monthly_average_dim_exchange_rate_key
		,voided_flag
		,dv_load_date_time
        ,'dec 31, 9999'
        ,dv_batch_id
        ,getdate()
         ,suser_sname()
		 from #etl_step_4
   
 commit tran
 

/* Get FactPackage records associated with Employees in the currentBatch, in the current month based on BeginExtractDateTime, and MMSClubID 13 or DimLocationKey = 2  */
if object_id('tempdb..#fact_mms_package') is not null drop table #fact_mms_package
create table dbo.#fact_mms_package with(distribution=hash(fact_mms_package_key), location=user_db) as
select fact_mms_package.fact_mms_package_key fact_mms_package_key,
       fact_mms_package.reporting_dim_club_key reporting_dim_club_key,
	   fact_mms_package.reporting_local_currency_monthly_average_dim_exchange_rate_key reporting_local_currency_monthly_average_dim_exchange_rate_key,
	   fact_mms_package.reporting_local_currency_dim_plan_exchange_rate_key reporting_local_currency_dim_plan_exchange_rate_key
into #fact_mms_package
from fact_mms_package
join dim_employee
on fact_mms_package.primary_sales_dim_employee_key = dim_employee.dim_employee_key
join dim_date
on dim_date.calendar_date = convert(varchar, @begin_extract_date_time, 112) 
join dim_club
on fact_mms_package.dim_club_key =  dim_club.dim_club_key
where fact_mms_package.transaction_post_dim_date_key >= dim_date.month_starting_dim_date_key
and (dim_club.club_id = 13
      or fact_mms_package.dim_club_key = '-998')

	  
 /* Main query*/
 if object_id('tempdb..#fact_mms_package_session') is not null drop table #fact_mms_package_session
create table dbo.#fact_mms_package_session with(distribution=hash(fact_mms_package_session_key), location=user_db) as
select fact_mms_package_session.fact_mms_package_session_key fact_mms_package_session_key,
       #fact_mms_package.reporting_dim_club_key reporting_dim_club_key,
	   #fact_mms_package.reporting_local_currency_monthly_average_dim_exchange_rate_key reporting_local_currency_monthly_average_dim_exchange_rate_key,
	   #fact_mms_package.reporting_local_currency_dim_plan_exchange_rate_key reporting_local_currency_dim_plan_exchange_rate_key
into #fact_mms_package_session
from fact_mms_package_session
join #fact_mms_package
on fact_mms_package_session.fact_mms_package_key = #fact_mms_package.fact_mms_package_key
 
 update fact_mms_package_session
 set reporting_dim_club_key = #fact_mms_package_session.reporting_dim_club_key,
 reporting_local_currency_monthly_average_dim_exchange_rate_key=#fact_mms_package_session.reporting_local_currency_monthly_average_dim_exchange_rate_key,
 reporting_local_currency_dim_plan_exchange_rate_key= #fact_mms_package_session.reporting_local_currency_dim_plan_exchange_rate_key
 from  #fact_mms_package_session
 where fact_mms_package_session.fact_mms_package_session_key=#fact_mms_package_session.fact_mms_package_session_key

  /*-UDW-7767/*Logic from proc_LTFDW_FactECommerceSalesTransaction_HybrisPackages is mimiced*/*/
 if object_id('tempdb..#LTFDW_FactECommerceSalesTransaction_HybrisPackages') is not null drop table #LTFDW_FactECommerceSalesTransaction_HybrisPackages
 create table dbo.#LTFDW_FactECommerceSalesTransaction_HybrisPackages with(distribution=hash(fact_mms_package_session_key), location=user_db) as  
 SELECT fact_mms_package_session.fact_mms_package_session_key  fact_mms_package_session_key,
        case when number_of_sessions_in_package = 0 then 0
             else (fact_hybris_transaction_item.transaction_amount_gross / fact_mms_package_session.number_of_sessions_in_package) 
         end delivered_session_price,
        case when number_of_sessions_in_package = 0 then 0
             else fact_hybris_transaction_item.bucks_amount / fact_mms_package_session.number_of_sessions_in_package
         end delivered_session_lt_bucks_amount,
        case when number_of_sessions_in_package = 0 then 0
             else fact_hybris_transaction_item.discount_amount / fact_mms_package_session.number_of_sessions_in_package
         end delivered_session_discount_value
  from fact_mms_package_session
  join fact_hybris_transaction_item
    on fact_mms_package_session.fact_mms_sales_transaction_key=fact_hybris_transaction_item.fact_mms_sales_transaction_key
 Where (fact_mms_package_session.dv_batch_id=@load_dv_batch_id
        or fact_hybris_transaction_item.dv_batch_id = @load_dv_batch_id)
   and fact_mms_package_session.delivered_session_price <> case when number_of_sessions_in_package = 0 then 0
	                                                            else (fact_hybris_transaction_item.transaction_amount_gross / fact_mms_package_session.number_of_sessions_in_package) end
union
/*UDW-9465 Include Magento Transaction Item */
select fact_mms_package_session.fact_mms_package_session_key  fact_mms_package_session_key,
       case when fact_mms_package_session.number_of_sessions_in_package = 0 then 0
            else fact_magento_order_item.per_package_amount / fact_mms_package_session.number_of_sessions_in_package
        end delivered_session_price,
       case when fact_mms_package_session.number_of_sessions_in_package = 0 then 0
            else fact_magento_order_item.per_package_bucks / fact_mms_package_session.number_of_sessions_in_package
        end delivered_session_lt_bucks_amount,
       case when fact_mms_package_session.number_of_sessions_in_package = 0 then 0
            else fact_magento_order_item.per_package_discount / fact_mms_package_session.number_of_sessions_in_package
        end delivered_session_discount_value
  from fact_mms_package_session fact_mms_package_session
  join d_magento_lifetime_order_item_change_log 
    on fact_mms_package_session.fact_mms_sales_transaction_key=d_magento_lifetime_order_item_change_log.fact_mms_transaction_key
  join (select dlog.fact_magento_order_item_key,
               oi.item_discount_amount / cast(count(*) as decimal(26,6)) per_package_discount,
               oi.item_total_amount / cast(count(*) as decimal(26,6)) per_package_amount,
               oi.item_bucks_amount / cast(count(*) as decimal(26,6)) per_package_bucks,
               oi.dv_batch_id
          from d_magento_lifetime_order_item_change_log dlog
          join fact_magento_order_item oi on dlog.fact_magento_order_item_key = oi.fact_magento_order_item_key
         where dlog.transaction_type = 'mms transaction'
         group by dlog.fact_magento_order_item_key,
                  oi.item_total_amount, 
                  oi.item_discount_amount, 
                  oi.item_bucks_amount,
                  oi.dv_batch_id) fact_magento_order_item
    on d_magento_lifetime_order_item_change_log.fact_magento_order_item_key = fact_magento_order_item.fact_magento_order_item_key
where (fact_mms_package_session.dv_batch_id=@load_dv_batch_id
       or fact_magento_order_item.dv_batch_id = @load_dv_batch_id 
       or d_magento_lifetime_order_item_change_log.dv_batch_id = @load_dv_batch_id)
  and number_of_sessions_in_package <> 0



 update fact_mms_package_session
 set delivered_session_price =  #LTFDW_FactECommerceSalesTransaction_HybrisPackages.delivered_session_price,
     delivered_session_lt_bucks_amount = #LTFDW_FactECommerceSalesTransaction_HybrisPackages.delivered_session_lt_bucks_amount,
     delivered_session_discount_value = #LTFDW_FactECommerceSalesTransaction_HybrisPackages.delivered_session_discount_value
 from  #LTFDW_FactECommerceSalesTransaction_HybrisPackages
 where fact_mms_package_session.fact_mms_package_session_key=#LTFDW_FactECommerceSalesTransaction_HybrisPackages.fact_mms_package_session_key
 
 end
