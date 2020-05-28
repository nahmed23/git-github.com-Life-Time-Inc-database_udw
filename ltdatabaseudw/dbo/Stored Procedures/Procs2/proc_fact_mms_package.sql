CREATE PROC [dbo].[proc_fact_mms_package] @dv_batch_id [varchar](500),@begin_extract_date_time [datetime] AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_package)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#mms_package') is not null drop table #mms_package
create table dbo.#mms_package with(distribution=hash(fact_mms_package_key), location=user_db) as
 /* Get the MMSPackage data in the currentBatch*/
SELECT
       d_mms_package.fact_mms_package_key fact_mms_package_key,
	   d_mms_package.fact_mms_sales_transaction_item_key fact_mms_sales_transaction_item_key,
       d_mms_package.package_id package_id,
	   d_mms_package.club_id club_id,
	   d_mms_package.dim_mms_product_key dim_mms_product_key,
       d_mms_package.dim_mms_member_key dim_mms_member_key,
       d_mms_package.dim_club_key dim_club_key,
	   d_mms_package.package_entered_dim_employee_key package_entered_dim_employee_key,
       d_mms_package.number_of_sessions number_of_sessions,
       d_mms_package.price_per_session price_per_session,
	   d_mms_package.mms_tran_id mms_tran_id,
       d_mms_package.tran_item_id tran_item_id,
       d_mms_package.created_date_time created_date_time,
	   d_mms_package.created_dim_date_key created_dim_date_key,
	   d_mms_package.created_dim_time_key created_dim_time_key,
	   d_mms_package.sessions_remaining sessions_remaining,
	   d_mms_package.balance_amount balance_amount,
	   d_mms_package.package_edited_flag package_edited_flag,
	   d_mms_package.package_edit_dim_date_key package_edit_dim_date_key,
	   d_mms_package.package_edit_dim_time_key package_edit_dim_time_key,
	   d_mms_package.package_status_dim_description_key package_status_dim_description_key,
	   d_mms_package.external_package_id external_package_id,
	   d_mms_package.transaction_source transaction_source,
	   d_mms_package.inserted_date_time,  /*-added UDW-8114*/
       d_mms_package.updated_date_time,  /*-added UDW-8114*/
       d_mms_package.inserted_dim_date_key, /*-added UDW-8114*/
       d_mms_package.inserted_dim_time_key, /*-added UDW-8114*/
       d_mms_package.updated_dim_date_key, /*-added UDW-8114*/
       d_mms_package.updated_dim_time_key, /*-added UDW-8114*/
	   d_mms_package.dv_batch_id dv_batch_id,
	   d_mms_package.dv_load_date_time dv_load_date_time
  INTO #mms_package
  FROM d_mms_package
 WHERE d_mms_package.dv_batch_id >= @load_dv_batch_id

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_sales_transaction_item_key), location=user_db) as

SELECT #mms_package.fact_mms_package_key fact_mms_package_key,
       #mms_package.package_id package_id,
	   #mms_package.club_id club_id,
	   fact_mms_sales_transaction_item.fact_mms_sales_transaction_key fact_mms_sales_transaction_key, /*/* added as part of analysis UDW-7195*/*/
       fact_mms_sales_transaction_item.primary_sales_dim_employee_key primary_sales_dim_employee_key,
	   fact_mms_sales_transaction_item.secondary_sales_dim_employee_key secondary_sales_dim_employee_key,
	   #mms_package.package_entered_dim_employee_key package_entered_dim_employee_key,
	   #mms_package.dim_mms_member_key dim_mms_member_key,
	   #mms_package.dim_mms_product_key dim_mms_product_key,
	   #mms_package.created_dim_date_key created_dim_date_key,
	   #mms_package.created_dim_time_key created_dim_time_key,
	   case when fact_mms_sales_transaction_item.tran_item_id is null then '-998'
	   when fact_mms_sales_transaction_item.post_dim_date_key is null then '-997'
       else post_dim_date_key end transaction_post_dim_date_key,
	   #mms_package.dim_club_key dim_club_key,
	   #mms_package.package_status_dim_description_key package_status_dim_description_key,
	   #mms_package.package_edit_dim_date_key package_edit_dim_date_key,
	   #mms_package.package_edit_dim_time_key package_edit_dim_time_key,
	   fact_mms_sales_transaction_item.sales_channel_dim_description_key sales_channel_dim_description_key,
	   #mms_package.mms_tran_id mms_tran_id,
	   #mms_package.tran_item_id tran_item_id,
	   #mms_package.package_edited_flag package_edited_flag,
	   fact_mms_sales_transaction_item.voided_flag transaction_void_flag,
	   #mms_package.number_of_sessions number_of_sessions,
	   #mms_package.sessions_remaining sessions_remaining,
	   #mms_package.price_per_session price_per_session,
	   #mms_package.balance_amount balance_amount,
	   fact_mms_sales_transaction_item.sales_discount_dollar_amount sales_discount_amount,
	   case when #mms_package.tran_item_id is null then 'USD'
	   when fact_mms_sales_transaction_item.original_currency_code  is null then 'USD'
	   else fact_mms_sales_transaction_item.original_currency_code
	   end original_currency_code,
	   fact_mms_sales_transaction_item.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
	   fact_mms_sales_transaction_item.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
	   #mms_package.external_package_id external_package_id,
	   #mms_package.transaction_source transaction_source,
	   fact_mms_sales_transaction_item.item_lt_bucks_amount,
	   #mms_package.inserted_date_time,
       #mms_package.updated_date_time,
       #mms_package.inserted_dim_date_key,
       #mms_package.inserted_dim_time_key,
       #mms_package.updated_dim_date_key,
       #mms_package.updated_dim_time_key,
	   #mms_package.dv_batch_id dv_batch_id,
	   #mms_package.dv_load_date_time dv_load_date_time,
       fact_mms_sales_transaction_item.fact_mms_sales_transaction_item_key
  INTO #etl_step_1
  FROM #mms_package
 LEFT JOIN fact_mms_sales_transaction_item fact_mms_sales_transaction_item
  on #mms_package.fact_mms_sales_transaction_item_key = fact_mms_sales_transaction_item.fact_mms_sales_transaction_item_key

if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_mms_package_key), location=user_db) as
SELECT #etl_step_1.fact_mms_package_key fact_mms_package_key,
       #etl_step_1.package_id package_id,
	   #etl_step_1.club_id club_id,
	   #etl_step_1.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
	   #etl_step_1.primary_sales_dim_employee_key primary_sales_dim_employee_key,
	   #etl_step_1.secondary_sales_dim_employee_key secondary_sales_dim_employee_key,
	   #etl_step_1.package_entered_dim_employee_key package_entered_dim_employee_key,
	   #etl_step_1.dim_mms_member_key dim_mms_member_key,
	   #etl_step_1.dim_mms_product_key dim_mms_product_key,
	   #etl_step_1.created_dim_date_key created_dim_date_key,
	   #etl_step_1.created_dim_time_key created_dim_time_key,
	   #etl_step_1.transaction_post_dim_date_key transaction_post_dim_date_key,
	   #etl_step_1.dim_club_key dim_club_key,
	   #etl_step_1.package_status_dim_description_key package_status_dim_description_key,
	   #etl_step_1.package_edit_dim_date_key package_edit_dim_date_key,
	   #etl_step_1.package_edit_dim_time_key package_edit_dim_time_key,
	   #etl_step_1.sales_channel_dim_description_key sales_channel_dim_description_key,
	   #etl_step_1.mms_tran_id mms_tran_id,
	   #etl_step_1.tran_item_id tran_item_id,
	   #etl_step_1.package_edited_flag package_edited_flag,
	   #etl_step_1.transaction_void_flag transaction_void_flag,
	   #etl_step_1.number_of_sessions number_of_sessions,
	   #etl_step_1.sessions_remaining sessions_remaining,
	   #etl_step_1.price_per_session price_per_session,
	   #etl_step_1.balance_amount balance_amount,
	   #etl_step_1.sales_discount_amount sales_discount_amount,
	   #etl_step_1.original_currency_code original_currency_code,
	   #etl_step_1.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
	   #etl_step_1.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
	   #etl_step_1.external_package_id external_package_id,
	   #etl_step_1.transaction_source transaction_source,
	   #etl_step_1.item_lt_bucks_amount item_lt_bucks_amount,
	   #etl_step_1.inserted_date_time inserted_date_time,
       #etl_step_1.updated_date_time updated_date_time,
       #etl_step_1.inserted_dim_date_key inserted_dim_date_key,
       #etl_step_1.inserted_dim_time_key inserted_dim_time_key,
       #etl_step_1.updated_dim_date_key updated_dim_date_key,
       #etl_step_1.updated_dim_time_key updated_dim_time_key,
	   dim_date.year calendar_year,
	   dim_date.month_ending_date calendar_month_ending_date,
	   dim_date.month_ending_dim_date_key month_ending_dim_date_key,
	   dim_employee.dim_club_key employee_dim_club_key,
	   #etl_step_1.dv_batch_id dv_batch_id,
	   #etl_step_1.dv_load_date_time dv_load_date_time
INTO #etl_step_2
FROM #etl_step_1
LEFT JOIN dim_date
on #etl_step_1.transaction_post_dim_date_key = dim_date.dim_date_key
LEFT JOIN dim_employee
on #etl_step_1.primary_sales_dim_employee_key = dim_employee.dim_employee_key


if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(employee_dim_club_key), location=user_db) as
SELECT  #etl_step_2.fact_mms_package_key fact_mms_package_key,
        #etl_step_2.employee_dim_club_key employee_dim_club_key,
        dim_club.club_id club_id
INTO #etl_step_3
FROM #etl_step_2
LEFT JOIN dim_club
on #etl_step_2.employee_dim_club_key = dim_club.dim_club_key



 declare @corporate_dim_club_key char(32)
 select @corporate_dim_club_key = (select dim_club_key
 from dim_club where club_id = 13)

if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(fact_mms_package_key), location=user_db) as
select #etl_step_2.fact_mms_package_key fact_mms_package_key,
       #etl_step_2.package_id package_id,
	   #etl_step_2.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
	   #etl_step_2.primary_sales_dim_employee_key primary_sales_dim_employee_key,
	   #etl_step_2.secondary_sales_dim_employee_key secondary_sales_dim_employee_key,
	   #etl_step_2.package_entered_dim_employee_key package_entered_dim_employee_key,
	   #etl_step_2.dim_mms_member_key dim_mms_member_key,
	   #etl_step_2.dim_mms_product_key dim_mms_product_key,
	   #etl_step_2.created_dim_date_key created_dim_date_key,
	   #etl_step_2.created_dim_time_key created_dim_time_key,
	   #etl_step_2.transaction_post_dim_date_key transaction_post_dim_date_key,
	   #etl_step_2.dim_club_key dim_club_key,
	   #etl_step_2.package_status_dim_description_key package_status_dim_description_key,
	   #etl_step_2.package_edit_dim_date_key package_edit_dim_date_key,
	   #etl_step_2.package_edit_dim_time_key package_edit_dim_time_key,
	   #etl_step_2.sales_channel_dim_description_key sales_channel_dim_description_key,
	   #etl_step_2.mms_tran_id mms_tran_id,
	   #etl_step_2.tran_item_id tran_item_id,
	   #etl_step_2.package_edited_flag package_edited_flag,
	   #etl_step_2.transaction_void_flag transaction_void_flag,
	   CASE WHEN #etl_step_2.Number_Of_Sessions IS NULL THEN 0 ELSE #etl_step_2.Number_Of_Sessions END Number_Of_Sessions,
	   CASE WHEN #etl_step_2.sessions_remaining IS NULL THEN 0 ELSE #etl_step_2.sessions_remaining END sessions_remaining,
	   CASE WHEN #etl_step_2.price_per_session IS NULL THEN 0 ELSE #etl_step_2.price_per_session END price_per_session,
	   CASE WHEN #etl_step_2.balance_amount IS NULL THEN 0 ELSE #etl_step_2.balance_amount END balance_amount,
	   #etl_step_2.sales_discount_amount sales_discount_amount,
	   #etl_step_2.original_currency_code original_currency_code,
	   #etl_step_2.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
	   #etl_step_2.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
	   #etl_step_2.calendar_year calendar_year,
	   #etl_step_2.calendar_month_ending_date calendar_month_ending_date,
	   #etl_step_2.month_ending_dim_date_key month_ending_dim_date_key,
	   #etl_step_2.external_package_id external_package_id,
	   #etl_step_2.transaction_source transaction_source,
	   CASE WHEN #etl_step_2.item_lt_bucks_amount IS NULL THEN 0 ELSE #etl_step_2.item_lt_bucks_amount END item_lt_bucks_amount,
	   #etl_step_2.inserted_date_time inserted_date_time,
       #etl_step_2.updated_date_time updated_date_time,
       #etl_step_2.inserted_dim_date_key inserted_dim_date_key,
       #etl_step_2.inserted_dim_time_key inserted_dim_time_key,
       #etl_step_2.updated_dim_date_key updated_dim_date_key,
       #etl_step_2.updated_dim_time_key updated_dim_time_key,
case when #etl_step_2.club_id is null then '-998'
when #etl_step_2.club_id <> 13 then #etl_step_2.dim_club_key
when (#etl_step_2.primary_sales_dim_employee_key not in ('-997','-998','-999') )and (( #etl_step_2.calendar_month_ending_date is null )
or (#etl_step_3.employee_dim_club_key is null )or (#etl_step_3.club_id is null) ) then '-997'
when #etl_step_2.primary_sales_dim_employee_key not in ('-997','-998','-999') and #etl_step_3.club_id <> 13
then #etl_step_2.employee_dim_club_key
when @corporate_dim_club_key is null then '-997'
when @corporate_dim_club_key  is not null then @corporate_dim_club_key
end reporting_dim_club_key,
	   #etl_step_2.dv_batch_id dv_batch_id,
	   #etl_step_2.dv_load_date_time dv_load_date_time
into #etl_step_4
from #etl_step_2
left join #etl_step_3
on #etl_step_2.fact_mms_package_key = #etl_step_3.fact_mms_package_key


if object_id('tempdb..#etl_step_5') is not null drop table #etl_step_5
create table dbo.#etl_step_5 with(distribution=hash(fact_mms_package_key), location=user_db) as
select #etl_step_4.fact_mms_package_key fact_mms_package_key,
       #etl_step_4.package_id package_id,
	   #etl_step_4.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
	   #etl_step_4.primary_sales_dim_employee_key primary_sales_dim_employee_key,
	   #etl_step_4.secondary_sales_dim_employee_key secondary_sales_dim_employee_key,
	   #etl_step_4.package_entered_dim_employee_key package_entered_dim_employee_key,
	   #etl_step_4.dim_mms_member_key dim_mms_member_key,
	   #etl_step_4.dim_mms_product_key dim_mms_product_key,
	   #etl_step_4.created_dim_date_key created_dim_date_key,
	   #etl_step_4.created_dim_time_key created_dim_time_key,
	   #etl_step_4.transaction_post_dim_date_key transaction_post_dim_date_key,
	   #etl_step_4.dim_club_key dim_club_key,
	   #etl_step_4.package_status_dim_description_key package_status_dim_description_key,
	   #etl_step_4.package_edit_dim_date_key package_edit_dim_date_key,
	   #etl_step_4.package_edit_dim_time_key package_edit_dim_time_key,
	   #etl_step_4.sales_channel_dim_description_key sales_channel_dim_description_key,
	   #etl_step_4.mms_tran_id mms_tran_id,
	   #etl_step_4.tran_item_id tran_item_id,
	   #etl_step_4.package_edited_flag package_edited_flag,
	   #etl_step_4.transaction_void_flag transaction_void_flag,
	   #etl_step_4.number_of_sessions number_of_sessions,
	   #etl_step_4.sessions_remaining sessions_remaining,
	   #etl_step_4.price_per_session price_per_session,
	   #etl_step_4.balance_amount balance_amount,
	   #etl_step_4.sales_discount_amount sales_discount_amount,
	   #etl_step_4.original_currency_code original_currency_code,
	   #etl_step_4.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
	   #etl_step_4.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
	   #etl_step_4.calendar_year calendar_year,
	   #etl_step_4.calendar_month_ending_date calendar_month_ending_date,
	   #etl_step_4.month_ending_dim_date_key month_ending_dim_date_key,
       #etl_step_4.reporting_dim_club_key reporting_dim_club_key,
       #etl_step_4.external_package_id external_package_id,
	   #etl_step_4.transaction_source transaction_source,
       #etl_step_4.item_lt_bucks_amount item_lt_bucks_amount,
	   #etl_step_4.inserted_date_time inserted_date_time,
       #etl_step_4.updated_date_time updated_date_time,
       #etl_step_4.inserted_dim_date_key inserted_dim_date_key,
       #etl_step_4.inserted_dim_time_key inserted_dim_time_key,
       #etl_step_4.updated_dim_date_key updated_dim_date_key,
       #etl_step_4.updated_dim_time_key updated_dim_time_key,
case when #etl_step_4.fact_mms_package_key in ('-997', '-998', '-999') then #etl_step_4.fact_mms_package_key
when #etl_step_4.month_ending_dim_date_key in ('-997', '-998', '-999') then #etl_step_4.month_ending_dim_date_key
when dim_club.local_currency_code is null
then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_4.month_ending_dim_date_key),'z#@$k%&P')+
                                          'P%#&z$@k'+isnull(#etl_step_4.original_currency_code,'z#@$k%&P')+
                                          'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                          'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_4.month_ending_dim_date_key),'z#@$k%&P')+
                                     'P%#&z$@k'+isnull(#etl_step_4.original_currency_code,'z#@$k%&P')+
                                     'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                     'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
end reporting_local_currency_monthly_average_dim_exchange_rate_key,
case when #etl_step_4.fact_mms_package_key in ('-997', '-998', '-999') then #etl_step_4.fact_mms_package_key
when dim_club.local_currency_code is null
then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_4.original_currency_code,'z#@$k%&P')+
                                     'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_4.original_currency_code,'z#@$k%&P')+
                                          'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
end reporting_local_currency_dim_plan_exchange_rate_key,
#etl_step_4.price_per_session - case when #etl_step_4.item_lt_bucks_amount = 0 or #etl_step_4.Number_Of_Sessions = 0 then 0 else (#etl_step_4.item_lt_bucks_amount / #etl_step_4.Number_Of_Sessions) end price_per_session_less_lt_bucks,
#etl_step_4.sessions_remaining * (#etl_step_4.price_per_session - case when #etl_step_4.item_lt_bucks_amount = 0 or #etl_step_4.Number_Of_Sessions = 0 then 0 else (#etl_step_4.item_lt_bucks_amount / #etl_step_4.Number_Of_Sessions) end) balance_amount_less_lt_bucks,
#etl_step_4.dv_batch_id dv_batch_id,
#etl_step_4.dv_load_date_time dv_load_date_time
into #etl_step_5
from #etl_step_4
left join dim_club
on #etl_step_4.reporting_dim_club_key = dim_club.dim_club_key


/*   Delete and re-insert as a single transaction*/
/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/




begin tran

  delete dbo.fact_mms_package
   where fact_mms_package_key in (select fact_mms_package_key from dbo.#etl_step_5)

   insert into fact_mms_package
           (fact_mms_package_key,
            package_id,
			fact_mms_sales_transaction_key,
			primary_sales_dim_employee_key,
			secondary_sales_dim_employee_key,
			package_entered_dim_employee_key,
			dim_mms_member_key,
			dim_mms_product_key,
			created_dim_date_key,
			created_dim_time_key,
			transaction_post_dim_date_key,
			dim_club_key,
			package_status_dim_description_key,
			package_edit_dim_date_key,
			package_edit_dim_time_key,
			sales_channel_dim_description_key,
			mms_tran_id,
			tran_item_id,
			package_edited_flag,
			transaction_void_flag,
			number_of_sessions,
			sessions_left,
			price_per_session,
			balance_amount,
			sales_discount_amount,
			original_currency_code,
			usd_monthly_average_dim_exchange_rate_key,
			usd_dim_plan_exchange_rate_key,
			reporting_dim_club_key,
			external_package_id,
	        transaction_source,
			item_lt_bucks_amount, /*-added UDW-8114*/
			inserted_date_time,  /*-added UDW-8114*/
			updated_date_time,  /*-added UDW-8114*/
			inserted_dim_date_key, /*-added UDW-8114*/
			inserted_dim_time_key, /*-added UDW-8114*/
			updated_dim_date_key, /*-added UDW-8114*/
			updated_dim_time_key, /*-added UDW-8114*/
			reporting_local_currency_monthly_average_dim_exchange_rate_key,
			reporting_local_currency_dim_plan_exchange_rate_key,
			price_per_session_less_lt_bucks, /*-added UDW-9449*/
			balance_amount_less_lt_bucks, /*-added UDW-9449*/
			dv_load_date_time,
            dv_load_end_date_time,
            dv_batch_id,
            dv_inserted_date_time,
            dv_insert_user)

	select fact_mms_package_key,
            package_id,
			fact_mms_sales_transaction_key,
			primary_sales_dim_employee_key,
			secondary_sales_dim_employee_key,
			package_entered_dim_employee_key,
			dim_mms_member_key,
			dim_mms_product_key,
			created_dim_date_key,
			created_dim_time_key,
			transaction_post_dim_date_key,
			dim_club_key,
			package_status_dim_description_key,
			package_edit_dim_date_key,
			package_edit_dim_time_key,
			sales_channel_dim_description_key,
			mms_tran_id,
			tran_item_id,
			package_edited_flag,
			transaction_void_flag,
			number_of_sessions,
			sessions_remaining,
			price_per_session,
			balance_amount,
			sales_discount_amount,
			original_currency_code,
			usd_monthly_average_dim_exchange_rate_key,
			usd_dim_plan_exchange_rate_key,
			reporting_dim_club_key,
			external_package_id,
	        transaction_source,
			item_lt_bucks_amount, /*-added UDW-8114*/
			inserted_date_time,  /*-added UDW-8114*/
			updated_date_time,  /*-added UDW-8114*/
			inserted_dim_date_key, /*-added UDW-8114*/
			inserted_dim_time_key, /*-added UDW-8114*/
			updated_dim_date_key, /*-added UDW-8114*/
			updated_dim_time_key, /*-added UDW-8114			*/
			reporting_local_currency_monthly_average_dim_exchange_rate_key,
			reporting_local_currency_dim_plan_exchange_rate_key,
			price_per_session_less_lt_bucks, /* added UDW-9449  */
			balance_amount_less_lt_bucks, /* added UDW-9449  */
			dv_load_date_time,
            'dec 31, 9999',
            dv_batch_id,
            getdate() ,
            suser_sname()
			from #etl_step_5

commit tran


DECLARE @month_starting_dim_date_key char(8)
  /* Find the DimDateKey for the first day of the month in $$BeginExtractDateTime*/
select @month_starting_dim_date_key =
    month_starting_dim_date_key
  from dim_date
 where convert(varchar, @begin_extract_date_time, 112) = dim_date.calendar_date

 if object_id('tempdb..#fact_mms_package') is not null drop table #fact_mms_package
create table dbo.#fact_mms_package with(distribution=hash(fact_mms_package_key), location=user_db) as
SELECT fact_mms_package.fact_mms_package_key,
       fact_mms_package.primary_sales_dim_employee_key,
       fact_mms_package.transaction_post_dim_date_key,
       fact_mms_package.reporting_local_currency_monthly_average_dim_exchange_rate_key,
       fact_mms_package.reporting_local_currency_dim_plan_exchange_rate_key
  INTO #fact_mms_package
  FROM fact_mms_package
  JOIN dim_employee dim_employee
    ON dim_employee.dim_employee_key = fact_mms_package.primary_sales_dim_employee_key
  JOIN dim_club dim_club
    ON fact_mms_package.dim_club_key = dim_club.dim_club_key
WHERE (dim_club.dim_club_id = 13 OR fact_mms_package.dim_club_key = '-998')
   AND dim_employee.dv_batch_id = @load_dv_batch_id
   AND fact_mms_package.transaction_post_dim_date_key >= @month_starting_dim_date_key


if object_id('tempdb..#dim_date_transaction_post') is not null drop table #dim_date_transaction_post
create table dbo.#dim_date_transaction_post with(distribution=hash(fact_mms_package_key), location=user_db) as
SELECT #fact_mms_package.fact_mms_package_key fact_mms_package_key,
       dim_date.month_ending_date month_ending_date,
       dim_date.month_ending_dim_date_key month_ending_dim_date_key,
	   #fact_mms_package.primary_sales_dim_employee_key primary_sales_dim_employee_key
 FROM #fact_mms_package
JOIN  dim_date
ON #fact_mms_package.transaction_post_dim_date_key = dim_date.dim_date_key

if object_id('tempdb..#dim_employee_club') is not null drop table #dim_employee_club
create table dbo.#dim_employee_club with(distribution=hash(fact_mms_package_key), location=user_db) as
SELECT #dim_date_transaction_post.fact_mms_package_key fact_mms_package_key,
       dim_employee.dim_club_key dim_club_key
 FROM #dim_date_transaction_post
 LEFT JOIN dim_employee
ON #dim_date_transaction_post.primary_sales_dim_employee_key = dim_employee.dim_employee_key


if object_id('tempdb..#dim_club_employee') is not null drop table #dim_club_employee
create table dbo.#dim_club_employee with(distribution=hash(fact_mms_package_key), location=user_db) as
SELECT #dim_employee_club.fact_mms_package_key fact_mms_package_key,
       dim_club.dim_club_id dim_club_id,
       dim_club.club_close_dim_date_key club_close_dim_date_key
FROM #dim_employee_club
LEFT JOIN dim_club
on #dim_employee_club.dim_club_key = dim_club.dim_club_key



if object_id('tempdb..#etl_post_processing1') is not null drop table #etl_post_processing1
create table dbo.#etl_post_processing1 with(distribution=hash(fact_mms_package_key), location=user_db) as
SELECT #dim_date_transaction_post.month_ending_date month_ending_date,
       #dim_employee_club.dim_club_key employee_dim_club_key,
	   #dim_club_employee.dim_club_id dim_club_id,
	   #dim_club_employee.club_close_dim_date_key club_close_dim_date_key,
	   #fact_mms_package.fact_mms_package_key fact_mms_package_key,
	   #fact_mms_package.primary_sales_dim_employee_key primary_sales_dim_employee_key,
	   #fact_mms_package.transaction_post_dim_date_key transaction_post_dim_date_key,
	   #fact_mms_package.reporting_local_currency_monthly_average_dim_exchange_rate_key reporting_local_currency_monthly_average_dim_exchange_rate_key,
	   #fact_mms_package.reporting_local_currency_dim_plan_exchange_rate_key reporting_local_currency_dim_plan_exchange_rate_key,
	   case when #fact_mms_package.primary_sales_dim_employee_key not in ('-997','-998','-999') and (#dim_date_transaction_post.month_ending_date is null or #dim_employee_club.dim_club_key is null or #dim_club_employee.dim_club_id is null )
	   then '-997'
	   when #fact_mms_package.primary_sales_dim_employee_key not in ('-997','-998','-999') and #dim_club_employee.dim_club_id<> 13
	   and (club_close_dim_date_key = '-998' or club_close_dim_date_key>= transaction_post_dim_date_key)
	   then #dim_employee_club.dim_club_key
	   when @corporate_dim_club_key is null then '-997'
	   else @corporate_dim_club_key end reporting_dim_club_key
FROM  #fact_mms_package
LEFT JOIN #dim_employee_club
on #fact_mms_package.fact_mms_package_key = #dim_employee_club.fact_mms_package_key
LEFT JOIN #dim_date_transaction_post
on #dim_date_transaction_post.fact_mms_package_key = #fact_mms_package.fact_mms_package_key
LEFT JOIN #dim_club_employee
on #fact_mms_package.fact_mms_package_key = #dim_club_employee.fact_mms_package_key


if object_id('tempdb..#etl_post_processing2') is not null drop table #etl_post_processing2
create table dbo.#etl_post_processing2 with(distribution=hash(fact_mms_package_key), location=user_db) as
SELECT #etl_post_processing1.fact_mms_package_key fact_mms_package_key,
       #etl_post_processing1.reporting_dim_club_key reporting_dim_club_key,
       dim_exchange_rate.effective_dim_date_key input_effective_dim_date_key,
       dim_exchange_rate.from_currency_code input_from_currency_code,
	   dim_plan_exchange_rate.from_currency_code input_from_currency_code_plan,
	   #etl_post_processing1.reporting_local_currency_monthly_average_dim_exchange_rate_key reporting_local_currency_monthly_average_dim_exchange_rate_key
FROM #etl_post_processing1
LEFT JOIN dim_exchange_rate
on #etl_post_processing1.reporting_local_currency_monthly_average_dim_exchange_rate_key = dim_exchange_rate.dim_exchange_rate_key
LEFT JOIN dim_plan_exchange_rate
on #etl_post_processing1.reporting_local_currency_dim_plan_exchange_rate_key = dim_plan_exchange_rate_key


if object_id('tempdb..#etl_post_processing3') is not null drop table #etl_post_processing3
create table dbo.#etl_post_processing3 with(distribution=hash(fact_mms_package_key), location=user_db) as
SELECT #etl_post_processing2.fact_mms_package_key fact_mms_package_key,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_post_processing2.input_effective_dim_date_key),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(#etl_post_processing2.input_from_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
									   reporting_local_currency_monthly_average_dim_exchange_rate_key,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_post_processing2.input_from_currency_code_plan,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
								       reporting_local_currency_dim_plan_exchange_rate_key
FROM #etl_post_processing2
join dim_club
on #etl_post_processing2.reporting_dim_club_key = dim_club.dim_club_key

 update fact_mms_package
 set reporting_local_currency_monthly_average_dim_exchange_rate_key =  #etl_post_processing3.reporting_local_currency_monthly_average_dim_exchange_rate_key,
 reporting_local_currency_dim_plan_exchange_rate_key = #etl_post_processing3.reporting_local_currency_dim_plan_exchange_rate_key
 from  #etl_post_processing3
 where fact_mms_package.fact_mms_package_key=#etl_post_processing3.fact_mms_package_key

 /*-UDW-7767/*Logic from proc_LTFDW_FactECommerceSalesTransaction_HybrisPackages is mimiced*/*/
 if object_id('tempdb..#LTFDW_FactECommerceSalesTransaction_HybrisPackages') is not null drop table #LTFDW_FactECommerceSalesTransaction_HybrisPackages
 create table dbo.#LTFDW_FactECommerceSalesTransaction_HybrisPackages with(distribution=hash(fact_mms_package_key), location=user_db) as
 SELECT fact_mms_package.fact_mms_package_key fact_mms_package_key,
        case when fact_mms_package.number_of_sessions = 0 then 0
		     else (fact_hybris_transaction_item.transaction_amount_gross / fact_mms_package.number_of_sessions) * fact_mms_package.sessions_left 
         end balance_amount,
        case when fact_mms_package.number_of_sessions = 0 then 0
		     else ((fact_hybris_transaction_item.transaction_amount_gross - fact_hybris_transaction_item.bucks_amount) / fact_mms_package.number_of_sessions) * fact_mms_package.sessions_left 
         end balance_amount_less_lt_bucks,
        case when fact_mms_package.number_of_sessions = 0 then 0
		     else fact_hybris_transaction_item.transaction_amount_gross / fact_mms_package.number_of_sessions 
         end price_per_session,
        case when fact_mms_package.number_of_sessions = 0 then 0
		     else (fact_hybris_transaction_item.transaction_amount_gross - fact_hybris_transaction_item.bucks_amount) / fact_mms_package.number_of_sessions 
         end price_per_session_less_lt_bucks,
        fact_hybris_transaction_item.bucks_amount,
        fact_hybris_transaction_item.discount_amount,
		'' primary_sales_dim_employee_key
      from fact_mms_package
      join fact_hybris_transaction_item
        on fact_mms_package.fact_mms_sales_transaction_key=fact_hybris_transaction_item.fact_mms_sales_transaction_key /*/* added as part of analysis UDW-7195*/*/
     Where fact_hybris_transaction_item.fact_hybris_transaction_item_key not in ('-998','-997','-999')
       and fact_mms_package.fact_mms_package_key not in ('-998','-997','-999')
	   and (fact_mms_package.dv_batch_id>=@load_dv_batch_id
            or fact_hybris_transaction_item.dv_batch_id >= @load_dv_batch_id)
       and fact_mms_package.price_per_session <> case when number_of_sessions = 0 then 0
                                                      else (fact_hybris_transaction_item.transaction_amount_gross / fact_mms_package.number_of_sessions) end
union
/*UDW-9465 Include Magento Transaction Item */
select fact_mms_package.fact_mms_package_key fact_mms_package_key,
       case when fact_mms_package.number_of_sessions = 0 then 0
            else fact_magento_order_item.per_package_amount / fact_mms_package.number_of_sessions * fact_mms_package.sessions_left
       end balance_amount,
       case when fact_mms_package.number_of_sessions = 0 then 0
            else (fact_magento_order_item.per_package_amount - fact_magento_order_item.per_package_bucks) / fact_mms_package.number_of_sessions * fact_mms_package.sessions_left
        end balance_amount_less_lt_bucks,
       case when fact_mms_package.number_of_sessions = 0 then 0
            else fact_magento_order_item.per_package_amount / fact_mms_package.number_of_sessions
        end price_per_session,
       case when fact_mms_package.number_of_sessions = 0 then 0
            else (fact_magento_order_item.per_package_amount - fact_magento_order_item.per_package_bucks) / fact_mms_package.number_of_sessions
        end price_per_session_less_lt_bucks,
        fact_magento_order_item.per_package_bucks item_lt_bucks_amount,
        fact_magento_order_item.per_package_discount sales_discount_amount,
		fact_magento_order_item.dim_employee_key primary_sales_dim_employee_key
  from fact_mms_package fact_mms_package
  join d_magento_lifetime_order_item_change_log
    on fact_mms_package.fact_mms_sales_transaction_key=d_magento_lifetime_order_item_change_log.fact_mms_transaction_key
  join (select dlog.fact_magento_order_item_key,
               oi.item_discount_amount / cast(count(*) as decimal(26,6)) per_package_discount,
               oi.item_total_amount / cast(count(*) as decimal(26,6)) per_package_amount,
               oi.item_bucks_amount / cast(count(*) as decimal(26,6)) per_package_bucks,
               oi.dv_batch_id,
			   oi.dim_employee_key     /* added for defect UDW-10771****/
          from d_magento_lifetime_order_item_change_log dlog
          join fact_magento_order_item oi on dlog.fact_magento_order_item_key = oi.fact_magento_order_item_key
         where dlog.transaction_type = 'mms transaction'
         group by dlog.fact_magento_order_item_key,
                  oi.item_total_amount, 
                  oi.item_discount_amount, 
                  oi.item_bucks_amount,
                  oi.dv_batch_id,
				  oi.dim_employee_key) fact_magento_order_item        /* added for defect UDW-10771****/
    on d_magento_lifetime_order_item_change_log.fact_magento_order_item_key = fact_magento_order_item.fact_magento_order_item_key
 where fact_mms_package.fact_mms_package_key not in ('-998','-997','-999')
   and fact_magento_order_item.fact_magento_order_item_key not in ('-998','-997','-999')
   and ( fact_mms_package.dv_batch_id>=@load_dv_batch_id
         or fact_magento_order_item.dv_batch_id >= @load_dv_batch_id
         or d_magento_lifetime_order_item_change_log.dv_batch_id >= @load_dv_batch_id)

 update fact_mms_package
    set balance_amount =  #LTFDW_FactECommerceSalesTransaction_HybrisPackages.balance_amount,
        balance_amount_less_lt_bucks =  #LTFDW_FactECommerceSalesTransaction_HybrisPackages.balance_amount_less_lt_bucks,
        price_per_session = #LTFDW_FactECommerceSalesTransaction_HybrisPackages.price_per_session,
        price_per_session_less_lt_bucks = #LTFDW_FactECommerceSalesTransaction_HybrisPackages.price_per_session_less_lt_bucks,
        item_lt_bucks_amount = #LTFDW_FactECommerceSalesTransaction_HybrisPackages.bucks_amount,
        sales_discount_amount = #LTFDW_FactECommerceSalesTransaction_HybrisPackages.discount_amount,
		primary_sales_dim_employee_key = #LTFDW_FactECommerceSalesTransaction_HybrisPackages.primary_sales_dim_employee_key /* added for defect UDW-10771****/
 from  #LTFDW_FactECommerceSalesTransaction_HybrisPackages
 where fact_mms_package.fact_mms_package_key=#LTFDW_FactECommerceSalesTransaction_HybrisPackages.fact_mms_package_key /*/* added as part of analysis UDW-7195**/*/

 end

