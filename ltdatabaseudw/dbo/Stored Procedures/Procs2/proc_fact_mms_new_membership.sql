CREATE PROC [dbo].[proc_fact_mms_new_membership] @dv_batch_id [varchar](500),@begin_extract_date_time [datetime] AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_new_membership)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


-- #corporate_membership
if object_id('tempdb..#corporate_membership') is not null drop table #corporate_membership
create table dbo.#corporate_membership with(distribution=hash(dim_mms_membership_key), location=user_db) as
SELECT DISTINCT
       dim_mms_membership_history.dim_mms_membership_key dim_mms_membership_key,
       dim_mms_membership_history.membership_id membership_id,
       dim_mms_membership_history.corporate_membership_flag corporate_membership_flag
  FROM dim_mms_membership_history
  JOIN dim_date
    ON dim_mms_membership_history.membership_created_dim_date_key = dim_date.dim_date_key
 WHERE dim_mms_membership_history.membership_created_dim_date_key IN
       (SELECT DISTINCT dim_date_key
          FROM dim_date
         WHERE calendar_date >= CONVERT(DATETIME,CONVERT(VARCHAR,DATEPART(yyyy, @begin_extract_date_time)) + '-' + CONVERT(VARCHAR,DATEPART(mm, @begin_extract_date_time)) + '-01', 101)
           )
   AND dim_mms_membership_history.effective_date_time <= dim_date.month_ending_date
   AND dim_mms_membership_history.corporate_membership_flag = 'Y'


---Main query---
if object_id('tempdb..#SQ1') is not null drop table #SQ1
create table dbo.#SQ1 with(distribution=hash(dim_mms_membership_key), location=user_db) as
SELECT dim_mms_membership.dim_mms_membership_key dim_mms_membership_key,
       dim_mms_membership.created_date_time_key created_date_time_key,
       dim_mms_membership.home_dim_club_key home_dim_club_key,
       dim_mms_membership.original_sales_dim_team_member_key original_sales_dim_employee_key,  --its renamed in dim_mms_membership as original_sales_dim_team_member_key, confirm
       dim_mms_membership.membership_id membership_id,
        ---DimCustomer.DimCustomerSCDKey, --- we do not bring SCD columns
       d_mms_member.dim_mms_member_key dim_mms_member_key,
	   dim_mms_membership_type.attribute_dssr_group_description attribute_dssr_group_description,
       ----MembershipDimProduct.MembershipTypeDSSRGroupDescription,   ---no DSSR column available in dim_mms_product
       dim_mms_membership_type.dim_mms_membership_type_key dim_mms_membership_type_key,
       #corporate_membership.corporate_membership_flag corporate_membership_flag,
	   dim_mms_membership.dv_batch_id dv_batch_id,
	   dim_mms_membership.dv_load_date_time dv_load_date_time
  FROM dim_mms_membership
  LEFT JOIN #corporate_membership
    ON dim_mms_membership.membership_id = #corporate_membership.membership_id
  JOIN d_mms_member d_mms_member
    ON dim_mms_membership.dim_mms_membership_key = d_mms_member.dim_mms_membership_Key ---- Primary_dim_member_key is not available in dim_mms_membership
  JOIN dim_description member_type_dim_description
  ON d_mms_member.member_type_dim_description_key = member_type_dim_description.dim_description_key
  JOIN dim_mms_membership_type
   on dim_mms_membership_type.dim_mms_membership_type_key = dim_mms_membership.dim_mms_membership_type_key
  WHERE (member_type_dim_description.description = 'Primary'
   AND dim_mms_membership.created_date_time_key IN
       (SELECT DISTINCT dim_date_key
          FROM dim_date
         WHERE calendar_date >	= CONVERT(DATETIME,CONVERT(VARCHAR,DATEPART(yyyy, @begin_extract_date_time)) + '-' + CONVERT(VARCHAR,DATEPART(mm, @begin_extract_date_time)) + '-01', 101)
          ))
		  OR dim_mms_membership.dim_mms_membership_key in ('-997','-998','-999')
--MembershipDimProductKey is dim_mms_membership_type_key.  Anything related should be in dim_mms_membership_type.
 -- If you need to expose the dim_mms_product_key, it�s in dim_mms_membership_type.  If you don�t actually need anything from dim_mms_product
 -- , you can probably just alias dim_mms_membership_type_key as dim_mms_product_key since they�re always the same.
 -- Not sure if that�s actually enforced by MMS though, so probably better to do the lookup just in case.
--select * from dim_mms_membership_type where dim_mms_membership_type_key <> dim_mms_product_key


-------SQ 2---
if object_id('tempdb..#SQ2') is not null drop table #SQ2
create table dbo.#SQ2 with(distribution=hash(dim_mms_membership_key), location=user_db) as
SELECT dim_mms_membership.dim_mms_membership_key dim_mms_membership_key,
       dim_mms_membership.membership_id  membership_id,
       fact_mms_sales_transaction_item.sales_quantity sales_quantity,
       fact_mms_sales_transaction_item.sales_dollar_amount sales_dollar_amount,
       fact_mms_sales_transaction_item.refund_flag refund_flag,
       fact_mms_sales_transaction_item.voided_flag voided_flag,
       fact_mms_sales_transaction_item.transaction_edited_flag transaction_edited_flag,
       fact_mms_sales_transaction_item.reversal_flag reversal_flag,
       fact_mms_sales_transaction_item.original_currency_code original_currency_code,
	   MembershipCreatedDimDate.month_starting_dim_date_key membership_created_month_starting_dim_date_key,
       SalesTransactionDimDate.month_starting_dim_date_key sales_transaction_month_starting_dim_date_key
  FROM dim_mms_membership
  JOIN dim_date MembershipCreatedDimDate
    ON dim_mms_membership.created_date_time_key = MembershipCreatedDimDate.dim_date_key
 -- Get all members on the membership - the primary may have been switched during the first month
  JOIN d_mms_member d_mms_member
    ON dim_mms_membership.dim_mms_membership_key = d_mms_member.dim_mms_membership_key
 -- Get the enrollment fee sales transactions from the beginning to the end of the month in which each membership was created
  LEFT JOIN fact_mms_sales_transaction_item
    ON d_mms_member.dim_mms_member_key = fact_mms_sales_transaction_item.dim_mms_member_key
  LEFT JOIN dim_mms_product SalesTransactionDimProduct
    ON fact_mms_sales_transaction_item.dim_mms_product_key = SalesTransactionDimProduct.dim_mms_product_key
  LEFT JOIN dim_date SalesTransactionDimDate
    ON fact_mms_sales_transaction_item.post_dim_date_key = SalesTransactionDimDate.dim_date_key
 WHERE
    (SalesTransactionDimProduct.product_id IN (88, 286, 3132)
   AND dim_mms_membership.created_date_time_key IN
       (SELECT DISTINCT dim_date_key
          FROM dim_date
         WHERE calendar_date >= CONVERT(DATETIME,CONVERT(VARCHAR,DATEPART(yyyy, @begin_extract_date_time)) + '-' + CONVERT(VARCHAR,DATEPART(mm, @begin_extract_date_time)) + '-01', 101))
   AND fact_mms_sales_transaction_item.post_dim_date_key IN
       (SELECT DISTINCT dim_date_key
          FROM dim_date
         WHERE calendar_date >= CONVERT(DATETIME,CONVERT(VARCHAR,DATEPART(yyyy, @begin_extract_date_time)) + '-' + CONVERT(VARCHAR,DATEPART(mm, @begin_extract_date_time)) + '-01', 101)
        ))OR dim_mms_membership.dim_mms_membership_key in ('-997','-998','-999')


if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(dim_mms_membership_key), location=user_db) as
SELECT #SQ2.dim_mms_membership_key dim_mms_membership_key,
       #SQ2.membership_id membership_id,
       #SQ2.sales_quantity sales_quantity,
       #SQ2.sales_dollar_amount sales_dollar_amount,
       #SQ2.refund_flag refund_flag,
       #SQ2.voided_flag voided_flag,
       #SQ2.transaction_edited_flag transaction_edited_flag,
	   #SQ2.reversal_flag reversal_flag,
       #SQ2.original_currency_code input_original_currency_code,
	   #SQ2.membership_created_month_starting_dim_date_key membership_created_month_starting_dim_date_key,
	   #SQ2.sales_transaction_month_starting_dim_date_key sales_transaction_month_starting_dim_date_key
from #SQ2
where refund_flag='N'



if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(dim_mms_membership_key), location=user_db) as
SELECT #etl_step_1.dim_mms_membership_key dim_mms_membership_key,
       SUM(#etl_step_1.sales_quantity * #etl_step_1.sales_dollar_amount) total_ef,
       MIN(#etl_step_1.input_original_currency_code) original_currency_code
from #etl_step_1
group by dim_mms_membership_key


if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(dim_mms_membership_key), location=user_db) as
SELECT #SQ1.dim_mms_membership_key dim_mms_membership_key,
       #SQ1.membership_id membership_id,
       #SQ1.created_date_time_key created_date_time_key,
       #SQ1.home_dim_club_key home_dim_club_key,
       #SQ1.original_sales_dim_employee_key original_sales_dim_employee_key,  --its renamed in dim_mms_membership as original_sales_dim_team_member_key, confirm
       ---DimCustomer.DimCustomerSCDKey, --- we do not bring SCD columns
       #SQ1.dim_mms_member_key dim_mms_member_key,
	   #SQ1.attribute_dssr_group_description attribute_dssr_group_description,
       ----MembershipDimProduct.MembershipTypeDSSRGroupDescription,   ---no DSSR column available in dim_mms_product
       #SQ1.dim_mms_membership_type_key dim_mms_membership_type_key,
       #SQ1.corporate_membership_flag corporate_membership_flag,
	   #SQ1.dv_batch_id dv_batch_id,
	   #SQ1.dv_load_date_time dv_load_date_time,
	   #etl_step_2.total_ef total_ef,
	   ISNULL(#etl_step_2.original_currency_code,'USD') original_currency_code
       from #SQ1
	   join #etl_step_2
	   ON #etl_step_2.dim_mms_membership_key=#SQ1.dim_mms_membership_key



if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(dim_mms_membership_key), location=user_db) as
SELECT #etl_step_3.dim_mms_membership_key dim_mms_membership_key,
	   #etl_step_3.membership_id membership_id,
       #etl_step_3.created_date_time_key created_date_time_key,
	   #etl_step_3.home_dim_club_key home_dim_club_key,
	   #etl_step_3.original_sales_dim_employee_key primary_sales_dim_employee_key,
	   #etl_step_3.dim_mms_member_key dim_mms_member_key,
	   #etl_step_3.attribute_dssr_group_description attribute_dssr_group_description,
	   #etl_step_3.dim_mms_membership_type_key dim_mms_membership_type_key,
	   #etl_step_3.original_currency_code original_currency_code,
	   isnull(#etl_step_3.total_ef,0)  enrollment_fee,
	   case when #etl_step_3.corporate_membership_flag = 'Y' then 'Y' else 'N' end corporate_membership_flag,
	   case when #etl_step_3.attribute_dssr_group_description <> 'DSSR_Other' then 'Y' else 'N' end include_in_dssr_flag,
	   dim_date.year calendar_year,
	   case when #etl_step_3.dim_mms_membership_key in ('-997','-998','-999') then #etl_step_3.dim_mms_membership_key
       when dim_date.month_ending_dim_date_key in ('-997', '-998', '-999') then #etl_step_3.dim_mms_membership_key
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,dim_date.month_ending_dim_date_key),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(#etl_step_3.original_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                         'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
       end usd_monthly_average_dim_exchange_rate_key,
	    case when #etl_step_3.dim_mms_membership_key in ('-997','-998','-999') then #etl_step_3.dim_mms_membership_key
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_3.original_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
       end usd_dim_plan_exchange_rate_key,
	   case when #etl_step_3.dim_mms_membership_key in ('-997','-998','-999') then #etl_step_3.dim_mms_membership_key
       when dim_date.month_ending_dim_date_key in ('-997', '-998', '-999') then #etl_step_3.dim_mms_membership_key
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,dim_date.month_ending_dim_date_key),'z#@$k%&P')+
                                     'P%#&z$@k'+isnull(#etl_step_3.original_currency_code,'z#@$k%&P')+
                                     'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                     'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
	   end local_currency_monthly_average_dim_exchange_rate_key,
	   case when #etl_step_3.dim_mms_membership_key in ('-997','-998','-999') then #etl_step_3.dim_mms_membership_key
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_3.original_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
	   end local_currency_dim_plan_exchange_rate_key,
	   #etl_step_3.dv_batch_id dv_batch_id,
	   #etl_step_3.dv_load_date_time dv_load_date_time
INTO #etl_step_4
FROM #etl_step_3
LEFT JOIN dim_date
on #etl_step_3.created_date_time_key = dim_date.dim_date_key
LEFT JOIN dim_club
on #etl_step_3.home_dim_club_key= dim_club.dim_club_key




 -- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.fact_mms_new_membership
   where fact_mms_new_membership_key in (select dim_mms_membership_key from dbo.#etl_step_4)

   insert into fact_mms_new_membership
           (fact_mms_new_membership_key,
			membership_id,
			created_date_time_key,
			home_dim_club_key,
			primary_sales_dim_employee_key,
			dim_mms_member_key,
			enrollment_fee,
			corporate_membership_flag,
			include_in_dssr_flag,
			dim_mms_membership_type_key,
			original_currency_code,
			usd_monthly_average_dim_exchange_rate_key,
			usd_dim_plan_exchange_rate_key,
			local_currency_monthly_average_dim_exchange_rate_key,
			local_currency_dim_plan_exchange_rate_key,
			dv_load_date_time,
            dv_load_end_date_time,
            dv_batch_id,
            dv_inserted_date_time,
            dv_insert_user)

	select  dim_mms_membership_key,
			membership_id,
			created_date_time_key,
			home_dim_club_key,
			primary_sales_dim_employee_key,
			dim_mms_member_key,
			enrollment_fee,
			corporate_membership_flag,
			include_in_dssr_flag,
			dim_mms_membership_type_key,
			original_currency_code,
			usd_monthly_average_dim_exchange_rate_key,
			usd_dim_plan_exchange_rate_key,
			local_currency_monthly_average_dim_exchange_rate_key,
			local_currency_dim_plan_exchange_rate_key,
			dv_load_date_time,
            'dec 31, 9999',
            dv_batch_id,
            getdate() ,
            suser_sname()
			from #etl_step_4




commit tran
end
