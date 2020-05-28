CREATE PROC [dbo].[gary_proc_fact_mms_drawer_activity_amount] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

--declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_cafe_profit_center)
--declare @current_dv_batch_id bigint = @dv_batch_id
--declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

declare @load_dv_batch_id bigint = 20180203080157

-- Create the base data using only the driving tables (d_mms_drawer_activity_amount and d_mms_drawer_activity)
-- This avoids needing to replicate calculations from data that comes out of non-driving tables
-- Note that calculation are generally not performed at this step since code would need to be duplicated
-- Note that inner joins are done instead of left joins.  This is more efficient and should be used whenever possible.
-- Note that when the data is large joining on a distribution column is the best approach
if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(fact_mms_drawer_activity_amount_key)) as
select d_mms_drawer_activity_amount.fact_mms_drawer_activity_amount_key,
       d_mms_drawer_activity_amount.drawer_activity_amount_id,
       d_mms_drawer_activity_amount.payment_type_dim_description_key,
       d_mms_drawer_activity_amount.transaction_total_amount,
       d_mms_drawer_activity_amount.actual_total_amount,
       d_mms_drawer_activity_amount.dim_mms_drawer_activity_key,
       d_mms_drawer_activity.closed_business_dim_date_key,
       d_mms_drawer_activity.closed_flag drawer_activity_closed_flag,
       d_mms_drawer_activity.d_mms_drawer_bk_hash,
       d_mms_drawer_activity_amount.r_mms_val_currency_code_bk_hash,
       d_mms_drawer_activity_amount.dv_load_date_time d_mms_drawer_activity_amount_dv_load_date_time,
       d_mms_drawer_activity_amount.dv_batch_id d_mms_drawer_activity_amount_dv_batch_id,
       d_mms_drawer_activity.dv_load_date_time d_mms_drawer_activity_dv_load_date_time,
       d_mms_drawer_activity.dv_batch_id d_mms_drawer_activity_dv_batch_id
  from dbo.gary_d_mms_drawer_activity_amount d_mms_drawer_activity_amount
  join dbo.gary_d_mms_drawer_activity d_mms_drawer_activity
    on d_mms_drawer_activity_amount.dim_mms_drawer_activity_key = d_mms_drawer_activity.dim_mms_drawer_activity_key
 where d_mms_drawer_activity_amount.dv_batch_id >= @load_dv_batch_id
union
select d_mms_drawer_activity_amount.fact_mms_drawer_activity_amount_key,
       d_mms_drawer_activity_amount.drawer_activity_amount_id,
       d_mms_drawer_activity_amount.payment_type_dim_description_key,
       d_mms_drawer_activity_amount.transaction_total_amount,
       d_mms_drawer_activity_amount.actual_total_amount,
       d_mms_drawer_activity_amount.dim_mms_drawer_activity_key,
       d_mms_drawer_activity.closed_business_dim_date_key,
       d_mms_drawer_activity.closed_flag drawer_activity_closed_flag,
       d_mms_drawer_activity.d_mms_drawer_bk_hash,
       d_mms_drawer_activity_amount.r_mms_val_currency_code_bk_hash,
       d_mms_drawer_activity_amount.dv_load_date_time d_mms_drawer_activity_amount_dv_load_date_time,
       d_mms_drawer_activity_amount.dv_batch_id d_mms_drawer_activity_amount_dv_batch_id,
       d_mms_drawer_activity.dv_load_date_time d_mms_drawer_activity_dv_load_date_time,
       d_mms_drawer_activity.dv_batch_id d_mms_drawer_activity_dv_batch_id
  from dbo.gary_d_mms_drawer_activity d_mms_drawer_activity
  join dbo.gary_d_mms_drawer_activity_amount d_mms_drawer_activity_amount
    on d_mms_drawer_activity.dim_mms_drawer_activity_key = d_mms_drawer_activity_amount.dim_mms_drawer_activity_key
 where d_mms_drawer_activity.dv_batch_id >= @load_dv_batch_id

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
-- Note that the following is slower due to the more complex where clause
--select ...
--  from dbo.gary_d_mms_drawer_activity_amount d_mms_drawer_activity_amount
--  join dbo.gary_d_mms_drawer_activity d_mms_drawer_activity
--    on d_mms_drawer_activity_amount.dim_mms_drawer_activity_key = d_mms_drawer_activity.dim_mms_drawer_activity_key
-- where d_mms_drawer_activity_amount.dv_batch_id >= @load_dv_batch_id
--    or d_mms_drawer_activity.dv_batch_id >= @load_dv_batch_id
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------

-- Note that there are multiple steps beyond the driving tables
-- A best practice is to
--   1) Get the data from the driving tables
--      Name the temp table #etl_step1
--   2) Get the data from the next logical set of non-driving tables and perform and calculations
--      that can be done at this point.  The idea is to break complex calculations into multiple steps.
--      Name the temp table #etl_step2 (the same name as on the 
--   3) continue with the next logical set of tables/calculations

if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution=hash(fact_mms_drawer_activity_amount_key)) as
select 
       #etl_step1.fact_mms_drawer_activity_amount_key,
       #etl_step1.drawer_activity_amount_id,
       #etl_step1.payment_type_dim_description_key,
       d_mms_drawer.dim_club_key,
       #etl_step1.transaction_total_amount,
       #etl_step1.actual_total_amount,
       r_mms_val_currency_code.currency_code original_currency_code,
       #etl_step1.dim_mms_drawer_activity_key,
       dim_date.month_ending_dim_date_key closed_business_month_ending_dim_date_key,
       #etl_step1.drawer_activity_closed_flag,
       dim_club.local_currency_code,
       dim_club.club_type_description,
       case when #etl_step1.d_mms_drawer_activity_amount_dv_load_date_time >= isnull(#etl_step1.d_mms_drawer_activity_dv_load_date_time,'jan 1, 1753')
            then #etl_step1.d_mms_drawer_activity_amount_dv_load_date_time
            else #etl_step1.d_mms_drawer_activity_dv_load_date_time
        end dv_load_date_time,
       'dec 31, 9999' dv_load_end_date_time,
       case when #etl_step1.d_mms_drawer_activity_amount_dv_batch_id >= isnull(#etl_step1.d_mms_drawer_activity_dv_batch_id,-1)
            then #etl_step1.d_mms_drawer_activity_amount_dv_batch_id
            else #etl_step1.d_mms_drawer_activity_dv_batch_id
        end dv_batch_id
  from #etl_step1
  join dbo.gary_d_mms_drawer d_mms_drawer
    on #etl_step1.d_mms_drawer_bk_hash = d_mms_drawer.bk_hash
  join dbo.gary_dim_club dim_club
    on d_mms_drawer.dim_club_key = dim_club.dim_club_key
  join dbo.r_mms_val_currency_code
    on #etl_step1.r_mms_val_currency_code_bk_hash = r_mms_val_currency_code.bk_hash
   and r_mms_val_currency_code.dv_load_end_date_time = 'dec 31, 9999'
  join dbo.dim_date
    on #etl_step1.closed_business_dim_date_key = dim_date.dim_date_key

if object_id('tempdb..#etl_step3') is not null drop table #etl_step3
create table dbo.#etl_step3 with(distribution=hash(fact_mms_drawer_activity_amount_key)) as
select 
       #etl_step2.fact_mms_drawer_activity_amount_key,
       #etl_step2.drawer_activity_amount_id,
       #etl_step2.payment_type_dim_description_key,
       #etl_step2.dim_club_key,
       --dim_merchant_number_key
       #etl_step2.transaction_total_amount,
       #etl_step2.actual_total_amount,
       #etl_step2.original_currency_code,
       case when #etl_step2.fact_mms_drawer_activity_amount_key in ('-997', '-998', '-999') then #etl_step2.fact_mms_drawer_activity_amount_key
            when #etl_step2.drawer_activity_closed_flag = 'N' then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step2.closed_business_month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(#etl_step2.original_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
        end usd_monthly_average_dim_exchange_rate_key,
       case when #etl_step2.fact_mms_drawer_activity_amount_key in ('-997', '-998', '-999') then #etl_step2.fact_mms_drawer_activity_amount_key
            when #etl_step2.drawer_activity_closed_flag = 'N' then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step2.original_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
        end usd_dim_plan_exchange_rate_key,
       case when #etl_step2.fact_mms_drawer_activity_amount_key in ('-997', '-998', '-999') then #etl_step2.fact_mms_drawer_activity_amount_key
            when #etl_step2.drawer_activity_closed_flag = 'N' then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step2.closed_business_month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(#etl_step2.original_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(#etl_step2.local_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
        end local_currency_monthly_average_dim_exchange_rate_key,
       case when #etl_step2.fact_mms_drawer_activity_amount_key in ('-997', '-998', '-999') then #etl_step2.fact_mms_drawer_activity_amount_key
            when #etl_step2.drawer_activity_closed_flag = 'N' then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step2.original_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(#etl_step2.local_currency_code,'z#@$k%&P'))),2)
        end local_currency_dim_plan_exchange_rate_key,
       #etl_step2.dim_mms_drawer_activity_key,
       -- dim_club_currency_code_key
       #etl_step2.dv_load_date_time,
       #etl_step2.dv_load_end_date_time,
       #etl_step2.dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from #etl_step2

/*
-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.fact_mms_drawer_activity_amount
   where fact_mms_drawer_activity_amount_key in (select fact_mms_drawer_activity_amount_key from dbo.#etl_step3) 

  insert into dbo.fact_mms_drawer_activity_amount
    (fact_mms_drawer_activity_amount_key,
     .
     .
     .
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
     )
  select fact_mms_drawer_activity_amount_key,
         .
         .
         .
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user
    from #etl_step3

commit tran
*/

end
