CREATE PROC [dbo].[proc_fact_mms_membership_balance] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_membership_balance)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(dim_mms_membership_key)) as
select 
     membership_balance_snapshot.dim_mms_membership_key,
     membership_balance_snapshot.membership_balance_id,
     membership_balance_snapshot.membership_id,
     dim_club.local_currency_code original_currency_code,
     membership_balance_snapshot.end_of_day_current_balance,
     membership_balance_snapshot.end_of_day_statement_balance,
     membership_balance_snapshot.end_of_day_committed_balance,
     membership_balance_snapshot.committed_balance_products,
     membership_balance_snapshot.current_balance_products,
     membership_balance_snapshot.processing_complete_flag,
     case when membership_balance_snapshot.dim_mms_membership_key in ('-997', '-998', '-999') then membership_balance_snapshot.dim_mms_membership_key
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,dim_date.month_ending_dim_date_key),'z#@$k%&P')+
                                                'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                                'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
     end usd_monthly_average_dim_exchange_rate_key,
     case when membership_balance_snapshot.dim_mms_membership_key in ('-997', '-998', '-999') then membership_balance_snapshot.dim_mms_membership_key
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                                'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
     end usd_dim_plan_exchange_rate_key,
     membership_balance_snapshot.dv_load_date_time dv_load_date_time,
     'dec 31, 9999' dv_load_end_date_time,
     membership_balance_snapshot.dv_batch_id dv_batch_id, 
     getdate() dv_inserted_date_time,
     suser_sname() dv_insert_user
from d_mms_membership_balance_snapshot membership_balance_snapshot
      join d_mms_membership membership
     on membership_balance_snapshot.dim_mms_membership_key = membership.dim_mms_membership_key
     and (membership_balance_snapshot.end_of_day_current_balance != 0 OR  
          membership_balance_snapshot.end_of_day_statement_balance != 0 OR  
          membership_balance_snapshot.End_Of_Day_Committed_Balance != 0 OR  
          membership_balance_snapshot.committed_balance_products != 0 OR   
          membership_balance_snapshot.current_balance_products != 0)
      join dbo.dim_club dim_club
     on membership.home_dim_club_key = dim_club.dim_club_key
     left join dbo.dim_date
     on  convert(varchar(20),getdate(),112)=dim_date.dim_date_key
     where 
     membership_balance_snapshot.dv_batch_id >= @load_dv_batch_id


-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.fact_mms_membership_balance
   where dim_mms_membership_key in (select dim_mms_membership_key from dbo.#etl_step1) 

  insert into dbo.fact_mms_membership_balance
    (dim_mms_membership_key,
     membership_balance_id,
     end_of_day_committed_balance,
     end_of_day_current_balance,
     end_of_day_statement_balance,
     committed_balance_products,
     current_balance_products,
     processing_complete_flag,
     membership_id,
     original_currency_code,
     usd_dim_plan_exchange_rate_key,
     usd_monthly_average_dim_exchange_rate_key,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user)
     select 
     dim_mms_membership_key,
     membership_balance_id,
     end_of_day_committed_balance,
     end_of_day_current_balance,
     end_of_day_statement_balance,
     committed_balance_products,
     current_balance_products,
     processing_complete_flag,
     membership_id,
     original_currency_code,
     usd_dim_plan_exchange_rate_key,
     usd_monthly_average_dim_exchange_rate_key,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
     from #etl_step1
     commit tran


end
