CREATE PROC [dbo].[proc_fact_mms_member_spend] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_member_spend)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(dim_mms_member_key), location=user_db) as
select dim_mms_member_key,
	   dim_mms_membership_key,
	   max(dv_load_date_time) dv_load_date_time,
	   max(dv_load_end_date_time) dv_load_end_date_time,
	   max(dv_batch_id) dv_batch_id
from fact_mms_sales_transaction_item
where dv_batch_id >= @load_dv_batch_id
  and (membership_charge_flag='Y' or pos_flag='Y') /*valtrantypeid IN(1,3)*/
group by dim_mms_member_key,
	   dim_mms_membership_key


if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution=hash(dim_mms_member_key), location=user_db) as
select  etl_step1.dim_mms_member_key,
        etl_step1.dim_mms_membership_key,
        sum (case when post_dim_date_key >=CONVERT(char(8), dateadd(year, - 1, getdate()), 112)
                  then sales_amount_gross
		     else 0 end) last_12_month_spend_amount,
        sum(sales_amount_gross) as total_spend_amount,
		etl_step1.dv_load_date_time,
	    etl_step1.dv_load_end_date_time,
	    etl_step1.dv_batch_id
from dbo.#etl_step1 etl_step1
join fact_mms_sales_transaction_item on etl_step1.dim_mms_member_key=fact_mms_sales_transaction_item.dim_mms_member_key
where membership_charge_flag='Y' or pos_flag='Y'
group by etl_step1.dim_mms_member_key,
	     etl_step1.dim_mms_membership_key,
	     etl_step1.dv_load_date_time,
	     etl_step1.dv_load_end_date_time,
	     etl_step1.dv_batch_id


begin tran

  delete from dbo.fact_mms_member_spend
   where dim_mms_member_key in (select dim_mms_member_key from dbo.#etl_step2)

   insert into fact_mms_member_spend
        (dim_mms_member_key,
         dim_mms_membership_key,
   	     last_12_month_spend_amount,
	     total_spend_amount,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user)
  select dim_mms_member_key,
         dim_mms_membership_key,
         last_12_month_spend_amount,
	     total_spend_amount,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate() ,
         suser_sname()
    from #etl_step2

commit tran

end

