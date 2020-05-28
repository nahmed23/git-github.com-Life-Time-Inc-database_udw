CREATE PROC [dbo].[proc_fact_mms_gift_card_issuance] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_gift_card_issuance)
declare @current_dv_batch_id bigint = -1
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_gift_card_issuance_key), location=user_db) as  
select 
d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash fact_mms_gift_card_issuance_key,
d_mms_tran_item_gift_card_issuance.tran_item_gift_card_issuance_id,
d_mms_tran_item_gift_card_issuance.tran_item_id,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.dim_mms_drawer_activity_key,'-998')
   end dim_mms_drawer_activity_key,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.dim_mms_member_key,'-998')
   end dim_mms_member_key,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.dim_mms_product_key,'-998')
   end dim_mms_product_key,
isnull(fact_mms_sales_transaction_item.original_currency_code,'USD') original_currency_code,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.post_dim_date_key,'-998')
   end post_dim_date_key,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.post_dim_time_key,'-998')
   end post_dim_time_key,
fact_mms_sales_transaction_item.receipt_comment,
isnull(fact_mms_sales_transaction_item.sales_dollar_amount,0) as sales_dollar_amount,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.sales_entered_dim_employee_key,'-998')
   end sales_entered_dim_employee_key,
isnull(fact_mms_sales_transaction_item.sales_quantity,0) as sales_quantity,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.transaction_reporting_dim_club_key,'-998')
   end transaction_reporting_dim_club_key,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,'-998')
   end transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key,'-998')
   end  transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.usd_dim_plan_exchange_rate_key,'-998')
   end  usd_dim_plan_exchange_rate_key,
case when d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash in ('-997','-998','-999') 
     then d_mms_tran_item_gift_card_issuance.d_mms_tran_item_gift_card_issuance_bk_hash
    else isnull(fact_mms_sales_transaction_item.usd_monthly_average_dim_exchange_rate_key,'-998')
   end  usd_monthly_average_dim_exchange_rate_key,
isnull(fact_mms_sales_transaction_item.voided_flag,'N') as voided_flag,
d_mms_tran_item_gift_card_issuance.issuance_amount,
isnull(fact_mms_sales_transaction_item.dv_load_date_time,'Jan 1, 1753') as dv_load_date_time,
isnull(fact_mms_sales_transaction_item.dv_batch_id,'-1') as dv_batch_id 
from 
d_mms_tran_item_gift_card_issuance
left join fact_mms_sales_transaction_item 
on d_mms_tran_item_gift_card_issuance.tran_item_id = fact_mms_sales_transaction_item.tran_item_id
 where d_mms_tran_item_gift_card_issuance.dv_batch_id >= @load_dv_batch_id

begin tran

  delete dbo.fact_mms_gift_card_issuance
   where fact_mms_gift_card_issuance_key in (select fact_mms_gift_card_issuance_key from dbo.#etl_step_1) 

insert into fact_mms_gift_card_issuance
	(   
     fact_mms_gift_card_issuance_key,
     tran_item_gift_card_issuance_id,
     tran_item_id,
     dim_mms_drawer_activity_key,
     dim_mms_member_key,
     dim_mms_product_key,
     original_currency_code,
     post_dim_date_key,
     post_dim_time_key,
     sales_transaction_receipt_comment,
     sales_transaction_item_amount,
     sales_entered_dim_employee_key,
     sales_transaction_item_quantity,
     transaction_reporting_dim_club_key,
     transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
     transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key,
     usd_dim_plan_exchange_rate_key,
     usd_monthly_average_dim_exchange_rate_key,
     voided_flag,
     issuance_amount,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
	)
   
   select
     fact_mms_gift_card_issuance_key,
     tran_item_gift_card_issuance_id,
     tran_item_id,
     dim_mms_drawer_activity_key,
     dim_mms_member_key,
     dim_mms_product_key,
     original_currency_code,
     post_dim_date_key,
     post_dim_time_key,
     receipt_comment,
     sales_dollar_amount,
     sales_entered_dim_employee_key,
     sales_quantity,
     transaction_reporting_dim_club_key,
     transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
     transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key,
     usd_dim_plan_exchange_rate_key,
     usd_monthly_average_dim_exchange_rate_key,
     voided_flag,
     issuance_amount,
     dv_load_date_time,
     'dec 31, 9999',
     dv_batch_id,
     getdate(),
     suser_sname()
 from #etl_step_1
	 
commit tran

if object_id('tempdb..#etl_step_addtional_processing') is not null drop table #etl_step_addtional_processing
create table dbo.#etl_step_addtional_processing with(distribution=hash(fact_mms_gift_card_issuance_key), location=user_db) as  
select fact_mms_gift_card_issuance.fact_mms_gift_card_issuance_key,
                 factsalestransaction.transaction_reporting_dim_club_key,
                 factsalestransaction.voided_flag,
                 factsalestransaction.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
                 factsalestransaction.transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key
    from fact_mms_gift_card_issuance 
      join fact_mms_sales_transaction_item factsalestransaction
         on factsalestransaction.tran_item_id = fact_mms_gift_card_issuance.tran_item_id
 where fact_mms_gift_card_issuance.dv_batch_id = @load_dv_batch_id

update fact_mms_gift_card_issuance set
 transaction_reporting_dim_club_key = #etl_step_addtional_processing.transaction_reporting_dim_club_key,
 voided_flag = #etl_step_addtional_processing.voided_flag,
 transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key = #etl_step_addtional_processing.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
 transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key = #etl_step_addtional_processing.transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key
    FROM #etl_step_addtional_processing 
 WHERE #etl_step_addtional_processing.fact_mms_gift_card_issuance_key = fact_mms_gift_card_issuance.fact_mms_gift_card_issuance_key

end
