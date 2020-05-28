CREATE PROC [dbo].[proc_fact_mms_sales_transaction_item_automated_refund] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_sales_transaction_item_automated_refund)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_sales_transaction_item_automated_refund_key), location=user_db) as
 select tran_item_refund.bk_hash fact_mms_sales_transaction_item_automated_refund_key,
        tran_item_refund.tran_item_refund_id ,
        tran_item_refund.tran_item_id tran_item_id,
        tran_item_refund.original_tran_item_id original_tran_item_id,
		tran_item_refund.fact_mms_sales_transaction_item_key fact_mms_sales_transaction_item_key,
        tran_item_refund.dv_batch_id,
        tran_item_refund.dv_load_date_time
 from d_mms_tran_item_refund tran_item_refund 
 where exists
   (select sales_transaction_item.fact_mms_sales_transaction_item_key
  from  fact_mms_sales_transaction_item sales_transaction_item
  where sales_transaction_item.fact_mms_sales_transaction_item_key = tran_item_refund.fact_mms_sales_transaction_item_key
  and sales_transaction_item.dv_batch_id >= @load_dv_batch_id
  and tran_item_id is not null)
 
 union
 
 select d_mms_tran_item_refund.bk_hash fact_mms_sales_transaction_item_automated_refund_key ,
        d_mms_tran_item_refund.tran_item_refund_id tran_item_refund_id,
        d_mms_tran_item_refund.tran_item_id tran_item_id,
        d_mms_tran_item_refund.original_tran_item_id original_tran_item_id,
		d_mms_tran_item_refund.fact_mms_sales_transaction_item_key  fact_mms_sales_transaction_item_key,
        d_mms_tran_item_refund.dv_batch_id,
        d_mms_tran_item_refund.dv_load_date_time
 from   d_mms_tran_item_refund
 where  d_mms_tran_item_refund.dv_batch_id >= @load_dv_batch_id
 
if object_id('tempdb..#etl_step_2a') is not null drop table #etl_step_2a
create table dbo.#etl_step_2a with(distribution=hash(refund_fact_mms_sales_transaction_item_key), location=user_db) as
select #etl_step_1.fact_mms_sales_transaction_item_automated_refund_key fact_mms_sales_transaction_item_automated_refund_key,
       #etl_step_1.tran_item_refund_id tran_item_refund_id,
       fact_mms_sales_transaction_item_refund.mms_tran_id refund_mms_tran_id,      /*/*This is MMSTranID in fact_mms_sales_transaction_item */*/
       #etl_step_1.tran_item_id refund_tran_item_id,
       #etl_step_1.original_tran_item_id original_tran_item_id,
       fact_mms_sales_transaction_item_refund.fact_mms_sales_transaction_item_key refund_fact_mms_sales_transaction_item_key,
       convert(char(8),getdate(),112) step1_udw_inserted_dim_date_key,
       fact_mms_sales_transaction_item_refund.post_dim_date_key refund_post_dim_date_key,
       fact_mms_sales_transaction_item_refund.dim_club_key refund_dim_club_key,
       fact_mms_sales_transaction_item_refund.dim_mms_product_key refund_dim_mms_product_key,
       fact_mms_sales_transaction_item_refund.dim_mms_member_key refund_dim_mms_member_key,
       fact_mms_sales_transaction_item_refund.dim_mms_transaction_reason_key refund_dim_mms_transaction_reason_key,
       fact_mms_sales_transaction_item_refund.sales_quantity refund_quantity,
       fact_mms_sales_transaction_item_refund.sales_dollar_amount refund_dollar_amount,   /*-This is mentioned twice in SDS*/
       fact_mms_sales_transaction_item_refund.voided_flag refund_voided_flag,
       fact_mms_sales_transaction_item_refund.sales_entered_dim_employee_key refund_entered_dim_employee_key,
       fact_mms_sales_transaction_item_refund.sales_discount_dollar_amount refund_discount_dollar_amount,
       fact_mms_sales_transaction_item_refund.post_dim_time_key refund_post_dim_time_key, 
       fact_mms_sales_transaction_item_refund.original_currency_code refund_original_currency_code,
       fact_mms_sales_transaction_item_refund.usd_monthly_average_dim_exchange_rate_key refund_usd_monthly_average_dim_exchange_rate_key,
       fact_mms_sales_transaction_item_refund.usd_dim_plan_exchange_rate_key refund_usd_dim_plan_exchange_rate_key,
       fact_mms_sales_transaction_item_refund.dim_mms_drawer_activity_key refund_dim_mms_drawer_activity_key,
       fact_mms_sales_transaction_item_refund.void_dim_date_key refund_void_dim_date_key,
       fact_mms_sales_transaction_item_refund.void_dim_time_key refund_void_dim_time_key,
       fact_mms_sales_transaction_item_refund.void_dim_employee_key refund_void_dim_employee_key,
       fact_mms_sales_transaction_item_refund.dim_mms_reimbursement_program_key refund_dim_mms_reimbursement_program_key ,
       fact_mms_sales_transaction_item_refund.void_comment refund_void_comment,
       fact_mms_sales_transaction_item_refund.voided_flag refund_void_flag,
       fact_mms_sales_transaction_item_refund.receipt_number refund_receipt_number,
       fact_mms_sales_transaction_item_refund.receipt_comment refund_receipt_comment,
       fact_mms_sales_transaction_item_refund.sales_tax_amount refund_tax_amount,
	   fact_mms_sales_transaction_item_refund.item_lt_bucks_sales_tax item_lt_bucks_sales_tax,  /*--added UDW-9085*/
       /*/*convert(decimal,fact_mms_sales_transaction_item_refund.sales_discount_dollar_amount)+convert(decimal,fact_mms_sales_transaction_item_refund.sales_dollar_amount) refund_amount_gross, */*/
       /*/*changes for JIRA story UDW-8751------*/*/
	   case  
		when fact_mms_sales_transaction_item_refund.refund_flag='Y' and 
	   (fact_mms_sales_transaction_item_refund.sales_discount_dollar_amount + fact_mms_sales_transaction_item_refund.sales_dollar_amount) > 0 
	      then abs(fact_mms_sales_transaction_item_refund.sales_discount_dollar_amount + fact_mms_sales_transaction_item_refund.sales_dollar_amount) * -1 
		    else (fact_mms_sales_transaction_item_refund.sales_discount_dollar_amount + fact_mms_sales_transaction_item_refund.sales_dollar_amount) end refund_amount_gross, 
       #etl_step_1.dv_batch_id dv_batch_id,
       #etl_step_1.dv_load_date_time dv_load_date_time,
       'dec 31, 9999' dv_load_end_date_time 
       from #etl_step_1
join fact_mms_sales_transaction_item fact_mms_sales_transaction_item_refund
on #etl_step_1.tran_item_id = fact_mms_sales_transaction_item_refund.tran_item_id
and #etl_step_1.fact_mms_sales_transaction_item_key = fact_mms_sales_transaction_item_refund.fact_mms_sales_transaction_item_key


if object_id('tempdb..#etl_step_2b') is not null drop table #etl_step_2b
create table dbo.#etl_step_2b with(distribution=hash(fact_mms_sales_transaction_item_automated_refund_key), location=user_db) as
select #etl_step_1.fact_mms_sales_transaction_item_automated_refund_key fact_mms_sales_transaction_item_automated_refund_key,
      fact_mms_sales_transaction_item_original.mms_tran_id original_mms_tran_id,
      fact_mms_sales_transaction_item_original.fact_mms_sales_transaction_item_key original_fact_sales_transaction_item_key,
      fact_mms_sales_transaction_item_original.primary_sales_dim_employee_key original_primary_sales_dim_employee_key,
      fact_mms_sales_transaction_item_original.secondary_sales_dim_employee_key original_secondary_sales_dim_employee_key,  
      fact_mms_sales_transaction_item_original.udw_inserted_dim_date_key original_fact_sales_transaction_edw_inserted_dim_date_key,
      fact_mms_sales_transaction_item_original.dim_club_key original_dim_club_key,
      fact_mms_sales_transaction_item_original.post_dim_date_key original_post_dim_date_key,
      fact_mms_sales_transaction_item_original.sales_channel_dim_description_key original_sales_channel_dim_description_key,
      fact_mms_sales_transaction_item_original.transaction_reporting_dim_club_key original_transaction_reporting_dim_club_key,
      fact_mms_sales_transaction_item_original.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
      fact_mms_sales_transaction_item_original.transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key original_transaction_reporting_local_currency_dim_plan_exchange_rate_key,
      fact_mms_sales_transaction_item_original.dim_mms_reimbursement_program_key original_dim_mms_reimbursement_program_key,
      fact_mms_sales_transaction_item_original.sold_not_serviced_flag original_sold_not_serviced_flag
 from #etl_step_1
join fact_mms_sales_transaction_item fact_mms_sales_transaction_item_original
on #etl_step_1.original_tran_item_id = fact_mms_sales_transaction_item_original.tran_item_id


if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(fact_mms_sales_transaction_item_automated_refund_key), location=user_db) as
select #etl_step_2a.fact_mms_sales_transaction_item_automated_refund_key fact_mms_sales_transaction_item_automated_refund_key,
      #etl_step_2a.tran_item_refund_id tran_item_refund_id,
      #etl_step_2a.refund_mms_tran_id refund_mms_tran_id,
      #etl_step_2a.refund_tran_item_id refund_tran_item_id,
      #etl_step_2b.original_mms_tran_id original_mms_tran_id,
      #etl_step_2a.original_tran_item_id original_tran_item_id,
      #etl_step_2a.refund_fact_mms_sales_transaction_item_key refund_fact_mms_sales_transaction_item_key,
      #etl_step_2b.original_fact_sales_transaction_item_key original_fact_sales_transaction_item_key,
      #etl_step_2b.original_primary_sales_dim_employee_key original_primary_sales_dim_employee_key,
      #etl_step_2b.original_secondary_sales_dim_employee_key original_secondary_sales_dim_employee_key,
      isnull(fact_mms_sales_transaction_item_automated_refund.udw_inserted_dim_date_key,#etl_step_2a.step1_udw_inserted_dim_date_key) udw_inserted_dim_date_key,
      #etl_step_2a.refund_post_dim_date_key refund_post_dim_date_key,
      #etl_step_2a.refund_dim_club_key refund_dim_club_key,
      #etl_step_2a.refund_dim_mms_product_key refund_dim_mms_product_key,
      #etl_step_2a.refund_dim_mms_member_key refund_dim_mms_member_key,
      #etl_step_2a.refund_dim_mms_transaction_reason_key refund_dim_mms_transaction_reason_key,
      #etl_step_2a.refund_quantity refund_quantity,
      #etl_step_2a.refund_dollar_amount refund_dollar_amount,
      #etl_step_2a.refund_voided_flag refund_voided_flag,
      #etl_step_2a.refund_entered_dim_employee_key refund_entered_dim_employee_key,
	  #etl_step_2a.refund_discount_dollar_amount refund_discount_dollar_amount,
      #etl_step_2b.original_fact_sales_transaction_edw_inserted_dim_date_key original_fact_sales_transaction_edw_inserted_dim_date_key,
      #etl_step_2a.refund_post_dim_time_key refund_post_dim_time_key,
      #etl_step_2a.refund_original_currency_code refund_original_currency_code,
      #etl_step_2a.refund_usd_monthly_average_dim_exchange_rate_key refund_usd_monthly_average_dim_exchange_rate_key,
      #etl_step_2a.refund_usd_dim_plan_exchange_rate_key refund_usd_dim_plan_exchange_rate_key,
      #etl_step_2b.original_dim_club_key original_dim_club_key,
      #etl_step_2b.original_post_dim_date_key original_post_dim_date_key,
      #etl_step_2b.original_sales_channel_dim_description_key original_sales_channel_dim_description_key,
      #etl_step_2b.original_transaction_reporting_dim_club_key original_transaction_reporting_dim_club_key,
      #etl_step_2b.original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
      #etl_step_2b.original_transaction_reporting_local_currency_dim_plan_exchange_rate_key original_transaction_reporting_local_currency_dim_plan_exchange_rate_key,
      #etl_step_2a.refund_dim_mms_drawer_activity_key refund_dim_mms_drawer_activity_key,
      #etl_step_2a.refund_void_dim_date_key refund_void_dim_date_key,
      #etl_step_2a.refund_void_dim_time_key refund_void_dim_time_key,
      #etl_step_2a.refund_void_dim_employee_key refund_void_dim_employee_key,
      #etl_step_2a.refund_dim_mms_reimbursement_program_key refund_dim_mms_reimbursement_program_key,
      #etl_step_2b.original_dim_mms_reimbursement_program_key original_dim_mms_reimbursement_program_key,
	  #etl_step_2b.original_sold_not_serviced_flag original_sold_not_serviced_flag,
      #etl_step_2a.refund_void_comment refund_void_comment,
      #etl_step_2a.refund_void_flag refund_void_flag,
      #etl_step_2a.refund_receipt_number refund_receipt_number,
      #etl_step_2a.refund_receipt_comment refund_receipt_comment,
      #etl_step_2a.refund_tax_amount refund_tax_amount,
	  #etl_step_2a.item_lt_bucks_sales_tax item_lt_bucks_sales_tax,
      #etl_step_2a.refund_amount_gross refund_amount_gross,
      #etl_step_2a.dv_batch_id dv_batch_id,
      #etl_step_2a.dv_load_date_time dv_load_date_time,
      #etl_step_2a.dv_load_end_date_time dv_load_end_date_time
 from #etl_step_2a
 join #etl_step_2b
 on #etl_step_2a.fact_mms_sales_transaction_item_automated_refund_key = #etl_step_2b.fact_mms_sales_transaction_item_automated_refund_key
 left join fact_mms_sales_transaction_item_automated_refund
 on #etl_step_2a.fact_mms_sales_transaction_item_automated_refund_key = fact_mms_sales_transaction_item_automated_refund.fact_mms_sales_transaction_item_automated_refund_key

/* Delete and re-insert as a single transaction*/
/* Delete records from the table that exist*/
/* Insert records from records from current and missing batches*/

  begin tran

  delete dbo.fact_mms_sales_transaction_item_automated_refund
   where fact_mms_sales_transaction_item_automated_refund_key in (select fact_mms_sales_transaction_item_automated_refund_key from dbo.#etl_step_3) 

   
   insert into  fact_mms_sales_transaction_item_automated_refund
          (fact_mms_sales_transaction_item_automated_refund_key 
          ,tran_item_refund_id 
          ,refund_mms_tran_id 
          ,refund_tran_item_id 
          ,original_mms_tran_id 
          ,original_tran_item_id 
          ,refund_fact_mms_sales_transaction_item_key
          ,original_fact_sales_transaction_item_key
          ,original_primary_sales_dim_employee_key
          ,original_secondary_sales_dim_employee_key
    	  ,udw_inserted_dim_date_key
    	  ,refund_post_dim_date_key 
          ,refund_dim_club_key 
          ,refund_dim_mms_product_key 
          ,refund_dim_mms_member_key 
          ,refund_dim_mms_transaction_reason_key 
          ,refund_quantity 
          ,refund_dollar_amount 
          ,refund_voided_flag 
          ,refund_entered_dim_employee_key 
		  ,refund_discount_dollar_amount
		  ,original_fact_sales_transaction_edw_inserted_dim_date_key 
          ,refund_post_dim_time_key 
          ,refund_original_currency_code 
          ,refund_usd_monthly_average_dim_exchange_rate_key 
          ,refund_usd_dim_plan_exchange_rate_key 
          ,original_dim_club_key 
          ,original_post_dim_date_key 
          ,original_sales_channel_dim_description_key 
          ,original_transaction_reporting_dim_club_key 
          ,original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key
          ,original_transaction_reporting_local_currency_dim_plan_exchange_rate_key 
          ,refund_dim_mms_drawer_activity_key 
          ,refund_void_dim_date_key 
           ,refund_void_dim_time_key 
           ,refund_void_dim_employee_key 
           ,refund_dim_mms_reimbursement_program_key 
           ,original_dim_mms_reimbursement_program_key 
		   ,original_sold_not_serviced_flag
           ,refund_void_comment 
           ,refund_void_flag 
           ,refund_receipt_number 
           ,refund_receipt_comment 
           ,refund_tax_amount
           ,item_lt_bucks_sales_tax		   
           ,refund_amount_gross 
           ,dv_batch_id 
           ,dv_load_date_time 
           ,dv_load_end_date_time 
           ,dv_inserted_date_time
           ,dv_insert_user)

		  select fact_mms_sales_transaction_item_automated_refund_key 
      ,tran_item_refund_id 
      ,refund_mms_tran_id 
      ,refund_tran_item_id 
      ,original_mms_tran_id 
      ,original_tran_item_id 
      ,refund_fact_mms_sales_transaction_item_key
      ,original_fact_sales_transaction_item_key
      ,original_primary_sales_dim_employee_key
      ,original_secondary_sales_dim_employee_key
      ,udw_inserted_dim_date_key
      ,refund_post_dim_date_key 
      ,refund_dim_club_key 
      ,refund_dim_mms_product_key 
      ,refund_dim_mms_member_key 
      ,refund_dim_mms_transaction_reason_key 
      ,refund_quantity 
      ,refund_dollar_amount 
      ,refund_voided_flag 
      ,refund_entered_dim_employee_key 
	  ,refund_discount_dollar_amount
      ,original_fact_sales_transaction_edw_inserted_dim_date_key 
      ,refund_post_dim_time_key 
      ,refund_original_currency_code 
      ,refund_usd_monthly_average_dim_exchange_rate_key 
      ,refund_usd_dim_plan_exchange_rate_key 
      ,original_dim_club_key 
      ,original_post_dim_date_key 
      ,original_sales_channel_dim_description_key 
      ,original_transaction_reporting_dim_club_key 
      ,original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key
      ,original_transaction_reporting_local_currency_dim_plan_exchange_rate_key 
      ,refund_dim_mms_drawer_activity_key 
      ,refund_void_dim_date_key 
      ,refund_void_dim_time_key 
      ,refund_void_dim_employee_key 
      ,refund_dim_mms_reimbursement_program_key 
      ,original_dim_mms_reimbursement_program_key 
	  ,original_sold_not_serviced_flag
      ,refund_void_comment 
      ,refund_void_flag 
      ,refund_receipt_number 
      ,refund_receipt_comment 
      ,refund_tax_amount
      ,item_lt_bucks_sales_tax	  
      ,refund_amount_gross 
      ,dv_batch_id 
      ,dv_load_date_time 
      ,dv_load_end_date_time 
      ,getdate() 
      ,suser_sname()
    from #etl_step_3
  
    commit tran
 
		 
declare @club_13_dim_club_key char(32)

  /* Find the dim_club_key for club_id 13*/
select @club_13_dim_club_key = dim_club_key
  from dim_club
 where club_id = 13

if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(fact_mms_sales_transaction_item_key), location=user_db) as
select fact_mms_sales_transaction_item.fact_mms_sales_transaction_item_key fact_mms_sales_transaction_item_key,
       fact_mms_sales_transaction_item.transaction_reporting_dim_club_key transaction_reporting_dim_club_key
       from fact_mms_sales_transaction_item
where fact_mms_sales_transaction_item.dv_batch_id >= @load_dv_batch_id
and fact_mms_sales_transaction_item.dim_club_key = @club_13_dim_club_key
and fact_mms_sales_transaction_item.automated_refund_flag = 'Y'
 
 
if object_id('tempdb..#etl_step_5') is not null drop table #etl_step_5
create table dbo.#etl_step_5 with(distribution=hash(fact_mms_sales_transaction_item_automated_refund_key), location=user_db) as
select fact_mms_sales_transaction_item_automated_refund.fact_mms_sales_transaction_item_automated_refund_key fact_mms_sales_transaction_item_automated_refund_key,
       fact_mms_sales_transaction_item_automated_refund.original_transaction_reporting_dim_club_key original_transaction_reporting_dim_club_key,
       fact_mms_sales_transaction_item_automated_refund.original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
       fact_mms_sales_transaction_item_automated_refund.original_transaction_reporting_local_currency_dim_plan_exchange_rate_key original_transaction_reporting_local_currency_dim_plan_exchange_rate_key
    from  fact_mms_sales_transaction_item_automated_refund
    join #etl_step_4
    on fact_mms_sales_transaction_item_automated_refund.refund_fact_mms_sales_transaction_item_key = #etl_step_4.fact_mms_sales_transaction_item_key
   where fact_mms_sales_transaction_item_automated_refund.original_transaction_reporting_dim_club_key <> #etl_step_4.transaction_reporting_dim_club_key


      
 update fact_mms_sales_transaction_item_automated_refund
 set original_transaction_reporting_dim_club_key =  #etl_step_5.original_transaction_reporting_dim_club_key,
 original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key = #etl_step_5.original_transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
 original_transaction_reporting_local_currency_dim_plan_exchange_rate_key=#etl_step_5.original_transaction_reporting_local_currency_dim_plan_exchange_rate_key
 from  #etl_step_5
 where fact_mms_sales_transaction_item_automated_refund.fact_mms_sales_transaction_item_automated_refund_key=#etl_step_5.fact_mms_sales_transaction_item_automated_refund_key

end





