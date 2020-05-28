CREATE PROC [dbo].[proc_fact_cafe_sales_transaction_item] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_cafe_sales_transaction_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_cafe_sales_transaction_item_key), location=user_db) as 
select d_ig_it_trn_order_item.fact_cafe_sales_transaction_item_key,
       d_ig_it_trn_order_item.order_hdr_id,
       d_ig_it_trn_order_item.check_seq,
       d_ig_it_trn_order_header.check_number,
       d_ig_it_trn_order_header.order_close_dim_date_key,
       d_ig_it_trn_order_header.order_close_dim_time_key,
       d_ig_it_trn_order_header.order_close_month_ending_dim_date_key,
       d_ig_it_trn_order_header.order_discount_amount,
       d_ig_it_trn_order_header.order_open_dim_date_key,
       d_ig_it_trn_order_header.order_open_dim_time_key,
       d_ig_it_trn_order_header.order_refund_flag,
       d_ig_it_trn_order_header.order_sales_amount_gross,
       d_ig_it_trn_order_header.order_tax_amount,
       d_ig_it_trn_order_header.order_service_charge_amount,
       d_ig_it_trn_order_header.order_tax_removed_flag,
       d_ig_it_trn_order_header.order_tip_amount,
       d_ig_it_trn_order_header.order_commissionable_dim_employee_key,
       d_ig_it_trn_order_header.dim_cafe_profit_center_key,
       d_ig_it_trn_order_header.order_void_flag,
       d_ig_it_trn_order_header.order_closed_flag,
       d_ig_it_trn_order_item.item_discount_amount,
       d_ig_it_trn_order_item.item_quantity,
       d_ig_it_trn_order_item.dim_cafe_product_key,
       d_ig_it_trn_order_item.item_sales_amount_gross,
       d_ig_it_trn_order_item.item_sales_dollar_amount_excluding_tax,
       d_ig_it_trn_order_item.item_tax_amount,
       d_ig_it_trn_order_item.item_voided_flag,
       d_ig_it_trn_order_item.item_refund_flag,
	   d_ig_it_trn_order_item.dim_cafe_discount_coupon_key,
       d_ig_it_trn_business_day_dates.business_day_start_dim_date_key  posted_business_start_dim_date_key,
       d_ig_it_trn_business_day_dates.business_day_end_dim_date_key  posted_business_end_dim_date_key,
       dim_club.dim_club_key,
       dim_club.local_currency_code as original_currency_code,
       case when d_ig_it_trn_order_item.fact_cafe_sales_transaction_item_key in ('-997', '-998', '-999') then d_ig_it_trn_order_item.fact_cafe_sales_transaction_item_key
            when d_ig_it_trn_order_header.order_close_month_ending_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,d_ig_it_trn_order_header.order_close_month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(dim_club.local_currency_code,'USD'),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2) end usd_monthly_average_dim_exchange_rate_key,
       case when d_ig_it_trn_order_item.fact_cafe_sales_transaction_item_key in ('-997', '-998', '-999') then d_ig_it_trn_order_item.fact_cafe_sales_transaction_item_key
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2) end usd_dim_plan_exchange_rate,
	   convert(varchar,getdate(), 112) udw_inserted_dim_date_key,
       --isnull(d_mms_pt_credit_card_transaction.dim_mms_member_key, '-998')  dim_mms_member_key,
	   case when d_ig_it_trn_order_header.dv_load_date_time >= isnull(d_ig_it_trn_order_item.dv_load_date_time,'Jan 1, 1753') then d_ig_it_trn_order_header.dv_load_date_time
            else d_ig_it_trn_order_item.dv_load_date_time end dv_load_date_time,
	   'dec 31, 9999'  dv_load_end_date_time,
	   case when d_ig_it_trn_order_header.dv_batch_id >= isnull(d_ig_it_trn_order_item.dv_batch_id,-1) then d_ig_it_trn_order_header.dv_batch_id
            else d_ig_it_trn_order_item.dv_batch_id end dv_batch_id,
	   getdate()  dv_inserted_date_time,
	   suser_sname()  dv_insert_user,
       case when d_ig_it_trn_order_header.bk_hash in ('-997','-998','-999') then 'Dec 31, 9999'
            else dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,transaction_close_dim_date.calendar_date)),0)) end allocated_recalculate_through_datetime,
       case when d_ig_it_trn_order_header.bk_hash in ('-997','-998','-999') then 99991231
            else convert(varchar(8),dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,transaction_close_dim_date.calendar_date)),0)),112) end allocated_recalculate_through_dim_date_key,
       convert(varchar(8),DATEADD(mm,DATEDIFF(mm,0,transaction_close_dim_date.calendar_date),0),112) allocated_month_starting_dim_date_key,
       d_ig_it_trn_order_header.bk_hash header_bk_hash
from d_ig_it_trn_order_header
join d_ig_it_trn_order_item
  on d_ig_it_trn_order_header.bk_hash = d_ig_it_trn_order_item.d_ig_it_trn_order_header_bk_hash
join d_ig_it_trn_business_day_dates
  on d_ig_it_trn_order_header.d_ig_it_trn_business_day_dates_bk_hash = d_ig_it_trn_business_day_dates.bk_hash
join d_ig_it_cfg_profit_center_master
  on d_ig_it_trn_order_header.profit_center_id = d_ig_it_cfg_profit_center_master.profit_center_id
 and d_ig_it_cfg_profit_center_master.store_id not in (2, 45)
join dim_club
  on d_ig_it_cfg_profit_center_master.store_id = dim_club.info_genesis_store_id
  join dim_date transaction_close_dim_date
    on d_ig_it_trn_order_header.order_close_dim_date_key = transaction_close_dim_date.dim_date_key
where d_ig_it_trn_order_header.dv_batch_id >= @load_dv_batch_id
   or d_ig_it_trn_order_item.dv_batch_id >= @load_dv_batch_id

if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(header_bk_hash), location=user_db) as 
select header_bk_hash,
       dim_mms_member_key,
       row_number() over (partition by header_bk_hash order by tender_amount desc, d_mms_pt_credit_card_transaction_id) r
from (
    select #etl_step_1.header_bk_hash,
           d_mms_pt_credit_card_transaction.dim_mms_member_key,
           sum(tender_amount) tender_amount,
           min(d_mms_pt_credit_card_transaction_id) d_mms_pt_credit_card_transaction_id
    from #etl_step_1
    join d_ig_it_trn_order_tender
      on #etl_step_1.header_bk_hash = d_ig_it_trn_order_tender.d_ig_it_trn_order_header_bk_hash
     and d_ig_it_trn_order_tender.tender_id = 24 /* Club Tab*/
    join d_mms_pt_credit_card_transaction
      on d_ig_it_trn_order_tender.d_mms_pt_credit_card_transaction_bk_hash = d_mms_pt_credit_card_transaction.bk_hash
     and d_mms_pt_credit_card_transaction.bk_hash not in ('-999','-998','-997')
    group by #etl_step_1.header_bk_hash,
             d_mms_pt_credit_card_transaction.dim_mms_member_key
) a

delete from #etl_step_2 where r > 1
   	
begin tran

  delete dbo.fact_cafe_sales_transaction_item
   where fact_cafe_sales_transaction_item_key in (select fact_cafe_sales_transaction_item_key from dbo.#etl_step_1) 
    
   insert into fact_cafe_sales_transaction_item
   (fact_cafe_sales_transaction_item_key,
   order_hdr_id,
   check_seq,
   check_number,
   order_close_dim_date_key,
   order_close_dim_time_key,
   order_close_month_ending_dim_date_key,
   order_discount_amount,
   order_open_dim_date_key,
   order_open_dim_time_key,
   order_refund_flag,
   order_sales_amount_gross,
   order_tax_amount,
   order_service_charge_amount,
   order_tax_removed_flag,
   order_tip_amount,
   order_commissionable_dim_employee_key,
   dim_cafe_profit_center_key,
   order_void_flag,
   order_closed_flag,
   item_discount_amount,
   item_quantity,
   dim_cafe_product_key,
   item_sales_amount_gross,
   item_sales_dollar_amount_excluding_tax,
   item_tax_amount,
   item_voided_flag,
   item_refund_flag,
   dim_cafe_discount_coupon_key,
   posted_business_start_dim_date_key,
   posted_business_end_dim_date_key,
   dim_club_key,
   original_currency_code,
   usd_monthly_average_dim_exchange_rate_key,
   usd_dim_plan_exchange_rate,
   udw_inserted_dim_date_key,
   dim_mms_member_key,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user,
   allocated_recalculate_through_datetime,
   allocated_recalculate_through_dim_date_key,
   allocated_month_starting_dim_date_key)
select #etl_step_1.fact_cafe_sales_transaction_item_key,
       #etl_step_1.order_hdr_id,
       #etl_step_1.check_seq,
       #etl_step_1.check_number,
       #etl_step_1.order_close_dim_date_key,
       #etl_step_1.order_close_dim_time_key,
       #etl_step_1.order_close_month_ending_dim_date_key,
       #etl_step_1.order_discount_amount,
       #etl_step_1.order_open_dim_date_key,
       #etl_step_1.order_open_dim_time_key,
       #etl_step_1.order_refund_flag,
       #etl_step_1.order_sales_amount_gross,
       #etl_step_1.order_tax_amount,
       #etl_step_1.order_service_charge_amount,
       #etl_step_1.order_tax_removed_flag,
       #etl_step_1.order_tip_amount,
       #etl_step_1.order_commissionable_dim_employee_key,
       #etl_step_1.dim_cafe_profit_center_key,
       #etl_step_1.order_void_flag,
       #etl_step_1.order_closed_flag,
       #etl_step_1.item_discount_amount,
       #etl_step_1.item_quantity,
       #etl_step_1.dim_cafe_product_key,
       #etl_step_1.item_sales_amount_gross,
       #etl_step_1.item_sales_dollar_amount_excluding_tax,
       #etl_step_1.item_tax_amount,
       #etl_step_1.item_voided_flag,
       #etl_step_1.item_refund_flag,
       #etl_step_1.dim_cafe_discount_coupon_key,
       #etl_step_1.posted_business_start_dim_date_key,
       #etl_step_1.posted_business_end_dim_date_key,
       #etl_step_1.dim_club_key,
       #etl_step_1.original_currency_code,
       #etl_step_1.usd_monthly_average_dim_exchange_rate_key,
       #etl_step_1.usd_dim_plan_exchange_rate,
       #etl_step_1.udw_inserted_dim_date_key,
       isnull(#etl_step_2.dim_mms_member_key,'-998') dim_mms_member_key,
       #etl_step_1.dv_load_date_time,
       #etl_step_1.dv_load_end_date_time,
       #etl_step_1.dv_batch_id,
       #etl_step_1.dv_inserted_date_time,
       #etl_step_1.dv_insert_user,
       #etl_step_1.allocated_recalculate_through_datetime,
       #etl_step_1.allocated_recalculate_through_dim_date_key,
       #etl_step_1.allocated_month_starting_dim_date_key
  from #etl_step_1
  left join #etl_step_2 on #etl_step_1.header_bk_hash = #etl_step_2.header_bk_hash
   
 commit tran	

end

