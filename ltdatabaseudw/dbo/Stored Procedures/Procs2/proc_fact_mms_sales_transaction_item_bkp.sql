CREATE PROC [dbo].[proc_fact_mms_sales_transaction_item_bkp] @dv_batch_id [varchar](500),@job_group [varchar](500),@begin_extract_date_time [datetime] AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_sales_transaction_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


 /* tran_items from current batch*/
if object_id('tempdb..#tran_item_current_batch') is not null drop table #tran_item_current_batch
create table dbo.#tran_item_current_batch with(distribution=hash(fact_mms_sales_transaction_key), location=user_db) as
select d_mms_tran_item.bk_hash,
       d_mms_tran_item.fact_mms_sales_transaction_item_key,
       d_mms_tran_item.club_id,
       d_mms_tran_item.dim_club_key,
       d_mms_tran_item.dim_mms_product_key,
       d_mms_tran_item.fact_mms_sales_transaction_key,
       d_mms_tran_item.sales_amount_gross,
       d_mms_tran_item.sales_discount_dollar_amount,
       d_mms_tran_item.sales_dollar_amount,
       d_mms_tran_item.sales_quantity,
       d_mms_tran_item.sales_tax_amount,
       d_mms_tran_item.sold_not_serviced_flag,
       d_mms_tran_item.tran_item_id,
	   d_mms_tran_item.transaction_source,
	   d_mms_tran_item.external_item_id,
	   d_mms_tran_item.item_lt_bucks_amount,
	   d_mms_tran_item.inserted_date_time,
	   d_mms_tran_item.updated_date_time,
	   d_mms_tran_item.inserted_dim_date_key,
	   d_mms_tran_item.inserted_dim_time_key,
	   d_mms_tran_item.updated_dim_date_key,
	   d_mms_tran_item.updated_dim_time_key,
       d_mms_tran_item.dv_batch_id,
       d_mms_tran_item.dv_load_date_time
  from d_mms_tran_item d_mms_tran_item
 where d_mms_tran_item.dv_batch_id >= @load_dv_batch_id

 /* mms_tran from current batch*/
if object_id('tempdb..#mms_tran_current_batch') is not null drop table #mms_tran_current_batch
create table dbo.#mms_tran_current_batch with(distribution=hash(bk_hash), location=user_db) as
select d_mms_mms_tran.bk_hash,
       d_mms_mms_tran.fact_mms_sales_transaction_key,
       d_mms_mms_tran.club_id,
       d_mms_mms_tran.dim_club_key,
       d_mms_mms_tran.dim_mms_drawer_activity_key,
       d_mms_mms_tran.dim_mms_member_key,
       d_mms_mms_tran.dim_mms_membership_key,
       d_mms_mms_tran.dim_mms_reimbursement_program_key,
       d_mms_mms_tran.dim_mms_transaction_reason_key,
       d_mms_mms_tran.employee_id,
       d_mms_mms_tran.membership_adjustment_flag,
       d_mms_mms_tran.membership_charge_flag,
       d_mms_mms_tran.membership_id,
       d_mms_mms_tran.mms_tran_id,
       d_mms_mms_tran.mms_tran_voided_bk_hash,
       d_mms_mms_tran.month_ending_post_dim_date_key,
       d_mms_mms_tran.original_currency_code,
       d_mms_mms_tran.original_fact_mms_sales_transaction_key,
       d_mms_mms_tran.original_mms_tran_id,
       d_mms_mms_tran.pos_flag,
       d_mms_mms_tran.post_dim_date_key,
       d_mms_mms_tran.post_dim_time_key,
       d_mms_mms_tran.receipt_comment,
       d_mms_mms_tran.receipt_number,
       d_mms_mms_tran.refund_flag,
       d_mms_mms_tran.reversal_flag,
       d_mms_mms_tran.sales_entered_dim_employee_key,
       d_mms_mms_tran.transaction_edited_flag,
       d_mms_mms_tran.voided_flag,
       d_mms_mms_tran.dv_batch_id,
       d_mms_mms_tran.dv_load_date_time
  from d_mms_mms_tran d_mms_mms_tran
 where d_mms_mms_tran.dv_batch_id >= @load_dv_batch_id

 /* tran_items related to #mms_tran_current_batch*/
if object_id('tempdb..#tran_item_from_mms_tran_current_batch') is not null drop table #tran_item_from_mms_tran_current_batch
create table dbo.#tran_item_from_mms_tran_current_batch with(distribution=hash(bk_hash), location=user_db) as
select d_mms_tran_item.bk_hash,
       d_mms_tran_item.fact_mms_sales_transaction_item_key,
       d_mms_tran_item.club_id,
       d_mms_tran_item.dim_club_key,
       d_mms_tran_item.dim_mms_product_key,
       d_mms_tran_item.fact_mms_sales_transaction_key,
       d_mms_tran_item.sales_amount_gross,
       d_mms_tran_item.sales_discount_dollar_amount,
       d_mms_tran_item.sales_dollar_amount,
       d_mms_tran_item.sales_quantity,
       d_mms_tran_item.sales_tax_amount,
       d_mms_tran_item.sold_not_serviced_flag,
       d_mms_tran_item.tran_item_id,
	   d_mms_tran_item.transaction_source,
	   d_mms_tran_item.external_item_id,
	   d_mms_tran_item.item_lt_bucks_amount,
	   d_mms_tran_item.inserted_date_time,
	   d_mms_tran_item.updated_date_time,
	   d_mms_tran_item.inserted_dim_date_key,
	   d_mms_tran_item.inserted_dim_time_key,
	   d_mms_tran_item.updated_dim_date_key,
	   d_mms_tran_item.updated_dim_time_key,
       d_mms_tran_item.dv_batch_id,
       d_mms_tran_item.dv_load_date_time
  from d_mms_tran_item d_mms_tran_item
 where d_mms_tran_item.fact_mms_sales_transaction_key in (select bk_hash from #mms_tran_current_batch)

 /* mms_trans related to #tran_item_current_batch*/
if object_id('tempdb..#mms_tran_from_tran_item_current_batch') is not null drop table #mms_tran_from_tran_item_current_batch
create table dbo.#mms_tran_from_tran_item_current_batch with(distribution=hash(bk_hash), location=user_db) as
select d_mms_mms_tran.bk_hash,
       d_mms_mms_tran.fact_mms_sales_transaction_key,
       d_mms_mms_tran.club_id,
       d_mms_mms_tran.dim_club_key,
       d_mms_mms_tran.dim_mms_drawer_activity_key,
       d_mms_mms_tran.dim_mms_member_key,
       d_mms_mms_tran.dim_mms_membership_key,
       d_mms_mms_tran.dim_mms_reimbursement_program_key,
       d_mms_mms_tran.dim_mms_transaction_reason_key,
       d_mms_mms_tran.employee_id,
       d_mms_mms_tran.membership_adjustment_flag,
       d_mms_mms_tran.membership_charge_flag,
       d_mms_mms_tran.membership_id,
       d_mms_mms_tran.mms_tran_id,
       d_mms_mms_tran.mms_tran_voided_bk_hash,
       d_mms_mms_tran.month_ending_post_dim_date_key,
       d_mms_mms_tran.original_currency_code,
       d_mms_mms_tran.original_fact_mms_sales_transaction_key,
       d_mms_mms_tran.original_mms_tran_id,
       d_mms_mms_tran.pos_flag,
       d_mms_mms_tran.post_dim_date_key,
       d_mms_mms_tran.post_dim_time_key,
       d_mms_mms_tran.receipt_comment,
       d_mms_mms_tran.receipt_number,
       d_mms_mms_tran.refund_flag,
       d_mms_mms_tran.reversal_flag,
       d_mms_mms_tran.sales_entered_dim_employee_key,
       d_mms_mms_tran.transaction_edited_flag,
       d_mms_mms_tran.voided_flag,
       d_mms_mms_tran.dv_batch_id,
       d_mms_mms_tran.dv_load_date_time
  from d_mms_mms_tran d_mms_mms_tran
 where d_mms_mms_tran.fact_mms_sales_transaction_key in (select fact_mms_sales_transaction_key from #tran_item_current_batch)


  /* create combined #tran_item*/
if object_id('tempdb..#tran_item') is not null drop table #tran_item
create table dbo.#tran_item with(distribution=hash(fact_mms_sales_transaction_key), location=user_db) as
select * from #tran_item_current_batch
 union
select * from #tran_item_from_mms_tran_current_batch

 /* create combined #mms_tran*/
if object_id('tempdb..#mms_tran') is not null drop table #mms_tran
create table dbo.#mms_tran with(distribution=hash(fact_mms_sales_transaction_key), location=user_db) as
select * from #mms_tran_current_batch
 union
select * from #mms_tran_from_tran_item_current_batch

 /* sale_commission records associated with tran_items*/
 /* there can two sales commission employees so abritrarily give the one with the lowest sale_commission_id a rank of 1*/
if object_id('tempdb..#sale_commission') is not null drop table #sale_commission
create table dbo.#sale_commission with(distribution=hash(fact_mms_sales_transaction_item_key), location=user_db) as
select d_mms_sale_commission.fact_mms_sales_transaction_item_key,
       d_mms_sale_commission.dim_employee_key,
       row_number() over(partition by d_mms_sale_commission.fact_mms_sales_transaction_item_key order by d_mms_sale_commission.sale_commission_id) r
  from d_mms_sale_commission
 where d_mms_sale_commission.fact_mms_sales_transaction_item_key in (select fact_mms_sales_transaction_item_key from #tran_item)

  /* membership records associated with mms_trans*/
if object_id('tempdb..#membership') is not null drop table #membership
create table dbo.#membership with(distribution=hash(dim_mms_membership_key), location=user_db) as
select d_mms_membership.dim_mms_membership_key,
       d_mms_membership.dim_mms_company_key,
       d_mms_membership.home_dim_club_key
  from d_mms_membership
 where d_mms_membership.dim_mms_membership_key in (select dim_mms_membership_key from #mms_tran)

  /* tran_item_refund records associated with tran_items*/
if object_id('tempdb..#tran_item_refund') is not null drop table #tran_item_refund
create table dbo.#tran_item_refund with(distribution=hash(fact_mms_sales_transaction_item_key), location=user_db) as
select d_mms_tran_item_refund.tran_item_refund_id,
       d_mms_tran_item_refund.fact_mms_sales_transaction_item_key
  from d_mms_tran_item_refund
 where d_mms_tran_item_refund.fact_mms_sales_transaction_item_key in (select fact_mms_sales_transaction_item_key from #tran_item)

  /* tran_voided records associated with mms_trans*/
if object_id('tempdb..#tran_voided') is not null drop table #tran_voided
create table dbo.#tran_voided with(distribution=hash(bk_hash), location=user_db) as
select d_mms_tran_voided.bk_hash,
       d_mms_tran_voided.void_comment,
       d_mms_tran_voided.void_dim_date_key,
       d_mms_tran_voided.void_dim_employee_key,
       d_mms_tran_voided.void_dim_time_key
  from d_mms_tran_voided
 where d_mms_tran_voided.bk_hash in (select mms_tran_voided_bk_hash from #mms_tran)

  /* web_order_mms_tran records associated with mms_trans*/
if object_id('tempdb..#web_order_mms_tran') is not null drop table #web_order_mms_tran
create table dbo.#web_order_mms_tran with(distribution=hash(fact_mms_sales_transaction_key), location=user_db) as
select d_mms_web_order_mms_tran.fact_mms_sales_transaction_key,
       d_mms_web_order_mms_tran.mms_web_order_bk_hash
  from d_mms_web_order_mms_tran
 where d_mms_web_order_mms_tran.fact_mms_sales_transaction_key in (select fact_mms_sales_transaction_key from #mms_tran)

  /* web_order records associated with web_order_mms_trans*/
if object_id('tempdb..#web_order') is not null drop table #web_order
create table dbo.#web_order with(distribution=hash(bk_hash), location=user_db) as
select d_mms_web_order.bk_hash,
       d_mms_web_order.val_product_sales_channel_id
  from d_mms_web_order
 where d_mms_web_order.bk_hash in (select mms_web_order_bk_hash from #web_order_mms_tran)

  /* Main query*/
if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_sales_transaction_item_key), location=user_db) as
    select #tran_item.fact_mms_sales_transaction_item_key fact_mms_sales_transaction_item_key,
    #tran_item.tran_item_id tran_item_id,
	#mms_tran.post_dim_date_key post_dim_date_key,
    #mms_tran.month_ending_post_dim_date_key month_ending_post_dim_date_key,
    case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999')
         then #tran_item.fact_mms_sales_transaction_item_key
         when #mms_tran.club_id = 9999 and #tran_item.club_id is not null
         then #tran_item.dim_club_key
         else #mms_tran.dim_club_key
     end dim_club_key,
    #tran_item.dim_mms_product_key dim_mms_product_key,
    #mms_tran.dim_mms_member_key dim_mms_member_key,
    case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999')
         then #tran_item.fact_mms_sales_transaction_item_key
         when sale_commission_1.dim_employee_key is null then '-998'
         else sale_commission_1.dim_employee_key
         end primary_sales_dim_employee_key,
         case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
         when sale_commission_2.dim_employee_key is null then '-998'
         else sale_commission_2.dim_employee_key
     end secondary_sales_dim_employee_key,
   #mms_tran.dim_mms_transaction_reason_key dim_mms_transaction_reason_key,
   #mms_tran.mms_tran_id mms_tran_id,
   #mms_tran.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
   case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then 0
        when #mms_tran.dim_mms_member_key in ('-997','-998','-999') then 0
        when #membership.dim_mms_company_key not in ('-997','-998','-999') then 1
        else 0
    end corporate_rate_count,
   #tran_item.sales_dollar_amount sales_dollar_amount,
   #tran_item.sales_quantity sales_quantity,
   #mms_tran.refund_flag refund_flag,
   #mms_tran.voided_flag voided_flag,
   #mms_tran.reversal_flag reversal_flag,
   #mms_tran.original_mms_tran_id original_mms_tran_id,
   #mms_tran.original_fact_mms_sales_transaction_key original_fact_mms_sales_transaction_key,
   #mms_tran.transaction_edited_flag transaction_edited_flag,
   #mms_tran.membership_charge_flag membership_charge_flag,
   #mms_tran.membership_adjustment_flag membership_adjustment_flag,
   #mms_tran.pos_flag pos_flag,
   case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
        when #mms_tran.employee_id = -2 and post_dim_date.day_number_in_month  =1 and getdate_dim_date.day_number_in_month=1
        then getdate_dim_date.next_day_dim_date_key
        else getdate_dim_date.dim_date_key
    end step1_udw_inserted_dim_date_key,
   #mms_tran.sales_entered_dim_employee_key sales_entered_dim_employee_key,
   #tran_item.sales_discount_dollar_amount sales_discount_dollar_amount,
   #mms_tran.post_dim_time_key post_dim_time_key,
   #mms_tran.original_currency_code original_currency_code,
   case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
        when #mms_tran.month_ending_post_dim_date_key in ('-997', '-998', '-999') then #tran_item.fact_mms_sales_transaction_item_key
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#mms_tran.month_ending_post_dim_date_key),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(#mms_tran.original_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                         'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
    end usd_monthly_average_dim_exchange_rate_key,
    case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#mms_tran.original_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
     end usd_dim_plan_exchange_rate_key,
    case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
         when #mms_tran.club_id = 13 and #membership.home_dim_club_key is null then '-998'
         when #mms_tran.club_id = 13 then #membership.home_dim_club_key
         else #mms_tran.dim_club_key
     end transaction_reporting_dim_club_key,
    #mms_tran.dim_mms_drawer_activity_key dim_mms_drawer_activity_key,
    case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
         when #tran_voided.void_dim_date_key  is null then '-998'
         else #tran_voided.void_dim_date_key
      end void_dim_date_key,
     case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
          when #tran_voided.void_dim_time_key  is null then '-998'
          else #tran_voided.void_dim_time_key
      end void_dim_time_key,
    case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
         when #tran_voided.void_dim_employee_key  is null then '-998'
         else #tran_voided.void_dim_employee_key
      end void_dim_employee_key,
    case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
         when #web_order.val_product_sales_channel_id is not null
         then 'r_mms_val_product_sales_channel_'+ convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(#web_order.val_product_sales_channel_id as varchar(500)),'z#@$k%&P'))),2)
         when #mms_tran.employee_id in (-2,-4,-5) then  'mms_sales_channel_special_employee_' + convert(varchar, #mms_tran.employee_id)
         else 'mms_sales_channel_mms_default'
      end sales_channel_dim_description_key,
    #mms_tran.dim_mms_reimbursement_program_key dim_mms_reimbursement_program_key,
    case when #tran_item.fact_mms_sales_transaction_item_key in ('-997','-998','-999') then #tran_item.fact_mms_sales_transaction_item_key
        when #tran_voided.void_comment is null then ''
        else #tran_voided.void_comment
         end void_comment,
    #mms_tran.receipt_number receipt_number,
    #mms_tran.receipt_comment receipt_comment,
    #tran_item.sales_tax_amount sales_tax_amount,
    #tran_item.sold_not_serviced_flag sold_not_serviced_flag,
    case when #tran_item_refund.tran_item_refund_id is not null and #mms_tran.refund_flag = 'Y' then 'Y'
         else 'N'
       end  automated_refund_flag,
    #tran_item.sales_amount_gross sales_amount_gross,
    #mms_tran.membership_id membership_id,
    #mms_tran.dim_mms_membership_key dim_mms_membership_key,
     case when #mms_tran.reversal_flag = 'Y' then 'N'
          when #mms_tran.transaction_edited_flag = 'Y' then 'N'
          when #mms_tran.voided_flag = 'Y' then 'N'
          else 'Y'
        end active_transaction_flag,
	#tran_item.transaction_source transaction_source, /*--added UDW-8711*/
	#tran_item.external_item_id external_item_id,   /*--added UDW-8711*/
	#tran_item.item_lt_bucks_amount item_lt_bucks_amount, /*-added UDW-8114*/
	#tran_item.inserted_date_time inserted_date_time,  /*-added UDW-8114*/
	#tran_item.updated_date_time updated_date_time,  /*-added UDW-8114*/
	#tran_item.inserted_dim_date_key inserted_dim_date_key, /*-added UDW-8114*/
	#tran_item.inserted_dim_time_key inserted_dim_time_key, /*-added UDW-8114*/
	#tran_item.updated_dim_date_key updated_dim_date_key, /*-added UDW-8114*/
	#tran_item.updated_dim_time_key updated_dim_time_key, /*-added UDW-8114*/
     case when #tran_item.dv_load_date_time >= isnull(#mms_tran.dv_load_date_time,'jan 1, 1753')
          then #tran_item.dv_load_date_time
         else #mms_tran.dv_load_date_time
        end dv_load_date_time,
    'dec 31, 9999' dv_load_end_date_time,
     case when #tran_item.dv_batch_id >= isnull(#mms_tran.dv_batch_id,-1)
          then #tran_item.dv_batch_id
          else #mms_tran.dv_batch_id
        end dv_batch_id
  from #tran_item
  join #mms_tran
    on #tran_item.fact_mms_sales_transaction_key = #mms_tran.fact_mms_sales_transaction_key
  join dim_date getdate_dim_date
    on getdate_dim_date.dim_date_key = convert(varchar, getdate(), 112)
  join dim_date post_dim_date
    on #mms_tran.post_dim_date_key = post_dim_date.dim_date_key
  left join #sale_commission sale_commission_1
    on #tran_item.fact_mms_sales_transaction_item_key = sale_commission_1.fact_mms_sales_transaction_item_key
   and sale_commission_1.r = 1
  left join #sale_commission sale_commission_2
    on #tran_item.fact_mms_sales_transaction_item_key = sale_commission_2.fact_mms_sales_transaction_item_key
   and sale_commission_2.r = 2
  left join #membership
    on #mms_tran.dim_mms_membership_key = #membership.dim_mms_membership_key
  left join #tran_voided
    on #mms_tran.mms_tran_voided_bk_hash = #tran_voided.bk_hash
  left join #tran_item_refund
    on #tran_item.fact_mms_sales_transaction_item_key = #tran_item_refund.fact_mms_sales_transaction_item_key
  left join #web_order_mms_tran
    on #mms_tran.fact_mms_sales_transaction_key = #web_order_mms_tran.fact_mms_sales_transaction_key
  left join #web_order
    on #web_order_mms_tran.mms_web_order_bk_hash = #web_order.bk_hash


if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_mms_sales_transaction_item_key), location=user_db) as
select #etl_step_1.fact_mms_sales_transaction_item_key fact_mms_sales_transaction_item_key,
       #etl_step_1.tran_item_id tran_item_id,
   #etl_step_1.post_dim_date_key,
   #etl_step_1.month_ending_post_dim_date_key month_ending_post_dim_date_key,
   #etl_step_1.dim_club_key dim_club_key,
   #etl_step_1.dim_mms_product_key dim_mms_product_key,
   #etl_step_1.dim_mms_member_key dim_mms_member_key,
   #etl_step_1.primary_sales_dim_employee_key primary_sales_dim_employee_key,
   #etl_step_1.secondary_sales_dim_employee_key secondary_sales_dim_employee_key,
   #etl_step_1.dim_mms_transaction_reason_key dim_mms_transaction_reason_key,
   #etl_step_1.mms_tran_id mms_tran_id,
   #etl_step_1.fact_mms_sales_transaction_key fact_mms_sales_transaction_key,
   #etl_step_1.corporate_rate_count corporate_rate_count,
   #etl_step_1.sales_dollar_amount sales_dollar_amount,
   #etl_step_1.sales_quantity sales_quantity,
   #etl_step_1.refund_flag refund_flag,
   #etl_step_1.voided_flag voided_flag,
   #etl_step_1.reversal_flag reversal_flag,
   #etl_step_1.original_mms_tran_id original_mms_tran_id,
   #etl_step_1.original_fact_mms_sales_transaction_key original_fact_mms_sales_transaction_key,
   #etl_step_1.transaction_edited_flag transaction_edited_flag,
   #etl_step_1.membership_charge_flag membership_charge_flag,
   #etl_step_1.membership_adjustment_flag membership_adjustment_flag,
   #etl_step_1.pos_flag pos_flag,
   isnull(fact_mms_sales_transaction_item.udw_inserted_dim_date_key,#etl_step_1.step1_udw_inserted_dim_date_key) udw_inserted_dim_date_key,
   #etl_step_1.sales_entered_dim_employee_key sales_entered_dim_employee_key,
   #etl_step_1.sales_discount_dollar_amount sales_discount_dollar_amount,
   #etl_step_1.post_dim_time_key post_dim_time_key,
   #etl_step_1.original_currency_code original_currency_code,
   #etl_step_1.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key,
   #etl_step_1.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
   #etl_step_1.dim_mms_drawer_activity_key dim_mms_drawer_activity_key,
   #etl_step_1.void_dim_date_key void_dim_date_key,
   #etl_step_1.void_dim_time_key void_dim_time_key,
   #etl_step_1.void_dim_employee_key void_dim_employee_key,
   #etl_step_1.sales_channel_dim_description_key sales_channel_dim_description_key,
   #etl_step_1.dim_mms_reimbursement_program_key dim_mms_reimbursement_program_key,
   #etl_step_1.void_comment void_comment,
   #etl_step_1.receipt_number receipt_number,
   #etl_step_1.receipt_comment receipt_comment,
   #etl_step_1.sales_tax_amount sales_tax_amount,
   #etl_step_1.sold_not_serviced_flag sold_not_serviced_flag,
   #etl_step_1.automated_refund_flag automated_refund_flag,
   #etl_step_1.sales_amount_gross sales_amount_gross,
   #etl_step_1.membership_id membership_id,
   #etl_step_1.dim_mms_membership_key dim_mms_membership_key,
   #etl_step_1.active_transaction_flag active_transaction_flag,
   #etl_step_1.transaction_source transaction_source, /*--added UDW-8711*/
   #etl_step_1.external_item_id external_item_id, /*--added UDW-8711*/
   #etl_step_1.item_lt_bucks_amount item_lt_bucks_amount, /*-added UDW-8114*/
   #etl_step_1.inserted_date_time inserted_date_time,  /*-added UDW-8114*/
   #etl_step_1.updated_date_time updated_date_time,  /*-added UDW-8114*/
   #etl_step_1.inserted_dim_date_key inserted_dim_date_key, /*-added UDW-8114*/
   #etl_step_1.inserted_dim_time_key inserted_dim_time_key, /*-added UDW-8114*/
   #etl_step_1.updated_dim_date_key updated_dim_date_key, /*-added UDW-8114*/
   #etl_step_1.updated_dim_time_key updated_dim_time_key, /*-added UDW-8114*/
   #etl_step_1.transaction_reporting_dim_club_key,
  case when #etl_step_1.fact_mms_sales_transaction_item_key in ('-997', '-998', '-999') then #etl_step_1.fact_mms_sales_transaction_item_key
       when #etl_step_1.month_ending_post_dim_date_key in ('-997', '-998', '-999') then #etl_step_1.fact_mms_sales_transaction_item_key
       when dim_club.local_currency_code is null
       then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_1.month_ending_post_dim_date_key),'z#@$k%&P')+
                                          'P%#&z$@k'+isnull(#etl_step_1.original_currency_code,'z#@$k%&P')+
                                          'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                          'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
  else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_1.month_ending_post_dim_date_key),'z#@$k%&P')+
                                     'P%#&z$@k'+isnull(#etl_step_1.original_currency_code,'z#@$k%&P')+
                                     'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                     'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
  end transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
  case when #etl_step_1.fact_mms_sales_transaction_item_key in ('-997', '-998', '-999')
  then #etl_step_1.fact_mms_sales_transaction_item_key
    when dim_club.local_currency_code is null then
   convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_1.original_currency_code,'z#@$k%&P')+
                                     'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
   else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_1.original_currency_code,'z#@$k%&P')+
                                          'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
  end transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key,
  #etl_step_1.dv_load_date_time,
  #etl_step_1.dv_load_end_date_time,
  #etl_step_1.dv_batch_id
    from #etl_step_1
 left join dim_club
 on  #etl_step_1.transaction_reporting_dim_club_key = dim_club.dim_club_key
 left join fact_mms_sales_transaction_item
 on #etl_step_1.fact_mms_sales_transaction_item_key = fact_mms_sales_transaction_item.fact_mms_sales_transaction_item_key
 /*where fact_mms_sales_transaction_item.udw_inserted_dim_date_key is not null --should be necessary, if it is NULL, there's a wider issue*/


 /* Delete and re-insert as a single transaction*/
/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/

begin tran

  delete dbo.fact_mms_sales_transaction_item
   where fact_mms_sales_transaction_item_key in (select fact_mms_sales_transaction_item_key from dbo.#etl_step_2)

   insert into fact_mms_sales_transaction_item
     (
    fact_mms_sales_transaction_item_key,
    tran_item_id,
	post_dim_date_key,
    month_ending_post_dim_date_key,
    dim_club_key ,
    dim_mms_product_key,
    dim_mms_member_key,
    primary_sales_dim_employee_key,
    secondary_sales_dim_employee_key,
    dim_mms_transaction_reason_key,
    mms_tran_id,
    fact_mms_sales_transaction_key,
    corporate_rate_count,
    sales_dollar_amount,
    sales_quantity,
    refund_flag,
    voided_flag,
    reversal_flag,
    original_mms_tran_id,
    original_fact_mms_sales_transaction_key,
    transaction_edited_flag,
    membership_charge_flag,
    membership_adjustment_flag,
    pos_flag,
    udw_inserted_dim_date_key,
    sales_entered_dim_employee_key,
    sales_discount_dollar_amount,
    post_dim_time_key,
    original_currency_code,
    usd_monthly_average_dim_exchange_rate_key,
    usd_dim_plan_exchange_rate_key,
    dim_mms_drawer_activity_key,
    void_dim_date_key,
    void_dim_time_key,
    void_dim_employee_key,
    sales_channel_dim_description_key,
    dim_mms_reimbursement_program_key,
    void_comment,
    receipt_number,
    receipt_comment,
    sales_tax_amount,
    sold_not_serviced_flag,
    automated_refund_flag,
    sales_amount_gross,
    membership_id,
    dim_mms_membership_key,
    active_transaction_flag,
	transaction_source, /*--added UDW-8711*/
    external_item_id,   /*--added UDW-8711*/
	item_lt_bucks_amount, /*-added UDW-8114*/
    inserted_date_time,  /*-added UDW-8114*/
    updated_date_time,  /*-added UDW-8114*/
    inserted_dim_date_key, /*-added UDW-8114*/
    inserted_dim_time_key, /*-added UDW-8114*/
    updated_dim_date_key, /*-added UDW-8114*/
    updated_dim_time_key, /*-added UDW-8114*/
	transaction_reporting_dim_club_key,
    transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
    transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user)

 select
    fact_mms_sales_transaction_item_key,
    tran_item_id,
	post_dim_date_key,
    month_ending_post_dim_date_key,
    dim_club_key ,
    dim_mms_product_key,
    dim_mms_member_key,
    primary_sales_dim_employee_key,
    secondary_sales_dim_employee_key,
    dim_mms_transaction_reason_key,
    mms_tran_id,
    fact_mms_sales_transaction_key,
    corporate_rate_count,
    sales_dollar_amount,
    sales_quantity,
    refund_flag,
    voided_flag,
    reversal_flag,
    original_mms_tran_id,
    original_fact_mms_sales_transaction_key,
    transaction_edited_flag,
    membership_charge_flag,
    membership_adjustment_flag,
    pos_flag,
    udw_inserted_dim_date_key,
    sales_entered_dim_employee_key,
    sales_discount_dollar_amount,
    post_dim_time_key,
    original_currency_code,
    usd_monthly_average_dim_exchange_rate_key,
    usd_dim_plan_exchange_rate_key,
    dim_mms_drawer_activity_key,
    void_dim_date_key,
    void_dim_time_key,
    void_dim_employee_key,
    sales_channel_dim_description_key,
    dim_mms_reimbursement_program_key,
    void_comment,
    receipt_number,
    receipt_comment,
    sales_tax_amount,
    sold_not_serviced_flag,
    automated_refund_flag,
    sales_amount_gross,
    membership_id,
    dim_mms_membership_key,
    active_transaction_flag,
	transaction_source, /*--added UDW-8711*/
    external_item_id,   /*--added UDW-8711*/
	item_lt_bucks_amount, /*-added UDW-8114*/
    inserted_date_time,  /*-added UDW-8114*/
    updated_date_time,  /*-added UDW-8114*/
    inserted_dim_date_key, /*-added UDW-8114*/
    inserted_dim_time_key, /*-added UDW-8114*/
    updated_dim_date_key, /*-added UDW-8114*/
    updated_dim_time_key, /*-added UDW-8114*/
	transaction_reporting_dim_club_key,
    transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
    transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    getdate() ,
    suser_sname()
   from #etl_step_2

 commit tran

 declare @max_dv_batch_id_etl_step_3 bigint
declare @extract_month_starting_dim_date_key char(8)
declare @club_13_dim_club_key char(32)

 /* Find the dv_batch_id of the most recent completed job (excluding the current batch)*/
 select @max_dv_batch_id = max(dv_batch_id)
  from dv_job_status_history
 where job_name = 'wf_' + @job_group + '_master_begin'
   and job_status = 'Complete'

 /* Find the month_starting_dim_date_key for the begin_extract_date_time*/
 select @extract_month_starting_dim_date_key = month_starting_dim_date_key
  from dim_date
 where convert(varchar, @begin_extract_date_time, 112) = dim_date.calendar_date

  /* Find the dim_club_key for club_id 13*/
select @club_13_dim_club_key = dim_club_key
  from dim_club
 where club_id = 13


 if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(fact_mms_sales_transaction_item_key), location=user_db) as
select fact_mms_sales_transaction_item.fact_mms_sales_transaction_item_key,
       d_mms_membership.home_dim_club_key transaction_reporting_dim_club_key,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,dim_date.month_ending_dim_date_key),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(fact_mms_sales_transaction_item.original_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
									   transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(fact_mms_sales_transaction_item.original_currency_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
								       transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key
    from  fact_mms_sales_transaction_item
    join d_mms_membership
    on fact_mms_sales_transaction_item.dim_mms_membership_key = d_mms_membership.dim_mms_membership_key
    and d_mms_membership.dv_batch_id > @max_dv_batch_id
    join dim_club
    on d_mms_membership.home_dim_club_key = dim_club.dim_club_key
    join dim_date
    on fact_mms_sales_transaction_item.post_dim_date_key = dim_date.dim_date_key
    where fact_mms_sales_transaction_item.post_dim_date_key >= @extract_month_starting_dim_date_key
   and fact_mms_sales_transaction_item.dim_club_key = @club_13_dim_club_key


 update fact_mms_sales_transaction_item
 set transaction_reporting_dim_club_key =  #etl_step_3.transaction_reporting_dim_club_key,
 transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key = #etl_step_3.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
 transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key=#etl_step_3.transaction_reporting_local_currency_monthly_average_dim_plan_exchange_rate_key
 from  #etl_step_3
 where fact_mms_sales_transaction_item.fact_mms_sales_transaction_item_key=#etl_step_3.fact_mms_sales_transaction_item_key

end
