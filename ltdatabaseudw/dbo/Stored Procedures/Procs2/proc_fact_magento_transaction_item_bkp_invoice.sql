CREATE PROC [dbo].[proc_fact_magento_transaction_item_bkp_invoice] @dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_magento_refund_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end
DECLARE @month_starting_dim_date_key varchar(8)
  -- Find the DimDateKey for the first day of the month in $$BeginExtractDateTime
Set @month_starting_dim_date_key = 
    (SELECT case when @dv_batch_id < cast(1 as bigint) then '17630101' else convert(varchar,isnull(DATEADD(mm,DATEDIFF(mm,0,begin_extract_date_time),0),'jan 1, 1763'),112) end
       FROM dv_job_status
      WHERE job_name = 'wf_bv_fact_magento_transaction_item')

if object_id('tempdb..#invoice') is not null drop table #invoice
create table dbo.#invoice with(distribution=hash(fact_magento_invoice_item_key), location=user_db) as 
select fact_magento_invoice_item.fact_magento_invoice_item_key fact_magento_invoice_item_key,
       fact_magento_invoice_item.invoice_dim_date_key,
       fact_magento_invoice_item.invoice_datetime,
       fact_magento_invoice_item.invoice_dim_date_key transaction_dim_date_key,
       fact_magento_invoice_item.invoice_datetime transaction_datetime,
       fact_magento_invoice_item.shipping_amount,
       fact_magento_invoice_item.shipping_tax_amount,
       fact_magento_invoice_item.item_quantity transaction_quantity,
       fact_magento_invoice_item.item_cost product_cost,
       fact_magento_invoice_item.item_price product_price,
       --fact_magento_invoice_item.item_amount transaction_amount,
       fact_magento_invoice_item.item_amount transaction_item_amount,
       fact_magento_invoice_item.item_tax_amount transaction_tax_amount,
       fact_magento_invoice_item.item_discount_amount transaction_discount_amount,
       fact_magento_invoice_item.item_quantity * isnull(fact_magento_order_item.item_bucks_per_quantity,0) transaction_bucks_amount,
       fact_magento_invoice_item.allocated_recalculate_through_dim_date_key,
       fact_magento_invoice_item.allocated_recalculate_through_datetime,
       fact_magento_invoice_item.allocated_month_starting_dim_date_key,

       fact_magento_order_item.fact_magento_order_item_key,
       fact_magento_order_item.order_id,
       fact_magento_order_item.order_number,
       fact_magento_order_item.order_item_id,
       fact_magento_order_item.dim_club_key,
       fact_magento_order_item.dim_magento_customer_key,
       fact_magento_order_item.dim_mms_member_key,
       fact_magento_order_item.dim_employee_key,
       fact_magento_order_item.dim_magento_product_key,
       fact_magento_order_item.order_dim_date_key,
       fact_magento_order_item.order_datetime,
       fact_magento_order_item.shipping_state,
       fact_magento_order_item.order_currency_code currency_code,
       fact_magento_order_item.parent_fact_magento_order_item_key,

       fact_magento_payment.cc_type payment_type,
       fact_magento_payment.cc_last_4 cc_last_4,
       fact_magento_payment.batch_number batch_number,
       fact_magento_payment.credit_tran_id credit_tran_id,
       
       fact_magento_invoice_item.dv_load_date_time,
       'dec 31, 1900' dv_load_end_date_time,
       fact_magento_invoice_item.dv_batch_id,
       fact_magento_payment.fact_magento_payment_key
from fact_magento_invoice_item
join fact_magento_order_item
  on fact_magento_invoice_item.fact_magento_order_item_key = fact_magento_order_item.fact_magento_order_item_key 
join fact_magento_payment 
  on fact_magento_invoice_item.fact_magento_payment_key = fact_magento_payment.fact_magento_payment_key
where fact_magento_invoice_item.dv_batch_id >= @load_dv_batch_id
   or fact_magento_invoice_item.fact_magento_invoice_item_key in (select fact_magento_invoice_item_key--or tran_report_club_key recalc
                                                                    from fact_magento_transaction_item
                                                                   where fact_magento_transaction_item.dim_club_key = (select dim_club_key from dim_club where club_id = 13)
                                                                     and fact_magento_transaction_item.invoice_dim_date_key >= @month_starting_dim_date_key)

if object_id('tempdb..#mms_tran_id') is not null drop table #mms_tran_id
create table dbo.#mms_tran_id with(distribution=hash(fact_magento_order_item_key), location=user_db) as
with mms_tran_id (fact_magento_order_item_key,mms_tran_id,r) 
as
(
        select fact_magento_order_item_key,
               cast(mms_tran_id as varchar(255)) mms_tran_id,
               row_number() over (partition by fact_magento_order_item_key order by mms_tran_id) r
        from d_magento_lifetime_order_item_change_log
        --where fact_magento_order_item_key in (select fact_magento_order_item_key from #invoice)
)
select m1.fact_magento_order_item_key,
       m1.mms_tran_id + isnull(','+m2.mms_tran_id,'')+ isnull(','+m3.mms_tran_id,'')+ isnull(','+m4.mms_tran_id,'')+ isnull(','+m5.mms_tran_id,'') as mms_tran_id
  from mms_tran_id m1
  left join mms_tran_id m2 on m1.fact_magento_order_item_key = m2.fact_magento_order_item_key and m1.r+1 = m2.r
  left join mms_tran_id m3 on m1.fact_magento_order_item_key = m3.fact_magento_order_item_key and m1.r+2 = m3.r
  left join mms_tran_id m4 on m1.fact_magento_order_item_key = m4.fact_magento_order_item_key and m1.r+3 = m4.r
  left join mms_tran_id m5 on m1.fact_magento_order_item_key = m5.fact_magento_order_item_key and m1.r+4 = m5.r
where m1.r = 1

--this query is to connect amounts from "configurable" parents to their "simple" children.  
--"configurable" records are then removed
if object_id('tempdb..#invoice_2') is not null drop table #invoice_2
create table dbo.#invoice_2 with(distribution=hash(fact_magento_invoice_item_key), location=user_db) as 
select i1.fact_magento_invoice_item_key,
       i1.invoice_dim_date_key,
       i1.invoice_datetime,
       i1.transaction_dim_date_key,
       i1.transaction_datetime,
       isnull(i2.shipping_amount,i1.shipping_amount) shipping_amount,
       isnull(i2.shipping_tax_amount,i1.shipping_tax_amount) shipping_tax_amount,
       isnull(i2.transaction_quantity,i1.transaction_quantity) transaction_quantity,
       i1.product_cost,
       isnull(i2.product_price,i1.product_price) product_price,
       --isnull(i2.transaction_amount,i1.transaction_amount) transaction_amount,
       isnull(i2.transaction_item_amount,i1.transaction_item_amount) transaction_item_amount,
       isnull(i2.transaction_tax_amount,i1.transaction_tax_amount) transaction_tax_amount,
       isnull(i2.transaction_discount_amount,i1.transaction_discount_amount) transaction_discount_amount,
       isnull(i2.transaction_bucks_amount,i1.transaction_bucks_amount) transaction_bucks_amount,
       i1.allocated_recalculate_through_dim_date_key,
       i1.allocated_recalculate_through_datetime,
       i1.allocated_month_starting_dim_date_key,
       i1.fact_magento_order_item_key,
       i1.order_id,
       i1.order_number,
       i1.order_item_id,
       i1.dim_club_key,
       i1.dim_magento_customer_key,
       i1.dim_mms_member_key,
       i1.dim_employee_key,
       i1.dim_magento_product_key,
       i1.order_dim_date_key,
       i1.order_datetime,
       i1.shipping_state,
       i1.currency_code,
       i1.payment_type,
       i1.cc_last_4,
       i1.batch_number,
       i1.credit_tran_id,
       #mms_tran_id.mms_tran_id,
       null fact_mms_transaction_key,
       --i1.parent_fact_magento_order_item_key --not forward facing
       i1.dv_load_date_time,
       i1.dv_load_end_date_time,
       i1.dv_batch_id
from #invoice i1
left join #invoice i2 
  on i1.parent_fact_magento_order_item_key = i2.fact_magento_order_item_key --bundle products are removed order item,so the parent key actually points nowhere
 and i1.fact_magento_payment_key = i2.fact_magento_payment_key
 and i2.fact_magento_order_item_key not in ('-998','-999','-997')
left join #mms_tran_id
  on i1.fact_magento_order_item_key = #mms_tran_id.fact_magento_order_item_key
where i1.fact_magento_order_item_key not in (select parent_fact_magento_order_item_key from #invoice) --exclude parents (configurable)

if object_id('tempdb..#refund') is not null drop table #refund
create table dbo.#refund with(distribution=hash(fact_magento_refund_item_key), location=user_db) as 
select distinct --linking to payment via invoice (not invoice item) so results in duplicates.  All we want from order_item is what's at the "invoice" level anyway
       fact_magento_refund_item.fact_magento_refund_item_key fact_magento_refund_item_key,
       fact_magento_refund_item.refund_dim_date_key,
       fact_magento_refund_item.refund_datetime,
       fact_magento_refund_item.refund_dim_date_key transaction_dim_date_key,
       fact_magento_refund_item.refund_datetime transaction_datetime,
       -1 * abs(fact_magento_refund_item.refund_shipping_amount) shipping_amount,
       -1 * abs(fact_magento_refund_item.refund_shipping_tax_amount) shipping_tax_amount,
       -1 * abs(fact_magento_refund_item.refund_item_quantity) transaction_quantity,
       -1 * abs(fact_magento_refund_item.refund_item_cost) product_cost,
       -1 * abs(fact_magento_refund_item.refund_item_price) product_price,
       -1 * abs(fact_magento_refund_item.refund_item_amount) transaction_item_amount,
       -1 * abs(fact_magento_refund_item.refund_item_tax_amount) transaction_tax_amount,
       -1 * abs(fact_magento_refund_item.refund_item_discount_amount) transaction_discount_amount,
       -1 * abs(fact_magento_refund_item.refund_item_quantity * isnull(fact_magento_order_item.item_refund_bucks_per_quantity,0)) transaction_bucks_amount,
       fact_magento_refund_item.refund_adjustment_amount transaction_adjustment_amount,
       fact_magento_refund_item.refund_currency_code,
       fact_magento_refund_item.allocated_recalculate_through_dim_date_key,
       fact_magento_refund_item.allocated_recalculate_through_datetime,
       fact_magento_refund_item.allocated_month_starting_dim_date_key,
       fact_magento_order_item.order_id,
       fact_magento_order_item.order_number,
       fact_magento_order_item.order_item_id,
       fact_magento_order_item.dim_club_key,
       fact_magento_order_item.dim_magento_customer_key,
       fact_magento_order_item.dim_mms_member_key,
       fact_magento_order_item.dim_employee_key,
       fact_magento_order_item.dim_magento_product_key,
       fact_magento_order_item.order_dim_date_key,
       fact_magento_order_item.order_datetime,
       fact_magento_order_item.shipping_state,
       fact_magento_payment.cc_type payment_type,
       fact_magento_payment.cc_last_4,
       fact_magento_payment.batch_number,
       fact_magento_payment.credit_tran_id,
       fact_magento_refund_item.dv_load_date_time,
       'dec 31, 1900' dv_load_end_date_time,
       fact_magento_refund_item.dv_batch_id
from fact_magento_refund_item
join fact_magento_order_item 
  on fact_magento_refund_item.fact_magento_order_item_key = fact_magento_order_item.fact_magento_order_item_key
join fact_magento_payment 
  on fact_magento_refund_item.fact_magento_payment_key = fact_magento_payment.fact_magento_payment_key
where fact_magento_refund_item.dv_batch_id >= @load_dv_batch_id
   or fact_magento_refund_item.fact_magento_refund_item_key in (select fact_magento_refund_item_key--or tran_report_club_key recalc
                                                                  from fact_magento_transaction_item
                                                                 where fact_magento_transaction_item.dim_club_key = (select dim_club_key from dim_club where club_id = 13)
                                                                   and fact_magento_transaction_item.invoice_dim_date_key >= @month_starting_dim_date_key)

if object_id('tempdb..#tran_item') is not null drop table #tran_item
create table dbo.#tran_item with(distribution=round_robin, location=user_db) as 
select fact_magento_invoice_item_key,
       case when fact_magento_invoice_item_key in ('-999','-998','-997') then fact_magento_invoice_item_key else '-998' end fact_magento_refund_item_key,
       invoice_dim_date_key,
       invoice_datetime,
       null refund_dim_date_key,
       null refund_datetime,
       transaction_dim_date_key,
       transaction_datetime,
       shipping_amount,
       shipping_tax_amount,
       transaction_quantity,
       product_cost,
       product_price,
       0 transaction_adjustment_amount,
       transaction_item_amount,
       --has bucks "built in" to the item_amount
       transaction_item_amount - transaction_discount_amount transaction_amount,
       transaction_tax_amount,
       transaction_discount_amount,
       transaction_bucks_amount,
       allocated_recalculate_through_dim_date_key,
       allocated_recalculate_through_datetime,
       allocated_month_starting_dim_date_key,
       order_id,
       order_number,
       order_item_id,
       dim_club_key,
       dim_magento_customer_key,
       dim_mms_member_key,
       dim_employee_key,
       dim_magento_product_key,
       order_dim_date_key,
       order_datetime,
       shipping_state,
       currency_code,
       payment_type,
       cc_last_4,
       batch_number,
       credit_tran_id,
       mms_tran_id,
       fact_mms_transaction_key,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       'N' refund_flag
from #invoice_2
union all
select case when fact_magento_refund_item_key in ('-999','-998','-997') then fact_magento_refund_item_key else '-998' end fact_magento_invoice_item_key,
       fact_magento_refund_item_key,
       null invoice_dim_date_key,
       null invoice_datetime,
       refund_dim_date_key,
       refund_datetime,
       transaction_dim_date_key,
       transaction_datetime,
       shipping_amount,
       shipping_tax_amount,
       transaction_quantity,
       product_cost,
       product_price,
       transaction_adjustment_amount,
       transaction_item_amount,
       --item + tax + ship + ship_tax  --has bucks "built in" to the item_amount
       transaction_item_amount + transaction_adjustment_amount - transaction_discount_amount transaction_amount,
       transaction_tax_amount,
       transaction_discount_amount,
       transaction_bucks_amount,
       allocated_recalculate_through_dim_date_key,
       allocated_recalculate_through_datetime,
       allocated_month_starting_dim_date_key,
       order_id,
       order_number,
       order_item_id,
       dim_club_key,
       dim_magento_customer_key,
       dim_mms_member_key,
       dim_employee_key,
       dim_magento_product_key,
       order_dim_date_key,
       order_datetime,
       shipping_state,
       refund_currency_code currency_code,
       payment_type,
       cc_last_4,
       batch_number,
       credit_tran_id,
       null mms_tran_id,
       case when fact_magento_refund_item_key in ('-999','-998','-997') then fact_magento_refund_item_key else '-998' end fact_mms_transaction_key,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       'Y' refund_flag
from #refund

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=round_robin, location=user_db) as 
select #tran_item.fact_magento_invoice_item_key,
       #tran_item.fact_magento_refund_item_key,
       #tran_item.transaction_dim_date_key,
       #tran_item.dim_employee_key,
       #tran_item.dim_mms_member_key,
       d_mms_member.dim_mms_membership_key dim_mms_membership_key,
       dim_mms_membership_history.home_dim_club_key membership_dim_club_key,
       d_mms_employee_history.dim_club_key employee_dim_club_key,
       corporate_club.dim_club_key corporate_dim_club_key,
       #tran_item.dim_club_key,
       dim_date.month_ending_dim_date_key invoice_month_ending_dim_date_key,
       d_mms_member.member_id,
       d_mms_employee_history.employee_id
from #tran_item
left join dim_date
  on #tran_item.transaction_dim_date_key = dim_date.dim_date_key
left join d_mms_member
  on #tran_item.dim_mms_member_key = d_mms_member.dim_mms_member_key
left join dim_mms_membership_history
  on d_mms_member.dim_mms_membership_key = dim_mms_membership_history.dim_mms_membership_key
 and dim_mms_membership_history.dim_mms_membership_key not in ('-998','-997','-998')
 and dim_mms_membership_history.effective_date_time <= dim_date.next_month_starting_date--jan 1, 12:00:00.000
 and dim_mms_membership_history.expiration_date_time > dim_date.next_month_starting_date--jan 1, 12:00:00.000
left join d_mms_employee_history
  on #tran_item.dim_employee_key = d_mms_employee_history.dim_employee_key
 and d_mms_employee_history.bk_hash not in ('-998','-997','-998')
 and d_mms_employee_history.effective_date_time <= dim_date.next_month_starting_date--#etl_step_1.invoice_datetime
 and d_mms_employee_history.expiration_date_time > dim_date.next_month_starting_date--#etl_step_1.invoice_datetime
join dim_club corporate_club
  on corporate_club.club_id = 13
  
if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=round_robin, location=user_db) as 
select #etl_step_1.fact_magento_invoice_item_key,
       #etl_step_1.fact_magento_refund_item_key,
       #etl_step_1.dim_mms_member_key,
       #etl_step_1.dim_mms_membership_key,
       #etl_step_1.dim_club_key,
       #etl_step_1.invoice_month_ending_dim_date_key,
       #etl_step_1.transaction_dim_date_key,
       #etl_step_1.dim_employee_key,
       dim_club.club_id,
       isnull(dim_club.local_currency_code,'USD') original_currency_code,

       case when #etl_step_1.dim_club_key not in ('-999','-998','-997') and dim_club.club_id <> 13 and (dim_club.club_close_dim_date_key = '-998' or dim_club.club_close_dim_date_key >= #etl_step_1.transaction_dim_date_key)
                 then dim_club.dim_club_key
            when #etl_step_1.dim_employee_key not in ('-999','-998','-997') and employee_club.club_id <> 13 and (employee_club.club_close_dim_date_key = '-998' or employee_club.club_close_dim_date_key >= #etl_step_1.transaction_dim_date_key)
                 then employee_club.dim_club_key
            else #etl_step_1.corporate_dim_club_key
        end payroll_dim_club_key,
       
       case when #etl_step_1.dim_club_key not in ('-999','-998','-997') and dim_club.club_id <> 13 and (dim_club.club_close_dim_date_key = '-998' or dim_club.club_close_dim_date_key >= #etl_step_1.transaction_dim_date_key)
                 then dim_club.dim_club_key
            when #etl_step_1.dim_employee_key not in ('-999','-998','-997') and employee_club.club_id <> 13 and (employee_club.club_close_dim_date_key = '-998' or employee_club.club_close_dim_date_key >= #etl_step_1.transaction_dim_date_key)
                 then employee_club.dim_club_key
            when #etl_step_1.dim_mms_member_key not in ('-999','-998','-997') and membership_club.club_id <> 13 and (membership_club.club_close_dim_date_key = '-998' or membership_club.club_close_dim_date_key >= #etl_step_1.transaction_dim_date_key)
                 then membership_club.dim_club_key
            else #etl_step_1.corporate_dim_club_key
        end transaction_reporting_dim_club_key,
       #etl_step_1.membership_dim_club_key,
       #etl_step_1.employee_dim_club_key,
       #etl_step_1.member_id,
       #etl_step_1.employee_id
from #etl_step_1
left join dim_club
  on #etl_step_1.dim_club_key = dim_club.dim_club_key
left join dim_club membership_club
  on #etl_step_1.membership_dim_club_key = membership_club.dim_club_key
left join dim_club employee_club
  on #etl_step_1.employee_dim_club_key = employee_club.dim_club_key

if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=round_robin, location=user_db) as
select #etl_step_2.fact_magento_invoice_item_key,
       #etl_step_2.fact_magento_refund_item_key,
       #etl_step_2.dim_mms_member_key,
       #etl_step_2.dim_mms_membership_key,
       #etl_step_2.dim_club_key,
       #etl_step_2.club_id,
       #etl_step_2.original_currency_code,
       #etl_step_2.payroll_dim_club_key,
       #etl_step_2.transaction_reporting_dim_club_key,
       case when #etl_step_2.fact_magento_invoice_item_key in ('-997', '-998', '-999') and #etl_step_2.fact_magento_refund_item_key in ('-997', '-998', '-999')
                 then #etl_step_2.fact_magento_invoice_item_key
            when #etl_step_2.transaction_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_2.invoice_month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(#etl_step_2.original_currency_code,'USD'),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
        end usd_monthly_average_dim_exchange_rate_key,
       case when #etl_step_2.fact_magento_invoice_item_key in ('-997', '-998', '-999') and #etl_step_2.fact_magento_refund_item_key in ('-997', '-998', '-999')
                 then #etl_step_2.fact_magento_invoice_item_key
            when #etl_step_2.transaction_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_2.transaction_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(#etl_step_2.original_currency_code,'USD'),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Daily Exchange Rate','z#@$k%&P'))),2)
        end usd_daily_dim_exchange_rate_key,
      case when #etl_step_2.fact_magento_invoice_item_key in ('-997', '-998', '-999') and #etl_step_2.fact_magento_refund_item_key in ('-997', '-998', '-999')
                then #etl_step_2.fact_magento_invoice_item_key
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_2.original_currency_code,'z#@$k%&P')+
                                               'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
       end usd_dim_plan_exchange_rate_key,
      case when #etl_step_2.fact_magento_invoice_item_key in ('-997', '-998', '-999') and #etl_step_2.fact_magento_refund_item_key in ('-997', '-998', '-999')
                then #etl_step_2.fact_magento_invoice_item_key
           when #etl_step_2.transaction_dim_date_key is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_2.invoice_month_ending_dim_date_key),'z#@$k%&P')+
                                                  'P%#&z$@k'+isnull(isnull(#etl_step_2.original_currency_code,'USD'),'z#@$k%&P')+
                                                  'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                                  'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
       end transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
      case when #etl_step_2.fact_magento_invoice_item_key in ('-997', '-998', '-999') and #etl_step_2.fact_magento_refund_item_key in ('-997', '-998', '-999')
                then #etl_step_2.fact_magento_invoice_item_key
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_2.original_currency_code,'z#@$k%&P')+
                                               'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
       end transaction_reporting_local_currency_dim_plan_exchange_rate_key,
       #etl_step_2.member_id,
       #etl_step_2.employee_id
from #etl_step_2
left join dim_club
  on #etl_step_2.transaction_reporting_dim_club_key = dim_club.dim_club_key
left join dim_club membership_club
  on #etl_step_2.membership_dim_club_key = membership_club.dim_club_key
left join dim_club employee_club
  on #etl_step_2.employee_dim_club_key = employee_club.dim_club_key


begin tran

declare @get_date_time datetime = (select get_datetime from v_get_date)
declare @get_date_dim_date_key int = (select convert(varchar(8),@get_date_time,112))

insert into idk_fact_magento_transaction_item (
    fact_magento_invoice_item_key,
    fact_magento_refund_item_key,
    udw_inserted_date_time,
    udw_inserted_dim_date_key,
    dv_batch_id
)
select fact_magento_invoice_item_key,
       '-998',
       @get_date_time,
       @get_date_dim_date_key,
       @dv_batch_id
from #tran_item
where fact_magento_invoice_item_key not in ('-998','-999','-997')
and fact_magento_invoice_item_key not in (select fact_magento_invoice_item_key from idk_fact_magento_transaction_item)
union all
select '-998',
       fact_magento_refund_item_key,
       @get_date_time,
       @get_date_dim_date_key,
       @dv_batch_id
from #tran_item
where fact_magento_refund_item_key not in ('-998','-999','-997')
and fact_magento_refund_item_key not in (select fact_magento_refund_item_key from idk_fact_magento_transaction_item)
     
delete dbo.fact_magento_transaction_item
where fact_magento_refund_item_key in (select fact_magento_refund_item_key from dbo.#tran_item where fact_magento_refund_item_key not in ('-998','-999','-997')) 
   or fact_magento_invoice_item_key in (select fact_magento_invoice_item_key from dbo.#tran_item where fact_magento_invoice_item_key not in ('-998','-999','-997')) 
                 

insert into fact_magento_transaction_item (
    allocated_dim_club_key,
    allocated_month_starting_dim_date_key,
    allocated_recalculate_through_datetime,
    allocated_recalculate_through_dim_date_key,
    dim_club_key,
    dim_employee_key,
    employee_id,
    dim_magento_customer_key,
    dim_magento_product_key,
    dim_mms_member_key,
    dim_mms_membership_key,
    member_id,
    fact_magento_invoice_item_key,
    fact_magento_refund_item_key,
    fact_mms_transaction_key,
    invoice_datetime,
    invoice_dim_date_key,
    refund_datetime,
    refund_dim_date_key,
    order_datetime,
    order_dim_date_key,
    order_id,
    order_number,
    order_item_id,
    shipping_amount,
    shipping_tax_amount,
    original_currency_code,
    payment_type,
    cc_last_4,
    batch_number,
    credit_tran_id,
    payroll_dim_club_key,
    product_cost,
    product_price,
    refund_flag,
    shipping_state,
    transaction_adjustment_amount,
    transaction_amount,
    transaction_item_amount,
    bucks_change,
    allocated_amount,
    transaction_bucks_amount,
    transaction_datetime,
    transaction_dim_date_key,
    transaction_discount_amount,
    transaction_quantity,
    transaction_reporting_dim_club_key,
    transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
    transaction_tax_amount,
    udw_inserted_dim_date_key,
    usd_daily_dim_exchange_rate_key,
    usd_dim_plan_exchange_rate_key,
    usd_monthly_average_dim_exchange_rate_key,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user)
select transaction_reporting_dim_club_key allocated_dim_club_key, --same value
       #tran_item.allocated_month_starting_dim_date_key,
       #tran_item.allocated_recalculate_through_datetime,
       #tran_item.allocated_recalculate_through_dim_date_key,
       #tran_item.dim_club_key,
       #tran_item.dim_employee_key,
       #etl_step_3.employee_id,
       #tran_item.dim_magento_customer_key,
       #tran_item.dim_magento_product_key,
       #tran_item.dim_mms_member_key,
       #etl_step_3.dim_mms_membership_key,
       #etl_step_3.member_id,
       #tran_item.fact_magento_invoice_item_key,
       #tran_item.fact_magento_refund_item_key,
       #tran_item.fact_mms_transaction_key,
       #tran_item.invoice_datetime,
       #tran_item.invoice_dim_date_key,
       #tran_item.refund_datetime,
       #tran_item.refund_dim_date_key,
       #tran_item.order_datetime,
       #tran_item.order_dim_date_key,
       #tran_item.order_id,
       #tran_item.order_number,
       #tran_item.order_item_id,
       #tran_item.shipping_amount,
       #tran_item.shipping_tax_amount,
       #tran_item.currency_code original_currency_code,
       #tran_item.payment_type,
       #tran_item.cc_last_4,
       #tran_item.batch_number,
       #tran_item.credit_tran_id,
       #etl_step_3.payroll_dim_club_key,
       #tran_item.product_cost,
       #tran_item.product_price,
       #tran_item.refund_flag,
       #tran_item.shipping_state,
       #tran_item.transaction_adjustment_amount,
       #tran_item.transaction_amount,
       #tran_item.transaction_item_amount,
       null,--#tran_item.bucks_change,
       case when #tran_item.transaction_amount - #tran_item.transaction_bucks_amount > 0 and #tran_item.refund_flag = 'Y' then 0
            when #tran_item.transaction_amount - #tran_item.transaction_bucks_amount < 0 and #tran_item.refund_flag = 'N' then 0 
            else #tran_item.transaction_amount - #tran_item.transaction_bucks_amount
        end allocated_amount,
       #tran_item.transaction_bucks_amount,
       #tran_item.transaction_datetime,
       #tran_item.transaction_dim_date_key,
       #tran_item.transaction_discount_amount,
       #tran_item.transaction_quantity,
       #etl_step_3.transaction_reporting_dim_club_key,
       #etl_step_3.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
       #tran_item.transaction_tax_amount,
       null udw_inserted_dim_date_key,
       #etl_step_3.usd_daily_dim_exchange_rate_key,
       #etl_step_3.usd_dim_plan_exchange_rate_key,
       #etl_step_3.usd_monthly_average_dim_exchange_rate_key,
       #tran_item.dv_load_date_time,
       'dec 31, 9999' dv_load_end_date_time,
       #tran_item.dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from #tran_item
  join #etl_step_3 on #tran_item.fact_magento_invoice_item_key = #etl_step_3.fact_magento_invoice_item_key and #tran_item.fact_magento_invoice_item_key not in ('-998','-999','-997')
                   or #tran_item.fact_magento_refund_item_key = #etl_step_3.fact_magento_refund_item_key and #tran_item.fact_magento_refund_item_key not in ('-998','-999','-997')

commit tran

end
