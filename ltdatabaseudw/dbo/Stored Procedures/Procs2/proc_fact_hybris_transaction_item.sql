CREATE PROC [dbo].[proc_fact_hybris_transaction_item] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

/*declare @dv_batch_id bigint = -1*/
declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_hybris_transaction_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

DECLARE @month_starting_dim_date_key varchar(8)
  /* Find the DimDateKey for the first day of the month in $$BeginExtractDateTime*/
Set @month_starting_dim_date_key = 
    (SELECT case when @dv_batch_id < cast(1 as bigint) then '17630101' else convert(varchar,isnull(DATEADD(mm,DATEDIFF(mm,0,begin_extract_date_time),0),'jan 1, 1763'),112) end
       FROM dv_job_status
      WHERE job_name = 'wf_bv_fact_hybris_transaction_item')


if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_hybris_transaction_item_key), location=user_db) as 
select d_hybris_seven_day_orders.sale_fact_hybris_transaction_item_key fact_hybris_transaction_item_key,
       d_hybris_seven_day_orders.affiliate_id,
       d_hybris_seven_day_orders.capture_amex amex_amount,
       d_hybris_seven_day_orders.auto_ship_flag,
       d_hybris_seven_day_orders.capture_lt_bucks bucks_amount,
       d_hybris_seven_day_orders.selected_club_id,
       d_hybris_seven_day_orders.selected_dim_club_key,
       d_hybris_seven_day_orders.customer_email,
       d_hybris_seven_day_orders.customer_group,
       d_hybris_seven_day_orders.customer_name,
       d_hybris_seven_day_orders.dim_hybris_product_key,
       d_hybris_seven_day_orders.ltf_party_id,/*convert to dim_mms_member_key       */
       d_hybris_seven_day_orders.capture_discover discover_amount,
       d_hybris_seven_day_orders.fact_mms_sales_transaction_key,
       d_hybris_seven_day_orders.fulfillment_partner,
       d_hybris_seven_day_orders.lt_bucks_earned,
       d_hybris_seven_day_orders.capture_master mastercard_amount,
       d_hybris_seven_day_orders.order_code,
       d_hybris_seven_day_orders.order_dim_date_key,
       d_hybris_seven_day_orders.original_unit_price,
       d_hybris_seven_day_orders.entry_number,
       d_hybris_seven_day_orders.settlement_datetime,
       d_hybris_seven_day_orders.settlement_dim_date_key,
	   d_hybris_seven_day_orders.settlement_dim_time_key,  /*-added for user story UDW-10020-----*/
       d_hybris_seven_day_orders.discount_amount,
       d_hybris_seven_day_orders.purchase_unit_price,
       d_hybris_seven_day_orders.sales_tax_amount tax_amount,
       d_hybris_seven_day_orders.sales_shipping_and_handling_amount shipping_and_handling_amount,
       d_hybris_seven_day_orders.tracking_number,
       d_hybris_seven_day_orders.capture_paypal paypal_amount,
       'N' refund_flag,
       '' refund_reason,
       d_hybris_seven_day_orders.sales_amount_gross transaction_amount_gross,
       d_hybris_seven_day_orders.sales_dim_employee_key,
       d_hybris_seven_day_orders.sales_quantity transaction_quantity,
       d_hybris_seven_day_orders.capture_visa visa_amount,
       d_hybris_seven_day_orders.sales_amount transaction_amount,
       d_hybris_seven_day_orders.dv_load_date_time,
       d_hybris_seven_day_orders.dv_batch_id,
       d_hybris_seven_day_orders.sale_allocated_recalculate_through_datetime allocated_recalculate_through_datetime,
       d_hybris_seven_day_orders.sale_allocated_recalculate_through_dim_date_key allocated_recalculate_through_dim_date_key,
       d_hybris_seven_day_orders.sale_allocated_month_starting_dim_date_key allocated_month_starting_dim_date_key
from d_hybris_seven_day_orders
where dv_batch_id >= @load_dv_batch_id /*today's batch*/
   or sale_fact_hybris_transaction_item_key in (select  fact_hybris_transaction_item_key /*or tran_report_club_key recalc*/
                                                  from fact_hybris_transaction_item
                                                 where fact_hybris_transaction_item.transaction_reporting_dim_club_key = (select dim_club_key from dim_club where club_id = 13)
                                                   and fact_hybris_transaction_item.settlement_dim_date_key >= @month_starting_dim_date_key)

union

select d_hybris_seven_day_orders.refund_fact_hybris_transaction_item_key fact_hybris_transaction_item_key,
       d_hybris_seven_day_orders.affiliate_id,
       -1 * abs(d_hybris_seven_day_orders.refund_amex) amex_amount,
       d_hybris_seven_day_orders.auto_ship_flag,
       -1 * abs(d_hybris_seven_day_orders.refund_lt_bucks) bucks_amount,
       d_hybris_seven_day_orders.selected_club_id,
       d_hybris_seven_day_orders.selected_dim_club_key,
       d_hybris_seven_day_orders.customer_email,
       d_hybris_seven_day_orders.customer_group,
       d_hybris_seven_day_orders.customer_name,
       d_hybris_seven_day_orders.dim_hybris_product_key,
       d_hybris_seven_day_orders.ltf_party_id,/*convert to dim_mms_member_key       */
       -1 * abs(d_hybris_seven_day_orders.refund_discover) discover_amount,
       '-998' fact_mms_sales_transaction_key,
       d_hybris_seven_day_orders.fulfillment_partner,
       d_hybris_seven_day_orders.lt_bucks_earned,
       -1 * abs(d_hybris_seven_day_orders.refund_master) mastercard_amount,
       d_hybris_seven_day_orders.order_code,
       d_hybris_seven_day_orders.order_dim_date_key,
       d_hybris_seven_day_orders.original_unit_price,
       d_hybris_seven_day_orders.entry_number,
       d_hybris_seven_day_orders.refund_datetime,/*settlement_date_time*/
       convert(varchar(8),d_hybris_seven_day_orders.refund_datetime,112), /*settlement_dim_date_key*/
	   case when d_hybris_seven_day_orders.refund_datetime is null then '-998' else '1'+ replace(substring(convert(varchar,convert(datetime,d_hybris_seven_day_orders.refund_datetime,126),114), 1, 5),':','')  end, /*-settlement_dim_time_key ---added for user story UDW-10020-----*/
       d_hybris_seven_day_orders.discount_amount,
       d_hybris_seven_day_orders.purchase_unit_price,
       -1 * abs(d_hybris_seven_day_orders.refund_tax_amount),
       -1 * abs(d_hybris_seven_day_orders.refund_shipping_and_handling_amount),
       d_hybris_seven_day_orders.tracking_number,
       -1 * abs(d_hybris_seven_day_orders.refund_paypal) paypal_amount,
       d_hybris_seven_day_orders.refund_flag,
       d_hybris_seven_day_orders.refund_reason,
       -1 * abs(d_hybris_seven_day_orders.refund_amount_gross),
       d_hybris_seven_day_orders.sales_dim_employee_key,
       -1 * abs(d_hybris_seven_day_orders.refund_quantity),
       -1 * abs(d_hybris_seven_day_orders.refund_visa),
       -1 * abs(d_hybris_seven_day_orders.refund_amount),
       d_hybris_seven_day_orders.dv_load_date_time,
       d_hybris_seven_day_orders.dv_batch_id,
       d_hybris_seven_day_orders.refund_allocated_recalculate_through_datetime allocated_recalculate_through_datetime,
       d_hybris_seven_day_orders.refund_allocated_recalculate_through_dim_date_key allocated_recalculate_through_dim_date_key,
       d_hybris_seven_day_orders.refund_allocated_month_starting_dim_date_key allocated_month_starting_dim_date_key
from d_hybris_seven_day_orders
where refund_flag = 'y'
  and (dv_batch_id >= @load_dv_batch_id /*today's load*/
         or refund_fact_hybris_transaction_item_key in (select fact_hybris_transaction_item_key  /*or tran_report_club_key recalc*/
                                                          from fact_hybris_transaction_item
                                                         where fact_hybris_transaction_item.transaction_reporting_dim_club_key = (select dim_club_key from dim_club where club_id = 13)
                                                           and fact_hybris_transaction_item.settlement_dim_date_key >= @month_starting_dim_date_key))


if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_hybris_transaction_item_key), location=user_db) as 
select distinct
       #etl_step_1.fact_hybris_transaction_item_key fact_hybris_transaction_item_key,
       #etl_step_1.settlement_dim_date_key settlement_dim_date_key,
	   #etl_step_1.settlement_dim_time_key settlement_dim_time_key,
       #etl_step_1.sales_dim_employee_key sales_dim_employee_key,
       isnull(d_mms_member.dim_mms_member_key,'-998') dim_mms_member_key,
       isnull(d_mms_member.dim_mms_membership_key,'-998') dim_mms_membership_key,
       isnull(dim_mms_membership_history.home_dim_club_key,'-998') membership_dim_club_key,
       isnull(d_mms_employee_history.dim_club_key,'-998') employee_dim_club_key,
       corporate_club.dim_club_key corporate_dim_club_key,
       case when map_ltfeb_party_id.assigned_id is null and d_mms_employee_history.dim_employee_key is null and #etl_step_1.selected_club_id is null
                             then corporate_club.dim_club_key
            when #etl_step_1.selected_club_id is not null then #etl_step_1.selected_dim_club_key
            when d_mms_employee_history.dim_club_key is not null then d_mms_employee_history.dim_club_key
            when dim_mms_membership_history.home_dim_club_key is not null then dim_mms_membership_history.home_dim_club_key
            else corporate_club.dim_club_key end dim_club_key,
       dim_date.month_ending_dim_date_key settlement_month_ending_dim_date_key,
       rank() over (partition by fact_hybris_transaction_item_key,map_ltfeb_party_id.ltfeb_party_id order by map_ltfeb_party_id.effective_from_dim_date_key desc,d_mms_member.member_id desc) r
from #etl_step_1
left join dim_date
  on #etl_step_1.settlement_dim_date_key = dim_date.dim_date_key
left join map_ltfeb_party_id
  on #etl_step_1.ltf_party_id = map_ltfeb_party_id.ltfeb_party_id
 and map_ltfeb_party_id.party_relationship_role_type = 'MMS Member'
 and #etl_step_1.settlement_dim_date_key >= map_ltfeb_party_id.effective_from_dim_date_key
 and #etl_step_1.settlement_dim_date_key < map_ltfeb_party_id.effective_to_dim_date_key
left join d_mms_member
  on map_ltfeb_party_id.assigned_id = d_mms_member.member_id
left join dim_mms_membership_history
  on d_mms_member.dim_mms_membership_key = dim_mms_membership_history.dim_mms_membership_key
 and dim_mms_membership_history.dim_mms_membership_key not in ('-998','-997','-998')
 and dim_mms_membership_history.effective_date_time <= dim_date.next_month_starting_date/*jan 1, 12:00:00.000*/
 and dim_mms_membership_history.expiration_date_time > dim_date.next_month_starting_date/*jan 1, 12:00:00.000*/
left join d_mms_employee_history
  on #etl_step_1.sales_dim_employee_key = d_mms_employee_history.dim_employee_key
 and d_mms_employee_history.bk_hash not in ('-998','-997','-998')
 and d_mms_employee_history.effective_date_time <= dim_date.next_month_starting_date/*#etl_step_1.settlement_datetime*/
 and d_mms_employee_history.expiration_date_time > dim_date.next_month_starting_date/*#etl_step_1.settlement_datetime*/
join dim_club corporate_club
  on corporate_club.club_id = 13
  
delete from #etl_step_2 where r <> 1
  
if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(fact_hybris_transaction_item_key), location=user_db) as 
select #etl_step_2.fact_hybris_transaction_item_key,
       #etl_step_2.dim_mms_member_key,
       #etl_step_2.dim_mms_membership_key,
       #etl_step_2.dim_club_key,
       #etl_step_2.settlement_month_ending_dim_date_key,
       #etl_step_2.settlement_dim_date_key,
       #etl_step_2.sales_dim_employee_key,
       dim_club.club_id,
       isnull(dim_club.local_currency_code,'USD') original_currency_code,
       case when #etl_step_2.dim_club_key not in ('-999','-998','-997') and dim_club.club_id <> 13 and (dim_club.club_close_dim_date_key = '-998' or dim_club.club_close_dim_date_key >= #etl_step_2.settlement_dim_date_key)
                 then dim_club.dim_club_key
            when #etl_step_2.sales_dim_employee_key not in ('-999','-998','-997') and employee_club.club_id <> 13 and (employee_club.club_close_dim_date_key = '-998' or employee_club.club_close_dim_date_key >= #etl_step_2.settlement_dim_date_key)
                 then employee_club.dim_club_key
            when #etl_step_2.dim_mms_member_key not in ('-999','-998','-997') and membership_club.club_id <> 13 and (membership_club.club_close_dim_date_key = '-998' or membership_club.club_close_dim_date_key >= #etl_step_2.settlement_dim_date_key)
                 then membership_club.dim_club_key
            else #etl_step_2.corporate_dim_club_key
        end transaction_reporting_dim_club_key,
       #etl_step_2.membership_dim_club_key,
       #etl_step_2.employee_dim_club_key,
       #etl_step_2.corporate_dim_club_key
from #etl_step_2
left join dim_club
  on #etl_step_2.dim_club_key = dim_club.dim_club_key
left join dim_club membership_club
  on #etl_step_2.membership_dim_club_key = membership_club.dim_club_key
left join dim_club employee_club
  on #etl_step_2.employee_dim_club_key = employee_club.dim_club_key

if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(fact_hybris_transaction_item_key), location=user_db) as
select #etl_step_3.fact_hybris_transaction_item_key,
       #etl_step_3.dim_mms_member_key,
       #etl_step_3.dim_mms_membership_key,
       #etl_step_3.dim_club_key,
       #etl_step_3.club_id,
       #etl_step_3.original_currency_code,
       #etl_step_3.transaction_reporting_dim_club_key,
       case when #etl_step_3.fact_hybris_transaction_item_key in ('-997', '-998', '-999') then #etl_step_3.fact_hybris_transaction_item_key
            when #etl_step_3.settlement_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_3.settlement_month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(#etl_step_3.original_currency_code,'USD'),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
        end usd_monthly_average_dim_exchange_rate_key,
       case when #etl_step_3.fact_hybris_transaction_item_key in ('-997', '-998', '-999') then #etl_step_3.fact_hybris_transaction_item_key
            when #etl_step_3.settlement_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_3.settlement_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(#etl_step_3.original_currency_code,'USD'),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Daily Exchange Rate','z#@$k%&P'))),2)
        end usd_daily_dim_exchange_rate_key,
      case when #etl_step_3.fact_hybris_transaction_item_key in ('-997', '-998', '-999') then #etl_step_3.fact_hybris_transaction_item_key
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_3.original_currency_code,'z#@$k%&P')+
                                               'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
       end usd_dim_plan_exchange_rate_key,
      case when #etl_step_3.fact_hybris_transaction_item_key in ('-997', '-998', '-999') then #etl_step_3.fact_hybris_transaction_item_key
            when #etl_step_3.settlement_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_3.settlement_month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(#etl_step_3.original_currency_code,'USD'),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
       end transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
      case when #etl_step_3.fact_hybris_transaction_item_key in ('-997', '-998', '-999') then #etl_step_3.fact_hybris_transaction_item_key
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_3.original_currency_code,'z#@$k%&P')+
                                               'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
       end transaction_reporting_local_currency_dim_plan_exchange_rate_key,
       case when dim_club.club_id <> 13 and dim_club.club_close_dim_date_key >= #etl_step_3.settlement_dim_date_key
                 then dim_club.dim_club_key
            when #etl_step_3.sales_dim_employee_key not in ('-999','-998','-997') and employee_club.club_id <> 13 and (employee_club.club_close_dim_date_key >= #etl_step_3.settlement_dim_date_key or employee_club.club_close_dim_date_key = '-998')
                 then employee_club.dim_club_key
            when #etl_step_3.dim_mms_member_key not in ('-999','-998','-997') and membership_club.club_id <> 13 and (membership_club.club_close_dim_date_key >= #etl_step_3.settlement_dim_date_key or membership_club.club_close_dim_date_key = '-998')
                 then membership_club.dim_club_key
            else #etl_step_3.corporate_dim_club_key
        end allocated_dim_club_key
from #etl_step_3
left join dim_club
  on #etl_step_3.transaction_reporting_dim_club_key = dim_club.dim_club_key
left join dim_club membership_club
  on #etl_step_3.membership_dim_club_key = membership_club.dim_club_key
left join dim_club employee_club
  on #etl_step_3.employee_dim_club_key = employee_club.dim_club_key

begin tran
     
delete dbo.fact_hybris_transaction_item
where fact_hybris_transaction_item_key in (select fact_hybris_transaction_item_key from dbo.#etl_step_4) 
declare @insert_date datetime = getdate()
insert into fact_hybris_transaction_item (
    fact_hybris_transaction_item_key,
    affiliate_id,
    amex_amount,
    auto_ship_flag,
    bucks_amount,
    club_id,
    customer_email,
    customer_group,
    customer_name,
    dim_club_key,
    dim_hybris_product_key,
    dim_mms_member_key,
    dim_mms_membership_key,
    discover_amount,
    fact_mms_sales_transaction_key,
    fulfillment_partner,
    lt_bucks_earned,
    mastercard_amount,
    order_code,
    order_dim_date_key,
    original_unit_price,
    entry_number,
    settlement_dim_date_key,
	settlement_dim_time_key,
    discount_amount,
    purchase_unit_price,
    tax_amount,
    tracking_number,
    original_currency_code,
    paypal_amount,
    refund_flag,
    refund_reason,
    transaction_amount,
    transaction_amount_gross,
    sales_dim_employee_key,
    transaction_quantity,
    transaction_reporting_dim_club_key,
    transaction_reporting_local_currency_dim_plan_exchange_rate_key,
    transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
    udw_inserted_dim_date_key,
    usd_daily_dim_exchange_rate_key,
    usd_dim_plan_exchange_rate_key,
    usd_monthly_average_dim_exchange_rate_key,
    visa_amount,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user,
    allocated_recalculate_through_datetime,
    allocated_recalculate_through_dim_date_key,
    allocated_month_starting_dim_date_key,
    allocated_dim_club_key,
    shipping_and_handling_amount
    )
select #etl_step_1.fact_hybris_transaction_item_key,
       #etl_step_1.affiliate_id,
       #etl_step_1.amex_amount,
       #etl_step_1.auto_ship_flag,
       #etl_step_1.bucks_amount,
       #etl_step_4.club_id,
       #etl_step_1.customer_email,
       #etl_step_1.customer_group,
       #etl_step_1.customer_name,
       #etl_step_4.dim_club_key,
       #etl_step_1.dim_hybris_product_key,
       #etl_step_4.dim_mms_member_key,
       #etl_step_4.dim_mms_membership_key,
       #etl_step_1.discover_amount,
       #etl_step_1.fact_mms_sales_transaction_key,
       #etl_step_1.fulfillment_partner,
       #etl_step_1.lt_bucks_earned,
       #etl_step_1.mastercard_amount,
       #etl_step_1.order_code,
       #etl_step_1.order_dim_date_key,
       #etl_step_1.original_unit_price,
       #etl_step_1.entry_number,
       #etl_step_1.settlement_dim_date_key,
	   #etl_step_1.settlement_dim_time_key,
       #etl_step_1.discount_amount,
       #etl_step_1.purchase_unit_price,
       #etl_step_1.tax_amount,
       #etl_step_1.tracking_number,
       #etl_step_4.original_currency_code,
       #etl_step_1.paypal_amount,
       #etl_step_1.refund_flag,
       #etl_step_1.refund_reason,
       #etl_step_1.transaction_amount,
       #etl_step_1.transaction_amount_gross,
       #etl_step_1.sales_dim_employee_key,
       #etl_step_1.transaction_quantity,
       #etl_step_4.transaction_reporting_dim_club_key,
       #etl_step_4.transaction_reporting_local_currency_dim_plan_exchange_rate_key,
       #etl_step_4.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
       convert(int,convert(varchar,@insert_date,112)) udw_inserted_dim_date_key,
       #etl_step_4.usd_daily_dim_exchange_rate_key,
       #etl_step_4.usd_dim_plan_exchange_rate_key,
       #etl_step_4.usd_monthly_average_dim_exchange_rate_key,
       #etl_step_1.visa_amount,
       #etl_step_1.dv_load_date_time,
       'dec 31, 9999' dv_load_end_date_time,
       #etl_step_1.dv_batch_id,
       @insert_date dv_inserted_date_time,
       suser_sname() dv_insert_user,
       #etl_step_1.allocated_recalculate_through_datetime,
       #etl_step_1.allocated_recalculate_through_dim_date_key,
       #etl_step_1.allocated_month_starting_dim_date_key,
       #etl_step_4.allocated_dim_club_key,
       #etl_step_1.shipping_and_handling_amount
  from #etl_step_1
  join #etl_step_4 on #etl_step_1.fact_hybris_transaction_item_key = #etl_step_4.fact_hybris_transaction_item_key
 
commit tran

end
