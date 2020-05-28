CREATE PROC [dbo].[proc_fact_magento_refund_item] @dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_magento_refund_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_magento_credit_memo_item_key), location=user_db) as 
select d_magento_sales_credit_memo_item.bk_hash fact_magento_credit_memo_item_key,
       first_value(d_magento_sales_credit_memo_item.bk_hash) over (partition by d_magento_sales_credit_memo_item.d_magento_sales_credit_memo_bk_hash
                                                                   order by d_magento_sales_credit_memo_item.sales_credit_memo_item_id asc
                                                                   rows unbounded preceding) first_value
  from d_magento_sales_credit_memo d_magento_sales_credit_memo
  join d_magento_sales_credit_memo_item d_magento_sales_credit_memo_item on d_magento_sales_credit_memo.bk_hash = d_magento_sales_credit_memo_item.d_magento_sales_credit_memo_bk_hash
 where d_magento_sales_credit_memo.dv_batch_id >= @load_dv_batch_id
    or d_magento_sales_credit_memo_item.dv_batch_id >= @load_dv_batch_id

if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_magento_refund_item_key), location=user_db) as  
select d_magento_sales_credit_memo.bk_hash fact_magento_refund_key,
       d_magento_sales_credit_memo.entity_id credit_memo_id, 
       d_magento_sales_credit_memo.sales_credit_memo_credit_memo_status refund_status, 
       d_magento_sales_credit_memo.created_at refund_datetime, 
       convert(varchar(8),d_magento_sales_credit_memo.created_at,112) refund_dim_date_key, 
       d_magento_sales_credit_memo.fact_magento_invoice_key,
       d_magento_sales_credit_memo_item.bk_hash fact_magento_refund_item_key,
       d_magento_sales_credit_memo_item.sales_credit_memo_item_id credit_memo_item_id, 
       case when #etl_step_1.first_value = d_magento_sales_credit_memo_item.bk_hash then d_magento_sales_credit_memo.base_adjustment--d_magento_sales_credit_memo.sales_credit_memo_base_adjustment_positive + d_magento_sales_credit_memo.base_adjustment_negative
            else 0 end refund_adjustment_amount,
       --d_magento_sales_credit_memo.sales_credit_memo_base_adjustment_positive refund_adjustment_positive_amount,
       --d_magento_sales_credit_memo.base_adjustment_negative refund_adjustment_negative_amount,
       case when #etl_step_1.first_value = d_magento_sales_credit_memo_item.bk_hash then d_magento_sales_credit_memo.base_shipping_amount 
            else 0 end refund_shipping_amount,
       case when #etl_step_1.first_value = d_magento_sales_credit_memo_item.bk_hash then d_magento_sales_credit_memo.base_shipping_tax_amount 
            else 0 end refund_shipping_tax_amount,
       case when #etl_step_1.first_value = d_magento_sales_credit_memo_item.bk_hash then d_magento_sales_credit_memo.sales_credit_memo_base_reward_currency_amount
            else 0 end refund_reward_amount,
       d_magento_sales_credit_memo.sales_credit_memo_base_currency_code refund_currency_code,
       d_magento_sales_credit_memo_item.fact_magento_order_item_key,
       d_magento_sales_credit_memo_item.qty refund_item_quantity,
       d_magento_sales_credit_memo_item.base_price refund_item_price,
       d_magento_sales_credit_memo_item.base_cost refund_item_cost,
       d_magento_sales_credit_memo_item.base_row_total refund_item_amount,
       d_magento_sales_credit_memo_item.base_discount_amount refund_item_discount_amount,
       d_magento_sales_credit_memo_item.base_tax_amount refund_item_tax_amount,
       d_magento_sales_credit_memo.allocated_month_starting_dim_date_key,
       d_magento_sales_credit_memo.allocated_recalculate_through_datetime,
       d_magento_sales_credit_memo.allocated_recalculate_through_dim_date_key,
       case when d_magento_sales_credit_memo.dv_batch_id >= isnull(d_magento_sales_credit_memo_item.dv_batch_id,-1)
                 then d_magento_sales_credit_memo.dv_batch_id
            else isnull(d_magento_sales_credit_memo_item.dv_batch_id,-1) end dv_batch_id,
       case when d_magento_sales_credit_memo.dv_load_date_time >= isnull(d_magento_sales_credit_memo_item.dv_load_date_time,'Jan 1, 1753')
                 then d_magento_sales_credit_memo.dv_load_date_time
            else isnull(d_magento_sales_credit_memo_item.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
       d_magento_sales_credit_memo.fact_magento_payment_key
from #etl_step_1
join d_magento_sales_credit_memo_item 
  on #etl_step_1.fact_magento_credit_memo_item_key = d_magento_sales_credit_memo_item.bk_hash
join d_magento_sales_credit_memo d_magento_sales_credit_memo 
  on d_magento_sales_credit_memo.bk_hash = d_magento_sales_credit_memo_item.d_magento_sales_credit_memo_bk_hash

if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(fact_magento_refund_item_key), location=user_db) as  
select e21.fact_magento_refund_key,
       e21.credit_memo_id, 
       e21.refund_status, 
       e21.refund_datetime, 
       e21.refund_dim_date_key, 
       e21.fact_magento_invoice_key,
       e21.fact_magento_refund_item_key,
       e21.fact_magento_payment_key,
       e21.credit_memo_item_id, 
       e21.refund_adjustment_amount refund_adjustment_amount,
       e21.refund_shipping_amount refund_shipping_amount,
       e21.refund_shipping_tax_amount refund_shipping_tax_amount,
       e21.refund_reward_amount refund_reward_amount,
       e21.refund_currency_code,
       e21.fact_magento_order_item_key,
       e21.refund_item_quantity refund_item_quantity,
       e21.refund_item_price refund_item_price,
       refund_item_cost refund_item_cost,
       e21.refund_item_amount refund_item_amount,
       e21.refund_item_discount_amount refund_item_discount_amount,
       e21.refund_item_tax_amount refund_item_tax_amount,
       e21.allocated_month_starting_dim_date_key,
       e21.allocated_recalculate_through_datetime,
       e21.allocated_recalculate_through_dim_date_key,
       e21.dv_batch_id,
       e21.dv_load_date_time,
       d_magento_sales_order_item.sales_order_item_product_type,
       d_magento_sales_order_item.parent_fact_magento_order_item_key,
       d_magento_sales_order_item.bk_hash order_item_bk_hash
from #etl_step_2 e21
join d_magento_sales_order_item 
  on e21.fact_magento_order_item_key = d_magento_sales_order_item.bk_hash


if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(fact_magento_refund_item_key), location=user_db) as  
select e21.fact_magento_refund_key,
       e21.credit_memo_id, 
       e21.refund_status, 
       e21.refund_datetime, 
       e21.refund_dim_date_key, 
       e21.fact_magento_invoice_key,
       e21.fact_magento_refund_item_key,
       e21.fact_magento_payment_key,
       e21.credit_memo_item_id, 
       isnull(e22.refund_adjustment_amount,e21.refund_adjustment_amount) refund_adjustment_amount,
       isnull(e22.refund_shipping_amount,e21.refund_shipping_amount) refund_shipping_amount,
       isnull(e22.refund_shipping_tax_amount,e21.refund_shipping_tax_amount) refund_shipping_tax_amount,
       isnull(e22.refund_reward_amount,e21.refund_reward_amount) refund_reward_amount,
       e21.refund_currency_code,
       e21.fact_magento_order_item_key,
       isnull(e22.refund_item_quantity,e21.refund_item_quantity) refund_item_quantity,
       isnull(e22.refund_item_price,e21.refund_item_price) refund_item_price,
       isnull(e22.refund_item_cost,e21.refund_item_cost) refund_item_cost,
       isnull(e22.refund_item_amount,e21.refund_item_amount) refund_item_amount,
       isnull(e22.refund_item_discount_amount,e21.refund_item_discount_amount) refund_item_discount_amount,
       isnull(e22.refund_item_tax_amount,e21.refund_item_tax_amount) refund_item_tax_amount,
       e21.allocated_month_starting_dim_date_key,
       e21.allocated_recalculate_through_datetime,
       e21.allocated_recalculate_through_dim_date_key,
       e21.dv_batch_id,
       e21.dv_load_date_time
from #etl_step_3 e21
left join #etl_step_3 e22
  on e21.parent_fact_magento_order_item_key = e22.fact_magento_order_item_key
 and e21.fact_magento_payment_key = e22.fact_magento_payment_key
 and e21.fact_magento_refund_item_key not in ('-999','-998','-997')
 and e22.sales_order_item_product_type = 'configurable'
where e21.sales_order_item_product_type <> 'configurable'

begin tran
     
delete dbo.fact_magento_refund_item
where fact_magento_refund_item_key in (select fact_magento_refund_item_key from dbo.#etl_step_3) 
                 
insert into fact_magento_refund_item (
    fact_magento_refund_key,
    credit_memo_id, 
    refund_status, 
    refund_datetime, 
    refund_dim_date_key, 
    fact_magento_invoice_key,
    fact_magento_refund_item_key,
    fact_magento_order_item_key,
    fact_magento_payment_key,
    credit_memo_item_id, 
    refund_adjustment_amount,
    refund_shipping_amount,
    refund_shipping_tax_amount,
    refund_reward_amount,
    refund_currency_code,
    refund_item_quantity,
    refund_item_price,
    refund_item_cost,
    refund_item_amount,
    refund_item_discount_amount,
    refund_item_tax_amount,
    allocated_recalculate_through_datetime,
    allocated_recalculate_through_dim_date_key,
    allocated_month_starting_dim_date_key,
    dv_batch_id,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_inserted_date_time,
    dv_insert_user)
select fact_magento_refund_key,
       credit_memo_id, 
       refund_status, 
       refund_datetime, 
       refund_dim_date_key, 
       fact_magento_invoice_key,
       fact_magento_refund_item_key,
       fact_magento_order_item_key,
       fact_magento_payment_key,
       credit_memo_item_id, 
       isnull(refund_adjustment_amount,0),
       isnull(refund_shipping_amount,0),
       isnull(refund_shipping_tax_amount,0),
       isnull(refund_reward_amount,0),
       isnull(refund_currency_code,0),
       isnull(refund_item_quantity,0),
       isnull(refund_item_price,0),
       isnull(refund_item_cost,0),
       isnull(refund_item_amount,0),
       isnull(refund_item_discount_amount,0),
       isnull(refund_item_tax_amount,0),
       allocated_recalculate_through_datetime,
       allocated_recalculate_through_dim_date_key,
       allocated_month_starting_dim_date_key,
       dv_batch_id,
       dv_load_date_time,
       'dec 31, 9999',
       getdate(),
       suser_sname()
  from #etl_step_3

commit tran

end
