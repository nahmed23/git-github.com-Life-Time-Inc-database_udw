CREATE PROC [dbo].[proc_fact_magento_invoice_item] @dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_magento_invoice_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_magento_invoice_item_key), location=user_db) as 
select d_magento_sales_invoice_item.bk_hash fact_magento_invoice_item_key,
       first_value(d_magento_sales_invoice_item.bk_hash) over (partition by d_magento_sales_invoice_item.fact_magento_invoice_key
                                                                   order by d_magento_sales_invoice_item.sales_invoice_item_id asc
                                                                   rows unbounded preceding) first_value
  from d_magento_sales_invoice d_magento_sales_invoice
  join d_magento_sales_invoice_item d_magento_sales_invoice_item on d_magento_sales_invoice.bk_hash = d_magento_sales_invoice_item.d_magento_sales_invoice_bk_hash
 where d_magento_sales_invoice.dv_batch_id >= @load_dv_batch_id
    or d_magento_sales_invoice_item.dv_batch_id >= @load_dv_batch_id

if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_magento_invoice_item_key), location=user_db) as 
select d_magento_sales_invoice_item.bk_hash fact_magento_invoice_item_key,
       d_magento_sales_invoice_item.sales_invoice_item_id invoice_item_id,
       d_magento_sales_invoice_item.fact_magento_invoice_key,
       d_magento_sales_invoice.fact_magento_order_key,
       d_magento_sales_invoice_item.fact_magento_order_item_key,
       d_magento_sales_invoice.fact_magento_payment_key,
       d_magento_sales_invoice.created_at invoice_datetime,
       d_magento_sales_invoice.created_dim_date_key invoice_dim_date_key,
       case when #etl_step_1.first_value = d_magento_sales_invoice_item.bk_hash then d_magento_sales_invoice.base_shipping_amount
            else 0 end shipping_amount,
       case when #etl_step_1.first_value = d_magento_sales_invoice_item.bk_hash then d_magento_sales_invoice.base_shipping_tax_amount
            else 0 end shipping_tax_amount,
       case when #etl_step_1.first_value = d_magento_sales_invoice_item.bk_hash then d_magento_sales_invoice.base_grand_total
            else 0 end invoice_amount,
       d_magento_sales_invoice.base_currency_code currency_code,
       d_magento_sales_invoice_item.qty item_quantity,
       d_magento_sales_invoice_item.base_tax_amount item_tax_amount,
       d_magento_sales_invoice_item.base_cost item_cost,
       d_magento_sales_invoice_item.base_price item_price,
       d_magento_sales_invoice_item.base_row_total item_amount,
       d_magento_sales_invoice_item.base_discount_amount item_discount_amount,
       d_magento_sales_invoice.allocated_recalculate_through_datetime,
       d_magento_sales_invoice.allocated_recalculate_through_dim_date_key,
       d_magento_sales_invoice.allocated_month_starting_dim_date_key,
       case when d_magento_sales_invoice.dv_batch_id >= isnull(d_magento_sales_invoice_item.dv_batch_id,-1)
                 then d_magento_sales_invoice.dv_batch_id
            else isnull(d_magento_sales_invoice_item.dv_batch_id,-1) end dv_batch_id,
       case when d_magento_sales_invoice.dv_load_date_time >= isnull(d_magento_sales_invoice_item.dv_load_date_time,'Jan 1, 1753')
                 then d_magento_sales_invoice.dv_load_date_time
            else isnull(d_magento_sales_invoice_item.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time
  from #etl_step_1
  join d_magento_sales_invoice_item on #etl_step_1.fact_magento_invoice_item_key = d_magento_sales_invoice_item.bk_hash
  join d_magento_sales_invoice on d_magento_sales_invoice.bk_hash = d_magento_sales_invoice_item.d_magento_sales_invoice_bk_hash

if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(fact_magento_invoice_item_key), location=user_db) as
select distinct e21.fact_magento_invoice_item_key,
       e21.invoice_item_id,
       e21.fact_magento_invoice_key,
       e21.fact_magento_order_key,
       e21.fact_magento_order_item_key,
       e21.fact_magento_payment_key,
       e21.invoice_datetime,
       e21.invoice_dim_date_key,
       e21.shipping_amount,
       e21.shipping_tax_amount,
       e21.invoice_amount,
       e21.currency_code,
       e21.item_quantity,
       e21.item_tax_amount,
       e21.item_cost,
       e21.item_price,
       e21.item_amount,
       e21.item_discount_amount,
       e21.allocated_recalculate_through_datetime,
       e21.allocated_recalculate_through_dim_date_key,
       e21.allocated_month_starting_dim_date_key,
       e21.dv_batch_id,
       e21.dv_load_date_time,
       d_magento_sales_order_item.sales_order_item_product_type,
       d_magento_sales_order_item.parent_fact_magento_order_item_key,
       d_magento_sales_order_item.bk_hash order_item_bk_hash
  from #etl_step_2 e21
  join d_magento_sales_order_item 
    on e21.fact_magento_order_item_key = d_magento_sales_order_item.bk_hash

if object_id('tempdb..#etl_step_4') is not null drop table #etl_step_4
create table dbo.#etl_step_4 with(distribution=hash(fact_magento_invoice_item_key), location=user_db) as
select distinct e21.fact_magento_invoice_item_key,
       e21.invoice_item_id,
       e21.fact_magento_invoice_key,
       e21.fact_magento_order_key,
       e21.fact_magento_order_item_key,
       e21.fact_magento_payment_key,
       e21.invoice_datetime,
       e21.invoice_dim_date_key,
       isnull(e22.shipping_amount,e21.shipping_amount) shipping_amount,
       isnull(e22.shipping_tax_amount,e21.shipping_tax_amount) shipping_tax_amount,
       isnull(e22.invoice_amount,e21.invoice_amount) invoice_amount,
       e21.currency_code,
       isnull(e22.item_quantity,e21.item_quantity) item_quantity,
       isnull(e22.item_tax_amount,e21.item_tax_amount) item_tax_amount,
       isnull(e22.item_cost,e21.item_cost) item_cost,
       isnull(e22.item_price,e21.item_price) item_price,
       isnull(e22.item_amount,e21.item_amount) item_amount,
       isnull(e22.item_discount_amount,e21.item_discount_amount) item_discount_amount,
       e21.allocated_recalculate_through_datetime,
       e21.allocated_recalculate_through_dim_date_key,
       e21.allocated_month_starting_dim_date_key,
       e21.dv_batch_id,
       e21.dv_load_date_time
  from #etl_step_3 e21
  left join #etl_step_3 e22
    on e21.parent_fact_magento_order_item_key = e22.fact_magento_order_item_key
   and e21.fact_magento_payment_key = e22.fact_magento_payment_key
   and e21.order_item_bk_hash not in ('-999','-998','-997')
   and e22.sales_order_item_product_type = 'configurable'
 --where e21.sales_order_item_product_type <> 'configurable'

begin tran
     
delete dbo.fact_magento_invoice_item
where fact_magento_invoice_item_key in (select fact_magento_invoice_item_key from dbo.#etl_step_4) 
                 
insert into fact_magento_invoice_item (
    fact_magento_invoice_item_key,
    invoice_item_id,
    fact_magento_invoice_key,
    fact_magento_order_key,
    fact_magento_order_item_key,
    fact_magento_payment_key,
    invoice_datetime,
    invoice_dim_date_key,
    shipping_amount,
    shipping_tax_amount,
    currency_code,
    invoice_amount,
    item_tax_amount,
    item_quantity,
    item_cost,
    item_price,
    item_amount,
    item_discount_amount,
    allocated_recalculate_through_datetime,
    allocated_recalculate_through_dim_date_key,
    allocated_month_starting_dim_date_key,
    dv_batch_id,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_inserted_date_time,
    dv_insert_user)
select fact_magento_invoice_item_key,
       invoice_item_id,
       fact_magento_invoice_key,
       fact_magento_order_key,
       fact_magento_order_item_key,
       fact_magento_payment_key,
       invoice_datetime,
       invoice_dim_date_key,
       isnull(shipping_amount,0),
       isnull(shipping_tax_amount,0),
       currency_code,
       isnull(invoice_amount,0),
       isnull(item_tax_amount,0),
       isnull(item_quantity,0),
       isnull(item_cost,0),
       isnull(item_price,0),
       isnull(item_amount,0),
       isnull(item_discount_amount,0),
       allocated_recalculate_through_datetime,
       allocated_recalculate_through_dim_date_key,
       allocated_month_starting_dim_date_key,
       dv_batch_id,
       dv_load_date_time,
       'dec 31, 9999',
       getdate(),
       suser_sname()
  from #etl_step_4

commit tran

end
