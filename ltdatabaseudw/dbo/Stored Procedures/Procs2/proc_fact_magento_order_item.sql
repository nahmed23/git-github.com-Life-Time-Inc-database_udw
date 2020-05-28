CREATE PROC [dbo].[proc_fact_magento_order_item] @dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_magento_order_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

DECLARE @month_starting_dim_date_key varchar(8)
Set @month_starting_dim_date_key = 
    (SELECT case when @dv_batch_id < cast(1 as bigint) then '17630101' else convert(varchar,isnull(DATEADD(mm,DATEDIFF(mm,0,begin_extract_date_time),0),'jan 1, 1763'),112) end
       FROM dv_job_status
      WHERE job_name = 'wf_bv_fact_magento_order_item')
      
if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_magento_order_item_key), location=user_db) as  
select d_magento_sales_order.bk_hash fact_magento_order_key,
       d_magento_sales_order.order_id, 
       d_magento_sales_order.order_number, 
       d_magento_sales_order.dim_magento_customer_key,
       d_magento_sales_order.dim_mms_member_key,
       d_workday_employee_trainer_bk_hash dim_employee_key,
       d_magento_sales_order.status order_status, 
       d_magento_sales_order.created_at order_datetime, 
       convert(varchar(8),d_magento_sales_order.created_at,112) order_dim_date_key, 
       d_magento_sales_order.sales_order_base_shipping_amount order_shipping_amount,
       d_magento_sales_order_item.bk_hash fact_magento_order_item_key,
       d_magento_sales_order_item.order_item_id order_item_id, 
       d_magento_sales_order_item.dim_magento_product_key,
       --d_magento_sales_order_item.product_id, 
       isnull(parent_order_item.qty_ordered,d_magento_sales_order_item.qty_ordered) item_quantity,
       isnull(parent_order_item.base_price , d_magento_sales_order_item.base_price) item_base_price,
       isnull(parent_order_item.base_discount_amount, d_magento_sales_order_item.base_discount_amount) item_discount_amount, 
       isnull(parent_order_item.base_tax_before_discount, d_magento_sales_order_item.base_tax_before_discount) item_tax_amount, 
       isnull(parent_order_item.base_row_total, d_magento_sales_order_item.base_row_total) item_total_amount, 
       isnull(parent_order_item.sales_order_item_lt_bucks_redeemed, d_magento_sales_order_item.sales_order_item_lt_bucks_redeemed) item_bucks_amount, 
       isnull(parent_order_item.sales_order_item_lt_bucks_refunded, d_magento_sales_order_item.sales_order_item_lt_bucks_refunded) item_refund_bucks_amount, 
       d_magento_sales_order_item.dim_club_key,
       d_magento_sales_order.sales_order_base_currency_code order_currency_code,
       case when d_magento_sales_order.dv_batch_id >= isnull(d_magento_sales_order_item.dv_batch_id,-1)
                 then d_magento_sales_order.dv_batch_id
            else isnull(d_magento_sales_order_item.dv_batch_id,-1) end dv_batch_id,
       case when d_magento_sales_order.dv_load_date_time >= isnull(d_magento_sales_order_item.dv_load_date_time,'Jan 1, 1753')
                 then d_magento_sales_order.dv_load_date_time
            else isnull(d_magento_sales_order_item.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
       d_magento_sales_order_item.sales_order_item_product_type,
       d_magento_sales_order_item.parent_item_id,
       d_magento_sales_order.d_magento_sales_order_address_bk_hash,
       isnull(parent_order_item.bucks_per_quantity,d_magento_sales_order_item.bucks_per_quantity) item_bucks_per_quantity,
       isnull(parent_order_item.refund_bucks_per_quantity,d_magento_sales_order_item.refund_bucks_per_quantity) item_refund_bucks_per_quantity,
       d_magento_sales_order_item.parent_fact_magento_order_item_key
from d_magento_sales_order d_magento_sales_order
join d_magento_sales_order_item d_magento_sales_order_item 
  on d_magento_sales_order.bk_hash = d_magento_sales_order_item.d_magento_sales_order_bk_hash
left join d_magento_sales_order_item parent_order_item
  on d_magento_sales_order_item.parent_fact_magento_order_item_key = parent_order_item.bk_hash
 and parent_order_item.sales_order_item_product_type = 'configurable'
where d_magento_sales_order_item.sales_order_item_product_type != 'configurable'
  and (d_magento_sales_order.dv_batch_id >= @load_dv_batch_id
        or d_magento_sales_order_item.dv_batch_id >= @load_dv_batch_id)

--where d_magento_sales_order_item.sales_order_item_product_type <> 'configurable' 
--  and (d_magento_sales_order.dv_batch_id >= @load_dv_batch_id
--        or d_magento_sales_order_item.dv_batch_id >= @load_dv_batch_id)

--remove bundle child records
delete
from #etl_step_1
where parent_item_id in (select order_item_id from #etl_step_1 where sales_order_item_product_type = 'bundle')

--shipping_state
if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_magento_order_item_key), location=user_db) as  
select #etl_step_1.*,
       isnull(isnull(isnull(r1.abbreviation,r2.abbreviation),r3.abbreviation),r4.abbreviation) shipping_state
from #etl_step_1
left join d_magento_sales_order_address sa on #etl_step_1.d_magento_sales_order_address_bk_hash = sa.bk_hash
left join r_mms_val_state r1 on sa.region = r1.description and r1.dv_load_end_date_time = 'dec 31, 9999'
left join r_mms_val_state r2 on sa.region = r2.abbreviation and r2.dv_load_end_date_time = 'dec 31, 9999'
left join d_magento_sales_order_address ba on #etl_step_1.d_magento_sales_order_address_bk_hash= ba.bk_hash
left join r_mms_val_state r3 on ba.region = r3.description and r3.dv_load_end_date_time = 'dec 31, 9999'
left join r_mms_val_state r4 on ba.region = r4.abbreviation and r4.dv_load_end_date_time = 'dec 31, 9999'

begin tran
     
delete dbo.fact_magento_order_item
where fact_magento_order_item_key in (select fact_magento_order_item_key from dbo.#etl_step_2) 
                 
insert into fact_magento_order_item (
    fact_magento_order_item_key,
    order_item_id,
    fact_magento_order_key,
    order_id,
    order_number,
    order_datetime,
    order_dim_date_key,
    dim_magento_customer_key,
    dim_mms_member_key,
    dim_employee_key,
    order_status,
    order_shipping_amount,
    dim_magento_product_key,
    parent_fact_magento_order_item_key,
    --product_id,
    item_quantity,
    item_base_price,
    item_discount_amount,
    item_tax_amount,
    item_total_amount,
    item_bucks_amount,
    item_refund_bucks_amount,
    item_bucks_per_quantity,
    item_refund_bucks_per_quantity,
    dim_club_key,
    order_currency_code,
    shipping_state,
    dv_batch_id,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_inserted_date_time,
    dv_insert_user)
select fact_magento_order_item_key,
       order_item_id,
       fact_magento_order_key,
       order_id,
       order_number,
       order_datetime,
       order_dim_date_key,
       dim_magento_customer_key,
       dim_mms_member_key,
       dim_employee_key,
       order_status,
       isnull(order_shipping_amount,0),
       dim_magento_product_key,
       parent_fact_magento_order_item_key,
       --product_id,
       isnull(item_quantity,0),
       isnull(item_base_price,0),
       isnull(item_discount_amount,0),
       isnull(item_tax_amount,0),
       isnull(item_total_amount,0),
       isnull(item_bucks_amount,0),
       isnull(item_refund_bucks_amount,0),
       isnull(item_bucks_per_quantity,0),
       isnull(item_refund_bucks_per_quantity,0),
       dim_club_key,
       order_currency_code,
       shipping_state,
       dv_batch_id,
       dv_load_date_time,
       'dec 31, 9999',
       getdate(),
       suser_sname()
  from #etl_step_2

commit tran

end
