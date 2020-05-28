CREATE PROC [dbo].[proc_fact_magento_shipment_item] @dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_magento_shipment_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_magento_shipment_item_key), location=user_db) as  
select d_magento_sales_shipment_item.bk_hash fact_magento_shipment_item_key,
       d_magento_sales_shipment_item.sales_shipment_item_id shipment_item_id,
       d_magento_sales_shipment_item.fact_magento_shipment_key fact_magento_shipment_key,
       d_magento_sales_shipment.fact_magento_order_key fact_magento_order_key,
       d_magento_sales_shipment_item.fact_magento_order_item_key fact_magento_order_item_key,
       d_magento_sales_shipment_item.sales_shipment_item_qty shipment_item_quantity,
       d_magento_sales_shipment_item.sales_shipment_item_price shipment_item_price,
	   d_magento_sales_shipment.created_at shipment_datetime,
       d_magento_sales_shipment.created_at_dim_time_key shipment_dim_date_key, /* the d-table column should be created_dim_time_key	   */
       d_magento_sales_shipment.shipping_address_id shipping_address_id,
       d_magento_sales_shipment.billing_address_id billing_address_id,
	   d_magento_sales_shipment.shipment_status shipment_status,	   
       case when d_magento_sales_shipment.dv_batch_id >= isnull(d_magento_sales_shipment_item.dv_batch_id,-1)
                 then d_magento_sales_shipment.dv_batch_id
            else isnull(d_magento_sales_shipment_item.dv_batch_id,-1) end dv_batch_id,
       case when d_magento_sales_shipment.dv_load_date_time >= isnull(d_magento_sales_shipment_item.dv_load_date_time,'Jan 1, 1753')
                 then d_magento_sales_shipment.dv_load_date_time
            else isnull(d_magento_sales_shipment_item.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time
  from d_magento_sales_shipment d_magento_sales_shipment
  join d_magento_sales_shipment_item d_magento_sales_shipment_item on d_magento_sales_shipment.bk_hash = d_magento_sales_shipment_item.d_magento_sales_shipment_bk_hash
 where d_magento_sales_shipment.dv_batch_id >= @load_dv_batch_id
    or d_magento_sales_shipment_item.dv_batch_id >= @load_dv_batch_id



begin tran
     
delete dbo.fact_magento_shipment_item
where fact_magento_shipment_item_key in (select fact_magento_shipment_item_key from dbo.#etl_step_1) 
                 
insert into fact_magento_shipment_item (
    fact_magento_shipment_item_key,
    shipment_item_id,
    fact_magento_shipment_key,
    fact_magento_order_key,
    fact_magento_order_item_key,
    shipment_item_quantity,
	shipment_item_price,
	shipment_datetime,
	shipment_dim_date_key,
	shipping_address_id,
	billing_address_id,
	shipment_status,	
    dv_batch_id,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_inserted_date_time,
    dv_insert_user)
select fact_magento_shipment_item_key,
    shipment_item_id,
    fact_magento_shipment_key,
    fact_magento_order_key,
    fact_magento_order_item_key,
    shipment_item_quantity,
	shipment_item_price,
	shipment_datetime,
	shipment_dim_date_key,
	shipping_address_id,
	billing_address_id,
	shipment_status,	
    dv_batch_id,
    dv_load_date_time,
    'dec 31, 9999',
    getdate(),
    suser_sname()
  from #etl_step_1

commit tran

end
