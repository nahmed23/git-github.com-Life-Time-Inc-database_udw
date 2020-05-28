CREATE PROC [dbo].[proc_fact_magento_payment] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-1)  from fact_magento_payment)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end



if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_magento_payment_key), location=user_db) as
    select d_magento_sales_payment_transaction.fact_magento_payment_key,
	     d_magento_sales_order_payment.base_amount_ordered base_amount_ordered,
	     d_magento_sales_order_payment.base_amount_paid base_amount_paid,
		 d_magento_sales_order_payment.base_amount_authorized base_amount_authorized,
		 d_magento_sales_order_payment.cc_type cc_type,
		 d_magento_sales_payment_transaction.created_dim_date_key created_dim_date_key,
		 d_magento_sales_payment_transaction.created_dim_time_key created_dim_time_key,
		 d_magento_sales_payment_transaction.fact_magento_sales_order_key fact_magento_sales_order_key,
		 d_magento_sales_payment_transaction.fact_magento_sales_order_payment_key fact_magento_sales_order_payment_key,
		 d_magento_sales_order_payment.entity_id sales_order_payment_id,
		 d_magento_sales_payment_transaction.transaction_id transaction_id,	
           case when isnull(d_magento_sales_payment_transaction.dv_load_date_time,'Jan 1, 1753') >= isnull(d_magento_sales_order_payment.dv_load_date_time,'Jan 1, 1753')
                then isnull(d_magento_sales_payment_transaction.dv_load_date_time,'Jan 1, 1753')					
           else isnull(d_magento_sales_order_payment.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
           convert(datetime, '99991231', 112) dv_load_end_date_time,
           case when isnull(d_magento_sales_payment_transaction.dv_batch_id,'-1') >= isnull(d_magento_sales_order_payment.dv_batch_id,'-1')
                then isnull(d_magento_sales_payment_transaction.dv_batch_id,'-1')					
           else isnull(d_magento_sales_order_payment.dv_batch_id,'-1') end dv_batch_id,
         d_magento_sales_order_payment.cc_last_4,
         case when d_magento_sales_order_payment.batch_number = 'null' then null else d_magento_sales_order_payment.batch_number end batch_number,
         case when d_magento_sales_order_payment.credit_tran_id = 'null' then null else d_magento_sales_order_payment.credit_tran_id end credit_tran_id,
         d_magento_sales_payment_transaction.txn_type
  from d_magento_sales_payment_transaction
  join d_magento_sales_order_payment
    on d_magento_sales_payment_transaction.fact_magento_sales_order_payment_key = d_magento_sales_order_payment.fact_magento_sales_order_payment_key 
 where d_magento_sales_payment_transaction.dv_batch_id >= @load_dv_batch_id
    or d_magento_sales_order_payment.dv_batch_id >= @load_dv_batch_id



/* delete and re-insert as a single transaction*/
/*   delete records from the table that exist*/
/*   insert records from records from current and missing batches*/

begin tran

  delete dbo.fact_magento_payment
   where fact_magento_payment_key in (select fact_magento_payment_key from #etl_step_1) 
   
  insert into fact_magento_payment
        ( fact_magento_payment_key
	    ,base_amount_ordered
	    ,base_amount_paid
	    ,base_amount_authorized
	    ,cc_type
	    ,created_dim_date_key
	    ,created_dim_time_key
	    ,fact_magento_sales_order_key
	    ,fact_magento_sales_order_payment_key
	    ,sales_order_payment_id
	    ,transaction_id
         ,dv_load_date_time
         ,dv_load_end_date_time
         ,dv_batch_id
         ,dv_inserted_date_time
         ,dv_insert_user,
         cc_last_4,
         batch_number,
         credit_tran_id,
         txn_type)
  select fact_magento_payment_key
	    ,base_amount_ordered
	    ,base_amount_paid
	    ,base_amount_authorized
	    ,cc_type
	    ,created_dim_date_key
	    ,created_dim_time_key
	    ,fact_magento_sales_order_key
	    ,fact_magento_sales_order_payment_key
	    ,sales_order_payment_id
	    ,transaction_id
         ,dv_load_date_time
	    ,convert(datetime, '99991231', 112)
        ,dv_batch_id
        ,getdate() 
        ,suser_sname(),
         cc_last_4,
         batch_number,
         credit_tran_id,
         txn_type
    from #etl_step_1
 
commit tran

end

