CREATE PROC [dbo].[proc_fact_cafe_member_spend] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_cafe_member_spend)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

	
if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(dim_mms_member_key), location=user_db) as
SELECT  fact_cafe_sales_transaction_item.dim_mms_member_key,
       SUM(CASE WHEN fact_cafe_sales_transaction_item.order_close_dim_date_key >=  CONVERT(VARCHAR,DATEADD(YEAR, - 1, GETDATE()),112) 
             THEN fact_cafe_payment.tender_amount ELSE 0  
		   END) AS last_12_month_spend_amount,
		SUM(fact_cafe_payment.tender_amount)  AS total_spend_amount,
	    max(case when isnull(fact_cafe_sales_transaction_item.dv_load_date_time,'Jan 1, 1753') >= isnull(fact_cafe_payment.dv_load_date_time,'Jan 1, 1753') 
	             then isnull(fact_cafe_sales_transaction_item.dv_load_date_time,'Jan 1, 1753')     			
            else isnull(fact_cafe_payment.dv_load_date_time,'Jan 1, 1753') end) dv_load_date_time,
	    convert(datetime, '99991231', 112) dv_load_end_date_time,
	    max(case when isnull(fact_cafe_sales_transaction_item.dv_batch_id,'-1') >= isnull(fact_cafe_payment.dv_batch_id,'-1') 
	           then isnull(fact_cafe_sales_transaction_item.dv_batch_id,'-1')     			
            else isnull(fact_cafe_payment.dv_batch_id,'-1') end) dv_batch_id
 FROM fact_cafe_sales_transaction_item fact_cafe_sales_transaction_item
 LEFT JOIN fact_cafe_payment fact_cafe_payment 
       ON fact_cafe_sales_transaction_item.dim_mms_member_key=fact_cafe_payment.dim_mms_member_key
 AND fact_cafe_payment.order_hdr_id = fact_cafe_sales_transaction_item.order_hdr_id
      AND  fact_cafe_sales_transaction_item.dim_mms_member_key <> '-998'
      AND   fact_cafe_sales_transaction_item.order_close_dim_date_key < convert(varchar(500),getdate(),112)
      AND  (fact_cafe_payment.dv_batch_id >= @load_dv_batch_id
            or fact_cafe_sales_transaction_item.dv_batch_id >= @load_dv_batch_id)
group by 
        fact_cafe_sales_transaction_item.dim_mms_member_key


if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution=hash(dim_mms_member_key), location=user_db) as
SELECT  etl_step1.dim_mms_member_key,
                d_mms_member.dim_mms_membership_key,
                etl_step1.last_12_month_spend_amount,
	            etl_step1.total_spend_amount,
	            etl_step1.dv_load_date_time,
	            etl_step1.dv_load_end_date_time,
	            etl_step1.dv_batch_id	
 FROM dbo.#etl_step1 etl_step1
 left JOIN  d_mms_member d_mms_member
      ON d_mms_member.dim_mms_member_key = etl_step1.dim_mms_member_key

begin tran

  delete from dbo.fact_cafe_member_spend
   where dim_mms_member_key in (select dim_mms_member_key from dbo.#etl_step2) 
   
   insert into fact_cafe_member_spend
        (dim_mms_member_key,
		 dim_mms_membership_key,
	     last_12_month_spend_amount,
	     total_spend_amount,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user)
  select dim_mms_member_key,
         dim_mms_membership_key,
	     last_12_month_spend_amount,
	     total_spend_amount,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate() ,
         suser_sname()
    from #etl_step2
 
commit tran

end

