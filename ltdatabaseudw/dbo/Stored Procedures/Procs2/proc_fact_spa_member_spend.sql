CREATE PROC [dbo].[proc_fact_spa_member_spend] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_spa_member_spend)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(dim_mms_member_key), location=user_db) as
SELECT dim_spabiz_customer.dim_mms_member_key,
	   dim_spabiz_customer.dim_mms_membership_key,
       max(case when isnull(d_spabiz_ticket_data.dv_load_date_time,'Jan 1, 1753') >= isnull(dim_spabiz_service.dv_load_date_time,'Jan 1, 1753')
	         and isnull(d_spabiz_ticket_data.dv_load_date_time,'Jan 1, 1753') >= isnull(dim_spabiz_customer.dv_load_date_time,'Jan 1, 1753')
            then isnull(d_spabiz_ticket_data.dv_load_date_time,'Jan 1, 1753')
            when isnull(dim_spabiz_service.dv_load_date_time,'Jan 1, 1753') >= isnull(dim_spabiz_customer.dv_load_date_time,'Jan 1, 1753')
	        then isnull(dim_spabiz_service.dv_load_date_time,'Jan 1, 1753')
           else isnull(dim_spabiz_customer.dv_load_date_time,'Jan 1, 1753') end) dv_load_date_time,
	   convert(datetime, '99991231', 112) dv_load_end_date_time,
	   max(case when isnull(d_spabiz_ticket_data.dv_batch_id,'-1') >= isnull(dim_spabiz_service.dv_batch_id,'-1')
	         and isnull(d_spabiz_ticket_data.dv_batch_id,'-1') >= isnull(dim_spabiz_customer.dv_batch_id,'-1')
            then isnull(d_spabiz_ticket_data.dv_batch_id,'-1')
            when isnull(dim_spabiz_service.dv_batch_id,'-1') >= isnull(dim_spabiz_customer.dv_batch_id,'-1')
	        then isnull(dim_spabiz_service.dv_batch_id,'-1')
           else isnull(dim_spabiz_customer.dv_batch_id,'-1') end) dv_batch_id
FROM d_spabiz_ticket_data
LEFT JOIN dim_spabiz_service ON d_spabiz_ticket_data.dim_spabiz_service_key = dim_spabiz_service.dim_spabiz_service_key
LEFT JOIN dim_spabiz_customer ON d_spabiz_ticket_data.dim_spabiz_customer_key=dim_spabiz_customer.dim_spabiz_customer_key
WHERE d_spabiz_ticket_data.status_id = 1 AND d_spabiz_ticket_data.service_amount > 0
AND dim_spabiz_customer.dim_mms_member_key <> '-998'
 and  (d_spabiz_ticket_data.dv_batch_id >= @load_dv_batch_id
    or dim_spabiz_service.dv_batch_id >= @load_dv_batch_id
	or dim_spabiz_customer.dv_batch_id >= @load_dv_batch_id)
group by dim_spabiz_customer.dim_mms_member_key,
	   dim_spabiz_customer.dim_mms_membership_key



if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution=hash(dim_mms_member_key), location=user_db) as
SELECT dim_spabiz_customer.dim_mms_member_key,
	   dim_spabiz_customer.dim_mms_membership_key,
	   SUM(CASE WHEN d_spabiz_ticket_data.ticket_item_date_time >= DATEADD(YEAR, - 1, GETDATE())
	            THEN d_spabiz_ticket_data.service_amount ELSE 0
		   END) AS last_12_month_spend_amount,
			SUM(d_spabiz_ticket_data.service_amount) AS total_spend_amount,
	etl_step1.dv_load_date_time,
	etl_step1.dv_load_end_date_time,
	etl_step1.dv_batch_id
FROM dbo.#etl_step1 etl_step1
JOIN dim_spabiz_customer ON etl_step1.dim_mms_member_key=dim_spabiz_customer.dim_mms_member_key
JOIN d_spabiz_ticket_data ON d_spabiz_ticket_data.dim_spabiz_customer_key=dim_spabiz_customer.dim_spabiz_customer_key
LEFT JOIN dim_spabiz_service ON d_spabiz_ticket_data.dim_spabiz_service_key = dim_spabiz_service.dim_spabiz_service_key
WHERE d_spabiz_ticket_data.status_id = 1 AND d_spabiz_ticket_data.service_amount > 0
AND dim_spabiz_customer.dim_mms_member_key <> '-998'
GROUP BY dim_spabiz_customer.dim_mms_member_key
	,dim_spabiz_customer.dim_mms_membership_key
	,etl_step1.dv_load_date_time
	,etl_step1.dv_load_end_date_time
	,etl_step1.dv_batch_id


begin tran

  delete from dbo.fact_spa_member_spend
   where dim_mms_member_key in (select dim_mms_member_key from dbo.#etl_step2)

   insert into fact_spa_member_spend
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

