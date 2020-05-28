CREATE PROC [dbo].[proc_fact_healthcheckusa_allocated_transaction_item] @dv_batch_id [varchar](500),@job_group [varchar](500),@begin_extract_date_time [datetime] AS
begin
set xact_abort on
set nocount on


/* declare @dv_batch_id bigint = -1*/
/* declare @job_group varchar(500) = @job_group*/
/* declare @begin_extract_date_time datetime = '1753-11-14 01:59:13'*/

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_healthcheckusa_allocated_transaction_item)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end
declare @tran_rpt_month_starting_date datetime = (select dateadd(mm,datediff(month,0,begin_extract_date_time),0)
                                                    from dv_job_status
                                                   where job_name = 'wf_bv_fact_healthcheckusa_allocated_transaction_item')


if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_healthcheckusa_allocated_transaction_item_key), location=user_db) as
select d_healthcheckusa_transactions.d_healthcheckusa_transactions_bk_hash as fact_healthcheckusa_allocated_transaction_item_key,
       d_healthcheckusa_transactions.order_number,
       d_healthcheckusa_transactions.product_sku,
       d_healthcheckusa_transactions.item_amount,
       d_healthcheckusa_transactions.item_discount,
       d_healthcheckusa_transactions.dim_employee_key sales_dim_employee_key,
       d_healthcheckusa_transactions.gl_club_id,
       d_healthcheckusa_transactions.order_for_employee_flag,
       d_healthcheckusa_transactions.quantity,
       d_healthcheckusa_transactions.transaction_date,
       d_healthcheckusa_transactions.transaction_type,
       d_healthcheckusa_transactions.transaction_post_dim_date_key,
	   d_healthcheckusa_transactions.transaction_post_dim_time_key,  /*--added for UDW-10020-----*/

       case when d_healthcheckusa_transactions.d_healthcheckusa_transactions_bk_hash in ('-997', '-998', '-999') then d_healthcheckusa_transactions.d_healthcheckusa_transactions_bk_hash
            when dim_club.dim_club_key is null then '-998'
            else dim_club.dim_club_key end as dim_club_key,
       isnull(dim_club.local_currency_code,'USD') as local_currency_code,
       dim_club.club_id as club_id,
       d_healthcheckusa_transactions.dim_healthcheckusa_product_key,
       'USD' as original_currency_code,
       'USD' as usd_currency_code,
       (d_healthcheckusa_transactions.item_amount * d_healthcheckusa_transactions.quantity) as sales_amount_gross,
       (d_healthcheckusa_transactions.item_discount * d_healthcheckusa_transactions.quantity) as discount_amount,
       ((d_healthcheckusa_transactions.item_amount  -  d_healthcheckusa_transactions.item_discount) * d_healthcheckusa_transactions.quantity) as sales_amount,
       case when d_healthcheckusa_transactions.transaction_type = 'Sale' then d_healthcheckusa_transactions.quantity else  (-1 * d_healthcheckusa_transactions.quantity) end as sales_quantity,
       case when d_healthcheckusa_transactions.transaction_type = 'Refund' then 'Y' else 'N' end as refund_flag,
       convert(varchar, getdate(), 112)  as udw_inserted_dim_date_key,
       d_healthcheckusa_transactions.dv_batch_id,
       d_healthcheckusa_transactions.dv_load_date_time,
       d_healthcheckusa_transactions.allocated_recalculate_through_dim_date_key,
       d_healthcheckusa_transactions.allocated_recalculate_through_datetime,
       d_healthcheckusa_transactions.allocated_month_starting_dim_date_key,
	   case when dim_club.club_id <> 13 and dim_club.club_close_dim_date_key >= d_healthcheckusa_transactions.transaction_post_dim_date_key then dim_club.dim_club_key
            when d_healthcheckusa_transactions.dim_employee_key not in ('-999','-998','-997') and employee_dim_club.club_id <> 13 and employee_dim_club.club_close_dim_date_key >= d_healthcheckusa_transactions.transaction_post_dim_date_key then employee_dim_club.dim_club_key
            else corporate_club.dim_club_key
        end transaction_reporting_dim_club_key /*also allocated_dim_club_key, no member so no that clause is unnecessary*/
  from d_healthcheckusa_transactions
  left join dim_club
    on dim_club.gl_club_id = d_healthcheckusa_transactions.gl_club_id
  left join dim_employee
    on d_healthcheckusa_transactions.dim_employee_key = dim_employee.dim_employee_key
  left join dim_club employee_dim_club
    on dim_employee.dim_club_key = employee_dim_club.dim_club_key
  join dim_club corporate_club 
    on corporate_club.club_id = 13 /*effectively a cross join, adds the column as a "constant" to every record*/
 where d_healthcheckusa_transactions.dv_batch_id >= @load_dv_batch_id
    or ((dim_club.club_id = 13 or dim_club.dim_club_key is null )
        and d_healthcheckusa_transactions.transaction_date >= @tran_rpt_month_starting_date)

if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(fact_healthcheckusa_allocated_transaction_item_key), location=user_db) as
select #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key,
       #etl_step_1.order_number,
       #etl_step_1.product_sku,
       #etl_step_1.item_amount,
       #etl_step_1.item_discount,
       #etl_step_1.sales_dim_employee_key,
       #etl_step_1.gl_club_id,
       #etl_step_1.order_for_employee_flag,
       #etl_step_1.quantity,
       #etl_step_1.transaction_date,
       #etl_step_1.transaction_type,
       #etl_step_1.transaction_post_dim_date_key,
	   #etl_step_1.transaction_post_dim_time_key,  /*--added for UDW-10020-----*/
       #etl_step_1.dim_club_key,
       #etl_step_1.club_id,
       #etl_step_1.dim_healthcheckusa_product_key,
       #etl_step_1.original_currency_code,
       #etl_step_1.usd_currency_code,
       #etl_step_1.local_currency_code,
       #etl_step_1.sales_amount_gross,
       #etl_step_1.discount_amount,
       case when #etl_step_1.refund_flag = 'Y' then abs(#etl_step_1.sales_amount) * -1
            else #etl_step_1.sales_amount end as sales_amount,
       #etl_step_1.sales_quantity,
       #etl_step_1.refund_flag,
       #etl_step_1.udw_inserted_dim_date_key,
       case when #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key in ('-997', '-998', '-999') then #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key
            when #etl_step_1.transaction_post_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_1.transaction_post_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
        end as usd_monthly_average_dim_exchange_rate_key,
       case when #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key in ('-997', '-998', '-999') then #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                 'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
        end as usd_dim_plan_exchange_rate_key,
       /*case when #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key in ('-997', '-998', '-999') then #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key*/
       /*     when #etl_step_1.club_id != 13 and #etl_step_1.club_close_dim_date_key = '-998' and #etl_step_1.club_id is null then #etl_step_1.home_dim_club_key*/
       /*     else #etl_step_1.dim_club_key*/
       /* end as transaction_reporting_dim_club_key,*/
       case when #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key in ('-997', '-998', '-999') then #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key
            when dim_date.month_ending_dim_date_key in ('-997', '-998', '-999') then #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,dim_date.month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
        end transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
       case when #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key in ('-997', '-998', '-999') then #etl_step_1.fact_healthcheckusa_allocated_transaction_item_key
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
        end transaction_reporting_local_currency_dim_plan_exchange_rate_key,
       #etl_step_1.dv_batch_id,
       #etl_step_1.dv_load_date_time,
       allocated_recalculate_through_dim_date_key,
       allocated_recalculate_through_datetime,
       allocated_month_starting_dim_date_key,
       #etl_step_1.transaction_reporting_dim_club_key
  from #etl_step_1
  join dim_date
    on #etl_step_1.transaction_post_dim_date_key = dim_date.dim_date_key
  left join dim_club
    on #etl_step_1.transaction_reporting_dim_club_key = dim_club.dim_club_key

  


/*        Delete records from the table that exist*/
/*        Insert records from temp table for current and missing batches*/

begin tran

delete dbo.fact_healthcheckusa_allocated_transaction_item
    where fact_healthcheckusa_allocated_transaction_item_key in (select fact_healthcheckusa_allocated_transaction_item_key from dbo.#etl_step_2)

        insert into fact_healthcheckusa_allocated_transaction_item
          (      fact_healthcheckusa_allocated_transaction_item_key,
                 order_number,
                 product_sku,
                 sales_amount,
                 discount_amount,
                 sales_dim_employee_key,
                 gl_club_id,
                 order_for_employee_flag,
                 sales_quantity,
                 transaction_date,
                 transaction_type,
                 transaction_post_dim_date_key,
				 transaction_post_dim_time_key, /*--added for UDW-10020-----*/
                 dim_club_key,
                 dim_healthcheckusa_product_key,
                 refund_flag,
				 udw_inserted_dim_date_key,
                 original_currency_code,
                 usd_monthly_average_dim_exchange_rate_key,
                 usd_dim_plan_exchange_rate_key,
                 transaction_reporting_dim_club_key,
                 transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
                 transaction_reporting_local_currency_dim_plan_exchange_rate_key,
                 dv_batch_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_inserted_date_time,
                 dv_insert_user,
                 allocated_recalculate_through_dim_date_key,
                 allocated_recalculate_through_datetime,
                 allocated_month_starting_dim_date_key,
				 allocated_dim_club_key)
                select
                 fact_healthcheckusa_allocated_transaction_item_key,
                 order_number,
                 product_sku,
                 sales_amount,
                 discount_amount,
                 sales_dim_employee_key,
                 gl_club_id,
                 order_for_employee_flag,
                 sales_quantity,
                 transaction_date,
                 transaction_type,
                 transaction_post_dim_date_key,
				 transaction_post_dim_time_key, /*--added for UDW-10020-----*/
                 dim_club_key,
                 dim_healthcheckusa_product_key,
                 refund_flag,
				 udw_inserted_dim_date_key,
                 original_currency_code,
                 usd_monthly_average_dim_exchange_rate_key,
                 usd_dim_plan_exchange_rate_key,
                 transaction_reporting_dim_club_key,
                 transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
                 transaction_reporting_local_currency_dim_plan_exchange_rate_key,
                 dv_batch_id,
                 dv_load_date_time,
                 'dec 31, 9999',
                 getdate(),
                 suser_sname(),
                 allocated_recalculate_through_dim_date_key,
                 allocated_recalculate_through_datetime,
                 allocated_month_starting_dim_date_key,
				 transaction_reporting_dim_club_key allocated_dim_club_key
        from #etl_step_2

    commit tran


/*added a WHERE clause to #etl_step_1 to include the below set of transactions */
/*A*/


/*/* Find the dv_batch_id of the most recent completed job (excluding the current batch)*/*/
/*      select @max_dv_batch_id = max(dv_batch_id)*/
/*       from dv_job_status_history*/
/*      where job_name = 'wf_' + @job_group + '_master_begin'*/
/*        and job_status = 'Complete'*/


/*if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3*/
/*create table dbo.#etl_step_3 with(distribution=hash(fact_healthcheckusa_allocated_transaction_item_key), location=user_db) as*/
/*     select fact_healthcheckusa_allocated_transaction_item.fact_healthcheckusa_allocated_transaction_item_key,*/
/*       /*fact_healthcheckusa_allocated_transaction_item.dim_mms_member_key,*/*/
/*       fact_healthcheckusa_allocated_transaction_item.sales_dim_employee_key,*/
/*       /*membership.dim_mms_membership_key as dim_mms_membership_key,*/*/
/*	   club.local_currency_code as local_currency_code,*/
/*	   allocated_dim_club_key as transaction_reporting_dim_club_key,*/
/*       fact_healthcheckusa_allocated_transaction_item.original_currency_code,*/
/*       fact_healthcheckusa_allocated_transaction_item.transaction_post_dim_date_key*/
/*   from fact_healthcheckusa_allocated_transaction_item*/
/*  join dim_club club*/
/*    on fact_healthcheckusa_allocated_transaction_item.dim_club_key = club.dim_club_key*/
/*  join dim_employee dim_employee*/
/*    on dim_employee.dim_employee_key = fact_healthcheckusa_allocated_transaction_item.sales_dim_employee_key*/
/*  where (club.club_id = 13 or fact_healthcheckusa_allocated_transaction_item.dim_club_key = '-998')*/
/*   and dim_employee.dv_batch_id >= @max_dv_batch_id*/
/*   and fact_healthcheckusa_allocated_transaction_item.transaction_post_dim_date_key >= convert(varchar, dateadd(day, 1, eomonth(@begin_extract_date_time, -1)), 112)*/

/*if object_id('tempdb..#etl_transaction_reporting') is not null drop table #etl_transaction_reporting*/
/*     create table dbo.#etl_transaction_reporting with(distribution=hash(fact_healthcheckusa_allocated_transaction_item_key), location=user_db) as*/
/*     select #etl_step_3.fact_healthcheckusa_allocated_transaction_item_key,*/
/*            #etl_step_3.transaction_reporting_dim_club_key,*/
/*            convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,dim_date.month_ending_dim_date_key),'z#@$k%&P')+*/
/*                                              'P%#&z$@k'+isnull(#etl_step_3.original_currency_code,'z#@$k%&P')+*/
/*                                              'P%#&z$@k'+isnull(#etl_step_3.local_currency_code,'z#@$k%&P')+*/
/*                                              'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)*/
/*                                                     transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,*/
/*            convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_3.original_currency_code,'z#@$k%&P')+*/
/*                                              'P%#&z$@k'+isnull(#etl_step_3.local_currency_code,'z#@$k%&P'))),2)*/
/*                                                    transaction_reporting_local_currency_dim_plan_exchange_rate_key*/
/*         from  #etl_step_3*/
/*         join dim_date*/
/*         on #etl_step_3.transaction_post_dim_date_key = dim_date.dim_date_key*/
/*         where #etl_step_3.transaction_post_dim_date_key >= convert(varchar, dateadd(day, 1, eomonth(@begin_extract_date_time, -1)), 112)*/




/*      update fact_healthcheckusa_allocated_transaction_item*/
/*      set transaction_reporting_dim_club_key =  #etl_transaction_reporting.transaction_reporting_dim_club_key,*/
/*      transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key = #etl_transaction_reporting.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,*/
/*      transaction_reporting_local_currency_dim_plan_exchange_rate_key=#etl_transaction_reporting.transaction_reporting_local_currency_dim_plan_exchange_rate_key*/
/*      from  #etl_transaction_reporting*/
/*      where fact_healthcheckusa_allocated_transaction_item.fact_healthcheckusa_allocated_transaction_item_key=#etl_transaction_reporting.fact_healthcheckusa_allocated_transaction_item_key*/

/*	 end*/

/* exec proc_fact_healthcheckusa_allocated_transaction_item '-1','dv_main_azure','1753-01-01 00:00:00'*/

end
