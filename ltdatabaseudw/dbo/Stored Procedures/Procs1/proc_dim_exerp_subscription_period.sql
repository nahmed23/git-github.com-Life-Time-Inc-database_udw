CREATE PROC [dbo].[proc_dim_exerp_subscription_period] @dv_batch_id [varchar](500) AS
begin

    set xact_abort on
    set nocount on

    declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_exerp_subscription_period)
    declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @dv_batch_id then @max_dv_batch_id else @dv_batch_id end

    /* determine which dim_exerp_subsription_period_keys are associated with changes in the batch(es)*/
    if object_id('tempdb..#key_list') is not null 
        drop table #key_list
    create table dbo.#key_list with ( distribution = hash(dim_exerp_subscription_period_key),location = user_db) as
    SELECT d_exerp_subscription_period.dim_exerp_subscription_period_key
    FROM d_exerp_subscription_period 
    left join d_exerp_subscription
      on d_exerp_subscription_period.dim_exerp_subscription_key =d_exerp_subscription.dim_exerp_subscription_key
    left join d_exerp_sale_log 
      on d_exerp_subscription_period.fact_exerp_transaction_log_key = d_exerp_sale_log.fact_exerp_transaction_log_key  
    left join d_exerp_access_privilege_usage
      on d_exerp_subscription_period.dim_exerp_subscription_key = d_exerp_access_privilege_usage.source_dim_exerp_subscription_key 
     and d_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION'  
     and d_exerp_access_privilege_usage.target_type = 'PARTICIPATION'
    left join d_exerp_participation 
      on d_exerp_access_privilege_usage.target_fact_exerp_participation_key = d_exerp_participation.fact_exerp_participation_key 
    left join d_exerp_booking
      on d_exerp_participation.dim_exerp_booking_key = d_exerp_booking.bk_hash
     and d_exerp_booking.start_dim_date_key >= d_exerp_subscription_period.from_dim_date_key
     and d_exerp_booking.start_dim_date_key <= d_exerp_subscription_period.to_dim_date_key 
    left join d_mms_tran_item
      on d_mms_tran_item.external_item_id = d_exerp_sale_log.external_id
     and d_mms_tran_item.transaction_source = 'Exerp'
    where (d_exerp_subscription_period.dv_batch_id >= @load_dv_batch_id 
           or d_exerp_subscription.dv_batch_id >= @load_dv_batch_id 
           or d_exerp_sale_log.dv_batch_id >= @load_dv_batch_id  
           or d_exerp_access_privilege_usage.dv_batch_id >= @load_dv_batch_id 
           or d_exerp_participation.dv_batch_id >= @load_dv_batch_id 
           or d_exerp_booking.dv_batch_id >= @load_dv_batch_id 
           or d_mms_tran_item.dv_batch_id >= @load_dv_batch_id)
    group by d_exerp_subscription_period.dim_exerp_subscription_period_key


    /* gather data related to subscription periods that isn't booking related. */
    IF object_id('tempdb..#etl_step1') IS NOT NULL
        DROP TABLE #etl_step1
    CREATE TABLE dbo.#etl_step1 WITH ( distribution = HASH (dim_exerp_subscription_period_key),location = user_db) AS
    SELECT d_exerp_subscription_period.dim_exerp_subscription_period_key,
           d_exerp_subscription_period.subscription_period_id,
           d_exerp_subscription_period.dim_exerp_subscription_key,
           d_exerp_subscription_period.fact_exerp_transaction_log_key,
           d_exerp_subscription_period.dim_club_key,
           d_exerp_subscription_period.subscription_period_state,
           d_exerp_subscription_period.subscription_period_type,
           isnull(exerp_managed_club_department.migration_dim_date_key,d_exerp_subscription_period.from_dim_date_key) from_dim_date_key,
           d_exerp_subscription_period.to_dim_date_key,
           d_exerp_subscription.dim_mms_member_key,
           d_exerp_subscription.dim_exerp_product_key,
           d_exerp_sale_log.net_amount as net_amount,
           d_mms_tran_item.item_lt_bucks_amount as lt_bucks_amount,
		   isnull(d_mms_mms_tran.post_dim_date_key,'-998') billing_dim_date_key,
           case 
            when 
                 isnull(d_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753') 
             and isnull(d_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753') >= isnull(d_mms_tran_item.dv_load_date_time,'Jan 1, 1753')
             and isnull(d_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_subscription.dv_load_date_time,'Jan 1, 1753')
            then isnull(d_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753')  
            when 
                 isnull(d_exerp_subscription.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753')       
             and isnull(d_exerp_subscription.dv_load_date_time,'Jan 1, 1753') >= isnull(d_mms_tran_item.dv_load_date_time,'Jan 1, 1753')
            then isnull(d_exerp_subscription.dv_load_date_time,'Jan 1, 1753') 
            when 
                 isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753') >= isnull(d_mms_tran_item.dv_load_date_time,'Jan 1, 1753') 
            then isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753') 
            else 
                 isnull(d_mms_tran_item.dv_load_date_time,'Jan 1, 1753') 
           end dv_load_date_time,
           case 
            when 
                 isnull(d_exerp_subscription_period.dv_batch_id,'-1') >= isnull(d_exerp_subscription.dv_batch_id,'-1')
             and isnull(d_exerp_subscription_period.dv_batch_id,'-1') >= isnull(d_exerp_sale_log.dv_batch_id,'-1')
             and isnull(d_exerp_subscription_period.dv_batch_id,'-1') >= isnull(d_mms_tran_item.dv_batch_id,'-1')                
            then isnull(d_exerp_subscription_period.dv_batch_id,'-1')  
            when 
                 isnull(d_exerp_subscription.dv_batch_id,'-1') >= isnull(d_exerp_sale_log.dv_batch_id,'-1')       
             and isnull(d_exerp_subscription.dv_batch_id,'-1') >= isnull(d_mms_tran_item.dv_batch_id,'-1') 
            then isnull(d_exerp_subscription.dv_batch_id,'-1') 
            when 
                 isnull(d_exerp_sale_log.dv_batch_id,'-1') >= isnull(d_mms_tran_item.dv_batch_id,'-1')
            then isnull(d_exerp_sale_log.dv_batch_id,'-1') 
            else
                isnull(d_mms_tran_item.dv_batch_id,'-1') 
           end dv_batch_id
    FROM #key_list
    join d_exerp_subscription_period 
      on d_exerp_subscription_period.dim_exerp_subscription_period_key = #key_list.dim_exerp_subscription_period_key
    join d_exerp_subscription
      on d_exerp_subscription_period.dim_exerp_subscription_key =d_exerp_subscription.dim_exerp_subscription_key
    join d_exerp_product
      on d_exerp_subscription.dim_exerp_product_key = d_exerp_product.bk_hash
    join d_mms_product
      on d_exerp_product.dim_mms_product_key = d_mms_product.bk_hash
    join dim_club
      on d_exerp_subscription_period.dim_club_key = dim_club.dim_club_key
    left join exerp_managed_club_department
      on d_mms_product.department_id = exerp_managed_club_department.department_id
     and dim_club.club_id = exerp_managed_club_department.club_id
     and exerp_managed_club_department.migration_dim_date_key between d_exerp_subscription_period.from_dim_date_key and d_exerp_subscription_period.to_dim_date_key
    left join d_exerp_sale_log 
      on d_exerp_subscription_period.fact_exerp_transaction_log_key = d_exerp_sale_log.fact_exerp_transaction_log_key  
    left join d_mms_tran_item
      on d_mms_tran_item.external_item_id = d_exerp_sale_log.external_id
     and d_mms_tran_item.transaction_source = 'Exerp'
	left join d_mms_mms_tran
	  on d_mms_tran_item.fact_mms_sales_transaction_key = d_mms_mms_tran.bk_hash
	  
/* 
   This is the full set of participation records associated with a subscription period
   this result set will get used only for r = 1
*/
    IF object_id('tempdb..#etl_step2') IS NOT NULL
        DROP TABLE #etl_step2
    CREATE TABLE dbo.#etl_step2 WITH ( distribution = HASH (dim_exerp_subscription_period_key),location = user_db) AS
    SELECT #etl_step1.dim_exerp_subscription_period_key,
           #etl_step1.from_dim_date_key,
           #etl_step1.to_dim_date_key,
           substring(recurrence,charindex(';',recurrence)+1,len(recurrence)) day_list,
           d_exerp_booking.main_d_exerp_booking_bk_hash recurrence_main_dim_exerp_booking_key,
           rank() over (partition by #etl_step1.dim_exerp_subscription_period_key order by d_exerp_booking.start_dim_date_key, d_exerp_booking.start_dim_time_key) r,
           case 
            when isnull(#etl_step1.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753')
                 and isnull(#etl_step1.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_participation.dv_load_date_time,'Jan 1, 1753')
                 and isnull(#etl_step1.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_booking.dv_load_date_time,'Jan 1, 1753') 
            then isnull(#etl_step1.dv_load_date_time,'Jan 1, 1753')  
            when isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_participation.dv_load_date_time,'Jan 1, 1753')    
                 and isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_booking.dv_load_date_time,'Jan 1, 1753') 
            then isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753')
            when isnull(d_exerp_participation.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_booking.dv_load_date_time,'Jan 1, 1753') 
            then isnull(d_exerp_participation.dv_load_date_time,'Jan 1, 1753')  
            else isnull(d_exerp_booking.dv_load_date_time,'Jan 1, 1753')
           end dv_load_date_time,
           case 
            when isnull(#etl_step1.dv_batch_id,'-1') >= isnull(d_exerp_access_privilege_usage.dv_batch_id,'-1')
                 and isnull(#etl_step1.dv_batch_id,'-1') >= isnull(d_exerp_participation.dv_batch_id,'-1')
                 and isnull(#etl_step1.dv_batch_id,'-1') >= isnull(d_exerp_booking.dv_batch_id,'-1')
            then isnull(#etl_step1.dv_batch_id,'-1')  
            when isnull(d_exerp_access_privilege_usage.dv_batch_id,'-1') >= isnull(d_exerp_participation.dv_batch_id,'-1')    
                 and isnull(d_exerp_access_privilege_usage.dv_batch_id,'-1') >= isnull(d_exerp_booking.dv_batch_id,'-1') 
            then isnull(d_exerp_access_privilege_usage.dv_batch_id,'-1')
            when isnull(d_exerp_participation.dv_batch_id,'-1') >= isnull(d_exerp_booking.dv_batch_id,'-1') 
            then isnull(d_exerp_participation.dv_batch_id,'-1')  
            else isnull(d_exerp_booking.dv_batch_id,'-1')  
           end dv_batch_id
    FROM #etl_step1
    join d_exerp_access_privilege_usage
      on #etl_step1.dim_exerp_subscription_key = d_exerp_access_privilege_usage.source_dim_exerp_subscription_key 
     and d_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION'  
     and d_exerp_access_privilege_usage.target_type = 'PARTICIPATION'
    join d_exerp_participation 
      on d_exerp_access_privilege_usage.target_fact_exerp_participation_key = d_exerp_participation.fact_exerp_participation_key    
    join d_exerp_booking
      on d_exerp_participation.dim_exerp_booking_key = d_exerp_booking.bk_hash
     and d_exerp_booking.start_dim_date_key >= #etl_step1.from_dim_date_key
     and d_exerp_booking.start_dim_date_key <= #etl_step1.to_dim_date_key
    join d_exerp_booking_recurrence
      on d_exerp_booking.main_d_exerp_booking_bk_hash = d_exerp_booking_recurrence.bk_hash
  where d_exerp_participation.participation_state <> 'cancelled' 
     or isnull(d_exerp_participation.cancel_reason,'') = 'NO_SHOW'
     
    /* 
    calculate number_of_bookings for first booking in period (r =1)
    while calculated, this value will not overwrite anything in IDK
    */
    IF object_id('tempdb..#etl_step3') IS NOT NULL
        DROP TABLE #etl_step3
    CREATE TABLE dbo.#etl_step3 WITH ( distribution = HASH (dim_exerp_subscription_period_key),location = user_db) AS
    select x.dim_exerp_subscription_period_key,
           x.recurrence_main_dim_exerp_booking_key,
           x.dv_load_date_time,
           x.dv_batch_id,
           count(*) number_of_bookings
      from (select #etl_step2.dim_exerp_subscription_period_key,
                   #etl_step2.from_dim_date_key,
                   #etl_step2.to_dim_date_key,
                   #etl_step2.recurrence_main_dim_exerp_booking_key,
                   #etl_step2.dv_load_date_time,
                   #etl_step2.dv_batch_id,
                   value day
              from #etl_step2
             cross apply string_split(#etl_step2.day_list,',')
             where #etl_step2.r =1) x
      join dim_date 
        on dim_date.dim_date_key >= x.from_dim_date_key 
       and dim_date.dim_date_key <= x.to_dim_date_key
       and dim_date.day_of_week_name = x.day
     group by x.dim_exerp_subscription_period_key,
           x.recurrence_main_dim_exerp_booking_key,
           x.dv_load_date_time,
           x.dv_batch_id

/* 
   combine data together from #etl_step1 (non booking) and #etl_step3 (booking)
   booking related calculated values are persisted from idk_dim_exerp_subscription_period if available
*/
    IF object_id('tempdb..#etl_step4') IS NOT NULL
        DROP TABLE #etl_step4
    CREATE TABLE dbo.#etl_step4 WITH ( distribution = HASH (dim_exerp_subscription_period_key),location = user_db) AS
    select #etl_step1.dim_exerp_subscription_period_key,
           #etl_step1.subscription_period_id,
           #etl_step1.dim_exerp_subscription_key,
           #etl_step1.fact_exerp_transaction_log_key,
           #etl_step1.dim_club_key,
           #etl_step1.subscription_period_state,
           #etl_step1.subscription_period_type,
           #etl_step1.from_dim_date_key,
           #etl_step1.to_dim_date_key,
           #etl_step1.dim_mms_member_key,
           #etl_step1.dim_exerp_product_key,
		   #etl_step1.billing_dim_date_key,
           #etl_step1.net_amount,
           #etl_step1.lt_bucks_amount,
           case when idk_dim_exerp_subscription_period.recurrence_main_dim_exerp_booking_key is null then #etl_step3.recurrence_main_dim_exerp_booking_key
                else idk_dim_exerp_subscription_period.recurrence_main_dim_exerp_booking_key
            end as recurrence_main_dim_exerp_booking_key,
           case when idk_dim_exerp_subscription_period.number_of_bookings is null then #etl_step3.number_of_bookings
                else idk_dim_exerp_subscription_period.number_of_bookings
            end as number_of_bookings,
           case when idk_dim_exerp_subscription_period.price_per_booking is null then #etl_step1.net_amount / #etl_step3.number_of_bookings
                else idk_dim_exerp_subscription_period.price_per_booking
            end as price_per_booking,
           case when idk_dim_exerp_subscription_period.price_per_booking_less_lt_bucks is null then (#etl_step1.net_amount - #etl_step1.lt_bucks_amount) / #etl_step3.number_of_bookings
                else idk_dim_exerp_subscription_period.price_per_booking_less_lt_bucks
            end as price_per_booking_less_lt_bucks,
           case when (idk_dim_exerp_subscription_period.number_of_bookings is null and #etl_step3.number_of_bookings is not null)
                     or (idk_dim_exerp_subscription_period.price_per_booking is null and #etl_step1.net_amount / #etl_step3.number_of_bookings is not null)
                     or (idk_dim_exerp_subscription_period.price_per_booking_less_lt_bucks is null and (#etl_step1.net_amount - #etl_step1.lt_bucks_amount) / #etl_step3.number_of_bookings is not null)
                     then 'Y'
                else 'N'
            end idk_change_flag,
           case when isnull(#etl_step1.dv_load_date_time,'Jan 1, 1753') >= isnull(#etl_step3.dv_load_date_time,'Jan 1, 1753') 
                     then isnull(#etl_step1.dv_load_date_time,'Jan 1, 1753')
                else isnull(#etl_step3.dv_load_date_time,'Jan 1, 1753')
            end dv_load_date_time,
           'dec 31, 9999' dv_load_end_date_time,
           case when isnull(#etl_step1.dv_batch_id,-1) >= isnull(#etl_step3.dv_batch_id,-1) 
                     then isnull(#etl_step1.dv_batch_id,-1)
                else isnull(#etl_step3.dv_batch_id,-1)
            end dv_batch_id,
            #etl_step3.number_of_bookings nob,
            #etl_step1.net_amount na,
            #etl_step1.lt_bucks_amount lba
    from #etl_step1
    left join #etl_step3 on #etl_step1.dim_exerp_subscription_period_key = #etl_step3.dim_exerp_subscription_period_key
    left join idk_dim_exerp_subscription_period on #etl_step1.dim_exerp_subscription_period_key = idk_dim_exerp_subscription_period.dim_exerp_subscription_period_key
   


       IF object_id('tempdb..#etl_step5') IS NOT NULL DROP TABLE #etl_step5
    CREATE TABLE dbo.#etl_step5 WITH ( distribution = HASH (dim_exerp_subscription_period_key),location = user_db) AS
    with cancel_sub_period as (
        select dim_exerp_subscription_key,
               from_dim_date_key,
               dim_exerp_subscription_period_key,
               d_exerp_sale_log.net_amount
          from d_exerp_subscription_period
          join d_exerp_sale_log
            on d_exerp_subscription_period.fact_exerp_transaction_log_key = d_exerp_sale_log.fact_exerp_transaction_log_key
          join dim_date dd1
            on d_exerp_subscription_period.from_dim_date_key = dd1.dim_date_key
          join dim_date dd2
            on d_exerp_subscription_period.to_dim_date_key = dd2.dim_date_key
         where d_exerp_subscription_period.subscription_period_state = 'cancelled'
           and datediff(mm,dd1.calendar_date, dd2.calendar_date) < 2 --there seem to be some problematic erroneous multi month periods that are cancelled
    )
    select #etl_step4.*,
           isnull(refunded_period.dim_exerp_subscription_period_key,'-998') refunded_dim_exerp_subscription_period_key,
           case when refunded_period.dim_exerp_subscription_period_key is not null 
                     then 'Y' 
                else 'N' 
            end refund_period_flag, --flag a period as the period that got refunded
           case when refunded_period.dim_exerp_subscription_period_key is not null 
                     then refunded_period.net_amount - #etl_step4.net_amount 
                else 0 
            end refund_amount
    from #etl_step4
    left join cancel_sub_period refunded_period
      on #etl_step4.dim_exerp_subscription_key = refunded_period.dim_exerp_subscription_key --same subscription
     and #etl_step4.from_dim_date_key = refunded_period.from_dim_date_key --same start date
     and #etl_step4.dim_exerp_subscription_period_key <> refunded_period.dim_exerp_subscription_period_key --different period
     and #etl_step4.net_amount < refunded_period.net_amount

    /*   Delete records from the table that exist*/
    /*   Insert records from records from current and missing batches*/
    BEGIN TRAN
    
    /* delete and insert into dim_exerp_subscription_period table irrespective of upsert flag*/

    DELETE dbo.dim_exerp_subscription_period 
    WHERE dim_exerp_subscription_period_key IN (SELECT dim_exerp_subscription_period_key FROM dbo.#etl_step4 )

    INSERT INTO dim_exerp_subscription_period(
        dim_exerp_subscription_period_key
        ,subscription_period_id
        ,dim_exerp_subscription_key
        ,fact_exerp_transaction_log_key
        ,dim_club_key
        ,subscription_period_state
        ,subscription_period_type
        ,from_dim_date_key
        ,to_dim_date_key
        ,dim_mms_member_key
        ,dim_exerp_product_key
		,billing_dim_date_key
        ,net_amount
        ,number_of_bookings
        ,price_per_booking
        ,lt_bucks_amount
        ,price_per_booking_less_lt_bucks
		,refunded_dim_exerp_subscription_period_key
        ,refund_period_flag
        ,refund_amount
        ,dv_load_date_time
        ,dv_load_end_date_time
        ,dv_batch_id
        ,dv_inserted_date_time
        ,dv_insert_user
    )
    SELECT
        dim_exerp_subscription_period_key
        ,subscription_period_id
        ,dim_exerp_subscription_key
        ,fact_exerp_transaction_log_key
        ,dim_club_key
        ,subscription_period_state
        ,subscription_period_type
        ,from_dim_date_key
        ,to_dim_date_key
        ,dim_mms_member_key
        ,dim_exerp_product_key
		,billing_dim_date_key
        ,isnull(net_amount,0)
        ,isnull(number_of_bookings,0)
        ,isnull(price_per_booking,0)
        ,isnull(lt_bucks_amount,0)
        ,isnull(price_per_booking_less_lt_bucks,isnull(price_per_booking,0))
		,refunded_dim_exerp_subscription_period_key
        ,refund_period_flag
        ,refund_amount
        ,dv_load_date_time
        ,dv_load_end_date_time
        ,dv_batch_id
        ,getdate()
        ,suser_sname()
    FROM #etl_step5
    
    /*delete and insert into idk tableonly for upsert_flag ='Y"*/
    
    DELETE dbo.idk_dim_exerp_subscription_period 
    WHERE dim_exerp_subscription_period_key IN (SELECT dim_exerp_subscription_period_key FROM dbo.#etl_step4 where idk_change_flag='Y')

    INSERT INTO idk_dim_exerp_subscription_period(
        dim_exerp_subscription_period_key
        ,number_of_bookings
        ,price_per_booking
        ,price_per_booking_less_lt_bucks
        ,recurrence_main_dim_exerp_booking_key
        ,dv_load_date_time
        ,dv_batch_id
        ,dv_inserted_date_time
        ,dv_insert_user
    )
    SELECT
        dim_exerp_subscription_period_key
        ,number_of_bookings
        ,price_per_booking
        ,price_per_booking_less_lt_bucks
        ,recurrence_main_dim_exerp_booking_key
        ,dv_load_date_time
        ,dv_batch_id
        ,getdate()
        ,suser_sname()
    FROM #etl_step5
    where idk_change_flag='Y'

    COMMIT TRAN

END
