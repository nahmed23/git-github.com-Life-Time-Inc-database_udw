CREATE PROC [dbo].[proc_fact_exerp_participation] @dv_batch_id [varchar](500) AS
BEGIN
    SET XACT_ABORT ON
    SET NOCOUNT ON

    DECLARE @max_dv_batch_id BIGINT = ( SELECT max(isnull(dv_batch_id,-1)) FROM fact_exerp_participation  )
    DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
    DECLARE @load_dv_batch_id BIGINT = CASE  WHEN @max_dv_batch_id < @current_dv_batch_id   THEN @max_dv_batch_id  ELSE @current_dv_batch_id  END
    declare @today_dim_date_key varchar(8) = (select dim_date_key from dim_date where calendar_date = convert(datetime,convert(Varchar,getdate(),110),110))

    if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with ( distribution = hash(fact_exerp_participation_key), location = user_db ) as
select d_exerp_participation.fact_exerp_participation_key
      ,d_exerp_participation.participation_id
      ,d_exerp_booking.start_dim_date_key booking_dim_date_key
      ,d_exerp_booking.start_dim_time_key booking_dim_time_key
      ,case when d_exerp_access_privilege_usage.bk_hash is not null then 'Y' else 'N' end billable_flag
      ,case when d_exerp_booking.booking_state = 'cancelled' then 'Y' else 'N' end booking_cancelled_flag
      ,case when d_exerp_participation.participation_state = 'CANCELLED' then 'Y' else 'N' end participation_cancelled_flag
      ,case when d_exerp_participation.participation_state = 'CANCELLED' and d_exerp_participation.cancel_reason = 'NO_SHOW' then 'Y' else 'N' end participation_cancelled_no_show_flag
      ,d_exerp_participation.cancel_dim_date_key
      ,d_exerp_participation.cancel_dim_time_key
      ,d_exerp_participation.cancel_interface_type
      ,d_exerp_participation.cancel_reason
      ,d_exerp_participation.creation_dim_date_key
      ,d_exerp_participation.creation_dim_time_key
      ,d_exerp_participation.dim_club_key dim_club_key
      ,d_exerp_participation.dim_exerp_booking_key
      ,d_exerp_booking.d_exerp_activity_bk_hash dim_exerp_activity_key
      ,d_exerp_product.bk_hash dim_exerp_product_key
      ,d_exerp_product.dim_mms_product_key
      ,case when d_exerp_access_privilege_usage.source_type = 'CLIPCARD' then 'Y' else 'N' end clipcard_flag
      ,case when d_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION' then 'Y' else 'N' end subscription_flag
      ,case when d_exerp_participation.bk_hash in ('-997','-998','-999') then d_exerp_participation.bk_hash
            when d_exerp_access_privilege_usage.source_type = 'CLIPCARD'  
                 then d_exerp_access_privilege_usage.source_dim_exerp_clipcard_key
            else '-998'
        end dim_exerp_clipcard_key
      ,case when d_exerp_participation.bk_hash in ('-997','-998','-999') then d_exerp_participation.bk_hash
            when d_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION'  
                 then d_exerp_access_privilege_usage.source_dim_exerp_subscription_key
            else '-998'
        end dim_exerp_subscription_key
      ,case when d_exerp_participation.bk_hash in ('-997','-998','-999') then d_exerp_participation.bk_hash
            when d_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION'  
                 then dim_exerp_subscription_period.dim_exerp_subscription_period_key
            else '-998'
        end dim_exerp_subscription_period_key
      ,d_exerp_participation.dim_mms_member_key
      ,d_exerp_participation.ets
      ,case when d_exerp_participation.participation_state = 'PARTICIPATION' then 'Y' else 'N' end participated_flag
      ,d_exerp_participation.participation_state
      ,d_exerp_participation.show_up_dim_date_key
      ,d_exerp_participation.show_up_dim_time_key
      ,d_exerp_participation.show_up_interface_type
      ,d_exerp_participation.show_up_using_card_flag
      ,d_exerp_participation.user_interface_type
      ,d_exerp_participation.was_on_waiting_list_flag
      ,d_exerp_participation.seat_obtained_dim_date_key
      ,d_exerp_participation.seat_obtained_dim_time_key
      ,d_exerp_participation.participant_number
      ,d_exerp_participation.seat_id
      ,d_exerp_participation.seat_state
      ,case when d_exerp_participation.dv_load_date_time >= isnull(d_exerp_booking.dv_load_date_time,'Jan 1, 1753')
             and d_exerp_participation.dv_load_date_time >= isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753')
             and d_exerp_participation.dv_load_date_time >= isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753')
             and d_exerp_participation.dv_load_date_time >= isnull(dim_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753')
            then d_exerp_participation.dv_load_date_time
            when d_exerp_booking.dv_load_date_time >= isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753')
             and d_exerp_booking.dv_load_date_time >= isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753')
             and d_exerp_booking.dv_load_date_time >= isnull(dim_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753')
            then d_exerp_booking.dv_load_date_time
            when d_exerp_access_privilege_usage.dv_load_date_time >= isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753')
             and d_exerp_access_privilege_usage.dv_load_date_time >= isnull(dim_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753')
            then d_exerp_access_privilege_usage.dv_load_date_time
            when d_exerp_sale_log.dv_load_date_time >= isnull(dim_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753')
            then d_exerp_sale_log.dv_load_date_time
            else isnull(dim_exerp_subscription_period.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time
      ,convert(datetime,'99991231',112) dv_load_end_date_time
      ,case when d_exerp_participation.dv_batch_id >= isnull(d_exerp_booking.dv_batch_id,-1)
             and d_exerp_participation.dv_batch_id >= isnull(d_exerp_access_privilege_usage.dv_batch_id,-1)
             and d_exerp_participation.dv_batch_id >= isnull(d_exerp_sale_log.dv_batch_id,-1)
             and d_exerp_participation.dv_batch_id >= isnull(dim_exerp_subscription_period.dv_batch_id,-1)
            then d_exerp_participation.dv_batch_id
            when d_exerp_booking.dv_batch_id >= isnull(d_exerp_access_privilege_usage.dv_batch_id,-1)
             and d_exerp_booking.dv_batch_id >= isnull(d_exerp_sale_log.dv_batch_id,-1)
             and d_exerp_booking.dv_batch_id >= isnull(dim_exerp_subscription_period.dv_batch_id,-1)
            then d_exerp_booking.dv_batch_id
            when d_exerp_access_privilege_usage.dv_batch_id >= isnull(d_exerp_sale_log.dv_batch_id,-1)
             and d_exerp_access_privilege_usage.dv_batch_id >= isnull(dim_exerp_subscription_period.dv_batch_id,-1)
            then d_exerp_access_privilege_usage.dv_batch_id
            when d_exerp_sale_log.dv_batch_id >= isnull(dim_exerp_subscription_period.dv_batch_id,-1)
            then d_exerp_sale_log.dv_batch_id
            else isnull(dim_exerp_subscription_period.dv_batch_id,-1) end dv_batch_id
      ,case when d_exerp_access_privilege_usage.source_type = 'CLIPCARD' then d_exerp_clipcard.fact_exerp_transaction_log_key
            when d_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION' then dim_exerp_subscription_period.fact_exerp_transaction_log_key
            else '-998' end fact_exerp_transaction_log_key
      ,case when d_exerp_booking.start_dim_date_key >= @today_dim_date_key then 1 else 0 end future_booking_flag
      ,d_exerp_booking.class_capacity
  from d_exerp_participation
  join d_exerp_booking 
    on d_exerp_participation.dim_exerp_booking_key = d_exerp_booking.bk_hash
  left join d_exerp_access_privilege_usage
    on d_exerp_participation.bk_hash = d_exerp_access_privilege_usage.target_fact_exerp_participation_key
   and d_exerp_access_privilege_usage.target_type = 'PARTICIPATION'
  left join d_exerp_sale_log
    on d_exerp_access_privilege_usage.source_fact_exerp_transaction_log_key = d_exerp_sale_log.bk_hash
  left join d_exerp_product
    on d_exerp_sale_log.dim_exerp_product_key = d_exerp_product.bk_hash
  left join dim_exerp_subscription_period
    on d_exerp_access_privilege_usage.source_dim_exerp_subscription_key = dim_exerp_subscription_period.dim_exerp_subscription_key
   and d_exerp_access_privilege_usage.source_type = 'SUBSCRIPTION' 
   and d_exerp_booking.start_dim_date_key >= dim_exerp_subscription_period.from_dim_date_key
   and d_exerp_booking.start_dim_date_key <= dim_exerp_subscription_period.to_dim_date_key
   and dim_exerp_subscription_period.subscription_period_state <> 'cancelled'
  left join d_exerp_clipcard
    on d_exerp_access_privilege_usage.source_dim_exerp_clipcard_key = d_exerp_clipcard.bk_hash
   and d_exerp_access_privilege_usage.source_type = 'CLIPCARD' 
 where (d_exerp_participation.dv_batch_id >= @load_dv_batch_id
        or d_exerp_booking.dv_batch_id >= @load_dv_batch_id
        or d_exerp_access_privilege_usage.dv_batch_id >= @load_dv_batch_id
        or d_exerp_sale_log.dv_batch_id >= @load_dv_batch_id
        or dim_exerp_subscription_period.dv_batch_id >= @load_dv_batch_id
        or d_exerp_booking.start_dim_date_key >= @today_dim_date_key) --to recalculate waitlists

if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with ( distribution = hash(fact_exerp_participation_key), location = user_db ) as
with over_capcity as (
    select dim_exerp_booking_key,
           class_capacity
    from #etl_step_1
    where future_booking_flag = 1
    group by dim_exerp_booking_key,
           class_capacity 
    having sum(case when participation_state != 'cancelled' then 1 else 0 end) > class_capacity --class is overflowing!
), waitlist as (
--cheeky bit of code: for each participant number (e1), attach all the participant numbers equal or less than e1 (e2)
--sum the number of non cancelled e2s: only one e1 will have a number of valid e2s that equals the class capacity
    select oc.dim_exerp_booking_key,
           e1.participant_number
    from #etl_step_1 e1
    join over_capcity oc on e1.dim_exerp_booking_key = oc.dim_exerp_booking_key
    join #etl_step_1 e2 on e1.dim_exerp_booking_key = e2.dim_exerp_booking_key and e1.participant_number >= e2.participant_number
   where e1.participation_state <> 'cancelled' --because a non cancelled participant 15, and a cancelled participant 16 will both end up with the same non cancelled counts under them
    group by oc.dim_exerp_booking_key,e1.participant_number, oc.class_capacity
    having sum(case when e2.participation_state != 'cancelled' then 1 else 0 end) = oc.class_capacity
)
select e.*,
       case when e.participant_number > wl.participant_number then 'Y' else 'N' end waitlist_flag
from #etl_step_1 e
left join waitlist wl on e.dim_exerp_booking_key = wl.dim_exerp_booking_key

    /*   Delete records from the table that exist*/
    /*   Insert records from records from current and missing batches*/
    BEGIN TRAN

    DELETE dbo.fact_exerp_participation WHERE fact_exerp_participation_key IN (SELECT fact_exerp_participation_key FROM dbo.#etl_step_2 )

    INSERT INTO fact_exerp_participation(
         fact_exerp_participation_key,
         participation_id,
         billable_flag, ---
         booking_cancelled_flag, ------
         booking_dim_date_key,  ------
         booking_dim_time_key,-------------
         cancel_dim_date_key,
         cancel_dim_time_key,
         cancel_interface_type,
         cancel_reason,
         clipcard_flag,----------
         creation_dim_date_key,
         creation_dim_time_key,
         dim_club_key,
         dim_exerp_activity_key,--------
         dim_exerp_booking_key,
         dim_exerp_clipcard_key,
         dim_exerp_product_key,--------------
         dim_exerp_subscription_key,
         dim_exerp_subscription_period_key,--------
         dim_mms_member_key,
         dim_mms_product_key,--------------
         ets,
         fact_exerp_transaction_log_key, -----------
         participant_number,
         participated_flag, -----
         participation_cancelled_flag, --------
         participation_cancelled_no_show_flag, -------
         participation_state,
         seat_id,
         seat_obtained_dim_date_key,
         seat_obtained_dim_time_key,
         seat_state,
         show_up_dim_date_key,
         show_up_dim_time_key,
         show_up_interface_type,
         show_up_using_card_flag,
         subscription_flag, -------
         user_interface_type,
         waitlist_flag, --------
         was_on_waiting_list_flag,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user
    )
    select
         fact_exerp_participation_key,
         participation_id,
         billable_flag,
         booking_cancelled_flag,
         booking_dim_date_key,
         booking_dim_time_key,
         cancel_dim_date_key,
         cancel_dim_time_key,
         cancel_interface_type,
         cancel_reason,
         clipcard_flag,
         creation_dim_date_key,
         creation_dim_time_key,
         dim_club_key,
         dim_exerp_activity_key,
         dim_exerp_booking_key,
         dim_exerp_clipcard_key,
         dim_exerp_product_key,
         dim_exerp_subscription_key,
         dim_exerp_subscription_period_key,
         dim_mms_member_key,
         dim_mms_product_key,
         ets,
         fact_exerp_transaction_log_key,
         participant_number,
         participated_flag,
         participation_cancelled_flag,
         participation_cancelled_no_show_flag,
         participation_state,
         seat_id,
         seat_obtained_dim_date_key,
         seat_obtained_dim_time_key,
         seat_state,
         show_up_dim_date_key,
         show_up_dim_time_key,
         show_up_interface_type,
         show_up_using_card_flag,
         subscription_flag,
         user_interface_type,
         waitlist_flag,
         was_on_waiting_list_flag,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #etl_step_2

    COMMIT TRAN

END
