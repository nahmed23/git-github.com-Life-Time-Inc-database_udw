CREATE PROC [dbo].[proc_fact_exerp_clipcard_usage] @dv_batch_id [varchar](500) AS
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON

DECLARE @max_dv_batch_id BIGINT = (
  SELECT max(isnull(dv_batch_id, - 1))
  FROM fact_exerp_clipcard_usage
  )
DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
DECLARE @load_dv_batch_id BIGINT = CASE 
  WHEN @max_dv_batch_id < @current_dv_batch_id
   THEN @max_dv_batch_id
  ELSE @current_dv_batch_id
  END

if object_id('tempdb..#etl_step1') is not null
       drop table #etl_step1
    create table dbo.#etl_step1
        with (
               distribution = hash(dim_exerp_booking_key),
               location = user_db
     ) as
select d_exerp_clipcard_usage.bk_hash fact_exerp_clipcard_usage_key,
       d_exerp_clipcard_usage.clipcard_usage_id clipcard_usage_id,
   d_exerp_clipcard_usage.usage_dim_date_key usage_dim_date_key,
   d_exerp_clipcard_usage.usage_dim_time_key usage_dim_time_key,
   d_exerp_clipcard_usage.delivered_dim_club_key delivered_dim_club_key,
   d_exerp_clipcard_usage.clips clips,
   d_exerp_clipcard_usage.commission_units commission_units,
   d_exerp_clipcard_usage.dim_exerp_clipcard_key dim_exerp_clipcard_key,
   d_exerp_clipcard_usage.cancelled_flag cancelled_flag,
   d_exerp_clipcard_usage.clipcard_usage_type clipcard_usage_type,
   d_exerp_clipcard_usage.clipcard_usage_state clipcard_usage_state,
   d_exerp_clipcard_usage.clipcard_usage_entered_dim_employee_key clipcard_usage_entered_dim_employee_key,
   dim_exerp_clipcard.dim_mms_member_key dim_mms_member_key,
   dim_exerp_clipcard.dim_exerp_product_key dim_exerp_product_key,
   dim_exerp_clipcard.clips_initial clips_initial,
   dim_exerp_clipcard.sale_entry_dim_date_key sale_entry_dim_date_key,
   dim_exerp_clipcard.sale_entry_dim_time_key sale_entry_dim_time_key,
   dim_exerp_clipcard.sale_entered_dim_employee_key sale_entered_dim_employee_key,
   dim_exerp_clipcard.cancelled_flag clipcard_cancelled_flag,
   dim_exerp_clipcard.blocked_flag clipcard_blocked_flag,
   /*dim_exerp_clipcard.sale_dim_employee_key sale_dim_employee_key,*/
   dim_exerp_clipcard.sale_source_type sale_source_type,
   case when d_exerp_participation.participation_state = 'PARTICIPATION' then 'Y' else 'N' end  participation_complete_flag,
   d_exerp_access_privilege_usage.access_privilege_usage_state access_privilege_usage_state,
   d_exerp_participation.dim_exerp_booking_key dim_exerp_booking_key,
   case when isnull(d_exerp_clipcard_usage.dv_load_date_time,'Jan 1, 1753') >= isnull(dim_exerp_clipcard.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_exerp_clipcard_usage.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753')
                and isnull(d_exerp_clipcard_usage.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_participation.dv_load_date_time,'Jan 1, 1753')
                           then isnull(d_exerp_clipcard_usage.dv_load_date_time,'Jan 1, 1753')
            when isnull(dim_exerp_clipcard.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753')
                and isnull(dim_exerp_clipcard.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_participation.dv_load_date_time,'Jan 1, 1753')
                            then isnull(dim_exerp_clipcard.dv_load_date_time,'Jan 1, 1753')                
            when isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753') >= isnull(d_exerp_participation.dv_load_date_time,'Jan 1, 1753')
                              then isnull(d_exerp_access_privilege_usage.dv_load_date_time,'Jan 1, 1753')                
                            else isnull(d_exerp_participation.dv_load_date_time,'Jan 1, 1753')  end dv_load_date_time,
     convert(datetime, '99991231', 112) dv_load_end_date_time,
   case when isnull(d_exerp_clipcard_usage.dv_batch_id,'-1') >= isnull(dim_exerp_clipcard.dv_batch_id,'-1')
                and isnull(d_exerp_clipcard_usage.dv_batch_id,'-1') >= isnull(d_exerp_access_privilege_usage.dv_batch_id,'-1')
                and isnull(d_exerp_clipcard_usage.dv_batch_id,'-1') >= isnull(d_exerp_participation.dv_batch_id,'-1')
                              then isnull(d_exerp_clipcard_usage.dv_batch_id,'-1')
            when isnull(dim_exerp_clipcard.dv_batch_id,'-1') >= isnull(d_exerp_access_privilege_usage.dv_batch_id,'-1')
                and isnull(dim_exerp_clipcard.dv_batch_id,'-1') >= isnull(d_exerp_participation.dv_batch_id,'-1')
                              then isnull(dim_exerp_clipcard.dv_batch_id,'-1')                
            when isnull(d_exerp_access_privilege_usage.dv_batch_id,'-1') >= isnull(d_exerp_participation.dv_batch_id,'-1')
                              then isnull(d_exerp_access_privilege_usage.dv_batch_id,'-1')                
                            else isnull(d_exerp_participation.dv_batch_id,'-1')  end dv_batch_id  
     
from d_exerp_clipcard_usage 
join dim_exerp_clipcard
    on d_exerp_clipcard_usage.dim_exerp_clipcard_key = dim_exerp_clipcard.dim_exerp_clipcard_key
join d_exerp_access_privilege_usage
	/*deduction_key_bk_hash replaced by deduction_fact_exerp_clipcard_usage_key in sprint DW_2019_07_24 */
    on d_exerp_clipcard_usage.bk_hash = d_exerp_access_privilege_usage.deduction_fact_exerp_clipcard_usage_key 
join d_exerp_participation
/*target_id_bk_hash replaced by target_fact_exerp_participation_key in sprint DW_2019_07_24 */
    on d_exerp_access_privilege_usage.target_fact_exerp_participation_key = d_exerp_participation.bk_hash
where (d_exerp_access_privilege_usage.dv_batch_id >= -1
        or d_exerp_clipcard_usage.dv_batch_id >= -1
		or d_exerp_participation.dv_batch_id >= -1
		or dim_exerp_clipcard.dv_batch_id >= -1)
	    and ((d_exerp_access_privilege_usage.source_type = 'CLIPCARD'
        and d_exerp_access_privilege_usage.target_type = 'PARTICIPATION'
		and d_exerp_access_privilege_usage.deduction_fact_exerp_clipcard_usage_key != '-998')
		or d_exerp_access_privilege_usage.bk_hash in ('-997','-998','-999'))
		
 /* Note that access_privilege_usage.state = 'CANCELLED' always matches clipcard_usage.state = 'CANCELLED' so it is not needed in the where clause*/

 /* Get the most recent non-cancelled dim_employee_key for the set of dim_booking_keys from #etl_step1*/
 
 if object_id('tempdb..#etl_step2') is not null
        drop table #etl_step2
     create table dbo.#etl_step2 
     with (
            distribution = hash(d_exerp_booking_bk_hash),
            location = user_db 
           ) as
select  d_exerp_staff_usage.d_exerp_booking_bk_hash,
        d_exerp_staff_usage.dim_employee_key,
first_value(d_exerp_staff_usage.dim_employee_key) over (partition by d_exerp_staff_usage.d_exerp_booking_bk_hash order by d_exerp_staff_usage.staff_usage_id desc rows unbounded preceding)
    as most_recent_dim_employee_key ,
	isnull(d_exerp_staff_usage.dv_load_date_time, 'Jan 1, 1753') dv_load_date_time,
	isnull(d_exerp_staff_usage.dv_batch_id, - 1) dv_batch_id
	
from d_exerp_staff_usage
where (d_exerp_staff_usage.d_exerp_booking_bk_hash in (select dim_exerp_booking_key from #etl_step1)
    or d_exerp_staff_usage.bk_hash in ('-997','-998','-999'))
    and (d_exerp_staff_usage.staff_usage_state != 'CANCELLED'
    or d_exerp_staff_usage.bk_hash in ('-997','-998','-999'))
	
delete #etl_step2 where dim_employee_key != most_recent_dim_employee_key 
 
 /* combine the main information with the dim_employee_key*/
if object_id('tempdb..#etl_step3') is not null
        drop table #etl_step3
    create table dbo.#etl_step3
     with (
            distribution = hash(fact_exerp_clipcard_usage_key),
            location = user_db 
           ) as
select 
    #etl_step1.fact_exerp_clipcard_usage_key as fact_exerp_clipcard_usage_key
   ,#etl_step1.clipcard_usage_id as clipcard_usage_id
   ,#etl_step1.usage_dim_date_key as usage_dim_date_key
   ,#etl_step1.usage_dim_time_key as usage_dim_time_key
   ,#etl_step1.delivered_dim_club_key as delivered_dim_club_key
   ,#etl_step1.clips as clips
   ,#etl_step1.commission_units as commission_units
   ,#etl_step1.dim_exerp_clipcard_key as dim_exerp_clipcard_key
   ,#etl_step1.cancelled_flag as cancelled_flag
   ,#etl_step1.clipcard_usage_type as clipcard_usage_type
   ,#etl_step1.clipcard_usage_state as clipcard_usage_state
   ,#etl_step1.clipcard_usage_entered_dim_employee_key as clipcard_usage_entered_dim_employee_key
   ,#etl_step1.dim_mms_member_key as dim_mms_member_key
   ,#etl_step1.dim_exerp_product_key as dim_exerp_product_key
   ,#etl_step1.clips_initial as clips_initial
   ,#etl_step1.sale_entry_dim_date_key as sale_entry_dim_date_key
   ,#etl_step1.sale_entry_dim_time_key as sale_entry_dim_time_key
   ,#etl_step1.sale_entered_dim_employee_key as sale_entered_dim_employee_key
   ,#etl_step1.clipcard_cancelled_flag as clipcard_cancelled_flag
   ,#etl_step1.clipcard_blocked_flag as clipcard_blocked_flag
  /* #etl_step1.sale_dim_employee_key*/
   ,#etl_step1.sale_source_type as sale_source_type
   ,#etl_step1.participation_complete_flag as participation_complete_flag
   ,#etl_step1.access_privilege_usage_state as access_privilege_usage_state
   ,#etl_step1.dim_exerp_booking_key as dim_exerp_booking_key
   ,#etl_step2.dim_employee_key as delivered_dim_employee_key,
   
   case 
	    	when #etl_step1.dv_load_date_time >= isnull(#etl_step2.dv_load_date_time,'Jan 1, 1753')
	    		then #etl_step1.dv_load_date_time
	    	else isnull(#etl_step2.dv_load_date_time,'Jan 1, 1753') 
	    end dv_load_date_time,
	#etl_step1.dv_load_end_date_time as dv_load_end_date_time,	
	case 
	    	when #etl_step1.dv_batch_id >= isnull(#etl_step2.dv_batch_id,-1)
	    		then #etl_step1.dv_batch_id
            else isnull(#etl_step2.dv_batch_id,-1) 
	    end dv_batch_id
  
from #etl_step1
    join #etl_step2
         on #etl_step1.dim_exerp_booking_key = #etl_step2.d_exerp_booking_bk_hash

/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/
BEGIN TRAN

DELETE dbo.fact_exerp_clipcard_usage
WHERE fact_exerp_clipcard_usage_key IN (
  SELECT fact_exerp_clipcard_usage_key
  FROM dbo.#etl_step3
  )

INSERT INTO fact_exerp_clipcard_usage(
    fact_exerp_clipcard_usage_key
   ,clipcard_usage_id
   ,usage_dim_date_key
   ,usage_dim_time_key 
   ,delivered_dim_club_key 
   ,clips 
   ,commission_units 
   ,dim_exerp_clipcard_key 
   ,cancelled_flag 
   ,clipcard_usage_type 
   ,clipcard_usage_state 
   ,clipcard_usage_entered_dim_employee_key 
   ,dim_mms_member_key 
   ,dim_exerp_product_key 
   ,clips_initial 
   ,sale_entry_dim_date_key 
   ,sale_entry_dim_time_key 
   ,sale_entered_dim_employee_key 
   ,clipcard_cancelled_flag
   ,clipcard_blocked_flag
  /*sale_dim_employee_key*/
   ,sale_source_type
   ,participation_complete_flag
   ,access_privilege_usage_state
   ,dim_exerp_booking_key
   ,delivered_dim_employee_key
   ,dv_load_date_time
   ,dv_load_end_date_time
   ,dv_batch_id
   ,dv_inserted_date_time
   ,dv_insert_user
 )
SELECT
    fact_exerp_clipcard_usage_key
   ,clipcard_usage_id
   ,usage_dim_date_key
   ,usage_dim_time_key 
   ,delivered_dim_club_key 
   ,clips 
   ,commission_units 
   ,dim_exerp_clipcard_key 
   ,cancelled_flag 
   ,clipcard_usage_type 
   ,clipcard_usage_state 
   ,clipcard_usage_entered_dim_employee_key 
   ,dim_mms_member_key 
   ,dim_exerp_product_key 
   ,clips_initial 
   ,sale_entry_dim_date_key 
   ,sale_entry_dim_time_key 
   ,sale_entered_dim_employee_key 
   ,clipcard_cancelled_flag
   ,clipcard_blocked_flag
  /*sale_dim_employee_key*/
   ,sale_source_type
   ,participation_complete_flag
   ,access_privilege_usage_state
   ,dim_exerp_booking_key
   ,delivered_dim_employee_key
   ,dv_load_date_time
   ,dv_load_end_date_time
   ,dv_batch_id
   ,getdate()
   ,suser_sname()

FROM #etl_step3

COMMIT TRAN

END
