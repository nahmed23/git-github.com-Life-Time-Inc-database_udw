﻿CREATE PROC [sandbox_ebi].[proc_ExerpDeferred_Revenue_NonSession] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](4000),@DimLocationKeyList [VARCHAR](4000),@WorkDayOffering [VARCHAR](4000),@WorkDayCostCenter [VARCHAR](4000) AS

BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

----- Sample Execution
 ---   Exec [reporting].[proc_ExerpDeferred_Revenue_NonSession] '7/18/2019','8/24/2019','All Regions','178','OF10122|OF10056','51015'
 --    Exec [reporting].[proc_ExerpDeferred_Revenue_NonSession] '10/1/2019','10/3/2019','Hall-MN-West','-1','All Offerings','All Cost Centers'

--DECLARE @StartDate [DATETIME]
--DECLARE @EndDate [DATETIME]

SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                      THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0)    ----- returns 1st of yesterday's month
					  WHEN @StartDate = 'Dec 30, 1899'
					  THEN DATEADD(YEAR,DATEDIFF(YEAR,0,GETDATE()-1),0)      ----- returns 1st of yesterday's year
					  ELSE @StartDate END
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @EndDate END


DECLARE @StartDimDateKey INT,
        @EndDimDateKey INT


SET @StartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @EndDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @EndDate)


--DECLARE @StartDimDateKey VARCHAR(22) = '20191001'
--DECLARE @EndDimDateKey VARCHAR(22) = '20191021'
--DECLARE @RegionList [VARCHAR](4000) = 'All Regions'
--DECLARE @DimLocationKeyList VARCHAR(4000) = '238'
--DECLARE @WorkDayOffering VARCHAR(4000) = 'All Offerings'
--DECLARE @WorkDayCostCenter VARCHAR(4000) = 'All Cost Centers'


/* Parse the Passed Parameters */

 ----- Club Prompt ------  
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL  
  DROP TABLE #Clubs;

DECLARE @list_table VARCHAR(8000)
SET @list_table = 'club_list'

  EXEC sandbox_ebi.proc_parse_pipe_list @DimLocationKeyList,@list_table


	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubKeyList
    ON ClubKeyList.Item = DimClub.club_id
	  OR ClubKeyList.Item = -1
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key in('-997','-998','-999')  OR DimClub.club_close_dim_date_key > @StartDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code,
	   MMSRegion.description,
	   PTRCLRegion.description,
	   MemberActivitiesRegion.description

----- Region Prompt ---------------------------------------

IF OBJECT_ID('tempdb.dbo.#DimLocationInfo', 'U') IS NOT NULL
  DROP TABLE #DimLocationInfo;

SET @list_table = 'region_list'


  EXEC sandbox_ebi.proc_parse_pipe_list @RegionList,@list_table


	
SELECT DimClub.DimClubKey,      ------ name change
      DimClub.MMSRegion as Region,
       DimClub.ClubName AS MMSClubName,
	   DimClub.club_id AS MMSClubID,
	   DimClub.gl_club_id AS GLClubID,
	   DimClub.LocalCurrencyCode
  INTO #DimLocationInfo    
  FROM #Clubs DimClub     
  JOIN #region_list RegionList 
   ON RegionList.Item = DimClub.MMSRegion
     OR RegionList.Item = 'All Regions'
 GROUP BY DimClub.DimClubKey, 
      DimClub.MMSRegion,
       DimClub.ClubName,
	   DimClub.club_id,
	   DimClub.gl_club_id,
	   DimClub.LocalCurrencyCode


   --- Workday Offering Prompt ------  
IF OBJECT_ID('tempdb.dbo.#WDOfferingPromptList', 'U') IS NOT NULL
  DROP TABLE #WDOfferingPromptList;   

DECLARE @list_offering_table VARCHAR(100)
SET @list_offering_table = 'wd_offering_list'

EXEC sandbox_ebi.proc_parse_pipe_list @WorkDayOffering,@list_offering_table



SELECT DISTINCT wd_offering_list.Item
  INTO #WDOfferingPromptList
  FROM #wd_offering_list  wd_offering_list


 --- Workday Cost Center Prompt ------  
IF OBJECT_ID('tempdb.dbo.#WDCostCenterPromptList', 'U') IS NOT NULL
  DROP TABLE #WDCostCenterPromptList;   

DECLARE @list_costcenter_table VARCHAR(100)
SET @list_costcenter_table = 'wd_costcenter_list'

EXEC sandbox_ebi.proc_parse_pipe_list @WorkDayCostCenter,@list_costcenter_table

SELECT DISTINCT wd_costcenter_list.Item
  INTO #WDCostCenterPromptList
  FROM #wd_costcenter_list  wd_costcenter_list

IF OBJECT_ID('tempdb.dbo.#detail', 'U') IS NOT NULL
drop table #detail;

with sub_per (dim_exerp_subscription_period_key,dim_exerp_subscription_key,from_dim_date_key,to_dim_date_key,number_of_bookings,price_per_booking,
              price_per_booking_less_lt_bucks,net_amount,dim_club_key,dim_exerp_product_key,fact_exerp_transaction_log_key,subscription_period_state) as
(
    --get all sub periods in selected range, in case there are no associated bookings?
    select sp1.dim_exerp_subscription_period_key,
           sp1.dim_exerp_subscription_key,
           sp1.from_dim_date_key,
           sp1.to_dim_date_key,
           sp1.number_of_bookings,
           sp1.price_per_booking,
           sp1.price_per_booking_less_lt_bucks,
           sp1.net_amount,
           sp1.dim_club_key,
           sp1.dim_exerp_product_key,
           sp1.fact_exerp_transaction_log_key,
           sp1.subscription_period_state
    from marketing.v_dim_exerp_subscription_period sp1
    where (sp1.from_dim_date_key between @StartDimDateKey and @EndDimDateKey 
           or sp1.to_dim_date_key between @StartDimDateKey and @EndDimDateKey)
      and sp1.subscription_period_state <> 'cancelled'
)
SELECT c.club_code,
       c.club_name,
       c.workday_region,
       mmsregion.description mmsRegion,
       bo.booking_id,
       bo.booking_name,
       fact_exerp_subscription_participation.delivered_dim_date_key start_dim_date_key,
       mmsp.workday_cost_center,
       mmsp.workday_offering,
       p.product_name exerp_product_name,
       mmsp.product_description mms_product_description,
       dim_exerp_subscription.dim_exerp_subscription_key,
       dim_exerp_subscription.subscription_id,
       case when fact_exerp_subscription_participation.revenue_dim_date_key not in ('-999','-998','-997') then 1 else 0 end booking_serviced,
	   case when fact_exerp_subscription_participation.revenue_dim_date_key between @StartDimDateKey and @EndDimDateKey
                 then 1
            else 0
		end booking_serviced_this_period,
      case when fact_exerp_subscription_participation.revenue_dim_date_key > @EndDimDateKey
                 then 1
            else 0
        end booking_outstanding,
       case when fact_exerp_subscription_participation.revenue_dim_date_key between @StartDimDateKey and @EndDimDateKey
                 then dim_exerp_subscription_period.price_per_booking
            else 0
        end revenue_recognized_this_period,
       case when fact_exerp_subscription_participation.revenue_dim_date_key between @StartDimDateKey and @EndDimDateKey
                 then dim_exerp_subscription_period.price_per_booking_less_lt_bucks
            else 0
        end revenue_recognized_this_period_less_ltbucks,
       case when fact_exerp_subscription_participation.revenue_dim_date_key > @EndDimDateKey
                 then dim_exerp_subscription_period.price_per_booking_less_lt_bucks
            else 0
        end outstanding_booking_revenue,
		mem.member_id,
        mem.customer_name as member_name,
		dim_employee.employee_id as trainer_employee_id,
		dim_employee.employee_name as trainer_name,
       fact_exerp_subscription_participation.fact_exerp_participation_key,
       dim_exerp_subscription_period.fact_exerp_transaction_log_key,
       dim_exerp_subscription_period.dim_exerp_subscription_period_key,
       dim_exerp_subscription_period.number_of_bookings,
       dim_exerp_subscription_period.net_amount,
       dim_exerp_subscription_period.price_per_booking,
       dim_exerp_subscription_period.price_per_booking_less_lt_bucks,
       case when dim_exerp_subscription_period.to_dim_date_key <= @EndDimDateKey then 1 else 0 end period_closed_flag,
       dim_exerp_subscription_period.subscription_period_state,
       dim_exerp_subscription_period.to_dim_date_key,
       fact_exerp_subscription_participation.refund_amount
  INTO #detail
  from sub_per dim_exerp_subscription_period
  join marketing.v_dim_exerp_subscription dim_exerp_subscription 
       on dim_exerp_subscription_period.dim_exerp_subscription_key = dim_exerp_subscription.dim_exerp_subscription_key
  join marketing.v_dim_exerp_product p 
       on dim_exerp_subscription_period.dim_exerp_product_key = p.dim_exerp_product_key
  join marketing.v_dim_mms_Product mmsp 
       on p.dim_mms_product_key = mmsp.dim_mms_product_key
  join #WDCostCenterPromptList cc 
    on mmsp.workday_cost_center = cc.item or cc.item = 'All Cost Centers'
  join #WDOfferingPromptList ofr 
    on mmsp.workday_offering = ofr.item or ofr.item = 'All Offerings'
  join marketing.v_dim_club c 
       on dim_exerp_subscription_period.dim_club_key = c.dim_club_key
  join #DimLocationInfo  clist 
       on c.dim_club_key = clist.DimClubKey
  join marketing.v_dim_description mmsregion 
       on c.region_dim_description_key = mmsregion.dim_description_key
  left join sandbox_ebi.v_fact_exerp_subscription_participation fact_exerp_subscription_participation /* Brian's Magic Table*/
    on dim_exerp_subscription_period.dim_exerp_subscription_period_key = fact_exerp_subscription_participation.dim_exerp_subscription_period_key
  left join marketing.v_dim_exerp_booking bo 
       on fact_exerp_subscription_participation.dim_exerp_booking_key = bo.dim_exerp_booking_key
  left join marketing.v_dim_employee dim_employee
    on fact_exerp_subscription_participation.dim_employee_key = dim_employee.dim_employee_key
  left join marketing.v_dim_mms_member mem 
    on fact_exerp_subscription_participation.dim_mms_member_key = mem.dim_mms_member_key
  where 1=1
 --Exclude Clipcard Subscriptions
    and p.master_product_global_id in ('ALPHA_METCON_MONTHLY', 'ALPHA_STRONG_MONTHLY', 'GTX_BURN_MONTHLY', 'GTX_CUT_MONTHLY','ULTRA_FIT_COURSE_MONTHLY', 'TEAM_TRI_COURSE_MONTHLY', 'PILATES_GROUP_COURSE_MONTHLY','VIRTUAL_PT_MONTHLY','ALPHA_METCON_COURSE_MONTHLY','ALPHA_STRONG_COURSE_MONTHLY','GTX_BURN_COURSE_MONTHLY','GTX_CUT_COURSE_MONTHLY')

select
mmsRegion,
club_code,
club_name,
workday_region,
workday_cost_center,
workday_offering,
exerp_product_name,
mms_product_description,
subscription_id,
dim_exerp_subscription_period_key,
sum(isnull(booking_serviced_this_period,0)) as bookings_serviced_this_period,
sum(isnull(revenue_recognized_this_period,0)) as revenue_recognized_this_period,
sum(isnull(revenue_recognized_this_period_less_ltbucks,0)) as revenue_recognized_this_period_less_ltbucks,
sum(isnull(booking_outstanding,0)) as total_outstanding_booking_count,
sum(isnull(outstanding_booking_revenue,0)) as total_outstanding_booking_revenue,
sum(isnull(refund_amount,0)) as total_refund_amount,
--sum(case when isnull(refund_amount,0) > 0 then (isnull(defr.sub_period_payment_amount,0) - (isnull(defr.revenue_recognized_this_period_less_ltbucks,0) + isnull(defr.refund_amount,0))) else 0 end) trueup_amount,
case when refund_amount > 0 then net_amount - sum(revenue_recognized_this_period_less_ltbucks) + refund_amount else 0 end trueup_amount,
case when period_closed_flag = 1 and sum(booking_serviced) < number_of_bookings and subscription_period_state <> 'cancelled' then (number_of_bookings - sum(booking_serviced)) * price_per_booking_less_lt_bucks else 0 end unbooked_recognized_revenue
from #detail
group by mmsRegion,
club_code,
club_name,
workday_region,
workday_cost_center,
workday_offering,
exerp_product_name,
mms_product_description,
subscription_id,
dim_exerp_subscription_period_key,
net_amount,
refund_amount,
period_closed_flag,
number_of_bookings,
subscription_period_state,
number_of_bookings,
price_per_booking_less_lt_bucks



--select dim_exerp_subscription_key,dim_exerp_subscription_period_key, number_of_bookings, price_per_booking_less_lt_bucks,subscription_period_state, 
--      sum(revenue_recognized_this_period_less_ltbucks) a,
--      sum(outstanding_booking_revenue) b, 
--       case when period_closed_flag = 1 and sum(booking_serviced) < number_of_bookings and subscription_period_state <> 'cancelled' then (number_of_bookings - sum(booking_serviced)) * price_per_booking_less_lt_bucks else 0 end unbooked_recognized
--from #deferred_revenue_new
--group by dim_exerp_subscription_key,dim_exerp_subscription_period_key, number_of_bookings, price_per_booking_less_lt_bucks, period_closed_flag,subscription_period_state

/* Final SELECT Query */
/*
select
defr.mmsRegion,
defr.club_code,
defr.club_name,
defr.workday_region,
defr.workday_cost_center,
defr.workday_offering,
defr.exerp_product_name,
defr.mms_product_description,
defr.subscription_id,
sum(defr.bookings_serviced_this_period) as bookings_serviced_this_period,
sum(defr.revenue_recognized_this_period) as revenue_recognized_this_period,
sum(defr.revenue_recognized_this_period_less_ltbucks) as revenue_recognized_this_period_less_ltbucks,
sum(isnull(defr.total_outstanding_bookings,0)) as total_outstanding_booking_count,
sum(isnull(defr.total_outstanding_booking_revenue,0)) as total_outstanding_booking_revenue,
sum(isnull(defr.refund_amount,0)) as total_refund_amount,
--sum(case when isnull(defr.refund_amount,0) > 0 then (isnull(defr.sub_period_payment_amount,0) - (isnull(defr.revenue_recognized_this_period_less_ltbucks,0) + isnull(defr.refund_amount,0))) else 0 end) trueup_amount,
case when 
sum(isnull(unbooked_recognized_revenue,0)) as unbooked_recognized_revenue
from #detail defr
group by
defr.mmsRegion,
defr.club_code,
defr.club_name,
defr.workday_region,
defr.workday_cost_center,
defr.workday_offering,
defr.exerp_product_name,
defr.mms_product_description,
defr.subscription_id
*/
 
  END
