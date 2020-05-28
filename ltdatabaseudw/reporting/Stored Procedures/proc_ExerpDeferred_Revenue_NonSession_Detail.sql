CREATE PROC [reporting].[proc_ExerpDeferred_Revenue_NonSession_Detail] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](4000),@DimLocationKeyList [VARCHAR](4000),@WorkDayOffering [VARCHAR](4000),@WorkDayCostCenter [VARCHAR](4000) AS

BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

----- Sample Execution
 ---   Exec [reporting].[proc_ExerpDeferred_Revenue_NonSession_Detail] '01/01/2020','01/31/2020','All Regions','238','OF10122|OF10056','51015'
 --    Exec [reporting].[proc_ExerpDeferred_Revenue_NonSession_Detail] '01/01/2020','01/31/2020','All Regions','238','All Offerings','All Cost Centers'

----DECLARE @StartDate [DATETIME]
----DECLARE @EndDate [DATETIME]
----DECLARE @StartDimDateKey VARCHAR(22) = '20190718'
----DECLARE @EndtDimDateKey VARCHAR(22) = '20190824'
----DECLARE @DimLocationKeyList VARCHAR(4000) = '178'
----DECLARE @WorkDayOffering VARCHAR(4000) = 'All Offerings'
----DECLARE @WorkDayCostCenter VARCHAR(4000) = 'All Cost Centers'
----DECLARE @RegionList VARCHAR(4000) = 'All Regions'

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


--DECLARE @StartDimDateKey VARCHAR(22) = '20190718'
--DECLARE @EndtDimDateKey VARCHAR(22) = '20190824'
--DECLARE @DimClubIDList VARCHAR(4000) = 'BRT|ART'
--DECLARE @WorkDayOffering VARCHAR(4000) = 'OF10122|OF10056'
--DECLARE @WorkDayCostCenter VARCHAR(4000) = '10005|51015'


/* Parse the Passed Parameters */

 ----- Club Prompt ------  
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL  
  DROP TABLE #Clubs;

DECLARE @list_table VARCHAR(8000)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @DimLocationKeyList,@list_table


	
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


  EXEC marketing.proc_parse_pipe_list @RegionList,@list_table


	
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

EXEC marketing.proc_parse_pipe_list @WorkDayOffering,@list_offering_table



SELECT DISTINCT wd_offering_list.Item
  INTO #WDOfferingPromptList
  FROM #wd_offering_list  wd_offering_list


 --- Workday Cost Center Prompt ------  
IF OBJECT_ID('tempdb.dbo.#WDCostCenterPromptList', 'U') IS NOT NULL
  DROP TABLE #WDCostCenterPromptList;   

DECLARE @list_costcenter_table VARCHAR(100)
SET @list_costcenter_table = 'wd_costcenter_list'

EXEC marketing.proc_parse_pipe_list @WorkDayCostCenter,@list_costcenter_table

SELECT DISTINCT wd_costcenter_list.Item
  INTO #WDCostCenterPromptList
  FROM #wd_costcenter_list  wd_costcenter_list

  /******************** BRIAN'S CODE - BEGIN *************************/

IF OBJECT_ID('tempdb.dbo.#deferred_revenue', 'U') IS NOT NULL
drop table #deferred_revenue;

create table #deferred_revenue with (distribution = round_robin, heap) as
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
    join marketing.v_dim_exerp_product p on sp1.dim_exerp_product_key = p.dim_exerp_product_key
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
        fact_exerp_subscription_participation.participation_id
  from sub_per dim_exerp_subscription_period
  join marketing.v_fact_exerp_subscription_participation fact_exerp_subscription_participation /* Brian's Magic Table*/
    on dim_exerp_subscription_period.dim_exerp_subscription_period_key = fact_exerp_subscription_participation.dim_exerp_subscription_period_key
  join marketing.v_dim_exerp_subscription dim_exerp_subscription 
    on dim_exerp_subscription_period.dim_exerp_subscription_key = dim_exerp_subscription.dim_exerp_subscription_key
  join marketing.v_dim_exerp_booking bo 
    on fact_exerp_subscription_participation.dim_exerp_booking_key = bo.dim_exerp_booking_key
  join marketing.v_dim_exerp_product p 
    on fact_exerp_subscription_participation.dim_exerp_product_key = p.dim_exerp_product_key
  left join marketing.v_dim_mms_Product mmsp 
    on fact_exerp_subscription_participation.dim_mms_product_key = mmsp.dim_mms_product_key
  join marketing.v_dim_club c 
    on fact_exerp_subscription_participation.dim_club_key = c.dim_club_key
  join marketing.v_dim_description mmsregion 
    on c.region_dim_description_key = mmsregion.dim_description_key
  join marketing.v_dim_employee dim_employee
    on fact_exerp_subscription_participation.dim_employee_key = dim_employee.dim_employee_key
  join marketing.v_dim_mms_member mem on fact_exerp_subscription_participation.dim_mms_member_key = mem.dim_mms_member_key
  where 1=1


  /******************** BRIAN's CODE - END ***************************/

  select * from #deferred_revenue

 
  END

