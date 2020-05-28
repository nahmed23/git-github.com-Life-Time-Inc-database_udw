CREATE PROC [reporting].[proc_ExerpSubscription_Without_Participation] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](4000),@DimLocationKeyList [VARCHAR](4000) AS

BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

----- Sample Execution
 ---   Exec [reporting].[proc_ExerpSubscription_Without_Participation] '7/18/2019','8/24/2019','All Regions','-1'
 --    Exec [reporting].[proc_ExerpSubscription_Without_Participation] '7/18/2019','8/24/2019','All Regions','178'


SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                      THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0)    ----- returns 1st of yesterday's month
					  WHEN @StartDate = 'Dec 30, 1899'
					  THEN DATEADD(YEAR,DATEDIFF(YEAR,0,GETDATE()-1),0)      ----- returns 1st of yesterday's year
					  ELSE @StartDate END
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @EndDate END


DECLARE @StartDimDateKey INT,
        @EndtDimDateKey INT


SET @StartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @EndtDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @EndDate)


--DECLARE @StartDimDateKey VARCHAR(22) = '20190718'
--DECLARE @EndtDimDateKey VARCHAR(22) = '20190824'
--DECLARE @DimClubIDList VARCHAR(4000) = 'BRT|ART'



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



/* Query to find out the Subscription period data within the given Date Range */

IF OBJECT_ID('tempdb.dbo.#subscriptionSales_temp', 'U') IS NOT NULL
drop table #subscriptionSales_temp;

/* Subscription period data for that subscription */
select 
		  c.club_code,
		  c.club_name,
		  c.workday_region,
		  mmsregion.description mmsRegion,
		  s.dim_exerp_subscription_key,
		  s.subscription_id,
		  s.dim_mms_member_key,
		  mem.member_id,
		  mem.customer_name,
		  mem.join_date as member_join_date,
		  mem.membership_id,
		  s.stop_dim_date_key,
		  sp.[dim_exerp_subscription_period_key],
		  sp.from_dim_date_key, 
		  sp.to_dim_date_key, 
		  sp.net_amount sub_period_payment_amount, 
		  sp.lt_bucks_amount as sub_period_lt_bucks_amount,
		  sp.[number_of_bookings] as sub_period_number_of_booking,
		  sp.[price_per_booking] as sub_period_price_per_booking,
		  sp.[price_per_booking_less_lt_bucks] as sub_period_price_per_booking_less_lt_bucks,
		  @StartDimDateKey as FirstOfReportRangeDimDateKey ,
		  @EndtDimDateKey as EndOfReportRangeDimDateKey,
		  /* We are handling the situation where the Reporting Period spand across multiple Subscription pay periods*/
		  case when @StartDimDateKey >= sp.from_dim_date_key then @StartDimDateKey else sp.from_dim_date_key end effective_from_date_key,
		  case when @EndtDimDateKey >= sp.to_dim_date_key then sp.to_dim_date_key else @EndtDimDateKey end effective_to_date_key
		 into #subscriptionSales_temp
  from 
  [marketing].[v_dim_exerp_subscription] s
  inner join [marketing].[v_dim_exerp_subscription_period] sp on s.[dim_exerp_subscription_key]= sp.[dim_exerp_subscription_key]
  inner join [marketing].[v_dim_mms_member] mem on s.dim_mms_member_key = mem.dim_mms_member_key
  inner join marketing.v_dim_club c on s.dim_club_key = c.dim_club_key
  inner join #Clubs clist on c.club_code = clist.club_code
  inner join marketing.v_dim_description mmsregion on c.region_dim_description_key = mmsregion.dim_description_key
  where ((sp.from_dim_date_key between @StartDimDateKey and @EndtDimDateKey) or
  (sp.to_dim_date_key between @StartDimDateKey and @EndtDimDateKey))


  
/* Find All records from #subscriptionSales_temp where we DO have some Participation record for the Subscription Period*/
IF OBJECT_ID('tempdb.dbo.#participation_temp', 'U') IS NOT NULL
drop table #participation_temp;

select par.dim_exerp_subscription_key 
INTO #participation_temp
from 
#subscriptionSales_temp s
inner join marketing.v_fact_exerp_participation par on s.dim_exerp_subscription_key = par.dim_exerp_subscription_key
inner join marketing.v_dim_exerp_booking bo on bo.dim_exerp_booking_key = par.dim_exerp_booking_key
inner join marketing.v_dim_club c on bo.dim_club_key = c.dim_club_key
inner join #Clubs clist on c.club_code = clist.club_code
where bo.start_dim_date_key between s.effective_from_date_key and s.effective_to_date_key

/* Find All records from #subscriptionSales_temp where we DO NOT have some Participation record for the Subscription Period*/

select distinct s.* from 
#subscriptionSales_temp s where dim_exerp_subscription_key not in
(select dim_exerp_subscription_key from #participation_temp)
order by subscription_id 
 

 
  END
