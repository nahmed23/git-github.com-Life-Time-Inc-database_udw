CREATE PROC [reporting].[proc_RecurrentProductForecastDetail] @AssessmentDates [VARCHAR](8000),@RegionList [VARCHAR](2000),@ClubIDList [VARCHAR](8000),@DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 

---- Sample Execution
-- EXEC [reporting].[proc_RecurrentProductForecastDetail] '10/26/2019|10/27/2019|10/28/2019', 'Hall-MN-West|Messerli-Mountain|Logan-Ohio', '151|195|197','Personal Training','All Subdivisions','All Departments'
----

DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (SELECT Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
						  FROM map_utc_time_zone_conversion
						  WHERE getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')


	
IF OBJECT_ID('tempdb.dbo.#Dates', 'U') IS NOT NULL
  DROP TABLE #Dates;   

DECLARE @list_table VARCHAR(100)
SET @list_table = 'date_list'

EXEC marketing.proc_parse_pipe_list @AssessmentDates,@list_table

SELECT DISTINCT DimDate.calendar_date  AS AssessmentDate, 
   DimDate.day_number_in_month AS AssessmentDay,
   NextMonthDimDate.calendar_date  AS NextMonthAssessmentDate,
   DimDate.dim_date_key AS AssessmentDateDimDateKey,
   NextMonthDimDate.dim_date_key AS NextMonthAssessmentDimDateKey
  INTO #Dates    
  FROM #date_list date_list
   JOIN [marketing].[v_dim_date] DimDate
     ON CAST(date_list.Item AS Date) = DimDate.calendar_date
   JOIN [marketing].[v_dim_date] NextMonthDimDate
     ON CAST(DateAdd(mm,1,DimDate.calendar_date) AS Date) = NextMonthDimDate.calendar_date



DECLARE @MinAssessmentDate DATETIME
DECLARE @FirstOfCurrentMonth DateTime
DECLARE @FirstOfNextMonth DateTime
DECLARE @StartMonthStartingDimDateKey VARCHAR(32)
DECLARE @EndMonthEndingDimDateKey  VARCHAR(32)
DECLARE @MinAssessmentDateDimDateKey  VARCHAR(32)
DECLARE @RunDateFirstOfMonth DateTime
 
SET @MinAssessmentDate = (SELECT MIN(AssessmentDate) FROM #Dates)
SET @FirstOfCurrentMonth = (SELECT month_starting_date FROM [marketing].[v_dim_date] where calendar_date = @MinAssessmentDate )
SET @FirstOfNextMonth = (SELECT next_month_starting_date FROM [marketing].[v_dim_date] where calendar_date = @MinAssessmentDate )
SET @StartMonthStartingDimDateKey  = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] where calendar_date = @MinAssessmentDate )
SET @EndMonthEndingDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] where month_starting_dim_date_key = @StartMonthStartingDimDateKey AND last_day_in_month_flag = 'Y' )
SET @MinAssessmentDateDimDateKey =  (SELECT dim_date_key FROM [marketing].[v_dim_date] where calendar_date = @MinAssessmentDate)    
SET @RunDateFirstOfMonth = (Select month_starting_date FROM [marketing].[v_dim_date] where calendar_date = Cast(getdate() AS Date))

Exec [reporting].[proc_DimReportingHierarchy_history] @DivisionList,@SubdivisionList,@DepartmentMinDimReportingHierarchyKeyList,'N/A',@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey 
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 

 SELECT DimReportingHierarchyKey,  
       DivisionName,    
       SubdivisionName,
       DepartmentName,
	   ReportRegionType,
	   CASE WHEN ProductGroupName IN('Weight Loss Challenges','90 Day Weight Loss')
	        THEN 'Y'
		    ELSE 'N'
		END PTDeferredRevenueProductGroupFlag
 INTO #DimReportingHierarchy      
 FROM #OuterOutputTable

  IF OBJECT_ID('tempdb.dbo.#RegionTypes', 'U') IS NOT NULL
  DROP TABLE #RegionTypes; 

SELECT ReportRegionType AS RegionType
INTO #RegionTypes   
FROM #DimReportingHierarchy
GROUP BY ReportRegionType


  ---- Set variable to return just one region type
DECLARE @RegionType VARCHAR(50)
SET @RegionType = (SELECT CASE WHEN COUNT(*) = 1 THEN MIN(RegionType) ELSE 'MMS Region' END FROM #RegionTypes)


----- When All Regions and All Clubs are selection options, and the Regions could be of different types
 -----  must take a 2 step approach
 ----- This query replaces the function used in LTFDM_Revenue "fnRevenueHistoricalDimLocation"
 ----- This report only looks at current month so historical club attributes are not needed.
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL  
  DROP TABLE #Clubs;

  ----- Create club temp table

SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @ClubIDList,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey,    ------ note new name
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   DimClub.club_status AS ClubStatusDescription,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   ClubOpenDate.calendar_date AS ClubOpenDate,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  INTO #Clubs    
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubIDList
    ON ClubIDList.Item = DimClub.club_id
	  OR ClubIDList.Item = -1     
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
  JOIN [marketing].[v_dim_date] ClubOpenDate
   ON DimClub.club_open_dim_date_key = ClubOpenDate.dim_date_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND DimClub.club_status in('Open','Presale')
GROUP BY DimClub.dim_club_key,
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimClub.club_status,
	   DimClub.local_currency_code,
	   ClubOpenDate.calendar_date,
	   MMSRegion.description,
	   PTRCLRegion.description,
	   MemberActivitiesRegion.description

IF OBJECT_ID('tempdb.dbo.#DimLocationInfo', 'U') IS NOT NULL
  DROP TABLE #DimLocationInfo;

  ----- Create Region temp table
SET @list_table = 'region_list'

  EXEC marketing.proc_parse_pipe_list @RegionList,@list_table
	
SELECT DimClub.DimClubKey,      ------ name change
      CASE WHEN @RegionType = 'PT RCL Area' 
             THEN DimClub.PTRCLRegion
           WHEN @RegionType = 'Member Activities Region' 
             THEN DimClub.MemberActivitiesRegion
           WHEN @RegionTYpe = 'MMS Region' 
             THEN DimClub.MMSRegion 
		   END  RevenueReportingRegionName,
       DimClub.ClubName AS MMSClubName,
	   DimClub.club_id AS MMSClubID,
	   DimClub.LocalCurrencyCode,
	   DimClub.ClubStatusDescription,
	   DimClub.ClubOpenDate,
	   DimClub.club_code AS ClubCode
  INTO #DimLocationInfo      
  FROM #Clubs DimClub     
  JOIN #region_list RegionList 
   ON RegionList.Item = CASE WHEN @RegionType = 'PT RCL Area' 
                                   THEN DimClub.PTRCLRegion
                              WHEN @RegionType = 'Member Activities Region' 
                                   THEN DimClub.MemberActivitiesRegion
                              WHEN @RegionTYpe = 'MMS Region' 
                                   THEN DimClub.MMSRegion END
     OR RegionList.Item = 'All Regions'
 GROUP BY DimClub.DimClubKey, 
      CASE WHEN @RegionType = 'PT RCL Area' 
             THEN DimClub.PTRCLRegion
           WHEN @RegionType = 'Member Activities Region' 
             THEN DimClub.MemberActivitiesRegion
           WHEN @RegionTYpe = 'MMS Region' 
             THEN DimClub.MMSRegion 
		   END,
       DimClub.ClubName,
	   DimClub.club_id,
	   DimClub.LocalCurrencyCode,
	   DimClub.ClubStatusDescription,
	   DimClub.ClubOpenDate,
	   DimClub.club_code

 -------
 ------- Exerp specific code logic
 -------
 IF OBJECT_ID('tempdb.dbo.#NextAssessmentDate_Prelim', 'U') IS NOT NULL
DROP TABLE #NextAssessmentDate_Prelim;

----- To Determine next assessment date on subscriptions based on subscription periods
-----  Assumption - payment is 1 month in advance of subscriptionPeriodTo
----- This will eliminate subscriptions which have already ended in the prior month

SELECT sp.dim_exerp_subscription_key,
       MAX(sp.from_dim_date_key) AS from_dim_date_key,
       MAX(sp.to_dim_date_key) AS to_dim_date_key,
	   MAX(DateAdd(day,1,ToDimDate.calendar_date)) AS NextAssessment,
	   Day(MAX(DateAdd(day,1,ToDimDate.calendar_date))) AS AssessmentDayOfMonth
INTO #NextAssessmentDate_Prelim     
FROM [marketing].[v_dim_exerp_subscription_period] sp 
  JOIN [marketing].[v_dim_date] ToDimDate
    ON sp.to_dim_date_key = ToDimDate.dim_date_key
  JOIN [marketing].[v_dim_exerp_subscription] Subscription
    ON sp.dim_exerp_subscription_key = Subscription.dim_exerp_subscription_key
  
Where ToDimDate.calendar_date >= DateAdd(day,-1,@RunDateFirstOfMonth)   ----- to return all subscriptions that are set to assess in the current month and beyond
  AND (Subscription.end_dim_date_key = '-998' 
     OR Subscription.end_dim_date_key >= @MinAssessmentDateDimDateKey) ----not terminated or terminated after earliest selected assessment date
GROUP BY sp.dim_exerp_subscription_key


 ---- to limit the temp table to just the DOM selected for the report
 IF OBJECT_ID('tempdb.dbo.#NextAssessmentDate', 'U') IS NOT NULL
DROP TABLE #NextAssessmentDate;

Select dim_exerp_subscription_key,
       from_dim_date_key,
       to_dim_date_key,
	   NextAssessment,
	   AssessmentDayOfMonth
INTO #NextAssessmentDate  
FROM #NextAssessmentDate_Prelim 
   JOIN #Dates 
    ON #NextAssessmentDate_Prelim.AssessmentDayOfMonth = #Dates.AssessmentDay 


 ---- to find the last change log record for the subscriptions yet to assess
  IF OBJECT_ID('tempdb.dbo.#LastChangeLog', 'U') IS NOT NULL
DROP TABLE #LastChangeLog;

 SELECT ChangeLog.dim_exerp_subscription_key,
        MAX(ChangeLog.subscription_change_log_id) subscription_change_log_id
 INTO #LastChangeLog
 FROM [marketing].[v_dim_exerp_subscription_change_log] ChangeLog
   JOIN #NextAssessmentDate  NextAssessment
     ON ChangeLog.dim_exerp_subscription_key = NextAssessment.dim_exerp_subscription_key
GROUP BY ChangeLog.dim_exerp_subscription_key
--Order by ChangeLog.dim_exerp_subscription_key

--- Get the Commissionable Employee from the latest Subscription_change_log record
  IF OBJECT_ID('tempdb.dbo.#LatestCommissionableEmployee', 'U') IS NOT NULL
DROP TABLE #LatestCommissionableEmployee;

 select 
    ChangeLog.dim_exerp_subscription_key,
	e.dim_employee_key,
	e.[employee_id],
	e.[last_name],
	e.[first_name],
	e.[middle_name],
	CASE WHEN (ISNULL(ChangeLog.dim_employee_key,'-998') <> '-998' ) 
	        THEN E.last_name +', '+ E.first_name 
			ELSE 'None Designated' 
			END CommisionedEmployee 
 INTO #LatestCommissionableEmployee
 FROM [marketing].[v_dim_exerp_subscription_change_log] ChangeLog
 JOIN #LastChangeLog last_ChangeLog on ChangeLog.subscription_change_log_id = last_ChangeLog.subscription_change_log_id
 JOIN [marketing].[v_dim_employee] e on ChangeLog.dim_employee_key = e.dim_employee_key



Select DimClub.RevenueReportingRegionName AS Region,
       DimClub.ClubCode,
	   DimClub.MMSClubID AS ClubID, 
	   DimClub.MMSClubName AS ClubName, 
	   DimReportingHierarchy.DepartmentName AS Department,
	   DimReportingHierarchy.DimReportingHierarchyKey,
	   Member.member_id AS MemberID,
	   Membership.membership_status AS MembershipStatus,
	   Member.first_name AS MemberFirstName, 
	   Member.last_name AS MemberLastName,
	   '' AS  MemberMiddleName,
	   --Membership.membership_type AS MembershipType,
	   mship_type.product_description AS MembershipType,
	   DimMMSProduct.product_description AS ProductDescription,
	   DimMMSProduct.product_id AS ProductID,
	   CASE WHEN (RecurrentProductSubscription.end_dim_date_key = '-998' or RecurrentProductSubscription.end_dim_date_key >= #Dates.AssessmentDateDimDateKey)
	          AND (Freeze.start_dim_date_key is Null 
			         OR #Dates.AssessmentDateDimDateKey < Freeze.start_dim_date_key 
					 OR #Dates.AssessmentDateDimDateKey > Freeze.end_dim_date_key )
			THEN  RecurrentProductSubscription.Price
			ELSE 0
			END ThisMonthAmount,
       CASE WHEN (RecurrentProductSubscription.end_dim_date_key = '-998' or RecurrentProductSubscription.end_dim_date_key >= #Dates.NextMonthAssessmentDimDateKey)
	        AND (Freeze.start_dim_date_key is Null 
			         OR #Dates.NextMonthAssessmentDimDateKey < Freeze.start_dim_date_key 
					 OR #Dates.NextMonthAssessmentDimDateKey > Freeze.end_dim_date_key )
			THEN RecurrentProductSubscription.Price
			ELSE 0
			END NextMonthAmount,
	   SubscriptionStartDate.calendar_date as ActivationDate_Sort,
	   SubscriptionStartDate.standard_date_name as ActivationDate,
	   CASE WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
	        THEN NULL 
			ELSE SubscriptionEndDate.calendar_date
			END AS TerminationDate_Sort,
	   CASE WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
	        THEN 'No date end' 
			ELSE SubscriptionEndDate.standard_date_name
			END AS TerminationDate, 
        CASE WHEN IsNull(FreezeStartDate.dim_date_key,'-998') = '-998'
	        THEN NULL 
			ELSE FreezeStartDate.calendar_date
			END AS ProductHoldBeginDate_Sort, 
	   FreezeStartDate.standard_date_name as ProductHoldBeginDate,
        CASE WHEN IsNull(FreezeEndDate.dim_date_key,'-998') = '-998'
	        THEN NULL 
			ELSE FreezeEndDate.calendar_date
			END AS ProductHoldEndDate_Sort,
	   FreezeEndDate.standard_date_name as ProductHoldEndDate,
	   CASE WHEN IsNull(Freeze.start_dim_date_key,'-998') = '-998'
	        THEN CASE WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE DATEDIFF(month, @FirstOfCurrentMonth,SubscriptionEndDate.calendar_date)
					  END
			WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
			THEN 0
			WHEN Freeze.end_dim_date_key < #Dates.AssessmentDateDimDateKey
			THEN CASE WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE DATEDIFF(month, @FirstOfCurrentMonth,SubscriptionEndDate.calendar_date)
					  END
			WHEN Freeze.end_dim_date_key >= #Dates.AssessmentDateDimDateKey
			THEN CASE WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE (DATEDIFF(month, @FirstOfCurrentMonth,SubscriptionEndDate.calendar_date)) - DATEDIFF(month, #Dates.AssessmentDate, FreezeEndDate.calendar_date)
			          END
			END NumberOfMonthsLeft,
	   CASE WHEN IsNull(Freeze.start_dim_date_key,'-998') = '-998'
	        THEN CASE WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE (DATEDIFF(month, @FirstOfCurrentMonth,SubscriptionEndDate.calendar_date))*RecurrentProductSubscription.Price
					  END
			WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
			THEN 0
			WHEN Freeze.end_dim_date_key < #Dates.AssessmentDateDimDateKey
			THEN CASE WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE (DATEDIFF(month, @FirstOfCurrentMonth,SubscriptionEndDate.calendar_date))*RecurrentProductSubscription.Price
					  END
			WHEN Freeze.end_dim_date_key >= #Dates.AssessmentDateDimDateKey
			THEN CASE WHEN IsNull(RecurrentProductSubscription.end_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE (DATEDIFF(month, @FirstOfCurrentMonth,SubscriptionEndDate.calendar_date)*RecurrentProductSubscription.Price) - (DATEDIFF(month, #Dates.AssessmentDate, FreezeEndDate.calendar_date)*RecurrentProductSubscription.Price)
			          END
			END TotalAmountLeft,
	   ISNULL(comm_employee.CommisionedEmployee,'None Designated') CommisionedEmployee,
	   comm_employee.employee_id AS CommissionEmployeeID,
	   Cast(SubscriptionCreatedDate.calendar_date as varchar) +' '+ cast(SubscriptionCreatedTime.display_12_hour_time AS varchar) AS CreatedDateTime, 
	   comm_employee.last_name AS EmployeeLastName, 
	   comm_employee.first_name AS EmployeeFirstName, 
	   '' AS EmployeeMiddleInt, 
	   Replace(@AssessmentDates,'|',', ') AS HeaderAssessmentDates,
       '' AS HeaderDepartmentList,   ------- must be created within Cognos
       'Local Currency' as ReportingCurrencyCode,
       @ReportRunDateTime AS ReportRunDateTime,
	   Membership.membership_expiration_date AS MembershipTerminationDate,
	   NextAssessment.AssessmentDayOfMonth AS AssessmentDayOfMonth

FROM [marketing].[v_dim_exerp_subscription] RecurrentProductSubscription
JOIN #NextAssessmentDate  NextAssessment
  ON RecurrentProductSubscription.dim_exerp_subscription_key = NextAssessment.dim_exerp_subscription_key
LEFT JOIN #LatestCommissionableEmployee comm_employee on NextAssessment.dim_exerp_subscription_key = comm_employee.dim_exerp_subscription_key
JOIN [marketing].[v_dim_date] NextAssessmentDate
  ON NextAssessment.NextAssessment = NextAssessmentDate.calendar_date
JOIN #Dates 
  ON NextAssessmentDate.dim_date_key = #Dates.AssessmentDateDimDateKey
JOIN #DimLocationInfo DimClub
  ON RecurrentProductSubscription.dim_club_key = DimClub.DimClubKey 
LEFT JOIN [marketing].[v_dim_exerp_product] DimExerpProduct
  ON RecurrentProductSubscription.dim_exerp_product_key = DimExerpProduct.dim_exerp_product_key
LEFT JOIN [marketing].[v_dim_mms_product] DimMMSProduct
  ON DimExerpProduct.dim_mms_product_key = DimMMSProduct.dim_mms_product_key
JOIN #DimReportingHierarchy DimReportingHierarchy
    ON DimReportingHierarchy.DimReportingHierarchyKey = DimMMSProduct.dim_reporting_hierarchy_key
LEFT JOIN [marketing].[v_dim_mms_member] Member
  ON RecurrentProductSubscription.dim_mms_member_key = Member.dim_mms_member_key
LEFT JOIN [marketing].[v_dim_mms_membership] Membership                                 
  ON Member.dim_mms_membership_key = Membership.dim_mms_membership_key
LEFT JOIN [marketing].[v_dim_mms_product] mship_type 
  ON Membership.membership_type_id = mship_type.product_id
LEFT JOIN [marketing].[v_dim_exerp_freeze_period] Freeze
  ON RecurrentProductSubscription.dim_exerp_subscription_key = Freeze.dim_exerp_subscription_key
  AND Freeze.cancel_dim_date_key in('-997','-998','-999')
JOIN [marketing].[v_dim_date] SubscriptionStartDate
  ON RecurrentProductSubscription.start_dim_date_key = SubscriptionStartDate.dim_date_key
JOIN [marketing].[v_dim_date] SubscriptionEndDate
  ON RecurrentProductSubscription.end_dim_date_key = SubscriptionEndDate.dim_date_key
LEFT JOIN [marketing].[v_dim_date] FreezeStartDate
  ON Freeze.start_dim_date_key = FreezeStartDate.dim_date_key
LEFT JOIN [marketing].[v_dim_date] FreezeEndDate
  ON Freeze.end_dim_date_key = FreezeEndDate.dim_date_key
JOIN [marketing].[v_dim_date] SubscriptionCreatedDate
  ON RecurrentProductSubscription.creation_dim_date_key = SubscriptionCreatedDate.dim_date_key
JOIN [marketing].[v_dim_time] SubscriptionCreatedTime
  ON RecurrentProductSubscription.creation_dim_time_key = SubscriptionCreatedTime.dim_time_key
Where 1=1
AND RecurrentProductSubscription.Price > 0       ---- price is greater than $0
AND (RecurrentProductSubscription.end_dim_date_key = '-998' 
     or RecurrentProductSubscription.end_dim_date_key >= @EndMonthEndingDimDateKey)   ----not terminated or terminated in future month
AND NextAssessmentDate.dim_date_key > RecurrentProductSubscription.billed_until_dim_date_key           ---- not pre-paid for the remaining dates in the month
AND (Member.member_active_flag = 'Y' or  Member.member_active_flag is Null)  ----- member is currently active - assuming null is active
AND Membership.membership_status <> 'Suspended'   ----- membership is currently not suspended   
AND Membership.membership_status <> 'Terminated'   ----- membership is currently not suspended 


UNION ALL

SELECT 
       C.RevenueReportingRegionName AS Region,
	   C.ClubCode,	
	   C.MMSClubID AS ClubID, 
	   C.MMSClubName AS ClubName, 
	   DimReportingHierarchy.DepartmentName AS Department, 
	   DimReportingHierarchy.DimReportingHierarchyKey,
       -- report columns  
	   M.member_id AS MemberID, 
       Membership.membership_status AS MembershipStatus,
	   M.first_name AS MemberFirstName, 
	   M.last_name AS MemberLastName,
	   '' AS  MemberMiddleName,
       --Membership.membership_type AS MembershipType,
	   mship_type.product_description AS MembershipType,
	   P.product_description AS ProductDescription,
	   P.product_id AS ProductID,
	   ISNULL(CASE WHEN (MRP.activation_dim_date_key <= #Dates.AssessmentDateDimDateKey 
                          AND (MRP.termination_dim_date_key = '-998' OR IsNull(MRP.termination_dim_date_key,99991231) >= #Dates.AssessmentDateDimDateKey))
						  AND (MRP.hold_start_dim_date_key = '-998'
		                        OR (IsNull(MRP.hold_start_dim_date_key,19000101)< #Dates.AssessmentDateDimDateKey AND IsNull(MRP.hold_end_dim_date_key,19000101)< #Dates.AssessmentDateDimDateKey)
		                        OR (IsNull(MRP.hold_start_dim_date_key,99991231)> #Dates.AssessmentDateDimDateKey AND IsNull(MRP.hold_end_dim_date_key,99991231)> #Dates.AssessmentDateDimDateKey)
			                   )
				   THEN MRP.Price  
				   END,0) ThisMonthAmount,
	   ISNULL(CASE WHEN (MRP.activation_dim_date_key <= #Dates.NextMonthAssessmentDimDateKey 
	                      AND (MRP.termination_dim_date_key = '-998' OR IsNull(MRP.termination_dim_date_key,99991231) >= #Dates.NextMonthAssessmentDimDateKey ))
						  AND (MRP.hold_start_dim_date_key = '-998'
		                       OR (IsNull(MRP.hold_start_dim_date_key,19000101)< #Dates.NextMonthAssessmentDimDateKey AND IsNull(MRP.hold_end_dim_date_key,19000101)< #Dates.NextMonthAssessmentDimDateKey)
		                       OR (IsNull(MRP.hold_start_dim_date_key,99991231)> #Dates.NextMonthAssessmentDimDateKey AND IsNull(MRP.hold_end_dim_date_key,99991231)> #Dates.NextMonthAssessmentDimDateKey)
                              )
				   THEN MRP.Price 
				   END,0) NextMonthAmount,
	   ActivationDate.calendar_date as ActivationDate_Sort,
	   ActivationDate.standard_date_name as ActivationDate,
	   CASE WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
	        THEN NULL 
			ELSE TerminationDate.calendar_date
			END AS TerminationDate_Sort,
	   CASE WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
	        THEN 'No date end' 
			ELSE TerminationDate.standard_date_name
			END AS TerminationDate,    
       	CASE WHEN IsNull(MRP.hold_start_dim_date_key,'-998') = '-998'
	        THEN NULL 
			ELSE ProductHoldStartDate.calendar_date
			END AS ProductHoldBeginDate_Sort, 
	   ProductHoldStartDate.standard_date_name as ProductHoldBeginDate,
       	CASE WHEN IsNull(MRP.hold_end_dim_date_key,'-998') = '-998'
	        THEN NULL 
			ELSE ProductHoldEndDate.calendar_date
			END AS ProductHoldEndDate_Sort, 
	   ProductHoldEndDate.standard_date_name as ProductHoldEndDate,
	   CASE WHEN IsNull(MRP.hold_start_dim_date_key,'-998') = '-998'
	        THEN CASE WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE DATEDIFF(month, @FirstOfCurrentMonth,TerminationDate.calendar_date)
					  END
			WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
			THEN 0
			WHEN MRP.hold_end_dim_date_key < #Dates.AssessmentDateDimDateKey
			THEN CASE WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE DATEDIFF(month, @FirstOfCurrentMonth,TerminationDate.calendar_date)
					  END
			WHEN MRP.hold_end_dim_date_key >= #Dates.AssessmentDateDimDateKey
			THEN CASE WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE (DATEDIFF(month, @FirstOfCurrentMonth,TerminationDate.calendar_date)) - DATEDIFF(month, #Dates.AssessmentDate, ProductHoldEndDate.calendar_date)
			          END
			END NumberOfMonthsLeft,
	   CASE WHEN IsNull(MRP.hold_start_dim_date_key,'-998') = '-998'
	        THEN CASE WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE (DATEDIFF(month, @FirstOfCurrentMonth, TerminationDate.calendar_date)) * MRP.Price
					  END
			WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
			THEN 0
			WHEN MRP.hold_end_dim_date_key < #Dates.AssessmentDateDimDateKey
			THEN CASE WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE (DATEDIFF(month, @FirstOfCurrentMonth, TerminationDate.calendar_date)) * MRP.Price
					  END
			WHEN MRP.hold_end_dim_date_key >= #Dates.AssessmentDateDimDateKey
			THEN CASE WHEN IsNull(MRP.termination_dim_date_key,'-998') = '-998'
			          THEN 0
					  ELSE (DATEDIFF(month, @FirstOfCurrentMonth,TerminationDate.calendar_date) * MRP.Price) - (DATEDIFF(month, #Dates.AssessmentDate,ProductHoldEndDate.calendar_date)* MRP.Price)
					  END
			END TotalAmountLeft,
	   CASE WHEN (ISNULL(MRP.commission_dim_mms_employee_key,'-998') <> '-998' AND MRP.commission_employee_id <> 0) 
	        THEN E.last_name +', '+ E.first_name 
			ELSE ' None Designated' 
			END CommisionedEmployee,     
       MRP.commission_employee_id AS CommissionEmployeeID,
	   Cast(CreatedDate.calendar_date as varchar) +' '+ cast(CreatedTime.display_12_hour_time AS varchar) AS CreatedDateTime, 
       E.last_name AS EmployeeLastName, 
	   E.first_name AS EmployeeFirstName, 
	   '' AS EmployeeMiddleInt,      	   
       Replace(@AssessmentDates,'|',', ') AS HeaderAssessmentDates,
       '' AS HeaderDepartmentList,   ------- must be created within Cognos
       'Local Currency' as ReportingCurrencyCode,
       @ReportRunDateTime AS ReportRunDateTime,
	   Membership.membership_expiration_date AS MembershipTerminationDate,
       MRP.assessment_day_of_month AS AssessmentDayOfMonth
  FROM [marketing].[v_fact_mms_membership_recurrent_product] MRP
  JOIN #Dates 
    ON MRP.assessment_day_of_month = #Dates.AssessmentDay
  JOIN [marketing].[v_dim_date] ActivationDate
    ON MRP.activation_dim_date_key = ActivationDate.dim_date_key
  JOIN [marketing].[v_dim_date] CreatedDate
    ON MRP.created_dim_date_key = CreatedDate.dim_date_key
  JOIN [marketing].[v_dim_time] CreatedTime
    ON MRP.created_dim_time_key = CreatedTime.dim_time_key
  LEFT JOIN [marketing].[v_dim_date] TerminationDate
    ON MRP.termination_dim_date_key = TerminationDate.dim_date_key
  LEFT JOIN [marketing].[v_dim_date] ProductHoldStartDate
    ON MRP.hold_start_dim_date_key = ProductHoldStartDate.dim_date_key
  LEFT JOIN [marketing].[v_dim_date] ProductHoldEndDate
    ON MRP.hold_end_dim_date_key = ProductHoldEndDate.dim_date_key
  JOIN #DimLocationInfo C
    ON C.DimClubKey  = MRP.dim_club_key  
  JOIN [marketing].[v_dim_mms_product] P
    ON P.dim_mms_product_key = MRP.dim_mms_product_key
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON DimReportingHierarchy.DimReportingHierarchyKey = P.dim_reporting_hierarchy_key
  LEFT JOIN [marketing].[v_dim_employee] E
    ON E.dim_employee_key = MRP.commission_dim_mms_employee_key
  JOIN [marketing].[v_dim_mms_member] M
    ON M.dim_mms_member_key = MRP.dim_mms_member_key
  JOIN [marketing].[v_dim_mms_membership] Membership
    ON M.dim_mms_membership_key = Membership.dim_mms_membership_key
  LEFT JOIN [marketing].[v_dim_mms_product] mship_type 
  ON Membership.membership_type_id = mship_type.product_id

WHERE ((MRP.activation_dim_date_key <= #Dates.AssessmentDateDimDateKey AND IsNull(MRP.termination_dim_date_key,'-998') = '-998')
       OR
      (MRP.activation_dim_date_key <= #Dates.AssessmentDateDimDateKey AND IsNull(MRP.termination_dim_date_key,'99991231') >= #Dates.AssessmentDateDimDateKey)
       OR
	   (ActivationDate.calendar_date <= #Dates.NextMonthAssessmentDate AND IsNull(MRP.termination_dim_date_key,'-998') = '-998')
	   OR
       (ActivationDate.calendar_date <= #Dates.NextMonthAssessmentDate AND IsNull(TerminationDate.calendar_date,'12/31/9999') >= #Dates.NextMonthAssessmentDate))




	DROP TABLE #DimLocationInfo 
	DROP TABLE #Clubs 
	DROP TABLE #RegionTypes 
	DROP TABLE #Dates
	DROP TABLE #DimReportingHierarchy
	DROP TABLE #NextAssessmentDate_Prelim
	DROP TABLE #NextAssessmentDate



END
