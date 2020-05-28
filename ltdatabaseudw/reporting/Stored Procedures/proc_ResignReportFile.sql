CREATE PROC [reporting].[proc_ResignReportFile] @StartFourDigitYearDashTwoDigitMonth [Varchar](7),@ReportLookBackMonths [Varchar](15),@RegionList [VARCHAR](4000),@DimMMSClubID [VARCHAR](4000),@DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000) AS

BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



--------- Exec [reporting].[proc_ResignReportFile] '2015-06','3 Months','All Regions','10|52','Personal Training','All Subdivisions','All Departments'

 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                          from map_utc_time_zone_conversion where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

----  Setting up date range variables
DECLARE @EndMonthCalendarStartingDate Datetime,
        @EndMonthEndingDimDateKey INT

SELECT @EndMonthCalendarStartingDate = month_starting_date,
       @EndMonthEndingDimDateKey = month_ending_dim_date_key
FROM [marketing].[v_dim_date]
WHERE four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth


DECLARE @StartMonthStartingDimDateKey INT

SELECT @StartMonthStartingDimDateKey  = Month_Starting_Dim_Date_Key
FROM [marketing].[v_dim_date] Dim_Date
WHERE Dim_Date.Calendar_Date in 
(case when @ReportLookBackMonths = '3 Months' then DateAdd(Month,-3,@EndMonthCalendarStartingDate)  
  when @ReportLookBackMonths = '6 Months' then DateAdd(Month,-6,@EndMonthCalendarStartingDate) 
   when @ReportLookBackMonths = '9 Months' then DateAdd(Month,-9,@EndMonthCalendarStartingDate)
    when @ReportLookBackMonths = '12 Months' then DateAdd(Month,-12,@EndMonthCalendarStartingDate)
       else  @EndMonthCalendarStartingDate end )


------ Determine revenue hierarchies based on selected Division, Subdivision & Department
Exec [reporting].[proc_DimReportingHierarchy_History] @DivisionList,@SubdivisionList,@DepartmentMinDimReportingHierarchyKeyList,'N/A',@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey 
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
 DROP TABLE #DimReportingHierarchy; 

 SELECT DimReportingHierarchyKey,
       DivisionName,
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   RegionType,
	   ReportRegionType
 INTO #DimReportingHierarchy
 FROM #OuterOutputTable
	  

DECLARE @HeaderDivisionList VARCHAR(8000),
        @HeaderSubdivisionList VARCHAR(8000),
        @RevenueReportingDepartmentNameCommaList VARCHAR(8000),
        @RegionType VARCHAR(50)
		
		
IF OBJECT_ID('tempdb.dbo.#HeaderDivisionList', 'U') IS NOT NULL
DROP TABLE #HeaderDivisionList; 	

exec [marketing].[proc_parse_pipe_list] @DivisionList, 'HeaderDivisionList'

IF OBJECT_ID('tempdb.dbo.#HeaderSubdivisionList', 'U') IS NOT NULL
DROP TABLE #HeaderSubdivisionList; 	

exec [marketing].[proc_parse_pipe_list] @SubdivisionList, 'HeaderSubdivisionList'

IF OBJECT_ID('tempdb.dbo.#RevenueReportingDepartmentNameCommaList', 'U') IS NOT NULL
DROP TABLE #RevenueReportingDepartmentNameCommaList; 	

exec [marketing].[proc_parse_pipe_list] @DepartmentMinDimReportingHierarchyKeyList, 'RevenueReportingDepartmentNameCommaList'

		
SELECT @HeaderDivisionList = CASE WHEN 'All Divisions' IN (SELECT item FROM #HeaderDivisionList) THEN 'All Divisions'
                                  ELSE (SELECT MIN(item) FROM #HeaderDivisionList) END,
       @HeaderSubdivisionList = CASE WHEN 'All Subdivisions' IN (SELECT item FROM #HeaderSubdivisionList) THEN 'All Subdivisions'
                                     ELSE (SELECT MIN(item) FROM #HeaderSubdivisionList) END,
       @RevenueReportingDepartmentNameCommaList = CASE WHEN 'All Departments' IN (SELECT item FROM #RevenueReportingDepartmentNameCommaList) THEN 'All Departments'
                                                       ELSE (SELECT MIN(item) FROM #RevenueReportingDepartmentNameCommaList) END,                                        
       @RegionType = (SELECT MIN(ReportRegionType) FROM #DimReportingHierarchy)
   


 ----- When All Regions and All Clubs are selection options, and the Regions could be of different types
 -----  must take a 2 step approach
 ----- This query replaces the function used in LTFDM_Revenue "fnRevenueHistoricalDimLocation"

IF OBJECT_ID('tempdb.dbo.#DimClubInfo', 'U') IS NOT NULL  
  DROP TABLE #DimClubInfo;

  ----- Create club temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @DimMMSClubID,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  INTO #DimClubInfo  
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
  AND (DimClub.club_close_dim_date_key < '-997' 
        OR DimClub.club_close_dim_date_key > @StartMonthStartingDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       DimClub.club_code,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code,
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
		   END  Region,
       DimClub.ClubName AS MMSClubName,
	   DimClub.club_id AS MMSClubID,
	   DimClub.gl_club_id AS GLClubID,
	   DimClub.LocalCurrencyCode
  INTO #DimLocationInfo    
  FROM #DimClubInfo DimClub     
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
	   DimClub.gl_club_id,
	   DimClub.LocalCurrencyCode



 
 IF OBJECT_ID('tempdb.dbo.#ClubPOSAllocatedRevenue', 'U') IS NOT NULL
DROP TABLE #ClubPOSAllocatedRevenue; 
 
 SELECT 'MMS' as SalesSource,
          DimLocationInfo.Region,
		  DimLocationInfo.MMSClubID,
		  DimLocationInfo.MMSClubName,
		  DimReportingHierarchy.reporting_division DivisionName,
          DimReportingHierarchy.reporting_sub_division SubdivisionName,
		  DimReportingHierarchy.reporting_department RevenueReportingDepartmentName,
		  PrimarySalesDimEmployee.Employee_ID  PrimarySellingTeamMemberID, 
          PrimarySalesDimEmployee.Last_Name + ', ' + PrimarySalesDimEmployee.First_Name PrimarySellingTeamMember,
          Member.Membership_ID MembershipID,
		  Member.Member_ID MemberID,
          Member.Customer_Name_Last_First MemberName,
		  Member.First_Name MemberFirstName,
          Member.Last_Name MemberLastName,
		  Membership.membership_type MembershipTypeDescription,
          RevenueMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month RevenueYearMonth,
          Sum(AllocatedRevenue.allocated_amount) RevenueAmount,
		  DimLocationInfo.Region + '|' + DimLocationInfo.MMSClubName + '|' + PrimarySalesDimEmployee.Last_Name + ', ' + PrimarySalesDimEmployee.First_Name+ '|' + DimReportingHierarchy.reporting_division+ '|' + Convert(Varchar,Member.Member_ID) + '|' + Member.Customer_Name_Last_First as RevenueReportingDivisionPivotColumn,
		  DimLocationInfo.Region + '|' + DimLocationInfo.MMSClubName + '|' + PrimarySalesDimEmployee.Last_Name + ', ' + PrimarySalesDimEmployee.First_Name+ '|' + DimReportingHierarchy.reporting_division+ '|' + DimReportingHierarchy.reporting_sub_division +  '|' + Convert(Varchar,Member.Member_ID) + '|' + Member.Customer_Name_Last_First as RevenueReportingSubdivisionPivotColumn,
		  DimLocationInfo.Region + '|' + DimLocationInfo.MMSClubName + '|' + PrimarySalesDimEmployee.Last_Name + ', ' + PrimarySalesDimEmployee.First_Name+ '|' + DimReportingHierarchy.reporting_division+ '|' + DimReportingHierarchy.reporting_sub_division + '|' + DimReportingHierarchy.reporting_department + '|' + Convert(Varchar,Member.Member_ID) + '|' + Member.Customer_Name_Last_First as RevenueReportingDepartmentPivotColumn
  INTO #ClubPOSAllocatedRevenue
   FROM [marketing].[v_fact_mms_allocated_transaction_item] AllocatedRevenue
   JOIN [marketing].[v_Dim_mms_Product] DimProduct
     ON AllocatedRevenue.dim_mms_product_key = DimProduct.dim_mms_product_key
   JOIN #DimReportingHierarchy
     ON AllocatedRevenue.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
   JOIN [marketing].[v_dim_reporting_hierarchy] DimReportingHierarchy
     ON AllocatedRevenue.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
   JOIN #DimLocationInfo  DimLocationInfo
     ON AllocatedRevenue.dim_club_key = DimLocationInfo.DimClubKey
   JOIN [marketing].[v_Dim_Employee] PrimarySalesDimEmployee
     ON AllocatedRevenue.primary_sales_dim_employee_key = PrimarySalesDimEmployee.Dim_Employee_Key
   JOIN [marketing].[v_dim_mms_member] Member
     ON AllocatedRevenue.dim_mms_member_key = Member.dim_mms_member_key
   JOIN [marketing].[v_dim_mms_membership] Membership
     ON Member.dim_mms_membership_key = Membership.dim_mms_membership_key
   JOIN [marketing].[v_Dim_Date] RevenueMonthDimDate
     ON AllocatedRevenue.allocated_month_starting_dim_date_key = RevenueMonthDimDate.Dim_Date_Key

 WHERE  IsNull(DimProduct.package_product_flag,'N') = 'Y'
	 AND PrimarySalesDimEmployee.employee_id > 0 
	 AND
      ((AllocatedRevenue.transaction_post_dim_date_key >= @StartMonthStartingDimDateKey
          AND AllocatedRevenue.transaction_post_dim_date_key <= @EndMonthEndingDimDateKey)
	   OR (DimProduct.allocation_rule != 'Sale Month Activity'  	  
          AND AllocatedRevenue.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey       --------- allocated month start not post date
          AND AllocatedRevenue.allocated_month_starting_dim_date_key <= @EndMonthEndingDimDateKey))         --------- allocated month start not post date
	 AND DimReportingHierarchy.reporting_department not in('Fitness Products','90 Day Weight Loss', 'Devices', 'MyHealth Check', 'PT Nutritionals','PT E-Commerce')
Group by  DimLocationInfo.Region,
          DimLocationInfo.MMSClubName,
          DimLocationInfo.MMSClubID,
          DimReportingHierarchy.reporting_department,
          PrimarySalesDimEmployee.Employee_ID,
          PrimarySalesDimEmployee.Last_Name,
		  PrimarySalesDimEmployee.First_Name,
          Member.Membership_ID,
          Membership.membership_type,
          Member.Member_ID,
          Member.Customer_Name_Last_First,
          RevenueMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month,
		  ----RevenueMonthDimDate.CalendarMonthAbbreviation,
          Member.First_Name,
          Member.Last_Name,
          DimReportingHierarchy.reporting_division,
          DimReportingHierarchy.reporting_sub_division,
		  PrimarySalesDimEmployee.Last_Name + ', ' + PrimarySalesDimEmployee.First_Name

  SELECT SalesSource,
          Region,
		  MMSClubID,
		  MMSClubName,
		  DivisionName,
          SubdivisionName,
		  RevenueReportingDepartmentName,
		  PrimarySellingTeamMemberID, 
          PrimarySellingTeamMember,
          MembershipID,
		  MemberID,
		  MemberName,
		  MemberFirstName,
          MemberLastName,
		  MembershipTypeDescription,
		  @ReportRunDateTime as ReportRunDateTime,
		  RevenueReportingDivisionPivotColumn,
		  RevenueReportingSubdivisionPivotColumn,        
          RevenueReportingDepartmentPivotColumn,
		  RevenueYearMonth,
          RevenueAmount
		  From #ClubPOSAllocatedRevenue
		  Where RevenueAmount >0
		  AND PrimarySellingTeamMemberID < 9000000   ---- no consultant employees

DROP TABLE #ClubPOSAllocatedRevenue
DROP TABLE #DimClubInfo
DROP TABLE #DimLocationInfo
DROP TABLE #DimReportingHierarchy





END
