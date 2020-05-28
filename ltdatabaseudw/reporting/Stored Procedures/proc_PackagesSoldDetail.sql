CREATE PROC [reporting].[proc_PackagesSoldDetail] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](8000),@MMSClubIDList [VARCHAR](8000),@DivisionName [VARCHAR](255) AS
BEGIN

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END
 
SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0) ELSE @StartDate END
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) ELSE @EndDate END

DECLARE @StartReportDateDimDateKey INT
DECLARE @EndReportDateDimDateKey INT

SET @StartReportDateDimDateKey = (Select [dim_date_key] from [marketing].[v_dim_date] where [calendar_date] = @StartDate)
SET @EndReportDateDimDateKey = (Select [dim_date_key] from [marketing].[v_dim_date] where [calendar_date] = @EndDate)

DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time

DECLARE @BeginDimDateKey INT,
        @EndDimDateKey INT,
        @EndMonthEndingDate DATETIME,
        @HeaderDateRange VARCHAR(33)
SELECT @BeginDimDateKey = StartDimDate.[dim_date_key],
       @EndDimDateKey = EndDimDate.[dim_date_key],
       @EndMonthEndingDate = EndDimDate.[month_ending_date],
       @HeaderDateRange = StartDimDate.[standard_date_name] + ' through ' + EndDimDate.[standard_date_name]
  FROM [marketing].[v_dim_date] StartDimDate
 CROSS JOIN [marketing].[v_dim_date] EndDimDate
 WHERE StartDimDate.[calendar_date] = @StartDate
   AND EndDimDate.[calendar_date] = @EndDate


IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL DROP TABLE #Clubs;

-- Create club temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey, 
	   DimClub.club_name AS ClubName,
   	   MMSRegion.description AS MMSRegionName
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubKeyList
    ON ClubKeyList.Item = DimClub.club_id 
	   OR ClubKeyList.Item = -1
 JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @BeginDimDateKey)  
GROUP BY DimClub.dim_club_key, DimClub.club_name, MMSRegion.description

IF OBJECT_ID('tempdb.dbo.#DimLocationKeyList', 'U') IS NOT NULL DROP TABLE #DimLocationKeyList;

-- Create region temp table
SET @list_table = 'region_list'

EXEC marketing.proc_parse_pipe_list @RegionList,@list_table

SELECT Clubs.DimClubKey, 
	   Clubs.ClubName,
	   WDRegionClub.[workday_region] WorkdayRegion,
   	   Clubs.MMSRegionName
  INTO #DimLocationKeyList
  FROM #Clubs Clubs
  JOIN #region_list RegionList
        ON Clubs.MMSRegionName = RegionList.Item
    OR @RegionList like '%All Regions%'
  JOIN [marketing].[v_dim_club] WDRegionClub
        ON Clubs.DimClubKey = WDRegionClub.[dim_club_key]

SELECT DISTINCT DimProduct.[dim_mms_product_key] DimProductKey
	   , DimProduct.[dim_reporting_hierarchy_key] DimReportingHierarchyKey
	   , DimProduct.[workday_cost_center] WorkdayCostCenter
	   , DimProduct.[workday_offering] WorkdayOffering
	   , DimProduct.[product_description] ProductDescription
	   , DimProduct.[department_description] MMSDepartmentDescription
	   , DimProduct.[payroll_standard_group_description] StandardProductCommissionCode
       , DimProduct.[payroll_lt_bucks_group_description] LTBucksProductCommissionCode
	   , DimProduct.[reporting_division] DivisionName
	   , DimProduct.[reporting_sub_division] SubdivisionName
	   , DimProduct.[reporting_department] DepartmentName
	   , DimProduct.[lt_buck_cost_percent] LTBuckCostPercent 
INTO #DimProduct
  FROM [marketing].[v_dim_mms_product] DimProduct  
  JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
    ON DimProduct.[dim_reporting_hierarchy_key] = DimReportingHierarchy.[dim_reporting_hierarchy_key]
 WHERE DimProduct.[reporting_division] = @DivisionName


SELECT FactPackage.[package_id] PackageID, 
       DimLocation.MMSRegionName,
       DimLocation.ClubName,
       DimDate.[calendar_date] PackageCreatedDate,
	   FactPackage.[original_currency_code] CurrencyCode,
	   FactPackage.[price_per_session] AS SessionPrice, 
	   FactPackage.[number_of_sessions] AS SessionQuantity,	
       #DimProduct.MMSDepartmentDescription MMSDepartment,
       DimDescriptionActive.description MMSSalesChannel,
	   #DimProduct.WorkdayCostCenter,
	   #DimProduct.WorkdayOffering,
	   #DimProduct.ProductDescription,
	   #DimProduct.DivisionName Division,
	   #DimProduct.SubdivisionName Subdivision,
	   #DimProduct.DepartmentName ReportingDepartment,
	   DimLocation.WorkdayRegion,
	   PrimarySalesDimEmployee.[employee_name] PrimarySalesEmployeeName,
	   #DimProduct.StandardProductCommissionCode,
	   #DimProduct.LTBucksProductCommissionCode,
	   DimMMSMember.[member_id],
	   DimMMSMember.[customer_name] MemberName,
	   @HeaderDateRange HeaderDateRange,
	   @ReportRunDateTime AS ReportRunDateTime,
	   IsNull(#DimProduct.LTBuckCostPercent,0) *.01 AS ProductLTBucksCostPercent,
	   IsNull(FactPackage.[item_lt_bucks_amount],0) AS LTBucksPaymentForPackage
  FROM [marketing].[v_fact_mms_package] FactPackage
   JOIN #DimProduct
      ON FactPackage.[dim_mms_product_key] = #DimProduct.DimProductKey
   JOIN #DimLocationKeyList DimLocation
      On DimLocation.DimClubKey = FactPackage.[dim_club_key]
   JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee
      ON FactPackage.[primary_sales_dim_employee_key] = PrimarySalesDimEmployee.[dim_employee_key]
   LEFT JOIN [marketing].[v_dim_description] DimDescriptionActive
      ON FactPackage.[sales_channel_dim_description_key] = DimDescriptionActive.[dim_description_key]
   JOIN [marketing].[v_dim_date] DimDate
      ON DimDate.[dim_date_key] = FactPackage.[created_dim_date_key]
   JOIN [marketing].[v_dim_mms_member] DimMMSMember
      ON DimMMSMember.[dim_mms_member_key] = FactPackage.[dim_mms_member_key]
WHERE FactPackage.[created_dim_date_key] >= @StartReportDateDimDateKey
AND FactPackage.[created_dim_date_key] <= @EndReportDateDimDateKey
AND FactPackage.[transaction_void_flag] = 'N'
Order by DimLocation.ClubName, DimDate.[calendar_date], FactPackage.[package_id]


DROP TABLE #Clubs
DROP TABLE #DimLocationKeyList
DROP TABLE #DimProduct


END

