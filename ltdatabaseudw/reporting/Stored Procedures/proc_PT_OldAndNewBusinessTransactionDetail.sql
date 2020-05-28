CREATE PROC [reporting].[proc_PT_OldAndNewBusinessTransactionDetail] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](4000),@DimMMSClubIDList [VARCHAR](4000),@SalesSourceList [VARCHAR](4000),@CommissionTypeList [VARCHAR](4000),@DimReportingHierarchyKeyList [Varchar](8000),@TotalReportingHierarchyKeyCount [INT],@SubdivisionList [VARCHAR](8000),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@BusinessTypeFilter [VARCHAR](50) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


---------------  Sample execution
---  Exec [reporting].[proc_PT_OldAndNewBusinessTransactionDetail] '6/2/2019','6/10/2019','All Regions','151|8','MMS|HealthCheckUSA|Cafe','Commissioned|Non-Commissioned','All Product Groups',30,'All Subdivisions','All Departments','All Transactions'
---------------


DECLARE @DivisionList VARCHAR(8000)
DECLARE @HeaderDivisionList VARCHAR(8000)

SET @DivisionList = 'Personal Training|PT Division'
SET @HeaderDivisionList = 'Personal Training'


-- Use map_utc_time_zone_conversion to determine correct 'current' time, factoring in daylight savings / non daylight savings 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
						  from map_utc_time_zone_conversion
						  where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')


SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                      THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0)    ----- returns 1st of yesterday's month
					  WHEN @StartDate = 'Dec 30, 1899'
					  THEN DATEADD(YEAR,DATEDIFF(YEAR,0,GETDATE()-1),0)      ----- returns 1st of yesterday's year
					  ELSE @StartDate END
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @EndDate END


DECLARE @StartDimDateKey VARCHAR(32),
        @StartMonthStartingDimDateKey VARCHAR(32),
        @ReportStartDate VARCHAR(12)

SELECT @StartDimDateKey = dim_date_key,
       @StartMonthStartingDimDateKey  = month_starting_dim_date_key,
       @ReportStartDate = standard_date_name
FROM [marketing].[v_dim_date]
WHERE calendar_date = @StartDate



DECLARE @EndDimDateKey VARCHAR(32),
        @EndMonthStartingDimDateKey VARCHAR(32),
        @EndMonthEndingDimDateKey VARCHAR(32),
        @ReportEndDate VARCHAR(12),
		@EndMonthEndingDate DATETIME

SELECT @EndDimDateKey = dim_date_key,
       @EndMonthEndingDimDateKey = month_ending_dim_date_key,
       @ReportEndDate = standard_date_name,
	   @EndMonthEndingDate = month_ending_date
 FROM [marketing].[v_dim_date]
WHERE calendar_date = @EndDate



----- Create Hierarchy temp table to return selected group names      

Exec [reporting].[proc_DimReportingHierarchy_History] @DivisionList,@SubdivisionList,@DepartmentMinDimReportingHierarchyKeyList,@DimReportingHierarchyKeyList,@StartDimDateKey,@EndDimDateKey

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 

 SELECT DimReportingHierarchyKey,
        --HeaderDivisionList,   ----- Must be created in report processing based on prompt values
       --HeaderSubdivisionList,
       --HeaderDepartmentList,
       --HeaderProductGroupList,
       DivisionName,
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   RegionType,
	   ReportRegionType,
	   CASE WHEN ProductGroupName IN('Weight Loss Challenges','90 Day Weight Loss')
	        THEN 'Y'
		    ELSE 'N'
			END PTDeferredRevenueProductGroupFlag
 INTO #DimReportingHierarchy
 FROM #OuterOutputTable
                                            

DECLARE @RegionType VARCHAR(50)
SET @RegionType = (SELECT MIN(ReportRegionType) FROM #DimReportingHierarchy)


  ----- Create Sales Source temp table       ---------- found that this is not used as a filter in the query
IF OBJECT_ID('tempdb.dbo.#SalesSourceList', 'U') IS NOT NULL 
DROP TABLE #SalesSourceList; 

DECLARE @list_table VARCHAR(100)
SET @list_table = 'SalesSource'

EXEC marketing.proc_parse_pipe_list @SalesSourceList,@list_table
	
SELECT DISTINCT SalesSourceList.Item SalesSource
  INTO #SalesSourceList
  FROM #SalesSource SalesSourceList



    ----- Create Commission Type temp table       ---------- found that this is not used as a filter in the query
IF OBJECT_ID('tempdb.dbo.#CommissionTypeList', 'U') IS NOT NULL
  DROP TABLE #CommissionTypeList; 
  
SET @list_table = 'CommissionType'

  EXEC marketing.proc_parse_pipe_list @SalesSourceList,@list_table

SELECT CommissionTypeList.Item CommisionType,
       CASE WHEN CommissionTypeList.Item = 'Commissioned' 
	        THEN 'Y' 
			ELSE 'N' 
			END CommissionedSalesTransactionFlag
INTO #CommissionTypeList
FROM #CommissionType CommissionTypeList


 ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
 ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
 ------   revenue is deferred to the 2nd month

DECLARE @StartDateMonthStartDimDateKey VARCHAR(32)
DECLARE @EndDateMonthStartDimDateKey VARCHAR(32)
DECLARE @StartDateCalendarMonthNumberInYear INT
DECLARE @EndDateCalendarMonthNumberInYear INT
DECLARE @EndDatePriorMonthEndDateDimDateKey VARCHAR(32)


SET @StartDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @StartDimDateKey) 
SET @EndDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey) 
SET @StartDateCalendarMonthNumberInYear = (SELECT month_number_in_year  FROM [marketing].[v_dim_date] WHERE dim_date_key = @StartDimDateKey)
SET @EndDateCalendarMonthNumberInYear = (SELECT month_number_in_year   FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)
SET @EndDatePriorMonthEndDateDimDateKey = (SELECT prior_month_ending_dim_date_key  FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)

DECLARE @EComm60DayChallengeRevenueStartDimDateKey VARCHAR(32)
  ---- When the start date is the 1st of the 2nd month of the quarter, set the start date to the 1st of the prior month
SET @EComm60DayChallengeRevenueStartDimDateKey = (SELECT CASE WHEN (@StartDimDateKey = @StartDateMonthStartDimDateKey)          ---- Date range begins on the 1st of a month
															  THEN (CASE WHEN @StartDateCalendarMonthNumberInYear in(2,5,8,11)
																		 THEN (Select prior_month_starting_dim_date_key
                                                                                 FROM [marketing].[v_dim_date] 
                                                                                WHERE dim_date_key = @StartDimDateKey)
																	      WHEN @StartDateCalendarMonthNumberInYear in(1,4,7,10)
																		  THEN (Select month_starting_dim_date_key
                                                                                  FROM [marketing].[v_dim_date]  
                                                                                 WHERE dim_date_key = @StartDimDateKey) 
																		  ELSE @StartDimDateKey
																				   END)
												
															  ELSE  @StartDimDateKey END
												  FROM [marketing].[v_dim_date] 
												  WHERE dim_date_key = @StartDimDateKey ) ---- to limit result set to one record)

DECLARE @EComm60DayChallengeRevenueEndDimDateKey VARCHAR(32)
  ---- When the End Date is in the 1st month of the quarter, set the end date to the end of the prior month
SET @EComm60DayChallengeRevenueEndDimDateKey = (SELECT CASE WHEN @EndDateCalendarMonthNumberInYear in(1,4,7,10)
                                                            THEN @EndDatePriorMonthEndDateDimDateKey 
															ELSE @EndDimDateKey
															END
												FROM [marketing].[v_dim_date] 
												WHERE dim_date_key = @EndDimDateKey)  ---- to limit result set to one record



DECLARE @CurrencyCode VARCHAR(15)
SET @CurrencyCode = 'Local Currency'


				
DECLARE @CalendarMonthStartDate DATETIME	
DECLARE @ENDDateDimDateKey VARCHAR(32)				
				
DECLARE @ReportDate VARCHAR(20)				
DECLARE @FirstOfPriorMonthDimDateKey VARCHAR(32)				
DECLARE @FirstOf6MonthsPriorDimDateKey VARCHAR(32)				
DECLARE @FirstOfCurrentMonthDimDateKey VARCHAR(32)	
DECLARE @FirstOf2MonthsPriorDimDateKey VARCHAR(32)
DECLARE @FirstOf2MonthsPriorDate DATETIME

	
				
SET @CalendarMonthStartDate = (SELECT month_starting_date FROM [marketing].[v_dim_date]  WHERE calendar_date = @EndDate)				
SET @ENDDateDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date]  WHERE calendar_date = @EndDate)				
SET @ReportDate = Replace(Substring(convert(varchar,@EndDate,100),1,6)+', '+Substring(convert(varchar,@EndDate,100),8,4),'  ',' ')								
SET @FirstOfPriorMonthDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DATEADD(m,-1,@CalendarMonthStartDate))				
SET @FirstOf6MonthsPriorDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DATEADD(m,-6, @CalendarMonthStartDate))				
SET @FirstOfCurrentMonthDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @CalendarMonthStartDate)				
SET @FirstOf2MonthsPriorDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DATEADD(m,-2, @CalendarMonthStartDate))		
SET @FirstOf2MonthsPriorDate = (SELECT calendar_date FROM [marketing].[v_dim_date] WHERE dim_date_key = @FirstOf2MonthsPriorDimDateKey)



  ----- Create region temp table   
IF OBJECT_ID('tempdb.dbo.#RegionList', 'U') IS NOT NULL
  DROP TABLE #RegionList; 

SET @list_table = 'region_list'

EXEC marketing.proc_parse_pipe_list @RegionList,@list_table

SELECT RegionDescription.description AS Region,
       RegionDescription.dim_description_key
INTO #RegionList
FROM #region_list RegionList
JOIN [marketing].[v_dim_description] RegionDescription
    ON RegionList.Item = RegionDescription.description
	  OR RegionList.Item = 'All Regions'
JOIN [marketing].[v_dim_club] DimClub
    ON RegionDescription.dim_description_key = DimClub.pt_rcl_area_dim_description_key

GROUP BY RegionDescription.description,
       RegionDescription.dim_description_key




  ----- Create club temp table   
IF OBJECT_ID('tempdb.dbo.#DimLocationInfo', 'U') IS NOT NULL
  DROP TABLE #DimLocationInfo; 

SET @list_table = 'club_list'

EXEC marketing.proc_parse_pipe_list @DimMMSClubIDList,@list_table

SELECT DimClub.dim_club_key AS DimClubKey,     -----name change
       #RegionList.Region AS Region,
	   DimClub.club_name AS MMSClubName,
       DimClub.club_id AS MMSClubID, 
	   DimClub.gl_club_id AS GLClubID,
	   DimClub.workday_region AS WorkdayRegion,
       DimClub.local_currency_code AS LocalCurrencyCode,
       DimClub.club_code AS ClubCode,
	   ClubOpenDate.calendar_date AS ClubOpenDate,
	   CASE WHEN ClubOpenDate.calendar_date >= DATEADD(Month,-1,@CalendarMonthStartDate)
		    THEN 'Y'
			ELSE 'N'
			END NewBusinessOnlyClub
INTO #DimLocationInfo
FROM #club_list DimClubKeyList
JOIN [marketing].[v_dim_club] DimClub
  ON DimClubKeyList.Item = DimClub.club_id
  OR DimClubKeyList.Item = -1   ------'All Clubs'
JOIN #RegionList
  ON DimClub.pt_rcl_area_dim_description_key = #RegionList.dim_description_key
JOIN [marketing].[v_dim_date] ClubOpenDate
  ON DimClub.club_open_dim_date_key = ClubOpenDate.dim_date_key


-- Find all PT packages sold in the look back period where the purchaser was not the package customer

   ----- Packages sold through MMS
IF OBJECT_ID('tempdb.dbo.#PackagesWherePurchaserIsNotServicedCustomer', 'U') IS NOT NULL
  DROP TABLE #PackagesWherePurchaserIsNotServicedCustomer; 

    SELECT CAST(FactSalesTransaction.mms_tran_id AS VARCHAR(50)) AS TranID,
             FactSalesTransaction.tran_item_id AS TranItemID,
	       FactPackage.package_id AS PackageID,
		   FactSalesTransaction.dim_mms_member_key AS PkgPurchasingCustomer_DimMemberKey,    ------ Name Change
		   FactPackage.dim_mms_member_key  AS PkgServiceCustomer_DimMemberKey   ------ Name Change
		INTO #PackagesWherePurchaserIsNotServicedCustomer
	  FROM [marketing].[v_fact_mms_package] FactPackage
	    JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
		  ON FactPackage.tran_item_id = FactSalesTransaction.tran_item_id
		JOIN [marketing].[v_dim_mms_product] DimProduct
		  ON FactPackage.dim_mms_product_key = DimProduct.dim_mms_product_key
		JOIN #DimReportingHierarchy DimReportingHierarchy
		  ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
	  WHERE FactSalesTransaction.post_dim_date_key >=  @FirstOf2MonthsPriorDimDateKey
	    AND FactSalesTransaction.post_dim_date_key <= @ENDDateDimDateKey
	    AND FactSalesTransaction.dim_mms_member_key <> FactPackage.dim_mms_member_key



UNION ALL

   --- Packages sold through Hybris
     SELECT FactSalesTransaction.order_code AS TranID,
	        FactSalesTransaction.entry_number AS TranItemID,
            FactPackage.package_id AS PackageID,
	        FactSalesTransaction.dim_mms_member_key AS PkgPurchasingCustomer_DimMemberKey,  
	        FactPackage.dim_mms_member_key AS PkgServiceCustomer_DimMemberKey 

     FROM [marketing].[v_fact_hybris_transaction_item] FactSalesTransaction
      JOIN [marketing].[v_fact_mms_package] FactPackage
        ON FactSalesTransaction.fact_mms_sales_transaction_key = FactPackage.fact_mms_sales_transaction_key
     WHERE FactSalesTransaction.settlement_dim_date_key >=  @FirstOf2MonthsPriorDimDateKey
        AND FactSalesTransaction.settlement_dim_date_key <= @ENDDateDimDateKey
	    AND FactPackage.dim_mms_member_key <> FactSalesTransaction.dim_mms_member_key     
       AND FactSalesTransaction.refund_flag = 'N'    
       AND FactSalesTransaction.transaction_amount <> 0
	   AND FactSalesTransaction.fact_mms_sales_transaction_key <> '-998'

UNION ALL
  ----- blocked by defects UDW-9843 - FactSalesTransaction.fact_mms_transaction_key always NULL
   ----- waiting for reply from Brian re: FactSalesTransaction.order_item_id as this should be loaded into combined allocation view
  --- Packages sold through Magento
     SELECT FactSalesTransaction.order_number AS TranID,
	        FactSalesTransaction.order_item_id AS TranItemID,
            FactPackage.package_id AS PackageID,
	        FactSalesTransaction.dim_mms_member_key AS PkgPurchasingCustomer_DimMemberKey,  
	        FactPackage.dim_mms_member_key AS PkgServiceCustomer_DimMemberKey    
     FROM [marketing].[v_fact_magento_transaction_item] FactSalesTransaction
      JOIN [marketing].[v_fact_mms_package] FactPackage
        ON FactSalesTransaction.fact_mms_transaction_key = FactPackage.fact_mms_sales_transaction_key
     WHERE FactSalesTransaction.transaction_dim_date_key >=  @FirstOf2MonthsPriorDimDateKey
        AND FactSalesTransaction.transaction_dim_date_key <= @ENDDateDimDateKey
	    AND FactPackage.dim_mms_member_key <> FactSalesTransaction.dim_mms_member_key     
       AND FactSalesTransaction.refund_flag = 'N'    
       AND FactSalesTransaction.allocated_amount <> 0
	   AND FactSalesTransaction.fact_mms_transaction_key <> '-998'
	   AND FactSalesTransaction.fact_mms_transaction_key Is not null


IF OBJECT_ID('tempdb.dbo.#OldNewBusinessCustomerRecords', 'U') IS NOT NULL
  DROP TABLE #OldNewBusinessCustomerRecords; 

 ---- create a temp table to hold data on all PT Transactions for the past 2 months of revenue
 ---- Used for both current month customer and old business customer (2 month prior) data
	SELECT CASE WHEN FactAllocatedTransaction.sales_source = 'MMS'
		        THEN FactAllocatedTransaction.line_number
				ELSE ''
				END TranItemID,
	       FactAllocatedTransaction.dim_mms_member_key AS PkgPurchasingCustomerDimMemberKey,    ------ Name change
	       IsNull(PKG.PkgServiceCustomer_DimMemberKey,FactAllocatedTransaction.dim_mms_member_key) AS PkgServiceCustomerDimMemberKey,  ------ Name change
		   IsNull(PKG.PkgServiceCustomer_DimMemberKey,FactAllocatedTransaction.dim_mms_member_key) AS OldNewBusinessDimMemberKey,     ------ Name change
		   CASE WHEN sales_source in('MMS','Cafe')                             ------- I question the logic, but it matches current LTFDM_Revenue logic
		        THEN FactAllocatedTransaction.transaction_amount
				ELSE FactAllocatedTransaction.allocated_amount
				END ItemAmount,
		   FactAllocatedTransaction.dim_product_key AS DimProductKey,
		   FactAllocatedTransaction.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
		   FactAllocatedTransaction.transaction_dim_date_key AS TransactionPostDimDateKey, 				
           10000 AS TransactionPostDimTimeKey,      ----- key for midnight
		   FactAllocatedTransaction.allocated_month_starting_dim_date_key AS RevenuePostingMonthStartingDimDateKey,
		   RevenueMonthDimDate.four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth,
		   FactAllocatedTransaction.allocated_dim_club_key as DimClubKey,    ------ new name
		   CASE WHEN FactAllocatedTransaction.transaction_type = 'Refund'
		        THEN 'Y'
				ELSE 'N'
				END RefundFlag,
		   CASE WHEN FactAllocatedTransaction.transaction_type = 'Charge'
		        THEN 'Y'
				ELSE 'N'
				END ChargeFlag,
           NULL AS DimProductSCDKey,
		   ----FactClubPOSAllocatedRevenue.DimProductSCDKey,   ------ UDW does not have SCD Keys - returning dim_product_key see line 381 above
           NULL AS DimReportingHierarchySCDKey,
		   -----FactClubPOSAllocatedRevenue.DimReportingHierarchySCDKey,   ------ UDW does not have SCD Keys - return dim_reporting_hierarchy_Key see line 382 above
           CASE WHEN FactAllocatedTransaction.primary_sales_dim_employee_key <> '-998'
		        THEN 'Y'
				ELSE 'N'
				END CommissionedSalesTransactionFlag, 
           FactAllocatedTransaction.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,
           NULL AS SecondarySalesDimEmployeeKey,   ----- UDW does not have secondary employee - leave null
           FactAllocatedTransaction.allocated_quantity AS AllocatedQuantity,
		   FactAllocatedTransaction.transaction_quantity AS SalesQuantity,
		   CASE WHEN sales_source in('MMS','Cafe')                             ------- I question the logic, but it matches current LTFDM_Revenue logic
		        THEN FactAllocatedTransaction.transaction_amount
				ELSE FactAllocatedTransaction.allocated_amount
				END SalesAmount,     
		   0 AS CorporateTransferAmount,    ----- UDW does not have corporate transfer - enter 0
		   FactAllocatedTransaction.discount_amount AS SalesDiscountDollarAmount,
		   NULL AS SourceFactTableKey,   ------ Obsolete
		   NULL AS SourceFactTableDimDescriptionKey,   ------ Obsolete
		   NULL AS DimRevenueAllocationRuleSCDKey,    ------ Obsolete
		   NULL AS SoldNotServicedFlag,    ------ Obsolete
		   CASE WHEN FactAllocatedTransaction.sales_source = 'MMS'
		        THEN FactAllocatedTransaction.transaction_id
				ELSE ''
				END MMSTranID,
           FactAllocatedTransaction.dim_mms_transaction_reason_key AS DimTransactionReasonKey,
		   CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimCafeProduct.allocation_rule
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.allocation_rule
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.allocation_rule
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimMMSProduct.allocation_rule
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN 'Sale Month Activity'	
			   END  RevenueAllocationRuleName,
		   CASE WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
		        THEN (CASE WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Fitness Products'
				   THEN 'OF10220'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Lab Testing'
				   THEN 'OF10217'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Metabolic Assessments'
				   THEN 'OF10108'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Metabolic Conditioning'
				   THEN 'OF10224'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'PT E-Commerce'
				   THEN 'OF10122'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName in('Weight Loss Challenges','90 Day Weight Loss')
				   THEN 'OF10117'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Team Alpha'
				   THEN 'OF10091'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Team Boot Camp'
				   THEN 'OF10094'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Team Burn'
				   THEN 'OF10095'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Team Cut'
				   THEN 'OF10096'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Tri-PT'
				   THEN 'OF10121'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'MyHealthScore'
				   THEN 'OF10118'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Personal Training'
				   THEN 'OF10084'
				   ELSE ''
				   END)
				WHEN FactAllocatedTransaction.sales_source = 'Cafe'
				THEN (CASE WHEN DimReportingHierarchy_Cafe.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN DimReportingHierarchy_Cafe.DepartmentName = 'PT Nutritionals'
				   THEN 'OF10115'
				   WHEN DimReportingHierarchy_Cafe.DepartmentName = 'Cafe Nutritionals'
				   THEN 'OF54010'
				   ELSE ''
				   END)
				WHEN FactAllocatedTransaction.sales_source = 'Hybris'
		        THEN ( CASE WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Fitness Products'
				   THEN 'OF10220'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Lab Testing'
				   THEN 'OF10217'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Metabolic Assessments'
				   THEN 'OF10108'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Metabolic Conditioning'
				   THEN 'OF10224'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'PT E-Commerce'
				   THEN 'OF10122'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName in('Weight Loss Challenges','90 Day Weight Loss')
				   THEN 'OF10117'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Team Alpha'
				   THEN 'OF10091'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Team Boot Camp'
				   THEN 'OF10094'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Team Burn'
				   THEN 'OF10095'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Team Cut'
				   THEN 'OF10096'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Tri-PT'
				   THEN 'OF10121'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'MyHealthScore'
				   THEN 'OF10118'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Personal Training'
				   THEN 'OF10084'
				   ELSE ''
				   END)
		        WHEN FactAllocatedTransaction.sales_source = 'MMS'
				THEN DimMMSProduct.workday_offering
				WHEN FactAllocatedTransaction.sales_source = 'Magento'
				THEN DimMagentoProduct.workday_offering_id 
				END WorkdayOffering,
		   CASE WHEN FactAllocatedTransaction.sales_source in('Cafe','HealthCheckUSA')
		        THEN 'N'
				WHEN FactAllocatedTransaction.sales_source = 'Hybris'
				     AND DimReportingHierarchy_Hybris.DepartmentName in('Devices','Fitness Products','PT E-Commerce')
				THEN 'N'
		        WHEN FactAllocatedTransaction.sales_source = 'MMS'
				     AND DimMMSProduct.workday_offering in('OF10104','OF10105','OF10220','OF10202','OF10122','OF10123','OF10115')
		        THEN 'N'
				WHEN FactAllocatedTransaction.sales_source = 'Magento'
				     AND DimMagentoProduct.workday_offering_id in('OF10104','OF10105','OF10220','OF10202','OF10122','OF10123','OF10115')
				THEN 'N'
				ELSE 'Y'
				END PTServiceFlag,
		   FactAllocatedTransaction.sales_source AS SalesSource,						
       	   CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
				THEN CONVERT(VARCHAR(255),DimCafeProduct.menu_item_id) 
				WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'   
				THEN DimHealthCheckUSAProduct.product_sku
				WHEN FactAllocatedTransaction.sales_source = 'Magento'
				THEN DimMagentoProduct.sku
				END SKU,
		   CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimCafeProduct.menu_item_name
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.product_description
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimMMSProduct.product_description
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
			   END  ProductDescription,
 		 CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.ProductGroupName 
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.ProductGroupName
			   END  ProductGroupName, 
				      CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.DepartmentName 
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  DepartmentName, 
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.SubdivisionName
			   END  SubdivisionName,
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'   
                  THEN DimReportingHierarchy_Magento.DivisionName
			   END  DivisionName,
		   CASE WHEN FactAllocatedTransaction.transaction_type is null     ------ due to Cafe source always returning NULL - defect UDW-9868
		        THEN (CASE WHEN FactAllocatedTransaction.allocated_amount < 0
				           THEN 'Refund'
						   ELSE 'Sale'
						   END)
				ELSE FactAllocatedTransaction.transaction_type
				END TransactionType,
		   CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN CONVERT(VARCHAR(50),DimCafeProduct.menu_item_id)
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.code
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN CONVERT(VARCHAR(50),DimHealthCheckUSAProduct.product_sku)
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN CONVERT(VARCHAR(50),DimMMSProduct.product_id)
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'   
                  THEN CONVERT(VARCHAR(50),DimMagentoProduct.sku)
			   END SourceProductID,
			SalesChannelDimDescription.description AS SalesChannel

INTO #OldNewBusinessCustomerRecords    
FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedTransaction
   LEFT JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct
     ON FactAllocatedTransaction.dim_product_key = DimCafeProduct.dim_cafe_product_key
	   AND FactAllocatedTransaction.sales_source = 'Cafe'
	   AND DimCafeProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimCafeProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactAllocatedTransaction.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactAllocatedTransaction.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHybrisProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactAllocatedTransaction.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHealthCheckUSAProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_mms_product_history] DimMMSProduct
     ON FactAllocatedTransaction.dim_product_key = DimMMSProduct.dim_mms_product_key
	   AND FactAllocatedTransaction.sales_source = 'MMS'
	   AND DimMMSProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMMSProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactAllocatedTransaction.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactAllocatedTransaction.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMagentoProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Cafe
     ON DimCafeProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Cafe.DimReportingHierarchyKey
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'N'   
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'N'   
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_MMS
     ON DimMMSProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'N'   
   JOIN #DimReportingHierarchy                                                 ------- This improves performance but only partially filters the records due to the PTDeferredRevenueProductGroupFlag = 'N' condition above, a second filtering has to be done later also
     ON FactAllocatedTransaction.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
   JOIN #DimLocationInfo  DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_date] RevenueMonthDimDate
     ON FactAllocatedTransaction.allocated_month_starting_dim_date_key = RevenueMonthDimDate.dim_date_key
   LEFT JOIN [marketing].[v_dim_description] SalesChannelDimDescription
	   ON FactAllocatedTransaction.sales_channel_dim_description_key = SalesChannelDimDescription.dim_description_key
   LEFT JOIN #PackagesWherePurchaserIsNotServicedCustomer  PKG
	   ON FactAllocatedTransaction.transaction_id = PKG.TranID
	     AND FactAllocatedTransaction.line_number = PKG.TranItemID
	 WHERE 	FactAllocatedTransaction.allocated_month_starting_dim_date_key >=  @FirstOf2MonthsPriorDimDateKey			
       AND FactAllocatedTransaction.transaction_dim_date_key <= @ENDDateDimDateKey				
       AND FactAllocatedTransaction.transaction_amount  <> 0	


UNION ALL


	SELECT '' AS TranItemID,
	       FactAllocatedTransaction.dim_mms_member_key AS PkgPurchasingCustomerDimMemberKey,    ------ Name change
	       IsNull(PKG.PkgServiceCustomer_DimMemberKey,FactAllocatedTransaction.dim_mms_member_key) AS PkgServiceCustomerDimMemberKey,  ------ Name change
		   IsNull(PKG.PkgServiceCustomer_DimMemberKey,FactAllocatedTransaction.dim_mms_member_key) AS OldNewBusinessDimMemberKey,     ------ Name change
		   FactAllocatedTransaction.allocated_amount  AS ItemAmount,
		   FactAllocatedTransaction.dim_product_key AS DimProductKey,
		   FactAllocatedTransaction.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
		   FactAllocatedTransaction.transaction_dim_date_key AS TransactionPostDimDateKey, 				
           10000 AS TransactionPostDimTimeKey,      ----- key for midnight
		   FactAllocatedTransaction.allocated_month_starting_dim_date_key AS RevenuePostingMonthStartingDimDateKey,
		   RevenueMonthDimDate.four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth,
		   FactAllocatedTransaction.allocated_dim_club_key as DimClubKey,    ------ new name
		   CASE WHEN FactAllocatedTransaction.transaction_type = 'Refund'
		        THEN 'Y'
				ELSE 'N'
				END RefundFlag,
		   CASE WHEN FactAllocatedTransaction.transaction_type = 'Charge'
		        THEN 'Y'
				ELSE 'N'
				END ChargeFlag,
           NULL AS DimProductSCDKey,
		   ----FactClubPOSAllocatedRevenue.DimProductSCDKey,   ------ UDW does not have SCD Keys - return dim_product_key in earlier column
           NULL AS DimReportingHierarchySCDKey,
		   -----FactClubPOSAllocatedRevenue.DimReportingHierarchySCDKey,   ------ UDW does not have SCD Keys - return dim_reporting_hierarchy_Key in earlier column
           CASE WHEN FactAllocatedTransaction.primary_sales_dim_employee_key <> '-998'
		        THEN 'Y'
				ELSE 'N'
				END CommissionedSalesTransactionFlag, 
           FactAllocatedTransaction.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,
           NULL AS SecondarySalesDimEmployeeKey,   ----- UDW does not have secondary employee - leave null
           FactAllocatedTransaction.allocated_quantity AS AllocatedQuantity,
		   FactAllocatedTransaction.transaction_quantity AS SalesQuantity,
		   FactAllocatedTransaction.allocated_amount AS SalesAmount,     ---- matches LTFDM logic for e-commerce transactions
		   0 AS CorporateTransferAmount,    ----- UDW does not have corporate transfer - enter 0
		   FactAllocatedTransaction.discount_amount AS SalesDiscountDollarAmount,
		   NULL AS SourceFactTableKey,   ------ Obsolete
		   NULL AS SourceFactTableDimDescriptionKey,   ------ Obsolete
		   NULL AS DimRevenueAllocationRuleSCDKey,    ------ Obsolete
		   NULL AS SoldNotServicedFlag,    ------ Obsolete
           '' AS MMSTranID,
           FactAllocatedTransaction.dim_mms_transaction_reason_key AS DimTransactionReasonKey,
		   CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.allocation_rule
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.allocation_rule
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN 'Sale Month Activity'	
			   END  RevenueAllocationRuleName,
		   CASE WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
		        THEN (CASE WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Fitness Products'
				   THEN 'OF10220'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Lab Testing'
				   THEN 'OF10217'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Metabolic Assessments'
				   THEN 'OF10108'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Metabolic Conditioning'
				   THEN 'OF10224'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'PT E-Commerce'
				   THEN 'OF10122'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName in('Weight Loss Challenges','90 Day Weight Loss')
				   THEN 'OF10117'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Team Alpha'
				   THEN 'OF10091'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Team Boot Camp'
				   THEN 'OF10094'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Team Burn'
				   THEN 'OF10095'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Team Cut'
				   THEN 'OF10096'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Tri-PT'
				   THEN 'OF10121'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'MyHealthScore'
				   THEN 'OF10118'
				   WHEN DimReportingHierarchy_HealthCheckUSA.DepartmentName = 'Personal Training'
				   THEN 'OF10084'
				   ELSE ''
				   END)
				WHEN FactAllocatedTransaction.sales_source = 'Hybris'
		        THEN ( CASE WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Fitness Products'
				   THEN 'OF10220'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Lab Testing'
				   THEN 'OF10217'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Metabolic Assessments'
				   THEN 'OF10108'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Metabolic Conditioning'
				   THEN 'OF10224'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'PT E-Commerce'
				   THEN 'OF10122'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName in('Weight Loss Challenges','90 Day Weight Loss')
				   THEN 'OF10117'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Team Alpha'
				   THEN 'OF10091'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Team Boot Camp'
				   THEN 'OF10094'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Team Burn'
				   THEN 'OF10095'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Team Cut'
				   THEN 'OF10096'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Tri-PT'
				   THEN 'OF10121'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'MyHealthScore'
				   THEN 'OF10118'
				   WHEN DimReportingHierarchy_Hybris.DepartmentName = 'Personal Training'
				   THEN 'OF10084'
				   ELSE ''
				   END)
				WHEN FactAllocatedTransaction.sales_source = 'Magento'
				THEN DimMagentoProduct.workday_offering_id 
				END WorkdayOffering,
		   CASE WHEN FactAllocatedTransaction.sales_source in('HealthCheckUSA')
		        THEN 'N'
				WHEN FactAllocatedTransaction.sales_source = 'Hybris'
				     AND DimReportingHierarchy_Hybris.DepartmentName in('Devices','Fitness Products','PT E-Commerce')
				THEN 'N'
				WHEN FactAllocatedTransaction.sales_source = 'Magento'
				     AND DimMagentoProduct.workday_offering_id in('OF10104','OF10105','OF10220','OF10202','OF10122','OF10123','OF10115')
				THEN 'N'
				ELSE 'Y'
				END PTServiceFlag,
		   FactAllocatedTransaction.sales_source AS SalesSource,						
       	   CASE WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'    
				THEN DimHealthCheckUSAProduct.product_sku
				WHEN FactAllocatedTransaction.sales_source = 'Magento'
				THEN DimMagentoProduct.sku
				END SKU,
		   CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.product_description
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
			   END  ProductDescription,
 		 CASE  WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.ProductGroupName
			   END  ProductGroupName, 
		CASE   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  DepartmentName, 
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.SubdivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.SubdivisionName
			   END  SubdivisionName,
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DivisionName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'   
                  THEN DimReportingHierarchy_Magento.DivisionName
			   END  DivisionName,
		   FactAllocatedTransaction.transaction_type AS TransactionType,
		   CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.code
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN CONVERT(VARCHAR(50),DimHealthCheckUSAProduct.product_sku)
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'   
                  THEN CONVERT(VARCHAR(50),DimMagentoProduct.sku)
			   END SourceProductID,
			SalesChannelDimDescription.description AS SalesChannel

FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedTransaction
   LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactAllocatedTransaction.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactAllocatedTransaction.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHybrisProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactAllocatedTransaction.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHealthCheckUSAProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactAllocatedTransaction.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactAllocatedTransaction.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMagentoProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'Y'   
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'Y'   
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'Y' 
   JOIN #DimReportingHierarchy                                                 ------- This improves performance but  only partially filters the records due to the PTDeferredRevenueProductGroupFlag = 'N' condition above, a second filtering has to be done later also
     ON FactAllocatedTransaction.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey  
   JOIN #DimLocationInfo  DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_date] RevenueMonthDimDate
     ON FactAllocatedTransaction.allocated_month_starting_dim_date_key = RevenueMonthDimDate.dim_date_key
   LEFT JOIN [marketing].[v_dim_description] SalesChannelDimDescription
	   ON FactAllocatedTransaction.sales_channel_dim_description_key = SalesChannelDimDescription.dim_description_key
   LEFT JOIN #PackagesWherePurchaserIsNotServicedCustomer  PKG
	   ON FactAllocatedTransaction.transaction_id = PKG.TranID
	     AND FactAllocatedTransaction.line_number = PKG.TranItemID
	 WHERE 	FactAllocatedTransaction.allocated_month_starting_dim_date_key >=  @EComm60DayChallengeRevenueStartDimDateKey			
       AND FactAllocatedTransaction.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndDimDateKey				
       AND FactAllocatedTransaction.transaction_amount  <> 0	
	   AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')



IF OBJECT_ID('tempdb.dbo.#ReportMonthTransactionMembers', 'U') IS NOT NULL
  DROP TABLE #ReportMonthTransactionMembers; 

  ----- to find all current month business members				
				
SELECT 	DimMember.dim_mms_member_key AS DimMemberKey,   
        DimMember.member_id AS MemberID,
		DimMember.join_date_key AS JoinDimDateKey,
		Sum(ReportMonthCustomers.ItemAmount) AS ReportMonthAmount,
		ReportMonthCustomers.PTServiceFlag
INTO #ReportMonthTransactionMembers 
FROM #OldNewBusinessCustomerRecords  ReportMonthCustomers												
JOIN [marketing].[v_dim_mms_member] DimMember				
  ON ReportMonthCustomers.OldNewBusinessDimMemberKey = DimMember.dim_mms_member_key
WHERE ReportMonthCustomers.RevenuePostingMonthStartingDimDateKey = @FirstOfCurrentMonthDimDateKey				
  AND ReportMonthCustomers.TransactionPostDimDateKey <= @ENDDateDimDateKey	
  AND ReportMonthCustomers.DivisionName is not null    ------ this removes records which came through, due to the left join on dimreportinghierarhcy in earlier query				
  GROUP BY DimMember.dim_mms_member_key,    ------- name change
        DimMember.member_id,
		DimMember.join_date_key,
		ReportMonthCustomers.PTServiceFlag

				
	
IF OBJECT_ID('tempdb.dbo.#OldBusinessMembers1', 'U') IS NOT NULL
  DROP TABLE #OldBusinessMembers1; 
			
 ----- Find which of the current month business members have had business in the prior 2 months				
SELECT DimCustomer.MemberID,
       DimCustomer.DimMemberKey,				
       SUM(OldBusinessRecords.ItemAmount) AS Amount,				
	   OldBusinessRecords.PTServiceFlag
  INTO #OldBusinessMembers1
FROM #OldNewBusinessCustomerRecords	OldBusinessRecords				
JOIN #ReportMonthTransactionMembers DimCustomer				
  ON OldBusinessRecords.OldNewBusinessDimMemberKey = DimCustomer.DimMemberKey
  AND DimCustomer.PTServiceFlag = 'Y'				
JOIN #DimLocationInfo DimLocation
  ON OldBusinessRecords.DimClubKey = DimLocation.DimClubKey				
WHERE  OldBusinessRecords.TransactionPostDimDateKey < @FirstOfCurrentMonthDimDateKey				
  AND OldBusinessRecords.RevenuePostingMonthStartingDimDateKey< @FirstOfCurrentMonthDimDateKey				
  AND DimLocation.NewBusinessOnlyClub = 'N'	
  AND OldBusinessRecords.PTServiceFlag = 'Y'
  AND OldBusinessRecords.DivisionName is not null    ------ this removes records which came through, due to the left join on dimreportinghierarhcy in earlier query		
 GROUP BY DimCustomer.MemberID,
       DimCustomer.DimMemberKey,
	   OldBusinessRecords.PTServiceFlag
	   	
		
		
				

	 ---- to eliminate from Old Business, members who had only fully refunded purchases		
IF OBJECT_ID('tempdb.dbo.#OldBusinessMembers', 'U') IS NOT NULL
  DROP TABLE #OldBusinessMembers; 	
				
SELECT MemberID,
       DimMemberKey,
	   PTServiceFlag
INTO #OldBusinessMembers				
FROM #OldBusinessMembers1				
  WHERE  Amount <> 0	


	
IF OBJECT_ID('tempdb.dbo.#Results_ReportMonth_Memberships', 'U') IS NOT NULL
  DROP TABLE #Results_ReportMonth_Memberships; 				
				
SELECT ReportMonthCustomer.DimMemberKey,				
       ReportMonthCustomer.JoinDimDateKey,						
	   DimLocation.MMSClubName,			
	   DimLocation.MMSClubID,			
	   DimLocation.ClubCode,
	   DimLocation.DimClubKey,				
	   'Local Currency' AS HeaderReportingCurrency,	
	   CASE WHEN CurrentMonthRecords.PTServiceFlag ='Y'
	        THEN 'Services'
			ELSE 'Products'
			END ProductsOrServicesGrouping,			
	   CurrentMonthRecords.DimReportingHierarchyKey,
	   SUM(CurrentMonthRecords.ItemAmount) AS ItemAmount,			 

	   SUM(CASE WHEN CurrentMonthRecords.PTServiceFlag ='Y'	                                          ------ Service products only	
				          AND (ReportMonthCustomer.JoinDimDateKey >= @FirstOfPriorMonthDimDateKey      ---- joined since the 1st of the prior month - even if they bought something last month
						        OR DimLocation.NewBusinessOnlyClub = 'Y')                              ---- or club is new
				         THEN  CurrentMonthRecords.ItemAmount                                          
				         ELSE 0
				     END) AS NewBusiness_NewMember_Amount,

	     SUM(CASE WHEN IsNull(#OldBusinessMembers.MemberID,0) = 0      ------ Not old business member
		           AND CurrentMonthRecords.PTServiceFlag ='Y'			------ Service products only	
				   AND ReportMonthCustomer.JoinDimDateKey < @FirstOfPriorMonthDimDateKey      ---- joined prior to the 1st of the prior month
				   AND DimLocation.NewBusinessOnlyClub = 'N'                                  ---- and club is not new
				THEN  CurrentMonthRecords.ItemAmount
				ELSE 0
				END) AS NewBusiness_ExistingMember_Amount,  

		SUM(CASE WHEN IsNull(#OldBusinessMembers.MemberID,0) > 0        ----- Old Business member
		           AND  CurrentMonthRecords.PTServiceFlag ='Y'			----- Service products only
				   AND CurrentMonthRecords.ChargeFlag = 'Y'            ----- Charge transaction  
				   AND ReportMonthCustomer.JoinDimDateKey < @FirstOfPriorMonthDimDateKey     ---- Did not join since the 1st of the prior month
				   AND DimLocation.NewBusinessOnlyClub = 'N'                   ---  club is not new
				THEN  CurrentMonthRecords.ItemAmount
				WHEN  CurrentMonthRecords.RefundFlag = 'Y'               ----- For all refunds for non-new members (includes refunds for those who would otherwise be "existing members")
				   AND CurrentMonthRecords.PTServiceFlag ='Y'
				   AND ReportMonthCustomer.JoinDimDateKey < @FirstOfPriorMonthDimDateKey      ---- joined prior to the 1st of the prior month
				   AND DimLocation.NewBusinessOnlyClub = 'N' 
                THEN  CurrentMonthRecords.ItemAmount
				ELSE 0
				END) AS OldBusiness_EFT_Amount,


		SUM(CASE WHEN IsNull(#OldBusinessMembers.MemberID,0) > 0        ----- Old Business member
		           AND CurrentMonthRecords.PTServiceFlag ='Y'           ----- Service products only
		           AND CurrentMonthRecords.ChargeFlag = 'N'             ----- not Charge or a refund transaction
				   AND CurrentMonthRecords.RefundFlag = 'N'	
				   AND ReportMonthCustomer.JoinDimDateKey < @FirstOfPriorMonthDimDateKey     ---- Did not join since the 1st of the prior month
				   AND DimLocation.NewBusinessOnlyClub = 'N'                   ---  club is not new
				THEN  CurrentMonthRecords.ItemAmount
				ELSE 0
				END) AS OldBusiness_NonEFT_Amount,

		SUM(CASE WHEN CurrentMonthRecords.PTServiceFlag ='N'		    ----- Non-Service products only
				THEN  CurrentMonthRecords.ItemAmount
				ELSE 0
				END) AS Products_Amount,
		CurrentMonthRecords.TransactionType
		
INTO #Results_ReportMonth_Memberships		
FROM #OldNewBusinessCustomerRecords CurrentMonthRecords										
JOIN #ReportMonthTransactionMembers ReportMonthCustomer				
  ON CurrentMonthRecords.OldNewBusinessDimMemberKey = ReportMonthCustomer.DimMemberKey	
  AND CurrentMonthRecords.PTServiceFlag = ReportMonthCustomer.PTServiceFlag
JOIN #DimLocationInfo DimLocation				
  ON CurrentMonthRecords.DimClubKey = DimLocation.DimClubKey				
LEFT JOIN #OldBusinessMembers #OldBusinessMembers				
  ON ReportMonthCustomer.DimMemberKey = #OldBusinessMembers.DimMemberKey
  AND #OldBusinessMembers.PTServiceFlag = ReportMonthCustomer.PTServiceFlag				
WHERE CurrentMonthRecords.RevenuePostingMonthStartingDimDateKey = @FirstOfCurrentMonthDimDateKey				
  AND CurrentMonthRecords.TransactionPostDimDateKey <= @ENDDateDimDateKey
  AND CurrentMonthRecords.DivisionName is not null    ------ this removes records which came through, due to the left join on dimreportinghierarhcy in earlier query										
  GROUP BY  ReportMonthCustomer.DimMemberKey,				
       ReportMonthCustomer.JoinDimDateKey,						
	   DimLocation.MMSClubName,			
	   DimLocation.MMSClubID,			
	   DimLocation.ClubCode,
	   DimLocation.DimClubKey,				
       CASE WHEN CurrentMonthRecords.PTServiceFlag ='Y'
	        THEN 'Services'
			ELSE 'Products'
			END,					
	   CurrentMonthRecords.DimReportingHierarchyKey,
	   CurrentMonthRecords.TransactionType

	   
IF OBJECT_ID('tempdb.dbo.#Results_OldNewBusiness_ByMemberAndHierarchyKey', 'U') IS NOT NULL
  DROP TABLE #Results_OldNewBusiness_ByMemberAndHierarchyKey; 

  SELECT DimMemberKey,
  DimReportingHierarchyKey,
  DimClubKey,	
  Convert(varchar,DimMemberKey) + '-'+ Convert(varchar,DimReportingHierarchyKey) AS joinbusinesstypekey,
  @FirstOfCurrentMonthDimDateKey AS firsttransactiondimdatekey,
  @ENDDateDimDateKey AS lasttransactiondimdatekey,
  @FirstOfCurrentMonthDimDateKey AS monthstartingdimdatekey,
  CASE WHEN Products_Amount <> 0
       THEN 'Products'
       WHEN (NewBusiness_ExistingMember_Amount + NewBusiness_NewMember_Amount) <> 0
       THEN 'New Business'
	   ELSE 'Old Business'
	   END BusinessType,
  CASE WHEN Products_Amount <> 0
       THEN 'Products'
       WHEN NewBusiness_NewMember_Amount <> 0
       THEN 'New Member'
	   WHEN NewBusiness_ExistingMember_Amount <>0
	   THEN 'Existing Member'
	   WHEN OldBusiness_EFT_Amount <> 0
	   THEN 'EFT Amount'	   
	   ELSE 'Non-EFT Amount'
	   END BusinessSubType,
   SUM(ItemAmount) AS ItemAmount,
   TransactionType
  INTO #Results_OldNewBusiness_ByMemberAndHierarchyKey   
  FROM 	#Results_ReportMonth_Memberships 
  WHERE ItemAmount <> 0
  GROUP BY 
      DimMemberKey,
      DimReportingHierarchyKey,
	  DimClubKey,	
      Convert(varchar,DimMemberKey) + '-'+ Convert(varchar,DimReportingHierarchyKey),
  CASE WHEN Products_Amount <> 0
       THEN 'Products'
       WHEN (NewBusiness_ExistingMember_Amount + NewBusiness_NewMember_Amount) <> 0
       THEN 'New Business'
	   ELSE 'Old Business'
	   END,
  CASE WHEN Products_Amount <> 0
       THEN 'Products'
       WHEN NewBusiness_NewMember_Amount <> 0
       THEN 'New Member'
	   WHEN NewBusiness_ExistingMember_Amount <>0
	   THEN 'Existing Member'
	   WHEN OldBusiness_EFT_Amount <> 0
	   THEN 'EFT Amount'	   
	   ELSE 'Non-EFT Amount'
	   END,
	   TransactionType
	   

	   IF OBJECT_ID('tempdb.dbo.#PreliminaryResult', 'U') IS NOT NULL
       DROP TABLE #PreliminaryResult; 

	   SELECT Detail.SalesSource,
	          DimLocation.Region,
			  DimLocation.MMSClubName,
			  DimLocation.MMSClubID,
			  DimLocation.GLClubID,
			  TransactionPostDate.standard_date_name + ' ' + CASE WHEN TransactionPostTime.display_12_hour_time = 'N/A' 
			                                                           THEN '12:00 AM' 
																	   ELSE TransactionPostTime.display_12_hour_time 
																	   END AS  SaleDateAndTime,
	          TransactionPostDate.calendar_date AS PostedDate,
			  Detail.TransactionType,
			  Detail.SourceProductID,
              Detail.ProductDescription,
			  Detail.DepartmentName AS RevenueReportingDepartmentName,
			  Detail.ProductGroupName AS RevenueProductGroup,
              DimEmployee.employee_id AS PrimarySellingTeamMemberID,
			  CASE WHEN DimEmployee.dim_employee_key <= '0' 
					    THEN NULL
                        ELSE DimEmployee.last_name + ', ' + DimEmployee.first_name 
					END PrimarySellingTeamMember,
			  DimCustomer.membership_id AS MembershipID,
			  DimMembership.membership_type AS MembershipTypeDescription,
			  DimCustomer.member_id AS MemberID,
			  DimCustomer.customer_name_last_first AS MemberName,
	          CASE WHEN MemberJoinDate.dim_date_key < '0'
			       THEN NULL
				   ELSE MemberJoinDate.calendar_date 
				   END MemberJoinDate,    
			  Detail.FourDigitYearDashTwoDigitMonth AS RevenueYearMonth,
	          Detail.AllocatedQuantity AS RevenueQuantity,
			  Detail.ItemAmount AS RevenueAmount,
		      Detail.SalesQuantity AS SaleQuantity,
			  Detail.SalesAmount AS SaleAmount,
			  Detail.SalesDiscountDollarAmount AS TotalDiscountAmount,
			  'Local Currency' AS CurrencyCode,
			  TransactionPostDate.dim_date_key AS SaleDimDateKey,
			  TransactionPostTime.dim_time_key AS SaleDimTimeKey,
			  DimCustomer.first_name  AS MemberFirstName,
			  DimCustomer.last_name AS MemberLastName,
			  Detail.SoldNotServicedFlag,
			  DimTransactionReason.Description AS TransactionReason,
			  Detail.SalesChannel,
			  Detail.CorporateTransferAmount,
			  Detail.SubdivisionName,
			  Detail.DivisionName,
			  @ReportStartDate ReportStartDate,
              @ReportEndDate ReportEndDate,
              @ReportRunDateTime ReportRunDateTime,
              NULL AS RevenueReportingDepartmentNameCommaList,    ------- Create in Cognos
              NULL AS RevenueProductGroupNameCommaList,          ------- Create in Cognos
              @HeaderDivisionList HeaderDivisionList,
              NULL AS HeaderSubdivisionList,          ------- Create in Cognos
			  DimCustomer.dim_mms_member_key AS DimMemberKey,   ------- Name Change
			  Detail.DimReportingHierarchyKey,
			  OldNewCategorySummary.BusinessType,
			  OldNewCategorySummary.BusinessSubType,
			  NULL  AS HeaderBusinessTypeFilter,          ------- Create in Cognos
			  DimLocation.WorkdayRegion    
		INTO #PreliminaryResult
	     FROM #OldNewBusinessCustomerRecords Detail   
		   JOIN #Results_OldNewBusiness_ByMemberAndHierarchyKey  OldNewCategorySummary
		     ON Detail.OldNewBusinessDimMemberKey = OldNewCategorySummary.DimMemberKey
			 AND Detail.DimReportingHierarchyKey = OldNewCategorySummary.DimReportingHierarchyKey
			 AND Detail.DimClubKey = OldNewCategorySummary.DimClubKey
			 AND Detail.TransactionType = OldNewCategorySummary.TransactionType
			 AND OldNewCategorySummary.ItemAmount <> 0      ------- 
		   JOIN #DimLocationInfo DimLocation
		     ON Detail.DimClubKey = DimLocation.DimClubKey
		   JOIN [marketing].[v_dim_date] TransactionPostDate
		     ON Detail.TransactionPostDimDateKey = TransactionPostDate.dim_date_key
		   JOIN [marketing].[v_dim_time] TransactionPostTime
		     ON Detail.TransactionPostDimTimeKey = TransactionPostTime.dim_time_key
		   JOIN [marketing].[v_dim_mms_member]  DimCustomer
		     ON Detail.OldNewBusinessDimMemberKey = DimCustomer.dim_mms_member_key
		   JOIN [marketing].[v_dim_date] MemberJoinDate
		     ON DimCustomer.join_date_key = MemberJoinDate.dim_date_key
		   JOIN [marketing].[v_dim_mms_membership] DimMembership
		     ON DimCustomer.dim_mms_membership_key = DimMembership.dim_mms_membership_key
		   LEFT JOIN [marketing].[v_dim_employee] DimEmployee
		      ON Detail.PrimarySalesDimEmployeeKey = DimEmployee.dim_employee_key
		   LEFT JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason
		      ON Detail.DimTransactionReasonKey = DimTransactionReason.dim_mms_transaction_reason_key
		 WHERE Detail.RevenuePostingMonthStartingDimDateKey = @FirstOfCurrentMonthDimDateKey				
            AND Detail.TransactionPostDimDateKey <= @ENDDateDimDateKey
			AND Detail.DivisionName is not null    ------ this removes records which cam through, due to the left join on dimreportinghierarhcy in earlier query											



SELECT SalesSource,
       Region,
       MMSClubName,
       MMSClubID,
       GLClubID,
       SaleDateAndTime,
       PostedDate,
       TransactionType,
       SourceProductID,
       ProductDescription,
       RevenueReportingDepartmentName,
       RevenueProductGroup,
       PrimarySellingTeamMemberID,
       PrimarySellingTeamMember,
       MembershipID,
       MembershipTypeDescription,
       MemberID,
       MemberName,
	   MemberJoinDate,
       RevenueYearMonth,
       RevenueQuantity,
       RevenueAmount,
       SaleQuantity,
       SaleAmount,
       TotalDiscountAmount,
       CurrencyCode,
       SaleDimDateKey,
       SaleDimTimeKey,
       MemberFirstName,
       MemberLastName,
       SoldNotServicedFlag,
       TransactionReason,
       SalesChannel,
       CorporateTransferAmount,
       DivisionName,
       SubdivisionName,
       ReportStartDate,
       ReportEndDate,
       ReportRunDateTime,
       RevenueReportingDepartmentNameCommaList,
       RevenueProductGroupNameCommaList,
       HeaderDivisionList,
       HeaderSubdivisionList,
	   DimMemberKey,      -------- New Name
	   DimReportingHierarchyKey,
	   BusinessType,
	   BusinessSubType,
	   @BusinessTypeFilter AS HeaderBusinessTypeFilter,
	   WorkdayRegion
FROM #PreliminaryResult
 WHERE IsNull(BusinessType,'NULL') = @BusinessTypeFilter
       OR @BusinessTypeFilter = 'All Transactions'




DROP TABLE #OldNewBusinessCustomerRecords
DROP TABLE #SalesSourceList
DROP TABLE #CommissionTypeList
DROP TABLE #DimLocationInfo
DROP TABLE #DimReportingHierarchy
DROP TABLE #ReportMonthTransactionMembers				
DROP TABLE #OldBusinessMembers1				
DROP TABLE #OldBusinessMembers
DROP TABLE #Results_ReportMonth_Memberships
DROP TABLE #PackagesWherePurchaserIsNotServicedCustomer
DROP TABLE #Results_OldNewBusiness_ByMemberAndHierarchyKey
DROP TABLE #PreliminaryResult


END
