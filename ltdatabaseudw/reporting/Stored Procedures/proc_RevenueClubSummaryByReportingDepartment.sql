CREATE PROC [reporting].[proc_RevenueClubSummaryByReportingDepartment] @StartFourDigitYearDashTwoDigitMonth [CHAR](7),@EndFourDigitYearDashTwoDigitMonth [CHAR](7),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 


 ----- Execution Sample
 ------ exec [reporting].[proc_RevenueClubSummaryByReportingDepartment] '2019-03','2019-03','All Departments'
 -----
 ----- This stored procedure is used by Report ID 89 - Revenue Club Summary - By Reporting Department





DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time



DECLARE @StartMonthStartingDimDateKey INT,
        @PriorYearStartMonthStartingDimDateKey INT
SELECT @StartMonthStartingDimDateKey = DimDate.month_starting_dim_date_key,
       @PriorYearStartMonthStartingDimDateKey = PriorYearDimDate.month_starting_dim_date_key
FROM [marketing].[v_dim_date] DimDate
JOIN [marketing].[v_dim_date] PriorYearDimDate
  ON DimDate.year - 1 = PriorYearDimDate.year
 AND DimDate.month_number_in_year = PriorYearDimDate.month_number_in_year
 AND DimDate.day_number_in_month = PriorYearDimDate. day_number_in_month
WHERE DimDate.four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth
  AND DimDate.day_number_in_month = 1

DECLARE @EndMonthStartingDimDateKey INT,
        @EndMonthEndingDate DATETIME,
        @PriorYearEndMonthStartingDimDateKey INT,
        @PriorYearEndMonthEndingDimDateKey INT,
        @SSSGGrandOpeningDeadlineDimDateKey INT,
        @EndMonthEndingDimDateKey INT
SELECT @EndMonthStartingDimDateKey = DimDate.month_starting_dim_date_key,
       @EndMonthEndingDate = DimDate.month_ending_date,
       @PriorYearEndMonthStartingDimDateKey = PriorYearDimDate.month_starting_dim_date_key,
       @PriorYearEndMonthEndingDimDateKey = PriorYearDimDate.month_ending_dim_date_key,
       @SSSGGrandOpeningDeadlineDimDateKey = PriorYearDimDate.dim_date_key,
       @EndMonthEndingDimDateKey = DimDate.month_ending_dim_date_key
FROM [marketing].[v_dim_date]  DimDate
JOIN  [marketing].[v_dim_date] PriorYearDimDate
  ON DimDate.year- 1 = PriorYearDimDate.year
 AND DimDate.month_number_in_year = PriorYearDimDate.month_number_in_year
 AND DimDate.day_number_in_month = PriorYearDimDate.day_number_in_month
WHERE DimDate.four_digit_year_dash_two_digit_month = @EndFourDigitYearDashTwoDigitMonth
  AND DimDate.day_number_in_month = 1




  ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month
DECLARE @FirstOfReportRangeDimDateKey INT
DECLARE @EndOfReportRangeDimDateKey INT
SET @FirstOfReportRangeDimDateKey = (SELECT MIN(dim_date_key) FROM [marketing].[v_dim_date]  WHERE four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth)
SET @EndOfReportRangeDimDateKey = (SELECT MAX(dim_date_key) FROM [marketing].[v_dim_date] WHERE four_digit_year_dash_two_digit_month = @EndFourDigitYearDashTwoDigitMonth)

DECLARE @EComm60DayChallengeRevenueStartMonthStartDimDateKey INT
  ---- When the requested month is the 2nd month of the quarter, set the start date to the prior month
SET @EComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT CASE WHEN (SELECT month_number_in_year
                    FROM [marketing].[v_dim_date] 
				 WHERE dim_date_key = @FirstOfReportRangeDimDateKey) in (2,5,8,11)
			THEN (SELECT prior_month_starting_dim_date_key
			        FROM [marketing].[v_dim_date]  
			        WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
            ELSE (SELECT month_starting_dim_date_key
                    FROM [marketing].[v_dim_date] 
				   WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
			END 
            FROM [marketing].[v_dim_date] 
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record



DECLARE @EComm60DayChallengeRevenueEndMonthEndDimDateKey INT
  ---- When the requested month is the 1st month of the quarter, set the end date to the prior month
SET @EComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT CASE WHEN (SELECT month_number_in_year
                    FROM [marketing].[v_dim_date]   
				   WHERE dim_date_key = @EndOfReportRangeDimDateKey) in (1,4,7,10)
			THEN (SELECT prior_month_ending_dim_date_key
			        FROM [marketing].[v_dim_date]   
			        WHERE dim_date_key = @EndOfReportRangeDimDateKey)
            ELSE (SELECT month_ending_dim_date_key
                    FROM [marketing].[v_dim_date]   
				   WHERE dim_date_key = @EndOfReportRangeDimDateKey)
			END 
            FROM [marketing].[v_dim_date]  
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record


DECLARE @PriorYearEComm60DayChallengeRevenueStartMonthStartDimDateKey INT
SET @PriorYearEComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT PriorYearDimDate.dim_date_key 
                                                                       FROM [marketing].[v_dim_date] DimDate
                                                                       JOIN [marketing].[v_dim_date] PriorYearDimDate
                                                                         ON DimDate.year - 1 = PriorYearDimDate.year
                                                                           AND DimDate.month_number_in_year = PriorYearDimDate.month_number_in_year
                                                                           AND DimDate.day_number_in_month = PriorYearDimDate.day_number_in_month
																	   WHERE DimDate.dim_date_key = @EComm60DayChallengeRevenueStartMonthStartDimDateKey)




DECLARE @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey INT
SET @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT PriorYearDimDate.month_ending_dim_date_key
                                                                   FROM [marketing].[v_dim_date] DimDate
                                                                   JOIN [marketing].[v_dim_date] PriorYearDimDate
                                                                     ON DimDate.year - 1 = PriorYearDimDate.year
                                                                    AND DimDate.month_number_in_year = PriorYearDimDate.month_number_in_year
                                                                    -----AND DimDate.DayNumberInCalendarMonth = PriorYearDimDate.DayNumberInCalendarMonth  --- changed due to leap year
                                                                  WHERE DimDate.dim_date_key = @EComm60DayChallengeRevenueEndMonthEndDimDateKey
                                                                    AND PriorYearDimDate.last_day_in_month_flag = 'Y')


------- Create Hierarchy temp table to return selected group names      

Exec [reporting].[proc_DimReportingHierarchy_history] 'N/A','N/A',@DepartmentMinDimReportingHierarchyKeyList,'N/A',@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey 
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 

 SELECT DimReportingHierarchyKey,  
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


 IF OBJECT_ID('tempdb.dbo.#RegionTypes', 'U') IS NOT NULL
  DROP TABLE #RegionTypes; 

SELECT RegionType
INTO #RegionTypes
FROM #DimReportingHierarchy
GROUP BY RegionType


  ---- Set variable to return just one region type
DECLARE @RegionType VARCHAR(50)
SET @RegionType = (SELECT CASE WHEN COUNT(*) = 1 THEN MIN(RegionType) ELSE 'MMS Region' END FROM #RegionTypes)



----- When the Regions could be of different types
 -----  must take a 2 step approach
 ----- This query replaces the function used in LTFDM_Revenue "fnRevenueHistoricalDimLocation"
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL  
  DROP TABLE #Clubs;


SELECT DimClub.dim_club_key AS DimClubKey,      ----- new name
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
	   CASE WHEN DimClub.club_code <> '' 
	        THEN DimClub.club_code 
			ELSE DimClub.club_name END ClubCode,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   CASE WHEN @RegionType = 'PT RCL Area' 
             THEN PTRCLRegion.description
           WHEN @RegionType = 'Member Activities Region' 
             THEN MemberActivitiesRegion.description
           WHEN @RegionTYpe = 'MMS Region' 
             THEN MMSRegion.description  
		   END  Region,
	   CASE WHEN club_open_dim_date_key <= @EndMonthEndingDimDateKey
	       THEN 'Open'
		   ELSE 'Presale'
		   END ClubStatus,
       DimClub.club_open_dim_date_key
  INTO #Clubs                                   
  FROM [marketing].[v_dim_club] DimClub
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @StartMonthStartingDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id, 
	   DimClub.club_name,
       CASE WHEN DimClub.club_code <> '' 
	        THEN DimClub.club_code 
			ELSE DimClub.club_name END,
	   DimClub.gl_club_id,
	   DimClub.local_currency_code,
	   CASE WHEN @RegionType = 'PT RCL Area' 
             THEN PTRCLRegion.description
           WHEN @RegionType = 'Member Activities Region' 
             THEN MemberActivitiesRegion.description
           WHEN @RegionTYpe = 'MMS Region' 
             THEN MMSRegion.description  
		   END,
	   CASE WHEN club_open_dim_date_key <= @EndMonthEndingDimDateKey
	       THEN 'Open'
		   ELSE 'Presale'
		   END,
       DimClub.club_open_dim_date_key





--Revenue

IF OBJECT_ID('tempdb.dbo.#PromptYearRevenue', 'U') IS NOT NULL
  DROP TABLE #PromptYearRevenue; 

   SELECT 
         DimLocation.DimClubKey,      ------- Name Change
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
			   END  RevenueReportingDepartmentName,        
          FactAllocatedTransaction.allocated_amount AS ActualAmount,
		  'local currency' AS CurrencyCode,
		  FactAllocatedTransaction.allocated_month_starting_dim_date_key AS DimDateKey,
		  DimLocation.club_open_dim_date_key,
		  DimLocation.Region,
		  DimLocation.ClubStatus
	INTO #PromptYearRevenue    
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
   JOIN #Clubs   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey

   WHERE FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
          AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @EndMonthStartingDimDateKey

UNION ALL

   SELECT DimLocation.DimClubKey,      ------- Name Change
          CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,        
          FactAllocatedTransaction.allocated_amount AS ActualAmount,
		  'local currency' AS CurrencyCode,
	   CASE WHEN  TransactionPostDimDate.month_number_in_year in (1,4,7,10)  
			THEN  TransactionPostDimDate.next_month_starting_dim_date_key
            ELSE  TransactionPostDimDate.month_starting_dim_date_key
			END DimDateKey,
		DimLocation.club_open_dim_date_key,
		DimLocation.Region,
		DimLocation.ClubStatus
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
   JOIN #Clubs   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_date] TransactionPostDimDate
     ON FactAllocatedTransaction.transaction_dim_date_key = TransactionPostDimDate.dim_date_key
   WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartMonthStartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndMonthEndDimDateKey)
		  AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')



IF OBJECT_ID('tempdb.dbo.#RevenueSummary', 'U') IS NOT NULL
  DROP TABLE #RevenueSummary; 

SELECT DimClubKey, 
       club_open_dim_date_key,
	   Region,
	   ClubStatus,
       RevenueReportingDepartmentName, 
       MIN(CurrencyCode) CurrencyCode, 
       SUM(ActualAmount) ActualAmount
  INTO #RevenueSummary     
  FROM #PromptYearRevenue   
  WHERE RevenueReportingDepartmentName is not null
 GROUP BY DimClubKey, RevenueReportingDepartmentName, club_open_dim_date_key,Region,ClubStatus


IF OBJECT_ID('tempdb.dbo.#PriorYearRevenue', 'U') IS NOT NULL
  DROP TABLE #PriorYearRevenue; 

   SELECT 
         DimLocation.DimClubKey,      ------- Name Change
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
			   END  RevenueReportingDepartmentName,        
          FactAllocatedTransaction.allocated_amount AS ActualAmount,
		  'local currency' AS CurrencyCode,
		  FactAllocatedTransaction.allocated_month_starting_dim_date_key AS DimDateKey,
		  DimLocation.club_open_dim_date_key,
		  DimLocation.Region,
		  DimLocation.ClubStatus
	INTO #PriorYearRevenue
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
   JOIN #Clubs   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey

   WHERE FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @PriorYearStartMonthStartingDimDateKey
          AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @PriorYearEndMonthStartingDimDateKey

UNION ALL

   SELECT DimLocation.DimClubKey,      ------- Name Change
          CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,        
          FactAllocatedTransaction.allocated_amount AS ActualAmount,
		  'local currency' AS CurrencyCode,
	    CASE WHEN  TransactionPostDimDate.month_number_in_year in (1,4,7,10)  
			THEN  TransactionPostDimDate.next_month_starting_dim_date_key
            ELSE  TransactionPostDimDate.month_starting_dim_date_key
			END DimDateKey,
		DimLocation.club_open_dim_date_key,
		DimLocation.Region,
		DimLocation.ClubStatus

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
   JOIN #Clubs   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_date] TransactionPostDimDate
     ON FactAllocatedTransaction.transaction_dim_date_key = TransactionPostDimDate.dim_date_key
   WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @PriorYearEComm60DayChallengeRevenueStartMonthStartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey)
		  AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')


IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGClubRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGClubRevenueSummary; 

--Club SSSG calcs
SELECT DimClubKey,
       club_open_dim_date_key,
	   Region,
	   ClubStatus,
       Sum(ActualAmount) ActualAmount
  INTO #PriorYearSSSGClubRevenueSummary   
  FROM #PriorYearRevenue
  WHERE club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey
 GROUP BY DimClubKey,club_open_dim_date_key,Region,ClubStatus

IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGClubRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGClubRevenueSummary; 

SELECT DimClubKey,
       club_open_dim_date_key,
	   Region,
	   ClubStatus,
       Sum(ActualAmount) ActualAmount
  INTO #PromptYearSSSGClubRevenueSummary  
  FROM #PromptYearRevenue
  WHERE club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey
 GROUP BY DimClubKey,club_open_dim_date_key,Region,ClubStatus     

IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGRegionRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGRegionRevenueSummary; 

--Region SSSG calcs
SELECT Region,
       ClubStatus,
       SUM(ActualAmount) ActualAmount
  INTO #PriorYearSSSGRegionRevenueSummary
  FROM #PriorYearRevenue
  WHERE club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey
 GROUP BY Region,ClubStatus 

IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGRegionRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGRegionRevenueSummary; 
         
SELECT Region,
       ClubStatus,
       SUM(ActualAmount) ActualAmount
  INTO #PromptYearSSSGRegionRevenueSummary 
  FROM #PromptYearRevenue
  WHERE club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey
 GROUP BY Region,ClubStatus

IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGStatusRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGStatusRevenueSummary; 

--Status SSSG calcs
SELECT ClubStatus,
       SUM(ActualAmount) ActualAmount
  INTO #PriorYearSSSGStatusRevenueSummary
  FROM #PriorYearRevenue
  WHERE club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey
 GROUP BY ClubStatus 

IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGStatusRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGStatusRevenueSummary; 
         
SELECT ClubStatus,
       SUM(ActualAmount) ActualAmount
  INTO #PromptYearSSSGStatusRevenueSummary   
  FROM #PromptYearRevenue
    WHERE club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey
 GROUP BY ClubStatus  
 
IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGReportRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGReportRevenueSummary;          
         
--Report SSSG calcs
SELECT SUM(ActualAmount) ActualAmount
  INTO #PriorYearSSSGReportRevenueSummary
  FROM #PriorYearRevenue
  WHERE club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey

IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGReportRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGReportRevenueSummary;  

SELECT SUM(ActualAmount) ActualAmount
  INTO #PromptYearSSSGReportRevenueSummary    
  FROM #PromptYearRevenue
 WHERE club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey


IF OBJECT_ID('tempdb.dbo.#SSSGSummary', 'U') IS NOT NULL
  DROP TABLE #SSSGSummary;  


--SSSG summary
SELECT #Clubs.DimClubKey,
       #PriorYearSSSGClubRevenueSummary.ActualAmount AS PriorYearClubActualAmount,
       #PromptYearSSSGClubRevenueSummary.ActualAmount AS PromptYearClubActualAmount,
       #PriorYearSSSGRegionRevenueSummary.ActualAmount AS PriorYearRegionActualAmount,
       #PromptYearSSSGRegionRevenueSummary.ActualAmount AS PromptYearRegionActualAmount,
       #PriorYearSSSGStatusRevenueSummary.ActualAmount AS PriorYearStatusActualAmount,
       #PromptYearSSSGStatusRevenueSummary.ActualAmount AS PromptYearStatusActualAmount,
       #PriorYearSSSGReportRevenueSummary.ActualAmount AS PriorYearReportActualAmount,
       #PromptYearSSSGReportRevenueSummary.ActualAmount AS PromptYearReportActualAmount 
  INTO #SSSGSummary    
  FROM #Clubs 
  LEFT JOIN #PriorYearSSSGClubRevenueSummary
    ON #Clubs.DimClubKey = #PriorYearSSSGClubRevenueSummary.DimClubKey
  LEFT JOIN #PromptYearSSSGClubRevenueSummary
    ON #Clubs.DimClubKey = #PromptYearSSSGClubRevenueSummary.DimClubKey
  LEFT JOIN #PriorYearSSSGRegionRevenueSummary
    ON #Clubs.Region = #PriorYearSSSGRegionRevenueSummary.Region
   AND #Clubs.ClubStatus = #PriorYearSSSGRegionRevenueSummary.ClubStatus
  LEFT JOIN #PromptYearSSSGRegionRevenueSummary
    ON #Clubs.Region = #PromptYearSSSGRegionRevenueSummary.Region
   AND #Clubs.ClubStatus = #PromptYearSSSGRegionRevenueSummary.ClubStatus
  LEFT JOIN #PriorYearSSSGStatusRevenueSummary
    ON #Clubs.ClubStatus = #PriorYearSSSGStatusRevenueSummary.ClubStatus
  LEFT JOIN #PromptYearSSSGStatusRevenueSummary
    ON #Clubs.ClubStatus = #PromptYearSSSGStatusRevenueSummary.ClubStatus
  CROSS JOIN #PriorYearSSSGReportRevenueSummary
  CROSS JOIN #PromptYearSSSGReportRevenueSummary

IF OBJECT_ID('tempdb.dbo.#GoalSummary', 'U') IS NOT NULL
  DROP TABLE #GoalSummary; 

--Goals
SELECT #DimReportingHierarchy.DepartmentName AS RevenueReportingDepartmentName,
       FactRevenueGoal.dim_club_key AS DimClubKey,
	   'local currency' AS CurrencyCode,
	   SUM(FactRevenueGoal.goal_dollar_amount) GoalAmount
  INTO #GoalSummary
  FROM [marketing].[v_fact_revenue_goal] FactRevenueGoal
  JOIN #DimReportingHierarchy
    ON FactRevenueGoal.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
 WHERE FactRevenueGoal.goal_effective_dim_date_key >= @StartMonthStartingDimDatekey
   AND FactRevenueGoal.goal_effective_dim_date_key <= @EndMonthStartingDimDateKey
 GROUP BY FactRevenueGoal.dim_club_key, #DimReportingHierarchy.DepartmentName 



IF OBJECT_ID('tempdb.dbo.#UnassessedRecurrentProductAssessmentDimDateKeys', 'U') IS NOT NULL
  DROP TABLE #UnassessedRecurrentProductAssessmentDimDateKeys; 

--Recurrent Product revenue -- Recurrent Product Scheduling will be moving to Exerp  -  more updates will be coming
SELECT dim_date_key AS DimDateKey,
       day_number_in_month AS DayNumberInCalendarMonth,
       calendar_date AS CalendarDate,
       year as CalendarYear,
	   four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth
 INTO #UnassessedRecurrentProductAssessmentDimDateKeys       
  FROM [marketing].[v_dim_date] DimDate
 WHERE DimDate.dim_date_key <= @EndMonthEndingDimDateKey
   AND DimDate.calendar_date >= CONVERT(Datetime,CONVERT(Varchar,GetDate(),101),101)


IF OBJECT_ID('tempdb.dbo.#RecurrentProductRevenue', 'U') IS NOT NULL
  DROP TABLE #RecurrentProductRevenue; 

SELECT FactMembershipRecurrentProduct.dim_club_key AS DimClubKey,
       #DimReportingHierarchy.DepartmentName AS RevenueReportingDepartmentName,
       FactMembershipRecurrentProduct.original_currency_code AS CurrencyCode,
	   SUM(CONVERT(Decimal(14,2),FactMembershipRecurrentProduct.price) *  CASE WHEN DimRevenueAllocationRule.revenue_allocation_rule_name = 'Sale Month Activity'
	                                                                               THEN CONVERT(Decimal(14,2),DimRevenueAllocationRule.accumulated_ratio) 
																				   ELSE CONVERT(Decimal(14,2),DimRevenueALlocationRule.ratio)
																				  END) ForecastedRecurrentProductRevenue
  INTO #RecurrentProductRevenue
  FROM [marketing].[v_fact_mms_membership_recurrent_product] FactMembershipRecurrentProduct
  JOIN #UnassessedRecurrentProductAssessmentDimDateKeys
    ON FactMembershipRecurrentProduct.assessment_day_of_month = #UnassessedRecurrentProductAssessmentDimDateKeys.DayNumberInCalendarMonth
  JOIN [marketing].[v_dim_mms_product] DimProduct
    ON FactMembershipRecurrentProduct.dim_mms_product_key = DimProduct.dim_mms_product_key
  JOIN #Clubs DimLocation
     ON FactMembershipRecurrentProduct.dim_club_key = DimLocation.DimClubKey 
  LEFT JOIN [marketing].[v_dim_date] HoldStartDimDate
    ON HoldStartDimDate.dim_date_key = FactMembershipRecurrentProduct.hold_start_dim_date_key
   AND HoldStartDimDate.dim_date_key > '-997'
  LEFT JOIN [marketing].[v_dim_date] HoldEndDimDate
    ON HoldEndDimDate.dim_date_key = FactMembershipRecurrentProduct.hold_end_dim_date_key
   AND HoldEndDimDate.dim_date_key > '-997'
  LEFT JOIN [marketing].[v_dim_date] TerminationDimDate
    ON TerminationDimDate.dim_date_key = FactMembershipRecurrentProduct.termination_dim_date_key
   AND TerminationDimDate.dim_date_key > '-997'  
  JOIN #DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN [marketing].[v_dim_revenue_allocation_rule] DimRevenueAllocationRule
    ON DimProduct.allocation_rule = DimRevenueAllocationRule.revenue_allocation_rule_name
   AND DimRevenueAllocationRule.revenue_from_late_transaction_flag = 'N'
   AND #UnassessedRecurrentProductAssessmentDimDateKeys.DimDateKey between DimRevenueAllocationRule.earliest_transaction_dim_date_key and DimRevenueAllocationRule.latest_transaction_dim_date_key
  JOIN [marketing].[v_dim_date] RuleRevenuePostingMonthStartingDimDate
    ON DimRevenueAllocationRule.revenue_posting_month_starting_dim_date_key = RuleRevenuePostingMonthStartingDimDate.dim_date_key


 WHERE FactMembershipRecurrentProduct.activation_dim_date_key <= #UnassessedRecurrentProductAssessmentDimDateKeys.DimDateKey
   AND ISNULL(TerminationDimDate.dim_date_key,99991231) > #UnassessedRecurrentProductAssessmentDimDateKeys.DimDateKey
   AND #UnassessedRecurrentProductAssessmentDimDateKeys.DimDateKey NOT BETWEEN ISNULL(HoldStartDimDate.dim_date_key,99991231) AND ISNULL(HoldEndDimDate.dim_date_key,99991231)
   AND RuleRevenuePostingMonthStartingDimDate.four_digit_year_dash_two_digit_month >= @StartFourDigitYearDashTwoDigitMonth
   AND RuleRevenuePostingMonthStartingDimDate.four_digit_year_dash_two_digit_month <= @EndFourDigitYearDashTwoDigitMonth

 GROUP BY FactMembershipRecurrentProduct.dim_club_key,
          #DimReportingHierarchy.DepartmentName,
          FactMembershipRecurrentProduct.original_currency_code
    
 
 
IF OBJECT_ID('tempdb.dbo.#RevenueAndGoalSummary', 'U') IS NOT NULL
  DROP TABLE #RevenueAndGoalSummary; 
   
--RevenueAndGoals
SELECT CASE WHEN #RevenueSummary.RevenueReportingDepartmentName IS NOT NULL THEN #RevenueSummary.RevenueReportingDepartmentName
            ELSE #GoalSummary.RevenueReportingDepartmentName END RevenueReportingDepartmentName,
       CASE WHEN #RevenueSummary.DimClubKey IS NOT NULL THEN #RevenueSummary.DimClubKey
            ELSE #GoalSummary.DimClubKey END DimClubKey,
       CASE WHEN #RevenueSummary.CurrencyCode IS NOT NULL THEN #RevenueSummary.CurrencyCode
            ELSE #GoalSummary.CurrencyCode END CurrencyCode,
       CASE WHEN #RevenueSummary.ActualAmount IS NOT NULL THEN #RevenueSummary.ActualAmount ELSE 0 END ActualAmount,
       CASE WHEN #GoalSummary.GoalAmount IS NOT NULL THEN #GoalSummary.GoalAmount ELSE 0 END GoalAmount
  INTO #RevenueAndGoalSummary  
  FROM #RevenueSummary
  FULL OUTER JOIN #GoalSummary
    ON #RevenueSummary.DimClubKey = #GoalSummary.DimClubKey
   AND #RevenueSummary.RevenueReportingDepartmentName = #GoalSummary.RevenueReportingDepartmentName


IF OBJECT_ID('tempdb.dbo.#RecurrentRevenueAndGoalSummary', 'U') IS NOT NULL
  DROP TABLE #RecurrentRevenueAndGoalSummary; 

SELECT ISNULL(#RevenueAndGoalSummary.RevenueReportingDepartmentName,#RecurrentProductRevenue.RevenueReportingDepartmentName) RevenueReportingDepartmentName,
       ISNULL(#RevenueAndGoalSummary.DimClubKey,#RecurrentProductRevenue.DimClubKey) DimClubKey,
       ISNULL(#RevenueAndGoalSummary.CurrencyCode,#RecurrentProductRevenue.CurrencyCode) CurrencyCode,
       ISNULL(#RevenueAndGoalSummary.ActualAmount,0) ActualAmount,
       ISNULL(#RevenueAndGoalSummary.GoalAmount,0) GoalAmount,
       ISNULL(CONVERT(Decimal(14,6),#RecurrentProductRevenue.ForecastedRecurrentProductRevenue),0) ForecastedRecurrentProductRevenue
  INTO #RecurrentRevenueAndGoalSummary
  FROM #RevenueAndGoalSummary
  FULL OUTER JOIN #RecurrentProductRevenue 
    ON #RevenueAndGoalSummary.DimClubKey = #RecurrentProductRevenue.DimClubKey
   AND #RevenueAndGoalSummary.RevenueReportingDepartmentName = #RecurrentProductRevenue.RevenueReportingDepartmentName

IF OBJECT_ID('tempdb.dbo.#SummaryByClub', 'U') IS NOT NULL
  DROP TABLE #SummaryByClub; 

SELECT DimClubKey,
       Sum(ActualAmount) ClubActualAmount,
       Sum(GoalAmount) ClubGoalAmount,
       SUM(ForecastedRecurrentProductRevenue) ClubForecastedRecurrentProductRevenue
  INTO #SummaryByClub
  FROM #RecurrentRevenueAndGoalSummary
  GROUP BY DimClubKey

IF OBJECT_ID('tempdb.dbo.#SummaryByRegion', 'U') IS NOT NULL
  DROP TABLE #SummaryByRegion; 

SELECT #Clubs.Region,
       #Clubs.ClubStatus,
       Sum(ActualAmount) RegionActualAmount,
       Sum(GoalAmount) RegionGoalAmount,
       SUM(ForecastedRecurrentProductRevenue) RegionForecastedRecurrentProductRevenue
  INTO #SummaryByRegion
  FROM #RecurrentRevenueAndGoalSummary
  JOIN #Clubs 
    ON #RecurrentRevenueAndGoalSummary.DimClubKey = #Clubs.DimClubKey
  GROUP BY #Clubs.Region,
         #Clubs.ClubStatus

IF OBJECT_ID('tempdb.dbo.#SummaryByClubStatus', 'U') IS NOT NULL
  DROP TABLE #SummaryByClubStatus; 

SELECT #Clubs.ClubStatus,
       Sum(ActualAmount) ClubStatusActualAmount,
       Sum(GoalAmount) ClubStatusGoalAmount,
       SUM(ForecastedRecurrentProductRevenue) ClubStatusForecastedRecurrentProductRevenue
  INTO #SummaryByClubStatus
  FROM #RecurrentRevenueAndGoalSummary
  JOIN #Clubs 
    ON #RecurrentRevenueAndGoalSummary.DimClubKey = #Clubs.DimClubKey
  GROUP BY #Clubs.ClubStatus

IF OBJECT_ID('tempdb.dbo.#SummaryByReport', 'U') IS NOT NULL
  DROP TABLE #SummaryByReport; 

SELECT Sum(ActualAmount) ReportActualAmount,
       Sum(GoalAmount) ReportGoalAmount,
       SUM(ForecastedRecurrentProductRevenue) ReportForecastedRecurrentProductRevenue
  INTO #SummaryByReport
  FROM #RecurrentRevenueAndGoalSummary
  JOIN #Clubs 
    ON #RecurrentRevenueAndGoalSummary.DimClubKey = #Clubs.DimClubKey 


--Result set
SELECT #Clubs.ClubStatus,
       #Clubs.Region,
       #Clubs.ClubCode,
       #Clubs.LocalCurrencyCode,
       #RecurrentRevenueAndGoalSummary.RevenueReportingDepartmentName,
       #RecurrentRevenueAndGoalSummary.ActualAmount,
       #RecurrentRevenueAndGoalSummary.GoalAmount,
       2 DepartmentSortOrder,
       @ReportRunDateTime ReportRunDateTime,
       Cast(CASE WHEN #SummaryByClub.ClubGoalAmount = 0 THEN 0 ELSE Cast(100 * #SummaryByClub.ClubActualAmount/#SummaryByClub.ClubGoalAmount as Int) END as Varchar(10)) + '%' ClubPercentOfGoal,
       Left(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount - #SummaryByClub.ClubGoalAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount - #SummaryByClub.ClubGoalAmount,0) as Money),1))-3) ClubVariance,
       Cast(CASE WHEN #SummaryByRegion.RegionGoalAmount = 0 THEN 0 ELSE Cast(100 * #SummaryByRegion.RegionActualAmount/#SummaryByRegion.RegionGoalAmount as Int) END as Varchar(10)) + '%' RegionPercentOfGoal,
       Left(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount - #SummaryByRegion.RegionGoalAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount - #SummaryByRegion.RegionGoalAmount,0) as Money),1))-3) RegionVariance,
       Cast(CASE WHEN #SummaryByClubStatus.ClubStatusGoalAmount = 0 THEN 0 ELSE Cast(100 * #SummaryByClubStatus.ClubStatusActualAmount/#SummaryByClubStatus.ClubStatusGoalAmount as Int) END as Varchar(10)) + '%' StatusPercentOfGoal,
       Left(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount - #SummaryByClubStatus.ClubStatusGoalAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount - #SummaryByClubStatus.ClubStatusGoalAmount,0) as Money),1))-3) StatusVariance,
       Cast(CASE WHEN #SummaryByReport.ReportGoalAmount = 0 THEN 0 ELSE Cast(100 * #SummaryByReport.ReportActualAmount/#SummaryByReport.ReportGoalAmount as Int) END as Varchar(10)) + '%' ReportPercentOfGoal, 
       Left(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount - #SummaryByReport.ReportGoalAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount - #SummaryByReport.ReportGoalAmount,0) as Money),1))-3) ReportVariance,
       Cast(Cast(CASE WHEN ISNULL(#SSSGSummary.PriorYearClubActualAmount,0)=0 THEN NULL
                      ELSE 100 * (#SSSGSummary.PromptYearClubActualAmount - #SSSGSummary.PriorYearClubActualAmount)/#SSSGSummary.PriorYearClubActualAmount 
                 END as Decimal(11,1)) as Varchar) + '%' ClubSSSG,
       Cast(Cast(CASE WHEN ISNULL(#SSSGSummary.PriorYearRegionActualAmount,0)=0 THEN NULL
                      ELSE 100 * (#SSSGSummary.PromptYearRegionActualAmount - #SSSGSummary.PriorYearRegionActualAmount)/#SSSGSummary.PriorYearRegionActualAmount 
                 END as Decimal(11,1)) as Varchar) + '%' RegionSSSG,
       Cast(Cast(CASE WHEN ISNULL(#SSSGSummary.PriorYearStatusActualAmount,0)=0 THEN NULL
                      ELSE 100 * (#SSSGSummary.PromptYearStatusActualAmount - #SSSGSummary.PriorYearStatusActualAmount)/#SSSGSummary.PriorYearStatusActualAmount 
                 END as Decimal(11,1)) as Varchar) + '%' StatusSSSG,
       Cast(Cast(CASE WHEN ISNULL(#SSSGSummary.PriorYearReportActualAmount,0)=0 THEN NULL
                      ELSE 100 * (#SSSGSummary.PromptYearReportActualAmount - #SSSGSummary.PriorYearReportActualAmount)/#SSSGSummary.PriorYearReportActualAmount 
                 END as Decimal(11,1)) as Varchar) + '%' ReportSSSG,
       #RecurrentRevenueAndGoalSummary.ForecastedRecurrentProductRevenue ForecastedRecurrentProductRevenue,
       Left(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount + #SummaryByClub.ClubForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount + #SummaryByClub.ClubForecastedRecurrentProductRevenue,0) as Money),1))-3) ClubTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount + #SummaryByRegion.RegionForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount + #SummaryByRegion.RegionForecastedRecurrentProductRevenue,0) as Money),1))-3) RegionTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount + #SummaryByClubStatus.ClubStatusForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount + #SummaryByClubStatus.ClubStatusForecastedRecurrentProductRevenue,0) as Money),1))-3) StatusTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount  + #SummaryByReport.ReportForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount  + #SummaryByReport.ReportForecastedRecurrentProductRevenue,0) as Money),1))-3) ReportTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount,0) as Money),1))-3) ClubActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount,0) as Money),1))-3) RegionActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount,0) as Money),1))-3) StatusActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount,0) as Money),1))-3) ReportActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByClub.ClubGoalAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClub.ClubGoalAmount,0) as Money),1))-3) ClubGoal,
       Left(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionGoalAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionGoalAmount,0) as Money),1))-3) RegionGoal,
       Left(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusGoalAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusGoalAmount,0) as Money),1))-3) StatusGoal,
       Left(Convert(Varchar,Cast(Round(#SummaryByReport.ReportGoalAmount,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByReport.ReportGoalAmount,0) as Money),1))-3) ReportGoal,
       Left(Convert(Varchar,Cast(Round(#SummaryByClub.ClubForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClub.ClubForecastedRecurrentProductRevenue,0) as Money),1))-3) ClubForecastedRecurrentProductRevenue,
       Left(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionForecastedRecurrentProductRevenue,0) as Money),1))-3) RegionForecastedRecurrentProductRevenue,
       Left(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusForecastedRecurrentProductRevenue,0) as Money),1))-3) StatusForecastedRecurrentProductRevenue,
       Left(Convert(Varchar,Cast(Round(#SummaryByReport.ReportForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByReport.ReportForecastedRecurrentProductRevenue,0) as Money),1))-3) ReportForecastedRecurrentProductRevenue,
       NULL AS HeaderDivisionList,    ------@HeaderDivisionList HeaderDivisionList,      ----- Must create in Cognos
       NULL AS HeaderSubdivisionList,   -------@HeaderSubdivisionList HeaderSubdivisionList,     ----- Must create in Cognos
       NULL AS RevenueReportingDepartmentNameCommaList    -------- @RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList   ----- Must create in Cognos
  FROM #RecurrentRevenueAndGoalSummary
  JOIN #Clubs 
    ON #RecurrentRevenueAndGoalSummary.DimClubkey = #Clubs.DimClubKey
  JOIN #SummaryByClub 
    ON #RecurrentRevenueAndGoalSummary.DimClubKey = #SummaryByClub.DimClubKey
  JOIN #SummaryByRegion 
    ON #Clubs.Region = #SummaryByRegion.Region
   AND #Clubs.ClubStatus = #SummaryByRegion.ClubStatus
  JOIN #SummaryByClubStatus 
    ON #Clubs.ClubStatus = #SummaryByClubStatus.ClubStatus
  LEFT JOIN #SSSGSummary 
    ON #Clubs.DimClubKey = #SSSGSummary.DimClubKey
  CROSS JOIN #SummaryByReport	
 order by clubcode

DROP TABLE #Clubs
DROP TABLE #UnassessedRecurrentProductAssessmentDimDateKeys
DROP TABLE #RecurrentRevenueAndGoalSummary
DROP TABLE #RecurrentProductRevenue
DROP TABLE #SummaryByClub
DROP TABLE #SummaryByRegion
DROP TABLE #SummaryByClubStatus
DROP TABLE #SSSGSummary
DROP TABLE #SummaryByReport
DROP TABLE #GoalSummary
DROP TABLE #RevenueSummary
DROP TABLE #RevenueAndGoalSummary
DROP TABLE #PriorYearSSSGClubRevenueSummary
DROP TABLE #PromptYearSSSGClubRevenueSummary
DROP TABLE #PriorYearSSSGRegionRevenueSummary
DROP TABLE #PromptYearSSSGRegionRevenueSummary
DROP TABLE #PriorYearSSSGStatusRevenueSummary
DROP TABLE #PromptYearSSSGStatusRevenueSummary
DROP TABLE #PriorYearSSSGReportRevenueSummary
DROP TABLE #PromptYearSSSGReportRevenueSummary
DROP TABLE #DimReportingHierarchy
DROP TABLE #PriorYearRevenue
DROP TABLE #PromptYearRevenue

END
