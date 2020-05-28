CREATE PROC [reporting].[proc_RevenueClubSummaryByProductGroupAndProduct] @StartFourDigitYearDashTwoDigitMonth [VARCHAR](22),@EndFourDigitYearDashTwoDigitMonth [VARCHAR](22),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DimReportingHierarchyKeyList [VARCHAR](8000),@DivisionList [VARCHAR](8000),@SubDivisionList [VARCHAR](8000),@TotalReportingHierarchyKeyCount [INT] AS

BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

--DECLARE @StartFourDigitYearDashTwoDigitMonth VARCHAR(22) = '2019-01'
--DECLARE @EndFourDigitYearDashTwoDigitMonth VARCHAR(22) = '2019-01'
--DECLARE  @DepartmentMinDimReportingHierarchyKeyList VARCHAR(8000) = 'N/A'--'253|6509'
--DECLARE @DimReportingHierarchyKeyList VARCHAR(8000) = 'All Product Groups'
--DECLARE @DivisionList VARCHAR(8000) = 'Personal Training'
--DECLARE @SubdivisionList VARCHAR(8000) =  'Nutrition, Metabolism & Weight Mgmt'
--DECLARE @TotalReportingHierarchyKeyCount INT = '2'



SET @StartFourDigitYearDashTwoDigitMonth = (SELECT CASE WHEN @StartFourDigitYearDashTwoDigitMonth = 'Current Month' THEN CurrentMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Next Month' THEN NextMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                   WHEN @StartFourDigitYearDashTwoDigitMonth = 'Month After Next Month' THEN MonthAfterNextMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                   ELSE @StartFourDigitYearDashTwoDigitMonth END
       FROM [marketing].[v_dim_date] CurrentMonthDimDate
  JOIN [marketing].[v_dim_date] NextMonthDimDate
    ON CurrentMonthDimDate.next_month_starting_dim_date_key = NextMonthDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] MonthAfterNextMonthDimDate
    ON NextMonthDimDate.next_month_starting_dim_date_key = MonthAfterNextMonthDimDate.dim_date_key
WHERE CurrentMonthDimDate.Calendar_Date = CONVERT(DateTime,Convert(Varchar,GetDate()-2,101),101))

SET @EndFourDigitYearDashTwoDigitMonth = (SELECT CASE WHEN @EndFourDigitYearDashTwoDigitMonth = 'Current Month' THEN CurrentMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Next Month' THEN NextMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                 WHEN @EndFourDigitYearDashTwoDigitMonth = 'Month After Next Month' THEN MonthAfterNextMonthDimDate.Four_Digit_Year_Dash_Two_Digit_Month
                                                 ELSE @EndFourDigitYearDashTwoDigitMonth END
  FROM [marketing].[v_dim_date] CurrentMonthDimDate
  JOIN [marketing].[v_dim_date] NextMonthDimDate
    ON CurrentMonthDimDate.next_month_starting_dim_date_key = NextMonthDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] MonthAfterNextMonthDimDate
    ON NextMonthDimDate.next_month_starting_dim_date_key = MonthAfterNextMonthDimDate.dim_date_key
WHERE CurrentMonthDimDate.Calendar_Date = CONVERT(DateTime,Convert(Varchar,GetDate()-2,101),101))
												 


DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '
+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '
+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time

DECLARE @StartMonthStartingDimDateKey INT,
        @PriorYearStartMonthStartingDimDateKey INT,
        @PromptYear INT
SELECT @StartMonthStartingDimDateKey = DimDate.Month_Starting_Dim_Date_Key,
       @PriorYearStartMonthStartingDimDateKey = PriorYearDimDate.Month_Starting_Dim_Date_Key,
       @PromptYear = DimDate.Year
FROM [marketing].[v_dim_date] DimDate
JOIN [marketing].[v_dim_date] PriorYearDimDate
  ON DimDate.Year - 1 = PriorYearDimDate.Year
 AND DimDate.Month_Number_In_Year = PriorYearDimDate.Month_Number_In_Year
 AND DimDate.Day_Number_In_Month = PriorYearDimDate.Day_Number_In_Month
WHERE DimDate.Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth
  AND DimDate.Day_Number_In_Month = 1

DECLARE @EndMonthStartingDimDateKey INT,
        @EndMonthEndingDimDateKey INT,
        @EndMonthEndingDate DATETIME,
        @PriorYearEndMonthStartingDimDateKey INT,
        @SSSGGrandOpeningDeadlineDate DATETIME,
		@SSSGGrandOpeningDeadlineDimDateKey INT
SELECT @EndMonthStartingDimDateKey = DimDate.Month_Starting_Dim_Date_Key,
       @EndMonthEndingDimDateKey = DimDate.month_ending_dim_date_key,
       @EndMonthEndingDate = DimDate.Month_Ending_Date,
       @PriorYearEndMonthStartingDimDateKey = PriorYearDimDate.Month_Starting_Dim_Date_Key,
       @SSSGGrandOpeningDeadlineDate = PriorYearDimDate.Calendar_Date,
	   @SSSGGrandOpeningDeadlineDimDateKey = PriorYearDimDate.dim_date_key
FROM [marketing].[v_dim_date] DimDate
JOIN [marketing].[v_dim_date] PriorYearDimDate
  ON DimDate.Year - 1 = PriorYearDimDate.Year
 AND DimDate.Month_Number_In_Year = PriorYearDimDate.Month_Number_In_Year
 AND DimDate.Day_Number_In_Month = PriorYearDimDate.Day_Number_In_Month
WHERE DimDate.Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth
  AND DimDate.Day_Number_In_Month = 1

    ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month
DECLARE @FirstOfReportRangeDimDateKey INT
DECLARE @EndOfReportRangeDimDateKey INT
SET @FirstOfReportRangeDimDateKey = (SELECT MIN(dim_date_key) FROM [marketing].[v_dim_date] where Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth)
SET @EndOfReportRangeDimDateKey = (SELECT MAX(dim_date_key) FROM [marketing].[v_dim_date] where Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth)

DECLARE @EComm60DayChallengeRevenueStartMonthStartDimDateKey INT
  ---- When the requested month is the 2nd month of the quarter, set the start date to the prior month
SET @EComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT CASE WHEN (Select Month_Number_In_Year 
                    From [marketing].[v_dim_date] 
				   Where dim_date_key = @FirstOfReportRangeDimDateKey) in (2,5,8,11)
			THEN (Select Prior_Month_Starting_Dim_Date_Key
			        FROM [marketing].[v_dim_date] 
			        WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
            ELSE (Select Month_Starting_Dim_Date_Key
                    From [marketing].[v_dim_date] 
				   Where dim_date_key = @FirstOfReportRangeDimDateKey)
			END 
            FROM [marketing].[v_dim_date]
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record


DECLARE @EComm60DayChallengeRevenueEndMonthEndDimDateKey INT
  ---- When the requested month is the 1st month of the quarter, set the end date to the prior month
SET @EComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT CASE WHEN (Select Month_Number_In_Year 
                    From [marketing].[v_dim_date] 
				   Where dim_date_key = @EndOfReportRangeDimDateKey) in (1,4,7,10)
			THEN (Select Prior_Month_Ending_Dim_Date_Key
			        FROM [marketing].[v_dim_date] 
			        WHERE dim_date_key = @EndOfReportRangeDimDateKey)
            ELSE (Select month_ending_dim_date_key
                    From [marketing].[v_dim_date] 
				   Where dim_date_key = @EndOfReportRangeDimDateKey)
			END 
            FROM [marketing].[v_dim_date]
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record


DECLARE @PriorYearEComm60DayChallengeRevenueStartMonthStartDimDateKey INT
SET @PriorYearEComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT PriorYearDimDate.dim_date_key 
                                                                       FROM [marketing].[v_dim_date] DimDate
                                                                       JOIN [marketing].[v_dim_date] PriorYearDimDate
                                                                         ON DimDate.Year - 1 = PriorYearDimDate.Year
                                                                           AND DimDate.Month_Number_In_Year = PriorYearDimDate.Month_Number_In_Year
                                                                           AND DimDate.Day_Number_In_Month = PriorYearDimDate.Day_Number_In_Month
																	   WHERE DimDate.dim_date_key = @EComm60DayChallengeRevenueStartMonthStartDimDateKey)



DECLARE @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey INT
SET @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT PriorYearDimDate.month_ending_dim_date_key
                                                                   FROM [marketing].[v_dim_date] DimDate
                                                                   JOIN [marketing].[v_dim_date] PriorYearDimDate
                                                                     ON DimDate.Year - 1 = PriorYearDimDate.Year
                                                                    AND DimDate.Month_Number_In_Year = PriorYearDimDate.Month_Number_In_Year
                                                                    -----AND DimDate.Day_Number_In_Month = PriorYearDimDate.Day_Number_In_Month  --- changed due to leap year
                                                                  WHERE DimDate.dim_date_key = @EComm60DayChallengeRevenueEndMonthEndDimDateKey
                                                                    AND PriorYearDimDate.Last_Day_In_Month_Flag = 'Y')
  
  
------- Create Hierarchy temp table to return selected group names      

Exec [reporting].[proc_DimReportingHierarchy_history] @DivisionList,@SubdivisionList,@DepartmentMinDimReportingHierarchyKeyList,@DimReportingHierarchyKeyList,@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey 
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
 
 DECLARE @HeaderDivisionList VARCHAR(8000)
DECLARE @HeaderSubdivisionList VARCHAR(8000)
SET @HeaderDivisionList = CASE WHEN @DivisionList like '%All Divisions%' 
                                    THEN 'All Divisions'
                                  ELSE REPLACE(@DivisionList, '|', ', ') END
SET  @HeaderSubdivisionList = CASE WHEN @SubdivisionList like '%All Subdivisions%'
                                          THEN 'All Subdivisions'
                                     ELSE REPLACE(@SubdivisionList, '|', ', ') END

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
	   

IF OBJECT_ID('tempdb.dbo.#PromptYearRevenue', 'U') IS NOT NULL
  DROP TABLE #PromptYearRevenue; 

  SELECT 
	DimClubKey,
	Region,
	ClubName,
	clubcode,
	RevenueReportingDepartmentName,
	RevenueProductGroupName,
    RevenueProductGroupSortOrder,
	Product_Description,
	SUM(ActualAmount) as ActualAmount
	INTO #PromptYearRevenue  
	FROM
	(
SELECT DimLocation.DimClubKey,
       DimLocation.Region,
       DimLocation.ClubName,
       DimLocation.ClubCode,
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
			   END  RevenueProductGroupName,
       1 AS RevenueProductGroupSortOrder,
	   CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimCafeProduct.menu_item_name
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.Product_Description
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimMMSProduct.Product_Description
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
			   END  Product_Description,
       FactAllocatedTransaction.allocated_amount AS ActualAmount
	   --FactAllocatedTransaction.transaction_id,
       --FactAllocatedTransaction.line_number
	   --INTO #PromptYearRevenue    
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
   
    JOIN #DimReportingHierarchy DimReportingHierarchy_Cafe
     --ON DimCafeProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Cafe.DimReportingHierarchyKey 
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Cafe.DimReportingHierarchyKey 

    JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
    -- ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey 
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'N'   

    JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     --ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'N'   

    JOIN #DimReportingHierarchy DimReportingHierarchy_MMS
     --ON DimMMSProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey 
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey 

    JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     --ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey 
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'N'   


   JOIN #Clubs DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey

   WHERE FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
          AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @EndMonthStartingDimDateKey
	

UNION ALL

SELECT DimLocation.DimClubKey,
       DimLocation.Region,
       DimLocation.ClubName,
       DimLocation.ClubCode,
	   CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,
		CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.ProductGroupName	
			   END  RevenueProductGroupName,
       1 AS RevenueProductGroupSortOrder,
	   CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.Product_Description
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
			   END  Product_Description,
       FactAllocatedTransaction.allocated_amount AS ActualAmount
	   --FactAllocatedTransaction.transaction_id,
    --   FactAllocatedTransaction.line_number
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
   
    JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     --ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'Y'   

    JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     --ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'Y'   

    JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     --ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey 

	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'Y'   
   JOIN #Clubs   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_date] TransactionPostDimDate
     ON FactAllocatedTransaction.transaction_dim_date_key = TransactionPostDimDate.dim_date_key
   WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartMonthStartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndMonthEndDimDateKey)
		  AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')) PromptYearRevenue

	GROUP BY 
	DimClubKey,
	Region,
	ClubName,
	clubcode,
	RevenueReportingDepartmentName,
	RevenueProductGroupName,
    RevenueProductGroupSortOrder,
	Product_Description

IF OBJECT_ID('tempdb.dbo.#ClubRevenue', 'U') IS NOT NULL
  DROP TABLE #ClubRevenue; 

SELECT DimClubKey,
       SUM(ActualAmount) ClubActualAmount
  INTO #ClubRevenue
  FROM #PromptYearRevenue
 GROUP BY DimClubKey

IF OBJECT_ID('tempdb.dbo.#RegionRevenue', 'U') IS NOT NULL
  DROP TABLE #RegionRevenue; 
  
SELECT Region,
       SUM(ActualAmount) RegionActualAmount
  INTO #RegionRevenue
  FROM #PromptYearRevenue
 GROUP BY Region
 
 IF OBJECT_ID('tempdb.dbo.#ReportRevenue', 'U') IS NOT NULL
  DROP TABLE #ReportRevenue; 

SELECT SUM(ActualAmount) ReportActualAmount
  INTO #ReportRevenue
  FROM #PromptYearRevenue


IF OBJECT_ID('tempdb.dbo.#PriorYearRevenue', 'U') IS NOT NULL
  DROP TABLE #PriorYearRevenue;

    SELECT 
	DimClubKey,
	Region,
	ClubName,
	clubcode,
	RevenueReportingDepartmentName,
	RevenueProductGroupName,
    RevenueProductGroupSortOrder,
	Product_Description,
	SUM(ActualAmount) as ActualAmount
	INTO #PriorYearRevenue  
	FROM
	(
SELECT DimLocation.DimClubKey,
       DimLocation.Region,
       DimLocation.ClubName,
       DimLocation.ClubCode,
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
			   END  RevenueProductGroupName,
       1 AS RevenueProductGroupSortOrder,
	   CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimCafeProduct.menu_item_name
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.Product_Description
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimMMSProduct.Product_Description
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
			   END  Product_Description,
       FactAllocatedTransaction.allocated_amount AS ActualAmount  
  --INTO #PriorYearRevenue
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

    JOIN #DimReportingHierarchy DimReportingHierarchy_Cafe
     --ON DimCafeProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Cafe.DimReportingHierarchyKey 
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Cafe.DimReportingHierarchyKey 

    JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     --ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'N'   

    JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     --ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'N'   

    JOIN #DimReportingHierarchy DimReportingHierarchy_MMS
    -- ON DimMMSProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey 
	on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey

    JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     --ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'N'   

   JOIN #Clubs   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey

   WHERE FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @PriorYearStartMonthStartingDimDateKey
          AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @PriorYearEndMonthStartingDimDateKey
  
  
UNION ALL

SELECT DimLocation.DimClubKey,
       DimLocation.Region,
       DimLocation.ClubName,
       DimLocation.ClubCode,
	   CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,
		CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
			      THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.ProductGroupName	
			   END  RevenueProductGroupName,
       1 AS RevenueProductGroupSortOrder,
	   CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.Product_Description
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
			   END  Product_Description,
       FactAllocatedTransaction.allocated_amount AS ActualAmount  
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
    JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
    -- ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key =  DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'Y'
	   
    JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     --ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'Y'  

    JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     --ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 on FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'Y'   
   JOIN #Clubs   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_date] TransactionPostDimDate
     ON FactAllocatedTransaction.transaction_dim_date_key = TransactionPostDimDate.dim_date_key
   WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @PriorYearEComm60DayChallengeRevenueStartMonthStartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey)
		  AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento'))PriorYearRevenue
	GROUP BY 	
	DimClubKey,
	Region,
	ClubName,
	clubcode,
	RevenueReportingDepartmentName,
	RevenueProductGroupName,
    RevenueProductGroupSortOrder,
	Product_Description
		  
IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGClubRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGClubRevenueSummary;
  
--Club SSSG calcs
SELECT #PriorYearRevenue.DimClubKey,
       Sum(ActualAmount) ActualAmount
  INTO #PriorYearSSSGClubRevenueSummary
  FROM #PriorYearRevenue
  JOIN #Clubs DimLocation
    ON #PriorYearRevenue.DimClubKey = DimLocation.DimClubKey
   AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey     
 GROUP BY #PriorYearRevenue.DimClubKey
 
 IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGClubRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGClubRevenueSummary;

SELECT #PromptYearRevenue.DimClubKey,
       Sum(ActualAmount) ActualAmount
  INTO #PromptYearSSSGClubRevenueSummary
  FROM #PromptYearRevenue
  JOIN #Clubs DimLocation
    ON #PromptYearRevenue.DimClubKey = DimLocation.DimClubKey
   AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey     
 GROUP BY #PromptYearRevenue.DimClubKey       


IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGRegionRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGRegionRevenueSummary; 
  
--Region SSSG calcs
SELECT DimLocation.Region,
       SUM(ActualAmount) ActualAmount
  INTO #PriorYearSSSGRegionRevenueSummary  
  FROM #PriorYearRevenue
  JOIN #Clubs DimLocation
    ON #PriorYearRevenue.DimClubKey = DimLocation.DimClubKey
   AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey     
 GROUP BY DimLocation.Region
 
 IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGRegionRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGRegionRevenueSummary;
         
SELECT DimLocation.Region,
       SUM(ActualAmount) ActualAmount
  INTO #PromptYearSSSGRegionRevenueSummary  
  FROM #PromptYearRevenue
  JOIN #Clubs DimLocation
    ON #PromptYearRevenue.DimClubKey = DimLocation.DimClubKey
   AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey     
 GROUP BY DimLocation.Region
 
  IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGReportRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGReportRevenueSummary;

--Report SSSG calcs
SELECT SUM(ActualAmount) ActualAmount
  INTO #PriorYearSSSGReportRevenueSummary   
  FROM #PriorYearRevenue
  JOIN #Clubs DimLocation
    ON #PriorYearRevenue.DimClubKey = DimLocation.DimClubKey
   AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey    

 IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGReportRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGReportRevenueSummary;

SELECT SUM(ActualAmount) ActualAmount
  INTO #PromptYearSSSGReportRevenueSummary    
  FROM #PromptYearRevenue
  JOIN #Clubs DimLocation
    ON #PromptYearRevenue.DimClubKey = DimLocation.DimClubKey
   AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey    
   
    IF OBJECT_ID('tempdb.dbo.#SSSGSummary', 'U') IS NOT NULL
  DROP TABLE #SSSGSummary;

--SSSG summary
SELECT DimLocation.DimClubKey,
       #PriorYearSSSGClubRevenueSummary.ActualAmount PriorYearClubActualAmount,
       #PromptYearSSSGClubRevenueSummary.ActualAmount PromptYearClubActualAmount,
       #PriorYearSSSGRegionRevenueSummary.ActualAmount PriorYearRegionActualAmount,
       #PromptYearSSSGRegionRevenueSummary.ActualAmount PromptYearRegionActualAmount,
       #PriorYearSSSGReportRevenueSummary.ActualAmount PriorYearReportActualAmount,
       #PromptYearSSSGReportRevenueSummary.ActualAmount PromptYearReportActualAmount 
  INTO #SSSGSummary    
  FROM #Clubs DimLocation
  LEFT JOIN #PriorYearSSSGClubRevenueSummary
    ON DimLocation.DimClubKey = #PriorYearSSSGClubRevenueSummary.DimClubKey
  LEFT JOIN #PromptYearSSSGClubRevenueSummary 
    ON DimLocation.DimClubKey = #PromptYearSSSGClubRevenueSummary.DimClubKey
  LEFT JOIN #PriorYearSSSGRegionRevenueSummary 
    ON DimLocation.Region = #PriorYearSSSGRegionRevenueSummary.Region
  LEFT JOIN #PromptYearSSSGRegionRevenueSummary 
    ON DimLocation.Region = #PromptYearSSSGRegionRevenueSummary.Region
 CROSS JOIN #PriorYearSSSGReportRevenueSummary
 CROSS JOIN #PromptYearSSSGReportRevenueSummary


IF OBJECT_ID('tempdb.dbo.#ClubGoal', 'U') IS NOT NULL
  DROP TABLE #ClubGoal;

SELECT FactRevenueGoal.dim_club_key AS DimClubKey,   
       DimLocation.ClubName,
       DimLocation.ClubCode,
       DimLocation.Region,
       SUM(FactRevenueGoal.Goal_Dollar_Amount) ClubGoalAmount
  INTO #ClubGoal     
  FROM [marketing].[v_fact_revenue_goal] FactRevenueGoal
  JOIN #DimReportingHierarchy
	ON FactRevenueGoal.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #Clubs DimLocation
    ON FactRevenueGoal.dim_club_key = DimLocation.DimClubKey
 WHERE FactRevenueGoal.goal_effective_dim_date_key >= @StartMonthStartingDimDateKey
   AND FactRevenueGoal.goal_effective_dim_date_key <= @EndMonthStartingDimDateKey
 GROUP BY FactRevenueGoal.dim_club_key, 
          DimLocation.ClubName,
          DimLocation.ClubCode,
          DimLocation.Region

IF OBJECT_ID('tempdb.dbo.#RegionGoal', 'U') IS NOT NULL
  DROP TABLE #RegionGoal;
  
SELECT Region,
       SUM(ClubGoalAmount) RegionGoalAmount
  INTO #RegionGoal
  FROM #ClubGoal
 GROUP BY Region

IF OBJECT_ID('tempdb.dbo.#ReportGoal', 'U') IS NOT NULL
  DROP TABLE #ReportGoal;
  
SELECT SUM(ClubGoalAmount) ReportGoalAmount
  INTO #ReportGoal   
  FROM #ClubGoal

IF OBJECT_ID('tempdb.dbo.#ClubSummary', 'U') IS NOT NULL
  DROP TABLE #ClubSummary;
  
SELECT #PromptYearRevenue.DimClubKey,
       #PromptYearRevenue.Region,
       #PromptYearRevenue.ClubName,
       #PromptYearRevenue.ClubCode,
       #ClubRevenue.ClubActualAmount,
       #ClubGoal.ClubGoalAmount,
       #PromptYearRevenue.RevenueReportingDepartmentName,
       #PromptYearRevenue.RevenueProductGroupSortOrder,
       #PromptYearRevenue.RevenueProductGroupName,
       #PromptYearRevenue.Product_Description,
       #PromptYearRevenue.ActualAmount,
       1 DepartmentSortOrder
  INTO #ClubSummary
  FROM #PromptYearRevenue
  JOIN #ClubRevenue
    ON #PromptYearRevenue.DimClubKey = #ClubRevenue.DimClubKey
  LEFT JOIN #ClubGoal
    ON #ClubGoal.DimClubKey = #ClubRevenue.DimClubKey
UNION ALL
SELECT ISNULL(#PromptYearRevenue.DimClubKey,#ClubGoal.DimClubKey) DimClubKey,
       ISNULL(#PromptYearRevenue.Region,#ClubGoal.Region) Region,
       ISNULL(#PromptYearRevenue.ClubName,#ClubGoal.ClubName) ClubName,
       ISNULL(#PromptYearRevenue.ClubCode,#ClubGoal.ClubCode) ClubCode,
       ISNULL(#ClubRevenue.ClubActualAmount,0) ClubActualAmount,
       ISNULL(#ClubGoal.ClubGoalAmount,0) ClubGoalAmount,
       '',
       1,
       '',
       'Total Actual',
       ISNULL(#PromptYearRevenue.ActualAmount,0) ActualAmount,
       2 DepartmentSortOrder
  FROM #PromptYearRevenue
  JOIN #ClubRevenue
    ON #PromptYearRevenue.DimClubKey = #ClubRevenue.DimClubKey
  FULL OUTER JOIN #ClubGoal
    ON #ClubGoal.DimClubKey = #ClubRevenue.DimClubKey
WHERE (#ClubRevenue.ClubActualAmount IS NOT NULL
   OR #ClubGoal.ClubGoalAmount > 0)
  
SELECT  
		#ClubSummary.Region,
       #ClubSummary.ClubName,
       #ClubSummary.ClubCode,
       CAST(#ClubSummary.ClubActualAmount AS Decimal(26,2)) AS ClubActualAmount,
       CAST( #ClubSummary.ClubGoalAmount AS Decimal(26,2)) AS ClubGoalAmount,
       CASE WHEN #ClubSummary.ClubGoalAmount <> 0 THEN CAST(CONVERT(Decimal(26,2),ISNULL(#ClubSummary.ClubActualAmount,0) * 100 /  #ClubSummary.ClubGoalAmount) AS VARCHAR) + '%'
            ELSE '0%' END AS ClubPercentOfGoal,
       #ClubSummary.RevenueReportingDepartmentName,
       #ClubSummary.RevenueProductGroupSortOrder,
       #ClubSummary.RevenueProductGroupName,
       #ClubSummary.Product_Description AS ProductDescription,   ------- updated with alias to re-name
       CAST(#ClubSummary.ActualAmount AS Decimal(26,2)) AS ActualAmount,
       CAST(ISNULL(#RegionRevenue.RegionActualAmount,0)AS Decimal(26,2)) RegionActualAmount,
       CAST(ISNULL(#RegionGoal.RegionGoalAmount,0)AS Decimal(26,2)) RegionGoalAmount,
       CASE WHEN #RegionGoal.RegionGoalAmount <> 0 THEN CAST(CONVERT(Decimal(26,2),ISNULL(#RegionRevenue.RegionActualAmount,0) * 100 / #RegionGoal.RegionGoalAmount) AS VARCHAR) + '%'
            ELSE '0%' END AS RegionPercentOfGoal,
       CAST(ISNULL(#ReportRevenue.ReportActualAmount,0)AS Decimal(26,2)) ReportActualAmount,
       CAST(ISNULL(#ReportGoal.ReportGoalAmount,0)AS Decimal(26,2)) ReportGoalAmount,
       CASE WHEN #ReportGoal.ReportGoalAmount <> 0 THEN CAST(CONVERT(Decimal(26,2),ISNULL(#ReportRevenue.ReportActualAmount,0) * 100 / #ReportGoal.ReportGoalAmount) AS VARCHAR) + '%'
            ELSE '0%' END AS ReportPercentOfGoal,
       CAST(ISNULL(#ClubSummary.ClubActualAmount,0)- ISNULL(#ClubSummary.ClubGoalAmount,0) AS Decimal(26,2)) ClubVariance,
       CAST(ISNULL(#RegionRevenue.RegionActualAmount,0) - ISNULL(#RegionGoal.RegionGoalAmount,0) AS Decimal(26,2)) RegionVariance,
       CAST(ISNULL(#ReportRevenue.ReportActualAmount,0) - ISNULL(#ReportGoal.ReportGoalAmount,0) AS Decimal(26,2)) ReportVariance,
       NULL RevenueReportingDepartmentNameCommaList,
       'Local' CurrencyCode,
       @ReportRunDateTime ReportRunDateTime,
       @StartFourDigitYearDashTwoDigitMonth + ' through ' + @EndFourDigitYearDashTwoDigitMonth HeaderYearMonthRange,
       #ClubSummary.DepartmentSortOrder,
       @HeaderDivisionList AS HeaderDivisionList,
       @HeaderSubDivisionList AS HeaderSubDivisionList,
       NULL RevenueProductGroupNameCommaList,
       Cast(Cast(100 * CASE WHEN ISNULL(#SSSGSummary.PriorYearClubActualAmount,0) = 0 THEN NULL
                            ELSE (#SSSGSummary.PromptYearClubActualAmount - #SSSGSummary.PriorYearClubActualAmount)/#SSSGSummary.PriorYearClubActualAmount
                       END as Decimal(11,1)) as Varchar) + '%' ClubSSSGAmount,
       Cast(Cast(100 * CASE WHEN ISNULL(#SSSGSummary.PriorYearRegionActualAmount,0) = 0 THEN NULL
                            ELSE (#SSSGSummary.PromptYearRegionActualAmount - #SSSGSummary.PriorYearRegionActualAmount)/#SSSGSummary.PriorYearRegionActualAmount
                       END as Decimal(11,1)) as Varchar) + '%' RegionSSSGAmount,
       Cast(Cast(100 * CASE WHEN ISNULL(#SSSGSummary.PriorYearReportActualAmount,0) = 0 THEN NULL
                            ELSE (#SSSGSummary.PromptYearReportActualAmount - #SSSGSummary.PriorYearReportActualAmount)/#SSSGSummary.PriorYearReportActualAmount
                       END as Decimal(11,1)) as Varchar) + '%' ReportSSSGAmount
  FROM #ClubSummary
 LEFT JOIN #RegionGoal 
    ON #ClubSummary.Region = #RegionGoal.Region
  LEFT JOIN #RegionRevenue
    ON #ClubSummary.Region = #RegionRevenue.Region
 CROSS JOIN #ReportGoal
 CROSS JOIN #ReportRevenue
  LEFT JOIN #SSSGSummary 
    ON #ClubSummary.DimClubKey = #SSSGSummary.DimClubKey
ORDER BY Region, ClubName

DROP TABLE #Clubs
DROP TABLE #PromptYearRevenue
DROP TABLE #PromptYearSSSGRegionRevenueSummary
DROP TABLE #PromptYearSSSGClubRevenueSummary
DROP TABLE #PromptYearSSSGReportRevenueSummary
DROP TABLE #SSSGSummary
DROP TABLE #ClubRevenue
DROP TABLE #RegionRevenue
DROP TABLE #ReportRevenue
DROP TABLE #ClubGoal
DROP TABLE #RegionGoal
DROP TABLE #ReportGoal
DROP TABLE #ClubSummary
DROP TABLE #DimReportingHierarchy
DROP TABLE #PriorYearRevenue
DROP TABLE #PriorYearSSSGClubRevenueSummary
DROP TABLE #PriorYearSSSGRegionRevenueSummary
DROP TABLE #PriorYearSSSGReportRevenueSummary

END

