CREATE PROC [reporting].[proc_RealTimeClubSummarySalesByReportingDepartment] @StartDate [DATETIME],@EndDate [DATETIME],@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000),@RegionList [VARCHAR](8000),@ClubIDList [VARCHAR](8000) AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END


------ Sample Execution
--- Exec [reporting].[proc_RealTimeClubSummarySalesByReportingDepartment] '7/1/2019','7/16/2019','All Departments','Personal Training','Personal Training','All Regions','-1'
------

SET @StartDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                      THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)    
					  ELSE @StartDate END
SET @EndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101) 
					ELSE @EndDate END

-- Use map_utc_time_zone_conversion to determine correct 'current' time, factoring in daylight savings / non daylight savings 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
						  from map_utc_time_zone_conversion
						  where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')


DECLARE @StartDimDateKey INT,
		@StartMonthStartingDimDateKey INT,
		@StartStandardDateDescription VARCHAR(50),
		@PromptYear INT,
		@PriorYearStartDimDateKey INT,
        @PriorYearStartMonthStartingDimDateKey INT,
		@PriorYearStartDate DateTime

SELECT @StartDimDateKey = dim_date_key,
       @StartMonthStartingDimDateKey = month_starting_dim_date_key,
	   @StartStandardDateDescription = standard_date_name,
       @PromptYear = Year,
	   @PriorYearStartDate = prior_year_date
FROM [marketing].[v_dim_date]
WHERE calendar_date = @StartDate


SET @PriorYearStartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @PriorYearStartDate)
SET @PriorYearStartMonthStartingDimDateKey  =(SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @PriorYearStartDate)


DECLARE @EndDimDateKey INT,
        @EndMonthStartingDimDateKey INT,   
        @EndStandardDateDescription VARCHAR(50),
        @EndMonthEndingDate DATETIME,
        @SSSGGrandOpeningDeadlineDate DATETIME,
        @PriorYearEndDimDateKey INT,
        @EndMonthEndingDimDateKey INT,
        @PriorYearEndMonthEndingDimDateKey INT,
        @PromptEndDimDateKey INT,
		@PriorYearEndDate DATETIME

SELECT @EndDimDateKey = CASE WHEN calendar_date > = Convert(Datetime,Convert(Varchar,GetDate(),101),101)  
                             THEN prior_day_dim_date_key ELSE dim_date_key END, 
	   @EndMonthStartingDimDateKey = month_starting_dim_date_key,
	   @EndStandardDateDescription = standard_date_name,
	   @EndMonthEndingDate = month_ending_date,
	   @SSSGGrandOpeningDeadlineDate = prior_year_date,
       @EndMonthEndingDimDateKey = month_ending_dim_date_key,
	   @PromptEndDimDateKey = dim_date_key,
	   @PriorYearEndDate = prior_year_date
FROM [marketing].[v_dim_date]
WHERE calendar_date = @EndDate


SET @PriorYearEndDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @PriorYearEndDate)
SET @PriorYearEndMonthEndingDimDateKey  =(SELECT month_ending_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @PriorYearEndDate)

DECLARE @HeaderDateRange VARCHAR(150)
SET @HeaderDateRange = @StartStandardDateDescription + ' through ' + @EndStandardDateDescription

  ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month

DECLARE @StartDateMonthStartDimDateKey INT
DECLARE @EndDateMonthStartDimDateKey INT
DECLARE @StartDateCalendarMonthNumberInYear INT
DECLARE @EndDateCalendarMonthNumberInYear INT
DECLARE @EndDatePriorMonthEndDateDimDateKey INT

SET @StartDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @StartDimDateKey) 
SET @EndDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey) 
SET @StartDateCalendarMonthNumberInYear = (SELECT month_number_in_year  FROM [marketing].[v_dim_date] WHERE dim_date_key = @StartDimDateKey)
SET @EndDateCalendarMonthNumberInYear = (SELECT month_number_in_year  FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)
SET @EndDatePriorMonthEndDateDimDateKey = (SELECT prior_month_ending_dim_date_key  FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)


DECLARE @EComm60DayChallengeRevenueStartDimDateKey INT
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


DECLARE @EComm60DayChallengeRevenueEndDimDateKey INT
  ---- When the End Date is in the 1st month of the quarter, set the end date to the end of the prior month
SET @EComm60DayChallengeRevenueEndDimDateKey = (SELECT CASE WHEN @EndDateCalendarMonthNumberInYear in(1,4,7,10)
                                                            THEN @EndDatePriorMonthEndDateDimDateKey 
															ELSE @EndDimDateKey
															END
												FROM [marketing].[v_dim_date]  
												WHERE dim_date_key = @EndDimDateKey)  ---- to limit result set to one record


Exec [reporting].[proc_DimReportingHierarchy_history] @DivisionList,@SubDivisionList,@DepartmentMinDimReportingHierarchyKeyList,'N/A',@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey 
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
DECLARE @list_table Varchar(50)
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


--Sales

IF OBJECT_ID('tempdb.dbo.#PromptSales', 'U') IS NOT NULL
  DROP TABLE #PromptSales; 
	
   SELECT DimClubKey,
          RevenueReportingDepartmentName,
		  DimReportingHierarchyKey,
		  SUM(SalesAmount) AS SalesAmount,
		  DimDateKey,
		  SUM(EndDateSalesAmount) AS  EndDateSalesAmount
	INTO #PromptSales   
   FROM (SELECT
	     FactAllocatedTransaction.dim_club_key AS DimClubKey,      ------- Name Change
 		 DimReportingHierarchy_MMS.DepartmentName AS RevenueReportingDepartmentName, 
         DimReportingHierarchy_MMS.DimReportingHierarchyKey AS DimReportingHierarchyKey,     
         FactAllocatedTransaction.transaction_amount AS SalesAmount,
		 FactAllocatedTransaction.transaction_post_dim_date_key AS DimDateKey,
		 FactAllocatedTransaction.tran_item_id,
		 CASE WHEN FactAllocatedTransaction.transaction_post_dim_date_key = @PromptEndDimDateKey
		       THEN FactAllocatedTransaction.transaction_amount
			   ELSE 0
			   END EndDateSalesAmount        
         FROM [marketing].[v_fact_mms_allocated_transaction_item] FactAllocatedTransaction
           JOIN [marketing].[v_dim_mms_product_history] DimMMSProduct
             ON FactAllocatedTransaction.dim_mms_product_key = DimMMSProduct.dim_mms_product_key
	        AND DimMMSProduct.effective_date_time <= @EndMonthEndingDate
	        AND DimMMSProduct.expiration_date_time > @EndMonthEndingDate
           JOIN #DimReportingHierarchy DimReportingHierarchy_MMS
             ON DimMMSProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey 
           JOIN #DimLocationInfo   DimLocation
             ON FactAllocatedTransaction.dim_club_key = DimLocation.DimClubKey

          WHERE FactAllocatedTransaction.transaction_post_dim_date_key >= @StartDimDateKey
            AND FactAllocatedTransaction.transaction_post_dim_date_key <= @EndDimDateKey
		    
         GROUP BY FactAllocatedTransaction.dim_club_key,      ------- Name Change
 		    DimReportingHierarchy_MMS.DepartmentName, 
            DimReportingHierarchy_MMS.DimReportingHierarchyKey,     
            FactAllocatedTransaction.transaction_amount,
		    FactAllocatedTransaction.transaction_post_dim_date_key,
		    FactAllocatedTransaction.tran_item_id,
		    CASE WHEN FactAllocatedTransaction.transaction_post_dim_date_key = @PromptEndDimDateKey
		       THEN FactAllocatedTransaction.transaction_amount
			   ELSE 0
			   END) NonAllocatedTranData
	GROUP BY DimClubKey,
             RevenueReportingDepartmentName,
			 DimReportingHierarchyKey,
		     DimDateKey

UNION ALL

   SELECT 
         DimLocation.DimClubKey,      ------- Name Change
 		 CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.DepartmentName 
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName, 
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DimReportingHierarchyKey	
			   END  DimReportingHierarchyKey,       
          FactAllocatedTransaction.transaction_amount AS SalesAmount,
		  FactAllocatedTransaction.transaction_dim_date_key AS DimDateKey,
		  CASE WHEN FactAllocatedTransaction.transaction_dim_date_key = @PromptEndDimDateKey
		       THEN FactAllocatedTransaction.transaction_amount
			   ELSE 0
			   END EndDateSalesAmount
      
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
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'N'   
   JOIN #DimLocationInfo   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey

   WHERE FactAllocatedTransaction.transaction_dim_date_key >= @StartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @EndDimDateKey
		  AND FactAllocatedTransaction.sales_source <> 'MMS'
		 

UNION ALL

   SELECT DimLocation.DimClubKey,      ------- Name Change
          CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName, 
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DimReportingHierarchyKey	
			   END  DimReportingHierarchyKey,        
          FactAllocatedTransaction.transaction_amount AS SalesAmount,
	      FactAllocatedTransaction.transaction_dim_date_key AS DimDateKey,
		  CASE WHEN FactAllocatedTransaction.transaction_dim_date_key = @PromptEndDimDateKey
		       THEN FactAllocatedTransaction.transaction_amount
			   ELSE 0
			   END EndDateSalesAmount
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
   JOIN #DimLocationInfo  DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndDimDateKey)
		  AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')
 

IF OBJECT_ID('tempdb.dbo.#SalesSummary', 'U') IS NOT NULL
  DROP TABLE #SalesSummary; 

SELECT DimClubKey,
       RevenueReportingDepartmentName,
       SUM(SalesAmount) SalesAmount,
       SUM(EndDateSalesAmount) EndDateSalesAmount
  INTO #SalesSummary          
  FROM #PromptSales Sales
    JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey
 GROUP BY DimClubKey, RevenueReportingDepartmentName
 
 -- Prior Year Sales

IF OBJECT_ID('tempdb.dbo.#PriorSales', 'U') IS NOT NULL
  DROP TABLE #PriorSales; 

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
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DimReportingHierarchyKey	
			   END  DimReportingHierarchyKey,       
          FactAllocatedTransaction.transaction_amount AS SalesAmount,
		  FactAllocatedTransaction.transaction_dim_date_key AS DimDateKey,
		  CASE WHEN FactAllocatedTransaction.transaction_dim_date_key = @PromptEndDimDateKey
		       THEN FactAllocatedTransaction.transaction_amount
			   ELSE 0
			   END EndDateSalesAmount
          
	INTO #PriorSales     
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
   JOIN #DimLocationInfo   DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey

   WHERE FactAllocatedTransaction.transaction_dim_date_key >= @PriorYearStartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @PriorYearEndDimDateKey

UNION ALL

   SELECT DimLocation.DimClubKey,      ------- Name Change
          CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName, 
		  CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DimReportingHierarchyKey	
			   END  DimReportingHierarchyKey,        
          FactAllocatedTransaction.transaction_amount AS SalesAmount,
	      FactAllocatedTransaction.transaction_dim_date_key AS DimDateKey,
		  CASE WHEN FactAllocatedTransaction.transaction_dim_date_key = @PromptEndDimDateKey
		       THEN FactAllocatedTransaction.transaction_amount
			   ELSE 0
			   END EndDateSalesAmount
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
   JOIN #DimLocationInfo  DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartDimDateKey   ---- using this date variable matches old code but doesn't make sense for pulling prior year data!
          AND FactAllocatedTransaction.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndDimDateKey)  ---- using this date variable matches old code but doesn't make sense for pulling prior year data!
		  AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')

 --- Club SSSG calcs
IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGClubRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGClubRevenueSummary; 

SELECT Sales.DimClubKey,
       SUM(Sales.SalesAmount) ActualAmount
  INTO #PriorYearSSSGClubRevenueSummary  
  FROM #PriorSales Sales       
    JOIN #DimLocationInfo  Clubs
	  ON Clubs.DimClubKey = Sales.DimClubKey
	  AND Clubs.ClubOpenDate <= @SSSGGrandOpeningDeadlineDate
  JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey
 GROUP BY Sales.DimClubKey

 

IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGClubRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGClubRevenueSummary; 

SELECT Sales.DimClubKey,
       Sum(Sales.SalesAmount) ActualAmount
  INTO #PromptYearSSSGClubRevenueSummary
  FROM #PromptSales Sales 
  JOIN #DimLocationInfo  Clubs
    ON Sales.DimClubKey = Clubs.DimClubKey
    AND Clubs.ClubOpenDate <= @SSSGGrandOpeningDeadlineDate
  JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey
 GROUP BY Sales.DimClubKey


 --Region SSSG calcs
 IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGRegionRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGRegionRevenueSummary; 

SELECT Clubs.RevenueReportingRegionName,
       Clubs.ClubStatusDescription,
       SUM(Sales.SalesAmount) ActualAmount
  INTO #PriorYearSSSGRegionRevenueSummary
  FROM #PriorSales Sales
  JOIN #DimLocationInfo  Clubs
    ON Sales.DimClubKey = Clubs.DimClubKey
   AND Clubs.ClubOpenDate <= @SSSGGrandOpeningDeadlineDate
  JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey
 GROUP BY Clubs.RevenueReportingRegionName,
          Clubs.ClubStatusDescription 

IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGRegionRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGRegionRevenueSummary; 

SELECT Clubs.RevenueReportingRegionName,
       Clubs.ClubStatusDescription,
       SUM(Sales.SalesAmount) ActualAmount
  INTO #PromptYearSSSGRegionRevenueSummary
  FROM #PromptSales Sales
  JOIN #DimLocationInfo  Clubs
    ON Sales.DimClubKey = Clubs.DimClubKey
   AND Clubs.ClubOpenDate <= @SSSGGrandOpeningDeadlineDate
  JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey
 GROUP BY Clubs.RevenueReportingRegionName,
          Clubs.ClubStatusDescription

--Status SSSG calcs
IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGStatusRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGStatusRevenueSummary; 

SELECT Clubs.ClubStatusDescription,
       SUM(Sales.SalesAmount) ActualAmount
  INTO #PriorYearSSSGStatusRevenueSummary  
  FROM #PriorSales Sales
  JOIN #DimLocationInfo  Clubs
    ON Sales.DimClubKey = Clubs.DimClubKey
   AND Clubs.ClubOpenDate <= @SSSGGrandOpeningDeadlineDate
  JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey
 GROUP BY Clubs.ClubStatusDescription 

 IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGStatusRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGStatusRevenueSummary; 

SELECT Clubs.ClubStatusDescription,
       SUM(Sales.SalesAmount) ActualAmount
  INTO #PromptYearSSSGStatusRevenueSummary
  FROM #PromptSales Sales
  JOIN #DimLocationInfo  Clubs
    ON Sales.DimClubKey = Clubs.DimClubKey
   AND Clubs.ClubOpenDate <= @SSSGGrandOpeningDeadlineDate
  JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey
 GROUP BY Clubs.ClubStatusDescription  

--Report SSSG calcs

 IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGReportRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PriorYearSSSGReportRevenueSummary; 

SELECT SUM(Sales.SalesAmount) ActualAmount
  INTO #PriorYearSSSGReportRevenueSummary
  FROM #PriorSales Sales
  JOIN #DimLocationInfo  Clubs
    ON Sales.DimClubKey = Clubs.DimClubKey
   AND Clubs.ClubOpenDate <= @SSSGGrandOpeningDeadlineDate
  JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey

 IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGReportRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGReportRevenueSummary; 

SELECT SUM(Sales.SalesAmount) ActualAmount
  INTO #PromptYearSSSGReportRevenueSummary
  FROM #PromptSales  Sales
  JOIN #DimLocationInfo  Clubs
    ON Sales.DimClubKey = Clubs.DimClubKey
   AND Clubs.ClubOpenDate <= @SSSGGrandOpeningDeadlineDate
  JOIN #DimReportingHierarchy Hier
	  ON Sales.DimReportingHierarchyKey = Hier.DimReportingHierarchyKey
  

--SSSG summary

 IF OBJECT_ID('tempdb.dbo.#SSSGSummary', 'U') IS NOT NULL
  DROP TABLE #SSSGSummary;

SELECT Clubs.DimClubKey,
       #PriorYearSSSGClubRevenueSummary.ActualAmount PriorYearClubActualAmount,
       #PromptYearSSSGClubRevenueSummary.ActualAmount PromptYearClubActualAmount,
       #PriorYearSSSGRegionRevenueSummary.ActualAmount PriorYearRegionActualAmount,
       #PromptYearSSSGRegionRevenueSummary.ActualAmount PromptYearRegionActualAmount,
       #PriorYearSSSGStatusRevenueSummary.ActualAmount PriorYearStatusActualAmount,
       #PromptYearSSSGStatusRevenueSummary.ActualAmount PromptYearStatusActualAmount,
       #PriorYearSSSGReportRevenueSummary.ActualAmount PriorYearReportActualAmount,
       #PromptYearSSSGReportRevenueSummary.ActualAmount PromptYearReportActualAmount 
  INTO #SSSGSummary  
  FROM #DimLocationInfo  Clubs
  LEFT JOIN #PriorYearSSSGClubRevenueSummary
    ON Clubs.DimClubKey = #PriorYearSSSGClubRevenueSummary.DimClubKey
  LEFT JOIN #PromptYearSSSGClubRevenueSummary 
    ON Clubs.DimClubKey = #PromptYearSSSGClubRevenueSummary.DimClubKey
  LEFT JOIN #PriorYearSSSGRegionRevenueSummary 
    ON Clubs.RevenueReportingRegionName = #PriorYearSSSGRegionRevenueSummary.RevenueReportingRegionName
   AND Clubs.ClubStatusDescription = #PriorYearSSSGRegionRevenueSummary.ClubStatusDescription
  LEFT JOIN #PromptYearSSSGRegionRevenueSummary 
    ON Clubs.RevenueReportingRegionName = #PromptYearSSSGRegionRevenueSummary.RevenueReportingRegionName
   AND Clubs.ClubStatusDescription = #PromptYearSSSGRegionRevenueSummary.ClubStatusDescription
  LEFT JOIN #PriorYearSSSGStatusRevenueSummary 
    ON Clubs.ClubStatusDescription = #PriorYearSSSGStatusRevenueSummary.ClubStatusDescription
  LEFT JOIN #PromptYearSSSGStatusRevenueSummary 
    ON Clubs.ClubStatusDescription = #PromptYearSSSGStatusRevenueSummary.ClubStatusDescription
  CROSS JOIN #PriorYearSSSGReportRevenueSummary
  CROSS JOIN #PromptYearSSSGReportRevenueSummary

 IF OBJECT_ID('tempdb.dbo.#GoalSummary', 'U') IS NOT NULL
  DROP TABLE #GoalSummary;

SELECT #DimReportingHierarchy.DepartmentName AS RevenueReportingDepartmentName,
       FactGoal.dim_club_key AS DimClubKey,
       Sum(FactGoal.goal_dollar_amount) GoalAmount   
  INTO #GoalSummary
  FROM [marketing].[v_fact_revenue_goal] FactGoal
  JOIN #DimReportingHierarchy
    ON FactGoal.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
 WHERE FactGoal.goal_effective_dim_date_key >= @StartMonthStartingDimDateKey
   AND FactGoal.goal_effective_dim_date_key <= @EndMonthStartingDimDateKey
 GROUP BY FactGoal.dim_club_key,
          #DimReportingHierarchy.DepartmentName

 IF OBJECT_ID('tempdb.dbo.#SalesAndGoalSummary', 'U') IS NOT NULL
  DROP TABLE #SalesAndGoalSummary;

SELECT ISNULL(#SalesSummary.RevenueReportingDepartmentName,#GoalSummary.RevenueReportingDepartmentName) RevenueReportingDepartmentName,
       ISNULL(#SalesSummary.DimClubKey,#GoalSummary.DimClubKey) DimClubKey,
       ISNULL(#SalesSummary.SalesAmount,0) SalesAmount,
       ISNULL(#GoalSummary.GoalAmount,0) GoalAmount,
       ISNULL(#SalesSummary.EndDateSalesAmount,0) EndDateSalesAmount
  INTO #SalesAndGoalSummary
  FROM #SalesSummary
  FULL OUTER JOIN #GoalSummary 
    ON #SalesSummary.DimClubKey = #GoalSummary.DimClubKey
   AND #SalesSummary.RevenueReportingDepartmentName = #GoalSummary.RevenueReportingDepartmentName
 
 
--Result set


SELECT Clubs.MMSClubID,
       Clubs.RevenueReportingRegionname AS ReportingRegion,
       #SalesAndGoalSummary.RevenueReportingDepartmentName,
       Clubs.ClubCode,
       Clubs.MMSClubName AS ClubName,
       Clubs.ClubStatusDescription AS ClubStatus,
       #SalesAndGoalSummary.SalesAmount AS SaleAmount,
       #SalesAndGoalSummary.GoalAmount,
       'Local Currency' AS CurrencyCode,
       2 SortOrder,
       @HeaderDateRange AS HeaderDateRange,
       @ReportRunDateTime AS ReportRunDateTime,
       NULL  AS RevenueReportintgDepartmentNameCommaList,     ----- must come from Cognos
       0 ClubPriorYearActual,
       0 RegionPriorYearActual,
       0 StatusPriorYearActual,
       0 ReportPriorYearActual,
       0 SSSGClubPromptYearActual,
       NULL AS HeaderDivisionList,      ----- must come from Cognos
       NULL AS HeaderSubdivisionList,       ----- must come from Cognos
       #SalesAndGoalSummary.EndDateSalesAmount AS EndDateActual,
       NULL AS DepartmentMinDimReportingHierarchyKeyList       ----- must come from Cognos
  FROM #SalesAndGoalSummary
  JOIN #DimLocationInfo Clubs 
    ON #SalesAndGoalSummary.DimClubKey = Clubs.DimClubKey
  LEFT JOIN #SSSGSummary 
    ON Clubs.DimClubKey = #SSSGSummary.DimClubKey

UNION

SELECT MIN(Clubs.MMSClubID) AS MMSClubID,
       MIN(Clubs.RevenueReportingRegionName) AS ReportingRegion,
       'Total' AS RevenueReportingDepartmentName,
       MIN(Clubs.ClubCode) AS ClubCode,
       MIN(Clubs.MMSClubName) AS ClubName,
       MIN(Clubs.ClubStatusDescription) AS ClubStatus,
       Sum(#SalesAndGoalSummary.SalesAmount) AS SaleAmount,
       Sum(#SalesAndGoalSummary.GoalAmount) AS GoalAmount,
       'Local Currency' AS CurrencyCode,
       1 AS SortOrder,
       @HeaderDateRange AS HeaderDateRange,
       @ReportRunDateTime  AS ReportRunDateTime,
       NULL AS RevenueReportingDepartmentNameCommaList,   ----- must come from Cognos
       MIN(#SSSGSummary.PriorYearClubActualAmount) AS ClubPriorYearActual,
       MIN(#SSSGSummary.PriorYearRegionActualAmount) AS RegionPriorYearActual,
       MIN(#SSSGSummary.PriorYearStatusActualAmount) AS StatusPriorYearActual,
       MIN(#SSSGSummary.PriorYearReportActualAmount) AS ReportPriorYearActual,
       MIN(#SSSGSummary.PromptYearClubActualAmount) AS SSSGClubPromptYearActual,
       NULL AS HeaderDivisionList,      ----- must come from Cognos
       NULL AS HeaderSubdivisionList,        ----- must come from Cognos
       SUM(#SalesAndGoalSummary.EndDateSalesAmount) AS EndDateActual,
       NULL AS DepartmentMinDimReportingHierarchyKeyList      ----- must come from Cognos
  FROM #SalesAndGoalSummary
  JOIN #DimLocationInfo  Clubs 
    ON #SalesAndGoalSummary.DimClubKey = Clubs.DimClubKey
  LEFT JOIN #SSSGSummary 
    ON Clubs.DimClubKey = #SSSGSummary.DimClubKey
 GROUP BY Clubs.DimClubKey
 ----ORDER BY ReportingRegion,ClubCode
 
DROP TABLE #Clubs
DROP TABLE #DimLocationInfo 
DROP TABLE #DimReportingHierarchy
DROP TABLE #RegionTypes
DROP TABLE #PromptSales
DROP TABLE #SalesSummary
DROP TABLE #PriorSales
DROP TABLE #PriorYearSSSGClubRevenueSummary
DROP TABLE #PromptYearSSSGClubRevenueSummary
DROP TABLE #PriorYearSSSGRegionRevenueSummary
DROP TABLE #PromptYearSSSGRegionRevenueSummary
DROP TABLE #PriorYearSSSGStatusRevenueSummary
DROP TABLE #PromptYearSSSGStatusRevenueSummary
DROP TABLE #PriorYearSSSGReportRevenueSummary
DROP TABLE #PromptYearSSSGReportRevenueSummary
DROP TABLE #SSSGSummary
DROP TABLE #GoalSummary
DROP TABLE #SalesAndGoalSummary 
      
END 
