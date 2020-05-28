CREATE PROC [reporting].[proc_UDW_FactPTDSSRRevenueAndServiceEmployeeSummary] AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
    END


------  NOTE: 
------  Daily data - append to a database summary table
------  Table will hold daily data for 13 months;  only EOM data for prior months



DECLARE @ReportDate  DateTime = '1/1/1900'
SET @ReportDate = CASE WHEN @ReportDate = '1/1/1900' THEN CONVERT(DATE, getdate()-1, 101) ELSE @ReportDate END

DECLARE @DivisionName VARCHAR(255)				
SET @DivisionName = 'Personal Training'	
		
				
DECLARE @StartDate AS DATETIME				
DECLARE @EndDimDateKey AS VARCHAR(32)								
DECLARE @FirstOfReportMonthDimDateKey VARCHAR(32)				
DECLARE @EndDate AS DATETIME	
DECLARE @ReportRunDate DATETIME				
DECLARE @ReportDateDimDateKey VARCHAR(32)
DECLARE @ReportFourDigitYearDashTwoDigitMonth VARCHAR(7)
DECLARE @ReportDateLastDayInMonthIndicator 	VARCHAR(1)
DECLARE @FirstOf13MonthsPriorDimDateKey VARCHAR(32)
DECLARE @FirstOf18MonthsPriorDate AS DATETIME
DECLARE @EndDatePlusOne AS DATETIME    ------- In UDW datetime values have time and we want to know the next date and make the limit less than this date
				
SET @StartDate = (SELECT month_starting_date FROM [marketing].[v_dim_date] Where calendar_date = @ReportDate)				
SET @EndDimDateKey = (Select month_ending_dim_date_key from [marketing].[v_dim_date] where calendar_date = @ReportDate)								
		
SET @FirstOfReportMonthDimDateKey = (Select dim_date_key from [marketing].[v_dim_date] where calendar_date = @StartDate)				
SET @EndDate = (SELECT calendar_date FROM [marketing].[v_dim_date] Where dim_date_key = @EndDimDateKey)
SET @ReportRunDate = (SELECT calendar_date from [marketing].[v_dim_date] Where calendar_date > GetDate()-1 and calendar_date < GetDate())
SET @ReportDateDimDateKey = (Select dim_date_key from [marketing].[v_dim_date] where calendar_date = @ReportDate) 
SET @ReportFourDigitYearDashTwoDigitMonth = (Select four_digit_year_dash_two_digit_month FROM [marketing].[v_dim_date] where dim_date_key = @ReportDateDimDateKey )
SET @ReportDateLastDayInMonthIndicator = (Select last_day_in_month_flag FROM [marketing].[v_dim_date] WHERE dim_date_key = @ReportDateDimDateKey)
SET @FirstOf13MonthsPriorDimDateKey = (Select dim_date_key from [marketing].[v_dim_date] where calendar_date = DATEADD(m,-13, @StartDate))
SET @FirstOf18MonthsPriorDate = DATEADD(m,-18,@StartDate)
SET @EndDatePlusOne =  DATEADD(Day,1,@EndDate)


IF OBJECT_ID('tempdb.dbo.#DimClubKeyList', 'U') IS NOT NULL
  DROP TABLE #DimClubKeyList;  

---- Create temp table of the Active locations - returning only the required columns

SELECT DISTINCT DimClub.dim_club_key AS DimClubKey,   ------- Name change
                DimClub.club_id AS MMSClubID,
                DimClub.club_name AS ClubName,
                DimClub.local_currency_code AS LocalCurrencyCode,
                DimClub.club_code AS ClubCode,
				DimClub.club_close_dim_date_key AS ClubCloseDimDateKey,
                CASE WHEN DimClub.club_status = 'Presale'
				     THEN 'Y'
					 ELSE 'N'
					 END   PreSaleFlag,
				DimDescription.description AS RegionName,
				1 AS JoinKey
  INTO #DimClubKeyList    ------- Name change     
  FROM [marketing].[v_dim_club] DimClub
   JOIN [marketing].[v_dim_description] DimDescription
     ON DimClub.pt_rcl_area_dim_description_key = DimDescription.dim_description_key

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy;  

---- Create Temp table of active hierarchy keys
SELECT Distinct dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
       reporting_department AS DepartmentName,
       reporting_product_group AS ProductGroupName,
	   CASE WHEN reporting_product_group IN('Weight Loss Challenges','90 Day Weight Loss')
	        THEN 'Y'
		    ELSE 'N'
			END PTDeferredRevenueProductGroupFlag
  INTO #DimReportingHierarchy  
  FROM [marketing].[v_dim_reporting_hierarchy_history]
  WHERE reporting_division = @DivisionName
  AND effective_dim_date_key <= @EndDimDateKey
  AND expiration_dim_date_key > @EndDimDateKey

 
IF OBJECT_ID('tempdb.dbo.#PTDSSRCategories_Prelim', 'U') IS NOT NULL
  DROP TABLE #PTDSSRCategories_Prelim;  

---- To Pull in Categories and report row labels for the PT DSSR report
  SELECT dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
         reporting_division AS DivisionName,
         reporting_sub_division AS SubdivisionName,
         reporting_department AS DepartmentName,
         reporting_product_group AS ProductGroupName,
         PTDSSRCategory,
         CategoryDisplayOrder,
         PTDSSRRowLabel,
         effective_dim_date_key AS EffectiveDimDateKey,
         expiration_dim_date_key AS ExpirationDimDateKey,
         1 AS ClubJoinKey
	 INTO #PTDSSRCategories_Prelim   
	FROM [reporting].[v_PTDSSR_MoveIt_KnowIt_NourishIt] 
	 WHERE effective_dim_date_key <= @EndDimDateKey
	   AND expiration_dim_date_key > @EndDimDateKey
	   AND reporting_division = @DivisionName
	   AND ActiveFlag = 'Y'


 ----- need to make sure that every club is mapped to every PT hierarchy key
   --- This eliminates revenue being dropped for a club if the club has no goal set in the db
IF OBJECT_ID('tempdb.dbo.#PTDSSRCategories', 'U') IS NOT NULL
  DROP TABLE #PTDSSRCategories;

SELECT Club.DimClubKey,
       Club.MMSClubID,
       Categories.DimReportingHierarchyKey,
       Categories.DivisionName,
       Categories.SubdivisionName,
       Categories.DepartmentName,
       Categories.ProductGroupName,
       Categories.PTDSSRCategory,
       Categories.CategoryDisplayOrder,
       Categories.PTDSSRRowLabel,
       Categories.EffectiveDimDateKey,
       Categories.ExpirationDimDateKey
  INTO #PTDSSRCategories
FROM #DimClubKeyList Club
 JOIN #PTDSSRCategories_Prelim Categories
   ON Club.JoinKey = Categories.ClubJoinKey



IF OBJECT_ID('tempdb.dbo.#Goals_ClubHierarchyKey', 'U') IS NOT NULL
  DROP TABLE #Goals_ClubHierarchyKey;  

  --- to collect goal by club-HierarchyKey
  --- Setting goal to $0 for hierarchy key if there is no goal in the db

SELECT FactGoal.goal_effective_dim_date_key AS GoalEffectiveDimDateKey,
       PTDSSRCategories.DimReportingHierarchyKey AS DimReportingHierarchyKey,
       Sum(IsNull(FactGoal.goal_dollar_amount,0)) AS GoalDollarAmount,  ---- If there is no goal, set the product group goal to $0
	   PTDSSRCategories.DimClubKey,     -------- Name Change
	   PTDSSRCategories.MMSClubID AS ClubID,
	   PTDSSRCategories.PTDSSRCategory,
	   PTDSSRCategories.PTDSSRRowLabel
INTO #Goals_ClubHierarchyKey     
FROM #PTDSSRCategories PTDSSRCategories
 LEFT JOIN [marketing].[v_fact_revenue_goal] FactGoal
   ON FactGoal.dim_reporting_hierarchy_key = PTDSSRCategories.DimReportingHierarchyKey
   AND FactGoal.dim_club_key = PTDSSRCategories.DimClubKey
   AND FactGoal.goal_effective_dim_date_key = CAST(@FirstOfReportMonthDimDateKey AS Varchar(8))

GROUP BY FactGoal.goal_effective_dim_date_key,
       PTDSSRCategories.DimReportingHierarchyKey,
	   PTDSSRCategories.DimClubKey,
	   PTDSSRCategories.MMSClubID,
	   PTDSSRCategories.PTDSSRCategory,
	   PTDSSRCategories.PTDSSRRowLabel



  ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month

DECLARE @StartDateMonthStartDimDateKey VARCHAR(32)
DECLARE @EndDateMonthStartDimDateKey VARCHAR(32)
DECLARE @StartDateCalendarMonthNumberInYear INT
DECLARE @EndDateCalendarMonthNumberInYear INT
DECLARE @EndDatePriorMonthEndDateDimDateKey VARCHAR(32)

SET @StartDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @FirstOfReportMonthDimDateKey) 
SET @EndDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey) 
SET @StartDateCalendarMonthNumberInYear = (SELECT month_number_in_year  FROM [marketing].[v_dim_date] WHERE dim_date_key = @FirstOfReportMonthDimDateKey)
SET @EndDateCalendarMonthNumberInYear = (SELECT month_number_in_year  FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)
SET @EndDatePriorMonthEndDateDimDateKey = (SELECT next_month_ending_dim_date_key  FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)


DECLARE @EComm60DayChallengeRevenueStartDimDateKey VARCHAR(32)
  ---- When the start date is the 1st of the 2nd month of the quarter, set the start date to the 1st of the prior month
SET @EComm60DayChallengeRevenueStartDimDateKey = (SELECT CASE WHEN (@FirstOfReportMonthDimDateKey = @StartDateMonthStartDimDateKey)          ---- Date range begins on the 1st of a month
															  THEN (CASE WHEN @StartDateCalendarMonthNumberInYear in(2,5,8,11)
																		 THEN (Select prior_month_starting_dim_date_key
                                                                                 FROM [marketing].[v_dim_date] 
                                                                                WHERE dim_date_key = @FirstOfReportMonthDimDateKey)
																	      WHEN @StartDateCalendarMonthNumberInYear in(1,4,7,10)
																		  THEN (Select month_starting_dim_date_key
                                                                                  FROM [marketing].[v_dim_date]
                                                                                 WHERE dim_date_key = @FirstOfReportMonthDimDateKey) 
																		  ELSE @FirstOfReportMonthDimDateKey
																				   END)
												
															  ELSE  @FirstOfReportMonthDimDateKey END
												  FROM [marketing].[v_dim_date] 
												  WHERE dim_date_key = @FirstOfReportMonthDimDateKey ) ---- to limit result set to one record)

DECLARE @EComm60DayChallengeRevenueEndDimDateKey VARCHAR(32)
  ---- When the End Date is in the 1st month of the quarter, set the end date to the end of the prior month
SET @EComm60DayChallengeRevenueEndDimDateKey = (SELECT CASE WHEN @EndDateCalendarMonthNumberInYear in(1,4,7,10)
                                                            THEN @EndDatePriorMonthEndDateDimDateKey 
															ELSE @EndDimDateKey
															END
												FROM [marketing].[v_dim_date]
												WHERE dim_date_key = @EndDimDateKey)  ---- to limit result set to one record




   ----------
   --- Sales (Revenue) Data   ----- still allocating 60 day challenge E-Comm sales until further notice
   ----------

IF OBJECT_ID('tempdb.dbo.#Results_ClubPOS', 'U') IS NOT NULL
  DROP TABLE #Results_ClubPOS;

   SELECT FactMMSAllocatedRevenue.dim_mms_member_key AS DimMemberKey,   ----- Name change
       FactMMSAllocatedRevenue.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,
	   DimEmployeeActive.employee_id AS EmployeeID,
	   DimEmployeeActive.first_name AS SalesEmployeeFirstName,
	   DimEmployeeActive.last_name AS SalesEmployeeLastName,						
	   DimClub.RegionName,   		
	   DimClub.ClubName,			
	   DimClub.MMSClubID,			
	   DimClub.ClubCode,
	   DimProduct.product_id AS MMSProductID,
	   DimProduct.product_description AS ProductName,
	   TransactionDimDate.calendar_date AS SaleDate,		
	   'Local Currency' AS HeaderReportingCurrency,						
	   DimReportingHierarchy.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,	
	   FactMMSAllocatedRevenue.transaction_amount AS MTD_ItemAmount,	
	   CASE WHEN @ReportDateDimDateKey = TransactionDimDate.dim_date_key	
	        THEN FactMMSAllocatedRevenue.transaction_amount 
			ELSE 0
			END ReportDate_ItemAmount,
	   NULL AS PayPeriodToDate_ItemAmount,
	   'Non-1on1 Product' AS  OneOnOneProductGrouping    ------- Obsolete business logic

INTO #Results_ClubPOS			
FROM [marketing].[v_fact_mms_allocated_transaction_item] AS FactMMSAllocatedRevenue				
JOIN [marketing].[v_dim_date] TransactionDimDate				
  ON FactMMSAllocatedRevenue.transaction_post_dim_date_key = TransactionDimDate.dim_date_key	
JOIN [marketing].[v_dim_date] RevenueDimDate				
  ON FactMMSAllocatedRevenue.allocated_month_starting_dim_date_key = RevenueDimDate.dim_date_key	
JOIN [marketing].[v_dim_employee] DimEmployeeActive
  ON FactMMSAllocatedRevenue.primary_sales_dim_employee_key = DimEmployeeActive.dim_employee_key			
JOIN [marketing].[v_dim_mms_product_history] DimProduct				
  ON FactMMSAllocatedRevenue.dim_mms_product_key = DimProduct.dim_mms_product_key
  AND DimProduct.effective_date_time < @EndDatePlusOne
  AND DimProduct.expiration_date_time >= @EndDatePlusOne			
JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy				
  ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
  AND  DimReportingHierarchy.effective_dim_date_key <= @EndDimDateKey
  AND DimReportingHierarchy.expiration_dim_date_key	> @EndDimDateKey
JOIN #DimReportingHierarchy Hier				
  ON DimReportingHierarchy.dim_reporting_hierarchy_key = Hier.DimReportingHierarchyKey								
JOIN #DimClubKeyList DimClub				
  ON FactMMSAllocatedRevenue.dim_club_key = DimClub.DimClubKey			
WHERE RevenueDimDate.dim_date_key = @FirstOfReportMonthDimDateKey				
  AND TransactionDimDate.dim_date_key <= @EndDimDateKey				
  AND FactMMSAllocatedRevenue.transaction_amount <> 0  				

	   
IF OBJECT_ID('tempdb.dbo.#ECommerceData', 'U') IS NOT NULL
  DROP TABLE #ECommerceData;
  	   
  SELECT 				
       DimClub.RegionName,				
       DimClub.ClubCode,				
       DimClub.ClubName,				
       DimClub.MMSClubID,				
       DimEmployeeActive.employee_id AS EmployeeID,				
       DimEmployeeActive.first_name AS  SalesEmployeeFirstName,				
       DimEmployeeActive.last_name AS  SalesEmployeeLastName,
	   FactECommerceRevenue.dim_mms_member_key AS DimMemberKey,
	   CASE WHEN FactECommerceRevenue.sales_source = 'Hybris'
	        THEN DimReportingHierarchy_Hybris.DimReportingHierarchyKey
			WHEN FactECommerceRevenue.sales_source = 'HealthCheckUSA'
			THEN DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
			WHEN FactECommerceRevenue.sales_source = 'Magento'
			THEN DimReportingHierarchy_Magento.DimReportingHierarchyKey
			END DimReportingHierarchyKey,						
       IsNull(FactECommerceRevenue.allocated_amount,0) MTD_ItemAmount, 
	   CASE WHEN @ReportDateDimDateKey = TransactionPostDimDate.dim_date_key	
	        THEN IsNull(FactECommerceRevenue.allocated_amount,0)  
			ELSE 0
			END ReportDate_ItemAmount,
	   NULL AS PayPeriodToDate_ItemAmount,	
	   CASE WHEN FactECommerceRevenue.sales_source = 'Hybris'
	        THEN DimHybrisProduct.code
			WHEN FactECommerceRevenue.sales_source = 'HealthCheckUSA'
			THEN DimHealthCheckUSAProduct.product_sku
			WHEN FactECommerceRevenue.sales_source = 'Magento'
			THEN DimMagentoProduct.sku
			END SKU,
	   CASE WHEN FactECommerceRevenue.sales_source = 'Hybris'
	        THEN DimHybrisProduct.name
			WHEN FactECommerceRevenue.sales_source = 'HealthCheckUSA'
			THEN DimHealthCheckUSAProduct.product_description
			WHEN FactECommerceRevenue.sales_source = 'Magento'
			THEN DimMagentoProduct.product_name
			END ProductName,															
       TransactionPostDimDate.calendar_date AS SaleDate,
	   'On Line' AS ReportingDataSource
INTO #ECommerceData			
  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactECommerceRevenue				
  JOIN [marketing].[v_dim_date] TransactionPostDimDate				
    ON FactECommerceRevenue.transaction_dim_date_key = TransactionPostDimDate.dim_date_key	
  LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactECommerceRevenue.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactECommerceRevenue.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time < @EndDatePlusOne
	   AND DimHybrisProduct.expiration_date_time >= @EndDatePlusOne
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactECommerceRevenue.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactECommerceRevenue.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time < @EndDatePlusOne
	   AND DimHealthCheckUSAProduct.expiration_date_time >= @EndDatePlusOne
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactECommerceRevenue.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactECommerceRevenue.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time < @EndDatePlusOne
	   AND DimMagentoProduct.expiration_date_time >= @EndDatePlusOne
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'N'  
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'N'  
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'N' 
   LEFT JOIN [marketing].[v_dim_employee] DimEmployeeActive
      ON FactECommerceRevenue.primary_sales_dim_employee_key = DimEmployeeActive.dim_employee_key		
						
  JOIN #DimClubKeyList DimClub			
    ON FactECommerceRevenue.allocated_dim_club_key = DimClub.DimClubKey				
								
 WHERE FactECommerceRevenue.transaction_dim_date_key >= @FirstOfReportMonthDimDateKey				
   AND FactECommerceRevenue.transaction_dim_date_key <= @EndDimDateKey
   AND FactECommerceRevenue.sales_source in ('Hybris','HealthCheckUSA','Magento')

UNION ALL



     SELECT 				
       DimClub.RegionName,				
       DimClub.ClubCode,				
       DimClub.ClubName,				
       DimClub.MMSClubID,				
       DimEmployee.employee_id AS EmployeeID,				
       DimEmployee.first_name AS SalesEmployeeFirstName,				
       DimEmployee.last_name AS SalesEmployeeLastName,
	   FactECommerceRevenue.dim_mms_member_key  AS DimMemberKey,
	   CASE WHEN FactECommerceRevenue.sales_source = 'Hybris'
	        THEN DimReportingHierarchy_Hybris.DimReportingHierarchyKey
			WHEN FactECommerceRevenue.sales_source = 'HealthCheckUSA'
			THEN DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
			WHEN FactECommerceRevenue.sales_source = 'Magento'
			THEN DimReportingHierarchy_Magento.DimReportingHierarchyKey
			END DimReportingHierarchyKey,						
       IsNull(FactECommerceRevenue.allocated_amount,0) MTD_ItemAmount, 
	   CASE WHEN @ReportDateDimDateKey = TransactionPostDimDate.dim_date_key	
	        THEN IsNull(FactECommerceRevenue.allocated_amount,0)  
			ELSE 0
			END ReportDate_ItemAmount,
	   NULL AS PayPeriodToDate_ItemAmount,
	   CASE WHEN FactECommerceRevenue.sales_source = 'Hybris'
	        THEN DimHybrisProduct.code
			WHEN FactECommerceRevenue.sales_source = 'HealthCheckUSA'
			THEN DimHealthCheckUSAProduct.product_sku
			WHEN FactECommerceRevenue.sales_source = 'Magento'
			THEN DimMagentoProduct.sku
			END SKU,							
 	   CASE WHEN FactECommerceRevenue.sales_source = 'Hybris'
	        THEN DimHybrisProduct.name
			WHEN FactECommerceRevenue.sales_source = 'HealthCheckUSA'
			THEN DimHealthCheckUSAProduct.product_description
			WHEN FactECommerceRevenue.sales_source = 'Magento'
			THEN DimMagentoProduct.product_name
			END ProductName,							
       TransactionPostDimDate.calendar_date AS SaleDate,
	   'On Line' AS ReportingDataSource
		
  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactECommerceRevenue				
  JOIN [marketing].[v_dim_date]  TransactionPostDimDate				
    ON FactECommerceRevenue.transaction_dim_date_key = TransactionPostDimDate.dim_date_key				
LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactECommerceRevenue.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactECommerceRevenue.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time < @EndDatePlusOne
	   AND DimHybrisProduct.expiration_date_time >= @EndDatePlusOne
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactECommerceRevenue.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactECommerceRevenue.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time < @EndDatePlusOne
	   AND DimHealthCheckUSAProduct.expiration_date_time >= @EndDatePlusOne
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactECommerceRevenue.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactECommerceRevenue.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time < @EndDatePlusOne
	   AND DimMagentoProduct.expiration_date_time >= @EndDatePlusOne	
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	   AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'Y'  
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	   AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'Y'  
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	   AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'Y'
   LEFT JOIN [marketing].[v_dim_employee] DimEmployee
      ON FactECommerceRevenue.primary_sales_dim_employee_key = DimEmployee.dim_employee_key				
   JOIN #DimClubKeyList DimClub			
      ON FactECommerceRevenue.allocated_dim_club_key = DimClub.DimClubKey	
								
 WHERE FactECommerceRevenue.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartDimDateKey				
   AND FactECommerceRevenue.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndDimDateKey

				
IF OBJECT_ID('tempdb.dbo.#CafeData', 'U') IS NOT NULL
  DROP TABLE #CafeData;	
	
	SELECT				
       DimClub.RegionName,				
       DimClub.ClubCode,				
       DimClub.ClubName,				
       DimClub.MMSClubID,				
       DimEmployee.employee_id AS EmployeeID,				
       DimEmployee.first_name AS SalesEmployeeFirstName,				
       DimEmployee.last_name AS SalesEmployeeLastName,	
	   '-998' as DimMemberKey,
	   Hier.DimReportingHierarchyKey,						
	   FactCafePOSRevenue.allocated_amount AS MTD_ItemAmount,
	   CASE WHEN @ReportDateDimDateKey = TransactionCloseDimDate.dim_date_key	
	        THEN FactCafePOSRevenue.allocated_amount
			ELSE 0
			END ReportDate_ItemAmount,
	   NULL AS PayPeriodToDate_ItemAmount,						
       CAST(DimCafeProduct.menu_item_id as Varchar(50)) SKU,				
       DimCafeProduct.menu_item_name AS ProductName,								
       TransactionCloseDimDate.calendar_date AS SaleDate,
	   'In Club' AS ReportingDataSource
INTO #CafeData			
  FROM [marketing].[v_fact_cafe_allocated_transaction_item] FactCafePOSRevenue				
  JOIN [marketing].[v_dim_date]  TransactionCloseDimDate				
    ON FactCafePOSRevenue.transaction_close_dim_date_key = TransactionCloseDimDate.dim_date_key				
  JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct				
    ON FactCafePOSRevenue.dim_cafe_product_key = DimCafeProduct.dim_cafe_product_key
	  AND DimCafeProduct.effective_date_time < @EndDatePlusOne
	  AND DimCafeProduct.expiration_date_time >= @EndDatePlusOne		
  LEFT JOIN [marketing].[v_dim_employee] DimEmployee				
    ON FactCafePOSRevenue.commissioned_sales_dim_employee_key = DimEmployee.dim_employee_key						
  JOIN #DimClubKeyList DimClub				
    ON FactCafePOSRevenue.dim_club_key = DimClub.DimClubKey				
  JOIN #DimReportingHierarchy Hier				
  ON DimCafeProduct.dim_reporting_hierarchy_key = Hier.DimReportingHierarchyKey								
 WHERE FactCafePOSRevenue.transaction_close_dim_date_key >= @FirstOfReportMonthDimDateKey				
   AND FactCafePOSRevenue.transaction_close_dim_date_key <= @EndDimDateKey				
   AND DimCafeProduct.reporting_department in('Devices','PT Nutritionals')	


IF OBJECT_ID('tempdb.dbo.#Results_Summary', 'U') IS NOT NULL
  DROP TABLE #Results_Summary;	

   SELECT
	   #ECommerceData.RegionName,
	   #ECommerceData.ClubName,
	   #ECommerceData.ClubCode,
	   #ECommerceData.MMSClubID, 
	   #ECommerceData.DimMemberKey,
	   IsNull(#ECommerceData.EmployeeID,-998) AS PrimarySalesEmployeeID,
	   IsNull(#ECommerceData.SalesEmployeeFirstName,'No Data') SalesEmployeeFirstName,
	   IsNull(#ECommerceData.SalesEmployeeLastName,'No Data') SalesEmployeeLastName,
	   #ECommerceData.DimReportingHierarchyKey,
       CONVERT(VARCHAR,#ECommerceData.SKU) AS SKU_ProductID,
	   #ECommerceData.ProductName,
	   #ECommerceData.SaleDate,
	   #ECommerceData.ReportDate_ItemAmount,
	   #ECommerceData.PayPeriodToDate_ItemAmount,
	   #ECommerceData.MTD_ItemAmount,
	   #ECommerceData.ReportingDataSource,
       'Non-1on1 Product' AS OneOnOneProductGrouping
	 INTO #Results_Summary		
	   FROM #ECommerceData
	   JOIN #DimReportingHierarchy   ------ joining this to remove null values which return in #ECommerceData due to left joins to source specific hierarchy temp table
	     ON #ECommerceData.DimReportingHierarchyKey = #DimReportingHierarchy.DimReportingHierarchyKey

	   UNION ALL 
	       
	SELECT
	   RegionName,
	   ClubName,
	   ClubCode,
	   MMSClubID, 
	   DimMemberKey,
	   IsNull(EmployeeID,-998) AS PrimarySalesEmployeeID,
	   IsNull(SalesEmployeeFirstName,'No Data') SalesEmployeeFirstName,
	   IsNull(SalesEmployeeLastName,'No Data') SalesEmployeeLastName,
	   DimReportingHierarchyKey,
       CONVERT(VARCHAR,SKU) AS SKU_ProductID,
	   ProductName,
	   SaleDate,
	   ReportDate_ItemAmount,
	   PayPeriodToDate_ItemAmount,
	   MTD_ItemAmount,
	   ReportingDataSource,
	   'Non-1on1 Product' AS OneOnOneProductGrouping		
	   FROM #CafeData


	 UNION  ALL    

  SELECT
	   RegionName,
	   ClubName,
	   ClubCode,
	   MMSClubID, 
	   DimMemberKey,
	   IsNull(EmployeeID,-998) as PrimarySalesEmployeeID,
	   IsNull(SalesEmployeeFirstName,'No Data') SalesEmployeeFirstName,
	   IsNull(SalesEmployeeLastName,'No Data') SalesEmployeeLastName,
	   DimReportingHierarchyKey,
       CONVERT(VARCHAR,MMSProductID) as SKU_ProductID,
	   ProductName,
	   SaleDate,
	   ReportDate_ItemAmount,
	   PayPeriodToDate_ItemAmount,
	   MTD_ItemAmount,
	   'In Club' AS ReportingDataSource,
	   OneOnOneProductGrouping		
	 FROM #Results_ClubPOS


IF OBJECT_ID('tempdb.dbo.#Revenue_Summary', 'U') IS NOT NULL
  DROP TABLE #Revenue_Summary;

SELECT Summary.MMSClubID,
       DimClub.DimClubKey,
       Summary.PrimarySalesEmployeeID,
	   Summary.DimReportingHierarchyKey,
	   Sum(Summary.ReportDate_ItemAmount) AS ReportDate_ItemAmount,
	   NULL AS PayPeriodToDate_ItemAmount,
	   Sum(Summary.MTD_ItemAmount) AS MTD_ItemAmount,
	   Summary.ReportingDataSource,
	   Summary.OneOnOneProductGrouping
INTO #Revenue_Summary     
FROM #Results_Summary  Summary  
 JOIN #DimClubKeyList DimClub
   ON Summary.MMSClubID = DimClub.MMSClubID
GROUP BY Summary.MMSClubID, 
         DimClub.DimClubKey,   
         Summary.PrimarySalesEmployeeID,
	     Summary.DimReportingHierarchyKey,
		 Summary.ReportingDataSource,
		 Summary.OneOnOneProductGrouping



   ------------
   ---- Forecasted Data
   ------------


 
DECLARE @BeginDimDateKey VARCHAR(32)
DECLARE @EndDateDescription VARCHAR(21)
DECLARE @MonthEndingDate DATETIME
DECLARE @CalendarYear INT


SET @BeginDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportDate)
SET @CalendarYear = (SELECT year FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportDate)
SET @EndDateDescription = (SELECT standard_date_name FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)
SET @MonthEndingDate = (SELECT month_ending_date FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)


 IF OBJECT_ID('tempdb.dbo.#ProjectionUnassessedDates', 'U') IS NOT NULL
  DROP TABLE #ProjectionUnassessedDates;

 SELECT dim_date_key AS DimDateKey,
        day_number_in_month AS DayNumberInCalendarMonth,
        calendar_date AS CalendarDate,
        year AS CalendarYear,
		@ReportDateDimDateKey  ReportDateDimDateKey 
  INTO #ProjectionUnassessedDates  
  FROM [marketing].[v_dim_date]
  WHERE four_digit_year_dash_two_digit_month = @ReportFourDigitYearDashTwoDigitMonth
   AND dim_date_key > @ReportDateDimDateKey

DECLARE @MinAssessmentDateDimDateKey INT
SET @MinAssessmentDateDimDateKey = (SELECT MIN(DimDateKey) from #ProjectionUnassessedDates)


 -------
 ------- Exerp specific code logic
 -------
 IF OBJECT_ID('tempdb.dbo.#NextAssessmentDate_Prelim', 'U') IS NOT NULL
DROP TABLE #NextAssessmentDate_Prelim;

----- To Determine next assessment date on subscriptions based on subscription periods
-----  Assumption - payment is 1 month in advance of subscriptionPeriodTo
----- This will eliminate subscriptions which have already ended in the prior month

SELECT sp.dim_exerp_subscription_key,
       Subscription.start_dim_date_key,
	   Subscription.end_dim_date_key,
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
  
Where ToDimDate.calendar_date >= DateAdd(day,-1,@StartDate)   ----- to return all subscriptions that are set to assess in the current month and beyond
  AND (Subscription.end_dim_date_key = '-998' 
     OR Subscription.end_dim_date_key >= @MinAssessmentDateDimDateKey) ----not terminated or terminated after earliest projected assessment date
GROUP BY sp.dim_exerp_subscription_key,Subscription.start_dim_date_key,Subscription.end_dim_date_key


 ---- to limit the temp table to just the DOM selected for the report
 IF OBJECT_ID('tempdb.dbo.#NextAssessmentDate', 'U') IS NOT NULL
DROP TABLE #NextAssessmentDate;

Select NextAssessment.dim_exerp_subscription_key,
       NextAssessment.start_dim_date_key,
	   NextAssessment.end_dim_date_key,
       NextAssessment.from_dim_date_key,
       NextAssessment.to_dim_date_key,
	   NextAssessment.NextAssessment,
	   NextAssessment.AssessmentDayOfMonth,
	   NextAssessmentDate.dim_date_key
INTO #NextAssessmentDate      
FROM #NextAssessmentDate_Prelim NextAssessment
   JOIN #ProjectionUnassessedDates  Dates
    ON NextAssessment.AssessmentDayOfMonth = Dates.DayNumberInCalendarMonth 
   JOIN [marketing].[v_dim_date] NextAssessmentDate
    ON NextAssessment.NextAssessment = NextAssessmentDate.calendar_date
WHERE (NextAssessment.end_dim_date_key = '-998' 
        OR NextAssessment.end_dim_date_key > NextAssessmentDate.dim_date_key) ----not terminated or terminated after the calculated next assessment date


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


--- Get the Commissionable Employee from the latest Subscription_change_log record
  IF OBJECT_ID('tempdb.dbo.#LatestCommissionableEmployee', 'U') IS NOT NULL
DROP TABLE #LatestCommissionableEmployee;

 SELECT ChangeLog.dim_exerp_subscription_key,
	Employee.dim_employee_key,
	Employee.employee_id,
	Employee.last_name,
	Employee.first_name,
	Employee.middle_name,
	CASE WHEN (ISNULL(ChangeLog.dim_employee_key,'-998') <> '-998' ) 
	        THEN Employee.last_name +', '+ Employee.first_name 
			ELSE 'None Designated' 
			END CommisionedEmployee 
 INTO #LatestCommissionableEmployee    
 FROM [marketing].[v_dim_exerp_subscription_change_log] ChangeLog
 JOIN #LastChangeLog last_ChangeLog 
   ON ChangeLog.subscription_change_log_id = last_ChangeLog.subscription_change_log_id
 JOIN [marketing].[v_dim_employee] Employee 
   ON ChangeLog.dim_employee_key = Employee.dim_employee_key

IF OBJECT_ID('tempdb.dbo.#ProjectionSummary', 'U') IS NOT NULL
  DROP TABLE #ProjectionSummary; 

Select DimClub.DimClubKey,     ---- new name
	   DimClub.MMSClubID AS MMSClubID,
	   DimReportingHierarchy.DimReportingHierarchyKey,   
	   CASE WHEN (RecurrentProductSubscription.end_dim_date_key = '-998' 
	               or RecurrentProductSubscription.end_dim_date_key >= Dates.DimDateKey)
	          AND (Freeze.start_dim_date_key is Null 
			         OR Dates.DimDateKey < Freeze.start_dim_date_key 
					 OR Dates.DimDateKey > Freeze.end_dim_date_key )
			THEN  RecurrentProductSubscription.Price
			ELSE 0
			END LocalCurrency_ForecastedAmount,    
	   CommissionEmployee.employee_id AS CommissionEmployeeID,      
	   'Non-1on1 Product' AS OneOnOneProductGrouping    
INTO #ProjectionSummary
FROM [marketing].[v_dim_exerp_subscription] RecurrentProductSubscription
JOIN #NextAssessmentDate  NextAssessment
  ON RecurrentProductSubscription.dim_exerp_subscription_key = NextAssessment.dim_exerp_subscription_key
LEFT JOIN #LatestCommissionableEmployee CommissionEmployee 
  on NextAssessment.dim_exerp_subscription_key = CommissionEmployee.dim_exerp_subscription_key
JOIN #ProjectionUnassessedDates Dates 
  ON NextAssessment.dim_date_key = Dates.DimDateKey
JOIN #DimClubKeyList DimClub
  ON RecurrentProductSubscription.dim_club_key = DimClub.DimClubKey 
JOIN [marketing].[v_dim_exerp_product] DimExerpProduct
  ON RecurrentProductSubscription.dim_exerp_product_key = DimExerpProduct.dim_exerp_product_key
JOIN [marketing].[v_dim_mms_product] DimMMSProduct
  ON DimExerpProduct.dim_mms_product_key = DimMMSProduct.dim_mms_product_key
JOIN #DimReportingHierarchy DimReportingHierarchy
    ON DimReportingHierarchy.DimReportingHierarchyKey = DimMMSProduct.dim_reporting_hierarchy_key
JOIN [marketing].[v_dim_mms_member] Member
  ON RecurrentProductSubscription.dim_mms_member_key = Member.dim_mms_member_key
JOIN [marketing].[v_dim_mms_membership] Membership                                 
  ON Member.dim_mms_membership_key = Membership.dim_mms_membership_key
LEFT JOIN [marketing].[v_dim_exerp_freeze_period] Freeze
  ON RecurrentProductSubscription.dim_exerp_subscription_key = Freeze.dim_exerp_subscription_key
  AND Freeze.cancel_dim_date_key in('-997','-998','-999')
Where RecurrentProductSubscription.Price > 0       ---- price is greater than $0
AND (RecurrentProductSubscription.end_dim_date_key = '-998' 
     or RecurrentProductSubscription.end_dim_date_key >= @EndDimDateKey)   ----not terminated or terminated in future month
AND RecurrentProductSubscription.start_dim_date_key < @EndDimDateKey
AND NextAssessment.dim_date_key > RecurrentProductSubscription.billed_until_dim_date_key           ---- not pre-paid for the remaining dates in the month
AND (Member.member_active_flag = 'Y' or  Member.member_active_flag is Null)  ----- member is currently active - assuming null is active
AND Membership.membership_status <> 'Suspended'   ----- membership is currently not suspended   
AND Membership.membership_status <> 'Terminated'   ----- membership is currently not terminated

UNION ALL


SELECT Club.DimClubKey,  ------ new name
	   Club.MMSClubID,   
	   DimReportingHierarchy.DimReportingHierarchyKey, 
	   ISNULL(CASE WHEN (MRP.activation_dim_date_key <= Dates.DimDateKey 
                          AND (MRP.termination_dim_date_key = '-998' OR IsNull(MRP.termination_dim_date_key,99991231) >= Dates.DimDateKey))
						  AND (MRP.hold_start_dim_date_key = '-998'
		                        OR (IsNull(MRP.hold_start_dim_date_key,19000101)< Dates.DimDateKey AND IsNull(MRP.hold_end_dim_date_key,19000101)< Dates.DimDateKey)
		                        OR (IsNull(MRP.hold_start_dim_date_key,99991231)> Dates.DimDateKey AND IsNull(MRP.hold_end_dim_date_key,99991231)> Dates.DimDateKey)
			                   )
				   THEN MRP.Price  
				   END,0) LocalCurrency_ForecastedAmount,        
       MRP.commission_employee_id AS CommissionEmployeeID,   
	   'Non-1on1 Product' AS OneOnOneProductGrouping 
  FROM [marketing].[v_fact_mms_membership_recurrent_product] MRP
  JOIN #ProjectionUnassessedDates Dates 
    ON MRP.assessment_day_of_month = Dates.DayNumberInCalendarMonth
  JOIN #DimClubKeyList Club
    ON Club.DimClubKey  = MRP.dim_club_key  
  JOIN [marketing].[v_dim_mms_product] Product
    ON Product.dim_mms_product_key = MRP.dim_mms_product_key
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON DimReportingHierarchy.DimReportingHierarchyKey = Product.dim_reporting_hierarchy_key
WHERE (MRP.activation_dim_date_key <= Dates.DimDateKey AND IsNull(MRP.termination_dim_date_key,'-998') = '-998')
       OR
      (MRP.activation_dim_date_key <= Dates.DimDateKey AND IsNull(MRP.termination_dim_date_key,'99991231') >= Dates.DimDateKey)




  -------- Package Sales

  IF OBJECT_ID('tempdb.dbo.#PackageSales', 'U') IS NOT NULL
  DROP TABLE #PackageSales; 

   SELECT FactPackage.reporting_dim_club_key AS DimClubKey,
          DimClub.MMSClubID,
          SalesCommissionEmployee.employee_id AS EmployeeID,						
	      DimProduct.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
		  SUM(CASE WHEN FactPackage.created_dim_date_key = @ReportDateDimDateKey
		           THEN Factpackage.number_of_sessions
				   ELSE 0
				   END) ReportDate_PackageSessionCount,
		  SUM(Factpackage.number_of_sessions) AS PackageSessionCount,	
		  SUM(CASE WHEN FactPackage.created_dim_date_key = @ReportDateDimDateKey
			   THEN (FactPackage.number_of_sessions * FactPackage.price_per_session) 
			   ELSE 0
			  END) ReportDate_ItemAmount,
	      SUM(FactPackage.number_of_sessions * FactPackage.price_per_session) MTD_ItemAmount,	
	   'Non-1on1 Product' AS OneOnOneProductGrouping,
	   CASE WHEN FactPackage.package_entered_dim_employee_key in('472FCDFB3A0C0F13F71347423EFE788E','1AE84517D869A82B29B74A2939412358')   ---- Internet Sales and Ecommerce Sales
	        THEN 'On Line'
			ELSE 'In Club'
			END ReportingDataSource
    INTO #PackageSales   
	 FROM [marketing].[v_fact_mms_package]  FactPackage    
	   JOIN #DimClubKeyList   DimClub
	     ON FactPackage.reporting_dim_club_key = DimClub.DimClubKey
	   JOIN [marketing].[v_dim_mms_product_history] AS DimProduct
	     ON FactPackage.dim_mms_product_key = DimProduct.dim_mms_product_key
		 AND DimProduct.effective_date_time <  @EndDatePlusOne
		 AND DimProduct.expiration_date_time >=  @EndDatePlusOne
	   JOIN [marketing].[v_dim_date]  AS TransactionPostDimDate
	     ON FactPackage.transaction_post_dim_date_key = TransactionPostDimDate.dim_date_key
	   JOIN #PTDSSRCategories AS PTDSSRCategories
	     ON DimProduct.dim_reporting_hierarchy_key = PTDSSRCategories.DimReportingHierarchyKey
	   LEFT JOIN [marketing].[v_dim_employee] AS SalesCommissionEmployee
	     ON FactPackage.primary_sales_dim_employee_key = SalesCommissionEmployee.dim_employee_key

	 WHERE FactPackage.transaction_void_flag = 'N'
	   AND TransactionPostDimDate.dim_date_key >= @FirstOfReportMonthDimDateKey
	   AND TransactionPostDimDate.dim_date_key <= @EndDimDateKey
	   AND FactPackage.price_per_session <> 0

	GROUP BY  FactPackage.reporting_dim_club_key,
          DimClub.MMSClubID,
          SalesCommissionEmployee.employee_id,					
	      DimProduct.dim_reporting_hierarchy_key,
          CASE WHEN FactPackage.package_entered_dim_employee_key in('472FCDFB3A0C0F13F71347423EFE788E','1AE84517D869A82B29B74A2939412358')   ---- Internet Sales and Ecommerce Sales
	        THEN 'On Line'
			ELSE 'In Club'
			END

 ------
 --- Delivered Sessions
 ------

   IF OBJECT_ID('tempdb.dbo.#DeliveredSessions', 'U') IS NOT NULL
  DROP TABLE #DeliveredSessions;

 SELECT DimClub.DimClubKey,
    DimClub.MMSClubID,
	DimEmployee.employee_id AS EmployeeID,
    DimProduct.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
	SUM(CASE WHEN FactPackageSession.delivered_dim_date_key = @ReportDateDimDateKey
	         THEN FactPackageSession.session_complete_count
			 ELSE 0
			 END) AS ReportDate_DeliveredSessionCount,
	SUM(CASE WHEN FactPackageSession.delivered_dim_date_key = @ReportDateDimDateKey
	         THEN FactPackageSession.delivered_session_price
			 ELSE 0
			 END) AS ReportDate_DeliveredSessionPrice,
    SUM(FactPackageSession.session_complete_count) AS MTD_DeliveredSessionCount,
	SUM(FactPackageSession.delivered_session_price) AS MTD_DeliveredSessionPrice,
    'Non-1on1 Product' OneOnOneProductGrouping

INTO #DeliveredSessions   
FROM [marketing].[v_fact_mms_package_session] FactPackageSession
    JOIN [marketing].[v_dim_date] DeliveredDimDate
        ON FactPackageSession.delivered_dim_date_key = DeliveredDimDate.dim_date_key
    JOIN [marketing].[v_dim_mms_product_history] DimProduct
	    ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
		 AND DimProduct.effective_date_time <  @EndDatePlusOne
		 AND DimProduct.expiration_date_time >=  @EndDatePlusOne
    JOIN #DimReportingHierarchy DimReportingHierarchy
	    ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
	JOIN [marketing].[v_dim_employee] DimEmployee
	    ON FactPackageSession.delivered_dim_employee_key = DimEmployee.dim_employee_key
    JOIN #DimClubKeyList DimClub
	    ON FactPackageSession.reporting_dim_club_key = DimClub.DimClubKey

WHERE FactPackageSession.voided_flag = 'N'
    AND DeliveredDimDate.four_digit_year_dash_two_digit_month = @ReportFourDigitYearDashTwoDigitMonth
    AND DeliveredDimDate.dim_date_key <= @ReportDateDimDateKey
	AND FactPackageSession.delivered_session_price <> 0
GROUP BY DimClub.DimClubKey,
         DimClub.MMSClubID,
	     DimEmployee.employee_id,
         DimProduct.dim_reporting_hierarchy_key


----- package session adjustments

   IF OBJECT_ID('tempdb.dbo.#FactPackageAdjustment', 'U') IS NOT NULL
  DROP TABLE #FactPackageAdjustment;

  SELECT DISTINCT FactPackageAdjustment.fact_mms_package_adjustment_key AS FactPackageAdjustmentKey,
                FactPackageAdjustment.fact_mms_package_key AS FactPackageKey,
                FactPackageAdjustment.adjusted_dim_date_key AS AdjustedDimDateKey,
                FactPackageAdjustment.adjustment_mms_tran_id AS AdjustmentMMSTranID,
                FactPackageAdjustment.adjustment_type_dim_description_key AS AdjustmentTypeDimDescriptionKey,
                FactPackageAdjustment.dim_mms_member_key AS DimMemberKey,   ----- name change
                FactPackageAdjustment.dim_mms_product_key AS DimProductKey,
                FactPackageAdjustment.adjustment_dim_employee_key AS AdjustmentDimEmployeeKey,
                FactPackageAdjustment.number_of_sessions_adjusted AS NumberOfSessionsAdjusted,
                FactPackageAdjustment.package_adjustment_amount AS PackageAdjustmentAmount,
                FactPackageAdjustment.adjustment_comment AS AdjustmentComment,
				DimProduct.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
				'Non-1on1 Product' OneOnOneProductGrouping
INTO #FactPackageAdjustment   
  FROM [marketing].[v_fact_mms_package_adjustment] FactPackageAdjustment
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
	ON FactPackageAdjustment.dim_mms_product_key = DimProduct.dim_mms_product_key
	 AND DimProduct.effective_date_time <  @EndDatePlusOne
	 AND DimProduct.expiration_date_time >=  @EndDatePlusOne
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #DimClubKeyList DimClub
    ON FactPackageAdjustment.package_entered_dim_club_key = DimClub.DimClubKey
    OR FactPackageAdjustment.package_entered_dim_club_key = 'A0ED715C32B4C303C017597A02F6515D'   ----- MMS Club ID = 13 "Corporate INTERNAL"

 WHERE FactPackageAdjustment.adjusted_dim_date_key >= @FirstOfReportMonthDimDateKey
   AND FactPackageAdjustment.adjusted_dim_date_key <= @ReportDateDimDateKey
   AND FactPackageAdjustment.package_adjustment_amount <> 0
   AND FactPackageAdjustment.adjustment_type_dim_description_key <> 'r_mms_val_package_adjustment_type_2A7AC50812AD92E9BD4CD2E95E3BB652'   -------  Description = "Void"


   IF OBJECT_ID('tempdb.dbo.#AdjustmentSessions', 'U') IS NOT NULL
  DROP TABLE #AdjustmentSessions;

SELECT FactPackage.reporting_dim_club_key AS DimClubKey,   ----- name change
       #DimClubKeyList.MMSClubID,
       PrimarySalesDimEmployee.employee_id AS EmployeeID,
       #FactPackageAdjustment.DimReportingHierarchyKey,
	   SUM(CASE WHEN #FactPackageAdjustment.AdjustedDimDateKey = @ReportDateDimDateKey
	            THEN #FactPackageAdjustment.NumberOfSessionsAdjusted
				ELSE 0
				END) ReportDate_SessionCount,
	   SUM(CASE WHEN #FactPackageAdjustment.AdjustedDimDateKey = @ReportDateDimDateKey
	            THEN #FactPackageAdjustment.PackageAdjustmentAmount
				ELSE 0
				END) ReportDate_SessionAmount,
       SUM(#FactPackageAdjustment.NumberOfSessionsAdjusted) AS SessionCount,
       SUM(#FactPackageAdjustment.PackageAdjustmentAmount) AS SessionAmount,
	   #FactPackageAdjustment.OneOnOneProductGrouping
INTO #AdjustmentSessions   
  FROM #FactPackageAdjustment
  JOIN [marketing].[v_fact_mms_package] FactPackage
    ON #FactPackageAdjustment.FactPackageKey = FactPackage.fact_mms_package_key
  JOIN #DimClubKeyList
    ON FactPackage.reporting_dim_club_key = #DimClubKeyList.DimClubKey
  JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee
    ON FactPackage.primary_sales_dim_employee_key = PrimarySalesDimEmployee.dim_employee_key

 GROUP BY FactPackage.reporting_dim_club_key,
       #DimClubKeyList.MMSClubID,
       PrimarySalesDimEmployee.employee_id,
       #FactPackageAdjustment.DimReportingHierarchyKey,
	   #FactPackageAdjustment.OneOnOneProductGrouping



------ Outstanding Package sessions data

DECLARE @ReportDatePlus2 DATETIME
SET @ReportDatePlus2 = DATEADD(day,2,@ReportDate)

   IF OBJECT_ID('tempdb.dbo.#FactPackageKeys', 'U') IS NOT NULL
  DROP TABLE #FactPackageKeys;

SELECT Club.DimClubKey,
       Club.MMSClubID,
       FactPackage.fact_mms_package_key AS FactPackageKey,
       FactPackage.package_id AS PackageID,
       FactPackage.sessions_left AS SessionsLeft,
       FactPackage.balance_amount AS BalanceAmount,
       PrimarySalesDimEmployee.employee_id AS EmployeeID,
       DimProduct.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
       'Non-1on1 Product' AS OneOnOneProductGrouping
INTO #FactPackageKeys	
 FROM  [marketing].[v_fact_mms_package] FactPackage	
JOIN #DimClubKeyList Club	
   ON  FactPackage.reporting_dim_club_key = Club.DimclubKey	
JOIN [marketing].[v_dim_mms_product_history] DimProduct
   ON FactPackage.dim_mms_product_key = DimProduct.dim_mms_product_key
	AND DimProduct.effective_date_time <  @EndDatePlusOne
	AND DimProduct.expiration_date_time >=  @EndDatePlusOne	
JOIN #DimReportingHierarchy Hier	
   ON  DimProduct.dim_reporting_hierarchy_key = Hier.DimReportingHierarchyKey
JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee
   ON FactPackage.primary_sales_dim_employee_key = PrimarySalesDimEmployee.dim_employee_key

WHERE FactPackage.price_per_session > 0		
AND FactPackage.transaction_void_flag = 'N'	
AND FactPackage.inserted_date_time < @ReportDatePlus2  -- Insertions/Updates ON  the 1st of the month are for the prior month	
AND (FactPackage.sessions_left > 0 OR (FactPackage.sessions_left = 0 AND FactPackage.updated_date_time >= @ReportDatePlus2))	




   IF OBJECT_ID('tempdb.dbo.#DeliveredSessionsAdj', 'U') IS NOT NULL
  DROP TABLE #DeliveredSessionsAdj;
	
	----- to gather sessions delivered since the report date, so they can be added back
SELECT PackageSession.fact_mms_package_key AS FactPackageKey, 
       SUM(PackageSession.delivered_session_price) AS DeliveredSessionPrice, 
	   COUNT(PackageSession.fact_mms_package_session_key) AS DeliveredSessionQuantity	
INTO #DeliveredSessionsAdj	
 FROM  [marketing].[v_fact_mms_package_session] PackageSession	
JOIN #FactPackageKeys #Keys	
 ON PackageSession.fact_mms_package_key = #Keys.FactPackageKey
WHERE PackageSession.delivered_dim_date_key > @ReportDateDimDateKey
AND PackageSession.voided_flag = 'N'	
GROUP BY PackageSession.fact_mms_package_key	


   IF OBJECT_ID('tempdb.dbo.#Adjustments', 'U') IS NOT NULL
  DROP TABLE #Adjustments;	

	----- to gather sessions adjusted since the report date, so they can be added back	
SELECT PackageAdjustments.fact_mms_package_key AS FactPackageKey, 
       SUM(PackageAdjustments.number_of_sessions_adjusted) AS NumberOfSessionsAdjusted,
       SUM(PackageAdjustments.package_adjustment_amount) AS PackageAdjustmentAmount	
INTO #Adjustments	
 FROM  [marketing].[v_fact_mms_package_adjustment] PackageAdjustments	
 JOIN #FactPackageKeys #Keys	
   ON PackageAdjustments.fact_mms_package_key = #Keys.FactPackageKey	
WHERE PackageAdjustments.adjusted_dim_date_key > @ReportDateDimDateKey
GROUP BY PackageAdjustments.fact_mms_package_key	


   IF OBJECT_ID('tempdb.dbo.#OutstandingSessions', 'U') IS NOT NULL
  DROP TABLE #OutstandingSessions;	

SELECT #Keys.DimClubKey,
       #Keys.MMSClubID,
	   #Keys.EmployeeID,
	   #Keys.DimReportingHierarchyKey,
       Sum(#Keys.SessionsLeft + IsNull(#Delivered.DeliveredSessionQuantity,0) + IsNull(ADJ.NumberOfSessionsAdjusted,0)) AS RemainingPackageSessions,	
	   Sum(#Keys.BalanceAmount + IsNull(#Delivered.DeliveredSessionPrice,0) + IsNull(ADJ.PackageAdjustmentAmount,0)) AS RemainingPackageBalance,
	   #Keys.OneOnOneProductGrouping
INTO #OutstandingSessions  
 FROM  #FactPackageKeys #Keys		
LEFT JOIN #DeliveredSessionsAdj #Delivered	
 ON  #Keys.FactPackageKey = #Delivered.FactPackageKey	
LEFT JOIN #Adjustments ADJ	
 ON  #Keys.FactPackageKey = ADJ.FactPackageKey	
GROUP BY #Keys.DimClubKey,
       #Keys.MMSClubID,
	   #Keys.EmployeeID,
	   #Keys.DimReportingHierarchyKey,
	   #Keys.OneOnOneProductGrouping


------- Bring it all together

   IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results;	

 Select MMSClubID,
        PrimarySalesEmployeeID AS CommissionEmployeeID,
        DimReportingHierarchyKey, 
        ReportDate_ItemAmount,
        0 AS PayPeriodToDate_ItemAmount,
        MTD_ItemAmount,
        0 AS ForecastedAmount,
		0 As SessionSalesCount_ReportDate,	
        0 AS SessionSalesAmount_ReportDate,
        0 As SessionSalesCount,	
        0 AS SessionSalesAmount,
		0 AS SessionDeliveredCount_ReportDate,
        0 AS SessionDeliveredAmount_ReportDate,
        0 AS SessionDeliveredCount,
        0 AS SessionDeliveredAmount,
		0 AS SessionAdjustmentCount_ReportDate,
        0 AS SessionAdjustmentAmount_ReportDate,
        0 AS SessionAdjustmentCount,
        0 AS SessionAdjustmentAmount,
        0 AS SessionOutstandingCount,
        0 AS SessionOutstandingAmount,
        ReportingDataSource,
        OneOnOneProductGrouping
INTO #Results          
 FROM #Revenue_Summary


 UNION ALL

 SELECT MMSClubID,
        CommissionEmployeeID,
        DimReportingHierarchyKey, 
        0 AS ReportDate_ItemAmount,
        0 AS PayPeriodToDate_ItemAmount,
        0 AS MTD_ItemAmount,
        LocalCurrency_ForecastedAmount AS ForecastedAmount,
	    0 As SessionSalesCount_ReportDate,	
        0 AS SessionSalesAmount_ReportDate,
        0 As SessionSalesCount,	
        0 AS SessionSalesAmount,
		0 AS SessionDeliveredCount_ReportDate,
        0 AS SessionDeliveredAmount_ReportDate,
        0 AS SessionDeliveredCount,
        0 AS SessionDeliveredAmount,
		0 AS SessionAdjustmentCount_ReportDate,
        0 AS SessionAdjustmentAmount_ReportDate,
        0 AS SessionAdjustmentCount,
        0 AS SessionAdjustmentAmount,
        0 AS SessionOutstandingCount,
        0 AS SessionOutstandingAmount,
        'In Club' AS ReportingDataSource,
        OneOnOneProductGrouping
 FROM #ProjectionSummary

 UNION ALL


  SELECT MMSClubID,	
        EmployeeID AS CommissionEmployeeID,						
	    DimReportingHierarchyKey,
		0 AS ReportDate_ItemAmount,
        0 AS PayPeriodToDate_ItemAmount,
        0 AS MTD_ItemAmount,
		0 AS ForecastedAmount,
	    ReportDate_PackageSessionCount As SessionSalesCount_ReportDate,	
        ReportDate_ItemAmount AS SessionSalesAmount_ReportDate,
		PackageSessionCount As SessionSalesCount,	
	    MTD_ItemAmount AS SessionSalesAmount,
		0 AS SessionDeliveredCount_ReportDate,
        0 AS SessionDeliveredAmount_ReportDate,
        0 AS SessionDeliveredCount,
        0 AS SessionDeliveredAmount,
		0 AS SessionAdjustmentCount_ReportDate,
        0 AS SessionAdjustmentAmount_ReportDate,
        0 AS SessionAdjustmentCount,
        0 AS SessionAdjustmentAmount,
		0 AS SessionOutstandingCount,
		0 AS SessionOutstandingAmount,
		ReportingDataSource,	
	    OneOnOneProductGrouping
  FROM #PackageSales

  UNION ALL

   SELECT MMSClubID,	
        EmployeeID AS CommissionEmployeeID,						
	    DimReportingHierarchyKey,
		0 AS ReportDate_ItemAmount,
        0 AS PayPeriodToDate_ItemAmount,
        0 AS MTD_ItemAmount,
		0 AS ForecastedAmount,
	    0 As SessionSalesCount_ReportDate,	
        0 AS SessionSalesAmount_ReportDate,
		0 As SessionSalesCount,	
	    0 AS SessionSalesAmount,
		ReportDate_DeliveredSessionCount AS SessionDeliveredCount_ReportDate,
        ReportDate_DeliveredSessionPrice AS SessionDeliveredAmount_ReportDate,
		MTD_DeliveredSessionCount AS SessionDeliveredCount,
		MTD_DeliveredSessionPrice AS SessionDeliveredAmount,
		0 AS SessionAdjustmentCount_ReportDate,
        0 AS SessionAdjustmentAmount_ReportDate,
        0 AS SessionAdjustmentCount,
        0 AS SessionAdjustmentAmount,
		0 AS SessionOutstandingCount,
		0 AS SessionOutstandingAmount,
		'In Club' as ReportingDataSource,	
	    OneOnOneProductGrouping
 FROM #DeliveredSessions

  UNION ALL

    SELECT MMSClubID,	
        EmployeeID AS CommissionEmployeeID,						
	    DimReportingHierarchyKey,
		0 AS ReportDate_ItemAmount,
        0 AS PayPeriodToDate_ItemAmount,
        0 AS MTD_ItemAmount,
		0 AS ForecastedAmount,
		0 As SessionSalesCount_ReportDate,	
        0 AS SessionSalesAmount_ReportDate,
        0 As SessionSalesCount,	
        0 AS SessionSalesAmount,
		0 AS SessionDeliveredCount_ReportDate,
        0 AS SessionDeliveredAmount_ReportDate,
        0 AS SessionDeliveredCount,
        0 AS SessionDeliveredAmount,
		ReportDate_SessionCount AS SessionAdjustmentCount_ReportDate,
        ReportDate_SessionAmount AS SessionAdjustmentAmount_ReportDate,
		SessionCount AS SessionAdjustmentCount,
		SessionAmount AS SessionAdjustmentAmount,
		0 AS SessionOutstandingCount,
		0 AS SessionOutstandingAmount,
		'In Club' as ReportingDataSource,		
	    OneOnOneProductGrouping
     FROM #AdjustmentSessions

 UNION ALL

    SELECT MMSClubID,	
        EmployeeID AS CommissionEmployeeID,						
	    DimReportingHierarchyKey,
		0 AS ReportDate_ItemAmount,
        0 AS PayPeriodToDate_ItemAmount,
        0 AS MTD_ItemAmount,
        0 AS ForecastedAmount,
		0 As SessionSalesCount_ReportDate,	
        0 AS SessionSalesAmount_ReportDate,
        0 As SessionSalesCount,	
        0 AS SessionSalesAmount,
		0 AS SessionDeliveredCount_ReportDate,
        0 AS SessionDeliveredAmount_ReportDate,
        0 AS SessionDeliveredCount,
        0 AS SessionDeliveredAmount,
		0 AS SessionAdjustmentCount_ReportDate,
        0 AS SessionAdjustmentAmount_ReportDate,
        0 AS SessionAdjustmentCount,
        0 AS SessionAdjustmentAmount,
		RemainingPackageSessions AS SessionOutstandingCount,
		RemainingPackageBalance AS SessionOutstandingAmount,
		'In Club' as ReportingDataSource,		
	    OneOnOneProductGrouping
	FROM #OutstandingSessions




   IF OBJECT_ID('tempdb.dbo.#Prelim_Output', 'U') IS NOT NULL
  DROP TABLE #Prelim_Output;	

SELECT @ReportDateDimDateKey AS ReportDateDimDateKey,
 DimClub.MMSClubID,
 DimClub.DimClubKey,
 IsNull(DimEmployee.dim_employee_key,'-998') AS DimEmployeeKey,
 IsNull(DimEmployee.employee_id,0) AS EmployeeID,
 ClubGoal.DimReportingHierarchyKey,
 ClubGoal.PTDSSRCategory,
 ClubGoal.PTDSSRRowLabel,
 SUM(IsNull(Results.ReportDate_ItemAmount,0)) AS ReportDateItemAmount,
 SUM(IsNull(Results.PayPeriodToDate_ItemAmount,0)) AS PayPeriodToDateItemAmount,
 SUM(IsNull(Results.MTD_ItemAmount,0)) AS MonthToDateItemAmount,
 IsNull(Results.ReportingDataSource,'In Club') AS ReportingDataSource,
 SUM(IsNull(Results.ForecastedAmount,0)) AS ForecastAmount, 
 0 AS ReportDateDeliveredSessionPrice,
 0 AS PayPeriodToDateDeliveredSessionPrice,
 0 AS MonthToDateDeliveredSessionPrice,
 @ReportDateLastDayInMonthIndicator AS ReportDateIsLastDayInMonthIndicator,
 IsNull(Results.OneOnOneProductGrouping,'Non-1on1 Product') AS OneOnOneProductGrouping,
 SUM(IsNull(Results.SessionSalesCount,0)) AS MonthToDate_SessionSalesCount,	
 SUM(IsNull(Results.SessionSalesAmount,0)) AS MonthToDate_SessionSalesAmount,
 SUM(IsNull(Results.SessionDeliveredCount,0)) AS MonthToDate_SessionDeliveredCount,
 SUM(IsNull(Results.SessionDeliveredAmount,0)) AS MonthToDate_SessionDeliveredAmount,
 SUM(IsNull(Results.SessionAdjustmentCount,0)) AS MonthToDate_SessionAdjustmentCount,
 SUM(IsNull(Results.SessionAdjustmentAmount,0)) AS MonthToDate_SessionAdjustmentAmount,
 SUM(IsNull(Results.SessionOutstandingCount,0)) AS ReportDate_SessionOutstandingCount,
 SUM(IsNull(Results.SessionOutstandingAmount,0)) AS ReportDate_SessionOutstandingAmount,
 MAX(IsNull(ClubGoal.GoalDollarAmount,0)) AS ClubGoal, 
 SUM(IsNull(Results.SessionSalesCount_ReportDate,0)) AS ReportDate_SessionSalesCount,	
 SUM(IsNull(Results.SessionSalesAmount_ReportDate,0)) AS ReportDate_SessionSalesAmount, 
 SUM(IsNull(Results.SessionDeliveredCount_ReportDate,0)) AS ReportDate_SessionDeliveredCount,
 SUM(IsNull(Results.SessionDeliveredAmount_ReportDate,0)) AS ReportDate_SessionDeliveredAmount,
 SUM(IsNull(Results.SessionAdjustmentCount_ReportDate,0)) AS ReportDate_SessionAdjustmentCount,
 SUM(IsNull(Results.SessionAdjustmentAmount_ReportDate,0)) AS ReportDate_SessionAdjustmentAmount
 INTO #Prelim_Output   

 FROM  #Goals_ClubHierarchyKey ClubGoal
 LEFT JOIN #DimClubKeyList DimClub
   ON ClubGoal.ClubID = DimClub.MMSClubID
 LEFT JOIN [marketing].[v_dim_reporting_hierarchy] DimHierarchy
   ON ClubGoal.DimReportingHierarchyKey = DimHierarchy.dim_reporting_hierarchy_key
 LEFT JOIN #Results Results
   ON Results.MMSClubID = ClubGoal.ClubID
    AND Results.DimReportingHierarchyKey = ClubGoal.DimReportingHierarchyKey
 LEFT JOIN [marketing].[v_dim_employee] DimEmployee
   ON Results.CommissionEmployeeID = DimEmployee.employee_id
 GROUP BY 
 DimClub.DimClubKey,
 DimClub.MMSClubID,
 DimEmployee.dim_employee_key,
 DimEmployee.employee_id,
 ClubGoal.DimReportingHierarchyKey,
 Results.ReportingDataSource,
 Results.OneOnOneProductGrouping,
 ClubGoal.PTDSSRCategory,
 ClubGoal.PTDSSRRowLabel


 ----   Delete records for 14 months prior except for the final day's records for each month
  DELETE dbo.fact_ptdssr_revenue_and_service_employee_summary
  WHERE report_date_dim_date_key < @FirstOf13MonthsPriorDimDateKey
    AND report_date_is_last_day_in_month_indicator = 'N'

  ----  Populate table with new records

  INSERT INTO fact_ptdssr_revenue_and_service_employee_summary(
  report_date_dim_date_key,					------ varchar(8)
  dim_club_key,								------ varchar(32)								
  dim_employee_key,							------ varchar(32)
  dim_reporting_hierarchy_key,				------ varchar(32)
  ptdssr_category,							------ varchar(10)
  ptdssr_row_label,							------ varchar(500)
  report_date_item_amount,					------ decimal(26,2)
  month_to_date_item_amount,				------ decimal(26,2)
  reporting_data_source,					------ varchar(50)
  forecast_amount,							------ decimal(26,2)
  report_date_delivered_session_price,		------ decimal(26,2)
  month_to_date_delivered_session_price,	------ decimal(26,2)
  report_date_is_last_day_in_month_indicator, ---- varchar(1)
  one_on_one_product_grouping,				------ varchar(50)
  month_to_date_session_sales_count,		------ INT	
  month_to_date_session_sales_amount,		------ decimal(26,2)
  month_to_date_session_delivered_count,	------ INT
  month_to_date_session_delivered_amount,	------ decimal(26,2)
  month_to_date_session_adjustment_count,	------ INT
  month_to_date_session_adjustment_amount,	------ decimal(26,2)
  report_date_session_outstanding_count,	------ INT
  report_date_session_outstanding_amount,	------ decimal(26,2)
  club_goal,								------ decimal(26,2)
  report_date_session_sales_count,			------ INT
  report_date_session_sales_amount,			------ decimal(26,2)
  report_date_session_delivered_count,		------ INT
  report_date_session_delivered_amount,		------ decimal(26,2)  
  report_date_session_adjustment_count,		------ INT
  report_date_session_adjustment_amount,	------ decimal(26,2)
  dv_load_date_time,		-- need to include all dv_columns in stored procedure
  dv_load_end_date_time,	-- need to include all dv_columns in stored procedure
  dv_batch_id,				-- need to include all dv_columns in stored procedure
  dv_inserted_date_time,	-- need to include all dv_columns in stored procedure
  dv_insert_user			-- need to include all dv_columns in stored procedure
  )


 SELECT   ReportDateDimDateKey,
  DimClubKey,
  DimEmployeeKey,
  DimReportingHierarchyKey,
  PTDSSRCategory,
  PTDSSRRowLabel,
  ReportDateItemAmount,
  MonthToDateItemAmount,
  ReportingDataSource,
  ForecastAmount,
  ReportDateDeliveredSessionPrice,
  MonthToDateDeliveredSessionPrice,
  ReportDateIsLastDayInMonthIndicator,
  OneOnOneProductGrouping,
  MonthToDate_SessionSalesCount,	
  MonthToDate_SessionSalesAmount,
  MonthToDate_SessionDeliveredCount,
  MonthToDate_SessionDeliveredAmount,
  MonthToDate_SessionAdjustmentCount,
  MonthToDate_SessionAdjustmentAmount,
  ReportDate_SessionOutstandingCount,
  ReportDate_SessionOutstandingAmount,
  ClubGoal,
  ReportDate_SessionSalesCount,	
  ReportDate_SessionSalesAmount,  
  ReportDate_SessionDeliveredCount,
  ReportDate_SessionDeliveredAmount,  
  ReportDate_SessionAdjustmentCount,
  ReportDate_SessionAdjustmentAmount,
  getdate(),												--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
  convert(datetime, '99991231', 112),						--this value would be same for all the stored procedure
  '-1',														--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
  getdate(),												--this value would be same for all the stored procedure
  suser_sname()												--this value would be same for all the stored procedure								
	
FROM #Prelim_Output
WHERE ABS(MonthToDateItemAmount)+ForecastAmount+ClubGoal+MonthToDate_SessionSalesAmount+MonthToDate_SessionDeliveredAmount+MonthToDate_SessionAdjustmentAmount+ReportDate_SessionOutstandingAmount <> 0


 
  END
