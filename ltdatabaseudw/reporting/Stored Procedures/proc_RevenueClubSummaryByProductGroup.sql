CREATE PROC [reporting].[proc_RevenueClubSummaryByProductGroup] @StartFourDigitYearDashTwoDigitMonth [VARCHAR](22),@EndFourDigitYearDashTwoDigitMonth [VARCHAR](22),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DimReportingHierarchyKeyList [VARCHAR](8000),@DivisionList [VARCHAR](8000),@SubDivisionList [VARCHAR](8000),@TotalReportingHierarchyKeyCount [INT] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


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
												 

---------UDW in UTC TIME CONVERSION POPULATED --------------
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '
+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '
+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   


---CREATED SET PARAMETERS FOR DEFERRED E-COMM SALES OF 60 DAYS CHALLANGE PRODUCT
---RULE SET THAT CHALLANGE STARTS IN THE 2ND MONTH OF EACH QUARTER AND IF SALES ARE MADE IN THE 1ST MONTH OF THE QUARTER REVENUE IS DEFERRED TO 2ND MONTH.
DECLARE @FirstOfReportRangeDimDateKey INT
DECLARE @EndOfReportRangeDimDateKey INT
 
SET @FirstOfReportRangeDimDateKey = (SELECT MIN(Dim_Date_Key) FROM [marketing].[v_dim_date] WHERE Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth)
SET @EndOfReportRangeDimDateKey = (SELECT MAX(DIM_DATE_KEY) FROM [marketing].[v_dim_date] WHERE Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth)



DECLARE @EComm60DayChallengeRevenueStartMonthStartDimDateKey INT 
---When the requested month is the 2nd month of the quarter, set the start date to the prior month 
SET @EComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT CASE WHEN (Select Month_Number_In_Year
					FROM [marketing].[v_dim_date]
					WHERE dim_date_key = @FirstOfReportRangeDimDateKey) in (2,5,8,11)
			THEN (SELECT Prior_Month_Starting_Dim_Date_Key
					FROM [marketing].[v_dim_date] 
					WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
			ELSE (SELECT Month_Starting_Dim_Date_Key
					FROM [marketing].[v_dim_date]
					WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
			END
			FROM [marketing].[v_dim_date]
			WHERE dim_date_key = @FirstOfReportRangeDimDateKey) ---to limit result set to one record 


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


  ---continue from here

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

DECLARE @PriorYearEComm60DayChallengeRevenueStartMonthStartDimDateKey INT
SET @PriorYearEComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT PriorYearDimDate.dim_date_key 
                                                                       FROM [marketing].[v_dim_date] DimDate
                                                                       JOIN [marketing].[v_dim_date] PriorYearDimDate
                                                                         ON DimDate.Year - 1 = PriorYearDimDate.Year
                                                                           AND DimDate.Month_Number_In_Year = PriorYearDimDate.Month_Number_In_Year
                                                                           AND DimDate.Day_Number_In_Month = PriorYearDimDate.Day_Number_In_Month
																	   WHERE DimDate.dim_date_key = @EComm60DayChallengeRevenueStartMonthStartDimDateKey)
--Used SSSGGrandOpeningDeadlineDimDateKey to filter club sssg calc for off the dimDateKey - come back here to verify if needed.
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


DECLARE @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey INT
SET @PriorYearEComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT PriorYearDimDate.month_ending_dim_date_key
                                                                   FROM [marketing].[v_dim_date] DimDate
                                                                   JOIN [marketing].[v_dim_date] PriorYearDimDate
                                                                     ON DimDate.Year - 1 = PriorYearDimDate.Year
                                                                    AND DimDate.Month_Number_In_Year = PriorYearDimDate.Month_Number_In_Year
                                                                    -----AND DimDate.Day_Number_In_Month = PriorYearDimDate.Day_Number_In_Month  --- changed due to leap year
                                                                  WHERE DimDate.dim_date_key = @EComm60DayChallengeRevenueEndMonthEndDimDateKey
                                                                    AND PriorYearDimDate.Last_Day_In_Month_Flag = 'Y')
  
   
-----CREATE HIERARCHY TEMP TABLE TO RETURN SELECTED GROUP NAMES 
--get data from SP 
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
 
 ---============122 rows affected 
	--SELECT * FROM #DimReportingHierarchy
	--DimReportingHierarchyKey |DivisionName|SubdivisionName|DepartmentName|ProductGroupName|	RegionType|	ReportRegionType|	PTDeferredRevenueProductGroupFlag
--51523124532AD96AAEB0B8B212F41E37 |	Personal Training|	Nutrition, Metabolism & Weight Mgmt	|Lab Testing|Nutritional|	PT RCL Area	PT RCL Area|	N
--- ================

 DECLARE @HeaderDivisionList VARCHAR(8000)
 DECLARE @HeaderSubdivisionList VARCHAR(8000)
 DECLARE @RevenueReportingDepartmentNameCommaList VARCHAR(8000)
 DECLARE @RevenueProductGroupNameCommaList VARCHAR(8000)

 SET @HeaderDivisionList = CASE WHEN @DivisionList like '%All Divisions%'
									 THEN 'All Divisions'
								 ELSE REPLACE(@DivisionList, '|', ', ') END
 SET @HeaderSubdivisionList = CASE WHEN @SubdivisionList like '%All Subdivisions%'
									 THEN 'All Subdivisions'
									 --replace the pipe-dilimted in string_exp with comma
								 ELSE REPLACE(@SubdivisionList, '|', ',') END
 SET @RevenueReportingDepartmentNameCommaList = CASE WHEN @DepartmentMinDimReportingHierarchyKeyList like '%All Departments%'
									 THEN 'All Departments'
								 ELSE REPLACE(@DepartmentMinDimReportingHierarchyKeyList, '|', ', ') END
 SET @RevenueProductGroupNameCommaList = CASE WHEN @DimReportingHierarchyKeyList like '%All Product Groups%'
									 THEN 'All Product Groups'
								 ELSE REPLACE (@DimReportingHierarchyKeyList, '|', ', ') END




IF OBJECT_ID('tempdb.dbo.#RegionTypes', 'U') IS NOT NULL 
	DROP TABLE #RegionTypes;

SELECT RegionType
INTO #RegionTypes
FROM #DimReportingHierarchy
GROUP BY RegionType


---set variable to return just one region type 
DECLARE @RegionType VARCHAR(50)
SET @RegionType = (SELECT CASE WHEN COUNT(*) = 1 THEN MIN(RegionType) ELSE 'MMS Region' END 
					FROM #RegionTypes)
--==========1 Row affected --=====
--SELECT * FROM #RegionTypes
--RegionType
--PT RCL Area
--===========1 Row ====


---This query replaces the function used in LTFDM_Revenue "fnRevenueHistoricalDimLocation"
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
	   
-------End here for the fnRevenueHistoricalDimLocation in LTFDM_Revenue replacement in UDW
--SELECT * FROM #Clubs
--DimClubKey	club_id	ClubName	ClubCode	gl_club_id	LocalCurrencyCode	Region	ClubStatus	club_open_dim_date_key
--There seem to be club_open_dim_date_key returning -998 in multiple places in the #clubs table 
--FF9920FBE0ACAF55877B2297D0ED1C13	503	Midtown, New York-Deactivated	Midtown, New York-Deactivated	503	USD	NE Slechten	Open	-998
--189 Rows effected 
---==============


IF OBJECT_ID('tempdb.dbo.#PromptYearRevenue', 'U') IS NOT NULL 
	DROP TABLE #PromptYearRevenue;
SELECT DimLocation.DimClubKey,
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
	   
       FactAllocatedTransaction.allocated_amount AS ActualAmount,
	-- FactAllocatedTransaction.transaction_amount AS ActualAmount,
	   RevenuePostingDimDate.Dim_Date_Key DimDateKey --Null DimDateKey
	   --may not need below fields
	  -- FactAllocatedTransaction.transaction_id,
      -- FactAllocatedTransaction.line_number
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
   JOIN #Clubs DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
	 --uncomment below if its not needed 
   JOIN [marketing].[v_Dim_Date] RevenuePostingDimDate
	 ON FactAllocatedTransaction.allocated_month_starting_dim_date_key = RevenuePostingDimDate.Dim_Date_Key
	 --ends here ------
 --  WHERE  FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
          --AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @EndMonthStartingDimDateKey
 WHERE RevenuePostingDimDate.Dim_Date_Key >= @StartMonthStartingDimDateKey
   AND RevenuePostingDimDate.Dim_Date_Key <= @EndMonthStartingDimDateKey

UNION ALL 

SELECT DimLocation.DimClubKey,
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
       
       FactAllocatedTransaction.allocated_amount AS ActualAmount,
	-- FactAllocatedTransaction.transaction_amount AS ActualAmount,
	 --  TransactionPostDimDate.Dim_Date_Key DimDateKey
	   CASE WHEN TransactionPostDimDate.Month_Number_In_Year in (1, 4, 7, 10)
			  THEN TransactionPostDimDate.Next_Month_Starting_Dim_Date_Key 
			  ELSE TransactionPostDimDate.Month_Starting_Dim_Date_Key 
			  END DimDateKey
		
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

---==========2,919,050 Rows affected============================================
--SELECT * FROM #PromptYearRevenue
--Sample: DimClubKey	RevenueReportingDepartmentName	RevenueProductGroupName	RevenueProductGroupSortOrder	ActualAmount	DimDateKey
--5514F969A7B4A85FD63CB97D904427F1|	Weight Loss Challenges|	Weight Loss Challenges|	1|	60.000000|	20170801
--9BEF6D87E6113EFD550FF0C5777A15CC|	PT Nutritionals|	Omega-3 Fish Oil|	1|	40.480000|	20170801
---=============================================================================

IF OBJECT_ID('tempdb.dbo.#ClubRevenueSummary', 'U') IS NOT NULL
	DROP TABLE #ClubRevenueSummary;

	SELECT DimClubKey,
		   RevenueReportingDepartmentName,
		   RevenueProductGroupName,
		   SUM(ActualAmount) ClubActualAmount,
		   MAX(RevenueProductGroupSortOrder) RevenueProductGroupSortOrder
	INTO #ClubRevenueSummary
	 FROM #PromptYearRevenue --where RevenueProductGroupName    --217579
	WHERE DimDateKey BETWEEN @StartMonthStartingDimDateKey AND @EndMonthStartingDimDateKey
	--Change made to avoid Null value for productGroup and DepartmentName 
	AND RevenueProductGroupName IS NOT NULL
	GROUP BY DimClubKey,
			 RevenueReportingDepartmentName,
			 RevenueProductGroupName

		 
---======4440 rows affected ----====================================
	--SELECT * FROM #PromptYearRevenue where RevenueProductGroupName is NOT NULL null 6M with all null values [5967340 IS NULL]
	--SELECT * FROM #CLUBREVENUESUMMARY where revenueproductgroupname is not null - 5734 showing some null values for both depart and product group name 
--------------------------------------------------------------------

----CREATE TEMP TABLE FOR PRIOR YEAR REV COLLECTIONS
IF OBJECT_ID('tempdb.dbo.#PriorYearRevenue', 'U') IS NOT NULL
  DROP TABLE #PriorYearRevenue;

SELECT DimLocation.DimClubKey,
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

		FactAllocatedTransaction.original_currency_code CurrencyCode,
	    
       FactAllocatedTransaction.allocated_amount AS ActualAmount,
	 --FactAllocatedTransaction.transaction_amount AS ActualAmount,
		TransactionPostDimDate.Dim_Date_Key DimDateKey
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
   JOIN [marketing].[v_dim_date] TransactionPostDimDate
     ON FactAllocatedTransaction.transaction_dim_date_key = TransactionPostDimDate.dim_date_key

   WHERE FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @PriorYearStartMonthStartingDimDateKey
          AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @PriorYearEndMonthStartingDimDateKey
  
  
UNION ALL

SELECT DimLocation.DimClubKey,
	   CASE WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,
	   FactAllocatedTransaction.original_currency_code CurrencyCode,
      
       FactAllocatedTransaction.allocated_amount AS ActualAmount,
	-- FactAllocatedTransaction.transaction_amount AS ActualAmount,
	   TransactionPostDimDate.Dim_Date_Key DimDateKey
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
		  

---=========================2,781,491 ----------------
	--Select * From #PriorYearRevenue
-----================================================

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
	   DimLocation.ClubStatus,
       SUM(ActualAmount) ActualAmount
  INTO #PriorYearSSSGRegionRevenueSummary 
  FROM #PriorYearRevenue
  JOIN #Clubs DimLocation
    ON #PriorYearRevenue.DimClubKey = DimLocation.DimClubKey
   AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey  
 WHERE DimDateKey BETWEEN @PriorYearStartMonthStartingDimDateKey AND @PriorYearEndMonthStartingDimDateKey
 GROUP BY DimLocation.Region,
		  DimLocation.ClubStatus
		--continue from here end of thrusdy
	--	select * from #PriorYearSSSGRegionRevenueSummary
	--  select * from #clubs
 IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGRegionRevenueSummary', 'U') IS NOT NULL
  DROP TABLE #PromptYearSSSGRegionRevenueSummary;
         
SELECT DimLocation.Region,
	   DimLocation.ClubStatus,
       SUM(ActualAmount) ActualAmount
  INTO #PromptYearSSSGRegionRevenueSummary  
  FROM #PromptYearRevenue
  JOIN #Clubs DimLocation
    ON #PromptYearRevenue.DimClubKey = DimLocation.DimClubKey
   AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey     
 GROUP BY DimLocation.Region,
		  DimLocation.ClubStatus
---==================#PromptYearSSSGRegionRevenueSummary----
		-- SELECT * FROM #PromptYearSSSGRegionRevenueSummary
		--RERTUNS 18 ROWS AFFECTED. 
	--	Region	ClubStatus	ActualAmount
-----Ohio Booth 	Open	1261641.960000
----=======================================================

 --temp table for Status SSSG Calcs
IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGStatusRevenueSummary', 'U') IS NOT NULL 
	DROP TABLE #PriorYearSSSGStatusRevenueSummary;
--Status SSSG Calcs
SELECT 
	   DimLocation.ClubStatus,
	   SUM(ActualAmount) ActualAmount
INTO #PriorYearSSSGStatusRevenueSummary 
FROM #PriorYearRevenue 
JOIN #Clubs DimLocation
	ON #PriorYearRevenue.DimClubKey = DimLocation.DimClubKey
	AND DimLocation.club_open_dim_date_key <= @SSSGGrandOpeningDeadlineDimDateKey
GROUP BY DimLocation.ClubStatus
	

IF OBJECT_ID('tempdb.dbo.#PromptYearSSSGStatusRevenueSummary', 'U') IS NOT NULL
	DROP TABLE #PromptYearSSSGStatusRevenueSummary;
SELECT DimLocation.ClubStatus,
	   SUM(ActualAmount) ActualAmount
INTO #PromptYearSSSGStatusRevenueSummary
FROM #PromptYearRevenue
JOIN #Clubs DimLocation
	ON #PromptYearRevenue.DimClubKey = DimLocation.DimClubKey
	AND DimLocation.Club_Open_Dim_Date_Key <= @SSSGGrandOpeningDeadlineDimDateKey
GROUP BY DimLocation.ClubStatus



IF OBJECT_ID('tempdb.dbo.#PriorYearSSSGReportRevenueSummary', 'U') IS NOT NULL
DROP TABLE #PriorYearSSSGReportRevenueSummary;

--Report SSSG calcs
SELECT SUM(ActualAmount) ActualAmount
  INTO #PriorYearSSSGReportRevenueSummary   
  FROM #PriorYearRevenue
 --select count(*) FROM #PriorYearRevenue where revenuereportingdepartmentname is not null = 214260 where as null will be 5M close to 6M
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
	   #PriorYearSSSGStatusRevenueSummary.ActualAmount PriorYearStatusActualAmount,
	   #PromptYearSSSGStatusRevenueSummary.ActualAmount PromptYearStatusActualAmount,
       #PriorYearSSSGReportRevenueSummary.ActualAmount PriorYearReportActualAmount,
       #PromptYearSSSGReportRevenueSummary.ActualAmount PromptYearReportActualAmount 
  INTO  #SSSGSummary    
  FROM #Clubs DimLocation
  LEFT JOIN #PriorYearSSSGClubRevenueSummary
    ON DimLocation.DimClubKey = #PriorYearSSSGClubRevenueSummary.DimClubKey
  LEFT JOIN #PromptYearSSSGClubRevenueSummary 
    ON DimLocation.DimClubKey = #PromptYearSSSGClubRevenueSummary.DimClubKey
  LEFT JOIN #PriorYearSSSGRegionRevenueSummary 
    ON DimLocation.Region = #PriorYearSSSGRegionRevenueSummary.Region
	AND DimLocation.ClubStatus = #PriorYearSSSGRegionRevenueSummary.ClubStatus
  LEFT JOIN #PromptYearSSSGRegionRevenueSummary 
    ON DimLocation.Region = #PromptYearSSSGRegionRevenueSummary.Region
	AND DimLocation.ClubStatus = #PromptYearSSSGRegionRevenueSummary.ClubStatus
  LEFT JOIN #PriorYearSSSGStatusRevenueSummary
	ON DimLocation.ClubStatus = #PriorYearSSSGStatusRevenueSummary.ClubStatus
  LEFT JOIN #PromptYearSSSGStatusRevenueSummary 
	ON DimLocation.ClubStatus = #PromptYearSSSGStatusRevenueSummary.ClubStatus
 CROSS JOIN #PriorYearSSSGReportRevenueSummary
 CROSS JOIN #PromptYearSSSGReportRevenueSummary


IF OBJECT_ID('tempdb.dbo.#ClubGoal', 'U') IS NOT NULL
  DROP TABLE #ClubGoal;
  
SELECT FactRevenueGoal.dim_club_key AS DimClubKey,   
	   #DimReportingHierarchy.DepartmentName RevenueReportingDepartmentName,
	   #DimReportingHierarchy.ProductGroupName RevenueProductGroupName,
	   1 RevenueProductGroupSortOrder, --#DimReportingHierarchy.ProductGroupSortOrder
       SUM(FactRevenueGoal.Goal_Dollar_Amount) ClubGoalAmount
  INTO  #ClubGoal     
  FROM [marketing].[v_fact_revenue_goal] FactRevenueGoal
  JOIN #DimReportingHierarchy
	ON FactRevenueGoal.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  --JOIN #Clubs DimLocation
  --  ON FactRevenueGoal.dim_club_key = DimLocation.DimClubKey
 WHERE FactRevenueGoal.goal_effective_dim_date_key >= @StartMonthStartingDimDateKey
   AND FactRevenueGoal.goal_effective_dim_date_key <= @EndMonthStartingDimDateKey
 GROUP BY FactRevenueGoal.dim_club_key, 
          #DimReportingHierarchy.DepartmentName,
          #DimReportingHierarchy.ProductGroupName
		 -- RevenueProductGroupSortOrder
		  --#DimReportingHierarchy.ProductGroupSortOrder
 
IF OBJECT_ID('tempdb.dbo.#RevenueAndGoalSummary', 'U') IS NOT NULL
	DROP TABLE #RevenueAndGoalSummary;

SELECT CASE WHEN #ClubGoal.DimClubKey IS NULL THEN #ClubRevenueSummary.DimClubKey
			ELSE #ClubGoal.DimClubKey
       END DimClubKey,
	   CASE WHEN #ClubGoal.DimClubKey IS NULL THEN #ClubRevenueSummary.RevenueReportingDepartmentName
            ELSE #ClubGoal.RevenueReportingDepartmentName
       END RevenueReportingDepartmentName,
       CASE WHEN #ClubGoal.DimClubKey IS NULL THEN #ClubRevenueSummary.RevenueProductGroupName
            ELSE #ClubGoal.RevenueProductGroupName
       END RevenueProductGroupName,
       ISNULL(#ClubGoal.RevenueProductGroupSortOrder, #ClubRevenueSummary.RevenueProductGroupSortOrder) RevenueProductGroupSortOrder,
       ISNULL(#ClubRevenueSummary.ClubActualAmount,0) ActualAmount,
       ISNULL(#ClubGoal.ClubGoalAmount,0) GoalAmount
INTO #RevenueAndGoalSummary
FROM #ClubGoal
FULL OUTER JOIN #ClubRevenueSummary 
   ON #ClubGoal.DimClubKey = #ClubRevenueSummary.DimClubKey
   AND #ClubGoal.RevenueReportingDepartmentName = #ClubRevenueSummary.RevenueReportingDepartmentName
   AND #ClubGoal.RevenueProductGroupName = #ClubRevenueSummary.RevenueProductGroupName
   ----remove this when done testing 
 --Select * from #Clubgoal
 --select * from #clubrevenuesummary - there are null values in this table for both RevenueReportingDepartmentName and RevenueProductGroupName
 --select * from #RevenueAndGoalSummary
 ------------------------
--Recurrent Product Revenue -- edw 04-2012 qc#1748 c&D changes begin --carred note from lftdw        
IF OBJECT_ID('tempdb.dbo.#UnassessedRecurrentProductAssessmentDimDateKeys', 'U') IS NOT NULL
	DROP TABLE #UnassessedRecurrentProductAssessmentDimDateKeys;
SELECT Dim_Date_Key DimDateKey,
	   Day_Number_In_Month DayNumberInCalendarMonth,
	   Calendar_Date CalendarDate,
	   Year CalendarYear,
	   Four_Digit_year_Dash_two_digit_month FourDigitYearDashTwoDigitMonth

INTO #UnassessedRecurrentProductAssessmentDimDateKeys
FROM [marketing].[v_dim_date] 
WHERE four_digit_year_dash_two_digit_month <= @EndFourDigitYearDashTwoDigitMonth
	AND calendar_date >= CONVERT(Datetime,Convert(Varchar,GetDate(),101), 101)


	----Create tmp table for #RecurrentProductRevenue
IF OBJECT_ID('tempdb.dbo.#RecurrentProductRevenue', 'U') IS NOT NULL
	DROP TABLE #RecurrentProductRevenue;

SELECT FactMembershipRecurrentProduct.Dim_Club_key DimClubKey,
       DimReportingHierarchy.[reporting_department] RevenueReportingDepartmentName, --DepartmentName RevenueReportingDepartmentName,
       DimReportingHierarchy.[reporting_product_group] RevenueProductGroupName, --ProductGroupName RevenueProductGroupName,
       1 AS RevenueProductGroupSortOrder, -- DimReportingHierarchy.ProductGroupSortOrder RevenueProductGroupSortOrder,
	   SUM(CONVERT(Decimal(14,2),FactMembershipRecurrentProduct.Price) * CASE WHEN DimRevenueAllocationRule.[revenue_allocation_rule_name] = 'Sale Month Activity' 
	                                                                                       THEN CONVERT(Decimal(14,2),DimRevenueAllocationRule.Accumulated_Ratio) 
																			      ELSE CONVERT(Decimal(14,2),DimRevenueALlocationRule.Ratio)
																			  END) ForecastedRecurrentProductRevenue


  INTO  #RecurrentProductRevenue
  FROM [marketing].[v_fact_mms_membership_recurrent_product] FactMembershipRecurrentProduct --vFactMembershipRecurrentProductActive FactMembershipRecurrentProduct
  JOIN #UnassessedRecurrentProductAssessmentDimDateKeys
    ON FactMembershipRecurrentProduct.Assessment_Day_Of_Month = #UnassessedRecurrentProductAssessmentDimDateKeys.DayNumberInCalendarMonth
	
  JOIN [marketing].[v_dim_mms_product] DimProduct --vDimProductActive DimProduct
    ON FactMembershipRecurrentProduct.dim_mms_product_key = DimProduct.dim_mms_product_key --ON FactMembershipRecurrentProduct.DimProductKey = DimProduct.DimProductKey
  JOIN [marketing].[v_dim_reporting_hierarchy] DimReportingHierarchy
    ON DimProduct.Dim_Reporting_Hierarchy_Key = DimReportingHierarchy.Dim_Reporting_Hierarchy_Key
  JOIN #DimReportingHierarchy
    ON DimReportingHierarchy.Dim_Reporting_Hierarchy_Key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #Clubs DimLocation
   ON FactMembershipRecurrentProduct.Dim_Club_Key = DimLocation.DimClubKey
  LEFT JOIN [marketing].[v_dim_date] HoldStartDimDate
    ON HoldStartDimDate.Dim_Date_Key = FactMembershipRecurrentProduct.Hold_Start_Dim_Date_Key
	--=====================================verify with susan on > 3 logic
	--If am doing left join what use would it be and what does > 3 mean in this logic.
	--====================================Ends here===========================

   AND HoldStartDimDate.Dim_Date_Key > 3
  LEFT JOIN [marketing].[v_dim_date] HoldEndDimDate --vDimDate HoldEndDimDate
    ON HoldEndDimDate.Dim_Date_Key = FactMembershipRecurrentProduct.Hold_End_Dim_Date_Key
   AND HoldEndDimDate.Dim_Date_Key > 3
  LEFT JOIN [marketing].[v_dim_date] TerminationDimDate
    ON TerminationDimDate.Dim_Date_Key = FactMembershipRecurrentProduct.Termination_Dim_Date_Key
   AND TerminationDimDate.Dim_Date_Key > 3  
  JOIN [marketing].[v_dim_revenue_allocation_rule] DimRevenueAllocationRule
    ON DimProduct.allocation_rule = DimRevenueAllocationRule.revenue_allocation_rule_name
   AND DimRevenueAllocationRule.[revenue_from_late_transaction_flag] = 'N'
   AND #UnassessedRecurrentProductAssessmentDimDateKeys.DimDateKey between DimRevenueAllocationRule.[earliest_transaction_dim_date_key] and DimRevenueALlocationRule.[latest_transaction_dim_date_key]
  JOIN [marketing].[v_dim_date] RuleRevenuePostingMonthStartingDimDate
    ON DimRevenueALlocationRule.[revenue_posting_month_starting_dim_date_key] = RuleRevenuePostingMonthStartingDimDate.Dim_Date_Key
	--continue from here
 WHERE FactMembershipRecurrentProduct.Activation_Dim_Date_Key <= #UnassessedRecurrentProductAssessmentDimDateKeys.DimDateKey
   AND ISNULL(TerminationDimDate.Dim_Date_Key,99991231) > #UnassessedRecurrentProductAssessmentDimDateKeys.DimDateKey
   AND #UnassessedRecurrentProductAssessmentDimDateKeys.DimDateKey NOT BETWEEN ISNULL(HoldStartDimDate.Dim_Date_Key,99991231) AND ISNULL(HoldEndDimDate.Dim_Date_Key,99991231)
   AND RuleRevenuePostingMonthStartingDimDate.Four_Digit_Year_Dash_Two_Digit_Month >= @StartFourDigitYearDashTwoDigitMonth
   AND RuleRevenuePostingMonthStartingDimDate.Four_Digit_Year_Dash_Two_Digit_Month <= @EndFourDigitYearDashTwoDigitMonth
 GROUP BY FactMembershipRecurrentProduct.Dim_Club_Key,
          DimReportingHierarchy.[reporting_department],    --Department_Name,
          DimReportingHierarchy.[reporting_product_group]  --ProductGroupName
        --  DimReportingHierarchy.ProductGroupSortOrder 
		  ----end here ----
		  

IF OBJECT_ID('tempdb.dbo.#RecurrentRevenueAndGoalSummary', 'U') IS NOT NULL
	DROP TABLE #RecurrentRevenueAndGoalSummary;
	--Returns the specified expression value otherwise the actual value 
SELECT ISNULL(#RevenueAndGoalSummary.DimClubKey,#RecurrentProductRevenue.DimClubKey) DimClubKey,
       ISNULL(#RevenueAndGoalSummary.RevenueReportingDepartmentName,#RecurrentProductRevenue.RevenueReportingDepartmentName) RevenueReportingDepartmentName,
       ISNULL(#RevenueAndGoalSummary.RevenueProductGroupName,#RecurrentProductRevenue.RevenueProductGroupName) RevenueProductGroupName,
       ISNULL(#RevenueAndGoalSummary.RevenueProductGroupSortOrder, #RecurrentProductRevenue.RevenueProductGroupSortOrder) RevenueProductGroupSortOrder,
       ISNULL(#RevenueAndGoalSummary.ActualAmount,0) ActualAmount,
       ISNULL(#RevenueAndGoalSummary.GoalAmount,0) GoalAmount,
       ISNULL(CONVERT(Decimal(14,6),#RecurrentProductRevenue.ForecastedRecurrentProductRevenue),0) ForecastedRecurrentProductRevenue
  INTO #RecurrentRevenueAndGoalSummary
  FROM #RevenueAndGoalSummary
  FULL OUTER JOIN #RecurrentProductRevenue
    ON #RevenueAndGoalSummary.DimClubKey = #RecurrentProductRevenue.DimClubKey
   AND #RevenueAndGoalSummary.RevenueReportingDepartmentName = #RecurrentProductRevenue.RevenueReportingDepartmentName
   AND #RevenueAndGoalSummary.RevenueProductGroupName = #RecurrentProductRevenue.RevenueProductGroupName

   
---==============RevenueAndGoalSummary Full OuterJOIN RecurrentProductRevenue--==========
   --select * from #revenueandgoalsummary	   = 10568 records return
   --select * from #recurrentproductrevenue    = empty 

   ---INSERT #RecurrentRevenueAndGoalSummary - Select * from #RecurrentRevenueandgoalsummary = 10568 
					--Nothing from ForecastedRecurrentProductRevenue column
					--Due to #RecurrentProductRevenue being empty 
					--Revisit if expected data on the ForecastedRevenueProductRevenue

---==============End==========================================================

IF OBJECT_ID('tempdb.dbo.#RecurrentRevenueAndGoalSummaryTotal', 'U') IS NOT NULL 
	DROP TABLE #RecurrentRevenueAndGoalSummaryTotal;

SELECT DimLocation.DimClubKey,
       SUM(#RecurrentRevenueAndGoalSummary.ActualAmount) ActualAmount,
       SUM(#RecurrentRevenueAndGoalSummary.GoalAmount) GoalAmount,
       SUM(#RecurrentRevenueAndGoalSummary.ForecastedRecurrentProductRevenue) ForecastedRecurrentProductRevenue
  INTO #RecurrentRevenueAndGoalSummaryTotal
  FROM #RecurrentRevenueAndGoalSummary
  JOIN #Clubs DimLocation
    ON #RecurrentRevenueAndGoalSummary.DimClubKey = DimLocation.DimClubKey
 GROUP BY DimLocation.DimClubKey 
HAVING Sum(#RecurrentRevenueAndGoalSummary.ActualAmount) <> 0
    OR SUM(#RecurrentRevenueAndGoalSummary.GoalAmount) <> 0
    OR SUM(#RecurrentRevenueAndGoalSummary.ForecastedRecurrentProductRevenue) <> 0 --This currently holds 0.00 value

	

IF OBJECT_ID('tempdb.dbo.#SummaryByClub', 'U') IS NOT NULL 
	DROP TABLE #SummaryByClub;

SELECT #RecurrentRevenueAndGoalSummaryTotal.DimClubKey,
       Sum(#RecurrentRevenueAndGoalSummaryTotal.GoalAmount) ClubGoalAmount,
       Sum(#RecurrentRevenueAndGoalSummaryTotal.ActualAmount) ClubActualAmount,
       SUM(#RecurrentRevenueAndGoalSummaryTotal.ForecastedRecurrentProductRevenue) ClubForecastedRecurrentProductRevenue
  INTO #SummaryByClub
  FROM #RecurrentRevenueAndGoalSummaryTotal
 GROUP BY #RecurrentRevenueAndGoalSummaryTotal.DimClubKey
 

IF OBJECT_ID('tempdb.dbo.#SummaryByRegion', 'U') IS NOT NULL
	DROP TABLE #SummaryByRegion;

SELECT DimLocation.Region,
       DimLocation.ClubStatus,
       Sum(GoalAmount) RegionGoalAmount,
       Sum(ActualAmount) RegionActualAmount,
       SUM(#RecurrentRevenueAndGoalSummaryTotal.ForecastedRecurrentProductRevenue) RegionForecastedRecurrentProductRevenue
  INTO #SummaryByRegion
  FROM #RecurrentRevenueAndGoalSummaryTotal
  JOIN #Clubs DimLocation 
    ON #RecurrentRevenueAndGoalSummaryTotal.DimClubKey = DimLocation.DimClubKey
 GROUP BY DimLocation.Region,
          DimLocation.ClubStatus
	
	------------==================================---------------------	  
IF OBJECT_ID('tempdb.dbo.#SummaryByClubStatus', 'U') IS NOT NULL
	DROP TABLE #SummaryByClubStatus;

SELECT DimLocation.ClubStatus,
       Sum(GoalAmount) ClubStatusGoalAmount,
       Sum(ActualAmount) ClubStatusActualAmount,
       SUM(#RecurrentRevenueAndGoalSummaryTotal.ForecastedRecurrentProductRevenue) ClubStatusForecastedRecurrentProductRevenue
  INTO #SummaryByClubStatus
  FROM #RecurrentRevenueAndGoalSummaryTotal
  JOIN #Clubs DimLocation 
    ON #RecurrentRevenueAndGoalSummaryTotal.DimclubKey = DimLocation.DimClubKey
 GROUP BY DimLocation.ClubStatus
  

IF OBJECT_ID('tempdb.dbo.#SummaryByReport', 'U') IS NOT NULL 
	DROP TABLE #SummaryByReport;

SElECT Sum(GoalAmount) ReportGoalAmount,
       Sum(ActualAmount) ReportActualAmount,
       SUM(#RecurrentRevenueAndGoalSummaryTotal.ForecastedRecurrentProductRevenue) ReportForecastedRecurrentProductRevenue
  INTO  #SummaryByReport
  FROM #RecurrentRevenueAndGoalSummaryTotal
  JOIN #Clubs DimLocation 
    ON #RecurrentRevenueAndGoalSummaryTotal.DimClubkey = DimLocation.DimClubKey



	
  --Result set
SELECT DimLocation.ClubStatus + ' Clubs' ClubStatus,
       DimLocation.Region,
       CASE WHEN DimLocation.ClubCode = '' THEN DimLocation.ClubName ELSE DimLocation.ClubCode END ClubCode,
       2 DepartmentSortOrder,
       #RecurrentRevenueAndGoalSummary.RevenueReportingDepartmentName,
       #RecurrentRevenueAndGoalSummary.RevenueProductGroupName,
      #RecurrentRevenueAndGoalSummary.RevenueProductGroupSortOrder,
       'Local' CurrencyCode,
       #RecurrentRevenueAndGoalSummary.ActualAmount,
       #RecurrentRevenueAndGoalSummary.GoalAmount,
       @RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,
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
       @StartFourDigitYearDashTwoDigitMonth + ' through ' + @EndFourDigitYearDashTwoDigitMonth HeaderYearMonthRange,
       #RecurrentRevenueAndGoalSummary.ForecastedRecurrentProductRevenue ForecastedRecurrentProductRevenue,
       Left(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount + #SummaryByClub.ClubForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount + #SummaryByClub.ClubForecastedRecurrentProductRevenue,0) as Money),1))-3) ClubTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount + #SummaryByRegion.RegionForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount + #SummaryByRegion.RegionForecastedRecurrentProductRevenue,0) as Money),1))-3) RegionTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount + #SummaryByClubStatus.ClubStatusForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount + #SummaryByClubStatus.ClubStatusForecastedRecurrentProductRevenue,0) as Money),1))-3) StatusTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount + #SummaryByReport.ReportForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount + #SummaryByReport.ReportForecastedRecurrentProductRevenue,0) as Money),1))-3) ReportTotalForecastedActual,
       @HeaderDivisionList HeaderDivisionList,
       @HeaderSubdivisionList HeaderSubdivisionList,
       @RevenueProductGroupNameCommaList AS RevenueProductGroupNameCommaList
  FROM #RecurrentRevenueAndGoalSummary
  JOIN #Clubs DimLocation
    ON #RecurrentRevenueAndGoalSummary.DimClubKey = DimLocation.DimClubKey
  JOIN #SummaryByClub 
    ON #RecurrentRevenueAndGoalSummary.DimClubKey = #SummaryByClub.DimClubKey
  JOIN #SummaryByRegion 
    ON DimLocation.Region = #SummaryByRegion.Region
   AND DimLocation.ClubStatus = #SummaryByRegion.ClubStatus
  JOIN #SummaryByClubStatus 
    ON DimLocation.ClubStatus = #SummaryByClubStatus.ClubStatus
  LEFT JOIN #SSSGSummary 
    ON DimLocation.DimClubKey = #SSSGSummary.DimClubKey
 CROSS JOIN #SummaryByReport
 WHERE #RecurrentRevenueAndGoalSummary.ActualAmount <> 0
    OR #RecurrentRevenueAndGoalSummary.GoalAmount <> 0
    OR #RecurrentRevenueAndGoalSummary.ForecastedRecurrentProductRevenue <> 0
UNION
SELECT DimLocation.ClubStatus + ' Clubs' ClubStatus,
       DimLocation.Region,
       CASE WHEN DimLocation.ClubCode = '' THEN DimLocation.ClubName ELSE DimLocation.ClubCode END ClubCode,
       1 DepartmentSortOrder,
       '' RevenueReportingDepartmentName,
       'Total' RevenueProductGroupName,
       1 RevenueProductGroupSortOrder,
       'Local' CurrencyCode,
       #SummaryByClub.ClubActualAmount ActualAmount,
       #SummaryByClub.ClubGoalAmount GoalAmount,
       @RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,
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
       @StartFourDigitYearDashTwoDigitMonth + ' through ' + @EndFourDigitYearDashTwoDigitMonth HeaderYearMonthRange,
       #SummaryByClub.ClubForecastedRecurrentProductRevenue ForecastedRecurrentProductRevenue,
       Left(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount + #SummaryByClub.ClubForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClub.ClubActualAmount + #SummaryByClub.ClubForecastedRecurrentProductRevenue,0) as Money),1))-3) ClubTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount + #SummaryByRegion.RegionForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByRegion.RegionActualAmount + #SummaryByRegion.RegionForecastedRecurrentProductRevenue,0) as Money),1))-3) RegionTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount + #SummaryByClubStatus.ClubStatusForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByClubStatus.ClubStatusActualAmount + #SummaryByClubStatus.ClubStatusForecastedRecurrentProductRevenue,0) as Money),1))-3) StatusTotalForecastedActual,
       Left(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount + #SummaryByReport.ReportForecastedRecurrentProductRevenue,0) as Money),1),Len(Convert(Varchar,Cast(Round(#SummaryByReport.ReportActualAmount + #SummaryByReport.ReportForecastedRecurrentProductRevenue,0) as Money),1))-3) ReportTotalForecastedActual,
       @HeaderDivisionList HeaderDivisionList,
       @HeaderSubdivisionList HeaderSubdivisionList,
       @RevenueProductGroupNameCommaList AS RevenueProductGroupNameCommaList
  FROM #SummaryByClub
  JOIN #Clubs DimLocation 
    ON #SummaryByClub.DimClubKey = DimLocation.DimClubKey
  JOIN #SummaryByRegion 
    ON DimLocation.Region = #SummaryByRegion.Region
   AND DimLocation.ClubStatus = #SummaryByRegion.ClubStatus
  JOIN #SummaryByClubStatus 
    ON DimLocation.ClubStatus = #SummaryByClubStatus.ClubStatus
  LEFT JOIN #SSSGSummary 
    ON DimLocation.DimClubKey = #SSSGSummary.DimClubKey
 CROSS JOIN #SummaryByReport
 ORDER BY ClubStatus,
          Region,
          ClubCode,
          RevenueReportingDepartmentName,
          RevenueProductGroupSortOrder




DROP TABLE #UnassessedRecurrentProductAssessmentDimDateKeys
DROP TABLE #Clubs --#DimLocation
DROP TABLE #SummaryByClub
DROP TABLE #SummaryByRegion
DROP TABLE #SummaryByClubStatus
DROP TABLE #SummaryByReport
DROP TABLE #RecurrentRevenueAndGoalSummary
DROP TABLE #RecurrentRevenueAndGoalSummaryTotal
DROP TABLE #RevenueAndGoalSummary
DROP TABLE #ClubRevenueSummary
DROP TABLE #PriorYearSSSGClubRevenueSummary
DROP TABLE #PromptYearSSSGClubRevenueSummary
DROP TABLE #PriorYearSSSGRegionRevenueSummary
DROP TABLE #PromptYearSSSGRegionRevenueSummary
DROP TABLE #PriorYearSSSGStatusRevenueSummary
DROP TABLE #PromptYearSSSGStatusRevenueSummary
DROP TABLE #PriorYearSSSGReportRevenueSummary
DROP TABLE #PromptYearSSSGReportRevenueSummary
DROP TABLE #SSSGSummary
DROP TABLE #ClubGoal
DROP TABLE #RecurrentProductRevenue
DROP TABLE #DimReportingHierarchy
DROP TABLE #PriorYearRevenue
DROP TABLE #PromptYearRevenue

END

