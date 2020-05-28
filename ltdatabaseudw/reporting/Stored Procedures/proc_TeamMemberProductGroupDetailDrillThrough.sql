CREATE PROC [reporting].[proc_TeamMemberProductGroupDetailDrillThrough] @StartFourDigitYearDashTwoDigitMonth [CHAR](7),@EndFourDigitYearDashTwoDigitMonth [CHAR](7),@DimReportingHierarchyKeyList [VARCHAR](8000),@SalesSourceList [VARCHAR](4000),@CommissionTypeList [VARCHAR](4000),@RevenueReportingRegion [VARCHAR](50),@DimMMSClubIDList [VARCHAR](4000),@ReportingCurrencyCode [VARCHAR](15) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

--DECLARE  @StartFourDigitYearDashTwoDigitMonth CHAR(7) = '2019-05'
--DECLARE  @EndFourDigitYearDashTwoDigitMonth CHAR(7) = '2019-05'
--DECLARE  @DimReportingHierarchyKeyList VARCHAR(8000) = 'All Product Groups'  ------'BF0B84794811114DFD2E94BA687F43C2'
--DECLARE  @SalesSourceList VARCHAR(4000) = 'MMS|Cafe|Hybris|Magento'
--DECLARE  @CommissionTypeList VARCHAR(4000) = 'Commissioned|Non-Commissioned'
--DECLARE  @RevenueReportingRegion VARCHAR(50) = 'Hall-MN-West'
--DECLARE  @DimMMSClubIDList VARCHAR(4000) = '151'  
--DECLARE  @ReportingCurrencyCode VARCHAR(15) = 'USD'


DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                          from map_utc_time_zone_conversion where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

  
DECLARE @StartMonthStartingDimDateKey INT
SELECT @StartMonthStartingDimDateKey = month_starting_dim_date_key
FROM [marketing].[v_dim_date]
WHERE four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth 
  
  
DECLARE @EndMonthStartingDimDateKey Datetime,
        @EndMonthEndingDimDateKey INT,
		@CalendarMonthEndingdate DateTime

SELECT @EndMonthStartingDimDateKey = month_starting_dim_date_key,
       @EndMonthEndingDimDateKey = month_ending_dim_date_key,
	   @CalendarMonthEndingdate = month_ending_date
FROM [marketing].[v_dim_date]
WHERE four_digit_year_dash_two_digit_month = @EndFourDigitYearDashTwoDigitMonth


IF OBJECT_ID('tempdb.dbo.#SalesSourceList', 'U') IS NOT NULL
DROP TABLE #SalesSourceList; 	

exec [marketing].[proc_parse_pipe_list] @SalesSourceList, 'SalesSourceList'


IF OBJECT_ID('tempdb.dbo.#TempCommissionTypeList', 'U') IS NOT NULL
DROP TABLE #TempCommissionTypeList; 	

exec [marketing].[proc_parse_pipe_list] @CommissionTypeList, 'TempCommissionTypeList'

IF OBJECT_ID('tempdb.dbo.#CommissionTypeList', 'U') IS NOT NULL
DROP TABLE #CommissionTypeList;

SELECT temp.Item AS CommissionType, 
       CASE WHEN temp.Item = 'Commissioned' THEN 'Y' ELSE 'N' END AS CommissionedSalesTransactionFlag
  INTO #CommissionTypeList
  FROM #TempCommissionTypeList temp 
  
DECLARE @SalesSourceCommaList VARCHAR(4000),
        @CommissionTypeCommaList VARCHAR(4000)

SET @SalesSourceCommaList = REPLACE(@SalesSourceList,'|',', ')
SET @CommissionTypeCommaList = REPLACE(@CommissionTypeList,'|',', ')


Exec [reporting].[proc_DimReportingHierarchy_History] 'N/A', 'N/A', 'N/A',@DimReportingHierarchyKeyList,@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey 
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
 DROP TABLE #DimReportingHierarchy; 

 SELECT DimReportingHierarchyKey,
       DivisionName,
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   ReportRegionType,
	   CASE WHEN ProductGroupName IN('Weight Loss Challenges','90 Day Weight Loss')
	        THEN 'Y'
		    ELSE 'N'
		END PTDeferredRevenueProductGroupFlag
 INTO #DimReportingHierarchy
 FROM #OuterOutputTable
  
 
DECLARE @HeaderDivisionList VARCHAR(8000),
        @HeaderSubdivisionList VARCHAR(8000),
        @RevenueReportingDepartmentNameCommaList VARCHAR(8000),
        @RevenueProductGroupNameCommaList VARCHAR(8000),
        @RegionType VARCHAR(50)

SELECT @HeaderDivisionList = (SELECT MIN(DivisionName) FROM #DimReportingHierarchy),
       @HeaderSubdivisionList = (SELECT MIN(SubdivisionName) FROM #DimReportingHierarchy),
       @RevenueReportingDepartmentNameCommaList = (SELECT MIN(DepartmentName) FROM #DimReportingHierarchy),
       @RevenueProductGroupNameCommaList = (SELECT MIN(ProductGroupName) FROM #DimReportingHierarchy),
       @RegionType = (SELECT MIN(ReportRegionType) FROM #DimReportingHierarchy)

	   
  ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month
DECLARE @FirstOfReportRangeDimDateKey INT
DECLARE @EndOfReportRangeDimDateKey INT
DECLARE @StartDimDateKey INT
---DECLARE @StartDate DATETIME

SET @FirstOfReportRangeDimDateKey = (SELECT MIN(dim_date_key) FROM [marketing].[v_dim_date] WHERE four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth )
SET @EndOfReportRangeDimDateKey = (SELECT MAX(dim_date_key) FROM [marketing].[v_dim_date] WHERE four_digit_year_dash_two_digit_month = @EndFourDigitYearDashTwoDigitMonth ) 
---SET @StartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)


DECLARE @EComm60DayChallengeRevenueStartMonthStartDimDateKey INT
  ---- When the requested month is the 2nd month of the quarter, set the start date to the prior month
SET @EComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT CASE WHEN (Select Month_Number_In_Year 
                    From [marketing].[v_dim_date]
				   Where Dim_Date_Key = @FirstOfReportRangeDimDateKey) in (2,5,8,11)
			THEN (Select Prior_Month_Starting_Dim_Date_Key
			        From [marketing].[v_dim_date]
			        WHERE Dim_Date_Key = @FirstOfReportRangeDimDateKey)
            ELSE (Select Month_Starting_Dim_Date_Key
                    From [marketing].[v_dim_date] 
				   Where Dim_Date_Key = @FirstOfReportRangeDimDateKey)
			END 
            From [marketing].[v_dim_date]
            WHERE Dim_Date_Key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record			
			

			
DECLARE @EComm60DayChallengeRevenueEndMonthEndDimDateKey INT
  ---- When the requested month is the 1st month of the quarter, set the end date to the prior month
SET @EComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT CASE WHEN (Select Month_Number_In_Year 
                    From [marketing].[v_dim_date]
				   Where Dim_Date_Key = @EndOfReportRangeDimDateKey) in (2,5,8,11)
			THEN (Select Prior_Month_Starting_Dim_Date_Key
			        From [marketing].[v_dim_date]
			        WHERE Dim_Date_Key = @EndOfReportRangeDimDateKey)
            ELSE (Select Month_Starting_Dim_Date_Key
                    From [marketing].[v_dim_date] 
				   Where Dim_Date_Key = @EndOfReportRangeDimDateKey)
			END 
            From [marketing].[v_dim_date]
            WHERE Dim_Date_Key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record			
						

IF OBJECT_ID('tempdb.dbo.#DimMMSClubIDList', 'U') IS NOT NULL
DROP TABLE #DimMMSClubIDList; 	

EXEC [marketing].[proc_parse_pipe_list] @DimMMSClubIDList, 'DimMMSClubIDList' 

IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL  
DROP TABLE #Clubs;

	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_name AS ClubName,
       DimClub.club_code AS ClubCode,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #DimMMSClubIDList DimMMSClubIDList
    ON DimMMSClubIDList.Item = DimClub.club_id
	  OR DimMMSClubIDList.Item = -1
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key in('-997','-998','-999') OR DimClub.club_close_dim_date_key > @StartMonthStartingDimDateKey)  
GROUP BY DimClub.dim_club_key, 
       DimClub.club_name,
       DimClub.club_code,
	   MMSRegion.description,
	   PTRCLRegion.description,
	   MemberActivitiesRegion.description

IF OBJECT_ID('tempdb.dbo.#DimLocationInfo', 'U') IS NOT NULL
  DROP TABLE #DimLocationInfo;

  ----- Create Region temp table
DECLARE @list_table VARCHAR(100)
DECLARE @RegionList VARCHAR(4000)

SET @list_table = 'region_list'
SET @RegionList = @RevenueReportingRegion

EXEC marketing.proc_parse_pipe_list @RegionList,@list_table
	
SELECT DimClub.DimClubKey,      
      CASE WHEN @RegionType = 'PT RCL Area' 
             THEN DimClub.PTRCLRegion
           WHEN @RegionType = 'Member Activities Region' 
             THEN DimClub.MemberActivitiesRegion
           WHEN @RegionTYpe = 'MMS Region' 
             THEN DimClub.MMSRegion 
		   END  Region,
       DimClub.ClubName AS ClubName,
	   DimClub.ClubCode AS ClubCode
 INTO #DimLocationInfo    
  FROM #Clubs DimClub     
  LEFT JOIN #region_list RegionList 
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
	   DimClub.ClubCode
	

IF OBJECT_ID('tempdb.dbo.#Summary', 'U') IS NOT NULL
DROP TABLE #Summary; 
 
SELECT    ---FactAllocatedTransaction.sales_source AS SalesSource,
          DimLocation.Region,
          DimLocation.ClubCode,
          DimLocation.ClubName,
		  CASE WHEN IsNull(PrimarySalesDimEmployee.dim_employee_key,'-998') in('-997', '-998','-999')
                    THEN 'None Designated'
               ELSE IsNull(PrimarySalesDimEmployee.employee_name_last_first,'None Designated')
			   END TeamMember,
          FactAllocatedTransaction.allocated_amount as RevenueAmount,	  
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
          CASE WHEN IsNull(PrimarySalesDimEmployee.dim_employee_key,'-998') in('-997', '-998','-999')
		            THEN NULL
               ELSE PrimarySalesDimEmployee.employee_id 
			   END EmployeeID,
          0 as RevenueProductGroupSortOrder ,
		 FactAllocatedTransaction.dim_reporting_hierarchy_key AS  DimReportingHierarchyKey
	INTO #Summary
   FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedTransaction
   LEFT JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct
     ON FactAllocatedTransaction.dim_product_key = DimCafeProduct.dim_cafe_product_key
	   AND FactAllocatedTransaction.sales_source = 'Cafe'
	   AND DimCafeProduct.effective_date_time <= @CalendarMonthEndingdate
	   AND DimCafeProduct.expiration_date_time > @CalendarMonthEndingdate
   LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactAllocatedTransaction.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactAllocatedTransaction.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time <= @CalendarMonthEndingdate
	   AND DimHybrisProduct.expiration_date_time > @CalendarMonthEndingdate
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactAllocatedTransaction.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time <= @CalendarMonthEndingdate
	   AND DimHealthCheckUSAProduct.expiration_date_time > @CalendarMonthEndingdate
   LEFT JOIN [marketing].[v_dim_mms_product_history] DimMMSProduct
     ON FactAllocatedTransaction.dim_product_key = DimMMSProduct.dim_mms_product_key
	   AND FactAllocatedTransaction.sales_source = 'MMS'
	   AND DimMMSProduct.effective_date_time <= @CalendarMonthEndingdate
	   AND DimMMSProduct.expiration_date_time > @CalendarMonthEndingdate
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactAllocatedTransaction.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactAllocatedTransaction.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time <= @CalendarMonthEndingdate
	   AND DimMagentoProduct.expiration_date_time > @CalendarMonthEndingdate
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Cafe
     ON DimCafeProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Cafe.DimReportingHierarchyKey
	 ------AND DimReportingHierarchy_Cafe.PTDeferredRevenueProductGroupFlag = 'N'       --- this was eliminating any 60 Day challenge products sold through the cafe
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'N'   
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'N'   
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_MMS
     ON DimMMSProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_MMS.DimReportingHierarchyKey
	 ------AND DimReportingHierarchy_MMS.PTDeferredRevenueProductGroupFlag = 'N'   --- this was eliminating any 60 Day challenge products sold through MMS
   LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'N'   
   JOIN #DimLocationInfo  DimLocation
     ON FactAllocatedTransaction.allocated_dim_club_key = DimLocation.DimClubKey
   JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee
     ON FactAllocatedTransaction.primary_sales_dim_employee_key = PrimarySalesDimEmployee.dim_employee_key
   JOIN #SalesSourceList SalesSourceList
     ON FactAllocatedTransaction.sales_source = SalesSourceList.item
   JOIN #DimReportingHierarchy DimReportingHierarchy
	  ON FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
   WHERE  FactAllocatedTransaction.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
			   AND FactAllocatedTransaction.allocated_month_starting_dim_date_key <= @EndMonthEndingDimDateKey 
 AND (FactAllocatedTransaction.sales_source in('MMS','Cafe')
       OR DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'N' )                                   ------- need this to prevent duplicate e-comm transactions from coming through from unioned query
	
UNION ALL


  SELECT ---FactAllocatedTransaction.sales_source AS SalesSource,
          DimLocation.Region,
          DimLocation.ClubCode,
          DimLocation.ClubName,
		  CASE WHEN IsNull(PrimarySalesDimEmployee.dim_employee_key,'-998') in('-997', '-998','-999')
            THEN 'None Designated'
             ELSE IsNull(PrimarySalesDimEmployee.employee_name_last_first,'None Designated')
			 END TeamMember,
          FactAllocatedTransaction.allocated_amount AS RevenueAmount,
		 CASE  WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,
		 CASE  WHEN FactAllocatedTransaction.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			   WHEN FactAllocatedTransaction.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.ProductGroupName
			   END  RevenueProductGroupName,    
		CASE WHEN IsNull(PrimarySalesDimEmployee.dim_employee_key,'-998') in('-997', '-998','-999') 
		         THEN NULL
               ELSE PrimarySalesDimEmployee.employee_id 
			   END EmployeeID,	   
		 0 as RevenueProductGroupSortOrder,
		 FactAllocatedTransaction.dim_reporting_hierarchy_key AS  DimReportingHierarchyKey
   FROM [marketing].[v_fact_combined_allocated_transaction_item] FactAllocatedTransaction
   LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactAllocatedTransaction.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactAllocatedTransaction.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time <= @CalendarMonthEndingdate
	   AND DimHybrisProduct.expiration_date_time > @CalendarMonthEndingdate
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactAllocatedTransaction.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactAllocatedTransaction.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time <= @CalendarMonthEndingdate
	   AND DimHealthCheckUSAProduct.expiration_date_time > @CalendarMonthEndingdate
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactAllocatedTransaction.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactAllocatedTransaction.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time <= @CalendarMonthEndingdate
	   AND DimMagentoProduct.expiration_date_time > @CalendarMonthEndingdate
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
   JOIN [marketing].[v_dim_employee] PrimarySalesDimEmployee
     ON FactAllocatedTransaction.primary_sales_dim_employee_key = PrimarySalesDimEmployee.dim_employee_key
   JOIN #SalesSourceList SalesSourceList
     ON FactAllocatedTransaction.sales_source = SalesSourceList.item
   JOIN #DimReportingHierarchy DimReportingHierarchy
	 ON FactAllocatedTransaction.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
      AND DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'Y'                           ------- need this to prevent duplicate e-comm transactions from coming through from unioned query
   WHERE (FactAllocatedTransaction.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartMonthStartDimDateKey
          AND FactAllocatedTransaction.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndMonthEndDimDateKey)
		  AND FactAllocatedTransaction.sales_source in('Hybris','HealthCheckUSA','Magento')
		  
		  
--Result Set
SELECT #Summary.Region,
       CASE WHEN #Summary.ClubCode = '' THEN #Summary.ClubName ELSE #Summary.ClubCode END ClubCode,
       #Summary.TeamMember,
       @ReportingCurrencyCode CurrencyCode,
       SUM(#Summary.RevenueAmount) ActualRevenue,
       #Summary.RevenueProductGroupName,
       @SalesSourceCommaList SalesSourceCommaList,
       @CommissionTypeCommaList CommissionTypeCommaList,
       NULL RevenueReportingDepartmentNameCommaList,    --@RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,    ------ must build in Cognos
       ISNULL(#Summary.EmployeeID,'-998') as EmployeeID,
       #Summary.RevenueReportingDepartmentName,
       MAX(#Summary.RevenueProductGroupSortOrder) RevenueProductGroupSortOrder,
       @ReportRunDateTime ReportRunDateTime,
       NULL HeaderDivisionList,     ---@HeaderDivisionList HeaderDivisionList,------ must build in Cognos
       NULL HeaderSubdivisionList  -----@HeaderSubdivisionList HeaderSubdivisionList  ------ must build in Cognos
FROM #Summary 
  JOIN #DimReportingHierarchy Hierarchy
    ON #Summary.DimReportingHierarchyKey = Hierarchy.DimReportingHierarchyKey
GROUP BY #Summary.Region,
       #Summary.ClubCode,
       #Summary.ClubName,
       #Summary.TeamMember,
       #Summary.RevenueProductGroupName,
       ISNULL(#Summary.EmployeeID,'-998'),
       #Summary.RevenueReportingDepartmentName

UNION

SELECT #Summary.Region,
       CASE WHEN #Summary.ClubCode = '' THEN #Summary.ClubName ELSE #Summary.ClubCode END ClubCode,
       #Summary.TeamMember,
       @ReportingCurrencyCode CurrencyCode,
       Sum(#Summary.RevenueAmount) ActualRevenue,
       'Total' RevenueProductGroupName,
       @SalesSourceCommaList SalesSourceCommaList,
       @CommissionTypeCommaList CommissionTypeCommaList,
       NULL RevenueReportingDepartmentNameCommaList,    --@RevenueReportingDepartmentNameCommaList RevenueReportingDepartmentNameCommaList,    ------ must build in Cognos
       ISNULL(#Summary.EmployeeID,'-998') as EmployeeID,
       '' RevenueReportingDepartmentName,
       1 RevenueProductGroupSortOrder,
       @ReportRunDateTime ReportRunDateTime,
       NULL HeaderDivisionList,     ---@HeaderDivisionList HeaderDivisionList,------ must build in Cognos
       NULL HeaderSubdivisionList  -----@HeaderSubdivisionList HeaderSubdivisionList  ------ must build in Cognos
FROM #Summary 
  JOIN #DimReportingHierarchy Hierarchy
    ON #Summary.DimReportingHierarchyKey = Hierarchy.DimReportingHierarchyKey
GROUP BY #Summary.Region,
         #Summary.ClubCode,
         #Summary.ClubName,
         #Summary.TeamMember,
         ISNULL(#Summary.EmployeeID,'-998')
ORDER BY Region,
         ClubCode,
         TeamMember,
         EmployeeID,
         RevenueReportingDepartmentName,
         RevenueProductGroupSortOrder


DROP TABLE #DimLocationInfo
DROP TABLE #Clubs
DROP TABLE #DimMMSClubIDList
DROP TABLE #DimReportingHierarchy
DROP TABLE #CommissionTypeList
DROP TABLE #TempCommissionTypeList
DROP TABLE #SalesSourceList  
DROP TABLE #Summary

END

