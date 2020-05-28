CREATE PROC [reporting].[proc_TeamMemberRevenueTransactionDetailDrillThrough] @StartFourDigitYearDashTwoDigitMonth [CHAR](7),@EndFourDigitYearDashTwoDigitMonth [CHAR](7),@DimReportingHierarchyKeyList [VARCHAR](8000),@SalesSourceList [VARCHAR](4000),@Dim_club_key [VARCHAR](4000),@DimEmployeeKey [VARCHAR](32),@ReportingCurrencyCode [VARCHAR](15) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
------ Sample Execution
---  Exec [reporting].[proc_TeamMemberRevenueTransactionDetailDrillThrough] '2019-01','2019-01','All Product Groups','MMS|Cafe','77A444D90D0618C0B6BBE76761B2554E','2D18B04DC3010D9859B22118C73CA869','USD'
----

  --DECLARE    @StartFourDigitYearDashTwoDigitMonth CHAR(7) ='2019-01'
  --DECLARE    @EndFourDigitYearDashTwoDigitMonth CHAR(7) ='2019-07'
  --DECLARE    @DimReportingHierarchyKeyList [VARCHAR](8000) = '73C08A1FA167DACA5D0567193E3080C1|E04F6D5DC4273B05E20BF85BF7D61D59|1238C5D84789F67E94581D3A0C89067B|198C9B2DB668632E31F72B4A5A759602|BA670B690425506A8AE777B61EC483A2|78C32939F07031197F3EA0AC0B95EDA4|ACE28DDAF87B440B3EDE8D2FC88F7D3D|6AC8776E77AA81AA2F53FD0BD752AF45|A4385F1E59BC4F705B42DDB21D9B251B|0C70CA94BE4E8702B93E559874B6D3CA|B7C16A9362CAC57B3EAA199E82FCE4B9|2EAC429F1954A26F7251B12ED725283E'
  --DECLARE    @SalesSourceList VARCHAR(4000) ='MMS|Cafe|Magento|Hybris|HealthCheckUSA'
  --DECLARE    @Dim_club_key  VARCHAR(256) = '77A444D90D0618C0B6BBE76761B2554E'
  --DECLARE    @DimEmployeeKey  VARCHAR(256) = '6B84781EBE4620A3ECDDEF7823E37992'
  --DECLARE    @ReportingCurrencyCode VARCHAR(15) = 'USD'


DECLARE @ReportRunDateTime VARCHAR(21)
SELECT @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

DECLARE @StartMonthStartingDimDateKey INT
SELECT @StartMonthStartingDimDateKey = DimDate.month_starting_dim_date_key
FROM marketing.v_Dim_Date DimDate
WHERE DimDate.four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth
  AND DimDate.day_number_in_month = 1

DECLARE @EndMonthStartingDimDateKey INT,
        @EndMonthEndingDimDateKey INT,
		@EndMonthEndingDate Datetime
SELECT @EndMonthStartingDimDateKey = DimDate.Month_Starting_Dim_Date_Key,
       @EndMonthEndingDimDateKey = DimDate.Month_Ending_Dim_Date_Key,
	   @EndMonthEndingDate = DimDate.calendar_date
FROM  marketing.v_Dim_Date DimDate 
WHERE DimDate.four_digit_year_dash_two_digit_month = @EndFourDigitYearDashTwoDigitMonth
  AND DimDate.last_day_in_month_flag = 'Y'

 ----- Create Sales Source temp table   
IF OBJECT_ID('tempdb.dbo.#sales_sourceList', 'U') IS NOT NULL
  DROP TABLE #sales_sourceList;   

DECLARE @list_table VARCHAR(100)
SET @list_table = 'sales_source_list'

EXEC marketing.proc_parse_pipe_list @SalesSourceList,@list_table

SELECT DISTINCT sales_sourceList.Item sales_source
  INTO #sales_sourceList
  FROM #sales_source_list  sales_sourceList

DECLARE @SalesSourceCommaList VARCHAR(4000)
SET @SalesSourceCommaList = REPLACE(@SalesSourceList,'|',',')


------- Create Hierarchy temp table to return selected group names      

Exec [reporting].[proc_DimReportingHierarchy_history] 'N/A', 'N/A', 'N/A',@DimReportingHierarchyKeyList,@StartMonthStartingDimDateKey,@EndMonthEndingDimDateKey
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


 

 
DECLARE @HeaderDivisionList VARCHAR(8000),
        @HeaderSubdivisionList VARCHAR(8000),
        @RevenueReportingDepartmentNameCommaList VARCHAR(8000),
        @RevenueProductGroupNameCommaList VARCHAR(8000)

------- Must be created in report tool based on prompt values#########
-- SELECT @HeaderDivisionList = (SELECT MIN(HeaderDivisionList) FROM #DimReportingHierarchy),
       -- @HeaderSubdivisionList = (SELECT MIN(HeaderSubdivisionList) FROM #DimReportingHierarchy),
       -- @RevenueReportingDepartmentNameCommaList = (SELECT MIN(HeaderDepartmentList) FROM #DimReportingHierarchy),
       -- @RevenueProductGroupNameCommaList = (SELECT MIN(HeaderProductGroupList) FROM #DimReportingHierarchy)

  ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month
DECLARE @FirstOfReportRangeDimDateKey INT
DECLARE @EndOfReportRangeDimDateKey INT
SET @FirstOfReportRangeDimDateKey = (SELECT MIN(Dim_Date_Key) FROM marketing.v_Dim_Date where Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth)
SET @EndOfReportRangeDimDateKey = (SELECT MAX(Dim_Date_Key) FROM marketing.v_Dim_Date where Four_Digit_Year_Dash_Two_Digit_Month = @EndFourDigitYearDashTwoDigitMonth)

DECLARE @EComm60DayChallengeRevenueStartMonthStartDimDateKey INT
  ---- When the requested month is the 2nd month of the quarter, set the start date to the prior month
SET @EComm60DayChallengeRevenueStartMonthStartDimDateKey = (SELECT CASE WHEN (Select month_number_in_year 
                    From marketing.v_Dim_Date 
				   Where dim_date_key = @FirstOfReportRangeDimDateKey) in (2,5,8,11)
			THEN (Select prior_month_starting_dim_date_key
			        FROM marketing.v_Dim_Date 
			        WHERE dim_date_key = @FirstOfReportRangeDimDateKey)
            ELSE (Select month_starting_dim_date_key
                    From marketing.v_Dim_Date 
				   Where dim_date_key = @FirstOfReportRangeDimDateKey)
			END 
            FROM marketing.v_Dim_Date
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record


DECLARE @EComm60DayChallengeRevenueEndMonthEndDimDateKey INT
  ---- When the requested month is the 1st month of the quarter, set the end date to the prior month
SET @EComm60DayChallengeRevenueEndMonthEndDimDateKey = (SELECT CASE WHEN (Select month_number_in_year 
                    From marketing.v_Dim_Date 
				   Where dim_date_key = @EndOfReportRangeDimDateKey) in (1,4,7,10)
			THEN (Select prior_month_ending_dim_date_key
			        FROM marketing.v_Dim_Date 
			        WHERE dim_date_key = @EndOfReportRangeDimDateKey)
            ELSE (Select month_ending_dim_date_key
                    From marketing.v_Dim_Date 
				   Where dim_date_key = @EndOfReportRangeDimDateKey)
			END 
            FROM marketing.v_Dim_Date
            WHERE dim_date_key = @FirstOfReportRangeDimDateKey)  ---- to limit result set to one record

IF OBJECT_ID('tempdb.dbo.#TransactionDetail', 'U') IS NOT NULL
  DROP TABLE #TransactionDetail; 



CREATE TABLE #TransactionDetail(
clubcode VARCHAR(18),
TransactionDate VARCHAR(12),
MemberID INT,
MemberName VARCHAR(132),
Product VARCHAR(255),
SalesSource VARCHAR(50),
RevenueReportingDepartmentName VARCHAR(50),
RevenueProductGroupName VARCHAR(50),
CurrencyCode VARCHAR(15),
SalesAmount DECIMAL(12,2),
SalesQuantity INT,
RevenueAmount DECIMAL(12,2),
RevenueQuantity INT,
SalesSourceCommaList VARCHAR(4000),
HeaderTeamMemberIDAndName VARCHAR(115),
RevenueReportingDepartmentNameCommaList VARCHAR(8000),
ReportRunDateTime VARCHAR(21),
TransactionDimDateKey INT,
SoldNotServicedFlag CHAR(1),
CorporateTransferAmount DECIMAL(12,2),
RevenueProductGroupNameCommaList VARCHAR(8000),
DivisionName VARCHAR(255),
SubdivisionName VARCHAR(255),
HeaderDivisionList VARCHAR(8000),
PrimarySalesPersonKey VARCHAR(32),
HeaderSubdivisionList VARCHAR(8000),
dim_reporting_hierarchy_key VARCHAR(32))





  INSERT INTO #TransactionDetail   
  SELECT DimLocation.club_code clubcode,
         DimDate.standard_date_name TransactionDate,
         DimCustomer.member_id memberid,
         DimCustomer.customer_name_last_first MemberName,
		 CASE WHEN FactClubPOSAllocatedRevenue.sales_source = 'Cafe'
		          THEN DimCafeProduct.menu_item_name
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.product_description
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'MMS'
			      THEN DimMMSProduct.product_description
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
		 END  Product,
		 FactClubPOSAllocatedRevenue.sales_source as sales_source,
		 CASE WHEN FactClubPOSAllocatedRevenue.sales_source = 'Cafe'
				THEN DimReportingHierarchy_Cafe.DepartmentName 
			  WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			     THEN DimReportingHierarchy_Hybris.DepartmentName
			  WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			     THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			  WHEN FactClubPOSAllocatedRevenue.sales_source = 'MMS'
			     THEN DimReportingHierarchy_MMS.DepartmentName
			  WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                 THEN DimReportingHierarchy_Magento.DepartmentName	
		 END  RevenueReportingDepartmentName,
         CASE WHEN FactClubPOSAllocatedRevenue.sales_source = 'Cafe'
				THEN DimReportingHierarchy_Cafe.ProductGroupName 
			  WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			     THEN DimReportingHierarchy_Hybris.ProductGroupName
			  WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			     THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			  WHEN FactClubPOSAllocatedRevenue.sales_source = 'MMS'
			     THEN DimReportingHierarchy_MMS.ProductGroupName
			  WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                 THEN DimReportingHierarchy_Magento.ProductGroupName	
		 END  RevenueProductGroupName,
         @ReportingCurrencyCode CurrencyCode,
         FactClubPOSAllocatedRevenue.transaction_amount AS SalesAmount,
         FactClubPOSAllocatedRevenue.transaction_quantity AS SalesQuantity,
		 FactClubPOSAllocatedRevenue.allocated_amount RevenueAmount,  
         FactClubPOSAllocatedRevenue.allocated_quantity RevenueQuantity,
         @SalesSourceCommaList SalesSourceCommaList,
		 
         CASE WHEN IsNull(DimEmployee.dim_employee_key,'-998') in('-997', '-998','-999') 
		     THEN 'None Designated'		  
              ELSE Convert(VARCHAR,DimEmployee.employee_id) + ' - ' + DimEmployee.employee_name_last_first
             END HeaderTeamMemberIDAndName,
			 
         NULL RevenueReportingDepartmentNameCommaList,
         @ReportRunDateTime ReportRunDateTime,
         FactClubPOSAllocatedRevenue.transaction_dim_date_key AS TransactionDimDateKey,
         NULL SoldNotServicedFlag,
		  0  AS CorporateTransferAmount,
         NULL RevenueProductGroupNameCommaList,
		 CASE WHEN FactClubPOSAllocatedRevenue.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.DivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.DivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'   
                  THEN DimReportingHierarchy_Magento.DivisionName
		END  DivisionName,
		CASE WHEN FactClubPOSAllocatedRevenue.sales_source = 'Cafe'
		          THEN DimReportingHierarchy_Cafe.SubdivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.SubdivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.SubdivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'MMS'
			      THEN DimReportingHierarchy_MMS.SubdivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.SubdivisionName
		END  SubdivisionName,
         NULL HeaderDivisionList,
		 FactClubPOSAllocatedRevenue.primary_sales_dim_employee_key,
         NULL HeaderSubdivisionList,
		 FactClubPOSAllocatedRevenue.dim_reporting_hierarchy_key
  FROM [marketing].[v_fact_combined_allocated_transaction_item] FactClubPOSAllocatedRevenue
   LEFT JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct
     ON FactClubPOSAllocatedRevenue.dim_product_key = DimCafeProduct.dim_cafe_product_key
	   AND FactClubPOSAllocatedRevenue.sales_source = 'Cafe'
	   AND DimCafeProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimCafeProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactClubPOSAllocatedRevenue.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHybrisProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactClubPOSAllocatedRevenue.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHealthCheckUSAProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_mms_product_history] DimMMSProduct
     ON FactClubPOSAllocatedRevenue.dim_product_key = DimMMSProduct.dim_mms_product_key
	   AND FactClubPOSAllocatedRevenue.sales_source = 'MMS'
	   AND DimMMSProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMMSProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactClubPOSAllocatedRevenue.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactClubPOSAllocatedRevenue.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMagentoProduct.expiration_date_time > @EndMonthEndingDate
   JOIN #DimReportingHierarchy DimReportingHierarchy
	  ON FactClubPOSAllocatedRevenue.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
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
   LEFT JOIN [marketing].v_dim_mms_member DimCustomer 
    ON FactCLubPOSAllocatedRevenue.dim_mms_member_key = DimCustomer.dim_mms_member_key
	JOIN [marketing].[v_dim_date] DimDate
     ON FactClubPOSAllocatedRevenue.transaction_dim_date_key = DimDate.dim_date_key	
   JOIN [marketing].v_dim_club DimLocation
	 ON FactClubPOSAllocatedRevenue.allocated_dim_club_key = DimLocation.dim_club_key
   JOIN [marketing].[v_dim_employee] DimEmployee 
    ON FactClubPOSAllocatedRevenue.primary_sales_dim_employee_key  = DimEmployee.dim_employee_key
   JOIN #sales_sourceList SalesSourceList
     ON FactClubPOSAllocatedRevenue.sales_source = SalesSourceList.sales_source
  WHERE FactClubPOSAllocatedRevenue.allocated_month_starting_dim_date_key >= @StartMonthStartingDimDateKey
    AND FactClubPOSAllocatedRevenue.allocated_month_starting_dim_date_key <= @EndMonthStartingDimDateKey
   AND FactClubPOSAllocatedRevenue.allocated_dim_club_key = @Dim_club_key
   AND FactClubPOSAllocatedRevenue.primary_sales_dim_employee_key = @DimEmployeeKey
   AND (FactClubPOSAllocatedRevenue.sales_source in('MMS','Cafe')
       OR DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'N' )                                   ------- need this to prevent duplicate e-comm transactions from coming through from unioned query


UNION ALL
		 
  SELECT DimLocation.club_code,
         DimDate.standard_date_name TransactionDate,
		 DimCustomer.member_id memberid,
         DimCustomer.customer_name_last_first MemberName,
		 CASE WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			      THEN DimHybrisProduct.name
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			      THEN DimHealthCheckUSAProduct.product_description
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                  THEN DimMagentoProduct.product_name	
		 END  Product,
		 FactClubPOSAllocatedRevenue.sales_source SalesSource,	   
 		 CASE  WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DepartmentName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DepartmentName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DepartmentName	
			   END  RevenueReportingDepartmentName,        
 		 CASE  WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.ProductGroupName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.ProductGroupName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.ProductGroupName
			   END  RevenueProductGroup,
         @ReportingCurrencyCode CurrencyCode,
		 FactClubPOSAllocatedRevenue.transaction_amount * 1 SalesAmount,
         FactClubPOSAllocatedRevenue.transaction_quantity SalesQuantity,
		 FactClubPOSAllocatedRevenue.allocated_amount * 1 RevenueAmount, 
         FactClubPOSAllocatedRevenue.allocated_quantity RevenueQuantity,
         NULL SalesSourceCommaList, 
		 CASE WHEN IsNull(DimEmployee.dim_employee_key,'-998') in('-997', '-998','-999') THEN 'None Designated'
              ELSE Convert(VARCHAR,DimEmployee.employee_id) + ' - ' + DimEmployee.employee_name_last_first
             END HeaderTeamMemberIDAndName,			 
         NULL RevenueReportingDepartmentNameCommaList,
         @ReportRunDateTime ReportRunDateTime,
         FactClubPOSAllocatedRevenue.transaction_dim_date_key AS TransactionDimDateKey,--TransactionPostDimDateKey
         NULL SoldNotServicedFlag,
         0 CorporateTransferAmount,
         NULL RevenueProductGroupNameCommaList,
		CASE WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.DivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.DivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.DivisionName
		END  DivisionName,
		CASE WHEN FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
			      THEN DimReportingHierarchy_Hybris.SubdivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'  
			      THEN DimReportingHierarchy_HealthCheckUSA.SubdivisionName
			   WHEN FactClubPOSAllocatedRevenue.sales_source = 'Magento'    
                  THEN DimReportingHierarchy_Magento.SubdivisionName
		END  SubdivisionName,
         NULL HeaderDivisionList,
		 FactClubPOSAllocatedRevenue.primary_sales_dim_employee_key,
         NULL HeaderSubdivisionList,
		 FactClubPOSAllocatedRevenue.dim_reporting_hierarchy_key
  FROM marketing.v_fact_combined_allocated_transaction_item FactClubPOSAllocatedRevenue
   LEFT JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
     ON FactClubPOSAllocatedRevenue.dim_product_key = DimHybrisProduct.dim_hybris_product_key
	   AND FactClubPOSAllocatedRevenue.sales_source = 'Hybris'
	   AND DimHybrisProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHybrisProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
     ON FactClubPOSAllocatedRevenue.dim_product_key = DimHealthCheckUSAProduct.dim_healthcheckusa_product_key
	   AND FactClubPOSAllocatedRevenue.sales_source = 'HealthCheckUSA'
	   AND DimHealthCheckUSAProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimHealthCheckUSAProduct.expiration_date_time > @EndMonthEndingDate
   LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
     ON FactClubPOSAllocatedRevenue.dim_product_key = DimMagentoProduct.dim_magento_product_key
	   AND FactClubPOSAllocatedRevenue.sales_source = 'Magento'
	   AND DimMagentoProduct.effective_date_time <= @EndMonthEndingDate
	   AND DimMagentoProduct.expiration_date_time > @EndMonthEndingDate
	   
  JOIN #DimReportingHierarchy DimReportingHierarchy
	  ON FactClubPOSAllocatedRevenue.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
      AND DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'Y'                           ------- need this to prevent duplicate e-comm transactions from coming through from unioned query
    LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Hybris
     ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Hybris.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Hybris.PTDeferredRevenueProductGroupFlag = 'Y'  
    LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_HealthCheckUSA
     ON DimHealthCheckUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_HealthCheckUSA.DimReportingHierarchyKey
	 AND DimReportingHierarchy_HealthCheckUSA.PTDeferredRevenueProductGroupFlag = 'Y'  
    LEFT JOIN #DimReportingHierarchy DimReportingHierarchy_Magento
     ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy_Magento.DimReportingHierarchyKey
	 AND DimReportingHierarchy_Magento.PTDeferredRevenueProductGroupFlag = 'Y'   
   
   LEFT JOIN [marketing].[v_dim_mms_member] DimCustomer
     ON FactClubPOSAllocatedRevenue.dim_mms_member_key = DimCustomer.dim_mms_member_key 
   JOIN #sales_sourceList SalesSourceList
     ON FactClubPOSAllocatedRevenue.sales_source = SalesSourceList.sales_source
	 
   JOIN [marketing].[v_dim_date] DimDate
     ON FactClubPOSAllocatedRevenue.transaction_dim_date_key = DimDate.dim_date_key	
   JOIN [marketing].v_dim_club DimLocation
	 ON FactClubPOSAllocatedRevenue.allocated_dim_club_key = DimLocation.dim_club_key
   JOIN [marketing].[v_dim_employee] DimEmployee
     ON FactClubPOSAllocatedRevenue.primary_sales_dim_employee_key = DimEmployee.dim_employee_key---CommissionedSalesDimEmployeeKey

	 
 WHERE FactClubPOSAllocatedRevenue.transaction_dim_date_key >= @EComm60DayChallengeRevenueStartMonthStartDimDateKey
   AND FactClubPOSAllocatedRevenue.transaction_dim_date_key <= @EComm60DayChallengeRevenueEndMonthEndDimDateKey
   AND FactClubPOSAllocatedRevenue.allocated_dim_club_key = @Dim_club_key
   AND FactClubPOSAllocatedRevenue.primary_sales_dim_employee_key = @DimEmployeeKey
   AND FactClubPOSAllocatedRevenue.sales_source in('Hybris','HealthCheckUSA','Magento')

SELECT ClubCode,
       TransactionDate,
       MemberID,
       MemberName,
       Product,
       SalesSource,
       RevenueReportingDepartmentName,
       RevenueProductGroupName,
       CurrencyCode,
       SalesAmount,
       SalesQuantity,
       RevenueAmount,
       RevenueQuantity,
       SalesSourceCommaList,
       HeaderTeamMemberIDAndName,
       RevenueReportingDepartmentNameCommaList,
       ReportRunDateTime,
       TransactionDimDateKey,
       SoldNotServicedFlag,
       CorporateTransferAmount,
       RevenueProductGroupNameCommaList,
       TransactionDetail.DivisionName,
       TransactionDetail.SubdivisionName,
       HeaderDivisionList,
       HeaderSubdivisionList
  FROM #TransactionDetail  TransactionDetail

--ORDER BY TransactionDate, MemberName, MemberID


DROP TABLE #TransactionDetail
DROP TABLE #DimReportingHierarchy
DROP TABLE #sales_sourceList

END



