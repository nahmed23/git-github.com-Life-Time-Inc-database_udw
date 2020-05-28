CREATE PROC [dbo].[test_proc_TeamMemberSalesAndServiceProductivityByProductGroup] @StartDate [DATETIME],@EndDate [DATETIME],@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DimReportingHierarchyKeyList [VARCHAR](8000),@RegionList [VARCHAR](4000),@MMSClubIDList [VARCHAR](4000),@DivisionList [VARCHAR](8000),@SubdivisionList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 

 ----- Sample Execution
 ---   Exec [reporting].[proc_TeamMemberSalesAndServiceProductivityByProductGroup] '5/1/2019','5/10/2019','All Departments','All Product Groups','All Regions','167|137','Personal Training','All Subdivisions'
 ----


-- NOTE: When making changes to this procedure, review similar lt_udw SP to ensure business rules are in sync:
--       proc_TeamMemberSalesAndServiceProductivityByProductGroup_DataIntegration




DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (SELECT Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           FROM map_utc_time_zone_conversion
                                           WHERE getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

DECLARE @ReportBeginDate DateTime
DECLARE @ReportStartDimDateKey INT
DECLARE @BeginMonthEndingDate DATETIME
DECLARE @ReportBeginDate_Standard Varchar(12)

DECLARE @ReportEndDate DateTime
DECLARE @ReportEndDimDateKey INT
DECLARE @EndMonthEndingDate DATETIME
DECLARE @EndMonthEndingDimDateKey INT
DECLARE @ReportEndDate_Standard Varchar(12)

SET  @ReportBeginDate = CASE WHEN @StartDate = 'Jan 1, 1900' 
                                     THEN DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()-1),0)    ----- returns 1st of yesterday's month 
									 ELSE @StartDate END
SET  @ReportEndDate = CASE WHEN @EndDate = 'Jan 1, 1900' 
	                                 THEN GETDATE()-1 ---- yesterday's date
									 ELSE @EndDate END


SELECT  @ReportStartDimDateKey = dim_date_key, 
        @BeginMonthEndingDate = month_ending_date,
        @ReportBeginDate_Standard = standard_date_name
   FROM [marketing].[v_dim_date]
   WHERE calendar_date = CAST(@ReportBeginDate AS Date)

SELECT  @ReportEndDimDateKey = dim_date_key, 
        @EndMonthEndingDate = month_ending_date,
		@EndMonthEndingDimDateKey = month_ending_dim_date_key,
        @ReportEndDate_Standard = standard_date_name
   FROM [marketing].[v_dim_date]
   WHERE calendar_date = CAST(@ReportEndDate AS Date)


DECLARE @HeaderDateRange VARCHAR(51)
SET @HeaderDateRange = @ReportBeginDate_Standard + '  through ' + @ReportEndDate_Standard


----- Create Hierarchy temp table to return selected group names      

Exec [reporting].[proc_DimReportingHierarchy_History] @DivisionList,@SubdivisionList,@DepartmentMinDimReportingHierarchyKeyList,@DimReportingHierarchyKeyList,@ReportStartDimDateKey,@ReportEndDimDateKey

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
	   ReportRegionType
 INTO #DimReportingHierarchy
 FROM #OuterOutputTable


DECLARE @RegionType VARCHAR(50)
SET @RegionType = (SELECT MIN(ReportRegionType)  FROM #DimReportingHierarchy)



IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL
  DROP TABLE #Clubs;

  ----- Create club temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   DimClub.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegion,
	   PTRCLRegion.description AS PTRCLRegion,
	   MemberActivitiesRegion.description AS MemberActivitiesRegion
  INTO #Clubs
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list MMSClubIDList
    ON MMSClubIDList.Item = DimClub.club_id
	  OR MMSClubIDList.Item like '-1'
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
  JOIN [marketing].[v_dim_description]  PTRCLRegion
   ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
  JOIN [marketing].[v_dim_description]  MemberActivitiesRegion
   ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_type = 'Club'



IF OBJECT_ID('tempdb.dbo.#DimLocation', 'U') IS NOT NULL
  DROP TABLE #DimLocation;

  ----- Create Region temp table
SET @list_table = 'region_list'

  EXEC marketing.proc_parse_pipe_list @RegionList,@list_table
	
SELECT DimClub.DimClubKey,
       DimClub.club_id AS MMSClubID,
	   DimClub.ClubName,
	   DimClub.LocalCurrencyCode
  INTO #DimLocation  
  FROM #Clubs DimClub
  JOIN #region_list RegionList 
   ON RegionList.Item = CASE WHEN @RegionType = 'PT RCL Area' 
                                   THEN DimClub.PTRCLRegion
                              WHEN @RegionType = 'Member Activities Region' 
                                   THEN DimClub.MemberActivitiesRegion
                              WHEN @RegionTYpe = 'MMS Region' 
                                   THEN DimClub.MMSRegion END
     OR RegionList.Item = 'All Regions'
	GROUP BY DimClub.DimClubKey,                                ------- Group By Added with REP-7615 because duplicate club records returned if 
       DimClub.club_id,                                         ------- "All Clubs" was selected with individual club names from the prompt page
	   DimClub.ClubName,                                        ------- Duplication in this table caused duplication in joined table records downstream
	   DimClub.LocalCurrencyCode


IF OBJECT_ID('tempdb.dbo.#tmpDimLocation', 'U') IS NOT NULL
  DROP TABLE #tmpDimLocation;

create    Table    #tmpDimLocation
        (
        DimClubKey    VARCHAR(32)not null,
		MMSClubID     INT
        )
insert    #tmpDimLocation
        Select  DimClubKey, MMSClubID
        From    #DimLocation
    UNION
        Select dim_club_key,club_id
		FROM [marketing].[v_dim_club]
		 WHERE club_id = 13     -------- "Corporate INTERNAL"




IF OBJECT_ID('tempdb.dbo.#FactSalesTransactionDetail', 'U') IS NOT NULL
  DROP TABLE #FactSalesTransactionDetail;
create table #FactSalesTransactionDetail with (distribution = hash(FactSalesTransactionKey)) as
SELECT DISTINCT FactSalesTransaction.dim_club_key AS DimClubKey,   ------- Name Change
FactSalesTransaction.dim_mms_product_key AS DimProductKey,
FactSalesTransaction.fact_mms_sales_transaction_item_key AS FactSalesTransactionKey,
FactSalesTransaction.membership_charge_flag AS MembershipChargeFlag,
FactSalesTransaction.original_currency_code AS OriginalCurrencyCode,
FactSalesTransaction.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,
FactSalesTransaction.post_dim_date_key AS PostDimDateKey,
FactSalesTransaction.pos_flag AS POSFlag,
FactSalesTransaction.sales_dollar_amount AS SalesDollarAmount,
FactSalesTransaction.sales_quantity AS SalesQuantity,
FactSalesTransaction.transaction_reporting_dim_club_key AS TransactionReportingDimClubKey,   ------- Name Change
FactSalesTransaction.voided_flag AS VoidedFlag
  --INTO #FactSalesTransactionDetail
  FROM [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
 WHERE FactSalesTransaction.post_dim_date_key >= @ReportStartDimDateKey
 AND FactSalesTransaction.post_dim_date_key <= @ReportEndDimDateKey




IF OBJECT_ID('tempdb.dbo.#FactSalesTransaction', 'U') IS NOT NULL
  DROP TABLE #FactSalesTransaction;
create table #FactSalesTransaction with (distribution = hash(FactSalesTransactionKey)) as
SELECT DISTINCT FactSalesTransactionDetail.FactSalesTransactionKey,
                FactSalesTransactionDetail.DimClubKey,
				#tmpDimLocation.MMSClubID,
                FactSalesTransactionDetail.TransactionReportingDimClubKey,
                FactSalesTransactionDetail.PrimarySalesDimEmployeeKey,
                FactSalesTransactionDetail.OriginalCurrencyCode,
                FactSalesTransactionDetail.SalesDollarAmount AS SalesDollarAmount,
               #DimReportingHierarchy.DepartmentName + ' - ' + IsNull(#DimReportingHierarchy.ProductGroupName,'') AS ReportingDepartmentNameDashProductGroupName,
               PostDimDate.year AS CalendarYear
  --INTO #FactSalesTransaction
  FROM #FactSalesTransactionDetail FactSalesTransactionDetail
  JOIN [marketing].[v_dim_date] PostDimDate
    ON FactSalesTransactionDetail.PostDimDateKey = PostDimDate.dim_date_key
  JOIN #tmpDimLocation
    ON FactSalesTransactionDetail.DimClubKey = #tmpDimLocation.DimClubKey
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactSalesTransactionDetail.DimProductKey = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= PostDimDate.month_ending_date
   AND DimProduct.expiration_date_time > PostDimDate.month_ending_date
  JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
   AND DimReportingHierarchy.effective_dim_date_key <= PostDimDate.month_ending_dim_date_key
   AND DimReportingHierarchy.expiration_dim_date_key > PostDimDate.month_ending_dim_date_key
  JOIN #DimReportingHierarchy
    ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key

 WHERE FactSalesTransactionDetail.VoidedFlag = 'N'
   AND FactSalesTransactionDetail.PrimarySalesDimEmployeeKey > '0'
   AND (FactSalesTransactionDetail.MembershipChargeFlag = 'Y' OR FactSalesTransactionDetail.POSFlag = 'Y')




IF OBJECT_ID('tempdb.dbo.#SalesDetail', 'U') IS NOT NULL
  DROP TABLE #SalesDetail;
create table #SalesDetail with (distribution = hash(FactSalesTransactionKey)) as
SELECT #factsalestransaction.FactSalesTransactionKey,
       CASE WHEN #FactSalesTransaction.MMSClubID = 13
                 THEN CASE WHEN EmployeeClub.club_id <> 13 
				             THEN DimEmployee.dim_club_key
                           ELSE #FactSalesTransaction.TransactionReportingDimClubKey 
						   END
            ELSE #FactSalesTransaction.DimClubKey END DimClubKey,
       DimEmployee.dim_employee_key AS DimEmployeeKey,     ------- UDW does not have SCD keys
       #FactSalesTransaction.ReportingDepartmentNameDashProductGroupName,
       #FactSalesTransaction.SalesDollarAmount AS SalesDollarAmount
  --INTO #SalesDetail
  FROM #FactSalesTransaction
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON #FactSalesTransaction.PrimarySalesDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_club] EmployeeClub
    ON DimEmployee.dim_club_key = EmployeeClub.dim_club_key

    
--query 2  ---- refunds 
insert into #SalesDetail
SELECT FactSalesTransactionAutomatedRefund.fact_mms_sales_transaction_item_automated_refund_key,
       CASE WHEN OriginalTranDimClub.MMSClubID = 13
                 THEN CASE WHEN EmployeeClub.club_id <> 13 
				            THEN DimEmployee.dim_club_key
                           ELSE FactSalesTransactionAutomatedRefund.original_transaction_reporting_dim_club_key 
						     END
            ELSE FactSalesTransactionAutomatedRefund.original_transaction_reporting_dim_club_key 
			END DimClubKey,
       DimEmployee.dim_employee_key AS DimEmployeeKey,   ------- UDW does not have SCD keys
       #DimReportingHierarchy.DepartmentName + ' - ' + IsNull(#DimReportingHierarchy.ProductGroupName,'') AS ReportingDepartmentNameDashProductGroupName,
       FactSalesTransactionAutomatedRefund.refund_dollar_amount AS SalesDollarAmount
FROM [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
  JOIN [marketing].[v_dim_date] OriginalPostDimDate
    ON FactSalesTransactionAutomatedRefund.original_post_dim_date_key = OriginalPostDimDate.dim_date_key
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON FactSalesTransactionAutomatedRefund.original_primary_sales_dim_employee_key = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_club] EmployeeClub
    ON DimEmployee.dim_club_key = EmployeeClub.dim_club_key
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactSalesTransactionAutomatedRefund.refund_dim_mms_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= OriginalPostDimDate.month_ending_date
   AND DimProduct.expiration_date_time > OriginalPostDimDate.month_ending_date
  JOIN [marketing].[v_dim_reporting_hierarchy_history] AS DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
   AND DimReportingHierarchy.effective_dim_date_key <= OriginalPostDimDate.month_ending_dim_date_key
   AND DimReportingHierarchy.expiration_dim_date_key > OriginalPostDimDate.month_ending_dim_date_key
  JOIN #DimReportingHierarchy
    ON DimReportingHierarchy.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #tmpDimLocation OriginalTranDimClub
    ON FactSalesTransactionAutomatedRefund.original_transaction_reporting_dim_club_key = OriginalTranDimClub.DimClubKey
 WHERE FactSalesTransactionAutomatedRefund.refund_post_dim_date_key >= @ReportStartDimDateKey
   AND FactSalesTransactionAutomatedRefund.refund_post_dim_date_key <= @ReportEndDimDateKey
   AND FactSalesTransactionAutomatedRefund.original_primary_sales_dim_employee_key > '0'
   AND FactSalesTransactionAutomatedRefund.refund_void_flag = 'N'


--Query 3
insert into #SalesDetail
SELECT FactECommerceSalesTransaction.fact_hybris_transaction_item_key,
       CASE WHEN DimClub.MMSClubID = 13      
              THEN DimEmployee.dim_club_key
            ELSE FactECommerceSalesTransaction.dim_club_key  
			END DimClubKey,
       DimEmployee.dim_employee_key AS DimEmployeeKey,
       #DimReportingHierarchy.DepartmentName + ' - ' + IsNull(#DimReportingHierarchy.ProductGroupName,'') AS ReportingDepartmentNameDashProductGroupName,
       FactECommerceSalesTransaction.transaction_amount_gross AS SalesDollarAmount

FROM [marketing].[v_fact_hybris_transaction_item] FactECommerceSalesTransaction
JOIN [marketing].[v_dim_date] ShipmentDimDate
  ON FactECommerceSalesTransaction.settlement_dim_date_key = ShipmentDimDate.dim_date_key
JOIN [marketing].[v_dim_employee] DimEmployee
  ON FactECommerceSalesTransaction.sales_dim_employee_key = DimEmployee.dim_employee_key
JOIN [marketing].[v_dim_hybris_product_history] DimHybrisProduct
  ON FactECommerceSalesTransaction.dim_hybris_product_key = DimHybrisProduct.dim_hybris_product_key
  AND DimHybrisProduct.effective_date_time <= ShipmentDimDate.month_ending_date
  AND DimHybrisProduct.expiration_date_time > ShipmentDimDate.month_ending_date
JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
    ON DimHybrisProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
   AND DimReportingHierarchy.effective_dim_date_key <= ShipmentDimDate.month_ending_dim_date_key
   AND DimReportingHierarchy.expiration_dim_date_key > ShipmentDimDate.month_ending_dim_date_key
JOIN #DimReportingHierarchy
    ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key
JOIN #tmpDimLocation DimClub
    ON FactECommerceSalesTransaction.dim_club_key= DimClub.DimClubKey    
WHERE FactECommerceSalesTransaction.settlement_dim_date_key >= @ReportStartDimDateKey
  AND FactECommerceSalesTransaction.settlement_dim_date_key <= @ReportEndDimDateKey
  AND FactECommerceSalesTransaction.sales_dim_employee_key > '0'
 


insert into #SalesDetail
SELECT FactHealthCheckUSASalesTransactionItem.fact_healthcheckusa_allocated_transaction_item_key,
      CASE WHEN DimClub.MMSClubID = 13 
              THEN DimEmployee.dim_club_key
            ELSE FactHealthCheckUSASalesTransactionItem.dim_club_key  
			END DimClubKey,
       DimEmployee.dim_employee_key AS DimEmployeeKey,   ----- no SCD keys in UDW
       #DimReportingHierarchy.DepartmentName + ' - ' + IsNull(#DimReportingHierarchy.ProductGroupName,'') AS ReportingDepartmentNameDashProductGroupName,
       SIGN(FactHealthCheckUSASalesTransactionItem.sales_quantity) * FactHealthCheckUSASalesTransactionItem.sales_amount AS SalesDollarAmount

  FROM [marketing].[v_fact_healthcheckusa_transaction_item] AS FactHealthCheckUSASalesTransactionItem
  JOIN [marketing].[v_dim_date] AS TransactionPostDimDate
    ON FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key = TransactionPostDimDate.dim_date_key
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON FactHealthCheckUSASalesTransactionItem.sales_dim_employee_key  = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_healthcheckusa_product_history] AS DimECommerceProduct
    ON FactHealthCheckUSASalesTransactionItem.dim_healthcheckusa_product_key = DimECommerceProduct.dim_healthcheckusa_product_key      ------ should not return the hybris product key   UDW-8482
   AND DimECommerceProduct.effective_date_time <= TransactionPostDimDate.month_ending_date
   AND DimECommerceProduct.expiration_date_time > TransactionPostDimDate.month_ending_date
  JOIN [marketing].[v_dim_reporting_hierarchy_history] AS DimReportingHierarchy
    ON DimECommerceProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
   AND DimReportingHierarchy.effective_dim_date_key <= TransactionPostDimDate.month_ending_dim_date_key
   AND DimReportingHierarchy.expiration_dim_date_key > TransactionPostDimDate.month_ending_dim_date_key
  JOIN #DimReportingHierarchy
    ON DimReportingHierarchy.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #tmpDimLocation DimClub
    ON FactHealthCheckUSASalesTransactionItem.dim_club_key = DimClub.DimClubKey
 WHERE FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key >= @ReportStartDimDateKey
   AND FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key <= @ReportEndDimDateKey
   AND FactHealthCheckUSASalesTransactionItem.sales_dim_employee_key > '0'


   
--Query 5                     
insert into #SalesDetail

SELECT FactCafePOSSalesTransaction.fact_cafe_sales_transaction_item_key,
       DimLocation.DimClubKey,
       FactCafePOSSalesTransaction.order_commissionable_dim_employee_key AS DimEmployeeKey,   ----- UDW does not have SCD key
       #DimReportingHierarchy.DepartmentName + ' - ' + IsNull(#DimReportingHierarchy.ProductGroupName,'') AS ReportingDepartmentNameDashProductGroupName,
       CASE WHEN order_refund_flag = 'Y'
	        THEN (FactCafePOSSalesTransaction.item_quantity * FactCafePOSSalesTransaction.item_sales_dollar_amount_excluding_tax) 
			ELSE FactCafePOSSalesTransaction.item_sales_dollar_amount_excluding_tax
			END SalesDollarAmount     

  FROM [marketing].[v_fact_cafe_transaction_item] FactCafePOSSalesTransaction
  JOIN #tmpDimLocation DimLocation
    ON FactCafePOSSalesTransaction.dim_club_key = DimLocation.DimClubKey
  JOIN [marketing].[v_dim_date] TransactionCloseDimDate
    ON FactCafePOSSalesTransaction.order_close_dim_date_key = TransactionCloseDimDate.dim_date_key
  JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct                                   
    ON FactCafePOSSalesTransaction.dim_cafe_product_key = DimCafeProduct.dim_cafe_product_key    
   AND DimCafeProduct.effective_date_time <= TransactionCloseDimDate.month_ending_date
   AND DimCafeProduct.expiration_date_time > TransactionCloseDimDate.month_ending_date
  JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
    ON DimCafeProduct.dim_reporting_hierarchy_key =  DimReportingHierarchy.dim_reporting_hierarchy_key
   AND DimReportingHierarchy.effective_dim_date_key <= TransactionCloseDimDate.month_ending_dim_date_key
   AND DimReportingHierarchy.expiration_dim_date_key > TransactionCloseDimDate.month_ending_dim_date_key
  JOIN #DimReportingHierarchy 
    ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key
 --WHERE FactCafePOSSalesTransaction.order_close_dim_date_key >= @ReportStartDimDateKey
 --  AND FactCafePOSSalesTransaction.order_close_dim_date_key <= @ReportEndDimDateKey
 where TransactionCloseDimDate.dim_date_key >= @ReportStartDimDateKey
   AND TransactionCloseDimDate.dim_date_key <= @ReportEndDimDateKey
   AND FactCafePOSSalesTransaction.order_commissionable_dim_employee_key > '0'
   AND FactCafePOSSalesTransaction.item_voided_flag = 'N'
   AND FactCafePOSSalesTransaction.order_void_flag = 'N'

  

insert into #SalesDetail
SELECT FactMagentoTransaction.fact_magento_transaction_item_key,CASE WHEN DimClub.MMSClubID = 13 
              THEN DimEmployee.dim_club_key
            ELSE FactMagentoTransaction.dim_club_key  
			END DimClubKey,
       DimEmployee.dim_employee_key AS DimEmployeeKey,
       #DimReportingHierarchy.DepartmentName + ' - ' + IsNull(#DimReportingHierarchy.ProductGroupName,'') AS ReportingDepartmentNameDashProductGroupName,
       (FactMagentoTransaction.transaction_item_amount - FactMagentoTransaction.transaction_discount_amount) AS SalesDollarAmount
FROM [marketing].[v_fact_magento_transaction_item] FactMagentoTransaction
JOIN [marketing].[v_dim_date] InvoiceDimDate
  ON FactMagentoTransaction.invoice_dim_date_key = InvoiceDimDate.dim_date_key
JOIN [marketing].[v_dim_employee] DimEmployee
  ON FactMagentoTransaction.dim_employee_key = DimEmployee.dim_employee_key
JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
  ON FactMagentoTransaction.dim_magento_product_key = DimMagentoProduct.dim_magento_product_key
  AND DimMagentoProduct.effective_date_time <= InvoiceDimDate.month_ending_date
  AND DimMagentoProduct.expiration_date_time > InvoiceDimDate.month_ending_date
JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
    ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
   AND DimReportingHierarchy.effective_dim_date_key <= InvoiceDimDate.month_ending_dim_date_key
   AND DimReportingHierarchy.expiration_dim_date_key > InvoiceDimDate.month_ending_dim_date_key
JOIN #DimReportingHierarchy
    ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key
JOIN #tmpDimLocation DimClub
    ON FactMagentoTransaction.dim_club_key= DimClub.DimClubKey
WHERE FactMagentoTransaction.invoice_dim_date_key >= @ReportStartDimDateKey
  AND FactMagentoTransaction.invoice_dim_date_key <= @ReportEndDimDateKey
  AND FactMagentoTransaction.dim_employee_key > '0'
 
IF OBJECT_ID('tempdb.dbo.#SalesSummaryByTeamMemberAndProductGroup', 'U') IS NOT NULL
  DROP TABLE #SalesSummaryByTeamMemberAndProductGroup;

SELECT #SalesDetail.DimEmployeeKey,
       #SalesDetail.ReportingDepartmentNameDashProductGroupName,
       SUM(#SalesDetail.SalesDollarAmount) Amount
  INTO #SalesSummaryByTeamMemberAndProductGroup
  FROM #SalesDetail
  JOIN #DimLocation                                       ----- joining on this temp table removes all the Corporate Internal club records
    ON #SalesDetail.DimClubKey = #DimLocation.DimclubKey
 GROUP BY #SalesDetail.DimEmployeeKey,
          #SalesDetail.ReportingDepartmentNameDashProductGroupName



IF OBJECT_ID('tempdb.dbo.#SalesSummaryByTeamMember', 'U') IS NOT NULL
  DROP TABLE #SalesSummaryByTeamMember;

SELECT DimEmployeeKey,
       SUM(Amount) TotalSalesAmount
  INTO #SalesSummaryByTeamMember
  FROM #SalesSummaryByTeamMemberAndProductGroup
 GROUP BY DimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#FactPackageSession', 'U') IS NOT NULL
  DROP TABLE #FactPackageSession;
  create table #FactPackageSession with (distribution = hash(fact_mms_package_session_key)) as
SELECT FactPackageSession.fact_mms_package_session_key,
       FactPackageSession.package_id,   ----- added for testing
       FactPackageSession.created_dim_date_key AS CreatedDimDateKey,
       FactPackageSession.delivered_dim_date_key AS DeliveredDimDateKey,
       FactPackageSession.delivered_dim_employee_key AS DeliveredDimEmployeeKey,
       FactPackageSession.package_entered_dim_employee_key AS PackageEnteredDimEmployeeKey,
       FactPackageSession.original_currency_code AS OriginalCurrencyCode,
       FactPackageSession.delivered_dim_club_key AS DeliveredDimClubKey,    ----- New Name
	   #tmpDimLocation.MMSClubID,
       FactPackageSession.delivered_session_price AS DeliveredSessionPrice,
       1 DeliveredSessionQuantity      ----- no longer any 0.5 qty sessions
  --INTO #FactPackageSession
  FROM [marketing].[v_fact_mms_package_session] FactPackageSession
  JOIN [marketing].[v_dim_date] DeliveredDimDate
    ON FactPackageSession.created_dim_date_key = DeliveredDimDate.dim_date_key
  JOIN [marketing].[v_dim_mms_product_history] DimProduct
    ON FactPackageSession.fact_mms_package_dim_product_key = DimProduct.dim_mms_product_key
   AND DimProduct.effective_date_time <= DeliveredDimDate.month_ending_date
   AND DimProduct.expiration_date_time > DeliveredDimDate.month_ending_date
  JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
    ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
   AND DimReportingHierarchy.effective_dim_date_key <= DeliveredDimDate.month_ending_dim_date_key
   AND DimReportingHierarchy.expiration_dim_date_key > DeliveredDimDate.month_ending_dim_date_key
  JOIN #DimReportingHierarchy
    ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key
  JOIN #tmpDimLocation
    ON FactPackageSession.delivered_dim_club_key = #tmpDimLocation.DimClubKey
 WHERE FactPackageSession.created_dim_date_key >= @ReportStartDimDateKey
   AND FactPackageSession.created_dim_date_key <= @ReportEndDimDateKey
   AND FactPackageSession.voided_flag = 'N'


  


IF OBJECT_ID('tempdb.dbo.#ServiceDetail', 'U') IS NOT NULL
  DROP TABLE #ServiceDetail;

SELECT DimEmployee.dim_employee_key AS DimEmployeeKey,
       SUM(#FactPackageSession.DeliveredSessionPrice * 1) TotalServiceAmount,
       SUM(CASE WHEN #FactPackageSession.DeliveredSessionPrice = 0 
	                 AND PackageEnteredDimEmployee.employee_id <> -5     ------ Loyalty Program "employee" - sold through the bucks store
	             THEN #FactPackageSession.DeliveredSessionQuantity ELSE 0 END) TotalFreeSessionProductivityCount,
       SUM(CASE WHEN #FactPackageSession.DeliveredSessionPrice <> 0 
	             THEN #FactPackageSession.DeliveredSessionQuantity ELSE 0 END) TotalPaidSessionProductivityCount,
       SUM(CASE WHEN #FactPackageSession.DeliveredSessionPrice = 0 
	                 AND PackageEnteredDimEmployee.employee_id = -5 
				THEN #FactPackageSession.DeliveredSessionQuantity ELSE 0 END) TotalMyLTBucksProductivityCount
  INTO #ServiceDetail
  FROM #FactPackageSession
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON #FactPackageSession.DeliveredDimEmployeeKey = DimEmployee.dim_employee_key
  JOIN #DimLocation
    ON #DimLocation.DimClubKey = CASE WHEN #FactPackageSession.MMSClubID = 13 
	                                    THEN DimEmployee.dim_club_key
                                      ELSE #FactPackageSession.DeliveredDimClubKey END
  JOIN [marketing].[v_dim_employee] PackageEnteredDimEmployee
    ON #FactPackageSession.PackageEnteredDimEmployeeKey = PackageEnteredDimEmployee.dim_employee_key
 GROUP BY DimEmployee.dim_employee_key


IF OBJECT_ID('tempdb.dbo.#SalesAndServiceSummary', 'U') IS NOT NULL
  DROP TABLE #SalesAndServiceSummary;

SELECT ISNULL(#SalesSummaryByTeamMemberAndProductGroup.DimEmployeeKey, #ServiceDetail.DimEmployeeKey) DimEmployeeKey,   ----- Name change
       #SalesSummaryByTeamMemberAndProductGroup.ReportingDepartmentNameDashProductGroupName,
       Cast(ISNULL(#SalesSummaryByTeamMemberAndProductGroup.Amount,0) as Decimal(12,2)) Amount,
       Cast(ISNULL(#SalesSummaryByTeamMember.TotalSalesAmount,0) as Decimal(12,2)) TotalSalesAmount,
       Cast(ISNULL(#ServiceDetail.TotalServiceAmount,0) as Decimal(12,2)) TotalServiceAmount,
       ISNULL(#ServiceDetail.TotalFreeSessionProductivityCount,0) TotalFreeSessionProductivityCount,
       ISNULL(#ServiceDetail.TotalPaidSessionProductivityCount,0) TotalPaidSessionProductivityCount,
       ISNULL(#ServiceDetail.TotalMyLTBucksProductivityCount,0) TotalMyLTBucksProductivityCount
  INTO #SalesAndServiceSummary
  FROM #SalesSummaryByTeamMemberAndProductGroup
  JOIN #SalesSummaryByTeamMember
    ON #SalesSummaryByTeamMemberAndProductGroup.DimEmployeeKey = #SalesSummaryByTeamMember.DimEmployeeKey
  FULL OUTER JOIN #ServiceDetail  ----<<<<<==============
    ON #SalesSummaryByTeamMemberAndProductGroup.DimEmployeeKey = #ServiceDetail.DimEmployeeKey



IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results;

SELECT CASE WHEN @RegionType = 'PT RCL Area' 
                 THEN PTRCLArea.description
            WHEN @RegionType = 'Member Activities Region' 
                 THEN MemberActivitiesRegion.description
            WHEN @RegionType = 'MMS Region' 
                 THEN MMSRegion.description END Region,
       DimEmployee.employee_name_last_first AS TeamMember,
       DimEmployee.employee_id AS TeamMemberID,
       DimEmployeeDimLocation.club_name AS TeamMemberHomeClub,
       DimEmployeeRole.role_name TeamMemberTitle,
       #SalesAndServiceSummary.TotalSalesAmount + #SalesAndServiceSummary.TotalServiceAmount AS TotalProductivityAmount,
       #SalesAndServiceSummary.TotalServiceAmount,
       #SalesAndServiceSummary.TotalFreeSessionProductivityCount,
       #SalesAndServiceSummary.TotalPaidSessionProductivityCount,
       #SalesAndServiceSummary.TotalMyLTBucksProductivityCount,
       #SalesAndServiceSummary.TotalSalesAmount,
       #SalesAndServiceSummary.ReportingDepartmentNameDashProductGroupName,
       #SalesAndServiceSummary.Amount
  INTO #Results
  FROM #SalesAndServiceSummary
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON #SalesAndServiceSummary.DimEmployeeKey = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_club] DimEmployeeDimLocation
    ON DimEmployee.dim_club_key = DimEmployeeDimLocation.dim_club_key
  JOIN [marketing].[v_dim_description] MemberActivitiesRegion
    ON DimEmployeeDimLocation.member_activities_region_dim_description_key = MemberActivitiesRegion.dim_description_key
  JOIN [marketing].[v_dim_description] PTRCLArea
    ON DimEmployeeDimLocation.pt_rcl_area_dim_description_key = PTRCLArea.dim_description_key
  JOIN [marketing].[v_dim_description] MMSRegion
    ON DimEmployeeDimLocation.region_dim_description_key = MMSRegion.dim_description_key
  JOIN [marketing].[v_dim_employee_bridge_dim_employee_role] DimEmployeeBridgeDimEmployeeRole  
    ON DimEmployee.dim_employee_key = DimEmployeeBridgeDimEmployeeRole.dim_employee_key
   AND DimEmployeeBridgeDimEmployeeRole.primary_employee_role_flag = 'Y'
  LEFT JOIN [marketing].[v_dim_employee_role] DimEmployeeRole
    ON DimEmployeeBridgeDimEmployeeRole.dim_employee_role_key = DimEmployeeRole.dim_employee_role_key


SELECT Region,
       TeamMember,
       TeamMemberID,
       TeamMemberHomeClub,
       TeamMemberTitle,
       TotalProductivityAmount,
       TotalServiceAmount,
       TotalFreeSessionProductivityCount,
       TotalPaidSessionProductivityCount,
       TotalMyLTBucksProductivityCount,
       TotalSalesAmount,
       ReportingDepartmentNameDashProductGroupName,
       Amount,
       @ReportRunDateTime ReportRunDateTime,
       'Local Currency' ReportingCurrencyCode,
       NULL HeaderReportingDepartmentList,
       NULL HeaderRevenueProductGrouplist,
       @HeaderDateRange HeaderDateRange,
       Cast('' as Varchar(74)) HeaderEmptyResult,
       NULL HeaderDivisionList,
       NULL HeaderSubdivisionList
  FROM #Results
 WHERE EXISTS(SELECT top 1 * FROM #Results)
UNION ALL
SELECT Cast(NULL as Varchar(50)) Region,
       Cast(NULL as Varchar(102)) TeamMember,
       NULL TeamMemberID,
       Cast(NULL as Varchar(50)) TeamMemberHomeClub,
       Cast(NULL as Varchar(50)) TeamMemberTitle,
       Cast(NULL as Decimal(12,2)) TotalProductivityAmount,
       Cast(NULL as Decimal(12,2)) TotalServiceAmount,
       NULL TotalFreeSessionProductivityCount,
       NULL TotalPaidSessionProductivityCount,
       NULL TotalMyLTBucksProductivityCount,
       Cast(NULL as Decimal(12,2)) TotalSalesAmount,
       Cast(NULL as Varchar(103)) ReportingDepartmentNameDashProductGroupName,
       Cast(NULL as Varchar(50)) Amount,
       @ReportRunDateTime ReportRunDateTime,
       Cast(NULL as Varchar(50)) ReportingCurrencyCode,
       NULL HeaderReportingDepartmentList,
       NULL HeaderRevenueProductGrouplist,
       @HeaderDateRange HeaderDateRange,
       'There are no records available for the selected parameters. Please re-try.' HeaderEmptyResult,
       NULL HeaderDivisionList,
       NULL HeaderSubdivisionList
 WHERE NOT EXISTS(SELECT top 1 * FROM #Results)
-----ORDER BY TotalProductivityAmount DESC, ReportingDepartmentNameDashProductGroupName


DROP TABLE #DimLocation
DROP TABLE #Clubs
DROP TABLE #tmpDimLocation
DROP TABLE #SalesDetail
DROP TABLE #SalesSummaryByTeamMember
DROP TABLE #SalesSummaryByTeamMemberAndProductGroup
DROP TABLE #ServiceDetail
DROP TABLE #DimReportingHierarchy
DROP TABLE #FactPackageSession
DROP TABLE #FactSalesTransaction
DROP TABLE #FactSalesTransactionDetail
DROP TABLE #Results
DROP TABLE #SalesAndServiceSummary




END
