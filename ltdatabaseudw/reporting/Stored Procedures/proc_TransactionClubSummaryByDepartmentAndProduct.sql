CREATE PROC [reporting].[proc_TransactionClubSummaryByDepartmentAndProduct] @StartDate [Datetime],@EndDate [Datetime],@SalesSourceList [Varchar](4000),@MMSClubIDList [VARCHAR](4000),@DepartmentMinDimReportingHierarchyKeyList [Varchar](8000),@MembershipFilter [Varchar](50),@DivisionList [Varchar](8000),@SubdivisionList [Varchar](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


--DECLARE
--@StartDate Datetime,
--@EndDate Datetime,
--@SalesSourceList Varchar(4000),
--@MMSClubIDList Varchar(4000),
--@DepartmentMinDimReportingHierarchyKeyList Varchar(8000),
--@MembershipFilter Varchar(50),
--@DivisionList Varchar(8000),
--@SubdivisionList Varchar(8000)

--set @StartDate = '4/1/2019'
--set @EndDate = '5/1/2019'
--set @SalesSourceList = 'MMS'
--set @MMSClubIDList = '151'
--set @DepartmentMinDimReportingHierarchyKeyList = 'All Departments'
--set @MembershipFilter = 'All Memberships'
--set @DivisionList = 'All Divisions'
--set @SubdivisionList = 'All Subdivisions'


DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time


DECLARE @HeaderDateRange VARCHAR(51),
        @ReportStartDimDateKey INT,
        @ReportEndDimDateKey INT,
        @EndMonthEndingDimDateKey INT
        
SELECT @HeaderDateRange = ReportStartDimDate.standard_date_name + '  through ' + ReportEndDimDate.standard_date_name,
       @ReportStartDimDateKey = ReportStartDimDate.dim_date_key,
       @ReportEndDimDateKey = ReportEndDimDate.dim_date_key,
       @EndMonthEndingDimDateKey = ReportEndDimDate.month_ending_dim_date_key
  FROM [marketing].[v_dim_date] ReportStartDimDate
 CROSS JOIN [marketing].[v_dim_date] ReportEndDimDate
 WHERE ReportStartDimDate.calendar_date = @StartDate
   AND ReportEndDimDate.calendar_date = @EndDate

 ----- Create Sales Source temp table   
IF OBJECT_ID('tempdb.dbo.#SalesSourceList', 'U') IS NOT NULL
  DROP TABLE #SalesSourceList; 

DECLARE @list_table VARCHAR(100)
SET @list_table = 'SalesSource_list'

EXEC marketing.proc_parse_pipe_list @SalesSourceList,@list_table

SELECT DISTINCT SalesSourceList.Item SalesSource
  INTO #SalesSourceList
  FROM #SalesSource_list  SalesSourceList



 ----- Create club Key temp table   
IF OBJECT_ID('tempdb.dbo.#MMSClubIDList', 'U') IS NOT NULL
  DROP TABLE #MMSClubIDList; 

SET @list_table = 'ClubID_list'

EXEC marketing.proc_parse_pipe_list  @MMSClubIDList ,@list_table

SELECT DISTINCT ClubID_list.Item MMSClubID,
       Club.dim_club_key AS DimClubKey,
	   Club.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegion,
	   Club.club_name AS ClubName
  INTO #MMSClubIDList
  FROM #ClubID_list ClubID_list
   JOIN [marketing].[v_dim_club] Club
     ON ClubID_list.Item = Club.club_id
	  OR ClubID_list.Item = '-1'
   LEFT JOIN [marketing].[v_dim_description] MMSRegion
     ON Club.region_dim_description_key = MMSRegion.dim_description_key



----- Create Hierarchy temp table to return selected group names      

Exec [reporting].[proc_DimReportingHierarchy_History] @DivisionList,@SubdivisionList,@DepartmentMinDimReportingHierarchyKeyList,'All Product Groups',@ReportStartDimDateKey,@ReportEndDimDateKey

IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy; 

 SELECT DimReportingHierarchyKey,
       DivisionName,
       SubdivisionName,
       DepartmentName,
	   ProductGroupName,
	   RegionType,
	   ReportRegionType
 INTO #DimReportingHierarchy
 FROM #OuterOutputTable


DECLARE @HeaderDivisionList VARCHAR(8000) 
SET @HeaderDivisionList = REPLACE(@DivisionList, '|', ', ')

DECLARE @HeaderSubdivisionList VARCHAR(8000) 
SET @HeaderSubdivisionList = REPLACE(@SubdivisionList, '|', ', ')

DECLARE @HeaderSalesSourceList VARCHAR(4000)
SET @HeaderSalesSourceList = REPLACE(@SalesSourceList,'|',', ')


DECLARE @CurrencyCode VARCHAR(15)
SELECT @CurrencyCode = 'Local Currency'


IF OBJECT_ID('tempdb.dbo.#TransactionSummaryByProduct', 'U') IS NOT NULL
  DROP TABLE #TransactionSummaryByProduct; 

CREATE TABLE #TransactionSummaryByProduct (
Region    VARCHAR(50),
RegionDashClubName    VARCHAR(101),
ProductDepartment    VARCHAR(50),
SourceProductID    VARCHAR(61),
ProductDescription    VARCHAR(255),
SalesQuantity    INT,
SalesAmount    DECIMAL(26,6),
ChargesAmount    DECIMAL(26,6),
AdjustmentsAmount    DECIMAL(26,6),
RefundsAmount    DECIMAL(26,6),
TotalAmount    DECIMAL(26,6),
SalesSource    VARCHAR(50))

IF 'MMS' IN (SELECT SalesSource FROM #SalesSourceList)
  BEGIN

  INSERT INTO #TransactionSummaryByProduct
    SELECT DimLocation.MMSRegion AS Region,
           DimLocation.MMSRegion + '-' + DimLocation.ClubName AS RegionDashClubName,
           DimReportingHierarchy.reporting_department AS ProductDepartment,
           'MMS ' + CONVERT(VARCHAR,DimProduct.product_id) AS SourceProductID,
           DimProduct.product_description AS ProductDescription,
		   SUM(CASE WHEN FactSalesTransaction.sales_dollar_amount = 0 And FactSalesTransaction.Refund_Flag = 'Y'
		        THEN FactSalesTransaction.sales_quantity * -1
				WHEN FactSalesTransaction.sales_dollar_amount = 0 And FactSalesTransaction.Refund_Flag = 'N'
		        THEN FactSalesTransaction.sales_quantity
				ELSE FactSalesTransaction.sales_quantity * SIGN(FactSalesTransaction.sales_dollar_amount)
				END) SalesQuantity,
          -- SUM(FactSalesTransaction.sales_quantity) SalesQuantity,
           SUM(CASE WHEN FactSalesTransaction.pos_flag = 'Y' 
		             THEN FactSalesTransaction.sales_dollar_amount
                    ELSE 0 END ) SalesAmount,
           SUM(CASE WHEN FactSalesTransaction.membership_charge_flag = 'Y' 
		             THEN FactSalesTransaction.sales_dollar_amount
                    ELSE 0 END ) ChargesAmount,
           SUM(CASE WHEN FactSalesTransaction.membership_adjustment_flag = 'Y' 
		             THEN FactSalesTransaction.sales_dollar_amount
                    ELSE 0 END ) AdjustmentsAmount,
           SUM(CASE WHEN FactSalesTransaction.refund_flag = 'Y' 
		             THEN FactSalesTransaction.sales_dollar_amount
                    ELSE 0 END ) RefundsAmount,
           SUM(FactSalesTransaction.sales_dollar_amount) TotalAmount,
           'MMS' SalesSource

      FROM [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
      LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
        ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_item_key   ----- Comment out for DEV
      --ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_key            ----- Comment out for QA/PROD
	  JOIN #MMSClubIDList DimLocation
        ON FactSalesTransaction.transaction_reporting_dim_club_key = DimLocation.DimClubKey
	  JOIN [marketing].[v_dim_club] TransactionClub
	    ON FactSalesTransaction.dim_club_key = TransactionClub.dim_club_key
      JOIN [marketing].[v_dim_date] PostDimDate
        ON FactSalesTransaction.post_dim_date_key = PostDimDate.dim_date_key
      JOIN [marketing].[v_dim_mms_product_history] DimProduct
        ON FactSalesTransaction.dim_mms_product_key = DimProduct.dim_mms_product_key
       AND cast(DimProduct.effective_date_time as date) <= PostDimDate.month_ending_date
       AND cast(DimProduct.expiration_date_time as date) > PostDimDate.month_ending_date
      JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
        ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
       AND cast(DimReportingHierarchy.effective_dim_date_key as date) <= PostDimDate.month_ending_dim_date_key
       AND cast(DimReportingHierarchy.expiration_dim_date_key as date) > PostDimDate.month_ending_dim_date_key
      JOIN #DimReportingHierarchy
        ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key
      JOIN [marketing].[v_dim_mms_member_history] DimCustomer
        ON FactSalesTransaction.dim_mms_member_key = DimCustomer.dim_mms_member_key
       AND cast(DimCustomer.effective_date_time as date) <= PostDimDate.month_ending_date
       AND cast(DimCustomer.expiration_date_time as date) > PostDimDate.month_ending_date
      JOIN [marketing].[v_dim_mms_membership_history] FactMembership
        ON DimCustomer.membership_id = FactMembership.membership_id
       AND cast(FactMembership.effective_date_time as date) <= PostDimDate.month_ending_date
       AND cast(FactMembership.expiration_date_time as date) > PostDimDate.month_ending_date
      LEFT JOIN [marketing].[v_dim_mms_membership_type] MembershipDimProduct
        ON FactMembership.dim_mms_membership_type_key = MembershipDimProduct.dim_mms_membership_type_key
     WHERE FactSalesTransaction.post_dim_date_key >= @ReportStartDimDateKey
       AND FactSalesTransaction.post_dim_date_key<= @ReportEndDimDateKey
       AND FactSalesTransaction.voided_flag = 'N'
       AND (FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_item_key IS NULL           ----- Comment out for DEV
            OR (FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_item_key IS NOT NULL    ----- Comment out for DEV
            AND TransactionClub.club_id <> 13))                                                              ----- Comment out for DEV
       --AND (FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_key IS NULL              ----- Comment out for QA/PROD
       --     OR (FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_key IS NOT NULL      ----- Comment out for QA/PROD
       --     AND TransactionClub.club_id <> 13))                                                            ----- Comment out for QA/PROD
       AND (FactSalesTransaction.membership_adjustment_flag = 'Y'
            OR FactSalesTransaction.membership_charge_flag = 'Y'
            OR FactSalesTransaction.refund_flag = 'Y'
            OR FactSalesTransaction.pos_flag = 'Y')
       AND (@MembershipFilter = 'All Memberships'
            OR (@MembershipFilter = 'All Memberships - exclude Founders' AND MembershipDimProduct.attribute_founders_flag = 'N')
            OR (@MembershipFilter = 'Employee Memberships' AND MembershipDimProduct.attribute_employee_membership_flag = 'Y')
            OR (@MembershipFilter = 'Corporate Memberships' AND FactMembership.corporate_membership_flag = 'Y'))
     GROUP BY  DimLocation.MMSRegion,
	       DimLocation.MMSRegion + '-' + DimLocation.ClubName,
           DimReportingHierarchy.reporting_department,
           'MMS ' + CONVERT(VARCHAR,DimProduct.product_id),
           DimProduct.product_description


  INSERT INTO #TransactionSummaryByProduct
    SELECT DimLocation.MMSRegion AS Region,
           DimLocation.MMSRegion + '-' + DimLocation.ClubName AS RegionDashClubName,
           DimReportingHierarchy.reporting_department AS ProductDepartment,
           'MMS ' + CONVERT(VARCHAR,DimProduct.product_id) AS SourceProductID,
           DimProduct.product_description AS ProductDescription,
		   SUM(CASE WHEN FactSalesTransaction.sales_dollar_amount = 0 And FactSalesTransaction.Refund_Flag = 'Y'
		        THEN FactSalesTransaction.sales_quantity * -1
				WHEN FactSalesTransaction.sales_dollar_amount = 0 And FactSalesTransaction.Refund_Flag = 'N'
		        THEN FactSalesTransaction.sales_quantity
				ELSE FactSalesTransaction.sales_quantity * SIGN(FactSalesTransaction.sales_dollar_amount)
				END) SalesQuantity,
           --SUM(FactSalesTransaction.sales_quantity) SalesQuantity,
           SUM(CASE WHEN FactSalesTransaction.pos_flag = 'Y' THEN FactSalesTransaction.sales_dollar_amount
                    ELSE 0 END) SalesAmount,
           SUM(CASE WHEN FactSalesTransaction.membership_charge_flag = 'Y' THEN FactSalesTransaction.sales_dollar_amount
                    ELSE 0 END ) ChargesAmount,
           SUM(CASE WHEN FactSalesTransaction.membership_adjustment_flag = 'Y' THEN FactSalesTransaction.sales_dollar_amount
                    ELSE 0 END ) AdjustmentsAmount,
           SUM(CASE WHEN FactSalesTransaction.refund_flag = 'Y' THEN FactSalesTransaction.sales_dollar_amount
                    ELSE 0 END ) RefundsAmount,
           SUM( FactSalesTransaction.sales_dollar_amount ) TotalAmount,
           'MMS' SalesSource
      FROM [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
      JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
        ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_item_key   ----- Comment out in DEV
          --ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_key        ----- Comment out in QA/PROD
	  JOIN #MMSClubIDList DimLocation
        ON FactSalesTransactionAutomatedRefund.original_transaction_reporting_dim_club_key = DimLocation.DimClubKey
      JOIN [marketing].[v_dim_club] TransactionClub
	    ON FactSalesTransaction.dim_club_key = TransactionClub.dim_club_key
	  JOIN [marketing].[v_dim_date] PostDimDate
        ON FactSalesTransaction.post_dim_date_key = PostDimDate.dim_date_key
      JOIN [marketing].[v_dim_mms_product_history] DimProduct
        ON FactSalesTransaction.dim_mms_product_key = DimProduct.dim_mms_product_key
       AND cast(DimProduct.effective_date_time as date) <= PostDimDate.month_ending_date
       AND cast(DimProduct.expiration_date_time as date) > PostDimDate.month_ending_date
      JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
        ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
       AND cast(DimReportingHierarchy.effective_dim_date_key as date) <= PostDimDate.month_ending_dim_date_key
       AND cast(DimReportingHierarchy.expiration_dim_date_key as date) > PostDimDate.month_ending_dim_date_key
      JOIN #DimReportingHierarchy
        ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key
      JOIN [marketing].[v_dim_mms_member_history] DimCustomer
        ON FactSalesTransaction.dim_mms_member_key = DimCustomer.dim_mms_member_key
       AND cast(DimCustomer.effective_date_time as date) <= PostDimDate.month_ending_date
       AND cast(DimCustomer.expiration_date_time as date) > PostDimDate.month_ending_date
      JOIN [marketing].[v_dim_mms_membership_history] FactMembership
        ON DimCustomer.membership_id = FactMembership.membership_id
       AND cast(FactMembership.effective_date_time as date) <= PostDimDate.month_ending_date
       AND cast(FactMembership.expiration_date_time as date) > PostDimDate.month_ending_date
      LEFT JOIN [marketing].[v_dim_mms_membership_type] MembershipDimProduct
        ON FactMembership.dim_mms_membership_type_key = MembershipDimProduct.dim_mms_membership_type_key
     WHERE FactSalesTransaction.post_dim_date_key >= @ReportStartDimDateKey
       AND FactSalesTransaction.post_dim_date_key <= @ReportEndDimDateKey
       AND FactSalesTransaction.voided_flag = 'N'
       AND TransactionClub.club_id = 13
       AND (FactSalesTransaction.membership_adjustment_flag = 'Y'
            OR FactSalesTransaction.membership_charge_flag = 'Y'
            OR FactSalesTransaction.refund_flag = 'Y'
            OR FactSalesTransaction.pos_flag = 'Y')
       AND (@MembershipFilter = 'All Memberships'
            OR (@MembershipFilter = 'All Memberships - exclude Founders' AND MembershipDimProduct.attribute_founders_flag = 'N')
            OR (@MembershipFilter = 'Employee Memberships' AND MembershipDimProduct.attribute_employee_membership_flag  = 'Y')
            OR (@MembershipFilter = 'Corporate Memberships' AND FactMembership.corporate_membership_flag = 'Y'))
     GROUP BY DimLocation.MMSRegion,
              DimLocation.MMSRegion + '-' + DimLocation.ClubName,
              DimReportingHierarchy.reporting_department,
              'MMS ' + CONVERT(VARCHAR,DimProduct.product_id),
              DimProduct.product_description

  END


IF 'Cafe' IN (SELECT SalesSource FROM #SalesSourceList)
  BEGIN
     INSERT INTO #TransactionSummaryByProduct
    SELECT DimLocation.MMSRegion AS Region,
           DimLocation.MMSRegion + '-' + DimLocation.ClubName AS RegionDashClubName,
           DimReportingHierarchy.reporting_department AS ProductDepartment,
           'Cafe ' + CONVERT(VARCHAR,DimCafeProduct.menu_item_id) AS SourceProductID,
           DimCafeProduct.menu_item_name AS ProductDescription,
           SUM(FactCafePOSSalesTransaction.item_quantity) SalesQuantity,
           SUM(FactCafePOSSalesTransaction.item_sales_dollar_amount_excluding_tax) SalesAmount,
           0 ChargesAmount,
           0 AdjustmentsAmount,
           0 RefundsAmount,
           SUM(FactCafePOSSalesTransaction.item_sales_dollar_amount_excluding_tax) TotalAmount,
           'Cafe' SalesSource
      FROM [marketing].[v_fact_cafe_transaction_item] FactCafePOSSalesTransaction
      JOIN #MMSClubIDList DimLocation
        ON FactCafePOSSalesTransaction.dim_club_key =  DimLocation.DimClubKey
      JOIN [marketing].[v_dim_date] TransactionCloseDimDate
        ON FactCafePOSSalesTransaction.order_close_dim_date_key = TransactionCloseDimDate.dim_date_key
      JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct
        ON FactCafePOSSalesTransaction.dim_cafe_product_key= DimCafeProduct.dim_cafe_product_key
       AND cast(DimCafeProduct.effective_date_time as date) <= TransactionCloseDimDate.month_ending_date
       AND cast(DimCafeProduct.expiration_date_time as date) > TransactionCloseDimDate.month_ending_date
      JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
        ON DimReportingHierarchy.dim_reporting_hierarchy_key = DimCafeProduct.dim_reporting_hierarchy_key
       And cast(DimReportingHierarchy.effective_dim_date_key as date) <= TransactionCloseDimDate.month_ending_dim_date_key
       And cast(DimReportingHierarchy.expiration_dim_date_key as date) > TransactionCloseDimDate.month_ending_dim_date_key                             
      JOIN #DimReportingHierarchy
        ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key 
     WHERE FactCafePOSSalesTransaction.order_close_dim_date_key >= @ReportStartDimDateKey  ------- As in LTFDW - All records returned regardless of selected Membership Filter
       AND FactCafePOSSalesTransaction.order_close_dim_date_key <= @ReportEndDimDateKey
       AND FactCafePOSSalesTransaction.item_voided_flag = 'N'
       AND (FactCafePOSSalesTransaction.order_void_flag = 'N'
            OR FactCafePOSSalesTransaction.order_refund_flag = 'N')
     GROUP BY DimLocation.MMSRegion,
              DimLocation.MMSRegion + '-' + DimLocation.ClubName,
              'Cafe ' + CONVERT(VARCHAR,DimCafeProduct.menu_item_id),
              DimCafeProduct.menu_item_name,
              DimReportingHierarchy.reporting_department
  END



 

IF 'Hybris' IN (SELECT SalesSource FROM #SalesSourceList)
  BEGIN
     INSERT INTO #TransactionSummaryByProduct
    SELECT DimLocation.MMSRegion AS Region,
           DimLocation.MMSRegion + '-' + DimLocation.ClubName AS RegionDashClubName,
           DimReportingHierarchy.reporting_department AS ProductDepartment,
           'Hybris ' + DimECommerceProduct.code AS SourceProductID,
           DimECommerceProduct.name AS ProductDescription,
           SUM(FactECommerceSalesTransaction.transaction_quantity) SalesQuantity,
           SUM(CASE WHEN FactECommerceSalesTransaction.refund_flag = 'N'
                         THEN FactECommerceSalesTransaction.transaction_amount_gross
                    ELSE 0
               END) SalesAmount,
           0 ChargesAmount,
           0 AdjustmentsAmount,
           SUM(CASE WHEN FactECommerceSalesTransaction.refund_flag = 'Y'
                         THEN FactECommerceSalesTransaction.transaction_amount_gross 
					ELSE 0
               END) RefundsAmount,
           SUM(FactECommerceSalesTransaction.transaction_amount_gross) TotalAmount,
           'Hybris' SalesSource
	
      FROM [marketing].[v_fact_hybris_transaction_item] FactECommerceSalesTransaction
      JOIN #MMSClubIDList DimLocation
        ON FactECommerceSalesTransaction.transaction_reporting_dim_club_key = DimLocation.DimClubKey
      JOIN [marketing].[v_dim_date] ShipmentDimDate
        ON FactECommerceSalesTransaction.settlement_dim_date_key = ShipmentDimDate.dim_date_key
      LEFT JOIN [marketing].[v_dim_hybris_product_history] DimECommerceProduct
        ON FactECommerceSalesTransaction.dim_hybris_product_key = DimECommerceProduct.dim_hybris_product_key
       AND cast(DimECommerceProduct.effective_date_time as date) <= ShipmentDimDate.month_ending_date
       AND cast(DimECommerceProduct.expiration_date_time as date) > ShipmentDimDate.month_ending_date
      JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
        ON DimECommerceProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
       AND cast(DimReportingHierarchy.effective_dim_date_key as date) <= ShipmentDimDate.month_ending_dim_date_key
       AND cast(DimReportingHierarchy.expiration_dim_date_key as date) > ShipmentDimDate.month_ending_dim_date_key
      JOIN #DimReportingHierarchy
        ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key
      LEFT JOIN [marketing].[v_dim_mms_member_history] DimCustomer
        ON FactECommerceSalesTransaction.dim_mms_member_key = DimCustomer.dim_mms_member_key
       AND cast(DimCustomer.effective_date_time as date) <= ShipmentDimDate.month_ending_date
       AND cast(DimCustomer.expiration_date_time as date) > ShipmentDimDate.month_ending_date
      LEFT JOIN [marketing].[v_dim_mms_membership_history] FactMembership
        ON DimCustomer.membership_id = FactMembership.membership_id
       AND cast(FactMembership.effective_date_time as date) <= ShipmentDimDate.month_ending_date
       AND cast(FactMembership.expiration_date_time as date) > ShipmentDimDate.month_ending_date
      LEFT JOIN [marketing].[v_dim_mms_membership_type] MembershipDimProduct
        ON FactMembership.dim_mms_membership_type_key = MembershipDimProduct.dim_mms_membership_type_key
     WHERE FactECommerceSalesTransaction.settlement_dim_date_key >= @ReportStartDimDateKey
       AND FactECommerceSalesTransaction.settlement_dim_date_key <= @ReportEndDimDateKey
       AND (FactECommerceSalesTransaction.dim_mms_member_key Is Null               ------
	            OR (FactECommerceSalesTransaction.dim_mms_member_key < '0'         ------As in LTFDW, these filters will always return all records where dim_mms_member_key < '0', or NULL,regardless of selected Membership Filter
                OR (@MembershipFilter = 'All Memberships' 
                OR (@MembershipFilter = 'All Memberships - exclude Founders' AND MembershipDimProduct.attribute_founders_flag = 'N')
                OR (@MembershipFilter = 'Employee Memberships' AND MembershipDimProduct.attribute_employee_membership_flag = 'Y')
                OR (@MembershipFilter = 'Corporate Memberships' AND FactMembership.corporate_membership_flag = 'Y'))))
     GROUP BY DimLocation.MMSRegion,
              DimLocation.MMSRegion + '-' + DimLocation.ClubName,
              'Hybris ' + DimECommerceProduct.code,
              DimECommerceProduct.name,
              DimReportingHierarchy.reporting_department
  END



IF 'HealthCheckUSA' IN (SELECT SalesSource FROM #SalesSourceList)
  BEGIN
      INSERT INTO #TransactionSummaryByProduct
    SELECT DimLocation.MMSRegion AS Region,
           DimLocation.MMSRegion + '-' + DimLocation.ClubName AS RegionDashClubName,
           DimReportingHierarchy.reporting_department AS ProductDepartment,
           'HealthCheckUSA ' + CAST(DimHCUSAProduct.product_sku AS VARCHAR(50)) AS SourceProductID,
           DimHCUSAProduct.product_description AS ProductDescription,                                   
           SUM(FactHealthCheckUSASalesTransactionItem.sales_quantity) SalesQuantity,
           SUM(CASE WHEN FactHealthCheckUSASalesTransactionItem.refund_flag = 'N' 
                         THEN FactHealthCheckUSASalesTransactionItem.sales_amount
                    ELSE 0 END) SalesAmount,
           0 ChargesAmount,
           0 AdjustmentsAmount,
           SUM(CASE WHEN FactHealthCheckUSASalesTransactionItem.refund_flag = 'Y' 
                         THEN FactHealthCheckUSASalesTransactionItem.sales_amount
                    ELSE 0 END) RefundsAmount,
           SUM(FactHealthCheckUSASalesTransactionItem.sales_amount) TotalAmount,
           'HealthCheckUSA' SalesSource

      FROM [marketing].[v_fact_healthcheckusa_transaction_item] FactHealthCheckUSASalesTransactionItem
      JOIN #MMSClubIDList DimLocation
        ON FactHealthCheckUSASalesTransactionItem.transaction_reporting_dim_club_key = DimLocation.DimClubKey
      JOIN [marketing].[v_dim_date] TransactionPostDimDate
        ON FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key = TransactionPostDimDate.dim_date_key
      JOIN [marketing].[v_dim_healthcheckusa_product_history] DimHCUSAProduct
        ON FactHealthCheckUSASalesTransactionItem.dim_healthcheckusa_product_key = DimHCUSAProduct.dim_healthcheckusa_product_key      
       AND cast(DimHCUSAProduct.effective_date_time as date) <= TransactionPostDimDate.month_ending_date
       AND cast(DimHCUSAProduct.expiration_date_time as date) > TransactionPostDimDate.month_ending_date
      JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
        On DimHCUSAProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
       And cast(DimReportingHierarchy.effective_dim_date_key as date) <= TransactionPostDimDate.month_ending_dim_date_key
       And cast(DimReportingHierarchy.expiration_dim_date_key as date) > TransactionPostDimDate.month_ending_dim_date_key
      JOIN #DimReportingHierarchy
        ON DimReportingHierarchy.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey
      --JOIN [marketing].[v_dim_mms_member_history] DimCustomer
      --  ON FactHealthCheckUSASalesTransactionItem.dim_mms_member_key = DimCustomer.dim_mms_member_key
      -- AND DimCustomer.effective_date_time <= TransactionPostDimDate.month_ending_date
      -- AND DimCustomer.expiration_date_time > TransactionPostDimDate.month_ending_date
      --LEFT JOIN [marketing].[v_dim_mms_membership_history] FactMembership
      --  ON DimCustomer.dim_mms_membership_key = FactMembership.dim_mms_membership_key
      -- AND FactMembership.effective_date_time <= TransactionPostDimDate.month_ending_date
      -- AND FactMembership.expiration_date_time > TransactionPostDimDate.month_ending_date
      --LEFT JOIN [marketing].[v_dim_mms_membership_type] MembershipDimProduct
      --  ON FactMembership.dim_mms_membership_type_key = MembershipDimProduct.dim_mms_membership_type_key
     WHERE FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key >= @ReportStartDimDateKey
       AND FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key <= @ReportEndDimDateKey
       --AND (FactHealthCheckUSASalesTransactionItem.dim_mms_member_key < '0' Or FactHealthCheckUSASalesTransactionItem.dim_mms_member_key  Is Null     ------ AS in LTFDW, this filter will always return all records where dim_mms_member_key < '0', or NULL,  regardless of selected Membership Filter
       --     OR (@MembershipFilter = 'All Memberships' 
       --         OR (@MembershipFilter = 'All Memberships - exclude Founders' AND MembershipDimProduct.attribute_founders_flag = 'N')
       --         OR (@MembershipFilter = 'Employee Memberships' AND MembershipDimProduct.attribute_employee_membership_flag = 'Y')
       --         OR (@MembershipFilter = 'Corporate Memberships' AND FactMembership.corporate_membership_flag = 'Y')))
     GROUP BY DimLocation.MMSRegion,
              DimLocation.MMSRegion + '-' + DimLocation.ClubName,
              'HealthCheckUSA ' + CAST(DimHCUSAProduct.product_sku AS VARCHAR(50)),
              DimHCUSAProduct.product_description,                      
              DimReportingHierarchy.reporting_department
  END
  
  IF 'Magento' IN (SELECT SalesSource FROM #SalesSourceList)
  BEGIN
     INSERT INTO #TransactionSummaryByProduct
    SELECT DimLocation.MMSRegion AS Region,
           DimLocation.MMSRegion + '-' + DimLocation.ClubName AS RegionDashClubName,
           DimReportingHierarchy.reporting_department AS ProductDepartment,
           'Magento ' + DimMagentoProduct.product_id AS SourceProductID,
           DimMagentoProduct.product_name AS ProductDescription,
           SUM(FactMagentoSalesTransaction.transaction_quantity) SalesQuantity,
           SUM(CASE WHEN FactMagentoSalesTransaction.refund_flag = 'N'
                         THEN FactMagentoSalesTransaction.transaction_amount
                    ELSE 0
               END) SalesAmount,
           0 ChargesAmount,
           0 AdjustmentsAmount,
           SUM(CASE WHEN FactMagentoSalesTransaction.refund_flag = 'Y'
                         THEN FactMagentoSalesTransaction.transaction_amount 
					ELSE 0
               END) RefundsAmount,
           SUM(FactMagentoSalesTransaction.transaction_amount) TotalAmount,
           'Magento' SalesSource
	
      FROM [marketing].[v_fact_magento_transaction_item] FactMagentoSalesTransaction
      JOIN #MMSClubIDList DimLocation
        ON FactMagentoSalesTransaction.transaction_reporting_dim_club_key = DimLocation.DimClubKey
      JOIN [marketing].[v_dim_date] ShipmentDimDate
        ON FactMagentoSalesTransaction.transaction_dim_date_key = ShipmentDimDate.dim_date_key
      LEFT JOIN [marketing].[v_dim_magento_product_history] DimMagentoProduct
        ON FactMagentoSalesTransaction.dim_magento_product_key = DimMagentoProduct.dim_magento_product_key
       AND cast(DimMagentoProduct.effective_date_time as date) <= ShipmentDimDate.month_ending_date
       AND cast(DimMagentoProduct.expiration_date_time as date) > ShipmentDimDate.month_ending_date
      JOIN [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
        ON DimMagentoProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.dim_reporting_hierarchy_key
       AND cast(DimReportingHierarchy.effective_dim_date_key as date) <= ShipmentDimDate.month_ending_dim_date_key
       AND cast(DimReportingHierarchy.expiration_dim_date_key as date) > ShipmentDimDate.month_ending_dim_date_key
      JOIN #DimReportingHierarchy
        ON #DimReportingHierarchy.DimReportingHierarchyKey = DimReportingHierarchy.dim_reporting_hierarchy_key
      LEFT JOIN [marketing].[v_dim_mms_member_history] DimCustomer
        ON FactMagentoSalesTransaction.dim_mms_member_key = DimCustomer.dim_mms_member_key
       AND cast(DimCustomer.effective_date_time as date) <= ShipmentDimDate.month_ending_date
       AND cast(DimCustomer.expiration_date_time as date) > ShipmentDimDate.month_ending_date
      LEFT JOIN [marketing].[v_dim_mms_membership_history] FactMembership
        ON DimCustomer.membership_id = FactMembership.membership_id
       AND cast(FactMembership.effective_date_time as date) <= ShipmentDimDate.month_ending_date
       AND cast(FactMembership.expiration_date_time as date) > ShipmentDimDate.month_ending_date
      LEFT JOIN [marketing].[v_dim_mms_membership_type] MembershipDimProduct
        ON FactMembership.dim_mms_membership_type_key = MembershipDimProduct.dim_mms_membership_type_key
     WHERE FactMagentoSalesTransaction.transaction_dim_date_key >= @ReportStartDimDateKey
       AND FactMagentoSalesTransaction.transaction_dim_date_key <= @ReportEndDimDateKey
       AND (FactMagentoSalesTransaction.dim_mms_member_key Is Null               ------
	            OR (FactMagentoSalesTransaction.dim_mms_member_key < '0'         ------As in LTFDW, these filters will always return all records where dim_mms_member_key < '0', or NULL,regardless of selected Membership Filter
                OR (@MembershipFilter = 'All Memberships' 
                OR (@MembershipFilter = 'All Memberships - exclude Founders' AND MembershipDimProduct.attribute_founders_flag = 'N')
                OR (@MembershipFilter = 'Employee Memberships' AND MembershipDimProduct.attribute_employee_membership_flag = 'Y')
                OR (@MembershipFilter = 'Corporate Memberships' AND FactMembership.corporate_membership_flag = 'Y'))))
     GROUP BY DimLocation.MMSRegion,
              DimLocation.MMSRegion + '-' + DimLocation.ClubName,
              'Magento ' + DimMagentoProduct.product_id,
              DimMagentoProduct.product_name,
              DimReportingHierarchy.reporting_department
  END


SELECT Region,
       RegionDashClubName,
       ProductDepartment,
       SourceProductID,
       ProductDescription,
       SUM(SalesQuantity) SalesQuantity,
       CAST(SUM(SalesAmount) AS DECIMAL(12,2)) SalesAmount,
       CAST(SUM(ChargesAmount) AS DECIMAL(12,2)) ChargesAmount,
       CAST(SUM(AdjustmentsAmount) AS DECIMAL(12,2)) AdjustmentsAmount,
       CAST(SUM(RefundsAmount) AS DECIMAL(12,2)) RefundsAmount,
       CAST(SUM(TotalAmount) AS DECIMAL(12,2)) TotalAmount,
       SalesSource,
       @CurrencyCode ReportingCurrencyCode,
       @ReportRunDateTime ReportRunDateTime,
       @HeaderDateRange HeaderDateRange,
       CAST('' AS VARCHAR(71)) HeaderEmptyResult,
       --@HeaderDepartmentList HeaderDepartmentList,   -------- This will need to be built within Cognos from the prompt values
       @HeaderSalesSourceList HeaderSalesSourceList,
       @HeaderDivisionList HeaderDivisionList,
       @HeaderSubDivisionlist HeaderSubDivisionList
  FROM #TransactionSummaryByProduct
 GROUP BY Region,
          RegionDashClubName,
          ProductDepartment,
          SourceProductID,
          ProductDescription,
          SalesSource
UNION ALL
SELECT CAST(NULL AS VARCHAR(50)) Region,
       CAST(NULL AS VARCHAR(101)) RegionDashClubName,
       CAST(NULL AS VARCHAR(61)) ProductDepartment,
       CAST(NULL AS VARCHAR(50)) SourceProductID,
       CAST(NULL AS VARCHAR(255)) ProductDescription,
       NULL SalesQuantity,
       CAST(NULL AS DECIMAL(12,2)) SalesAmount,
       CAST(NULL AS DECIMAL(12,2)) ChargesAmount,
       CAST(NULL AS DECIMAL(12,2)) AdjustmentsAmount,
       CAST(NULL AS DECIMAL(12,2)) RefundsAmount,
       CAST(NULL AS DECIMAL(12,2)) TotalAmount,
       CAST(NULL AS VARCHAR(10)) SalesSource,
       CAST(NULL AS VARCHAR(18)) ReportingCurrencyCode,
       @ReportRunDateTime ReportRunDateTime,
       @HeaderDateRange HeaderDateRange,
       'There is no data available for the selected parameters.  Please re-try.' HeaderEmptyResult,
       --NULL HeaderDepartmentList,      -------- This will need to be built within Cognos from the prompt values
       @HeaderSalesSourceList HeaderSalesSourceList,
       @HeaderDivisionList HeaderDivisionList,
       @HeaderSubDivisionList HeaderSubDivisionList
 WHERE (SELECT COUNT(*) FROM #TransactionSummaryByProduct) = 0
 ORDER BY RegionDashClubName, ProductDepartment, ProductDescription
 

   DROP TABLE #SalesSourceList
   DROP TABLE #MMSClubIDList
   DROP TABLE #DimReportingHierarchy
   DROP TABLE #TransactionSummaryByProduct



END
