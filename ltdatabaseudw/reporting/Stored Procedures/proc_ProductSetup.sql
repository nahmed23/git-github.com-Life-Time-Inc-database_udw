CREATE PROC [reporting].[proc_ProductSetup] @StartDate [DATETIME],@SourceSystemList [VARCHAR](50),@DeptMinDimHierarchyKeyList [VARCHAR](8000),@ProductStatusList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


 ------- Execution Samples
 ------- Exec [reporting].[proc_ProductSetup]'1/22/2019','Hybris','All Departments','Active|Inactive|Obsolete'
 ------- Exec [reporting].[proc_ProductSetup]'1/22/2019','HealthCheckUSA','All Departments','Active'
 ------- Exec [reporting].[proc_ProductSetup]'1/22/2019','MMS','All Departments','Active|Inactive'
 ------- Exec [reporting].[proc_ProductSetup]'1/22/2019','Cafe|HealthCheckUSA|Magento','All Departments','Active'

-- Use map_utc_time_zone_conversion to determine correct 'current' time, factoring in daylight savings / non daylight savings 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

DECLARE @ReportDate VARCHAR(12),
        @DimDateKey VARCHAR(32) 
SELECT @ReportDate = standard_date_name,
       @DimDateKey = dim_date_key
  FROM [marketing].[v_dim_date]
 WHERE calendar_date = @StartDate


   ----- Create MMS Department name temp table to return selected dept names
IF OBJECT_ID('tempdb.dbo.#SourceSystems', 'U') IS NOT NULL
  DROP TABLE #SourceSystems; 
 
  ----- Create SourceSystem temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'SourceSystemList'
  EXEC marketing.proc_parse_pipe_list @SourceSystemList,@list_table

SELECT #SourceSystemList.Item  AS SourceSystem    
  INTO #SourceSystems
  FROM #SourceSystemList


DECLARE @HeaderSourceSystemList VARCHAR(50)
SET @HeaderSourceSystemList = REPLACE(@SourceSystemList, '|', ', ')



 
----- Create Hierarchy temp table to return selected group names      

Exec [reporting].[proc_DimReportingHierarchy_History] 'All Divisions','All Subdivisions',@DeptMinDimHierarchyKeyList,'All Product Groups',@DimDateKey,@DimDateKey

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


 ----- to find the "default" hierarchy key for all non-assigned products by source and set this as a variable
IF OBJECT_ID('tempdb.dbo.#SourceDefaultKeys', 'U') IS NOT NULL
  DROP TABLE #SourceDefaultKeys; 

 Select 'MMS' AS SourceSystem,
       dim_reporting_hierarchy_key
	INTO #SourceDefaultKeys   
 FROM [marketing].[v_dim_reporting_hierarchy]
 Where [reporting_division] = 'MMS'
   AND [reporting_sub_division] = 'MMS'
   AND [reporting_department] = 'MMS'
   AND [reporting_product_group]= ''

UNION

 SELECT 'Hybris' AS SourceSystem,
       dim_reporting_hierarchy_key
 FROM [marketing].[v_dim_reporting_hierarchy]
 WHERE [reporting_division] = 'E-Commerce'
   AND [reporting_sub_division] = 'E-Commerce'
   AND [reporting_department] = 'E-Commerce'
   AND [reporting_product_group]= ''

UNION

 SELECT 'HealthCheckUSA' AS SourceSystem,
       dim_reporting_hierarchy_key
 FROM [marketing].[v_dim_reporting_hierarchy]
 WHERE [reporting_division] = 'HealthCheckUSA'
   AND [reporting_sub_division] = 'HealthCheckUSA'
   AND [reporting_department] = 'HealthCheckUSA'
      AND [reporting_product_group]= ''

UNION


 SELECT 'Cafe' AS SourceSystem,
       dim_reporting_hierarchy_key
 FROM [marketing].[v_dim_reporting_hierarchy]
 WHERE [reporting_division] = 'LifeCafe'
   AND [reporting_sub_division] = 'LifeCafe'
   AND [reporting_department] = 'LifeCafe'
      AND [reporting_product_group]= ''
   
 UNION
 
 SELECT 'Magento' AS SourceSystem,
       dim_reporting_hierarchy_key
 FROM [marketing].[v_dim_reporting_hierarchy]
 WHERE [reporting_division] = 'Magento'
   AND [reporting_sub_division] = 'Magento'
   AND [reporting_department] = 'Magento'
      AND [reporting_product_group]= ''

DECLARE  @MMSDefaultKey Varchar(32)
SET @MMSDefaultKey = (SELECT dim_reporting_hierarchy_key FROM #SourceDefaultKeys WHERE SourceSystem = 'MMS')
DECLARE  @CafeDefaultKey Varchar(32)
SET @CafeDefaultKey = (SELECT dim_reporting_hierarchy_key FROM #SourceDefaultKeys WHERE SourceSystem = 'Cafe')
DECLARE  @HybrisDefaultKey Varchar(32)
SET @HybrisDefaultKey = (SELECT dim_reporting_hierarchy_key FROM #SourceDefaultKeys WHERE SourceSystem = 'Hybris')
DECLARE  @HCUSADefaultKey Varchar(32)
SET @HCUSADefaultKey = (SELECT dim_reporting_hierarchy_key FROM #SourceDefaultKeys WHERE SourceSystem = 'HealthCheckUSA')
DECLARE  @MagentoDefaultKey Varchar(32)
SET @MagentoDefaultKey = (SELECT dim_reporting_hierarchy_key FROM #SourceDefaultKeys WHERE SourceSystem = 'Magento')


   ----- Create Product Status temp table to return selected statuses
IF OBJECT_ID('tempdb.dbo.#ProductStatusList', 'U') IS NOT NULL
  DROP TABLE #ProductStatusList;

  ----- Create #ProductStatusList temp table
SET @list_table = 'ProductStatus'
  EXEC marketing.proc_parse_pipe_list @ProductStatusList,@list_table

SELECT #ProductStatus.Item  AS ProductStatus,
       CASE WHEN  #ProductStatus.Item = 'Active'
	        THEN  'Y'
			WHEN  #ProductStatus.Item = 'Inactive'  
			THEN 'N'
			ELSE 'O'       ------- Cafe products are either Active or Inactive never Obsolete
			END CafeMenuItemActiveFlagMapping,
       CASE WHEN  #ProductStatus.Item = 'Active'
	        THEN  1
			WHEN  #ProductStatus.Item = 'Inactive'  
			THEN 2
			ELSE 0      ------- Magento products are either Enabled or Disabled never Obsolete 
			END MagentoProductStatusMapping
  INTO #ProductStatusList
  FROM #ProductStatus


DECLARE @FirstSelectedStatus VARCHAR(8)
SET @FirstSelectedStatus = (SELECT MAX(ProductStatus) FROM #ProductStatusList)

DECLARE @HeaderProductStatusList VARCHAR(8000)
SET @HeaderProductStatusList = REPLACE(@ProductStatusList,'|',',')


IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results; 


SELECT 'Hybris' AS  SourceSystem,
       Cast(DimECommerceProduct.code AS VARCHAR(258)) AS ProductID,
       Cast(DimECommerceProduct.code AS VARCHAR(258)) AS ProductSKU,
       DimECommerceProduct.name AS ProductDescription,
       NULL ProductGLAccount,
       NULL ProductGLDepartmentCode,
       NULL ProductGLProductCode,
       NULL ProductRefundGLAccount,
       NULL ProductDiscountGLAccount,
       DimECommerceProduct.reporting_department AS ReportingDepartment,  
       DimECommerceProduct.reporting_department AS ReportingDepartmentForNonCommissionedSales,
       NULL MMSDepartment,
       NULL MMSProductDisplayUIFlag,
       NULL MMSproductGLOverrideClubID,
       NULL MMSRecurrentProductTypeDescription,
       NULL MMSPackageProductFlag,
       NULL MMSProductTipAllowedFlag,
       DimECommerceProduct.product_status AS ProductStatus, 
       DimECommerceProduct.payroll_description AS PayrollExtractDescription,   
       DimECommerceProduct.payroll_standard_product_group_flag AS PayrollStandardProductGroupFlag,     
       DimECommerceProduct.payroll_standard_group_description  AS PayrollProductGroupDescription,
       DimECommerceProduct.payroll_standard_group_sort_order AS PayrollExtractExportStandardSortOrder,
       DimECommerceProduct.payroll_lt_bucks_product_group_flag AS PayrollMyLTBucksProductGroupFlag,  
       DimECommerceProduct.payroll_lt_bucks_group_description AS PayrollMyLTBucksProductGroupDescription,
       DimECommerceProduct.payroll_lt_bucks_group_sort_order AS PayrollExtractExportMyLTBucksSortOrder,
       DimECommerceProduct.payroll_track_sales_flag AS PayrollTrackSalesFlag,       
       DimECommerceProduct.payroll_standard_sales_amount_flag AS PayrollSalesAmountFlag,
       DimECommerceProduct.payroll_lt_bucks_sales_amount_flag AS PayrollMyLTBucksSalesAmountFlag,
       DimECommerceProduct.payroll_track_service_flag AS PayrollTrackServiceFlag,    
       DimECommerceProduct.payroll_standard_service_amount_flag AS PayrollServiceAmountFlag,
       DimECommerceProduct.payroll_standard_service_quantity_flag  AS PayrollServiceQuantityFlag,
       DimECommerceProduct.payroll_lt_bucks_service_amount_flag  AS PayrollMyLTBucksServiceAmountFlag,
       DimECommerceProduct.payroll_lt_bucks_service_quantity_flag  AS PayrollMyLTBucksServiceQuantityFlag,
       NULL  RevenueProductGroupSortOrder,     ---------- Not available - if needed , just use alpha sort
       DimECommerceProduct.reporting_product_group AS RevenueProductGroupDescription,
       NULL RevenueProductGroupGLAccount,
       NULL RevenueProductGroupRefundGLAccount,  
       NULL RevenueProductGroupDiscountGLAccount,   
       NULL RevenueAllocationRuleName,
       NULL SalesCategoryDescription,
       DimECommerceProduct.reporting_region_type AS RevenueReportingRegionType,
       DimECommerceProduct.payroll_region_type  AS PayrollExtractRegionType,
       NULL AssessJuniorDuesFlag,
       NULL PackageProductCountAsHalfSessionFlag,
       NULL MTDAverageDeliveredSessionPriceFlag,
       DimECommerceProduct.mtd_average_sale_price_flag  AS MTDAverageSalePriceFlag,
       DimECommerceProduct.connectivity_lead_generator_flag  AS ConnectivityLeadGeneratorFlag,
       DimECommerceProduct.new_business_old_business_flag     AS NewBusinessOldBusinessFlag,
       NULL PackageProductSessionType,
       DimECommerceProduct.connectivity_primary_lead_generator_flag  AS ConnectivityPrimaryLeadGeneratorFlag,
       DimECommerceProduct.departmental_dssr_flag  AS DepartmentalDSSRFlag,
       'N' CorporateTransferFlag,
       NULL CorporateTransferMultiplier,
       'N' DSSRIFAdminFeeFlag,      
       'N' DSSRDowngradeOtherEnrollmentFeeFlag,    
       'N' ExperienceLifeMagazineFlag,    
       DimECommerceProduct.reporting_division AS DivisionName,
       DimECommerceProduct.reporting_sub_division AS SubdivisionName,
       NULL WorkdayAccount,
       NULL WorkdayCostCenter,
       NULL WorkdayOffering,
       NULL WorkdayOverRideRegion,
       NULL WorkdayRevenueProductGroupAccount,
       NULL DeferredRevenueFlag,
       NULL WorkdayRefundGLAccount,
       NULL WorkdayDiscountGLAccount,
       NULL WorkdayRevenueProductGroupRefundGLAccount,
       NULL WorkdayRevenueProductGroupDiscountGLAccount,
       DimECommerceProduct.ltf_offer_flag AS ECommerceOfferFlag
  INTO #Results
  FROM [marketing].[v_dim_hybris_product_history] DimECommerceProduct
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON IsNull(DimECommerceProduct.dim_reporting_hierarchy_key,@HybrisDefaultKey) = DimReportingHierarchy.DimReportingHierarchyKey
 WHERE 'Hybris' IN (SELECT SourceSystem FROM #SourceSystems)
   AND 'Active' IN (SELECT ProductStatus FROM #ProductStatusList)
   AND (DimECommerceProduct.dim_hybris_product_key Is Null OR  DimECommerceProduct.dim_hybris_product_key > '0' )   ------- removes the "unknown"  (-998) product records from being returned
   AND ((DimECommerceProduct.effective_date_time <= @StartDate
        AND DimECommerceProduct.expiration_date_time > @StartDate) OR DimECommerceProduct.effective_date_time Is Null)


UNION ALL


SELECT 'HealthCheckUSA' AS  SourceSystem,
       CAST(DimHealthCheckUSAProduct.product_sku as VARCHAR(258)) AS ProductID,
       CAST(DimHealthCheckUSAProduct.product_sku as VARCHAR(258)) AS ProductSKU,
       DimHealthCheckUSAProduct.product_description AS ProductDescription,  
       NULL ProductGLAccount,
       NULL ProductGLDepartmentCode,
       NULL ProductGLProductCode,
       NULL ProductRefundGLAccount,
       NULL ProductDiscountGLAccount,
       DimHealthCheckUSAProduct.reporting_department AS ReportingDepartment,  
       DimHealthCheckUSAProduct.reporting_department AS ReportingDepartmentForNonCommissionedSales,
       NULL MMSDepartment,
       NULL MMSProductDisplayUIFlag,
       NULL MMSproductGLOverrideClubID,
       NULL MMSRecurrentProductTypeDescription,
       NULL MMSPackageProductFlag,
       NULL MMSProductTipAllowedFlag,
       DimHealthCheckUSAProduct.product_status AS ProductStatus,
       DimHealthCheckUSAProduct.payroll_description AS PayrollExtractDescription,   
       DimHealthCheckUSAProduct.payroll_standard_product_group_flag AS PayrollStandardProductGroupFlag, 
       DimHealthCheckUSAProduct.payroll_standard_group_description  AS PayrollProductGroupDescription,
       DimHealthCheckUSAProduct.payroll_standard_group_sort_order AS PayrollExtractExportStandardSortOrder,
       DimHealthCheckUSAProduct.payroll_lt_bucks_product_group_flag AS PayrollMyLTBucksProductGroupFlag, 
       DimHealthCheckUSAProduct.payroll_lt_bucks_group_description AS PayrollMyLTBucksProductGroupDescription,
       DimHealthCheckUSAProduct.payroll_lt_bucks_group_sort_order AS PayrollExtractExportMyLTBucksSortOrder,
       DimHealthCheckUSAProduct.payroll_track_sales_flag AS PayrollTrackSalesFlag,
       DimHealthCheckUSAProduct.payroll_standard_sales_amount_flag AS PayrollSalesAmountFlag,
       DimHealthCheckUSAProduct.payroll_lt_bucks_sales_amount_flag AS PayrollMyLTBucksSalesAmountFlag,
       DimHealthCheckUSAProduct.payroll_track_service_flag AS PayrollTrackServiceFlag,
       DimHealthCheckUSAProduct.payroll_standard_service_amount_flag AS PayrollServiceAmountFlag,
       DimHealthCheckUSAProduct.payroll_standard_service_quantity_flag  AS PayrollServiceQuantityFlag,
       DimHealthCheckUSAProduct.payroll_lt_bucks_service_amount_flag  AS PayrollMyLTBucksServiceAmountFlag,
       DimHealthCheckUSAProduct.payroll_lt_bucks_service_quantity_flag  AS PayrollMyLTBucksServiceQuantityFlag,
       NULL  RevenueProductGroupSortOrder,     ---------- Not available - if needed, just use alpha sort
       DimHealthCheckUSAProduct.reporting_product_group AS RevenueProductGroupDescription,
       NULL RevenueProductGroupGLAccount,
       NULL RevenueProductGroupRefundGLAccount,  
       NULL RevenueProductGroupDiscountGLAccount,   
       NULL RevenueAllocationRuleName,
       NULL SalesCategoryDescription,
       DimHealthCheckUSAProduct.reporting_region_type AS RevenueReportingRegionType,
       DimHealthCheckUSAProduct.payroll_region_type  AS PayrollExtractRegionType,
       NULL AssessJuniorDuesFlag,
       NULL PackageProductCountAsHalfSessionFlag,
       NULL MTDAverageDeliveredSessionPriceFlag,
       DimHealthCheckUSAProduct.mtd_average_sale_price_flag  AS MTDAverageSalePriceFlag,
       DimHealthCheckUSAProduct.connectivity_lead_generator_flag  AS ConnectivityLeadGeneratorFlag,
       DimHealthCheckUSAProduct.new_business_old_business_flag  AS NewBusinessOldBusinessFlag,
       NULL PackageProductSessionType,
       DimHealthCheckUSAProduct.connectivity_primary_lead_generator_flag  AS ConnectivityPrimaryLeadGeneratorFlag,
       DimHealthCheckUSAProduct.departmental_dssr_flag  AS DepartmentalDSSRFlag,
       'N' CorporateTransferFlag,
       NULL CorporateTransferMultiplier,
       'N' DSSRIFAdminFeeFlag,      
       'N' DSSRDowngradeOtherEnrollmentFeeFlag,    
       'N' ExperienceLifeMagazineFlag,    
       DimHealthCheckUSAProduct.reporting_division AS DivisionName,
       DimHealthCheckUSAProduct.reporting_sub_division AS SubdivisionName,
       NULL WorkdayAccount,
       NULL WorkdayCostCenter,
       NULL WorkdayOffering,
       NULL WorkdayOverRideRegion,
       NULL WorkdayRevenueProductGroupAccount,
       NULL DeferredRevenueFlag,
       NULL WorkdayRefundGLAccount,
       NULL WorkdayDiscountGLAccount,
       NULL WorkdayRevenueProductGroupRefundGLAccount,
       NULL WorkdayRevenueProductGroupDiscountGLAccount,
       NULL ECommerceOfferFlag
  FROM [marketing].[v_dim_healthcheckusa_product_history] DimHealthCheckUSAProduct
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON IsNull(DimHealthCheckUSAProduct.dim_reporting_hierarchy_key,@HCUSADefaultKey) = DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #ProductStatusList
    ON IsNull(DimHealthCheckUSAProduct.product_status,@FirstSelectedStatus) = #ProductStatusList.ProductStatus 

 WHERE 'HealthCheckUSA' IN (SELECT SourceSystem FROM #SourceSystems)
      AND (DimHealthCheckUSAProduct.dim_healthcheckusa_product_key Is Null OR  DimHealthCheckUSAProduct.dim_healthcheckusa_product_key > '0' )   ------- removes the "unknown" (-998) product records from being returned
      AND ((DimHealthCheckUSAProduct.effective_date_time <= @StartDate
            AND DimHealthCheckUSAProduct.expiration_date_time > @StartDate) OR DimHealthCheckUSAProduct.effective_date_time is Null)



UNION ALL

SELECT 'Cafe' AS SourceSystem,
       CAST(DimCafeProduct.menu_item_id AS VARCHAR(258)) AS ProductID,
       CAST(DimCafeProduct.sku_number AS VARCHAR(258)) AS ProductSKU,
       DimCafeProduct.menu_item_name AS ProductDescription,
       NULL ProductGLAccount,
       NULL ProductGLDepartmentCode,
       NULL ProductGLProductCode,
       NULL ProductRefundGLAccount,
       NULL ProductDiscountGLAccount,
       DimCafeProduct.reporting_department AS ReportingDepartment,  
       DimCafeProduct.reporting_department AS ReportingDepartmentForNonCommissionedSales,
       NULL MMSDepartment,
       NULL MMSProductDisplayUIFlag,
       NULL MMSproductGLOverrideClubID,
       NULL MMSRecurrentProductTypeDescription,
       NULL MMSPackageProductFlag,
       NULL MMSProductTipAllowedFlag,
       CASE WHEN DimCafeProduct.menu_item_active_flag = 'Y'
	        THEN 'Active'
			ELSE 'Inactive'
			END ProductStatus,
       DimCafeProduct.payroll_description AS PayrollExtractDescription,  
       DimCafeProduct.payroll_standard_product_group_flag AS PayrollStandardProductGroupFlag,   
       DimCafeProduct.payroll_standard_group_description   AS PayrollProductGroupDescription,    
       DimCafeProduct.payroll_standard_group_sort_order AS PayrollExtractExportStandardSortOrder,
       DimCafeProduct.payroll_lt_bucks_product_group_flag AS PayrollMyLTBucksProductGroupFlag,   
       DimCafeProduct.payroll_lt_bucks_group_description   AS PayrollMyLTBucksProductGroupDescription,
       DimCafeProduct.payroll_lt_bucks_group_sort_order  AS PayrollExtractExportMyLTBucksSortOrder,
       DimCafeProduct.payroll_track_sales_flag AS PayrollTrackSalesFlag,   
       DimCafeProduct.payroll_standard_sales_amount_flag AS PayrollSalesAmountFlag,
       DimCafeProduct.payroll_lt_bucks_sales_amount_flag  AS PayrollMyLTBucksSalesAmountFlag,
       DimCafeProduct.payroll_track_service_flag AS PayrollTrackServiceFlag, 
       DimCafeProduct.payroll_standard_service_amount_flag AS PayrollServiceAmountFlag,
       DimCafeProduct.payroll_standard_service_quantity_flag AS PayrollServiceQuantityFlag,
       DimCafeProduct.payroll_lt_bucks_service_amount_flag   AS PayrollMyLTBucksServiceAmountFlag,
       DimCafeProduct.payroll_lt_bucks_service_quantity_flag  AS PayrollMyLTBucksServiceQuantityFlag,
       NULL  RevenueProductGroupSortOrder,         ---------- Not available - if needed downstream just use alpha sort
       DimCafeProduct.reporting_product_group AS RevenueProductGroupDescription,
       NULL RevenueProductGroupGLAccount,
       NULL RevenueProductGroupRefundGLAccount,  
       NULL RevenueProductGroupDiscountGLAccount,   
       NULL RevenueAllocationRuleName,
       NULL SalesCategoryDescription,
       DimReportingHierarchy.RegionType AS RevenueReportingRegionType,
       DimCafeProduct.payroll_region_type   AS PayrollExtractRegionType,
       NULL AssessJuniorDuesFlag,
       NULL PackageProductCountAsHalfSessionFlag,
       NULL MTDAverageDeliveredSessionPriceFlag,
       DimCafeProduct.mtd_average_delivered_session_price_flag AS MTDAverageSalePriceFlag,
       DimCafeProduct.connectivity_lead_generator_flag AS ConnectivityLeadGeneratorFlag,
       DimCafeProduct.new_business_old_business_flag AS NewBusinessOldBusinessFlag,
       NULL PackageProductSessionType,
       DimCafeProduct.connectivity_primary_lead_generator_flag AS ConnectivityPrimaryLeadGeneratorFlag,
       DimCafeProduct.departmental_dssr_flag AS DepartmentalDSSRFlag,
       'N' CorporateTransferFlag,
       NULL CorporateTransferMultiplier,
       'N' DSSRIFAdminFeeFlag,     
       'N' DSSRDowngradeOtherEnrollmentFeeFlag,    
       'N' ExperienceLifeMagazineFlag,     
       DimCafeProduct.reporting_division AS DivisionName,
       DimCafeProduct.reporting_sub_division  AS SubdivisionName,
       NULL WorkdayAccount,
       NULL WorkdayCostCenter,
       NULL WorkdayOffering,
       NULL WorkdayOverRideRegion,
       NULL WorkdayRevenueProductGroupAccount,
       NULL DeferredRevenueFlag,
       NULL WorkdayRefundGLAccount,
       NULL WorkdayDiscountGLAccount,
       NULL WorkdayRevenueProductGroupRefundGLAccount,
       NULL WorkdayRevenueProductGroupDiscountGLAccount,
       NULL ECommerceOfferFlag

  FROM [marketing].[v_dim_cafe_product_history] DimCafeProduct
  JOIN #ProductStatusList
    ON DimCafeProduct.menu_item_active_flag = #ProductStatusList.CafeMenuItemActiveFlagMapping
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON IsNull(DimCafeProduct.dim_reporting_hierarchy_key,@CafeDefaultKey) = DimReportingHierarchy.DimReportingHierarchyKey
 WHERE 'Cafe' IN (SELECT SourceSystem FROM #SourceSystems)
   AND (DimCafeProduct.dim_cafe_product_key Is Null OR  DimCafeProduct.dim_cafe_product_key > '0' )   ------- removes the "unknown"  (-998) product records from being returned
   AND ((DimCafeProduct.effective_date_time <= @StartDate
        AND DimCafeProduct.expiration_date_time > @StartDate) OR DimCafeProduct.effective_date_time Is Null)



UNION ALL


SELECT 'MMS' SourceSystem,
       CAST(DimProduct.product_id AS VARCHAR(258)) AS ProductID,
       CAST(DimProduct.sku AS VARCHAR(258)) AS ProductSKU,
       DimProduct.product_description AS ProductDescription,
       DimProduct.gl_account_number AS ProductGLAccount,     
       DimProduct.gl_department_code AS ProductGLDepartmentCode,  
       DimProduct.gl_product_code AS ProductGLProductCode,    
       DimProduct.refund_gl_account_number AS ProductRefundGLAccount,   
       DimProduct.discount_gl_account AS ProductDiscountGLAccount,       
       DimProduct.reporting_department AS ReportingDepartment,
       DimProduct.reporting_department AS ReportingDepartmentForNonCommissionedSales,   
       DimProduct.department_description AS MMSDepartment,
       DimProduct.display_ui_flag  AS MMSProductDisplayUIFlag,
       DimProduct.gl_over_ride_club_id AS MMSProductGLOverrideClubID,
       DimProduct.recurrent_product_type_description  AS MMSRecurrentProductTypeDescription,
       DimProduct.package_product_flag  AS MMSPackageProductFlag,
       DimProduct.tip_allowed_flag AS MMSProductTipAllowedFlag,
       DimProduct.product_status AS ProductStatus,
       DimProduct.payroll_description AS PayrollExtractDescription, 
       DimProduct.payroll_standard_product_group_flag AS PayrollStandardProductGroupFlag,   
       DimProduct.payroll_standard_group_description AS PayrollProductGroupDescription,
       DimProduct.payroll_standard_group_sort_order AS PayrollExtractExportStandardSortOrder,   
       DimProduct.payroll_lt_bucks_product_group_flag AS PayrollMyLTBucksProductGroupFlag,    
       DimProduct.payroll_lt_bucks_group_description AS PayrollMyLTBucksProductGroupDescription,
       DimProduct.payroll_standard_group_sort_order AS PayrollExtractExportMyLTBucksSortOrder,
       DimProduct.payroll_track_sales_flag AS PayrollTrackSalesFlag,     
       DimProduct.payroll_standard_sales_amount_flag AS PayrollSalesAmountFlag,
       DimProduct.payroll_lt_bucks_sales_amount_flag AS PayrollMyLTBucksSalesAmountFlag,
       DimProduct.payroll_track_service_flag AS PayrollTrackServiceFlag,    
       DimProduct.payroll_standard_service_amount_flag AS PayrollServiceAmountFlag,
       DimProduct.payroll_standard_service_quantity_flag AS PayrollServiceQuantityFlag,
       DimProduct.payroll_lt_bucks_service_amount_flag AS PayrollMyLTBucksServiceAmountFlag,
       DimProduct.payroll_lt_bucks_service_quantity_flag AS PayrollMyLTBucksServiceQuantityFlag,
       Null  RevenueProductGroupSortOrder,    ---------- Not available - not found that it is used , if needed use alpha sort 
       DimProduct.reporting_product_group AS RevenueProductGroupDescription,
       DimProduct.reporting_product_group_gl_account AS RevenueProductGroupGLAccount,
       DimProduct.revenue_product_group_refund_gl_account AS RevenueProductGroupRefundGLAccount,   
       DimProduct.revenue_product_group_discount_gl_account AS RevenueProductGroupDiscountGLAccount,    
       DimProduct.allocation_rule  AS RevenueAllocationRuleName,
       DimProduct.sales_category_description   AS SalesCategoryDescription,
       DimProduct.reporting_region_type AS RevenueReportingRegionType,
       DimProduct.payroll_region_type AS PayrollExtractRegionType,
       DimProduct.junior_member_dues_flag AS AssessJuniorDuesFlag,
       NULL  PackageProductCountAsHalfSessionFlag,                                   ------- obsolete business functionality
       DimProduct.mtd_average_delivered_session_price_flag AS MTDAverageDeliveredSessionPriceFlag,
       DimProduct.mtd_average_sale_price_flag AS MTDAverageSalePriceFlag,
       DimProduct.connectivity_lead_generator_flag AS ConnectivityLeadGeneratorFlag,
       DimProduct.new_business_old_business_flag AS NewBusinessOldBusinessFlag,
       NULL    PackageProductSessionType,                                             -------- obsolete - LTFDW data always returned null
       DimProduct.connectivity_primary_lead_generator_flag AS ConnectivityPrimaryLeadGeneratorFlag,
       DimProduct.departmental_dssr_flag AS DepartmentalDSSRFlag,
       DimProduct.corporate_transfer_flag AS CorporateTransferFlag,
       DimProduct.corporate_transfer_multiplier  AS CorporateTransferMultiplier,
       DimProduct.dssr_if_admin_fee_flag AS DSSRIFAdminFeeFlag,                       
       DimProduct.dssr_down_grade_other_enrollment_fee_flag AS DSSRDowngradeOtherEnrollmentFeeFlag,      
       DimProduct.experience_life_magazine_flag AS ExperienceLifeMagazineFlag,              
       DimProduct.reporting_division   AS DivisionName,
       DimProduct.reporting_sub_division   AS SubdivisionName,
       DimProduct.workday_account  AS WorkdayAccount,
       DimProduct.workday_cost_center AS WorkdayCostCenter,
       DimProduct.workday_offering AS WorkdayOffering,
       DimProduct.workday_over_ride_region AS WorkdayOverRideRegion,
       DimProduct.workday_revenue_product_group_account AS WorkdayRevenueProductGroupAccount,
       DimProduct.deferred_revenue_flag  AS DeferredRevenueFlag,
       DimProduct.workday_refund_gl_account   AS WorkdayRefundGLAccount,
       DimProduct.workday_discount_gl_account  AS WorkdayDiscountGLAccount,
       DimProduct.workday_revenue_product_group_refund_gl_account AS WorkdayRevenueProductGroupRefundGLAccount,
       DimProduct.workday_revenue_product_group_discount_gl_account  AS WorkdayRevenueProductGroupDiscountGLAccount,
       NULL ECommerceOfferFlag
  FROM [marketing].[v_dim_mms_product_history] DimProduct
  JOIN #ProductStatusList
    ON IsNull(DimProduct.product_status,@FirstSelectedStatus) = #ProductStatusList.ProductStatus
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON IsNull(DimProduct.dim_reporting_hierarchy_key,@MMSDefaultKey) = DimReportingHierarchy.DimReportingHierarchyKey
 WHERE 'MMS' IN (SELECT SourceSystem FROM #SourceSystems)
   AND (DimProduct.dim_mms_product_key Is Null OR  DimProduct.dim_mms_product_key > '0' )   ------- removes the "unknown"  (-998) product records from being returned
   AND ((DimProduct.effective_date_time <= @StartDate
       AND DimProduct.expiration_date_time > @StartDate) OR DimProduct.effective_date_time Is Null)    ---- to handle null effective date records in UDW
 
 
 UNION ALL

 
 SELECT 'Magento' AS  SourceSystem,
       CAST(DimMagentoProductHistory.product_id AS VARCHAR(258))  AS ProductID,
       CAST(DimMagentoProductHistory.sku AS VARCHAR(258))  AS ProductSKU,
       DimMagentoProductHistory.product_name AS ProductDescription,  
       NULL ProductGLAccount,
       NULL ProductGLDepartmentCode,
       NULL ProductGLProductCode,
       NULL ProductRefundGLAccount,
       NULL ProductDiscountGLAccount,
       DimMagentoProductHistory.reporting_department AS ReportingDepartment,  
       DimMagentoProductHistory.reporting_department AS ReportingDepartmentForNonCommissionedSales,
       NULL MMSDepartment,
       NULL MMSProductDisplayUIFlag,
       NULL MMSproductGLOverrideClubID,
       NULL MMSRecurrentProductTypeDescription,
       NULL MMSPackageProductFlag,
       NULL MMSProductTipAllowedFlag,
	   CASE WHEN DimMagentoProductHistory.status = 1
	        THEN 'Active'
			WHEN DimMagentoProductHistory.status = 2
			THEN 'Inactive'
			ELSE 'Not Designated'
			END ProductStatus,
       DimMagentoProductHistory.payroll_description AS PayrollExtractDescription,   
       DimMagentoProductHistory.payroll_standard_product_group_flag AS PayrollStandardProductGroupFlag, 
       DimMagentoProductHistory.payroll_standard_group_description  AS PayrollProductGroupDescription,
       DimMagentoProductHistory.payroll_standard_group_sort_order AS PayrollExtractExportStandardSortOrder,
       DimMagentoProductHistory.payroll_lt_bucks_product_group_flag AS PayrollMyLTBucksProductGroupFlag, 
       DimMagentoProductHistory.payroll_lt_bucks_group_description AS PayrollMyLTBucksProductGroupDescription,
       DimMagentoProductHistory.payroll_lt_bucks_group_sort_order AS PayrollExtractExportMyLTBucksSortOrder,
       DimMagentoProductHistory.payroll_track_sales_flag AS PayrollTrackSalesFlag,
       DimMagentoProductHistory.payroll_standard_sales_amount_flag AS PayrollSalesAmountFlag,
       DimMagentoProductHistory.payroll_lt_bucks_sales_amount_flag AS PayrollMyLTBucksSalesAmountFlag,
       DimMagentoProductHistory.payroll_track_service_flag AS PayrollTrackServiceFlag,
       DimMagentoProductHistory.payroll_standard_service_amount_flag AS PayrollServiceAmountFlag,
       DimMagentoProductHistory.payroll_standard_service_quantity_flag  AS PayrollServiceQuantityFlag,
       DimMagentoProductHistory.payroll_lt_bucks_service_amount_flag  AS PayrollMyLTBucksServiceAmountFlag,
       DimMagentoProductHistory.payroll_lt_bucks_service_quantity_flag  AS PayrollMyLTBucksServiceQuantityFlag,
       NULL  RevenueProductGroupSortOrder,     ---------- Not available - or needed, just use alpha sort
       DimMagentoProductHistory.reporting_product_group AS RevenueProductGroupDescription,
       NULL RevenueProductGroupGLAccount,
       NULL RevenueProductGroupRefundGLAccount,  
       NULL RevenueProductGroupDiscountGLAccount,   
       DimMagentoProductHistory.allocation_rule AS RevenueAllocationRuleName,
       DimMagentoProductHistory.sales_category_description AS SalesCategoryDescription,
       DimMagentoProductHistory.reporting_region_type AS RevenueReportingRegionType,
       DimMagentoProductHistory.payroll_region_type  AS PayrollExtractRegionType,
       NULL AssessJuniorDuesFlag,
       NULL PackageProductCountAsHalfSessionFlag,
       NULL MTDAverageDeliveredSessionPriceFlag,
       DimMagentoProductHistory.mtd_average_sale_price_flag  AS MTDAverageSalePriceFlag,
       DimMagentoProductHistory.connectivity_lead_generator_flag  AS ConnectivityLeadGeneratorFlag,
       DimMagentoProductHistory.new_business_old_business_flag  AS NewBusinessOldBusinessFlag,
       NULL PackageProductSessionType,
       DimMagentoProductHistory.connectivity_primary_lead_generator_flag  AS ConnectivityPrimaryLeadGeneratorFlag,
       DimMagentoProductHistory.departmental_dssr_flag  AS DepartmentalDSSRFlag,
       DimMagentoProductHistory.corporate_transfer_flag AS CorporateTransferFlag,
       DimMagentoProductHistory.corporate_transfer_multiplier AS CorporateTransferMultiplier,
       'N' DSSRIFAdminFeeFlag,      
       'N' DSSRDowngradeOtherEnrollmentFeeFlag,    
       'N' ExperienceLifeMagazineFlag,    
       DimMagentoProductHistory.reporting_division AS DivisionName,
       DimMagentoProductHistory.reporting_sub_division AS SubdivisionName,
       NULL WorkdayAccount,
       DimMagentoProductHistory.workday_costcenter_id AS WorkdayCostCenter,
       DimMagentoProductHistory.workday_offering_id AS WorkdayOffering,
       NULL WorkdayOverRideRegion,
       NULL WorkdayRevenueProductGroupAccount,
       NULL DeferredRevenueFlag,
       NULL WorkdayRefundGLAccount,
       NULL WorkdayDiscountGLAccount,
       NULL WorkdayRevenueProductGroupRefundGLAccount,
       NULL WorkdayRevenueProductGroupDiscountGLAccount,
       NULL ECommerceOfferFlag
  FROM [marketing].[v_dim_magento_product_history] DimMagentoProductHistory
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON IsNull(DimMagentoProductHistory.dim_reporting_hierarchy_key,@MagentoDefaultKey) = DimReportingHierarchy.DimReportingHierarchyKey
  JOIN #ProductStatusList
    ON DimMagentoProductHistory.status = #ProductStatusList.MagentoProductStatusMapping
 WHERE 'Magento' IN (SELECT SourceSystem FROM #SourceSystems)
      AND (DimMagentoProductHistory.dim_magento_product_key Is Null OR  DimMagentoProductHistory.dim_magento_product_key > '0' )   ------- removes the "unknown" (-998) product records from being returned
      AND ((DimMagentoProductHistory.effective_date_time <= @StartDate
            AND DimMagentoProductHistory.expiration_date_time > @StartDate) OR DimMagentoProductHistory.effective_date_time is Null)

  
SELECT SourceSystem,
       ProductID,
       ProductSKU,
       ProductStatus,
       ProductDescription,
       ReportingDepartment,
       ReportingDepartmentForNonCommissionedSales,
       RevenueProductGroupDescription,
       RevenueProductGroupSortOrder,
       RevenueProductGroupGLAccount,
       RevenueProductGroupRefundGLAccount,
       RevenueProductGroupDiscountGLAccount,
       PayrollExtractDescription, 
       PayrollProductGroupDescription,
       PayrollStandardProductGroupFlag,    
       PayrollExtractExportStandardSortOrder,
       PayrollTrackSalesFlag,   
       PayrollSalesAmountFlag,
       PayrollTrackServiceFlag,  
       PayrollServiceAmountFlag,
       PayrollServiceQuantityFlag,
       PayrollMyLTBucksProductGroupFlag,   
       PayrollMyLTBucksProductGroupDescription,
       PayrollExtractExportMyLTBucksSortOrder,
       PayrollMyLTBucksSalesAmountFlag,
       PayrollMyLTBucksServiceAmountFlag,
       PayrollMyLTBucksServiceQuantityFlag,
       MMSDepartment,
       MMSRecurrentProductTypeDescription,
       MMSPackageProductFlag,
       ProductGLAccount,
       ProductGLDepartmentCode,
       ProductGLProductCode,
       ProductRefundGLAccount,
       ProductDiscountGLAccount,
       MMSProductDisplayUIFlag,
       MMSProductGLOverrideClubID,
       MMSProductTipAllowedFlag,
       NULL Discount1Description,
       NULL Discount1EffectiveFromDate,
       NULL Discount1EffectiveThroughDate,
       NULL Discount1SalesCommissionPercent,
       NULL Discount1ServiceCommissionPercent,
       NULL Discount2Description,
       NULL Discount2EffectiveFromDate,
       NULL Discount2EffectiveThroughDate,
       NULL Discount2SalesCommissionPercent,
       NULL Discount2ServiceCommissionPercent,
       NULL Discount3Description,
       NULL Discount3EffectiveFromDate,
       NULL Discount3EffectiveThroughDate,
       NULL Discount3SalesCommissionPercent,
       NULL Discount3ServiceCommissionPercent,
       NULL Discount4Description,
       NULL Discount4EffectiveFromDate,
       NULL Discount4EffectiveThroughDate,
       NULL Discount4SalesCommissionPercent,
       NULL Discount4ServiceCommissionPercent,
       NULL Discount5Description,
       NULL Discount5EffectiveFromDate,
       NULL Discount5EffectiveThroughDate,
       NULL Discount5SalesCommissionPercent,
       NULL Discount5ServiceCommissionPercent,
       @ReportDate AS ReportDate,
       @HeaderSourceSystemList AS HeaderSourceSystemList,
       RevenueAllocationRuleName,
       SalesCategoryDescription,
       RevenueReportingRegionType,
       PayrollExtractRegionType,
       AssessJuniorDuesFlag,
       @ReportRunDateTime AS ReportRunDateTime,
       PackageProductCountAsHalfSessionFlag,
       MTDAverageDeliveredSessionPriceFlag,
       MTDAverageSalePriceFlag,
       ConnectivityLeadGeneratorFlag,
       NewBusinessOldBusinessFlag,
       PackageProductSessionType,
       ConnectivityPrimaryLeadGeneratorFlag,
       DepartmentalDSSRFlag,
       CorporateTransferFlag,
       CorporateTransferMultiplier,
       DSSRIFAdminFeeFlag,
       DSSRDowngradeOtherEnrollmentFeeFlag,
       ExperienceLifeMagazineFlag,
       DivisionName,
       SubdivisionName,
       @HeaderProductStatusList AS HeaderProductStatusList, 
       NULL AS HeaderDivisionList, ---@HeaderDivisionList HeaderDivisionList,              ----- to be created within Cognos using parameter values - need to retain placeholder
       NULL AS HeaderSubdivisionList, ---@HeaderSubdivisionList HeaderSubdivisionList,              ----- to be created within Cognos using parameter values - need to retain placeholder
       NULL AS HeaderReportingDepartmentList, ---@HeaderReportingDepartmentList HeaderReportingDepartmentList,       ----- to be created within Cognos using parameter values - need to retain placeholder
       WorkdayAccount,
       WorkdayCostCenter,
       WorkdayOffering,
       WorkdayOverRideRegion,
       WorkdayRevenueProductGroupAccount,
       DeferredRevenueFlag,
       WorkdayRefundGLAccount,
       WorkdayDiscountGLAccount,
       WorkdayRevenueProductGroupRefundGLAccount,
       WorkdayRevenueProductGroupDiscountGLAccount,
       ECommerceOfferFlag
  FROM #Results
 --ORDER BY SourceSystem, ProductID, ProductSKU

DROP TABLE #Results 
DROP TABLE #ProductStatusList
DROP TABLE #SourceDefaultKeys 
DROP TABLE #DimReportingHierarchy 
DROP TABLE #SourceSystems

END

