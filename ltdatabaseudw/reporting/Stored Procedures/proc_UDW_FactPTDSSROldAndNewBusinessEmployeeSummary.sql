CREATE PROC [reporting].[proc_UDW_FactPTDSSROldAndNewBusinessEmployeeSummary] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END




------  Daily data - append to a database summary table
------  @ReportDate is automated to set report to yesterday's date
------  Table will hold daily data for 13 months;  only EOM data for prior months


	
DECLARE @ReportDate Datetime = '1/1/1900'
SET @ReportDate = CASE WHEN @ReportDate = '1/1/1900' THEN CONVERT(DATE, getdate()-1, 101) ELSE @ReportDate END


DECLARE @DivisionName VARCHAR(255)
SET @DivisionName = 'Personal Training'	

				
DECLARE @StartDate  DATETIME				
DECLARE @EndDimDateKey  VARCHAR(32)						
DECLARE @FirstOfPriorMonthDimDateKey  VARCHAR(32)				
DECLARE @FirstOf2MonthsPriorDimDateKey  VARCHAR(32)				
DECLARE @FirstOfCurrentMonthDimDateKey  VARCHAR(32)				
DECLARE @EndDate  DATETIME
DECLARE @ReportDateLastDayInMonthIndicator 	VARCHAR(1)
DECLARE @FirstOf13MonthsPriorDimDateKey  VARCHAR(32)	
		
				
				
SET @StartDate = (SELECT month_starting_date FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportDate)				
SET @EndDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportDate)				
SET @FirstOfPriorMonthDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DATEADD(m,-1,@StartDate))	
SET @FirstOf2MonthsPriorDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DATEADD(m,-2, @StartDate))				
SET @FirstOfCurrentMonthDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)				
SET @EndDate = @ReportDate 
SET @ReportDateLastDayInMonthIndicator = (SELECT last_day_in_month_flag FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)
SET @FirstOf13MonthsPriorDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = DATEADD(m,-13, @StartDate))


IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchy', 'U') IS NOT NULL
  DROP TABLE #DimReportingHierarchy;  

---- Create Temp table of active hierarchy keys
SELECT Distinct dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
       reporting_division AS DivisionName,
	   reporting_sub_division AS SubdivisionName,
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
 

IF OBJECT_ID('tempdb.dbo.#DimClubKeyList', 'U') IS NOT NULL
  DROP TABLE #DimClubKeyList;  

---- Create temp table of the Active locations - returning only the required columns

SELECT DISTINCT DimClub.dim_club_key AS DimClubKey,   ------- Name change
                DimClub.club_id AS MMSClubID,
                DimClub.club_name AS ClubName,
                DimClub.local_currency_code AS LocalCurrencyCode,
                DimClub.club_code AS ClubCode,
				ClubOpenDate.calendar_date AS ClubOpenDate,
				DimDescription.description AS RegionName,
				DimClub.club_close_dim_date_key AS ClubCloseDimDateKey,
				CASE WHEN ClubOpenDate.calendar_date >= DATEADD(Month,-1,@StartDate)
				     THEN 'Y'
					 ELSE 'N'
					 END NewBusinessOnlyClub --- All transactions for clubs opening since the 1st of the prior month are considered New Member-New Business
				
  INTO #DimClubKeyList    ------- Name change   
  FROM [marketing].[v_dim_club] DimClub
   JOIN [marketing].[v_dim_description] DimDescription
     ON DimClub.pt_rcl_area_dim_description_key = DimDescription.dim_description_key
   JOIN [marketing].[v_dim_date] ClubOpenDate
     ON DimClub.club_open_dim_date_key = ClubOpenDate.dim_date_key

 ------ Created to set parameters for deferred E-comm sales of 60 Day challenge products
  ------ Rule set that challenge starts in the 2nd month of each quarter and if sales are made in the 1st month of the quarter
  ------   revenue is deferred to the 2nd month

DECLARE @StartDateMonthStartDimDateKey VARCHAR(32)
DECLARE @EndDateMonthStartDimDateKey VARCHAR(32)
DECLARE @StartDateCalendarMonthNumberInYear INT
DECLARE @EndDateCalendarMonthNumberInYear INT
DECLARE @EndDatePriorMonthEndDateDimDateKey VARCHAR(32)

SET @StartDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @FirstOfCurrentMonthDimDateKey) 
SET @EndDateMonthStartDimDateKey = (SELECT month_starting_dim_date_key FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey) 
SET @StartDateCalendarMonthNumberInYear = (SELECT month_number_in_year  FROM [marketing].[v_dim_date] WHERE dim_date_key = @FirstOfCurrentMonthDimDateKey)
SET @EndDateCalendarMonthNumberInYear = (SELECT month_number_in_year  FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)
SET @EndDatePriorMonthEndDateDimDateKey = (SELECT next_month_ending_dim_date_key  FROM [marketing].[v_dim_date] WHERE dim_date_key = @EndDimDateKey)


DECLARE @EComm60DayChallengeRevenueStartDimDateKey VARCHAR(32)
  ---- When the start date is the 1st of the 2nd month of the quarter, set the start date to the 1st of the prior month
SET @EComm60DayChallengeRevenueStartDimDateKey = (SELECT CASE WHEN (@FirstOfCurrentMonthDimDateKey = @StartDateMonthStartDimDateKey)          ---- Date range begins on the 1st of a month
															  THEN (CASE WHEN @StartDateCalendarMonthNumberInYear in(2,5,8,11)
																		 THEN (Select prior_month_starting_dim_date_key
                                                                                 FROM [marketing].[v_dim_date] 
                                                                                WHERE dim_date_key = @FirstOfCurrentMonthDimDateKey)
																	      WHEN @StartDateCalendarMonthNumberInYear in(1,4,7,10)
																		  THEN (Select month_starting_dim_date_key
                                                                                  FROM [marketing].[v_dim_date]
                                                                                 WHERE dim_date_key = @FirstOfCurrentMonthDimDateKey) 
																		  ELSE @FirstOfCurrentMonthDimDateKey
																				   END)
												
															  ELSE  @FirstOfCurrentMonthDimDateKey END
												  FROM [marketing].[v_dim_date] 
												  WHERE dim_date_key = @FirstOfCurrentMonthDimDateKey ) ---- to limit result set to one record)

DECLARE @EComm60DayChallengeRevenueEndDimDateKey VARCHAR(32)
  ---- When the End Date is in the 1st month of the quarter, set the end date to the end of the prior month
SET @EComm60DayChallengeRevenueEndDimDateKey = (SELECT CASE WHEN @EndDateCalendarMonthNumberInYear in(1,4,7,10)
                                                            THEN @EndDatePriorMonthEndDateDimDateKey 
															ELSE @EndDimDateKey
															END
												FROM [marketing].[v_dim_date]
												WHERE dim_date_key = @EndDimDateKey)  ---- to limit result set to one record





----- Find all PT packages sold in the look back period where the purchaser was not the package customer
 ----- 12/4/2019 - Confirmed with Neal G and Peggy O that Magento package sale processing only allows for the 
 ----- package member to be the purchasing member. The purchaser cannot assign the package to another member.

IF OBJECT_ID('tempdb.dbo.#PackagesWherePurchaserIsNotServicedCustomer', 'U') IS NOT NULL
  DROP TABLE #PackagesWherePurchaserIsNotServicedCustomer; 

   ----- Packages sold through MMS
    SELECT FactSalesTransaction.tran_item_id AS TranItemID,
	       FactPackage.package_id AS PackageID,
		   FactSalesTransaction.dim_mms_member_key AS PkgPurchasingCustomer_DimMMSMemberKey,  ------ New Name
		   FactPackage.dim_mms_member_key  AS PkgServiceCustomer_DimMMSMemberKey   ------ New Name
		INTO #PackagesWherePurchaserIsNotServicedCustomer
	  FROM [marketing].[v_fact_mms_package] FactPackage
	    JOIN [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
		  ON FactPackage.tran_item_id = FactSalesTransaction.tran_item_id
		JOIN [marketing].[v_dim_mms_product_history] DimProduct
		  ON FactPackage.dim_mms_product_key = DimProduct.dim_mms_product_key
		   AND DimProduct.effective_date_time <= @EndDate
		   AND DimProduct.expiration_date_time > @EndDate
		JOIN #DimReportingHierarchy DimReportingHierarchy
		  ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
	  WHERE FactSalesTransaction.post_dim_date_key >=  @FirstOf2MonthsPriorDimDateKey
	    AND FactSalesTransaction.post_dim_date_key <= @ENDDimDateKey
	    AND FactSalesTransaction.dim_mms_member_key <> FactPackage.dim_mms_member_key



 ---- create a temp table to hold data on all PT Transactions for the past 2 months of revenue
 ---- Used for both current month customer and old business customer (2 month prior) data

 IF OBJECT_ID('tempdb.dbo.#OldNewBusinessCustomerRecords', 'U') IS NOT NULL
  DROP TABLE #OldNewBusinessCustomerRecords;

	SELECT FactClubPOSAllocatedRevenue.tran_item_id AS TranItemID,
	       FactClubPOSAllocatedRevenue.dim_mms_member_key AS PkgPurchasingCustomerDimMMSMemberKey,    ----- Name Change
	       IsNull(PKG.PkgServiceCustomer_DimMMSMemberKey,FactClubPOSAllocatedRevenue.dim_mms_member_key) AS PkgServiceCustomerDimMMSMemberKey,   ----- Name Change
		   IsNull(PKG.PkgServiceCustomer_DimMMSMemberKey,FactClubPOSAllocatedRevenue.dim_mms_member_key) AS OldNewBusinessDimMMSMemberKey,     ----- Name Change
		   FactClubPOSAllocatedRevenue.transaction_amount AS ItemAmount,
		   CASE WHEN FactClubPOSAllocatedRevenue.transaction_post_dim_date_key = @EndDimDateKey
	        THEN FactClubPOSAllocatedRevenue.transaction_amount 
			WHEN (FactClubPOSAllocatedRevenue.transaction_post_dim_date_key < @FirstOfCurrentMonthDimDateKey) and (@EndDimDateKey = @FirstOfCurrentMonthDimDateKey)
			THEN FactClubPOSAllocatedRevenue.transaction_amount
			ELSE 0
			END Today_ItemAmount,	
		   DimProduct.dim_mms_product_key AS DimProductKey,
		   DimProduct.dim_reporting_hierarchy_key AS DimReportingHierarchyKey,
		   FactClubPOSAllocatedRevenue.transaction_post_dim_date_key AS TransactionPostDimDateKey, 				
		   FactClubPOSAllocatedRevenue.allocated_month_starting_dim_date_key AS RevenuePostingMonthStartingDimDateKey,
		   RevenueDimDate.four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth,
		   FactClubPOSAllocatedRevenue.dim_club_key AS DimClubKey,    ------NameChange
		   FactClubPOSAllocatedRevenue.refund_flag AS RefundFlag,
		   FactClubPOSAllocatedRevenue.charge_flag AS ChargeFlag,
           FactClubPOSAllocatedRevenue.commissioned_sales_transaction_flag AS CommissionedSalesTransactionFlag,
           IsNull(FactClubPOSAllocatedRevenue.primary_sales_dim_employee_key,'-998') AS PrimarySalesDimEmployeeKey,
           FactClubPOSAllocatedRevenue.secondary_sales_dim_employee_key AS SecondarySalesDimEmployeeKey,
           FactClubPOSAllocatedRevenue.allocated_quantity AS AllocatedQuantity,
		   FactClubPOSAllocatedRevenue.transaction_quantity AS SalesQuantity,
		   FactClubPOSAllocatedRevenue.transaction_amount AS SalesAmount,
		   0 AS CorporateTransferAmount,
		   FactClubPOSAllocatedRevenue.transaction_discount_dollar_amount AS SalesDiscountDollarAmount,
		   FactClubPOSAllocatedRevenue.allocation_rule_set AS RevenueAllocationRule,   ------ Name change
		   FactClubPOSAllocatedRevenue.sold_not_serviced_flag AS SoldNotServicedFlag,
           FactClubPOSAllocatedRevenue.mms_tran_id AS MMSTranID,
           FactClubPOSAllocatedRevenue.dim_mms_transaction_reason_key AS DimMMSTransactionReasonKey,   ----- Name Change
		   DimProduct.workday_offering AS WorkdayOffering,
		   CASE WHEN DimProduct.workday_offering in('OF10104','OF10105','OF10220','OF10202','OF10122','OF10123','OF10115')
		        THEN 'N'
				ELSE 'Y'
				END PTServiceFlag,
		   'MMS' SalesSource,						
       	   NULL AS SKU,
		   DimProduct.product_description AS ProductDescription,
		   #DimReportingHierarchy.ProductGroupName,
		   #DimReportingHierarchy.DepartmentName,
		   #DimReportingHierarchy.SubdivisionName,
		   #DimReportingHierarchy.DivisionName,
		   CASE WHEN FactClubPOSAllocatedRevenue.charge_flag = 'Y'
		        THEN 'Charge'
				WHEN FactClubPOSAllocatedRevenue.refund_flag = 'Y'
				THEN 'Refund'
				WHEN FactClubPOSAllocatedRevenue.sale_flag = 'Y'
				THEN 'Sale'
				WHEN FactClubPOSAllocatedRevenue.adjustment_flag = 'Y'
				THEN 'Adjustment'
				ELSE 'Unknown'
				END TransactionType,
			CONVERT(VARCHAR(255),DimProduct.product_id) AS SourceProductID,
			'MMS' AS SalesChannel
	INTO #OldNewBusinessCustomerRecords  
	FROM [marketing].[v_fact_mms_allocated_transaction_item] FactClubPOSAllocatedRevenue				
     JOIN [marketing].[v_dim_date] TransactionDimDate				
       ON FactClubPOSAllocatedRevenue.transaction_post_dim_date_key = TransactionDimDate.dim_date_key	
     JOIN [marketing].[v_dim_date] RevenueDimDate				
       ON FactClubPOSAllocatedRevenue.allocated_month_starting_dim_date_key = RevenueDimDate.dim_date_key	
	 JOIN [marketing].[v_dim_mms_product_history] DimProduct				
       ON FactClubPOSAllocatedRevenue.dim_mms_product_key = DimProduct.dim_mms_product_key
	    AND DimProduct.effective_date_time <= @EndDate
		AND DimProduct.expiration_date_time > @EndDate
     JOIN #DimReportingHierarchy 
       ON #DimReportingHierarchy.DimReportingHierarchyKey = DimProduct.dim_reporting_hierarchy_key
	 JOIN #DimClubKeyList	DimClub
       ON FactClubPOSAllocatedRevenue.dim_club_key = DimClub.DimClubKey
	 LEFT JOIN #PackagesWherePurchaserIsNotServicedCustomer  PKG
	   ON FactClubPOSAllocatedRevenue.tran_item_id = PKG.TranItemID
	 WHERE 	FactClubPOSAllocatedRevenue.allocated_month_starting_dim_date_key >=  @FirstOf2MonthsPriorDimDateKey			
       AND FactClubPOSAllocatedRevenue.transaction_post_dim_date_key <= @ENDDimDateKey				
       AND FactClubPOSAllocatedRevenue.transaction_amount <> 0 

UNION ALL

------ Bring in current month revenue from the Cafe

	   SELECT NULL AS TranItemID,
	          '-998' AS PkgPurchasingCustomerDimMMSMemberKey,
			  NULL AS PkgServiceCustomerDimMMSMemberKey,
			  '-998' AS 	OldNewBusinessDimMMSMemberKey,
			  FactCafePOSRevenue.transaction_amount AS ItemAmount,
			  CASE WHEN TransactionCloseDimDate.dim_date_key = @EndDimDateKey
	               THEN FactCafePOSRevenue.transaction_amount
			       ELSE 0
			  END Today_ItemAmount,
			  NULL AS DimProductKey,
			  #DimReportingHierarchy.DimReportingHierarchyKey,
			  TransactionCloseDimDate.dim_date_key AS TransactionPostDimDateKey,
			  FactCafePOSRevenue.allocated_month_starting_dim_date_key AS RevenuePostingMonthStartingDimDateKey,
			  TransactionCloseDimDate.four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth,
			  DimClub.DimClubKey,     ----- Name Change
			  FactCafePOSRevenue.refund_flag AS RefundFlag,
			  'N' AS ChargeFlag,
			  CASE WHEN IsNull(FactCafePOSRevenue.commissioned_sales_dim_employee_key,'-998') in('-997','-998','-999')
			       THEN 'N'
				   ELSE 'Y'
				   END CommissionedSalesTransactionFlag,
			  IsNull(FactCafePOSRevenue.commissioned_sales_dim_employee_key,'-998') AS PrimarySalesDimEmployeeKey,
			  '-998' AS SecondarySalesDimEmployeeKey,
			  FactCafePOSRevenue.allocated_quantity AS AllocatedQuantity,
			  FactCafePOSRevenue.transaction_quantity AS SalesQuantity,
			  FactCafePOSRevenue.transaction_amount AS SalesAmount,
			  0 AS CorporateTransferAmount,
			  FactCafePOSRevenue.discount_amount AS SalesDiscountDollarAmount,
			  'Sale Month Activity' AS RevenueAllocationRule,
			  'N' AS SoldNotServicedFlag,
			  NULL AS MMSTranID,
			  NULL AS DimMMSTransactionReasonKey,
			  CASE WHEN #DimReportingHierarchy.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN #DimReportingHierarchy.DepartmentName = 'PT Nutritionals'
				   THEN 'OF10115'
				   WHEN #DimReportingHierarchy.DepartmentName = 'Cafe Nutritionals'
				   THEN 'OF54010'
				   END WorkdayOffering,
              'N' AS PTServiceFlag,
              'Cafe ' SalesSource,						
       	      CONVERT(VARCHAR(255),DimCafeProduct.menu_item_id) AS SKU,
			  DimCafeProduct.menu_item_name AS ProductDescription,
			  #DimReportingHierarchy.ProductGroupName,
			  #DimReportingHierarchy.DepartmentName,
			  #DimReportingHierarchy.SubdivisionName,
			  #DimReportingHierarchy.DivisionName,
			  'Sale' AS TransactionType,
			  CONVERT(VARCHAR(255),DimCafeProduct.menu_item_id) AS SourceProductID,
			  'Cafe' AS SalesChannel
  FROM [marketing].[v_fact_cafe_allocated_transaction_item] FactCafePOSRevenue				
  JOIN [marketing].[v_dim_date] TransactionCloseDimDate				
    ON FactCafePOSRevenue.transaction_close_dim_date_key = TransactionCloseDimDate.dim_date_key				
  JOIN [marketing].[v_dim_cafe_product_history] DimCafeProduct				
    ON FactCafePOSRevenue.dim_cafe_product_key = DimCafeProduct.dim_cafe_product_key	-----  we want the product status at the report date not how it stood at the tran date		
     AND DimCafeProduct.effective_date_time <= @EndDate			
     AND DimCafeProduct.expiration_date_time > @EndDate			
  JOIN #DimClubKeyList DimClub				
    ON FactCafePOSRevenue.dim_club_key = DimClub.DimClubKey				
  LEFT JOIN [marketing].[v_dim_employee] DimEmployee                          				
    ON FactCafePOSRevenue.commissioned_sales_dim_employee_key = DimEmployee.dim_employee_key				
     AND DimEmployee.dim_employee_key not in('-997','-998','-999')				
  JOIN #DimReportingHierarchy
    ON 	DimCafeProduct.dim_reporting_hierarchy_key = #DimReportingHierarchy.DimReportingHierarchyKey							
 WHERE FactCafePOSRevenue.transaction_close_dim_date_key >= @FirstOfCurrentMonthDimDateKey	----- Since we don't have customer Key,(can't determine o/n business) we only need current month's trans.			
   AND FactCafePOSRevenue.transaction_close_dim_date_key <= @ENDDimDateKey	
   
UNION ALL
	
 ----- Bring in HealthCheckUSA transactions			   
	   SELECT NULL AS TranItemID,
	          '-998' AS PkgPurchasingCustomerDimMMSMemberKey,     
			  '-998' AS PkgServiceCustomerDimMMSMemberKey,
			  '-998' AS OldNewBusinessDimMMSMemberKey,    
			  FactECommerceRevenue.sales_amount AS ItemAmount,
			  CASE WHEN TransactionPostDimDate.dim_date_key = @EndDimDateKey
	               THEN FactECommerceRevenue.sales_amount
			       ELSE 0
			  END Today_ItemAmount,   
			  NULL AS DimProductKey,
			  DimReportingHierarchy.DimReportingHierarchyKey,   
			  TransactionPostDimDate.dim_date_key AS TransactionPostDimDateKey, 
			  TransactionPostDimDate.month_starting_dim_date_key AS RevenuePostingMonthStartingDimDateKey,  
			  TransactionPostDimDate.four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth,  
			  DimClub.DimClubKey,    ----- Name change
			  FactECommerceRevenue.refund_flag AS RefundFlag,
			  'N' AS ChargeFlag,
			 CASE WHEN IsNull(DimEmployee.dim_employee_key,'-998') in('-997','-998','-999')
			       THEN 'N'
				   ELSE 'Y'
				   END CommissionedSalesTransactionFlag,
			 IsNull(DimEmployee.dim_employee_key ,'-998') AS PrimarySalesDimEmployeeKey,   
			  '-998' AS SecondarySalesDimEmployeeKey,
			  FactECommerceRevenue.sales_quantity AS AllocatedQuantity,   
			  FactECommerceRevenue.sales_quantity AS SalesQuantity,     
			  FactECommerceRevenue.sales_amount AS SalesAmount,   
			  0 AS CorporateTransferAmount,
			  FactECommerceRevenue.discount_amount AS SalesDiscountDollarAmount,
			  'Sale Month Activity' AS RevenueAllocationRule,   ------ Name change
			  'N' AS SoldNotServicedFlag,
			  NULL AS MMSTranID,
			  NULL AS DimTransactionReasonKey,
			  CASE WHEN DimReportingHierarchy.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN DimReportingHierarchy.DepartmentName = 'Fitness Products'
				   THEN 'OF10220'
				   WHEN DimReportingHierarchy.DepartmentName = 'Lab Testing'
				   THEN 'OF10217'
				   WHEN DimReportingHierarchy.DepartmentName = 'Metabolic Assessments'
				   THEN 'OF10108'
				   WHEN DimReportingHierarchy.DepartmentName = 'Metabolic Conditioning'
				   THEN 'OF10224'
				   WHEN DimReportingHierarchy.DepartmentName = 'PT E-Commerce'
				   THEN 'OF10122'
				   WHEN DimReportingHierarchy.DepartmentName in('Weight Loss Challenges','90 Day Weight Loss')
				   THEN 'OF10117'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Alpha'
				   THEN 'OF10091'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Boot Camp'
				   THEN 'OF10094'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Burn'
				   THEN 'OF10095'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Cut'
				   THEN 'OF10096'
				   WHEN DimReportingHierarchy.DepartmentName = 'Tri-PT'
				   THEN 'OF10121'
				   WHEN DimReportingHierarchy.DepartmentName = 'MyHealthScore'
				   THEN 'OF10118'
				   WHEN DimReportingHierarchy.DepartmentName = 'Personal Training'
				   THEN 'OF10084'
				   END WorkdayOffering,
			  CASE WHEN DimReportingHierarchy.DepartmentName in('Devices','Fitness Products','PT E-Commerce')
		        THEN 'N'
				ELSE 'Y'
				END PTServiceFlag,
              'HealthCheckUSA' AS SalesSource,		 				
       	      DimECommerceProduct.product_sku AS SKU,   
			  DimECommerceProduct.product_description ProductDescription, 
			  DimReportingHierarchy.ProductGroupName,
			  DimReportingHierarchy.DepartmentName,
			  DimReportingHierarchy.SubdivisionName,
			  DimReportingHierarchy.DivisionName,
			  CASE WHEN FactECommerceRevenue.refund_flag = 'N'
                    THEN 'Sale'
                   ELSE 'Refund' 
              END TransactionType,
			  DimECommerceProduct.product_sku AS SourceProductID,
			  'E-Commerce' AS SalesChannel	
  FROM [marketing].[v_fact_healthcheckusa_allocated_transaction_item] FactECommerceRevenue				
  JOIN [marketing].[v_dim_date] TransactionPostDimDate				
    ON FactECommerceRevenue.transaction_post_dim_date_key = TransactionPostDimDate.dim_date_key	
  JOIN [marketing].[v_dim_healthcheckusa_product_history] DimECommerceProduct				
    ON FactECommerceRevenue.dim_healthcheckusa_product_key = DimECommerceProduct.dim_healthcheckusa_product_key
	 AND DimECommerceProduct.effective_date_time <= @EndDate				
     AND DimECommerceProduct.expiration_date_time > @EndDate								
  JOIN #DimClubKeyList DimClub				
    ON FactECommerceRevenue.allocated_dim_club_key = DimClub.DimClubKey				
  LEFT JOIN [marketing].[v_dim_employee] DimEmployee				
    ON FactECommerceRevenue.sales_dim_employee_key = DimEmployee.dim_employee_key			
    AND DimEmployee.dim_employee_key Not In('-997','-998','-999')						
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON 	DimECommerceProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey		
 WHERE FactECommerceRevenue.transaction_post_dim_date_key >= @FirstOfCurrentMonthDimDateKey	----- Since we don't have customer Key,(can't determine o/n business) we only need current month's trans.						
   AND FactECommerceRevenue.transaction_post_dim_date_key <= @ENDDimDateKey				
		

UNION ALL
	
 ----- Bring in Magento non-deferred transactions			   
	   SELECT NULL AS TranItemID,
	          IsNull(FactECommerceRevenue.dim_mms_member_key,'-998') AS PkgPurchasingCustomerDimMMSMemberKey,     
			  IsNull(FactECommerceRevenue.dim_mms_member_key,'-998') AS PkgServiceCustomerDimMMSMemberKey,
			  IsNull(FactECommerceRevenue.dim_mms_member_key,'-998') AS OldNewBusinessDimMMSMemberKey,    
			  FactECommerceRevenue.allocated_amount AS ItemAmount,
			  CASE WHEN TransactionPostDimDate.dim_date_key = @EndDimDateKey
	               THEN FactECommerceRevenue.allocated_amount 
			       ELSE 0
			  END Today_ItemAmount,   
			  NULL AS DimProductKey,
			  DimReportingHierarchy.DimReportingHierarchyKey,   
			  TransactionPostDimDate.dim_date_key AS TransactionPostDimDateKey, 
			  TransactionPostDimDate.month_starting_dim_date_key AS RevenuePostingMonthStartingDimDateKey,  
			  TransactionPostDimDate.four_digit_year_dash_two_digit_month AS FourDigitYearDashTwoDigitMonth,  
			  DimClub.DimClubKey,
			  FactECommerceRevenue.refund_flag AS RefundFlag,
			  'N' AS ChargeFlag,
			  CASE WHEN IsNull(DimEmployee.dim_employee_key,'-998') in('-997','-998','-999')
			       THEN 'N'
				   ELSE 'Y'
				   END CommissionedSalesTransactionFlag,
			  IsNull(DimEmployee.dim_employee_key,'-998') AS PrimarySalesDimEmployeeKey,    
			  '-998' AS SecondarySalesDimEmployeeKey,
			  FactECommerceRevenue.allocated_quantity AS AllocatedQuantity,   
			  FactECommerceRevenue.transaction_quantity AS SalesQuantity,     
			  FactECommerceRevenue.transaction_amount AS SalesAmount,   
			  0 AS CorporateTransferAmount,
			  FactECommerceRevenue.discount_amount AS SalesDiscountDollarAmount,
			  'Sale Month Activity' AS RevenueAllocationRule,   ------ Name change
			  'N' AS SoldNotServicedFlag,
			  NULL AS MMSTranID,
			  NULL AS DimTransactionReasonKey,
			  CASE WHEN DimReportingHierarchy.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN DimReportingHierarchy.DepartmentName = 'Fitness Products'
				   THEN 'OF10220'
				   WHEN DimReportingHierarchy.DepartmentName = 'Lab Testing'
				   THEN 'OF10217'
				   WHEN DimReportingHierarchy.DepartmentName = 'Metabolic Assessments'
				   THEN 'OF10108'
				   WHEN DimReportingHierarchy.DepartmentName = 'Metabolic Conditioning'
				   THEN 'OF10224'
				   WHEN DimReportingHierarchy.DepartmentName = 'PT E-Commerce'
				   THEN 'OF10122'
				   WHEN DimReportingHierarchy.DepartmentName in('Weight Loss Challenges','90 Day Weight Loss')
				   THEN 'OF10117'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Alpha'
				   THEN 'OF10091'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Boot Camp'
				   THEN 'OF10094'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Burn'
				   THEN 'OF10095'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Cut'
				   THEN 'OF10096'
				   WHEN DimReportingHierarchy.DepartmentName = 'Tri-PT'
				   THEN 'OF10121'
				   WHEN DimReportingHierarchy.DepartmentName = 'MyHealthScore'
				   THEN 'OF10118'
				   WHEN DimReportingHierarchy.DepartmentName = 'Personal Training'
				   THEN 'OF10084'
				   END WorkdayOffering,
			  CASE WHEN DimReportingHierarchy.DepartmentName in('Devices','Fitness Products','PT E-Commerce')
		        THEN 'N'
				ELSE 'Y'
				END PTServiceFlag,
              'Magento' AS SalesSource,		 				
       	      DimECommerceProduct.sku AS SKU,   
			  DimECommerceProduct.product_name AS ProductDescription, 
			  DimReportingHierarchy.ProductGroupName,
			  DimReportingHierarchy.DepartmentName,
			  DimReportingHierarchy.SubdivisionName,
			  DimReportingHierarchy.DivisionName,
			  CASE WHEN FactECommerceRevenue.refund_flag = 'N'
                    THEN 'Sale'
                   ELSE 'Refund' 
              END TransactionType,
			  DimECommerceProduct.sku AS SourceProductID,
			  'E-Commerce' AS SalesChannel	
  FROM [marketing].[v_fact_magento_allocated_transaction_item] FactECommerceRevenue				
  JOIN [marketing].[v_dim_date] TransactionPostDimDate				
    ON FactECommerceRevenue.invoice_dim_date_key	 = TransactionPostDimDate.dim_date_key	
  JOIN [marketing].[v_dim_magento_product_history] DimECommerceProduct				
    ON FactECommerceRevenue.dim_magento_product_key = DimECommerceProduct.dim_magento_product_key	
	 AND DimECommerceProduct.effective_date_time <= @EndDate				
     AND DimECommerceProduct.expiration_date_time > @EndDate								
  JOIN #DimClubKeyList DimClub		   ----- New name		
    ON FactECommerceRevenue.dim_club_key = DimClub.DimClubKey				
  LEFT JOIN [marketing].[v_dim_employee] DimEmployee				
    ON FactECommerceRevenue.commissioned_sales_dim_employee_key = DimEmployee.dim_employee_key				
    AND DimEmployee.dim_employee_key not in('-997','-998','-999')								
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON 	DimECommerceProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey		
 WHERE FactECommerceRevenue.invoice_dim_date_key >= @FirstOf2MonthsPriorDimDateKey				
   AND FactECommerceRevenue.invoice_dim_date_key <= @ENDDimDateKey				
   AND DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'N'

UNION ALL
   
   ----- Bring in Magento deferred transactions
   	   SELECT NULL AS TranItemID,
	          IsNull(FactECommerceRevenue.dim_mms_member_key,'-998') AS PkgPurchasingCustomerDimMMSMemberKey,     
			  IsNull(FactECommerceRevenue.dim_mms_member_key,'-998') AS PkgServiceCustomerDimMMSMemberKey,
			  IsNull(FactECommerceRevenue.dim_mms_member_key,'-998') AS OldNewBusinessDimMMSMemberKey,    
			  FactECommerceRevenue.allocated_amount AS ItemAmount, 
		      CASE WHEN TransactionPostDimDate.month_number_in_year in(1,4,7,10) AND TransactionPostDimDate.next_month_starting_dim_date_key = @EndDimDateKey
	               THEN FactECommerceRevenue.allocated_amount
			       WHEN TransactionPostDimDate.month_number_in_year Not In(1,4,7,10) AND TransactionPostDimDate.dim_date_key = @EndDimDateKey 
			       THEN FactECommerceRevenue.allocated_amount
			       ELSE 0
			  END Today_ItemAmount,  
			  NULL AS DimProductKey,
			  DimReportingHierarchy.DimReportingHierarchyKey,   
			  TransactionPostDimDate.dim_date_key AS TransactionPostDimDateKey, 
			  CASE WHEN TransactionPostDimDate.month_number_in_year in(1,4,7,10)
			       THEN TransactionPostDimDate.next_month_starting_dim_date_key
				   ELSE TransactionPostDimDate.month_starting_dim_date_key
				   END AS RevenuePostingMonthStartingDimDateKey, 
			  CASE WHEN TransactionPostDimDate.month_number_in_year in(1,4,7,10)
			       THEN PriorMonthStartDate.four_digit_year_dash_two_digit_month
				   ELSE TransactionPostDimDate.four_digit_year_dash_two_digit_month
				   END FourDigitYearDashTwoDigitMonth,
			  DimClub.DimClubKey,
			  FactECommerceRevenue.refund_flag AS RefundFlag,
			  'N' AS ChargeFlag,
			  CASE WHEN IsNull(DimEmployee.dim_employee_key,'-998') in('-997','-998','-999')
			       THEN 'N'
				   ELSE 'Y'
				   END CommissionedSalesTransactionFlag,
			  DimEmployee.dim_employee_key AS PrimarySalesDimEmployeeKey,    
			  '-998' AS SecondarySalesDimEmployeeKey,
			  FactECommerceRevenue.allocated_quantity AS AllocatedQuantity,   
			  FactECommerceRevenue.transaction_quantity AS SalesQuantity,     
			  FactECommerceRevenue.transaction_amount AS SalesAmount,   
			  0 AS CorporateTransferAmount,
			  FactECommerceRevenue.discount_amount AS SalesDiscountDollarAmount,
			  'Weight Loss Challenge Deferral' AS RevenueAllocationRule,   ------ Name change
			  'N' AS SoldNotServicedFlag,
			  NULL AS MMSTranID,
			  NULL AS DimTransactionReasonKey,
			  CASE WHEN DimReportingHierarchy.DepartmentName = 'Devices'
			       THEN 'OF10104'
				   WHEN DimReportingHierarchy.DepartmentName = 'Fitness Products'
				   THEN 'OF10220'
				   WHEN DimReportingHierarchy.DepartmentName = 'Lab Testing'
				   THEN 'OF10217'
				   WHEN DimReportingHierarchy.DepartmentName = 'Metabolic Assessments'
				   THEN 'OF10108'
				   WHEN DimReportingHierarchy.DepartmentName = 'Metabolic Conditioning'
				   THEN 'OF10224'
				   WHEN DimReportingHierarchy.DepartmentName = 'PT E-Commerce'
				   THEN 'OF10122'
				   WHEN DimReportingHierarchy.DepartmentName in('Weight Loss Challenges','90 Day Weight Loss')
				   THEN 'OF10117'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Alpha'
				   THEN 'OF10091'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Boot Camp'
				   THEN 'OF10094'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Burn'
				   THEN 'OF10095'
				   WHEN DimReportingHierarchy.DepartmentName = 'Team Cut'
				   THEN 'OF10096'
				   WHEN DimReportingHierarchy.DepartmentName = 'Tri-PT'
				   THEN 'OF10121'
				   WHEN DimReportingHierarchy.DepartmentName = 'MyHealthScore'
				   THEN 'OF10118'
				   WHEN DimReportingHierarchy.DepartmentName = 'Personal Training'
				   THEN 'OF10084'
				   END WorkdayOffering,
			  CASE WHEN DimReportingHierarchy.DepartmentName in('Devices','Fitness Products','PT E-Commerce')
		        THEN 'N'
				ELSE 'Y'
				END PTServiceFlag,
              'Magento' AS SalesSource,		 				
       	      DimECommerceProduct.sku AS SKU,   
			  DimECommerceProduct.product_name AS ProductDescription, 
			  DimReportingHierarchy.ProductGroupName,
			  DimReportingHierarchy.DepartmentName,
			  DimReportingHierarchy.SubdivisionName,
			  DimReportingHierarchy.DivisionName,
			  CASE WHEN FactECommerceRevenue.refund_flag = 'N'
                    THEN 'Sale'
                   ELSE 'Refund' 
              END TransactionType,
			  DimECommerceProduct.sku AS SourceProductID,
			  'E-Commerce' AS SalesChannel								
  FROM [marketing].[v_fact_magento_allocated_transaction_item] FactECommerceRevenue				
  JOIN [marketing].[v_dim_date] TransactionPostDimDate				
    ON FactECommerceRevenue.invoice_dim_date_key = TransactionPostDimDate.dim_date_key	
  JOIN [marketing].[v_dim_date] PriorMonthStartDate
    ON TransactionPostDimDate.prior_month_starting_dim_date_key = PriorMonthStartDate.dim_date_key			
  JOIN [marketing].[v_dim_magento_product_history] DimECommerceProduct				
    ON FactECommerceRevenue.dim_magento_product_key = DimECommerceProduct.dim_magento_product_key
	 AND DimECommerceProduct.effective_date_time <= @EndDate				
     AND DimECommerceProduct.expiration_date_time > @EndDate					
  LEFT JOIN [marketing].[v_dim_employee] DimEmployee				
    ON FactECommerceRevenue.commissioned_sales_dim_employee_key = DimEmployee.dim_employee_key				
     AND DimEmployee.dim_employee_key not in('-997','-998','-999')			
  JOIN #DimClubKeyList DimClub				
    ON FactECommerceRevenue.dim_club_key = DimClub.DimClubKey					
  JOIN #DimReportingHierarchy DimReportingHierarchy
    ON 	DimECommerceProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey					
 WHERE FactECommerceRevenue.invoice_dim_date_key >= @EComm60DayChallengeRevenueStartDimDateKey			
   AND FactECommerceRevenue.invoice_dim_date_key <= @EComm60DayChallengeRevenueEndDimDateKey					
   AND DimReportingHierarchy.PTDeferredRevenueProductGroupFlag = 'Y'  



IF OBJECT_ID('tempdb.dbo.#ReportMonthTransactionMembers', 'U') IS NOT NULL
  DROP TABLE #ReportMonthTransactionMembers;

  ----- to find all current month business members				
CREATE TABLE #ReportMonthTransactionMembers (DimMMSMemberKey VARCHAR(32),MemberID INT,JoinDimDateKey VARCHAR(32), ReportMonthAmount DECIMAL(10,2),PTServiceFlag Varchar(1))				
INSERT INTO #ReportMonthTransactionMembers (DimMMSMemberKey, MemberID,JoinDimDateKey,ReportMonthAmount,PTServiceFlag)				
SELECT 	DimMember.dim_mms_member_key,
        DimMember.member_id,
		DimMember.join_date_key,
		Sum(ReportMonthCustomers.ItemAmount),
		ReportMonthCustomers.PTServiceFlag   
FROM #OldNewBusinessCustomerRecords  ReportMonthCustomers												
JOIN [marketing].[v_dim_mms_member] DimMember 			----- New Name
  ON ReportMonthCustomers.OldNewBusinessDimMMSMemberKey = DimMember.dim_mms_member_key
WHERE ReportMonthCustomers.RevenuePostingMonthStartingDimDateKey = @FirstOfCurrentMonthDimDateKey				
  AND ReportMonthCustomers.TransactionPostDimDateKey <= @ENDDimDateKey							
  GROUP BY DimMember.dim_mms_member_key,
        DimMember.member_id,
		DimMember.join_date_key,
		ReportMonthCustomers.PTServiceFlag
		


IF OBJECT_ID('tempdb.dbo.#OldBusinessMembers1', 'U') IS NOT NULL
  DROP TABLE #OldBusinessMembers1;
  	
CREATE TABLE #OldBusinessMembers1 (				
MemberID INT,				
DimMMSMemberKey VARCHAR(32),				
Amount Decimal(10,2),								
PTServiceFlag Varchar(1))   				
			
				
INSERT INTO #OldBusinessMembers1 (				
MemberID,				
DimMMSMemberKey,				
Amount,							
PTServiceFlag) 				
	

			
 ----- Find which of the current month business members have had business in the prior 2 months				
SELECT DimCustomer.MemberID,
       DimCustomer.DimMMSMemberKey,				
       SUM(OldBusinessRecords.ItemAmount),				
	   OldBusinessRecords.PTServiceFlag
FROM #OldNewBusinessCustomerRecords	OldBusinessRecords				
JOIN #ReportMonthTransactionMembers DimCustomer				
  ON OldBusinessRecords.OldNewBusinessDimMMSMemberKey = DimCustomer.DimMMSMemberKey
  AND DimCustomer.PTServiceFlag = 'Y'				
JOIN #DimClubKeyList DimClub
  ON OldBusinessRecords.DimClubKey = DimClub.DimClubKey				
WHERE  OldBusinessRecords.TransactionPostDimDateKey < @FirstOfCurrentMonthDimDateKey				
  AND OldBusinessRecords.RevenuePostingMonthStartingDimDateKey< @FirstOfCurrentMonthDimDateKey				
  AND DimClub.NewBusinessOnlyClub = 'N'	
  AND OldBusinessRecords.PTServiceFlag = 'Y'
 GROUP BY DimCustomer.MemberID,
       DimCustomer.DimMMSMemberKey,
	   OldBusinessRecords.PTServiceFlag
	

				
IF OBJECT_ID('tempdb.dbo.#OldBusinessMembers', 'U') IS NOT NULL
  DROP TABLE #OldBusinessMembers;				

	 ---- to eliminate from Old Business, members who had only fully refunded purchases		
CREATE TABLE #OldBusinessMembers (				
MemberID INT,				
DimMMSMemberKey VARCHAR(32),
PTServiceFlag Varchar(1))				
				
INSERT INTO #OldBusinessMembers (				
MemberID,				
DimMMSMemberKey,
PTServiceFlag)				
				
SELECT MemberID,
       DimMMSMemberKey,
	   PTServiceFlag				
FROM #OldBusinessMembers1				
  WHERE  Amount <> 0	
  
			
IF OBJECT_ID('tempdb.dbo.#Results_ReportMonth_Memberships', 'U') IS NOT NULL
  DROP TABLE #Results_ReportMonth_Memberships;	

  ---- Allocate current month transactions 
SELECT ReportMonthCustomer.DimMMSMemberKey,				
       ReportMonthCustomer.JoinDimDateKey,						
	   DimClub.ClubName,			
	   DimClub.MMSClubID,			
	   DimClub.ClubCode,
	   DimClub.DimClubKey,				
	   'Local Currency' AS HeaderReportingCurrency,	
	   CASE WHEN CurrentMonthRecords.PTServiceFlag ='Y'
	        THEN 'Services'
			ELSE 'Products'
			END ProductsOrServicesGrouping,			
	   CurrentMonthRecords.DimReportingHierarchyKey,
	   SUM(CurrentMonthRecords.ItemAmount) AS ItemAmount,
	   			 
	   SUM(CASE WHEN CurrentMonthRecords.TransactionPostDimDateKey = @EndDimDateKey
	        THEN CurrentMonthRecords.ItemAmount
			WHEN (CurrentMonthRecords.TransactionPostDimDateKey < @FirstOfCurrentMonthDimDateKey) and (@EndDimDateKey = @FirstOfCurrentMonthDimDateKey)
			THEN CurrentMonthRecords.ItemAmount
			ELSE 0
			END) Today_ItemAmount,	

	   SUM(CASE WHEN CurrentMonthRecords.PTServiceFlag ='Y'	                                          ------ Service products only	
				          AND (ReportMonthCustomer.JoinDimDateKey >= @FirstOfPriorMonthDimDateKey      ---- joined since the 1st of the prior month - even if they bought something last month
						        OR DimClub.NewBusinessOnlyClub = 'Y')                              ---- or club is new
				THEN  CurrentMonthRecords.ItemAmount                                          
				ELSE 0
		    END) AS NewBusiness_NewMember_Amount,

	   SUM(CASE WHEN IsNull(#OldBusinessMembers.MemberID,0) = 0      ------ Not old business member
		           AND CurrentMonthRecords.PTServiceFlag ='Y'			------ Service products only	
				   AND ReportMonthCustomer.JoinDimDateKey < @FirstOfPriorMonthDimDateKey      ---- joined prior to the 1st of the prior month
				   AND DimClub.NewBusinessOnlyClub = 'N'            ---- and club is not new
				   AND CurrentMonthRecords.RefundFlag = 'N'              ---- and Transaction is not a refund
				THEN  CurrentMonthRecords.ItemAmount
				  ELSE 0
		   END) AS NewBusiness_ExistingMember_Amount,  


		SUM(CASE WHEN IsNull(#OldBusinessMembers.MemberID,0) > 0        ----- Old Business member
		           AND  CurrentMonthRecords.PTServiceFlag ='Y'			----- Service products only
				   AND CurrentMonthRecords.ChargeFlag = 'Y'            ----- Charge transaction  
				   AND ReportMonthCustomer.JoinDimDateKey < @FirstOfPriorMonthDimDateKey     ---- Did not join since the 1st of the prior month
				   AND DimClub.NewBusinessOnlyClub = 'N'                   ---  club is not new
				THEN  CurrentMonthRecords.ItemAmount
				WHEN  CurrentMonthRecords.RefundFlag = 'Y'               ----- For all refunds for non-new members (includes refunds for those who would otherwise be "existing members")
				   AND CurrentMonthRecords.PTServiceFlag ='Y'
				   AND ReportMonthCustomer.JoinDimDateKey < @FirstOfPriorMonthDimDateKey      ---- joined prior to the 1st of the prior month
				   AND DimClub.NewBusinessOnlyClub = 'N' 
                THEN  CurrentMonthRecords.ItemAmount
				ELSE 0
				END) AS OldBusiness_EFT_Amount,

		SUM(CASE WHEN IsNull(#OldBusinessMembers.MemberID,0) > 0        ----- Old Business member
		           AND CurrentMonthRecords.PTServiceFlag ='Y'           ----- Service products only
		           AND CurrentMonthRecords.ChargeFlag = 'N'             ----- not Charge or a refund transaction
				   AND CurrentMonthRecords.RefundFlag = 'N'	
				   AND ReportMonthCustomer.JoinDimDateKey < @FirstOfPriorMonthDimDateKey     ---- Did not join since the 1st of the prior month
				   AND DimClub.NewBusinessOnlyClub = 'N'                   ---  club is not new
				 THEN  CurrentMonthRecords.ItemAmount
				 ELSE 0
			END) AS OldBusiness_NonEFT_Amount,

		SUM(CASE WHEN CurrentMonthRecords.PTServiceFlag ='N'		    ----- Non-Service products only
				 THEN  CurrentMonthRecords.ItemAmount
				 ELSE 0
			END) AS Products_Amount,
		CurrentMonthRecords.TransactionType
		
INTO #Results_ReportMonth_Memberships				
FROM #OldNewBusinessCustomerRecords CurrentMonthRecords										
JOIN #ReportMonthTransactionMembers ReportMonthCustomer				
  ON CurrentMonthRecords.OldNewBusinessDimMMSMemberKey = ReportMonthCustomer.DimMMSMemberKey	
  AND CurrentMonthRecords.PTServiceFlag = ReportMonthCustomer.PTServiceFlag
JOIN #DimClubKeyList DimClub				
  ON CurrentMonthRecords.DimClubKey = DimClub.DimClubKey				
LEFT JOIN #OldBusinessMembers #OldBusinessMembers				
  ON ReportMonthCustomer.DimMMSMemberKey = #OldBusinessMembers.DimMMSMemberKey
  AND #OldBusinessMembers.PTServiceFlag = ReportMonthCustomer.PTServiceFlag				
WHERE CurrentMonthRecords.RevenuePostingMonthStartingDimDateKey = @FirstOfCurrentMonthDimDateKey				
  AND CurrentMonthRecords.TransactionPostDimDateKey <= @ENDDimDateKey								
GROUP BY  ReportMonthCustomer.DimMMSMemberKey,				
       ReportMonthCustomer.JoinDimDateKey,						
	   DimClub.ClubName,			
	   DimClub.MMSClubID,			
	   DimClub.ClubCode,
	   DimClub.DimClubKey,				
       CASE WHEN CurrentMonthRecords.PTServiceFlag ='Y'
	        THEN 'Services'
			ELSE 'Products'
			END,					
	   CurrentMonthRecords.DimReportingHierarchyKey,
	   CurrentMonthRecords.TransactionType


IF OBJECT_ID('tempdb.dbo.#Results_OldNewBusiness_ByMemberAndHierarchyKey', 'U') IS NOT NULL
  DROP TABLE #Results_OldNewBusiness_ByMemberAndHierarchyKey;	

	----- Set Allocation for each member based on MTD transactions   

  SELECT DimMMSMemberKey,
  DimReportingHierarchyKey,
  DimClubKey,	
  Convert(varchar,DimMMSMemberKey) + '-'+ Convert(varchar,DimReportingHierarchyKey) AS joinbusinesstypekey,
  @FirstOfCurrentMonthDimDateKey AS firsttransactiondimdatekey,
  @ENDDimDateKey AS lasttransactiondimdatekey,
  @FirstOfCurrentMonthDimDateKey AS monthstartingdimdatekey,
  CASE WHEN Products_Amount <> 0
       THEN 'Products'
       WHEN (NewBusiness_ExistingMember_Amount + NewBusiness_NewMember_Amount) <> 0
       THEN 'New Business'
	   ELSE 'Old Business'
	   END BusinessType,
  CASE WHEN Products_Amount <> 0
       THEN 'Products'
       WHEN NewBusiness_NewMember_Amount <> 0
       THEN 'New Member'
	   WHEN NewBusiness_ExistingMember_Amount <>0
	   THEN 'Existing Member'
	   WHEN OldBusiness_EFT_Amount <> 0
	   THEN 'EFT Amount'	   
	   ELSE 'Non-EFT Amount'
	   END BusinessSubType,
   SUM(ItemAmount) AS ItemAmount,
   SUM(Today_ItemAmount) AS Today_ItemAmount,
   TransactionType
  INTO #Results_OldNewBusiness_ByMemberAndHierarchyKey  
  FROM 	#Results_ReportMonth_Memberships 
  WHERE ItemAmount <> 0
  GROUP BY 
      DimMMSMemberKey,
      DimReportingHierarchyKey,
	  DimClubKey,	
      Convert(varchar,DimMMSMemberKey) + '-'+ Convert(varchar,DimReportingHierarchyKey),
  CASE WHEN Products_Amount <> 0
       THEN 'Products'
       WHEN (NewBusiness_ExistingMember_Amount + NewBusiness_NewMember_Amount) <> 0
       THEN 'New Business'
	   ELSE 'Old Business'
	   END,
  CASE WHEN Products_Amount <> 0
       THEN 'Products'
       WHEN NewBusiness_NewMember_Amount <> 0
       THEN 'New Member'
	   WHEN NewBusiness_ExistingMember_Amount <>0
	   THEN 'Existing Member'
	   WHEN OldBusiness_EFT_Amount <> 0
	   THEN 'EFT Amount'	   
	   ELSE 'Non-EFT Amount'
	   END,
	   TransactionType

	   
	   
	   
		
	
 -------------- Gathering forecasted amount

DECLARE @ReportDateFourDigitYearTwoDigitMonth Varchar(6)
DECLARE @ReportMonthLastDayOfMonth DateTime

SET @ReportDateFourDigitYearTwoDigitMonth = (Select Substring(four_digit_year_two_digit_month_two_digit_day,1,6) From [marketing].[v_dim_date] Where dim_date_key = @EndDimDateKey)
SET @ReportMonthLastDayOfMonth = (SELECT month_ending_date FROM [marketing].[v_dim_date] Where dim_date_key = @EndDimDateKey)

 ----- Recurrent products converted to Exerp
 IF OBJECT_ID('tempdb.dbo.#NextAssessmentDate_Prelim', 'U') IS NOT NULL
  DROP TABLE  #NextAssessmentDate_Prelim;

 ----- To Determine next assessment date on subscriptions based on subscription periods
-----  Assumption - payment is 1 month in advance of subscriptionPeriodTo
----- This will eliminate subscriptions which have already ended in the prior month
SELECT SubscriptionPeriod.dim_exerp_subscription_key,
       MAX(SubscriptionPeriod.from_dim_date_key) AS from_dim_date_key,
       MAX(SubscriptionPeriod.to_dim_date_key) AS to_dim_date_key,
	   MAX(DateAdd(day,1,ToDimDate.calendar_date)) AS NextAssessment,
	   Day(MAX(DateAdd(day,1,ToDimDate.calendar_date))) AS AssessmentDayOfMonth,
	   Subscription.dim_club_key AS DimClubKey,
       Subscription.dim_exerp_product_key AS DimExerpProductKey,
       Subscription.price AS Price
INTO #NextAssessmentDate_Prelim     
FROM [marketing].[v_dim_exerp_subscription_period] SubscriptionPeriod 
  JOIN [marketing].[v_dim_date] ToDimDate
    ON SubscriptionPeriod.to_dim_date_key = ToDimDate.dim_date_key
  JOIN [marketing].[v_dim_exerp_subscription] Subscription
    ON SubscriptionPeriod.dim_exerp_subscription_key = Subscription.dim_exerp_subscription_key
  
Where ToDimDate.calendar_date > @EndDate   ----- to return all subscriptions that are set to yet assess in the current month and beyond
  AND (Subscription.end_dim_date_key = '-998' 
     OR Subscription.end_dim_date_key > @EndDimDateKey) ----not terminated or terminated after the report date
GROUP BY SubscriptionPeriod.dim_exerp_subscription_key,
         Subscription.dim_club_key,
         Subscription.dim_exerp_product_key,
         Subscription.price

		 

 IF OBJECT_ID('tempdb.dbo.#NextAssessmentDate', 'U') IS NOT NULL
DROP TABLE #NextAssessmentDate;
 ---- to limit the temp table to just the remaining dates in the report month

SELECT dim_exerp_subscription_key,
       from_dim_date_key,
       to_dim_date_key,
	   NextAssessment,
	   AssessmentDayOfMonth,
	   DimClubKey,
       DimExerpProductKey,
       Price
INTO #NextAssessmentDate    
FROM #NextAssessmentDate_Prelim 
 WHERE NextAssessment > @EndDate
   AND NextAssessment <= @ReportMonthLastDayOfMonth

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

SELECT
    ChangeLog.dim_exerp_subscription_key,
	DimEmployee.dim_employee_key,
	DimEmployee.employee_id,
	DimEmployee.last_name,
	DimEmployee.first_name,
	DimEmployee.middle_name,
	CASE WHEN (ISNULL(ChangeLog.dim_employee_key,'-998') <> '-998' ) 
	        THEN DimEmployee.last_name +', '+ DimEmployee.first_name 
			ELSE 'None Designated' 
			END CommisionedEmployee 
 INTO #LatestCommissionableEmployee    
 FROM [marketing].[v_dim_exerp_subscription_change_log] ChangeLog
 JOIN #LastChangeLog last_ChangeLog 
   ON ChangeLog.subscription_change_log_id = last_ChangeLog.subscription_change_log_id
 JOIN [marketing].[v_dim_employee] DimEmployee 
   ON ChangeLog.dim_employee_key = DimEmployee.dim_employee_key

--- Get the forecast transaction detail
  IF OBJECT_ID('tempdb.dbo.#PrelimEmployeeRevenueForecast_Exerp', 'U') IS NOT NULL
DROP TABLE #PrelimEmployeeRevenueForecast_Exerp;

SELECT RemainingAssessments.dim_exerp_subscription_key,
       IsNull(CommissionEmployee.employee_id,-998) AS CommissionEmployeeID,
	   IsNull(CommissionEmployee.dim_employee_key, '-998') AS CommissionDimMMSEmployeeKey,
	   DimClub.MMSClubID,
	   DimClub.NewBusinessOnlyClub,
	   RemainingAssessments.Price,
	   RemainingAssessments.NextAssessment,
	   RemainingAssessments.AssessmentDayOfMonth,
	   DimMMSProduct.product_id
 INTO #PrelimEmployeeRevenueForecast_Exerp     
FROM #NextAssessmentDate RemainingAssessments   
 LEFT JOIN #LatestCommissionableEmployee CommissionEmployee
   ON RemainingAssessments.dim_exerp_subscription_key = CommissionEmployee.dim_exerp_subscription_key
 LEFT JOIN #DimClubKeyList DimClub
   ON RemainingAssessments.DimClubKey = DimClub.DimClubKey
 LEFT JOIN [marketing].[v_dim_exerp_product] DimExerpProduct
   ON RemainingAssessments.DimExerpProductKey = DimExerpProduct.dim_exerp_product_key
 LEFT JOIN [marketing].[v_dim_mms_product_history] DimMMSProduct
   ON DimExerpProduct.dim_mms_product_key = DimMMSProduct.dim_mms_product_key
      AND DimMMSProduct.effective_date_time <= @EndDate
      AND DimMMSProduct.expiration_date_time > @EndDate 



 ----- Recurrent products in MMS not yet converted to Exerp

IF OBJECT_ID('tempdb.dbo.#PreliminaryKeyList', 'U') IS NOT NULL
  DROP TABLE #PreliminaryKeyList;	

  ---- to remove $0, terminated, yet not activated and already assessed recurrent products from the query
Select FactMembershipRecurrentProduct.fact_mms_membership_recurrent_product_key AS FactMembershipRecurrentProductKey,
FactMembershipRecurrentProduct.activation_dim_date_key AS ActivationDimDateKey,
FactMembershipRecurrentProduct.termination_dim_date_key AS TerminationDimDateKey,
FactMembershipRecurrentProduct.membership_id AS MembershipID,
FactMembershipRecurrentProduct.membership_recurrent_product_id AS MembershipRecurrentProductID,
FactMembershipRecurrentProduct.hold_start_dim_date_key AS HoldStartDimDateKey, 
FactMembershipRecurrentProduct.hold_end_dim_date_key AS HoldEndDimDateKey, 
FactMembershipRecurrentProduct.last_assessment_dim_date_key AS LastAssessmentDimDateKey, 
CASE WHEN LEN(FactMembershipRecurrentProduct.assessment_day_of_month) < 2
     THEN '0'+ Convert(varchar,FactMembershipRecurrentProduct.assessment_day_of_month)
	 ELSE Convert(varchar,FactMembershipRecurrentProduct.assessment_day_of_month)
	 END AssessmentDayOfMonth_Varchar,
FactMembershipRecurrentProduct.commission_dim_mms_employee_key AS CommissionDimMMSEmployeeKey,
FactMembershipRecurrentProduct.Price,
FactMembershipRecurrentProduct.dim_club_key AS DimClubKey,
FactMembershipRecurrentProduct.dim_mms_product_key AS DimMMSProductKey,
DimProduct.dim_reporting_hierarchy_key AS DimReportingHierarchyKey
INTO #PreliminaryKeyList          
FROM [marketing].[v_fact_mms_membership_recurrent_product]  FactMembershipRecurrentProduct 
JOIN #DimClubKeyList DimClub
 ON FactMembershipRecurrentProduct.dim_club_key = DimClub.DimClubKey
JOIN [marketing].[v_dim_mms_product_history] DimProduct
 ON FactMembershipRecurrentProduct.dim_mms_product_key = DimProduct.dim_mms_product_key
  AND DimProduct.effective_date_time <= @EndDate
  AND DimProduct.expiration_date_time > @EndDate 
JOIN #DimReportingHierarchy DimReportingHierarchy
 ON DimProduct.dim_reporting_hierarchy_key = DimReportingHierarchy.DimReportingHierarchyKey
WHERE
(IsNull(FactMembershipRecurrentProduct.termination_dim_date_key,'-998') = '-998' OR FactMembershipRecurrentProduct.termination_dim_date_key >= @EndDimDateKey)
AND FactMembershipRecurrentProduct.price > 0
AND (FactMembershipRecurrentProduct.last_assessment_dim_date_key < @FirstOfCurrentMonthDimDateKey
     OR FactMembershipRecurrentProduct.last_assessment_dim_date_key > @EndDimDateKey)   ----- needed in the event a prior date is selected as the report date


IF OBJECT_ID('tempdb.dbo.#MembershipRecurrentProductIDList', 'U') IS NOT NULL
  DROP TABLE #MembershipRecurrentProductIDList;

  ---- to further remove recurrent products which are on hold or related to suspended memberships
    ----  also to only include those which are yet to assess in the current month and will not terminate before assessing

SELECT PreliminaryKeyList.FactMembershipRecurrentProductKey,
       PreliminaryKeyList.MembershipRecurrentProductID,
       PreliminaryKeyList.HoldStartDimDateKey, 
	   PreliminaryKeyList.HoldEndDimDateKey, 
	   PreliminaryKeyList.LastAssessmentDimDateKey, 
       PreliminaryKeyList.AssessmentDayOfMonth_Varchar,
	   PreliminaryKeyList.TerminationDimDateKey,
	   PreliminaryKeyList.ActivationDimDateKey,
	   PreliminaryKeyList.CommissionDimMMSEmployeeKey,
	   PreliminaryKeyList.Price,
	   PreliminaryKeyList.DimClubKey,
	   PreliminaryKeyList.DimMMSProductKey,
	   PreliminaryKeyList.DimReportingHierarchyKey,
       Convert(INT,(@ReportDateFourDigitYearTwoDigitMonth + AssessmentDayOfMonth_Varchar)) AS NextAssessmentDate,
       MembershipStatusDescription.description AS MembershipStatus
INTO #MembershipRecurrentProductIDList       
FROM #PreliminaryKeyList PreliminaryKeyList
JOIN [marketing].[v_dim_mms_membership_history] DimMembershipHistory
  ON PreliminaryKeyList.MembershipID = DimMembershipHistory.membership_id
   AND DimMembershipHistory.effective_date_time <= @EndDate
   AND DimMembershipHistory.expiration_date_time > @EndDate
JOIN [marketing].[v_dim_description] MembershipStatusDescription
  ON DimMembershipHistory.membership_status_dim_description_key = MembershipStatusDescription.dim_description_key
WHERE 
(Convert(INT,(@ReportDateFourDigitYearTwoDigitMonth + PreliminaryKeyList.AssessmentDayOfMonth_Varchar)) < PreliminaryKeyList.HoldStartDimDateKey    ------ assessment before hold period
     OR Convert(INT,(@ReportDateFourDigitYearTwoDigitMonth + PreliminaryKeyList.AssessmentDayOfMonth_Varchar)) > PreliminaryKeyList.HoldEndDimDateKey)         ------ assessment after hold period
AND Convert(INT,(@ReportDateFourDigitYearTwoDigitMonth + PreliminaryKeyList.AssessmentDayOfMonth_Varchar)) > @EndDimDateKey                               ------ assessment after report date
AND Convert(INT,(@ReportDateFourDigitYearTwoDigitMonth + PreliminaryKeyList.AssessmentDayOfMonth_Varchar)) >= PreliminaryKeyList.ActivationDimDateKey     ------ assessment on or after activation date
AND (IsNull(PreliminaryKeyList.TerminationDimDateKey,'-998') = '-998'  OR (Convert(INT,(@ReportDateFourDigitYearTwoDigitMonth + PreliminaryKeyList.AssessmentDayOfMonth_Varchar)) < PreliminaryKeyList.TerminationDimDateKey))  ------ assessment before any scheduled termination
AND MembershipStatusDescription.Description <> 'Suspended'


IF OBJECT_ID('tempdb.dbo.#EmployeeRevenueForecast_MMS', 'U') IS NOT NULL
  DROP TABLE #EmployeeRevenueForecast_MMS;

  ----  Then to return the Total price by commission employee from the most recently staged record for each recurrent product
  ---- Commissioned employee is not found in the recurrent product table in LTFDW
SELECT IsNull(DimEmployee.employee_id,-998) AS CommissionEmployeeID,
     IsNull(#MembershipRecurrentProductIDList.CommissionDimMMSEmployeeKey,-998) AS CommissionDimMMSEmployeeKey,
     DimClub.MMSClubID,
	 DimClub.NewBusinessOnlyClub,
	 Sum(#MembershipRecurrentProductIDList.Price) AS EmployeeRecurrentProductForecast
INTO #EmployeeRevenueForecast_MMS    
FROM #MembershipRecurrentProductIDList 
  JOIN #DimClubKeyList DimClub
    ON #MembershipRecurrentProductIDList.DimClubKey = DimClub.DimClubKey
  LEFT JOIN [marketing].[v_dim_employee]  DimEmployee
    ON IsNull(#MembershipRecurrentProductIDList.CommissionDimMMSEmployeeKey,-998) = DimEmployee.dim_employee_key

GROUP BY IsNull(DimEmployee.employee_id,-998),
     IsNull(#MembershipRecurrentProductIDList.CommissionDimMMSEmployeeKey,-998),
     DimClub.MMSClubID,
	 DimClub.NewBusinessOnlyClub


IF OBJECT_ID('tempdb.dbo.#EmployeeRevenueForecast', 'U') IS NOT NULL
  DROP TABLE #EmployeeRevenueForecast;

SELECT CommissionEmployeeID,
     CommissionDimMMSEmployeeKey,
     MMSClubID,
	 NewBusinessOnlyClub,
	 EmployeeRecurrentProductForecast 
 INTO #EmployeeRevenueForecast   
FROM #EmployeeRevenueForecast_MMS

UNION ALL

SELECT CommissionEmployeeID,
       CommissionDimMMSEmployeeKey,
	   MMSClubID,
	   NewBusinessOnlyClub,
	   SUM(Price) AS EmployeeRecurrentProductForecast
FROM  #PrelimEmployeeRevenueForecast_Exerp   
GROUP BY CommissionEmployeeID,
       CommissionDimMMSEmployeeKey,
	   MMSClubID,
	   NewBusinessOnlyClub



IF OBJECT_ID('tempdb.dbo.#Results_Summary', 'U') IS NOT NULL
  DROP TABLE #Results_Summary;
  		
 ---------- Pulling all totals together
 
SELECT '-998' AS  DimMemberKey,   ------ New Name
	   MMSClubID,
	   CommissionEmployeeID as PrimarySalesEmployeeID,
	   CommissionDimMMSEmployeeKey AS PrimarySalesDimEmployeeKey,
	   '-998' AS DimReportingHierarchyKey,
	   '-998' +'-'+ '-998' AS joinbusinesstypekey,
	   @FirstOfCurrentMonthDimDateKey AS firsttransactiondimdatekey,
       @EndDimDateKey AS lasttransactiondimdatekey,
       @FirstOfCurrentMonthDimDateKey AS monthstartingdimdatekey,
	   CASE WHEN NewBusinessOnlyClub = 'Y'
	        THEN 'New Business'
			ELSE 'Old Business'
			END BusinessType,
	   CASE WHEN NewBusinessOnlyClub = 'Y'
	        THEN 'New Member'
			ELSE 'EFT Amount'
			END  BusinessSubType,
	   0 AS ItemAmount,
	   0 AS Today_ItemAmount,
	   EmployeeRecurrentProductForecast AS ForecastedAmount		
  INTO #Results_Summary	   	
	 FROM #EmployeeRevenueForecast

UNION ALL

SELECT Detail.OldNewBusinessDimMMSMemberKey AS DimMMSMemberKey,   ---- New Name
       DimClub.MMSClubID,  
       IsNull(DimEmployee.employee_id,-998) as PrimarySalesEmployeeID, 
	   Detail.PrimarySalesDimEmployeeKey,
	   Detail.DimReportingHierarchyKey,  
	   Detail.OldNewBusinessDimMMSMemberKey +'-'+ Detail.DimReportingHierarchyKey as joinbusinesstypekey, 
	   @FirstOfCurrentMonthDimDateKey AS firsttransactiondimdatekey,  
	   @EndDimDateKey AS lasttransactiondimdatekey,
	   @FirstOfCurrentMonthDimDateKey AS monthstartingdimdatekey,  
       OldNewCategorySummary.BusinessType,  
	   OldNewCategorySummary.BusinessSubType,  
	   SUM(Detail.ItemAmount) ItemAmount,  
	   SUM(Detail.Today_ItemAmount) Today_ItemAmount,  
	   0 AS ForecastedAmount

	   FROM #OldNewBusinessCustomerRecords Detail
	   JOIN #Results_OldNewBusiness_ByMemberAndHierarchyKey  OldNewCategorySummary
		 ON Detail.OldNewBusinessDimMMSMemberKey = OldNewCategorySummary.DimMMSMemberKey
			 AND Detail.DimReportingHierarchyKey = OldNewCategorySummary.DimReportingHierarchyKey
			 AND Detail.DimClubKey = OldNewCategorySummary.DimClubKey
			 AND Detail.TransactionType = OldNewCategorySummary.TransactionType
			 AND OldNewCategorySummary.ItemAmount <> 0       
	   JOIN #DimClubKeyList DimClub
		 ON Detail.DimClubKey = DimClub.DimClubKey
	   LEFT JOIN [marketing].[v_dim_employee] DimEmployee
		 ON Detail.PrimarySalesDimEmployeeKey = DimEmployee.dim_employee_key

	 WHERE Detail.RevenuePostingMonthStartingDimDateKey = @FirstOfCurrentMonthDimDateKey				
            AND Detail.TransactionPostDimDateKey <= @ENDDimDateKey	
     GROUP BY Detail.OldNewBusinessDimMMSMemberKey,
              DimClub.MMSClubID,
              IsNull(DimEmployee.employee_id,-998), 
			  Detail.PrimarySalesDimEmployeeKey, 
	          Detail.DimReportingHierarchyKey,
	          Detail.OldNewBusinessDimMMSMemberKey +'-'+ Detail.DimReportingHierarchyKey,
              OldNewCategorySummary.BusinessType,  
	          OldNewCategorySummary.BusinessSubType


 
 ------   Delete records for 14 months prior except for the final day's records for each month
  DELETE fact_ptdssr_old_and_new_business_employee_summary
  WHERE report_date_dim_date_key < @FirstOf13MonthsPriorDimDateKey
    AND report_date_is_last_day_in_month_indicator = 'N'


 ------  Populate table with new records

  INSERT INTO fact_ptdssr_old_and_new_business_employee_summary (
  business_sub_type,
  business_type,
  dim_club_key,
  dim_employee_key,
  employee_id,
  forecast_amount,
  mms_club_id,
  month_to_date_revenue_item_amount,
  report_date_dim_date_key,
  report_date_is_last_day_in_month_indicator,
  report_date_item_amount,
  dv_load_date_time,		-- need to include all dv_columns in stored procedure
  dv_load_end_date_time,	-- need to include all dv_columns in stored procedure
  dv_batch_id,				-- need to include all dv_columns in stored procedure
  dv_inserted_date_time,	-- need to include all dv_columns in stored procedure
  dv_insert_user			-- need to include all dv_columns in stored procedure
  )

	SELECT
	   BusinessSubType,
	   BusinessType,
	   #DimClubKeyList.DimClubKey,
	   #Results_Summary.PrimarySalesDimEmployeeKey,
	   #Results_Summary.PrimarySalesEmployeeID,
	   Sum(ForecastedAmount),
       #DimClubKeyList.MMSClubID,
	   Sum(ItemAmount),
	   #Results_Summary.LastTransactionDimDateKey,
	   @ReportDateLastDayInMonthIndicator,
	   Sum(Today_ItemAmount),
	   getdate(),												--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
	   convert(datetime, '99991231', 112),						--this value would be same for all the stored procedure
	   '-1',													--default value is getdate() or we can also use the dv_load_date_time from tables used in stored procedure
	   getdate(),												--this value would be same for all the stored procedure
       suser_sname()											--this value would be same for all the stored procedure
	 From #Results_Summary
	   JOIN #DimClubKeyList
	    ON #Results_Summary.MMSClubID = #DimClubKeyList.MMSClubID

	 Group By 
	   #Results_Summary.LastTransactionDimDateKey,
	   #DimClubKeyList.DimClubKey,
	   #DimClubKeyList.MMSClubID,
	   #Results_Summary.PrimarySalesDimEmployeeKey,
	   #Results_Summary.PrimarySalesEmployeeID,
	   #Results_Summary.BusinessType,
	   #Results_Summary.BusinessSubType
	   


   DROP TABLE #ReportMonthTransactionMembers				
   DROP TABLE #OldBusinessMembers1				
   DROP TABLE #OldBusinessMembers
   DROP TABLE #Results_Summary
   DROP TABLE #DimClubKeyList
   DROP TABLE #DimReportingHierarchy
   DROP TABLE #PreliminaryKeyList
   DROP TABLE #MembershipRecurrentProductIDList
   DROP TABLE #EmployeeRevenueForecast

   DROP TABLE #OldNewBusinessCustomerRecords
   DROP TABLE #PackagesWherePurchaserIsNotServicedCustomer
   DROP TABLE #Results_ReportMonth_Memberships	
   DROP TABLE #Results_OldNewBusiness_ByMemberAndHierarchyKey
   DROP TABLE #EmployeeRevenueForecast_MMS 
   DROP TABLE #PrelimEmployeeRevenueForecast_Exerp     
   DROP TABLE #LatestCommissionableEmployee    
   DROP TABLE #LastChangeLog    
   DROP TABLE #NextAssessmentDate     
   DROP TABLE #NextAssessmentDate_Prelim     




END
