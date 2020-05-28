CREATE PROC [reporting].[proc_MagentoPaymentBreakdown] @StartDate [Datetime],@EndDate [Datetime] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 

 ----- Execution Sample
 ------ exec [reporting].[proc_MagentoPaymentBreakdown] '4/1/2019','1/1/1900'
 -----
 ----- This stored procedure is used by Report ID 395 - Magento Payment Breakdown 


DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

DECLARE @AdjustedEndDate DATETIME
DECLARE @StartDateDimDateKey INT
DECLARE @AdjustedEndDateDimDateKey INT

SET @StartDate = (CASE WHEN @StartDate = 'Jan 1, 1900'
                       THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 	  -- yesterday
					   WHEN @StartDate = 'Dec 31, 1899'
					   THEN (Select month_starting_date From [marketing].[v_dim_date] WHERE calendar_date = CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) ) ----First of yesterday's month
					   ELSE @StartDate
					   END)
SET @AdjustedEndDate = (CASE WHEN @EndDate = 'Jan 1, 1900' 
                             THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101)   ---- today
					         ELSE DATEADD(Day,1,@EndDate) 
					         END)
SET @StartDateDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @StartDate)
SET @AdjustedEndDateDimDateKey  = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = @AdjustedEndDate)



IF OBJECT_ID('tempdb.dbo.#ProductSubCategoryCalc', 'U') IS NOT NULL
  DROP TABLE #ProductSubCategoryCalc;   

Select [dim_magento_product_key],
       MAX(position) AS Position
	INTO #ProductSubCategoryCalc
 from [marketing].[v_dim_magento_product_category]
 GROUP BY dim_magento_product_key


IF OBJECT_ID('tempdb.dbo.#ProductSubCategory', 'U') IS NOT NULL
  DROP TABLE #ProductSubCategory;   

Select FullP.dim_magento_product_key,
       FullP.url_path AS SubCategory
INTO #ProductSubCategory
FROM [marketing].[v_dim_magento_product_category] FullP
 JOIN #ProductSubCategoryCalc Temp
   ON FullP.dim_magento_product_key = Temp.dim_magento_product_key
   AND FullP.position = Temp.Position

IF OBJECT_ID('tempdb.dbo.#TransactionTotals', 'U') IS NOT NULL
  DROP TABLE #TransactionTotals;



SELECT order_number AS OrderNum,
       order_item_id AS OENum,
       payment_type,
       allocated_dim_club_key,
       dim_magento_product_key,
       order_dim_date_key,
       invoice_dim_date_key,
	   transaction_dim_date_key,
       refund_dim_date_key,
	   dim_mms_member_key,
       refund_flag,
	   shipping_state AS DeliveryState,
	   mms_tran_id AS MMSTransactionID,
	   SUM(transaction_item_amount) AS TransactionItemAmount,
	   SUM(transaction_amount) AS TransactionAmount,
	   SUM(transaction_bucks_amount) AS TransactionBucksAmount,
	   SUM(transaction_item_amount - transaction_discount_amount) AS DiscountedItemAmount,
	   SUM(IsNULL(transaction_tax_amount,0)+IsNULL(shipping_tax_amount,0)) AS Tax,
	   SUM(IsNULL(shipping_amount,0)) AS OrderShippingAmount,
	   batch_number AS BatchNumber,
       credit_tran_id AS PaymenTechCreditTranID
 INTO #TransactionTotals   
FROM [marketing].[v_fact_magento_transaction_item] 
WHERE (transaction_dim_date_key >=  @StartDateDimDateKey
      AND transaction_dim_date_key < @AdjustedEndDateDimDateKey)       ---- this date also holds the refund date
	

GROUP BY order_number,
       order_item_id,
       payment_type,
       allocated_dim_club_key,
       dim_magento_product_key,
       order_dim_date_key,
       invoice_dim_date_key,
	   transaction_dim_date_key,
       refund_dim_date_key,
	   dim_mms_member_key,
       refund_flag,
	   shipping_state,
	   mms_tran_id,
	   batch_number,
       credit_tran_id



SELECT 'WD_COA' AS AccountSet,
       CASE WHEN RevenueClub.club_id = 51     ------- Bloomingdale, IL
	        THEN 'BLOOMJV'
			WHEN RevenueClub.club_id in(100,815,13)   ------- Corporate IT Dept, Corporate Club and Corporate INTERNAL
			THEN 'LTCORP'
			WHEN RevenueClub.local_currency_code = 'CAD'  ----- All Canadian Clubs
			THEN 'LTFOPCOCAN'
			ELSE 'LTFOPCO'
			END LineCompany,
		'430005' AS LedgerAccount,
		RevenueClub.club_id AS ClubID,
        RevenueClub.workday_region AS WorkdayRegion,
		Product.workday_costcenter_id AS CostCenter,
        Product.workday_offering_id AS OfferingID,
		Product.workday_spending_id AS SpendCategory,
        Product.workday_revenue_id AS RevenueCategory,
        'MerchantLocation TBD' AS MerchantLocation,
        TransactionItem.OrderNum,
        TransactionItem.OENum,
		SubCategory.SubCategory AS SubCatName,
		Product.sku AS ProductID,
		Product.product_name AS ProductName,
		MMSProduct.product_id AS MMSProductID,
		TransactionItem.DeliveryState,
        TransactionItem.Tax,
		TransactionItem.OrderShippingAmount,
		OrderDate.calendar_date AS OrdDate,
		CASE WHEN TransactionItem.refund_flag = 'Y'
		     THEN RefundDate.calendar_date
			 ELSE InvoiceDate.calendar_date 
			 END TranDate,
		InvoiceDate.calendar_date AS ShipDate,
		TransactionItem.MMSTransactionID,
		'' AS MMSPackageID,
		CustomerMember.member_id AS MemberID,
		
		CASE WHEN TransactionItem.refund_flag = 'N'
		     THEN (CASE WHEN TransactionBucksAmount > DiscountedItemAmount
			            THEN DiscountedItemAmount
						ELSE TransactionBucksAmount
						END )
			 ELSE 0
			 END Capture_LTBUCKS,
		CASE WHEN TransactionItem.payment_type = 'AE' and TransactionItem.refund_flag = 'N'
		     THEN (CASE WHEN TransactionBucksAmount > DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END )
			 ELSE 0
			 END Capture_amex,
		CASE WHEN TransactionItem.payment_type = 'DS' and TransactionItem.refund_flag = 'N'
		     THEN (CASE WHEN TransactionBucksAmount > DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END )
			 ELSE 0
			 END Capture_discover,
		CASE WHEN TransactionItem.payment_type = 'MC' and TransactionItem.refund_flag = 'N'
		     THEN (CASE WHEN TransactionBucksAmount > DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END  )
			 ELSE 0
			 END Capture_master,
		CASE WHEN TransactionItem.payment_type = 'VI' and TransactionItem.refund_flag = 'N' 
		     THEN (CASE WHEN TransactionBucksAmount > DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END  )
			 ELSE 0
			 END Capture_visa,
		CASE WHEN TransactionItem.payment_type = 'paypal' and TransactionItem.refund_flag = 'N' 
		     THEN (CASE WHEN TransactionBucksAmount > DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END  )
			 ELSE 0
			 END Capture_paypal,
		CASE WHEN TransactionItem.refund_flag = 'Y'
		     THEN (CASE WHEN TransactionBucksAmount < DiscountedItemAmount
			            THEN DiscountedItemAmount
						ELSE TransactionBucksAmount
						END )
			 ELSE 0
			 END Refund_LTBUCKS,
		CASE WHEN TransactionItem.payment_type = 'AE' and TransactionItem.refund_flag = 'Y'AND DiscountedItemAmount < 0
		     THEN (CASE WHEN TransactionBucksAmount < DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END )
			 ELSE 0
			 END Refund_amex,
		CASE WHEN TransactionItem.payment_type = 'DS' and TransactionItem.refund_flag = 'Y'AND DiscountedItemAmount< 0
		     THEN (CASE WHEN TransactionBucksAmount < DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END )
			 ELSE 0
			 END Refund_discover,
		CASE WHEN TransactionItem.payment_type = 'MC' and TransactionItem.refund_flag = 'Y'AND DiscountedItemAmount < 0
		     THEN (CASE WHEN TransactionBucksAmount < DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END )
			 ELSE 0
			 END Refund_master,
		CASE WHEN TransactionItem.payment_type = 'VI' and TransactionItem.refund_flag = 'Y'AND DiscountedItemAmount < 0
		     THEN (CASE WHEN TransactionBucksAmount < DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END )
			 ELSE 0
			 END Refund_visa,
		CASE WHEN TransactionItem.payment_type = 'paypal' and TransactionItem.refund_flag = 'Y' AND DiscountedItemAmount < 0
		     THEN (CASE WHEN TransactionBucksAmount < DiscountedItemAmount
			            THEN Tax + OrderShippingAmount
						ELSE DiscountedItemAmount - TransactionBucksAmount + Tax + OrderShippingAmount
						END )
			 ELSE 0
			 END Refund_paypal,
		@ReportRunDateTime AS ReportRunDateTime,
		@StartDate AS ReportStartDate,
	    DateAdd(day,-1,@AdjustedEndDate) AS ReportEndDate,
		TransactionItem.TransactionAmount,
	    BatchNumber,
        PaymenTechCreditTranID

FROM #TransactionTotals TransactionItem
LEFT JOIN [marketing].[v_dim_club] RevenueClub
  ON TransactionItem.allocated_dim_club_key = RevenueClub.dim_club_key
LEFT JOIN [marketing].[v_dim_magento_product] Product
  ON TransactionItem.dim_magento_product_key = Product.dim_magento_product_key
LEFT JOIN #ProductSubCategory SubCategory
  ON Product.dim_magento_product_key = SubCategory.dim_magento_product_key
LEFT JOIN [marketing].[v_dim_mms_product] MMSProduct
  ON Product.dim_mms_product_key = MMSProduct.dim_mms_product_key
LEFT JOIN [marketing].[v_dim_date] OrderDate
  ON TransactionItem.order_dim_date_key = OrderDate.dim_date_key
LEFT JOIN [marketing].[v_dim_date] InvoiceDate
  ON TransactionItem.invoice_dim_date_key = InvoiceDate.dim_date_key
LEFT JOIN [marketing].[v_dim_mms_member] CustomerMember
  ON TransactionItem.dim_mms_member_key = CustomerMember.dim_mms_member_key
LEFT JOIN [marketing].[v_dim_date] RefundDate
  ON TransactionItem.refund_dim_date_key = RefundDate.dim_date_key



DROP TABLE #ProductSubCategoryCalc
DROP TABLE #ProductSubCategory
DROP TABLE #TransactionTotals


END
