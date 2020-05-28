CREATE PROC [reporting].[proc_MagentoSalesDetail] @StartDate [DateTime],@EndDate [DateTime] AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
    END

----- Sample Execution
--Exec [reporting].[proc_MagentoSalesDetail]'12/1/2019','12/8/2019'


----Declare Date Variables

DECLARE @ReportRunDateTime DATETIME
DECLARE @ReportRunDateMinus1Month DATETIME
DECLARE @ReportRunDate DATETIME
	SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)
						 +', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)
						 +' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ----- UDW in UTC time
SET @ReportRunDateMinus1Month = CAST(DATEADD(month,-1,@ReportRunDateTime) AS Date)
SET @ReportRunDate = Cast(@ReportRunDateTime as Date)



DECLARE @EndDatePlus1 DATETIME


SET @StartDate = CASE WHEN @StartDate = '1/1/1900' 
                      THEN (SELECT month_starting_date FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportRunDate)  ----- Current month scheduled job - real time
					  WHEN @StartDate = '1/2/1900'
					  THEN (SELECT month_starting_date FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportRunDateMinus1Month)  ------ full prior month scheduled job
					  ELSE @StartDate
					  END
SET @EndDate = CASE WHEN @EndDate = '1/1/1900'     ----- Current month scheduled job
                      THEN DateAdd(day,-6,@ReportRunDate)    ------ most recent 5 days plus "today" will be returned by Magento database
					  WHEN @EndDate = '1/2/1900'    ------ full prior month run on 1st of month
					  THEN (SELECT month_ending_date FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportRunDateMinus1Month)    ------- first of current month (orders < this date)
					  ELSE @EndDate
					  END

SET @EndDatePlus1 = DateAdd(day,1,@EndDate)

IF OBJECT_ID('tempdb.dbo.#SubcategoryPrelim', 'U') IS NOT NULL
  DROP TABLE #SubcategoryPrelim; 

---- Find all product subcategories

SELECT catalog_category_flat_store_id,
       path,
	   name AS SubCategoryName,
	   Substring(path,5,100) AS CategoryPath
 INTO #SubcategoryPrelim   
FROM [marketing].[v_dim_magento_product_category]
WHERE level = 3
GROUP BY  catalog_category_flat_store_id,
       path,
	   name


IF OBJECT_ID('tempdb.dbo.#SubcategoryPrelim2', 'U') IS NOT NULL
 DROP TABLE #SubcategoryPrelim2; 

 ---- isolate the parent category story ID from the subcategory's path

SELECT catalog_category_flat_store_id AS SubcategoryFlatStoreID,
       path AS SubcategoryPath,
	   SubCategoryName,
	   value AS ParentCategoryFlatStoreID
 INTO #SubcategoryPrelim2    
FROM #SubcategoryPrelim
 CROSS APPLY STRING_SPLIT(CategoryPath,'/')


IF OBJECT_ID('tempdb.dbo.#Subcategory', 'U') IS NOT NULL
  DROP TABLE #Subcategory; 
--- Remove the parent category records

SELECT SubcategoryFlatStoreID,
 SubcategoryPath,
 SubCategoryName,
 ParentCategoryFlatStoreID
INTO #Subcategory   
 FROM #SubcategoryPrelim2
WHERE SubcategoryFlatStoreID <> ParentCategoryFlatStoreID

IF OBJECT_ID('tempdb.dbo.#Category', 'U') IS NOT NULL
  DROP TABLE #Category; 
--- find just the parent categories

SELECT catalog_category_flat_store_id AS CategoryFlatStoreID,
       path AS CategoryPath,
	   name AS CategoryName
INTO #Category   
FROM [marketing].[v_dim_magento_product_category] Category
WHERE Category.level = 2
GROUP BY catalog_category_flat_store_id,
       path,
	   name


IF OBJECT_ID('tempdb.dbo.#CategorySubCategoryPaths', 'U') IS NOT NULL
  DROP TABLE #CategorySubCategoryPaths; 

---- the full list of all categories and their subcategories

SELECT Subcategory.SubcategoryFlatStoreID,
       Subcategory.SubcategoryPath,
	   Subcategory.SubCategoryName,
	   Category.CategoryName AS CategoryName   
INTO #CategorySubCategoryPaths
FROM #Subcategory  Subcategory
   FULL OUTER JOIN #Category Category
     ON Subcategory.ParentCategoryFlatStoreID = Category.CategoryFlatStoreID
GROUP BY Subcategory.SubcategoryFlatStoreID,
       Subcategory.SubcategoryPath,
	   Subcategory.SubCategoryName,
	   Category.CategoryName

IF OBJECT_ID('tempdb.dbo.#MagentoProductsAndCategories_Prelim', 'U') IS NOT NULL
  DROP TABLE #MagentoProductsAndCategories_Prelim; 

 ---- create a pipe delimited string of subcategories for each category for each product

SELECT Product.dim_magento_product_key,
       Product.sku,
	   Product.product_name,
	   Product.free_shipping,
	   Categories.CategoryName AS CategoryName,
	   STRING_AGG(Categories.SubCategoryName, '|')
	        WITHIN GROUP(ORDER BY Product.dim_magento_product_key,Categories.CategoryName,Categories.SubCategoryName) AS SubCategoryList
INTO #MagentoProductsAndCategories_Prelim 
FROM [marketing].[v_dim_magento_product] Product
  LEFT JOIN [marketing].[v_dim_magento_product_category] Category   
    ON Product.dim_magento_product_key = Category.dim_magento_product_key
	AND Category.level <> 2
  LEFT JOIN #CategorySubCategoryPaths Categories
    ON Category.path = Categories.SubcategoryPath   
GROUP BY Product.dim_magento_product_key,
       Product.sku,
	   Product.product_name,
	   Product.free_shipping,
	   Categories.CategoryName

IF OBJECT_ID('tempdb.dbo.#ProductAndCategoryNameLists', 'U') IS NOT NULL
  DROP TABLE #ProductAndCategoryNameLists; 

---- found that some products mapped to multiple parent categories, so this puts them in a pipe delimited string
---- this creates 1 record per product

SELECT dim_magento_product_key,     
       sku,
	   product_name,
	   free_shipping,
	   STRING_AGG(CategoryName, '|')
	        WITHIN GROUP(ORDER BY dim_magento_product_key) AS CategoryNameList
INTO #ProductAndCategoryNameLists
FROM #MagentoProductsAndCategories_Prelim
GROUP BY dim_magento_product_key,
       sku,
	   product_name,
	   free_shipping

IF OBJECT_ID('tempdb.dbo.#MagentoProductsAndCategories', 'U') IS NOT NULL
  DROP TABLE #MagentoProductsAndCategories; 

----- then created a final pipe delimited string of subcategories
----- ending in just one record per product

SELECT Prelim.dim_magento_product_key,     
       Prelim.sku,
	   Prelim.product_name,
	   Prelim.free_shipping,
	   Categories.CategoryNameList,
	   STRING_AGG(Prelim.SubCategoryList, '|')
	        WITHIN GROUP(ORDER BY Prelim.dim_magento_product_key,Categories.CategoryNameList) AS SubCategoryList
INTO #MagentoProductsAndCategories                    
FROM #MagentoProductsAndCategories_Prelim Prelim
 JOIN #ProductAndCategoryNameLists Categories
   ON Prelim.dim_magento_product_key = Categories.dim_magento_product_key
GROUP BY Prelim.dim_magento_product_key,     
       Prelim.sku,
	   Prelim.product_name,
	   Prelim.free_shipping,
	   Categories.CategoryNameList




IF OBJECT_ID('tempdb.dbo.#MagentoTransInPeriod', 'U') IS NOT NULL
 DROP TABLE #MagentoTransInPeriod;

----- returned all Magento trans in period excluding the 5 days prior to the query run date
----- These 5 prior day's transactions will come from the Magento real time view 
----- if these dates are within the report date range

 SELECT TranItem.order_number,
TranItem.order_id,
TranItem.order_item_id,
TranItem.order_datetime,
DATEADD(hh,(CASE WHEN TranItem.order_datetime >= '11/4/2018 07:00:000' AND TranItem.order_datetime < '3/10/2019 08:00:000' 
				THEN -6 
		      WHEN TranItem.order_datetime >= '3/10/2019 08:00:000' AND TranItem.order_datetime < '11/3/2019 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/3/2019 07:00:000' AND TranItem.order_datetime < '3/8/2020 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/8/2020 08:00:000' AND TranItem.order_datetime < '11/1/2020 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/1/2020 07:00:000' AND TranItem.order_datetime < '3/14/2021 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/14/2021 08:00:000' AND TranItem.order_datetime < '11/7/2021 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/7/2021 07:00:000' AND TranItem.order_datetime < '3/13/2022 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/13/2022 08:00:000' AND TranItem.order_datetime < '11/6/2022 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/6/2022 07:00:000' AND TranItem.order_datetime < '3/12/2023 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/12/2023 08:00:000' AND TranItem.order_datetime < '11/5/2023 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/5/2023 07:00:000' AND TranItem.order_datetime < '3/10/2024 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/10/2024 08:00:000' AND TranItem.order_datetime < '11/3/2024 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/3/2024 07:00:000' AND TranItem.order_datetime < '3/9/2025 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/9/2025 08:00:000' AND TranItem.order_datetime < '11/2/2025 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/2/2025 07:00:000' AND TranItem.order_datetime < '3/8/2026 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/8/2026 08:00:000' AND TranItem.order_datetime < '11/1/2026 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/1/2026 07:00:000' AND TranItem.order_datetime < '3/14/2027 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/14/2027 08:00:000' AND TranItem.order_datetime < '11/7/2027 07:00:000' 
			    THEN -5 ELSE -5 END),TranItem.order_datetime) AS order_datetime_central_time,
TranItem.dim_magento_product_key,
TranItem.transaction_quantity,
TranItem.order_status,
TranItem.dim_club_key AS SelectedClubDimClubKey,
TranItem.allocated_dim_club_key,
TranItem.product_cost,
TranItem.product_price,
TranItem.transaction_item_amount,
TranItem.transaction_discount_amount,
TranItem.transaction_bucks_amount,
TranItem.transaction_tax_amount,
TranItem.shipping_amount,
TranItem.shipping_tax_amount,
TranItem.payment_type,
TranItem.refund_datetime,
TranItem.refund_dim_date_key,
TranItem.employee_id,
TranItem.shipping_state,
TranItem.dim_magento_customer_key,
TranItem.dim_mms_member_key,
TranItem.credit_tran_id,
TranItem.refund_flag,
TranItem.shipment_datetime,
TranItem.shipment_dim_date_key
INTO #MagentoTransInPeriod  
 FROM [marketing].[v_fact_magento_tran_item] TranItem
 WHERE DATEADD(hh,(CASE WHEN TranItem.order_datetime >= '11/4/2018 07:00:000' AND TranItem.order_datetime < '3/10/2019 08:00:000' 
				THEN -6 
		      WHEN TranItem.order_datetime >= '3/10/2019 08:00:000' AND TranItem.order_datetime < '11/3/2019 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/3/2019 07:00:000' AND TranItem.order_datetime < '3/8/2020 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/8/2020 08:00:000' AND TranItem.order_datetime < '11/1/2020 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/1/2020 07:00:000' AND TranItem.order_datetime < '3/14/2021 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/14/2021 08:00:000' AND TranItem.order_datetime < '11/7/2021 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/7/2021 07:00:000' AND TranItem.order_datetime < '3/13/2022 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/13/2022 08:00:000' AND TranItem.order_datetime < '11/6/2022 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/6/2022 07:00:000' AND TranItem.order_datetime < '3/12/2023 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/12/2023 08:00:000' AND TranItem.order_datetime < '11/5/2023 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/5/2023 07:00:000' AND TranItem.order_datetime < '3/10/2024 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/10/2024 08:00:000' AND TranItem.order_datetime < '11/3/2024 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/3/2024 07:00:000' AND TranItem.order_datetime < '3/9/2025 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/9/2025 08:00:000' AND TranItem.order_datetime < '11/2/2025 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/2/2025 07:00:000' AND TranItem.order_datetime < '3/8/2026 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/8/2026 08:00:000' AND TranItem.order_datetime < '11/1/2026 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/1/2026 07:00:000' AND TranItem.order_datetime < '3/14/2027 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/14/2027 08:00:000' AND TranItem.order_datetime < '11/7/2027 07:00:000' 
			    THEN -5 ELSE -5 END),TranItem.order_datetime) >= @StartDate
	AND  
	DATEADD(hh,(CASE WHEN TranItem.order_datetime >= '11/4/2018 07:00:000' AND TranItem.order_datetime < '3/10/2019 08:00:000' 
				THEN -6 
		      WHEN TranItem.order_datetime >= '3/10/2019 08:00:000' AND TranItem.order_datetime < '11/3/2019 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/3/2019 07:00:000' AND TranItem.order_datetime < '3/8/2020 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/8/2020 08:00:000' AND TranItem.order_datetime < '11/1/2020 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/1/2020 07:00:000' AND TranItem.order_datetime < '3/14/2021 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/14/2021 08:00:000' AND TranItem.order_datetime < '11/7/2021 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/7/2021 07:00:000' AND TranItem.order_datetime < '3/13/2022 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/13/2022 08:00:000' AND TranItem.order_datetime < '11/6/2022 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/6/2022 07:00:000' AND TranItem.order_datetime < '3/12/2023 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/12/2023 08:00:000' AND TranItem.order_datetime < '11/5/2023 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/5/2023 07:00:000' AND TranItem.order_datetime < '3/10/2024 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/10/2024 08:00:000' AND TranItem.order_datetime < '11/3/2024 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/3/2024 07:00:000' AND TranItem.order_datetime < '3/9/2025 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/9/2025 08:00:000' AND TranItem.order_datetime < '11/2/2025 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/2/2025 07:00:000' AND TranItem.order_datetime < '3/8/2026 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/8/2026 08:00:000' AND TranItem.order_datetime < '11/1/2026 07:00:000' 
			    THEN -5 
			  WHEN TranItem.order_datetime >= '11/1/2026 07:00:000' AND TranItem.order_datetime < '3/14/2027 08:00:000' 
			    THEN -6 
			  WHEN TranItem.order_datetime >= '3/14/2027 08:00:000' AND TranItem.order_datetime < '11/7/2027 07:00:000' 
			    THEN -5 ELSE -5 END),TranItem.order_datetime) < @EndDatePlus1

IF OBJECT_ID('tempdb.dbo.#OrderCapturedAmount', 'U') IS NOT NULL
 DROP TABLE #OrderCapturedAmount;

 ----- for the transactions in the period, payment captured amounts

SELECT Trans.order_number,
       MIN(Trans.order_item_id) AS order_item_id,
	   Trans.credit_tran_id,
	   Pmts.transaction_amount AS CapturedAmount
INTO #OrderCapturedAmount  
FROM #MagentoTransInPeriod Trans
JOIN [marketing].[v_fact_mms_pt_credit_card_transaction] Pmts
  ON Trans.credit_tran_id = Pmts.pt_credit_card_transaction_id
  AND Pmts.voided_flag = 'N'
GROUP BY Trans.order_number,
	   Trans.credit_tran_id,
	   Pmts.transaction_amount


IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
 DROP TABLE #Results;

----- bringing it all together

SELECT
TranItem.order_number,
TranItem.order_id,
TranItem.order_item_id,
TranItem.order_datetime,
OrderDate.four_digit_year_dash_two_digit_month AS OrderMonth_CentralTime,
TranItem.order_datetime_central_time,
TranItem.shipment_datetime AS ShipDateTime,
TranItem.dim_magento_product_key,
Product.CategoryNameList,
Product.SubCategoryList,
NULL AS Manufacturer,
Product.sku,
Product.product_name,
TranItem.transaction_quantity,
NULL AS ItemsInvoiced,
NULL AS ItemsShipped,
TranItem.order_status,
NULL AS ItemStatus,
NULL AS IsAutoship,
CASE WHEN TranItem.SelectedClubDimClubKey = '-998'
     THEN NULL
	 ELSE SelectedClub.club_id
	 END SelectedClub,
NULL AS FulfillmentPartner,
TranItem.allocated_dim_club_key,
TranItem.product_cost,
TranItem.product_price,
TranItem.transaction_discount_amount,
((TranItem.product_price * ABS(TranItem.transaction_quantity)) - IsNull(TranItem.transaction_discount_amount,0)) AS ItemSubTotal,
TranItem.transaction_bucks_amount,
((TranItem.product_price * ABS(TranItem.transaction_quantity)) - IsNull(TranItem.transaction_discount_amount,0)- IsNull(TranItem.transaction_bucks_amount,0)) AS ItemSubTotalLessLTBucks,
TranItem.transaction_tax_amount,
TranItem.shipping_amount,
TranItem.shipping_tax_amount,
Product.free_shipping,
TranItem.transaction_item_amount,
NULL AS VoucherCode,
TranItem.payment_type,
IsNull(CapturedAmount.CapturedAmount,0) AS CapturedAmount,
TranItem.refund_datetime,
TranItem.refund_dim_date_key,
CASE WHEN TranItem.refund_dim_date_key = '-998'
     THEN NULL
	 ELSE RefundDate.four_digit_year_dash_two_digit_month 
	 END  RefundMonth,
TranItem.employee_id,
TranItem.shipping_state,
TranItem.dim_magento_customer_key,
CASE WHEN Member.dim_mms_member_key is NULL
     THEN Customer.first_name +' '+ Customer.last_name 
	 ELSE Member.customer_name 
	 END  CustomerName,
CASE WHEN Member.dim_mms_member_key is NULL
     THEN Customer.email
	 ELSE Member.email_address
	 END CustomerEmail,
CASE WHEN Member.dim_mms_member_key is NULL
     THEN Customer.mms_party_id
	 ELSE Member.party_id
	 END CustomerPartyID,
Member.member_id AS CustomerMemberID,
Employee.employee_id AS CustomerEmployeeID,
CustomerHomeClub.club_name AS CustomerHomeClub,
@StartDate AS ReportStartDate,
@EndDate AS ReportEndDate,
ShipDate.four_digit_year_dash_two_digit_month AS ShipMonth
FROM #MagentoTransInPeriod TranItem
  JOIN [marketing].[v_dim_date] OrderDate
    ON Cast(TranItem.order_datetime_central_time AS Date) = OrderDate.calendar_date
  LEFT JOIN [marketing].[v_dim_date] ShipDate
    ON Cast(TranItem.shipment_datetime AS Date) = ShipDate.calendar_date
  LEFT JOIN #MagentoProductsAndCategories Product
    ON TranItem.dim_magento_product_key = Product.dim_magento_product_key
  LEFT JOIN [marketing].[v_dim_magento_customer] Customer
    ON TranItem.dim_magento_customer_key = Customer.dim_magento_customer_key
  LEFT JOIN [marketing].[v_dim_mms_member] Member
    ON TranItem.dim_mms_member_key = Member.dim_mms_member_key
  LEFT JOIN [marketing].[v_dim_employee] Employee
    ON Member.member_id = Employee.member_id
  LEFT JOIN [marketing].[v_dim_mms_membership] Membership
    ON Member.dim_mms_membership_key = Membership.dim_mms_membership_key
  LEFT JOIN [marketing].[v_dim_club] CustomerHomeClub
    ON Membership.home_dim_club_key = CustomerHomeClub.dim_club_key
  LEFT JOIN [marketing].[v_dim_date] RefundDate
    ON TranItem.refund_dim_date_key = RefundDate.dim_date_key
  LEFT JOIN [marketing].[v_dim_club] SelectedClub
    ON TranItem.SelectedClubDimClubKey = SelectedClub.dim_club_key
  LEFT JOIN #OrderCapturedAmount CapturedAmount
    ON TranItem.order_number = CapturedAmount.order_number
	AND TranItem.order_item_id = CapturedAmount.order_item_id
	AND TranItem.refund_flag = 'N'

DROP TABLE #SubcategoryPrelim
DROP TABLE #SubcategoryPrelim2
DROP TABLE #Subcategory
DROP TABLE #Category
DROP TABLE #CategorySubCategoryPaths
DROP TABLE #MagentoProductsAndCategories_Prelim
DROP TABLE #ProductAndCategoryNameLists 
DROP TABLE #MagentoProductsAndCategories
DROP TABLE #MagentoTransInPeriod 
DROP TABLE #OrderCapturedAmount

END
