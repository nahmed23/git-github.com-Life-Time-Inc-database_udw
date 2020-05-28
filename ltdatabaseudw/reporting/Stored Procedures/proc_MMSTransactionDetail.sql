CREATE PROC [reporting].[proc_MMSTransactionDetail] @StartFourDigitYearDashTwoDigitMonth [CHAR](7),@MMSDepartmentList [VARCHAR](4000),@MMSClubIDList [VARCHAR](4000),@GLAccountNumberList [VARCHAR](8000),@TransactionTypeList [VARCHAR](8000) AS

BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

----- Sample Execution
 ---   Exec [reporting].[proc_MMSTransactionDetail] '2019-09','Member Dues/Fees','151','4000','Refund'

DECLARE @ReportRunDatetime VARCHAR(21)
SET @ReportRunDateTime = (SELECT REPLACE(SUBSTRING(CONVERT(VARCHAR,DATEADD(HH,-1 * OFFSET, GETDATE()), 100),1,6) +', '+ SUBSTRING(CONVERT(VARCHAR,DATEADD(HH, -1 * OFFSET, GETDATE()), 100),8,10) +' '+SUBSTRING(CONVERT(VARCHAR,DATEADD(HH,-1 * OFFSET, GETDATE()), 100),18,2),' ',' ')GET_DATE_VARCHAR
	FROM MAP_UTC_TIME_ZONE_CONVERSION
	WHERE GETDATE() BETWEEN UTC_Start_Date_Time and UTC_End_Date_Time and Description = 'central time')


DECLARE @StartDimDateKey INT,
        @EndDimDateKey INT
SELECT @StartDimDateKey = MIN(Month_Starting_Dim_Date_Key),
       @EndDimDateKey = MAX(Month_Ending_Dim_Date_Key)
  FROM [marketing].[v_dim_date]
 WHERE Four_Digit_Year_Dash_Two_Digit_Month = @StartFourDigitYearDashTwoDigitMonth

IF OBJECT_ID('tempdb.dbo.#MMSClubIDList', 'U') IS NOT NULL
  DROP TABLE #MMSClubIDList; 

DECLARE @list_table VARCHAR(100)
SET @list_table = 'ClubID_list'

EXEC marketing.proc_parse_pipe_list  @MMSClubIDList, @list_table

SELECT DISTINCT ClubID_list.Item MMSClubID,
       Club.dim_club_key AS DimClubKey,
	   Club.club_code AS ClubCode,
	   Club.local_currency_code AS LocalCurrencyCode,
	   MMSRegion.description AS MMSRegionName, 
	   Club.club_name AS ClubName,
	   Club.Workday_region AS WorkdayRegion
  INTO #MMSClubIDList  
  FROM #ClubID_list ClubID_list
   JOIN [marketing].[v_dim_club] Club
     ON ClubID_list.Item = Club.club_id
	  OR ClubID_list.Item = '-1'
   LEFT JOIN [marketing].[v_dim_description] MMSRegion
     ON Club.region_dim_description_key = MMSRegion.dim_description_key

IF OBJECT_ID('tempdb.dbo.#GLAccountNumberList', 'U') IS NOT NULL
	DROP TABLE #GLAccountNumberList;
SET @list_table = 'GLAccountNumber_List'
EXEC marketing.proc_parse_pipe_list @GLAccountNumberList, @list_table

SELECT GLAccountNumberList.Item GLAccountNumber
  INTO #GLAccountNumberList
  FROM #GLAccountNumber_List GLAccountNumberList   


--temp table for TransactionTypeLIst
IF OBJECT_ID('tempdb.dbo.#TransactionTypeList', 'U') IS NOT NULL
	DROP TABLE #TransactionTypeList
SET @list_table = 'TransactionType_List'
EXEC marketing.proc_parse_pipe_list @TransactionTypeList, @list_table

SELECT TransactionTypeList.Item TransactionType
  INTO #TransactionTypeList
  FROM #TransactionType_List TransactionTypeList 

IF OBJECT_ID('tempdb.dbo.#MMSDepartmentList', 'U') IS NOT NULL
	DROP TABLE #MMSDepartmentList
SET @list_table = 'MMSDeparment_List'
EXEC marketing.proc_parse_pipe_list @MMSDepartmentList, @list_table

SELECT MMSDepartmentList.Item MMSDepartment
  INTO #MMSDepartmentList
  FROM #MMSDeparment_List MMSDepartmentList 


IF OBJECT_ID('temdb.dbo.#Transactions', 'U') IS NOT NULL
	DROP TABLE #Transactions


SELECT 
	   #DimLocation.MMSRegionName,
       #DimLocation.ClubName,
       #DimLocation.ClubCode MMSClubCode,
       #DimLocation.LocalCurrencyCode CurrencyCode,
       CASE WHEN FactSalesTransaction.[membership_charge_flag] = 'Y' THEN 'Charge'
            WHEN FactSalesTransaction.[pos_flag] = 'Y' THEN 'Sale'
            WHEN FactSalesTransaction.[membership_adjustment_flag] = 'Y' THEN 'Adjustment'
            WHEN FactSalesTransaction.[refund_flag] = 'Y' THEN 'Refund' END TransactionType,
       DimEmployee.First_Name EmployeeFirstName,
       DimEmployee.Last_Name EmployeeLastName,
       DimCustomer.First_Name MemberFirstName,
       DimCustomer.Last_Name MemberLastName,
       DimCustomer.Member_ID AS MemberID,
       PostDimDate.Standard_Date_name PostDate,
       FactSalesTransaction.[mms_tran_id] MMSTransactionID,
--	   FactSalesTransaction.tran_item_id,
	   FactSalesTransaction.Sales_Dollar_Amount,
	   FactSalesTransaction.sales_tax_amount,
       FactSalesTransaction.Sales_Dollar_Amount * LocalCurrencyMonthlyAverageExchangeRate.Exchange_Rate LocalCurrencyItemAmount,
       FactSalesTransaction.Sales_Dollar_Amount * USDMonthlyAverageExchangeRate.Exchange_Rate USDItemAmount,
       FactSalesTransaction.Sales_Tax_Amount * LocalCurrencyMonthlyAverageExchangeRate.Exchange_Rate LocalCurrencyItemSalesTax,
       FactSalesTransaction.Sales_Tax_Amount * USDMonthlyAverageExchangeRate.Exchange_Rate USDItemSalesTax,
       FactSalesTransaction.Tran_Item_ID TransactionItemId,
       MemberRegion.description AS MembershipRegion,
	
       MembershipDimLocation.Club_Name MembershipClub,
       #DimLocation.MMSClubID ClubID,
       CASE WHEN DimProduct.Workday_Over_Ride_Region NOT IN ('','0') THEN DimProduct.Workday_Over_Ride_Region 
            ELSE #DimLocation.WorkdayRegion END WorkdayRegion,       
       #DimLocation.MMSRegionName TransactionRegionDescription,
       DimProduct.Deferred_Revenue_Flag As DeferredRevenueFlag,
       DimProduct.Product_Description As ProductDescription,
       DimMMSDrawerActivity.Drawer_Activity_ID As DrawerActivityID,
       DimProduct.[department_description] MMSDepartment,
       MembershipCreatedDimDate.[standard_date_name] /*StandardDateDescription */ MembershipCreatedDateTime,
       DimEmployee.Employee_ID EmployeeNumber,
       DimTransactionReason.Description TransactionReason,
       FactMembership.Membership_ID As MembershipID,
       DimProduct.GL_Account_Number GLAccountNumber,
       DimProduct.Workday_Account As WorkdayAccount,
       DimProduct.reporting_product_group_gl_account As RevenueProductGroupGLAccount,	
       DimProduct.[workday_revenue_product_group_account] RevenueProductGroupWorkdayAccount,
       LTRIM(RTRIM(DimProduct.[gl_department_code]))+'-'+LTRIM(RTRIM(DimProduct.[gl_product_code])) GLSubAccountNumber,  	       
       DimProduct.[workday_cost_center] AS WorkdayCostCenter,
       DimProduct.Workday_Offering AS WorkdayOffering,
       DimProduct.[discount_gl_account] DiscountGLAccount,       
       DimProduct.[workday_discount_gl_account] As WorkdayDiscountGLAccount,
       DimProduct.[revenue_product_group_discount_gl_account] AS RevenueProductGroupDiscountGLAccount,
       DimProduct.[workday_revenue_product_group_discount_gl_account] WorkdayRevenueProductGroupDiscountGLAccount,
       DimProduct.[refund_gl_account_number] ProductRefundGLAccount,
       DimProduct.[workday_refund_gl_account] WorkdayRefundGLAccount,
       DimProduct.[revenue_product_group_refund_gl_account] RevenueProductGroupRefundGLAccount,
       DimProduct.[workday_revenue_product_group_refund_gl_account] WorkdayRevenueProductGroupRefundGLAccount,       
       FactSalesTransaction.Sales_Discount_Dollar_Amount * LocalCurrencyMonthlyAverageExchangeRate.Exchange_Rate LocalCurrencyTotalDiscountAmount,
       FactSalesTransaction.Sales_Discount_Dollar_Amount * USDMonthlyAverageExchangeRate.Exchange_Rate USDTotalDiscountAmount,
       SIGN(FactSalesTransaction.Sales_Quantity) * FactSalesTransaction.Sales_Amount_Gross * LocalCurrencyMonthlyAverageExchangeRate.Exchange_Rate LocalCurrencyGrossTransactionAmount,
       SIGN(FactSalesTransaction.Sales_Quantity) * FactSalesTransaction.Sales_Amount_Gross * USDMonthlyAverageExchangeRate.Exchange_Rate USDGrossTransactionAmount,
       SalesChannelDimDescription.Description SalesChannel,
       USDMonthlyAverageExchangeRate.Exchange_Rate ToUSDMonthlyAverageExchangeRate,
       LocalCurrencyMonthlyAverageExchangeRate.Exchange_Rate ToLocalCurrencyMonthlyAverageExchangeRate,
       DrawerClosedDimDate.Standard_date_name +' '+ DrawerCloseDimTime.[display_12_hour_time] DrawerClosedDate,

	   0 USDCorporateTransferAmount,   --hardcoding since we are not using corporatetransferamount 
	   0 LocalCurrencyCorporateTransferAmount, --hardcoding since we are not using corporatetransferamount. 
       CASE WHEN FactSalesTransaction.Refund_Flag = 'Y'
                 THEN DimProduct.[refund_gl_account_number]
            ELSE NULL
       END RefundGLAccount,
       FactSalesTransaction.Voided_Flag  VoidedFlag

  INTO #Transactions
  FROM [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
  JOIN #MMSClubIDList #DimLocation 
    ON FactSalesTransaction.Transaction_Reporting_Dim_Club_Key = #DimLocation.DimClubkey
  JOIN [marketing].[v_dim_employee] DimEmployee 
    ON FactSalesTransaction.sales_entered_dim_employee_key = DimEmployee.Dim_Employee_Key
  JOIN [marketing].[v_dim_mms_member] DimCustomer 
    ON FactSalesTransaction.Dim_MMS_Member_Key = DimCustomer.Dim_MMS_Member_key
  JOIN [marketing].[v_dim_date] PostDimDate 
    ON FactSalesTransaction.Post_Dim_Date_Key = PostDimDate.Dim_Date_Key
	
  JOIN [marketing].[v_dim_mms_membership] FactMembership 
    ON DimCustomer.Membership_ID = FactMembership.Membership_ID
	
  JOIN [marketing].[v_dim_club] MembershipDimLocation 
    ON FactMembership.[home_dim_club_key] /*DimLocationKey*/ = MembershipDimLocation.Dim_Club_Key   
  JOIN [marketing].[v_dim_mms_product] DimProduct --vDimProductActive DimProduct
    ON FactSalesTransaction.[dim_mms_product_key] = DimProduct.[dim_mms_product_key] 


  JOIN #GLAccountNumberList
    ON DimProduct.[gl_account_number] = #GLAccountNumberList.GLAccountNumber

  JOIN #MMSDepartmentList    
    ON DimProduct.[department_description] /*MMSDepartmentDescription*/ = #MMSDepartmentList.MMSDepartment --Closest correspondince to DepartmentDescription

  JOIN [marketing].[v_dim_mms_drawer_activity] DimMMSDrawerActivity
    ON FactSalesTransaction.[dim_mms_drawer_activity_key] = DimMMSDrawerActivity.[dim_mms_drawer_activity_key]
  JOIN [marketing].[v_dim_date] DrawerClosedDimDate
    ON DimMMSDrawerActivity.[closed_dim_date_key] = DrawerClosedDimDate.[dim_date_key]
  JOIN [marketing].[v_dim_time] DrawerCloseDimTime
    ON DimMMSDrawerActivity.[closed_dim_time_key] = DrawerCloseDimTime.[dim_time_key] 
  JOIN [marketing].[v_dim_date] MembershipCreatedDimDate
    ON FactMembership.[created_date_time_key] = MembershipCreatedDimDate.Dim_Date_Key
  JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason
    ON FactSalesTransaction.[dim_mms_transaction_reason_key] = DimTransactionReason.[dim_mms_transaction_reason_key]
  JOIN [marketing].[v_dim_description] SalesChannelDimDescription
    ON FactSalesTransaction.[sales_channel_dim_description_key] = SalesChannelDimDescription.Dim_Description_Key
  JOIN [marketing].[v_dim_exchange_rate] USDMonthlyAverageExchangeRate
    ON FactSalesTransaction.[usd_monthly_average_dim_exchange_rate_key] = USDMonthlyAverageExchangeRate.[dim_exchange_rate_key]
  JOIN [marketing].[v_dim_exchange_rate] LocalCurrencyMonthlyAverageExchangeRate
    ON FactSalesTransaction.[transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key] = LocalCurrencyMonthlyAverageExchangeRate.[dim_exchange_rate_key]

 JOIN [marketing].[v_dim_description] MemberRegion
	ON MemberRegion.dim_description_key = MembershipDimLocation.region_dim_description_key

 WHERE FactSalesTransaction.[post_dim_date_key] >= @StartDimDateKey
   AND FactSalesTransaction.[post_dim_date_key] <= @EndDimDateKey
   AND (('Adjustment' IN (SELECT TransactionType FROM #TransactionTypeList) AND FactSalesTransaction.[membership_adjustment_flag] = 'Y')
        OR ('Charge' IN (SELECT TransactionType FROM #TransactionTypeList) AND FactSalesTransaction.[membership_charge_flag] = 'Y')
        OR ('Refund' IN (SELECT TransactionType FROM #TransactionTypeList) AND FactSalesTransaction.[refund_flag] = 'Y')
        OR ('Sale' IN (SELECT TransactionType FROM #TransactionTypeList) AND FactSalesTransaction.[pos_flag] = 'Y' ))


IF OBJECT_ID('tempdb.dbo.#FactMMSPayment', 'U') IS NOT NULL 
	DROP Table #FactMMSPayment

SELECT DISTINCT FactMMSPayment.MMS_Tran_ID AS MMSTranID,
       FactMMSPayment.Payment_Type_Dim_Description_Key AS PaymentTypeDimDescriptionKey,
       FactMMSPayment.Payment_Amount AS PaymentAmount,
       #Transactions.ToUSDMonthlyAverageExchangeRate,
       #Transactions.ToLocalCurrencyMonthlyAverageExchangeRate
  INTO #FactMMSPayment
 FROM #Transactions
  JOIN [marketing].[v_fact_mms_payment] FactMMSPayment
    ON #Transactions.MMSTransactionID = FactMMSPayment.mms_tran_id
	


--Temp Table to resolve the FOR XML Path using Azure's String Aggreg 
IF OBJECT_ID('tempdb.dbo.#MMSPaymentTypes', 'U') IS NOT NULL
	DROP Table #MMSPaymentTypes

CREATE TABLE #MMSPaymentTypes (MMSTranID INT, USDPaymentTypes VARCHAR(4000), LocalCurrencyPaymentTypes VARCHAR(4000))

INSERT INTO #MMSPaymentTypes
SELECT MMSTranID,
	(SELECT 
			STRING_AGG(((DimDescription.Description) + ' ' + Convert(Varchar,Cast(InnerFactMMSPayment.PaymentAmount * InnerFactMMSPayment.ToUSDMonthlyAverageExchangeRate as Decimal(12,2)))), ', ')
							WITHIN GROUP (ORDER BY MMSTranID) 
				FROM #FactMMSPayment InnerFactMMSPayment 
				JOIN [marketing].[v_dim_description] DimDescription 
				 ON  InnerFactMMSPayment.PaymentTypeDimDescriptionKey = DimDescription.Dim_Description_key
				WHERE OuterFactMMSPayment.MMSTranID = InnerFactMMSPayment.MMSTranID) AS USDPaymentTypes,
			
	(SELECT
			STRING_AGG(((DimDescription.Description) + ' ' + Convert(Varchar,Cast(InnerFactMMSPayment.PaymentAmount * InnerFactMMSPayment.ToLocalCurrencyMonthlyAverageExchangeRate as Decimal(12,2)))), ', ')
							WITHIN GROUP(ORDER BY MMSTranID) 
				FROM #FactMMSPayment InnerFactMMSPayment 
				JOIN [marketing].[v_dim_description] DimDescription 
				 ON  InnerFactMMSPayment.PaymentTypeDimDescriptionKey = DimDescription.Dim_Description_key
				WHERE OuterFactMMSPayment.MMSTranID = InnerFactMMSPayment.MMSTranID) AS LocalCurrencyPaymentTypes
				

FROM #FactMMSPayment OuterFactMMSPayment
GROUP BY MMSTranID



--------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb.dbo.#MMSDiscounts', 'U') IS NOT NULL 
	DROP TABLE #MMSDiscounts;

SELECT DiscountRank.TranItemID,
       SUM(CASE WHEN DiscountRank.Ranking = 1 THEN DiscountRank.USDDiscountAmount ELSE 0 END) USDDiscountAmount1,
       SUM(CASE WHEN DiscountRank.Ranking = 2 THEN DiscountRank.USDDiscountAmount ELSE 0 END) USDDiscountAmount2,
       SUM(CASE WHEN DiscountRank.Ranking = 3 THEN DiscountRank.USDDiscountAmount ELSE 0 END) USDDiscountAmount3,
       SUM(CASE WHEN DiscountRank.Ranking = 4 THEN DiscountRank.USDDiscountAmount ELSE 0 END) USDDiscountAmount4,
       SUM(CASE WHEN DiscountRank.Ranking = 5 THEN DiscountRank.USDDiscountAmount ELSE 0 END) USDDiscountAmount5,
	   SUM(CASE WHEN DiscountRank.Ranking = 1 THEN DiscountRank.LocalCurrencyDiscountAmount ELSE 0 END) LocalCurrencyDiscountAmount1,
       SUM(CASE WHEN DiscountRank.Ranking = 2 THEN DiscountRank.LocalCurrencyDiscountAmount ELSE 0 END) LocalCurrencyDiscountAmount2,
       SUM(CASE WHEN DiscountRank.Ranking = 3 THEN DiscountRank.LocalCurrencyDiscountAmount ELSE 0 END) LocalCurrencyDiscountAmount3,
       SUM(CASE WHEN DiscountRank.Ranking = 4 THEN DiscountRank.LocalCurrencyDiscountAmount ELSE 0 END) LocalCurrencyDiscountAmount4,
       SUM(CASE WHEN DiscountRank.Ranking = 5 THEN DiscountRank.LocalCurrencyDiscountAmount ELSE 0 END) LocalCurrencyDiscountAmount5,

       CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 1 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) THEN NULL
            ELSE MAX(CASE WHEN DiscountRank.Ranking = 1 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) END Discount1,
       CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 2 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) THEN NULL
            ELSE MAX(CASE WHEN DiscountRank.Ranking = 2 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) END Discount2,
       CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 3 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) THEN NULL
            ELSE MAX(CASE WHEN DiscountRank.Ranking = 3 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) END Discount3,
       CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 4 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) THEN NULL
            ELSE MAX(CASE WHEN DiscountRank.Ranking = 4 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) END Discount4,
       CASE WHEN MAX(CASE WHEN DiscountRank.Ranking = 5 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) = Char(0) THEN NULL
            ELSE MAX(CASE WHEN DiscountRank.Ranking = 5 THEN DiscountRank.SalesPromotionReceiptText ELSE Char(0) END) END Discount5
  INTO #MMSDiscounts
  FROM (SELECT FactSalesTransactionDiscount.TranItemID, 
               RANK() OVER (PARTITION BY FactSalesTransactionDiscount.TranItemID 
                            ORDER BY FactSalesTransactionDiscount.FactSalesTransactionDiscountKey) Ranking,
               FactSalesTransactionDiscount.USDDiscountAmount,
               FactSalesTransactionDiscount.LocalCurrencyDiscountAmount,
               DimClubPOSPricingDiscount.sales_promotion_receipt_text AS SalesPromotionReceiptText
          FROM (SELECT MIN(#Transactions.TransactionItemId) TranItemID, 
                       MIN(FactSalesTransactionDiscount.Discount_Amount * #Transactions.ToUSDMonthlyAverageExchangeRate) USDDiscountAmount, 
                       MIN(FactSalesTransactionDiscount.Discount_Amount * #Transactions.ToLocalCurrencyMonthlyAverageExchangeRate) LocalCurrencyDiscountAmount, 
                       MIN(FactSalesTransactionDiscount.[dim_mms_pricing_discount_key]) DimClubPOSPricingDiscountKey, 
                       MIN(FactSalesTransactionDiscount.[fact_mms_sales_transaction_item_discount_key]) FactSalesTransactionDiscountKey
                  FROM #Transactions
                  JOIN [marketing].[v_fact_mms_transaction_item_discount] FactSalesTransactionDiscount
                    ON #Transactions.TransactionItemId = FactSalesTransactionDiscount.Tran_Item_ID
                 GROUP BY FactSalesTransactionDiscount.fact_mms_sales_transaction_item_discount_key) FactSalesTransactionDiscount
          JOIN [marketing].[v_dim_mms_pricing_discount] DimClubPOSPricingDiscount
            ON FactSalesTransactionDiscount.DimClubPOSPricingDiscountKey = DimClubPOSPricingDiscount.dim_mms_pricing_discount_key) DiscountRank             
 WHERE DiscountRank.Ranking <= 5
 GROUP BY DiscountRank.TranItemID

 
--- Temp table that contains the aggreg of each unique item transactions linked to one MMSTrnID- to resolve the vFactMMSTransaction
IF OBJECT_ID('tempdb.dbo.#TransactionAmount', 'U') IS NOT NULL
	DROP TABLE #TransactionAmount;

SELECT #Transactions.TransactionItemId,
SUM(TransactionItem.sales_dollar_amount + TransactionItem.sales_tax_amount)  AS TotalTransactionAmount  

 INTO #TransactionAmount

 FROM #Transactions 
 JOIN marketing.v_fact_mms_transaction_item TransactionItem
 ON #Transactions.MMSTransactionID = TransactionItem.mms_tran_id
 GROUP BY #Transactions.TransactionItemId

 -------------------------------------------------------------------------------------------------------------------
SELECT #Transactions.MMSRegionName,
       #Transactions.ClubName,
       #Transactions.MMSClubCode,
       #Transactions.CurrencyCode,
       #Transactions.ToUSDMonthlyAverageExchangeRate,
       #Transactions.ToLocalCurrencyMonthlyAverageExchangeRate,
       #Transactions.TransactionType,
       #Transactions.EmployeeFirstName,
       #Transactions.EmployeeLastName,
       #Transactions.MemberFirstName,
       #Transactions.MemberLastName,
       #Transactions.MemberID, 
	   FactMMSTransaction.TotalTransactionAmount * #Transactions.ToLocalCurrencyMonthlyAverageExchangeRate LocalCurrencyTransactionAmount,
	   FactMMSTransaction.TotalTransactionAmount  * #Transactions.ToUSDMonthlyAverageExchangeRate USDTransactionAmount,
       #Transactions.PostDate,
       #Transactions.MMSTransactionID,
       #Transactions.LocalCurrencyItemAmount,
       #Transactions.USDItemAmount,
       #Transactions.LocalCurrencyItemSalesTax,
       #Transactions.USDItemSalesTax,
	   
	   FactMMSTransaction.TotalTransactionAmount  * #Transactions.ToLocalCurrencyMonthlyAverageExchangeRate LocalCurrencyPOSAmount,
	   FactMMSTransaction.TotalTransactionAmount  * #Transactions.ToUSDMonthlyAverageExchangeRate USDPOSAmount,
       #Transactions.TransactionItemId,
       #Transactions.MembershipRegion,
       #Transactions.MembershipClub,
       #Transactions.ClubID,
       #Transactions.WorkdayRegion,
       #Transactions.TransactionRegionDescription,
       #Transactions.DeferredRevenueFlag,
       #Transactions.ProductDescription,
       #Transactions.DrawerActivityID,
       #Transactions.MMSDepartment,
       #Transactions.MembershipCreatedDateTime,
       #Transactions.EmployeeNumber,
       #Transactions.TransactionReason,
       #Transactions.MembershipID,
       #Transactions.GLAccountNumber,
       #Transactions.WorkdayAccount,
       #Transactions.RevenueProductGroupGLAccount,
       #Transactions.RevenueProductGroupWorkdayAccount,          
       #Transactions.GLSubAccountNumber,
       #Transactions.WorkdayCostCenter,
       #Transactions.WorkdayOffering,
       #Transactions.DiscountGLAccount,
       #Transactions.WorkdayDiscountGLAccount,
       #Transactions.RevenueProductGroupDiscountGLAccount,
       #Transactions.WorkdayRevenueProductGroupDiscountGLAccount,
       #Transactions.ProductRefundGLAccount,
       #Transactions.WorkdayRefundGLAccount,
       #Transactions.RevenueProductGroupRefundGLAccount,
       #Transactions.WorkdayRevenueProductGroupRefundGLAccount,        
       #Transactions.LocalCurrencyTotalDiscountAmount,
       #Transactions.USDTotalDiscountAmount,
       #Transactions.LocalCurrencyGrossTransactionAmount,
       #Transactions.USDGrossTransactionAmount,
       #MMSDiscounts.LocalCurrencyDiscountAmount1,
       #MMSDiscounts.USDDiscountAmount1,
       #MMSDiscounts.LocalCurrencyDiscountAmount2,
       #MMSDiscounts.USDDiscountAmount2,
       #MMSDiscounts.LocalCurrencyDiscountAmount3,
       #MMSDiscounts.USDDiscountAmount3,
       #MMSDiscounts.LocalCurrencyDiscountAmount4,
       #MMSDiscounts.USDDiscountAmount4,
       #MMSDiscounts.LocalCurrencyDiscountAmount5,
       #MMSDiscounts.USDDiscountAmount5,
       #MMSDiscounts.Discount1,
       #MMSDiscounts.Discount2,
       #MMSDiscounts.Discount3,
       #MMSDiscounts.Discount4,
       #MMSDiscounts.Discount5,
       #Transactions.SalesChannel,
       #MMSPaymentTypes.USDPaymentTypes,
       #MMSPaymentTypes.LocalCurrencyPaymentTypes,
       #Transactions.DrawerClosedDate,
       #Transactions.LocalCurrencyItemAmount + #Transactions.LocalCurrencyItemSalesTax LocalCurrencyItemAmountAfterTax,
       #Transactions.USDItemAmount + #Transactions.USDItemSalesTax USDItemAmountAfterTax,
       #Transactions.USDCorporateTransferAmount,
       #Transactions.LocalCurrencyCorporateTransferAmount,
       #Transactions.RefundGLAccount,
       #Transactions.VoidedFlag
  INTO  #Results
  FROM #Transactions
  LEFT JOIN #MMSPaymentTypes        
    ON #Transactions.MMSTransactionID = #MMSPaymentTypes.MMSTranID
  LEFT JOIN #MMSDiscounts           
    ON #Transactions.TransactionItemId = #MMSDiscounts.TranItemID
  JOIN #TransactionAmount FactMMSTransaction
	ON #Transactions.TransactionItemId = FactMMSTransaction.TransactionItemId



SELECT MMSRegionName,
       ClubName,
       MMSClubCode,
       CurrencyCode,
       ToUSDMonthlyAverageExchangeRate,
       ToLocalCurrencyMonthlyAverageExchangeRate,
       TransactionType,
       EmployeeFirstName,
       EmployeeLastName,
       MemberFirstName,
       MemberLastName,
       MemberID,
       LocalCurrencyTransactionAmount,
       USDTransactionAmount,
       PostDate,
       MMSTransactionID,
       LocalCurrencyItemAmount,
       USDItemAmount,
       LocalCurrencyItemSalesTax,
       USDItemSalesTax,
       LocalCurrencyPOSAmount,
       USDPOSAmount,
       TransactionItemId,
       MembershipRegion,
       MembershipClub,
       ClubID,
       WorkdayRegion,
       TransactionRegionDescription,
       DeferredRevenueFlag,
       ProductDescription,
       DrawerActivityID,
       MMSDepartment,
       MembershipCreatedDateTime,
       EmployeeNumber,
       TransactionReason,
       MembershipID,
       GLAccountNumber,
       WorkdayAccount,
       RevenueProductGroupGLAccount,
       RevenueProductGroupWorkdayAccount,
       GLSubAccountNumber,
       WorkdayCostCenter,
       WorkdayOffering,
       DiscountGLAccount,
       WorkdayDiscountGLAccount,
       RevenueProductGroupDiscountGLAccount,
       WorkdayRevenueProductGroupDiscountGLAccount,
       ProductRefundGLAccount,
       WorkdayRefundGLAccount,
       RevenueProductGroupRefundGLAccount,
       WorkdayRevenueProductGroupRefundGLAccount,       
       LocalCurrencyTotalDiscountAmount,
       USDTotalDiscountAmount,
       LocalCurrencyGrossTransactionAmount,
       USDGrossTransactionAmount,
       LocalCurrencyDiscountAmount1,
       USDDiscountAmount1,
       LocalCurrencyDiscountAmount2,
       USDDiscountAmount2,
       LocalCurrencyDiscountAmount3,
       USDDiscountAmount3,
       LocalCurrencyDiscountAmount4,
       USDDiscountAmount4,
       LocalCurrencyDiscountAmount5,
       USDDiscountAmount5,
       Discount1,
       Discount2,
       Discount3,
       Discount4,
       Discount5,
       SalesChannel,
       @ReportRunDatetime ReportRunDateTime,
       @StartFourDigitYearDashTwoDigitMonth HeaderYearMonth,
       CAST('' AS VARCHAR(71)) HeaderEmptyResult,
       USDPaymentTypes,
       LocalCurrencyPaymentTypes,
       DrawerClosedDate,
       LocalCurrencyItemAmountAfterTax,
       USDItemAmountAfterTax,
       USDCorporateTransferAmount,
       LocalCurrencyCorporateTransferAmount,
       RefundGLAccount,
       VoidedFlag
  FROM #Results
 WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(NULL AS VARCHAR(50)) MMSRegion,
       CAST(NULL AS VARCHAR(50)) ClubName,
       CAST(NULL AS VARCHAR(18)) MMSClubCode,
       CAST(NULL AS VARCHAR(15)) CurrencyCode,
       CAST(NULL AS DECIMAL(14,4)) ToUSDMonthlyAverageExchangeRate,
       CAST(NULL AS DECIMAL(14,4)) ToLocalCurrencyMonthlyAverageExchangeRate,
       CAST(NULL AS VARCHAR(50)) TransactionType,
       CAST(NULL AS VARCHAR(50)) EmployeeFirstName,
       CAST(NULL AS VARCHAR(50)) EmployeeLastName,
       CAST(NULL AS VARCHAR(50)) MemberFirstName,
       CAST(NULL AS VARCHAR(80)) MemberLastName,
       CAST(NULL AS INT) MemberID,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyTransactionAmount,
       CAST(NULL AS DECIMAL(16,6)) USDTransactionAmount,
       CAST(NULL AS VARCHAR(12)) PostDate,
       CAST(NULL AS INT) MMSTransactionID,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyItemAmount,
       CAST(NULL AS DECIMAL(16,6)) USDItemAmount,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyItemSalesTax,
       CAST(NULL AS DECIMAL(16,6)) USDItemSalesTax,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyPOSAmount,
       CAST(NULL AS DECIMAL(16,6)) USDPOSAmount,
       CAST(NULL AS INT) TransactionItemId,
       CAST(NULL AS VARCHAR(50)) MembershipRegion,
       CAST(NULL AS VARCHAR(50)) MembershipClub,
       CAST(NULL AS INT) ClubID,
       CAST(NULL AS VARCHAR(4)) WorkdayRegion,
       CAST(NULL AS VARCHAR(50)) TransactionRegionDescription,
       CAST(NULL AS VARCHAR(1)) DeferredRevenueFlag,
       CAST(NULL AS VARCHAR(50)) ProductDescription,
       CAST(NULL AS INT) DrawerActivityID,
       CAST(NULL AS VARCHAR(50)) MMSDepartment,
       CAST(NULL AS VARCHAR(12)) MembershipCreatedDateTime,
       CAST(NULL AS INT) EmployeeNumber,
       CAST(NULL AS VARCHAR(50)) TransactionReason,
       CAST(NULL AS INT) MembershipID,
       CAST(NULL AS VARCHAR(10)) GLAccountNumber,
       CAST(NULL AS VARCHAR(6))  WorkdayAccount,
       CAST(NULL AS VARCHAR(10)) RevenueProductGroupGLAccount,
       CAST(NULL AS VARCHAR(6))  RevenueProductGroupWorkdayAccount,
       CAST(NULL AS VARCHAR(21)) GLSubAccountNumber,
       CAST(NULL AS VARCHAR(6))  WorkdayCostCenter,
       CAST(NULL AS VARCHAR(10)) WorkdayOffering,
       CAST(NULL AS VARCHAR(10)) DiscountGLAccount,      
       CAST(NULL AS VARCHAR(10)) WorkdayDiscountGLAccount,
       CAST(NULL AS VARCHAR(10)) RevenueProductGroupDiscountGLAccount,
       CAST(NULL AS VARCHAR(10)) WorkdayRevenueProductGroupDiscountGLAccount,
       CAST(NULL AS VARCHAR(10)) ProductRefundGLAccount,
       CAST(NULL AS VARCHAR(10)) WorkdayRefundGLAccount,
       CAST(NULL AS VARCHAR(10)) RevenueProductGroupRefundGLAccount,
       CAST(NULL AS VARCHAR(10)) WorkdayRevenueProductGroupRefundGLAccount,       
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyTotalDiscountAmount,
       CAST(NULL AS DECIMAL(16,6)) USDTotalDiscountAmount,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyGrossTransactionAmount,
       CAST(NULL AS DECIMAL(16,6)) USDGrossTransactionAmount,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyDiscountAmount1,
       CAST(NULL AS DECIMAL(16,6)) USDDiscountAmount1,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyDiscountAmount2,
       CAST(NULL AS DECIMAL(16,6)) USDDiscountAmount2,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyDiscountAmount3,
       CAST(NULL AS DECIMAL(16,6)) USDDiscountAmount3,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyDiscountAmount4,
       CAST(NULL AS DECIMAL(16,6)) USDDiscountAmount4,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyDiscountAmount5,
       CAST(NULL AS DECIMAL(16,6)) USDDiscountAmount5,
       CAST(NULL AS VARCHAR(50)) Discount1,
       CAST(NULL AS VARCHAR(50)) Discount2,
       CAST(NULL AS VARCHAR(50)) Discount3,
       CAST(NULL AS VARCHAR(50)) Discount4,
       CAST(NULL AS VARCHAR(50)) Discount5,
       CAST(NULL AS VARCHAR(50)) SalesChannel,
       @ReportRunDatetime ReportRunDateTime,
       @StartFourDigitYearDashTwoDigitMonth HeaderYearMonth,
       'There is no data available for the selected parameters.  Please re-try.' HeaderEmptyResult,
       CAST(NULL AS VARCHAR(4000)) USDPaymentTypes,
       CAST(NULL AS VARCHAR(4000)) LocalCurrencyPaymentTypes,
       CAST(NULL AS VARCHAR(21)) DrawerClosedDate,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyItemAmountAfterTax,
       CAST(NULL AS DECIMAL(16,6)) USDItemAmountAfterTax,
       CAST(NULL AS DECIMAL(16,6)) USDCorporateTransferAmount,
       CAST(NULL AS DECIMAL(16,6)) LocalCurrencyCorporateTransferAmount,
       CAST(NULL AS VARCHAR(10)) RefundGLAccount,
       CAST(NULL AS VARCHAR(1))VoidedFlag
 WHERE (SELECT COUNT(*) FROM #Results) = 0

DROP TABLE #MMSClubIDList 
DROP TABLE #FactMMSPayment
DROP TABLE #GLAccountNumberList
DROP TABLE #MMSDepartmentList
DROP TABLE #MMSDiscounts
DROP TABLE #MMSPaymentTypes
DROP TABLE #Results
DROP TABLE #TransactionTypeList
DROP TABLE #Transactions
 
END
