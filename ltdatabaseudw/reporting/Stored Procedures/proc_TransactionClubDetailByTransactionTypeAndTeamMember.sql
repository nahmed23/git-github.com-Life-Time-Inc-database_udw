CREATE PROC [reporting].[proc_TransactionClubDetailByTransactionTypeAndTeamMember] @ReportStartDate [DATETIME],@ReportEndDate [DATETIME],@DimEmployeeKeyList [VARCHAR](8000),@TransactionTypeList [VARCHAR](4000),@TransactionReasonCodeIDList [VARCHAR](4000),@TotalTransactionReasonCount [INT],@TotalTeamMemberCount [INT] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

 ----- Execution Samples
 -----   Exec [reporting].[proc_TransactionClubDetailByTransactionTypeAndTeamMember] '1/1/2019','1/31/2019','2|89741','Sale|Refund|Payment|Adjustment','0|35|34|32|39|270',6,1
 -----   Exec [reporting].[proc_TransactionClubDetailByTransactionTypeAndTeamMember] '1/1/2019','1/31/2019','2','Sale|Refund|Payment|Adjustment','0|35|34|32|39|270',6,3

DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                                           from map_utc_time_zone_conversion
                                           where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')


  
DECLARE @HeaderDateRange VARCHAR(51),
        @ReportStartDimDateKey VARCHAR(8),
        @ReportEndDimDateKey VARCHAR(8)
SELECT @HeaderDateRange = ReportStartDimDate.standard_date_name + ' through ' + ReportEndDimDate.standard_date_name,
       @ReportStartDimDateKey = ReportStartDimDate.dim_date_key,
       @ReportEndDimDateKey = ReportEndDimDate.dim_date_key
  FROM [marketing].[v_dim_date] ReportStartDimDate
 CROSS JOIN [marketing].[v_dim_date] ReportEndDimDate
 WHERE ReportStartDimDate.calendar_date = @ReportStartDate
   AND ReportEndDimDate.calendar_date = @ReportEndDate



IF OBJECT_ID('tempdb.dbo.#DimEmployeeKeyList', 'U') IS NOT NULL
  DROP TABLE #DimEmployeeKeyList; 

	--- Create #DimEmployeeKeyList temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'EmployeeKeyList'
 EXEC marketing.proc_parse_pipe_list @DimEmployeeKeyList,@list_table

 SELECT DimEmployee.dim_employee_key  AS DimEmployeeKey,
        DimEmployee.employee_id,
        DimEmployee.employee_name_last_first
INTO #DimEmployeeKeyList
From [marketing].[v_dim_Employee] DimEmployee
 JOIN #EmployeeKeyList
   ON #EmployeeKeyList.Item = DimEmployee.employee_id

 
IF OBJECT_ID('tempdb.dbo.#TransactionTypeList', 'U') IS NOT NULL
  DROP TABLE #TransactionTypeList; 

  ----- Create #TransactionTypes temp table
SET @list_table = 'TransactionTypes'
  EXEC marketing.proc_parse_pipe_list @TransactionTypeList,@list_table

SELECT #TransactionTypes.Item  AS TransactionType
  INTO #TransactionTypeList
  FROM #TransactionTypes


IF OBJECT_ID('tempdb.dbo.#TransactionReasonCodeIDList', 'U') IS NOT NULL
  DROP TABLE #TransactionReasonCodeIDList; 

  ----- Create #TransactionTypes temp table
SET @list_table = 'ReasonCodeIDList'
  EXEC marketing.proc_parse_pipe_list @TransactionReasonCodeIDList ,@list_table

SELECT #ReasonCodeIDList.Item  AS TransactionReasonCodeID
 INTO #TransactionReasonCodeIDList
  FROM #ReasonCodeIDList


IF OBJECT_ID('tempdb.dbo.#DimTransactionReasonInfo', 'U') IS NOT NULL
  DROP TABLE #DimTransactionReasonInfo; 

SELECT DISTINCT TransactionReasons.DimTransactionReasonKey,
                TransactionReasons.TransactionReason
  INTO #DimTransactionReasonInfo   
  FROM #TransactionReasonCodeIDList TransactionReasonCodeIDList
  JOIN (SELECT 'Cafe Sale' TransactionReason,
               0 ReasonCodeID,
               '0' DimTransactionReasonKey
         UNION
        SELECT 'Cafe Refund',
               -1,
               '-1' DimTransactionReasonKey
         UNION
        SELECT 'E-Commerce Sale',
               -2,
               '-2' DimTransactionReasonKey
         UNION
        SELECT Description TransactionReason,
               reason_code_id AS ReasonCodeID,
               dim_mms_transaction_reason_key AS DimTransactionReasonKey
          FROM [marketing].[v_dim_mms_transaction_reason]
         UNION
        SELECT 'HealthCheckUSA Sale',
               -3,
               '-3'
         UNION
        SELECT 'HealthCheckUSA Refund',
               -4,
               '-4'
         UNION
         SELECT 'E-Commerce Refund',
                -5,
                '-5') TransactionReasons
    ON TransactionReasonCodeIDList.TransactionReasonCodeID = TransactionReasons.ReasonCodeID



DECLARE @HeaderTransactionReason VARCHAR(50)
SELECT @HeaderTransactionReason = CASE WHEN COUNT(*) = @TotalTransactionReasonCount THEN 'All Transaction Reasons'
                                       WHEN COUNT(*) = 1 THEN Min(#DimTransactionReasonInfo.TransactionReason)
                                       ELSE 'Multiple Transaction Reasons' END
FROM #DimTransactionReasonInfo



DECLARE @TransactionTypeCommaList VARCHAR(4000)
SET @TransactionTypeCommaList = Replace(@TransactionTypeList,'|',',')

DECLARE @HeaderTeamMember VARCHAR(50)
SELECT @HeaderTeamMember = CASE WHEN COUNT(*) = @TotalTeamMemberCount THEN 'All Team Members'
                                WHEN COUNT(*) = 1 THEN Min(Convert(Varchar,DimEmployee.employee_id) + ' - ' + DimEmployee.employee_name)
                                ELSE 'Multiple Team Members' END
  FROM [marketing].[v_dim_employee]  DimEmployee
  JOIN #DimEmployeeKeyList
    ON DimEmployee.dim_employee_key = #DimEmployeeKeyList.DimEmployeeKey

DECLARE @CurrencyCode VARCHAR(15)
SELECT @CurrencyCode = 'Local Currency'


IF OBJECT_ID('tempdb.dbo.#MMSTransactions', 'U') IS NOT NULL
  DROP TABLE #MMSTransactions;   

----  to return tran types where there is a related MMS TranItem record
SELECT CASE WHEN FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_item_key IS NOT NULL  
                       AND TransactionDimClub.club_id = 13    ------ Corporate INTERNAL
                 THEN FactSalesTransactionAutomatedRefund.original_transaction_reporting_dim_club_key
            ELSE FactSalesTransaction.transaction_reporting_dim_club_key 
			END DimClubKey,      ------ Name change
       CASE WHEN FactSalesTransaction.pos_flag = 'Y' THEN 'Sale'
            WHEN FactSalesTransaction.membership_charge_flag = 'Y' THEN 'Charge'
            WHEN FactSalesTransaction.membership_adjustment_flag = 'Y' THEN 'Adjustment'
            WHEN FactSalesTransaction.refund_flag = 'Y' THEN 'Refund' 
			END TransactionType,
       FactSalesTransaction.post_dim_date_key AS PostDimDateKey,
       FactSalesTransaction.udw_inserted_dim_date_key AS UDWInsertedDimDateKey,   ------ Name change
       FactSalesTransaction.dim_mms_product_key AS DimProductKey,
       FactSalesTransaction.dim_mms_member_key  AS DimMemberKey,     ------ Name change
       DimTransactionReasonInfo.TransactionReason,
       FactSalesTransaction.sales_entered_dim_employee_key AS TransactionDimEmployeeKey,
       FactSalesTransaction.original_currency_code AS OriginalCurrencyCode,
       FactSalesTransaction.sales_quantity AS SalesQuantity,
       FactSalesTransaction.sales_amount_gross AS GrossTransactionAmount,
       CAST(FactSalesTransaction.sales_discount_dollar_amount AS Decimal(26,6)) AS TotalDiscountAmount,  
       FactSalesTransaction.sales_tax_amount AS SalesTax,
       FactSalesTransaction.sales_dollar_amount AS TotalAmount,
       FactSalesTransaction.mms_tran_id AS MMSTranID,
       FactSalesTransaction.tran_item_id AS TranItemID,
       FactSalesTransaction.sales_channel_dim_description_key AS SalesChannelDimDescriptionKey,
       0 CorporateTransferAmount       ----------- obsolete business functionality
	    INTO #MMSTransactions  
  FROM [marketing].[v_fact_mms_transaction_item]  FactSalesTransaction
  LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
    ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_item_key   
  JOIN #DimTransactionReasonInfo   DimTransactionReasonInfo 
    ON FactSalesTransaction.dim_mms_transaction_reason_key = DimTransactionReasonInfo.DimTransactionReasonKey
  JOIN #DimEmployeeKeyList
    ON FactSalesTransaction.sales_entered_dim_employee_key = #DimEmployeeKeyList.DimEmployeeKey
  JOIN #TransactionTypeList
    ON #TransactionTypeList.TransactionType = CASE WHEN FactSalesTransaction.membership_adjustment_flag = 'Y' THEN 'Adjustment'
                                                   WHEN FactSalesTransaction.membership_charge_flag = 'Y' THEN 'Charge'
                                                   WHEN FactSalesTransaction.refund_flag = 'Y' THEN 'Refund'
                                                   WHEN FactSalesTransaction.pos_flag = 'Y' THEN 'Sale' 
												   END
  JOIN [marketing].[v_dim_club] TransactionDimClub
    ON FactSalesTransaction.dim_club_key = TransactionDimClub.dim_club_key
 WHERE FactSalesTransaction.post_dim_date_key >= @ReportStartDimDateKey
   AND FactSalesTransaction.post_dim_date_key <= @ReportEndDimDateKey
   AND FactSalesTransaction.voided_flag = 'N'

  


UNION ALL

------- to return all adjustment transactions which have no related MMS TranItem record

SELECT FactTransactionAdjustment.transaction_reporting_dim_club_key  AS DimClubKey,      ------ Name change
       'Adjustment' AS  TransactionType,
       FactTransactionAdjustment.post_dim_date_key AS PostDimDateKey,
       FactTransactionAdjustment.udw_inserted_dim_date_key AS UDWInsertedDimDateKey,   ------ Name change
       NULL AS DimProductKey,
       FactTransactionAdjustment.dim_mms_member_key  AS DimMemberKey,     ------ Name change
       DimTransactionReasonInfo.TransactionReason,
       FactTransactionAdjustment.transaction_entered_dim_employee_key AS TransactionDimEmployeeKey,
       TransactionClub.local_currency_code AS OriginalCurrencyCode,
       NULL AS SalesQuantity,    
       (FactTransactionAdjustment.tran_amount + FactTransactionAdjustment.pos_amount) AS GrossTransactionAmount,
       0 AS TotalDiscountAmount,
       0 AS SalesTax,
       (FactTransactionAdjustment.tran_amount + FactTransactionAdjustment.pos_amount)  AS TotalAmount,
       FactTransactionAdjustment.mms_tran_id AS MMSTranID,
       NULL AS TranItemID,
       NULL AS SalesChannelDimDescriptionKey, 
       0 CorporateTransferAmount      ----------- Obsolete business functionality
	  
  FROM [marketing].[v_fact_mms_tranaction_adjustment]  FactTransactionAdjustment
  JOIN #DimTransactionReasonInfo   DimTransactionReasonInfo 
    ON FactTransactionAdjustment.dim_mms_transaction_reason_key = DimTransactionReasonInfo.DimTransactionReasonKey
  JOIN #DimEmployeeKeyList
    ON FactTransactionAdjustment.transaction_entered_dim_employee_key = #DimEmployeeKeyList.DimEmployeeKey
  JOIN [marketing].[v_dim_club] TransactionClub
    ON FactTransactionAdjustment.dim_club_key = TransactionClub.dim_club_key
 WHERE FactTransactionAdjustment.post_dim_date_key >= @ReportStartDimDateKey
   AND FactTransactionAdjustment.post_dim_date_key <= @ReportEndDimDateKey
   AND FactTransactionAdjustment.tran_item_exists_flag = 'N'
   AND FactTransactionAdjustment.voided_flag = 'N'
   AND @TransactionTypeList like '%Adjustment%'
   



  UNION ALL
----- to return all Payment transaction types which also have no MMS TranItem record

SELECT FactMMSTransaction.dim_club_key DimClubKey,  --------name change   
       'Payment' AS TransactionType,
       FactMMSTransaction.payment_dim_date_key AS PostDimDateKey,
       NULL UDWInsertedDimDateKey,  ----- Name change
       NULL DimProductKey,
       FactMMSTransaction.dim_mms_member_key AS DimMemberKey,    ------ Name change    
       DimTransactionReasonInfo.TransactionReason,  
       FactMMSTransaction.sales_entered_dim_employee_key AS TransactionDimEmployeeKey,  
       TransactionClub.local_currency_code AS OriginalCurrencyCode, 
       NULL SalesQuantity,
       CAST(FactMMSTransaction.payment_amount AS Decimal(26,6))*-1 AS  GrossTransactionAmount,
       CAST(0 AS Decimal(26,6)) AS TotalDiscountAmount, 
       CAST(0 AS Decimal(26,6)) AS SalesTax,  
       CAST(FactMMSTransaction.payment_amount AS Decimal(26,6))*-1 AS TotalAmount,
       FactMMSTransaction.mms_tran_id AS MMSTranID,
       NULL TranItemID,
       NULL SalesChannelDimDescriptionKey,    
       0 AS CorporateTransferAmount    

  FROM [marketing].[v_fact_mms_payment]  FactMMSTransaction       
  JOIN #DimTransactionReasonInfo DimTransactionReasonInfo
    ON FactMMSTransaction.dim_mms_transaction_reason_key = DimTransactionReasonInfo.DimTransactionReasonKey     
  JOIN #DimEmployeeKeyList  DimEmployeeKeyList
    ON FactMMSTransaction.sales_entered_dim_employee_key = DimEmployeeKeyList.DimEmployeeKey   
  JOIN [marketing].[v_dim_date] PostDimDate
    ON FactMMSTransaction.payment_dim_date_key = PostDimDate.dim_date_key
  JOIN [marketing].[v_dim_club] TransactionClub
    ON FactMMSTransaction.dim_club_key = TransactionClub.dim_club_key     

 WHERE @TransactionTypeList like '%Payment%'
   AND FactMMSTransaction.payment_dim_date_key >= @ReportStartDimDateKey
   AND FactMMSTransaction.payment_dim_date_key <= @ReportEndDimDateKey
   AND FactMMSTransaction.voided_flag = 'N'  
   AND FactMMSTransaction.payment_flag = 'Y'             


    

   IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL
  DROP TABLE #Results; 

SELECT MMSRegionDescription.description AS Region,
       DimLocation.club_name AS Club,
       TransactionDimEmployee.employee_id AS TeamMemberID,
       TransactionDimEmployee.employee_name_last_first AS TeamMemberName,
       DimMember.member_id AS MemberID,
       DimMember.customer_name_last_first AS MemberName,
       'MMS ' + Convert(Varchar,DimProduct.product_id) AS SourceProductID,
       DimProduct.product_description AS ProductDescription,
       #MMSTransactions.TransactionReason,
       #MMSTransactions.GrossTransactionAmount,
       #MMSTransactions.TotalDiscountAmount AS DiscountAmount,
       #MMSTransactions.TotalAmount AS  TransactionAmount,
       PostDimDate.standard_date_name AS MMSPostDate,
       NULL CafeCloseDate,
       NULL CafePostDate,
       NULL ECommerceOrderDate,
       NULL ECommerceShipmentDate,
       UDWInsertedDimDate.standard_date_name AS UDWInsertedDate,    -------- Name change
       #MMSTransactions.SalesTax AS SalesTax,
       (#MMSTransactions.TotalAmount + #MMSTransactions.SalesTax) AS TransactionAmountPlusSalesTax,
       PostDimDate.calendar_date AS TransactionDate,
       'MMS' SalesSource,
       #MMSTransactions.TransactionType,
       SalesChannelDimDescription.Description AS SalesChannel,
       #MMSTransactions.CorporateTransferAmount
  INTO #Results    
  FROM #MMSTransactions
  JOIN [marketing].[v_dim_date] PostDimDate
    ON #MMSTransactions.PostDimDateKey = PostDimDate.dim_date_key
  JOIN [marketing].[v_dim_club] DimLocation
    ON #MMSTransactions.DimClubKey = DimLocation.dim_club_key
  LEFT JOIN [marketing].[v_dim_date] UDWInsertedDimDate
    ON #MMSTransactions.UDWInsertedDimDateKey = UDWInsertedDimDate.dim_date_key
  LEFT JOIN [marketing].[v_dim_mms_product] DimProduct
    ON #MMSTransactions.DimProductKey = DimProduct.dim_mms_product_key
  LEFT JOIN [marketing].[v_dim_mms_member] DimMember
    ON #MMSTransactions.DimMemberKey = DimMember.dim_mms_member_key
  --JOIN [marketing].[v_dim_employee] TransactionDimEmployee
  --  ON #MMSTransactions.TransactionDimEmployeeKey = TransactionDimEmployee.dim_employee_key
  JOIN #DimEmployeeKeyList  TransactionDimEmployee
    ON #MMSTransactions.TransactionDimEmployeeKey = TransactionDimEmployee.DimEmployeeKey  
  LEFT JOIN [marketing].[v_dim_description] SalesChannelDimDescription
    ON #MMSTransactions.SalesChannelDimDescriptionKey = SalesChannelDimDescription.dim_description_key
  LEFT JOIN [marketing].[v_dim_description] MMSRegionDescription
    ON DimLocation.region_dim_description_key = MMSRegionDescription.dim_description_key



UNION ALL

SELECT MMSRegionDescription.description AS Region,
       DimLocation.club_name AS Club,
       DimEmployee.employee_id AS TeamMemberID,
       DimEmployee.employee_name_last_first AS TeamMemberName,
       NULL AS MemberID,
       NULL AS MemberName,
       'Cafe ' + Convert(Varchar,DimCafeProduct.menu_item_id) AS SourceProductID,
       DimCafeProduct.menu_item_name AS ProductDescription,
       CASE WHEN FactCafePOSSalesTransaction.item_refund_flag = 'Y' 
	        THEN 'Cafe Refund'
            ELSE 'Cafe Sale' END TransactionReason,
       FactCafePOSSalesTransaction.item_sales_amount_gross AS GrossTransactionAmount,
       FactCafePOSSalesTransaction.item_discount_amount AS DiscountAmount,
       FactCafePOSSalesTransaction.item_sales_dollar_amount_excluding_tax AS TransactionAmount,
       NULL MMSPostDate,
       TransactionCloseDimDate.standard_date_name AS CafeCloseDate,
       PostedBusinessDayStartDimDate.standard_date_name AS CafePostDate,
       NULL ECommerceOrderDate,
       NULL ECommerceShipmentDate,
       UDWDimDate.standard_date_name AS UDWInsertedDate,    ------- Name Change
       FactCafePOSSalesTransaction.item_tax_amount AS SalesTax,
       (FactCafePOSSalesTransaction.item_sales_dollar_amount_excluding_tax + FactCafePOSSalesTransaction.item_tax_amount)  AS TransactionAmountPlusSalesTax,
       TransactionCloseDimDate.calendar_date AS TransactionDate,
       'Cafe' SalesSource,
       CASE WHEN FactCafePOSSalesTransaction.order_refund_flag = 'Y' 
	        THEN 'Refund'
            ELSE 'Sale' END TransactionType,
       'Cafe' SalesChannel,
       0 AS  CorporateTransferAmount
  FROM [marketing].[v_fact_cafe_transaction_item] FactCafePOSSalesTransaction
  JOIN #DimEmployeeKeyList DimEmployee
    ON FactCafePOSSalesTransaction.order_commissionable_dim_employee_key = DimEmployee.DimEmployeeKey
  --JOIN [marketing].[v_dim_employee] DimEmployee
  --  ON FactCafePOSSalestransaction.order_commissionable_dim_employee_key = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_club] DimLocation
    ON FactCafePOSSalesTransaction.dim_club_key = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_date] TransactionCloseDimDate
    ON FactCafePOSSalesTransaction.order_close_dim_date_key = TransactionCloseDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] PostedBusinessDayStartDimDate
    ON FactCafePOSSalesTransaction.posted_business_start_dim_date_key = PostedBusinessDayStartDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] UDWDimDate
    ON FactCafePOSSalesTransaction.udw_inserted_dim_date_key = UDWDimDate.dim_date_key
  JOIN [marketing].[v_dim_cafe_product] DimCafeProduct
    ON FactCafePOSSalesTransaction.dim_cafe_product_key = DimCafeProduct.dim_cafe_product_key
  JOIN [marketing].[v_dim_description] MMSRegionDescription
    ON DimLocation.region_dim_description_key = MMSRegionDescription.dim_description_key
 WHERE FactCafePOSSalesTransaction.order_close_dim_date_key >= @ReportStartDimDateKey
   AND FactCafePOSSalesTransaction.order_close_dim_date_key <= @ReportEndDimDateKey
   AND FactCafePOSSalesTransaction.item_voided_flag = 'N'
   AND (FactCafePOSSalesTransaction.order_void_flag = 'N'
        OR FactCafePOSSalesTransaction.order_refund_flag = 'Y')
   AND (('Sale' IN (SELECT TransactionType FROM #TransactionTypeList) 
          AND '0' IN (SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo) 
          AND FactCafePOSSalesTransaction.item_refund_flag = 'N')
     OR ('Refund' IN (SELECT TransactionType FROM #TransactionTypeList) 
          AND '-1' IN (SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo)
          AND FactCafePOSSalesTransaction.item_refund_flag = 'Y'))



UNION ALL


SELECT MMSRegionDescription.description AS Region,      
       DimLocation.club_name AS Club,              
       DimEmployee.employee_id AS TeamMemberID,
       DimEmployee.employee_name_last_first AS TeamMemberName,
       DimCustomer.member_id AS MemberID,                             
       DimCustomer.customer_name_last_first AS MemberName,      
       'Hybris ' + DimECommerceProduct.code AS SourceProductID,
       DimECommerceProduct.description AS ProductDescription,
       'E-Commerce Sale' AS TransactionReason,
       (HybrisSalesTransaction.original_unit_price * HybrisSalesTransaction.transaction_quantity) AS GrossTransactionAmount,
       HybrisSalesTransaction.discount_amount AS DiscountAmount,
       HybrisSalesTransaction.transaction_amount  AS TransactionAmount,
       NULL MMSPostDate,
       NULL CafeCloseDate,
       NULL CafePostDate,
       OrderDimDate.standard_date_name AS ECommerceOrderDate,
       ShipmentDimDate.standard_date_name AS ECommerceShipmentDate,
       UDWDimDate.standard_date_name AS UDWInsertedDate,    ------- Name Change   
	   HybrisSalesTransaction.tax_amount AS SalesTax,
       (HybrisSalesTransaction.transaction_amount + HybrisSalesTransaction.tax_amount) AS TransactionAmountPlusSalesTax,
       ShipmentDimDate.calendar_date AS TransactionDate,
       'Hybris' AS SalesSource,
       'Sale' AS TransactionType,
       'Hybris' SalesChannel,
       0 AS CorporateTransferAmount
  FROM [marketing].[v_fact_hybris_transaction_item] HybrisSalesTransaction   
  --JOIN [marketing].[v_dim_employee]  DimEmployee
  --  ON HybrisSalesTransaction.sales_dim_employee_key = DimEmployee.dim_employee_key
  JOIN #DimEmployeeKeyList DimEmployee
    ON HybrisSalesTransaction.sales_dim_employee_key = DimEmployee.DimEmployeeKey
  JOIN [marketing].[v_dim_club]  DimLocation
    ON HybrisSalesTransaction.transaction_reporting_dim_club_key = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_description] MMSRegionDescription
    ON DimLocation.region_dim_description_key = MMSRegionDescription.dim_description_key
  JOIN [marketing].[v_dim_date] ShipmentDimDate
    ON HybrisSalesTransaction.settlement_dim_date_key = ShipmentDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] OrderDimDate
    ON HybrisSalesTransaction.order_dim_date_key = OrderDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] UDWDimDate
    ON HybrisSalesTransaction.udw_inserted_dim_date_key = UDWDimDate.dim_date_key      
  JOIN [marketing].[v_dim_hybris_product] DimECommerceProduct
    ON HybrisSalesTransaction.dim_hybris_product_key = DimECommerceProduct.dim_hybris_product_key
  LEFT JOIN [marketing].[v_dim_mms_member] DimCustomer
    ON HybrisSalesTransaction.dim_mms_member_key = DimCustomer.dim_mms_member_key  

 WHERE HybrisSalesTransaction.settlement_dim_date_key >= @ReportStartDimDateKey
   AND HybrisSalesTransaction.settlement_dim_date_key  <= @ReportEndDimDateKey
   AND (('Sale' IN (SELECT TransactionType FROM #TransactionTypeList)
         AND '-2' IN (SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo)
         AND HybrisSalesTransaction.refund_flag = 'N')     
        OR ('Refund' IN (SELECT TransactionType FROM #TransactionTypeList) 
           AND '-5' IN (SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo)
           AND HybrisSalesTransaction.refund_flag = 'Y'))

UNION ALL


SELECT MMSRegionDescription.description AS Region,      
       DimLocation.club_name AS Club,              
       DimEmployee.employee_id AS TeamMemberID,
       DimEmployee.employee_name_last_first AS TeamMemberName,
       DimCustomer.member_id AS MemberID,                             
       DimCustomer.customer_name_last_first AS MemberName,      
       'Magento ' + Convert(Varchar,DimMagentoProduct.product_id) AS SourceProductID,
       DimMagentoProduct.description AS ProductDescription,
       'Magento Refund' AS TransactionReason,
       (MagentoSalesTransaction.transaction_item_amount + MagentoSalesTransaction.transaction_discount_amount) AS GrossTransactionAmount,
       MagentoSalesTransaction.transaction_discount_amount AS DiscountAmount,
       MagentoSalesTransaction.transaction_item_amount AS TransactionAmount,
       NULL MMSPostDate,
       NULL CafeCloseDate,
       NULL CafePostDate,
       OrderDimDate.standard_date_name AS ECommerceOrderDate,
       TransactionDimDate.standard_date_name AS ECommerceShipmentDate,   
       UDWDimDate.standard_date_name AS UDWInsertedDate,    
       (MagentoSalesTransaction.transaction_tax_amount + MagentoSalesTransaction.shipping_tax_amount) AS  SalesTax,
       (MagentoSalesTransaction.transaction_item_amount + MagentoSalesTransaction.transaction_tax_amount + MagentoSalesTransaction.shipping_tax_amount)  AS TransactionAmountPlusSalesTax,
       TransactionDimDate.calendar_date AS TransactionDate,
       'Magento' AS SalesSource,
       CASE WHEN MagentoSalesTransaction.refund_flag = 'Y'
	        THEN 'Refund' 
			ELSE 'Sale'
			END  TransactionType,   
       'Magento' AS SalesChannel,
       0 CorporateTransferAmount
  FROM [marketing].[v_fact_magento_transaction_item] MagentoSalesTransaction    
  --JOIN [marketing].[v_dim_employee]  DimEmployee
  --  ON MagentoSalesTransaction.dim_employee_key = DimEmployee.dim_employee_key   
  JOIN #DimEmployeeKeyList DimEmployee
    ON MagentoSalesTransaction.dim_employee_key = DimEmployee.DimEmployeeKey
  JOIN [marketing].[v_dim_club] DimLocation
    ON MagentoSalesTransaction.transaction_reporting_dim_club_key = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_description] MMSRegionDescription
    ON DimLocation.region_dim_description_key = MMSRegionDescription.dim_description_key
  JOIN [marketing].[v_dim_date] TransactionDimDate
    ON MagentoSalesTransaction.transaction_dim_date_key = TransactionDimDate.dim_date_key   
  JOIN [marketing].[v_dim_date] OrderDimDate
    ON MagentoSalesTransaction.order_dim_date_key = OrderDimDate.dim_date_key
  JOIN [marketing].[v_dim_date] UDWDimDate
    ON MagentoSalesTransaction.udw_inserted_dim_date_key = UDWDimDate.dim_date_key      
  JOIN [marketing].[v_dim_magento_product] DimMagentoProduct
    ON MagentoSalesTransaction.dim_magento_product_key = DimMagentoProduct.dim_magento_product_key
  JOIN [marketing].[v_dim_mms_member] DimCustomer
    ON MagentoSalesTransaction.dim_mms_member_key = DimCustomer.dim_mms_member_key   

 WHERE MagentoSalesTransaction.transaction_dim_date_key >= @ReportStartDimDateKey
           AND MagentoSalesTransaction.transaction_dim_date_key <= @ReportEndDimDateKey
    AND (('Refund' IN (SELECT TransactionType FROM #TransactionTypeList) 
           AND '-5' IN (SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo)
           AND MagentoSalesTransaction.transaction_item_amount <> 0)
	     OR ('Sale' IN (SELECT TransactionType FROM #TransactionTypeList) 
           AND '-2' IN (SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo)
           AND MagentoSalesTransaction.transaction_item_amount <> 0))




   
UNION ALL
SELECT MMSRegionDescription.description AS Region,
       DimLocation.club_name AS ClubName,
       DimEmployee.employee_id AS TeamMemberID,
       DimEmployee.employee_name_last_first AS TeamMemberName,
       NULL MemberID,
       '' AS MemberName,
       'HealthCheckUSA ' + Convert(Varchar,DimECommerceProduct.product_sku) AS SourceProductID,
       DimECommerceProduct.product_description AS ProductDescription,                        
       CASE WHEN FactHealthCheckUSASalesTransactionItem.refund_flag = 'Y' THEN 'HealthCheckUSA Refund'
            ELSE 'HealthCheckUSA Sale' END TransactionReason,
       FactHealthCheckUSASalesTransactionItem.sales_amount + IsNull(FactHealthCheckUSASalesTransactionItem.discount_amount,0) AS GrossTransactionAmount,
       FactHealthCheckUSASalesTransactionItem.discount_amount AS TotalDiscountAmount,
       FactHealthCheckUSASalesTransactionItem.sales_amount AS TransactionAmount,
       NULL MMSPostDate,
       NULL CafeCloseDate,
       NULL CafePostDate,
       TransactionPostDimDate.standard_date_name AS ECommerceOrderDate,
       TransactionPostDimDate.standard_date_name AS ECommerceShipmentDate,
       UDWDimDate.standard_date_name AS UDWInsertedDate,           ------- Name Change
       NULL SalesTax,
       FactHealthCheckUSASalesTransactionItem.sales_amount AS TransactionAmountPlusSalesTax,
       TransactionPostDimDate.calendar_date AS TransactionDate,
       'HealthCheckUSA' SalesSource,
       FactHealthCheckUSASalesTransactionItem.transaction_type AS TransactionType,
       'E-Commerce Vendor - HealthCheckUSA' AS SalesChannel,
       0 CorporateTransferAmount
  FROM [marketing].[v_fact_healthcheckusa_transaction_item] FactHealthCheckUSASalesTransactionItem
  JOIN #DimEmployeeKeyList DimEmployee
    ON FactHealthCheckUSASalesTransactionItem.sales_dim_employee_key = DimEmployee.DimEmployeeKey
  --JOIN [marketing].[v_dim_employee] DimEmployee
  --  ON FactHealthCheckUSASalesTransactionItem.sales_dim_employee_key = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_date] TransactionPostDimDate
    ON FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key = TransactionPostDimDate.dim_date_key
  JOIN [marketing].[v_dim_healthcheckusa_product] DimECommerceProduct
    ON FactHealthCheckUSASalesTransactionItem.dim_healthcheckusa_product_key = DimECommerceProduct.dim_healthcheckusa_product_key     
  JOIN [marketing].[v_dim_club] DimLocation
    ON FactHealthCheckUSASalesTransactionItem.transaction_reporting_dim_club_key = DimLocation.dim_club_key
  JOIN [marketing].[v_dim_description] MMSRegionDescription
    ON DimLocation.region_dim_description_key = MMSRegionDescription.dim_description_key
  JOIN [marketing].[v_dim_date]  UDWDimDate
    ON FactHealthCheckUSASalesTransactionItem.udw_inserted_dim_date_key = UDWDimDate.dim_date_key                 
 WHERE FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key >= @ReportStartDimDateKey
   AND FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key <= @ReportEndDimDateKey
   AND (('Sale' IN (SELECT TransactionType FROM #TransactionTypeList) 
          AND '-3' IN (SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo) 
          AND FactHealthCheckUSASalesTransactionItem.refund_flag = 'N')
     OR ('Refund' IN (SELECT TransactionType FROM #TransactionTypeList) 
          AND '-4' IN (SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo)
          AND FactHealthCheckUSASalesTransactionItem.refund_flag = 'Y'))

SELECT Region,
       Club,
       TeamMemberName,
       MemberID,
       MemberName,
       SourceProductID,
       ProductDescription,
       TransactionReason,
       Cast(GrossTransactionAmount as Decimal(12,2)) GrossTransactionAmount,
       Cast(DiscountAmount as Decimal(12,2)) DiscountAmount,
       Cast(TransactionAmount as Decimal(12,2)) TransactionAmount,
       MMSPostDate,
       CafeCloseDate,
       CafePostDate,
       ECommerceOrderDate,
       ECommerceShipmentDate,
       UDWInsertedDate,
       TeamMemberID,
       Cast(SalesTax as Decimal(12,2)) SalesTax,
       Cast(TransactionAmountPlusSalesTax as Decimal(12,2)) TransactionAmountPlusSalesTax,
       @CurrencyCode ReportingCurrencyCode,
       @ReportRunDateTime ReportRunDateTime,
       TransactionDate,
       SalesSource,
       TransactionType,
       @HeaderDateRange HeaderDateRange,
       @TransactionTypeCommaList HeaderTransactionTypeList,
       @HeaderTransactionReason HeaderTransactionReason,
       @HeaderTeamMember HeaderTeamMember,
       Cast(NULL as Varchar(71)) HeaderEmptyResults,
       SalesChannel,
       CAST(CorporateTransferAmount as Decimal(12,2)) CorporateTransferAmount
FROM #Results
UNION ALL
SELECT Cast(NULL as Varchar(50)) Region,
       Cast(NULL as Varchar(50)) Club,
       Cast(NULL as Varchar(102)) TeamMemberName,
       NULL MemberID,
       Cast(NULL as Varchar(132)) MemberName,
       Cast(NULL as Varchar(61)) SourceProductID,
       Cast(NULL as Varchar(255)) ProductDescription,
       Cast(NULL as Varchar(50)) TransactionReason,
       Cast(NULL as Decimal(12,2)) GrossTransactionAmount,
       Cast(NULL as Decimal(12,2)) DiscountAmount,
       Cast(NULL as Decimal(12,2)) TransactionAmount,
       Cast(NULL as Varchar(12)) MMSPostDate,
       Cast(NULL as Varchar(12)) CafeCloseDate,
       Cast(NULL as Varchar(12)) CafePostDate,
       Cast(NULL as Varchar(12)) ECommerceOrderDate,
       Cast(NULL as Varchar(12)) ECommerceShipmentDate,
       Cast(NULL as Varchar(12)) UDWInsertedDate,
       NULL TeamMemberID,
       Cast(NULL as Decimal(12,2)) SalesTax,
       Cast(NULL as Decimal(12,2)) TransactionAmountPlusSalesTax,
       @CurrencyCode ReportingCurrencyCode,
       @ReportRunDateTime ReportRunDateTime,
       Cast(NULL as Datetime) TransactionDate,
       Cast(NULL as Varchar(15)) SalesSource,
       Cast(NULL as Varchar(10)) TransactionType,
       @HeaderDateRange HeaderDateRange,
       @TransactionTypeCommaList HeaderTransactionTypeList,
       @HeaderTransactionReason HeaderTransactionReason,
       @HeaderTeamMember HeaderTeamMember,
       'There is no data available for the selected parameters.  Please re-try.' HeaderEmptyResult,
       Cast(NULL as Varchar(50)) SalesChannel,
       CAST(NULL as Decimal(12,2)) CorporateTransferAmount
 WHERE (SELECT COUNT(*) FROM #Results) = 0


DROP TABLE #TransactionTypeList
DROP TABLE #DimTransactionReasonInfo
DROP TABLE #DimEmployeeKeyList
DROP TABLE #MMSTransactions
DROP TABLE #Results




END
