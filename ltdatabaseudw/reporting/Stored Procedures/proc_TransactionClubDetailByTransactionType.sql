CREATE PROC [reporting].[proc_TransactionClubDetailByTransactionType] @ReportStartDate [DATETIME],@ReportEndDate [DATETIME],@MMSClubIDList [VARCHAR](4000),@RegionList [VARCHAR](4000),@SalesSourceList [VARCHAR](4000),@TransactionTypeList [VARCHAR](4000),@TransactionReasonCodeIDList [VARCHAR](4000),@TotalTransactionReasonCount [INT],@MembershipFilter [VARCHAR] AS
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON 

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

------ Sample Execution
--- Exec [reporting].[proc_TransactionClubDetailByTransactionType] '7/3/2012','7/10/2012','151','All Regions','MMS','Adjustment','-1',6,'All Memberships' 
------

SET @ReportStartDate = CASE
                           WHEN @ReportStartDate = 'Jan 1, 1900' THEN
                               DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE() - 1), 0)
                           ELSE
                               @ReportStartDate
                       END;
SET @ReportEndDate = CASE
                         WHEN @ReportEndDate = 'Jan 1, 1900' THEN
                             CONVERT(DATETIME, CONVERT(VARCHAR, GETDATE() - 1, 101), 101)
                         ELSE
                             @ReportEndDate
                     END;

DECLARE @AdjustedEndDate DATETIME;
--What is the AdjustedEndDate calculating here ? 
SET @AdjustedEndDate = CASE
                           WHEN @ReportEndDate >= CONVERT(DATETIME, CONVERT(VARCHAR, GETDATE(), 101), 101) THEN
                               CONVERT(DATETIME, CONVERT(VARCHAR, DATEADD(dd, -1, GETDATE()), 101), 101)
                           ELSE
                               @ReportEndDate
                       END;


--2 Use the map_utc_time_zone_conversion table rather than vCurrentDateTime
DECLARE @ReportRunDateTime VARCHAR(21);
SET @ReportRunDateTime =
(
    SELECT REPLACE(
                      SUBSTRING(CONVERT(VARCHAR, DATEADD(hh, -1 * offset, GETDATE()), 100), 1, 6) + ', '
                      + SUBSTRING(CONVERT(VARCHAR, DATEADD(hh, -1 * offset, GETDATE()), 100), 8, 10) + ' '
                      + SUBSTRING(CONVERT(VARCHAR, DATEADD(hh, -1 * offset, GETDATE()), 100), 18, 2),
                      '  ',
                      ' '
                  ) get_date_varchar
    FROM map_utc_time_zone_conversion
    WHERE GETDATE()
          BETWEEN utc_start_date_time AND utc_end_date_time
          AND description = 'central time'
);


DECLARE @ReportStartDimDateKey INT;
DECLARE @ReportEndDimDateKey INT;
DECLARE @HeaderDateRange VARCHAR(51);
DECLARE @AdjustedEndDimDateKey INT;
DECLARE @AdjustedEndDate_1stOfMonthDimDateKey INT; 
DECLARE @AdjustedEndDate_2ndOfMonthDimDateKey INT;

SET @ReportStartDimDateKey =
(
    SELECT dim_date_key
    FROM [marketing].[v_dim_date]
    WHERE calendar_date = @ReportStartDate  
);
SET @ReportEndDimDateKey =
(
    SELECT dim_date_key
    FROM [marketing].[v_dim_date]
    WHERE calendar_date =  @ReportEndDate  
);
SET @HeaderDateRange =
(
    SELECT standard_date_name
    FROM [marketing].[v_dim_date]
    WHERE calendar_date = @ReportStartDate
) + ' through ' +
(
    SELECT standard_date_name
    FROM [marketing].[v_dim_date]
    WHERE calendar_date = @ReportEndDate
);
SET @AdjustedEndDimDateKey =
(
    SELECT dim_date_key
    FROM [marketing].[v_dim_date]
    WHERE calendar_date = @AdjustedEndDate
);
SET @AdjustedEndDate_1stOfMonthDimDateKey =
(
    SELECT month_starting_dim_date_key
    FROM [marketing].[v_dim_date]
    WHERE calendar_date = @AdjustedEndDate
);
SET @AdjustedEndDate_2ndOfMonthDimDateKey =
(
    SELECT next_day_dim_date_key
    FROM [marketing].[v_dim_date]
    WHERE dim_date_key = @AdjustedEndDate_1stOfMonthDimDateKey
);

IF OBJECT_ID('tempdb.dbo.#DimClubInfo', 'U') IS NOT NULL
    DROP TABLE #DimClubInfo;

----- Create club temp table
DECLARE @list_table VARCHAR(100);
SET @list_table = 'club_list';

EXEC marketing.proc_parse_pipe_list @MMSClubIDList, @list_table;

SELECT DimClub.dim_club_key AS DimClubKey,
       DimClub.club_id,
       DimClub.club_name ClubName,
       DimClub.region_dim_description_key,
       DimClub.club_code ClubCode,
       DimClub.gl_club_id GLClubID,
       DimClub.local_currency_code AS LocalCurrencyCode,
       MMSRegion.description AS MMSRegion,
       PTRCLRegion.description AS PTRCLRegion,
       MemberActivitiesRegion.description AS MemberActivitiesRegion
INTO #DimClubInfo
FROM [marketing].[v_dim_club] DimClub
    JOIN #club_list ClubKeyList
        ON ClubKeyList.Item = DimClub.club_id
           OR ClubKeyList.Item = -1
    JOIN [marketing].[v_dim_description] MMSRegion
        ON MMSRegion.dim_description_key = DimClub.region_dim_description_key
    JOIN [marketing].[v_dim_description] PTRCLRegion
        ON PTRCLRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
    JOIN [marketing].[v_dim_description] MemberActivitiesRegion
        ON MemberActivitiesRegion.dim_description_key = DimClub.member_activities_region_dim_description_key
WHERE DimClub.club_id NOT IN ( -1, 99, 100 )
      AND DimClub.club_id < 900
      AND DimClub.club_type = 'Club'
      AND
      (
          DimClub.club_close_dim_date_key IN ( '-997', '-998', '-999' )
          OR DimClub.club_close_dim_date_key > @ReportStartDimDateKey
      )
GROUP BY DimClub.dim_club_key,
         DimClub.club_id,
         DimClub.club_name,
         DimClub.club_code,
         DimClub.region_dim_description_key,
         DimClub.gl_club_id,
         DimClub.local_currency_code,
         MMSRegion.description,
         PTRCLRegion.description,
         MemberActivitiesRegion.description;
IF OBJECT_ID('tempdb.dbo.#DimLocationInfo', 'U') IS NOT NULL
    DROP TABLE #DimLocationInfo;

----- Create Region temp table
SET @list_table = 'region_list';

EXEC marketing.proc_parse_pipe_list @RegionList, @list_table;

SELECT DimClub.DimClubKey, ------ name change
                           /*  CASE WHEN @RegionType = 'PT RCL Area' 
             THEN DimClub.PTRCLRegion
           WHEN @RegionType = 'Member Activities Region' 
             THEN DimClub.MemberActivitiesRegion
           WHEN @RegionTYpe = 'MMS Region' 
             THEN DimClub.MMSRegion  */
       DimClub.MMSRegion Region,
       DimClub.ClubName,
       DimClub.club_id AS MMSClubID,
       DimClub.GLClubID,
       DimClub.LocalCurrencyCode
INTO #DimLocationInfo
FROM #DimClubInfo DimClub
    JOIN #region_list RegionList
        /* ON RegionList.Item = CASE WHEN @RegionType = 'PT RCL Area' 
                                   THEN DimClub.PTRCLRegion
                              WHEN @RegionType = 'Member Activities Region' 
                                   THEN DimClub.MemberActivitiesRegion
                              WHEN @RegionTYpe = 'MMS Region' */
        ON RegionList.Item = DimClub.MMSRegion
           OR RegionList.Item = 'All Regions'
GROUP BY DimClub.MMSRegion,
         DimClub.DimClubKey,
         DimClub.ClubName,
         DimClub.club_id,
         DimClub.GLClubID,
         DimClub.LocalCurrencyCode;
		

IF OBJECT_ID('tempdb.dbo.#SalesSourceList', 'U') IS NOT NULL
    DROP TABLE #SalesSourceList;

SET @list_table = 'SalesSource_list';

EXEC marketing.proc_parse_pipe_list @SalesSourceList, @list_table;

SELECT DISTINCT
       SalesSourceList.Item SalesSource
INTO #SalesSourceList
FROM #SalesSource_list SalesSourceList;




IF OBJECT_ID('tempdb.dbo.#TransactionTypeList', 'U') IS NOT NULL
    DROP TABLE #TransactionTypeList;

SET @list_table = 'TransactionTypes';
EXEC marketing.proc_parse_pipe_list @TransactionTypeList, @list_table;

SELECT DISTINCT
       #TransactionTypes.Item AS TransactionType
INTO #TransactionTypeList
FROM #TransactionTypes;






IF OBJECT_ID('tempdb.dbo.#TransactionReasonCodeIDList', 'U') IS NOT NULL
    DROP TABLE #TransactionReasonCodeIDList;


SET @list_table = 'ReasonCodeIDList';
EXEC marketing.proc_parse_pipe_list @TransactionReasonCodeIDList,
                                    @list_table;

SELECT #ReasonCodeIDList.Item AS TransactionReasonCodeID
INTO #TransactionReasonCodeIDList
FROM #ReasonCodeIDList;



IF OBJECT_ID('tempdb.dbo.#DimTransactionReasonInfo', 'U') IS NOT NULL
    DROP TABLE #DimTransactionReasonInfo;

SELECT DISTINCT
       TransactionReasons.DimTransactionReasonKey,
       TransactionReasons.TransactionReason,
       TransactionReasons.MemberRelationsAdjustmentCategory
INTO #DimTransactionReasonInfo
FROM #TransactionReasonCodeIDList TransactionReasonCodeIDList
    JOIN
    (
        SELECT 'Cafe Sale' TransactionReason,
               0 ReasonCodeID,
               '0' DimTransactionReasonKey,
               '' AS MemberRelationsAdjustmentCategory
        UNION
        SELECT 'Cafe Refund',
               -1,
               '-1' DimTransactionReasonKey,
               '' AS MemberRelationsAdjustmentCategory
        UNION
        SELECT 'E-Commerce Sale',
               -2,
               '-2' DimTransactionReasonKey, --Conversion failed when converting the varchar value '6A1AE3B1FD7C098C11B3C37F43306F33' to data type int. we will convert the dimtransactionreasonkey to varchar ''
               '' AS MemberRelationsAdjustmentCategory
        UNION
        SELECT description TransactionReason,
               reason_code_id AS ReasonCodeID,
               dim_mms_transaction_reason_key AS DimTransactionReasonKey,
              
               CASE
                   WHEN reason_code_id IN ( 269, 270, 271, 273, 274 ) THEN
                       'Club Adjustments'
                   WHEN reason_code_id IN ( 281, 282, 283 ) THEN
                       'Delinquent Adjustments'
                   WHEN reason_code_id IN ( 49, 88, 108, 125, 138, 151, 206, 219, 224, 250, 251, 252, 268, 275, 280,
                                            284, 285, 286, 287, 289, 290
                                          ) THEN
                       'Corporate Adjustments'
                   ELSE
                       ''
               END MemberRelationsAdjustmentCategory
        FROM [marketing].[v_dim_mms_transaction_reason]
        UNION
        SELECT 'HealthCheckUSA Sale',
               -3,
               '-3',
               '' AS MemberRelationsAdjustmentCategory
        UNION
        SELECT 'HealthCheckUSA Refund',
               -4,
               '-4',
               '' AS MemberRelationsAdjustmentCategory
        UNION
        SELECT 'E-Commerce Refund',
               -5,
               '-5',
               '' AS MemberRelationsAdjustmentCategory
    ) TransactionReasons
        ON TransactionReasonCodeIDList.TransactionReasonCodeID = TransactionReasons.ReasonCodeID
           OR TransactionReasonCodeIDList.TransactionReasonCodeID = '-1'; --OR TransactionReasonCodeIDList.Item = '-1'   --This is the error (Invalid column name 'Item')

DECLARE @SalesSourceCommaList VARCHAR(4000);
SET @SalesSourceCommaList = REPLACE(@SalesSourceList, '|', ',');

DECLARE @TransactionTypeCommaList VARCHAR(4000);
SET @TransactionTypeCommaList = REPLACE(@TransactionTypeList, '|', ',');

DECLARE @HeaderTransactionReason VARCHAR(50);
SELECT @HeaderTransactionReason = CASE
                                      WHEN @TransactionReasonCodeIDList LIKE '%-1%' THEN
                                          'All Transaction Reasons'
                                      WHEN COUNT(*) = @TotalTransactionReasonCount THEN
                                          'All Transaction Reasons'
                                      WHEN COUNT(*) = 1 THEN
                                          MIN(#DimTransactionReasonInfo.TransactionReason)
                                      ELSE
                                          'Multiple Transaction Reasons'
                                  END
FROM #DimTransactionReasonInfo;



IF OBJECT_ID('tempdb.dbo.#MMSTransactions', 'U') IS NOT NULL
    DROP TABLE #MMSTransactions;
	
----   to return tran types where there is a related MMS TransItem record
SELECT CASE
           WHEN FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_item_key IS NOT NULL
                AND TransactionDimClub.club_id = 13 --- Corporate INTERNAL
    THEN
               FactSalesTransactionAutomatedRefund.original_transaction_reporting_dim_club_key
           ELSE
               FactSalesTransaction.transaction_reporting_dim_club_key
       END DimClubKey,                                                                        ---Name change
       CASE
           WHEN FactSalesTransaction.membership_adjustment_flag = 'Y' THEN
               'Adjustment'
           WHEN FactSalesTransaction.membership_charge_flag = 'Y' THEN
               'Charge'
           --	 WHEN FactSalesTransaction.PaymentFlag = 'Y' THEN 'Payment' --- There is no Payment flag column. Verify if this is needed
           WHEN FactSalesTransaction.refund_flag = 'Y' THEN
               'Refund'
           WHEN FactSalesTransaction.pos_flag = 'Y' THEN
               'Sale'
       END TransactionType,
       FactSalesTransaction.post_dim_date_key AS PostDimDateKey,
       FactSalesTransaction.dim_mms_drawer_activity_key AS DimMMSDrawerActivityKey,
       FactSalesTransaction.udw_inserted_dim_date_key AS UDWInsertedDimDateKey,               ------ Name changed
                                                                                              --NULL UDWInsertedDimDateKey,
       FactSalesTransaction.dim_mms_product_key AS DimProductKey,
       FactSalesTransaction.dim_mms_member_key AS DimMemberKey,                               ------ Name changed
                                                                                              --DimTransactionReasonInfo.TransactionReason,
       DimTransactionReasonInfo.TransactionReason,
       FactSalesTransaction.sales_entered_dim_employee_key AS TransactionDimEmployeeKey,
       FactSalesTransaction.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,     --Changed from PrimaryDimEmployeeSCDKey verify
       FactSalesTransaction.secondary_sales_dim_employee_key AS SecondarySalesDimEmployeeKey, --Chnaged from SecondaryDimEmployeeSCDKey verify
       FactSalesTransaction.original_currency_code AS OriginalCurencyCode,
                                                                                              --FactSalesTransaction.sales_quantity AS SalesQuantity, 
       CASE
           WHEN FactSalesTransaction.sales_dollar_amount = 0
                AND FactSalesTransaction.refund_flag = 'Y' THEN
               FactSalesTransaction.sales_quantity * -1
           WHEN FactSalesTransaction.sales_dollar_amount = 0
                AND FactSalesTransaction.refund_flag = 'N' THEN
               FactSalesTransaction.sales_quantity
           ELSE
               FactSalesTransaction.sales_quantity * SIGN(FactSalesTransaction.sales_dollar_amount)
       END SalesQuantity,
       FactSalesTransaction.sold_not_serviced_flag AS SoldNotServiceFlag,                     --Changed from SNS Flag verify
       FactSalesTransaction.sales_amount_gross AS GrossTransactionAmount,
       CAST(FactSalesTransaction.sales_discount_dollar_amount AS DECIMAL(26, 6)) AS TotalDiscountAmount,
       FactSalesTransaction.sales_tax_amount AS SalesTax,
       FactSalesTransaction.sales_dollar_amount AS TotalAmount,
       FactSalesTransaction.mms_tran_id AS MMSTranID,
       FactSalesTransaction.tran_item_id AS TranItemID,
       FactSalesTransaction.sales_channel_dim_description_key AS SalesChannelDimDescriptionKey,
       FactSalesTransaction.receipt_comment AS TransactionComment,
       0 CorporateTransferAmount,                                                             ---Obsolete business functionality
       DimTransactionReasonInfo.MemberRelationsAdjustmentCategory
INTO  #MMSTransactions --2030
FROM [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
    LEFT JOIN [marketing].[v_fact_mms_transaction_item_automated_refund] FactSalesTransactionAutomatedRefund
        ON FactSalesTransaction.fact_mms_sales_transaction_item_key = FactSalesTransactionAutomatedRefund.refund_fact_mms_sales_transaction_item_key
    JOIN #DimTransactionReasonInfo DimTransactionReasonInfo
        ON FactSalesTransaction.dim_mms_transaction_reason_key = DimTransactionReasonInfo.DimTransactionReasonKey
    JOIN #DimClubInfo
        ON FactSalesTransaction.transaction_reporting_dim_club_key = #DimClubInfo.DimClubKey
    
    JOIN #TransactionTypeList
        ON #TransactionTypeList.TransactionType = CASE
                                                      WHEN FactSalesTransaction.membership_adjustment_flag = 'Y' THEN
                                                          'Adjustment'
                                                      WHEN FactSalesTransaction.membership_charge_flag = 'Y' THEN
                                                          'Charge'
                                                      WHEN FactSalesTransaction.refund_flag = 'Y' THEN
                                                          'Refund'
                                                      WHEN FactSalesTransaction.pos_flag = 'Y' THEN
                                                          'Sale'
													
                                                  END
    JOIN [marketing].[v_dim_club] TransactionDimClub
        ON FactSalesTransaction.dim_club_key = TransactionDimClub.dim_club_key
WHERE FactSalesTransaction.voided_flag = 'N'
      AND FactSalesTransaction.post_dim_date_key >= /*'20190101' --*/ @ReportStartDimDateKey
      AND FactSalesTransaction.post_dim_date_key <= /*'20190101' --*/@AdjustedEndDimDateKey --@ReportEndDimDateKey --@AdjustedEndDimDateKey


UNION ALL


SELECT FactTransactionAdjustment.transaction_reporting_dim_club_key AS DimClubKey, ------ Name change
       'Adjustment' AS TransactionType,
       FactTransactionAdjustment.post_dim_date_key AS PostDimDateKey,
       FactTransactionAdjustment.dim_mms_drawer_activity_key AS DimMMSDrawerActivityKey,
                                                                                   -- FactTransactionAdjustment.udw_inserted_dim_date_key AS UDWInsertedDimDateKey,   ------ Name change
       NULL UDWInsertedDimDateKey,
       NULL AS DimProductKey,
       FactTransactionAdjustment.dim_mms_member_key AS DimMemberKey,               ------ Name change
       DimTransactionReasonInfo.TransactionReason,
       FactTransactionAdjustment.transaction_entered_dim_employee_key AS TransactionDimEmployeeKey,
       NULL AS PrimarySalesDimEmployeeKey,
       NULL AS SecondarySalesDimEmployeeKey,
                                                                                   -- FactTransactionAdjustment.primary_sales_dim_employee_key AS PrimarySalesDimEmployeeKey,             --Changed from PrimaryDimEmployeeSCDKey verify
                                                                                   -- FactTransactionAdjustment.secondary_sales_dim_employee_key AS SecondarySalesDimEmployeeKey,         --Chnaged from SecondaryDimEmployeeSCDKey verif
       TransactionClub.local_currency_code AS OriginalCurrencyCode,
       NULL AS SalesQuantity,
       NULL AS SoldNotServiceFlag,
       (FactTransactionAdjustment.tran_amount + FactTransactionAdjustment.pos_amount) AS GrossTransactionAmount,
       0 AS TotalDiscountAmount,
       0 AS SalesTax,
       (FactTransactionAdjustment.tran_amount + FactTransactionAdjustment.pos_amount) AS TotalAmount,
       FactTransactionAdjustment.mms_tran_id AS MMSTranID,
       NULL AS TranItemID,
       NULL AS SalesChannelDimDescriptionKey,
       NULL AS TransactionComment,
       0 CorporateTransferAmount,                                                  ----------- Obsolete business functionalit
       DimTransactionReasonInfo.MemberRelationsAdjustmentCategory
FROM [marketing].[v_fact_mms_transaction_adjustment] FactTransactionAdjustment
    JOIN #DimTransactionReasonInfo DimTransactionReasonInfo
        ON FactTransactionAdjustment.dim_mms_transaction_reason_key = DimTransactionReasonInfo.DimTransactionReasonKey
    JOIN #DimClubInfo
        ON FactTransactionAdjustment.transaction_reporting_dim_club_key = #DimClubInfo.DimClubKey
    --   ON FactTransactionAdjustment.transaction_entered_dim_employee_key = #DimEmployeeKeyList.DimEmployeeKey
    JOIN [marketing].[v_dim_club] TransactionClub
        ON FactTransactionAdjustment.dim_club_key = TransactionClub.dim_club_key
WHERE FactTransactionAdjustment.post_dim_date_key >= /*'20190101' --*/@ReportStartDimDateKey
      AND FactTransactionAdjustment.post_dim_date_key <= /*'20190101' --*/@AdjustedEndDimDateKey --@AdjustedEndDimDateKey --@ReportEndDimDateKey
      AND FactTransactionAdjustment.tran_item_exists_flag = 'N'
      AND FactTransactionAdjustment.voided_flag = 'N'
     AND @TransactionTypeList LIKE '%Adjustment%'
	  
UNION ALL
----- to return all Payment transaction types which also have no MMS TranItem record

SELECT FactMMSTransaction.dim_club_key DimClubKey,            --------name change   
       'Payment' AS TransactionType,
       FactMMSTransaction.payment_dim_date_key AS PostDimDateKey,
       FactMMSTransaction.dim_mms_drawer_activity_key AS DimMMSDrawerActivityKey,
       NULL UDWInsertedDimDateKey,                            ----- Name change
       NULL DimProductKey,
       FactMMSTransaction.dim_mms_member_key AS DimMemberKey, ------ Name change    
       DimTransactionReasonInfo.TransactionReason,
       FactMMSTransaction.sales_entered_dim_employee_key AS TransactionDimEmployeeKey,
       NULL AS PrimarySalesDimEmployeeKey,
       NULL AS SecondarySalesDimEmployeekey,
       TransactionClub.local_currency_code AS OriginalCurrencyCode,
       NULL SalesQuantity,
       NULL AS SoldNotServiceFlag,
       CAST(FactMMSTransaction.payment_amount AS DECIMAL(26, 6)) * -1 AS GrossTransactionAmount,
       CAST(0 AS DECIMAL(26, 6)) AS TotalDiscountAmount,
       CAST(0 AS DECIMAL(26, 6)) AS SalesTax,
       CAST(FactMMSTransaction.payment_amount AS DECIMAL(26, 6)) * -1 AS TotalAmount,
       FactMMSTransaction.mms_tran_id AS MMSTranID,
       NULL TranItemID,
       NULL SalesChannelDimDescriptionKey,
	
	 
       NULL AS TransactionComment,
       0 AS CorporateTransferAmount,
       DimTransactionReasonInfo.MemberRelationsAdjustmentCategory

--Conversion failed when converting the varchar value 'Exerp Payment Request Reference = 0151000664570010002,External Credit Line = 151cred292cnl1' to data type int.

--SELECT * 
 FROM [marketing].[v_fact_mms_payment] FactMMSTransaction
    JOIN #DimTransactionReasonInfo DimTransactionReasonInfo
        ON FactMMSTransaction.dim_mms_transaction_reason_key = DimTransactionReasonInfo.DimTransactionReasonKey
    JOIN #DimClubInfo DimClubInfo
        ON FactMMSTransaction.transaction_reporting_dim_club_key = DimClubInfo.DimClubKey
    -- JOIN #DimEmployeeKeyList  DimEmployeeKeyList
    --   ON FactMMSTransaction.sales_entered_dim_employee_key = DimEmployeeKeyList.DimEmployeeKey   
    JOIN [marketing].[v_dim_date] PostDimDate
        ON FactMMSTransaction.payment_dim_date_key = PostDimDate.dim_date_key
    JOIN [marketing].[v_dim_club] TransactionClub
        ON FactMMSTransaction.dim_club_key = TransactionClub.dim_club_key
WHERE @TransactionTypeList LIKE '%Payment%'
      AND FactMMSTransaction.payment_dim_date_key >= /*'20190101' --*/@ReportStartDimDateKey
      AND FactMMSTransaction.payment_dim_date_key <= /*'20190101' --*/@AdjustedEndDimDateKey -- @AdjustedEndDimDateKey --@ReportEndDimDateKey
      AND FactMMSTransaction.voided_flag = 'N'
      AND FactMMSTransaction.payment_flag = 'Y'
	  --AND ('Payment' IN (SELECT TransactionType FROM #TransactionTypeList))





IF OBJECT_ID('tempdb.dbo.#FactMMSPayment', 'U') IS NOT NULL
    DROP TABLE #FactMMSPayment;
-------Come back for this and possibly verify if we even need ---------

SELECT DISTINCT
       FactMMSPayment.mms_tran_id MMSTranID,
       FactMMSPayment.payment_type_dim_description_key PaymentTypeDimDescriptionKey
INTO #FactMMSPayment
FROM #MMSTransactions
    --JOIN vFactMMSPayment FactMMSPayment
    JOIN [marketing].[v_fact_mms_payment] FactMMSPayment
        ON #MMSTransactions.MMSTranID = FactMMSPayment.mms_tran_id;


/************************************************************************************************************************
	USE STRING AGGREGATION TO CREATE PAYMENTTYPES IN REPLACEMENT OF THE STUFF FUCNTION IN LTF - SUPPORTED FUNCTION IN UDW
***********************************************************************************************************************/
IF OBJECT_ID('tempdb.dbo.#MMSPaymentTypes', 'U') IS NOT NULL
	DROP TABLE #MMSPaymentTypes;

CREATE TABLE #MMSPaymentTypes 
(		MMSTranID INT,
		PaymentTypes VARCHAR(4000)
)
INSERT INTO #MMSPaymentTypes
SELECT MMSTranID,
	(SELECT 
		STRING_AGG(DimDescription.description, ', ')
			WITHIN GROUP  (ORDER BY MMSTranID)
	 FROM #FactMMSPayment InnerFactMMSPayment
	 JOIN [marketing].[v_dim_description] DimDescription 
		ON InnerFactMMSPayment.PaymentTypeDimDescriptionKey = DimDescription.dim_description_key
	 WHERE OuterFactMMSPayment.MMSTranID =  InnerFactMMSPayment.MMSTranID ) AS PaymentTypes

		
FROM #FactMMSPayment OuterFactMMSPayment
GROUP BY MMSTranID
	 
/*******************************************END HERE **********************************************/


IF OBJECT_ID('tempdb.dbo.#MMSDiscounts', 'U') IS NOT NULL
    DROP TABLE #MMSDiscounts;

SELECT DiscountRank.TranItemID,
       SUM(   CASE
                  WHEN DiscountRank.Ranking = 1 THEN
                      DiscountRank.DiscountAmount
                  ELSE
                      0
              END
          ) DiscountAmount1,
       SUM(   CASE
                  WHEN DiscountRank.Ranking = 2 THEN
                      DiscountRank.DiscountAmount
                  ELSE
                      0
              END
          ) DiscountAmount2,
       SUM(   CASE
                  WHEN DiscountRank.Ranking = 3 THEN
                      DiscountRank.DiscountAmount
                  ELSE
                      0
              END
          ) DiscountAmount3,
       SUM(   CASE
                  WHEN DiscountRank.Ranking = 4 THEN
                      DiscountRank.DiscountAmount
                  ELSE
                      0
              END
          ) DiscountAmount4,
       SUM(   CASE
                  WHEN DiscountRank.Ranking = 5 THEN
                      DiscountRank.DiscountAmount
                  ELSE
                      0
              END
          ) DiscountAmount5,
       CASE
           WHEN MAX(   CASE
                           WHEN DiscountRank.Ranking = 1 THEN
                               DiscountRank.SalesPromotionReceiptText
                           ELSE
                               CHAR(0)
                       END
                   ) = CHAR(0) THEN
               NULL
           ELSE
               MAX(   CASE
                          WHEN DiscountRank.Ranking = 1 THEN
                              DiscountRank.SalesPromotionReceiptText
                          ELSE
                              CHAR(0)
                      END
                  )
       END Discount1,
       CASE
           WHEN MAX(   CASE
                           WHEN DiscountRank.Ranking = 2 THEN
                               DiscountRank.SalesPromotionReceiptText
                           ELSE
                               CHAR(0)
                       END
                   ) = CHAR(0) THEN
               NULL
           ELSE
               MAX(   CASE
                          WHEN DiscountRank.Ranking = 2 THEN
                              DiscountRank.SalesPromotionReceiptText
                          ELSE
                              CHAR(0)
                      END
                  )
       END Discount2,
       CASE
           WHEN MAX(   CASE
                           WHEN DiscountRank.Ranking = 3 THEN
                               DiscountRank.SalesPromotionReceiptText
                           ELSE
                               CHAR(0)
                       END
                   ) = CHAR(0) THEN
               NULL
           ELSE
               MAX(   CASE
                          WHEN DiscountRank.Ranking = 3 THEN
                              DiscountRank.SalesPromotionReceiptText
                          ELSE
                              CHAR(0)
                      END
                  )
       END Discount3,
       CASE
           WHEN MAX(   CASE
                           WHEN DiscountRank.Ranking = 4 THEN
                               DiscountRank.SalesPromotionReceiptText
                           ELSE
                               CHAR(0)
                       END
                   ) = CHAR(0) THEN
               NULL
           ELSE
               MAX(   CASE
                          WHEN DiscountRank.Ranking = 4 THEN
                              DiscountRank.SalesPromotionReceiptText
                          ELSE
                              CHAR(0)
                      END
                  )
       END Discount4,
       CASE
           WHEN MAX(   CASE
                           WHEN DiscountRank.Ranking = 5 THEN
                               DiscountRank.SalesPromotionReceiptText
                           ELSE
                               CHAR(0)
                       END
                   ) = CHAR(0) THEN
               NULL
           ELSE
               MAX(   CASE
                          WHEN DiscountRank.Ranking = 5 THEN
                              DiscountRank.SalesPromotionReceiptText
                          ELSE
                              CHAR(0)
                      END
                  )
       END Discount5
INTO #MMSDiscounts
FROM 
(
    SELECT FactSalesTransactionDiscount.TranItemID,
           RANK() OVER (PARTITION BY FactSalesTransactionDiscount.TranItemID
                        ORDER BY FactSalesTransactionDiscount.FactSalesTransactionDiscountKey
                       ) Ranking,
           FactSalesTransactionDiscount.DiscountAmount,
           --  DimClubPOSPricingDiscount.SalesPromotionReceiptText
           DimMMSPricingDiscount.sales_promotion_receipt_text AS SalesPromotionReceiptText
    FROM
    (
        SELECT MIN(FactMMSTransactionItem.TranItemID) TranItemID,
               MIN(FactSalesTransactionDiscount.discount_amount) DiscountAmount,
               MIN(FactSalesTransactionDiscount.dim_mms_pricing_discount_key) DimPricingDiscountKey,
               MIN(FactSalesTransactionDiscount.fact_mms_sales_transaction_item_discount_key) FactSalesTransactionDiscountKey
        FROM #MMSTransactions FactMMSTransactionItem --[marketing].[v_fact_mms_transaction_item]   FactMMSTransactionItem-- #MMSTransactions
            --JOIN vFactSalesTransactionDiscount FactSalesTransactionDiscount
            JOIN [marketing].[v_fact_mms_transaction_item_discount] FactSalesTransactionDiscount
                ON FactMMSTransactionItem.TranItemID = FactSalesTransactionDiscount.tran_item_id
            -- JOIN vDimDate PostDimDate
            JOIN [marketing].[v_dim_date] PostDimDate
                --   ON #MMSTransactions.PostDimDateKey = PostDimDate.DimDateKey
                ON FactMMSTransactionItem.PostDimDateKey = PostDimDate.dim_date_key
        WHERE FactMMSTransactionItem.TranItemID <> ''
        GROUP BY FactSalesTransactionDiscount.fact_mms_sales_transaction_item_discount_key
    ) FactSalesTransactionDiscount
        --  JOIN vDimClubPOSPricingDiscountActive DimClubPOSPricingDiscount
        JOIN [marketing].[v_dim_mms_pricing_discount] DimMMSPricingDiscount
            ON FactSalesTransactionDiscount.DimPricingDiscountKey = DimMMSPricingDiscount.dim_mms_pricing_discount_key
) DiscountRank
WHERE DiscountRank.Ranking <= 5
GROUP BY DiscountRank.TranItemID;




IF OBJECT_ID('tempdb.dbo.#TransactionDetail', 'U') IS NOT NULL
    DROP TABLE #TransactionDetail;
	
CREATE TABLE #TransactionDetail
(
    Region VARCHAR(100),
    ClubCode VARCHAR(18),        
    ClubName VARCHAR(50),
    TransactionType VARCHAR(100),
    MMSPostDate VARCHAR(12),
    MMSDrawerCloseDate VARCHAR(12),
    CafeCloseDate VARCHAR(50),   
    CafePostDate VARCHAR(50),    
    ECommerceOrderDate VARCHAR(50),
    ECommerceShipmentDate VARCHAR(50),
    EDWInsertedDate VARCHAR(50), 
    Source VARCHAR(50),
    SourceProductID VARCHAR(255),
    ProductDescription VARCHAR(255),
    PaymentTypes VARCHAR(4000),
    MemberID INT,
    MemberName VARCHAR(132),
    TransactionReason VARCHAR(255),
    TransactionTeamMemberID INT,
    TransactionTeamMemberName VARCHAR(255),
    TransactionTeamMemberHomeClub VARCHAR(255),
    CommissionedTeamMember1ID INT,
    CommissionedTeamMember1Name VARCHAR(255),
    CommissionedTeamMember1HomeClub VARCHAR(255),
    CommissionedTeamMember2ID INT,
    CommissionedTeamMember2Name VARCHAR(255),
    CommissionedTeamMember2HomeClub VARCHAR(255),
    SalesQuantity INT,
    SNSFlag CHAR(1),
    GrossTransactionAmount DECIMAL(12, 2),
    TotalDiscountAmount DECIMAL(12, 2),
    SalesTax DECIMAL(12, 2),
    TotalAmount DECIMAL(12, 2),
    DiscountAmount1 DECIMAL(12, 2),
    DiscountAmount2 DECIMAL(12, 2),
    DiscountAmount3 DECIMAL(12, 2),
    DiscountAmount4 DECIMAL(12, 2),
    DiscountAmount5 DECIMAL(12, 2),
    Discount1 VARCHAR(255),
    Discount2 VARCHAR(255),
    Discount3 VARCHAR(255),
    Discount4 VARCHAR(255),
    Discount5 VARCHAR(255),
    TransactionDate DATETIME,
    SalesChannel VARCHAR(255),   --come back to verify the MMS sales channel
    MembershipID INT,
    CorporateTransferAmount DECIMAL(12, 2),
    ECommerceShipmentNumber VARCHAR(255),
    ECommerceOrderNumber INT,
    ECommerceAutoShipFlag CHAR(1),
    ECommerceOrderEntryTrackingNumber VARCHAR(255),
    ECommerceProductCost DECIMAL(12, 2),
    ECommerceShipmentLineNumber INT,
    ECommerceShippingAndHandlingAmount DECIMAL(12, 2),
    GLClubID INT,
    TransactionComment VARCHAR(4000),
    MemberRelationsAdjustmentCategory VARCHAR(255)
);



IF 'MMS' IN
   (
       SELECT SalesSource FROM #SalesSourceList
   )
BEGIN
    INSERT INTO #TransactionDetail
    SELECT -- DimLocation.region_dim_description_key Region, --MMSRegionName Region,
        DimLocation.MMSRegion region,
        DimLocation.ClubCode,                                      
        DimLocation.ClubName,                                       
                                                                    
        #MMSTransactions.TransactionType,
        PostDimDate.standard_date_name MMSPostDate,                 
        DrawerClosedDimDate.standard_date_name MMSDrawerClosedDate, 
        NULL CafeCloseDate,
        NULL CafePostDate,
        NULL ECommerceOrderDate,
        NULL ECommerceShipmentDate,
        EDWInsertedDimDate.standard_date_name EDWInsertedDate,      --EDWInsertedDimDate.StandardDateDescription EDWInsertedDate,
        'MMS' Source,
        CONVERT(VARCHAR, DimProduct.product_id) SourceProductID,    ---Null SourceProductID, --      Convert(Varchar,DimProduct.product_id) SourceProductID,

        DimProduct.product_description ProductDescription,          -- NULL ProductDescription, --   DimProduct.product_description AS ProductDescription,
        #MMSPaymentTypes.PaymentTypes,
        DimCustomer.member_id MemberID,
        DimCustomer.customer_name_last_first MemberName,
                                                                    --   #MMSTransactions.dim_mms_transaction_reason_key TransactionReason, --in ('-997' no data, '-998 N/a', '-999 unknown')
        #MMSTransactions.TransactionReason,
        CASE
            WHEN TransactionDimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                TransactionDimEmployee.employee_id
        END TransactionTeamMemberID,
        CASE
            WHEN TransactionDimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                TransactionDimEmployee.employee_name_last_first
        END TransactionTeamMemberName,
                                                                     
                                                                  
        CASE
            WHEN TransactionDimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                TransactionEmployeeDimLocation.club_name
        END TransactionTeamMemberHomeClub,
                                                                    ---END HERE 
        CASE
            WHEN PrimaryDimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                PrimaryDimEmployee.employee_id
        END CommissionedTeamMember1ID,
        CASE
            WHEN PrimaryDimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                PrimaryDimEmployee.employee_name_last_first
        END CommissionedTeamMember1Name,
        CASE
            WHEN PrimaryDimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
				
                PrimaryEmployeeDimLocation.club_name --formal_club_name
        END CommissionedTeamMember1HomeClub,
        NULL CommissionedTeamMember2ID,                             -- CASE WHEN SecondaryDimEmployee.dim_employee_key in ('-997', '-998', '-999') THEN NULL ELSE SecondaryDimEmployee.employee_id END CommissionedTeamMember2ID,
        NULL CommissionedTeamMember2Name,                           -- CASE WHEN SecondaryDimEmployee.dim_employee_key in ('-997', '-998', '-999') THEN NULL ELSE SecondaryDimEmployee.employee_name_last_first END CommissionedTeamMember2Name,
        NULL CommissionedTeamMember2HomeClub,                       -- CASE WHEN SecondaryDimEmployee.dim_employee_key in ('-997', '-998', '-999') THEN NULL ELSE SecondaryEmployeeDimLocation.formal_club_name END CommissionedTeamMember2HomeClub,

        #MMSTransactions.SalesQuantity,
        #MMSTransactions.SoldNotServiceFlag SNSFlag,
        #MMSTransactions.GrossTransactionAmount,
        #MMSTransactions.TotalDiscountAmount,
        #MMSTransactions.SalesTax,
        #MMSTransactions.TotalAmount,
        #MMSDiscounts.DiscountAmount1,
        #MMSDiscounts.DiscountAmount2,
        #MMSDiscounts.DiscountAmount3,
        #MMSDiscounts.DiscountAmount4,
        #MMSDiscounts.DiscountAmount5,
        #MMSDiscounts.Discount1,
        #MMSDiscounts.Discount2,
        #MMSDiscounts.Discount3,
        #MMSDiscounts.Discount4,
        #MMSDiscounts.Discount5,
        PostDimDate.calendar_date TransactionDate,
       -- #MMSTransactions.SalesChannelDimDescriptionKey SalesChannel,
	    SalesChannelDimDescription.description SalesChannel,
        DimCustomer.membership_id MembershipID,
        #MMSTransactions.CorporateTransferAmount,
        NULL ECommerceShipmentNumber,
        NULL ECommerceOrderNumber,
        NULL ECommerceAutoShipFlag,
        NULL ECommerceOrderEntryTrackingNumber,
        NULL ECommerceProductCost,
        NULL ECommerceShipmentLineNumber,
        NULL ECommerceShippingAndHandlingAmount,
        DimLocation.GLClubID,                                       --getting from DimClubInfo
                                                                    --#MMSTransactions.void_comment TransactionComment, --verify
        #MMSTransactions.TransactionComment,
                                                                    --#MMSTransactions.membership_adjustment_flag MemberRelationsAdjustmentCategory --MemberRelationsAdjustmentCategory
        #MMSTransactions.MemberRelationsAdjustmentCategory
		
  FROM #MMSTransactions --[marketing].[v_fact_mms_transaction_item] #MMSTransactions --#MMSTransactions
        JOIN #DimClubInfo DimLocation 
            ON #MMSTransactions.DimClubKey = DimLocation.DimClubKey --DimLocation.DimLocationKey
        --JOIN vDimDate PostDimDate
        JOIN [marketing].[v_dim_date] PostDimDate
            ON #MMSTransactions.PostDimDateKey = PostDimDate.dim_date_key
        JOIN [marketing].[v_dim_mms_drawer_activity] DimMMSDrawerActivity
            ON #MMSTransactions.DimMMSDrawerActivityKey = DimMMSDrawerActivity.dim_mms_drawer_activity_key
        --JOIN vDimDate DrawerClosedDimDate
        JOIN [marketing].[v_dim_date] DrawerClosedDimDate
            ON DimMMSDrawerActivity.closed_dim_date_key = DrawerClosedDimDate.dim_date_key
        LEFT JOIN [marketing].[v_dim_date] EDWInsertedDimDate --vDimDate EDWInsertedDimDate
            ON #MMSTransactions.UDWInsertedDimDateKey = EDWInsertedDimDate.dim_date_key
        LEFT JOIN [marketing].[v_dim_mms_product] DimProduct --vDimProduct DimProduct   --changed from from v_dim_mms_product since it does not exist in dev 
            ON #MMSTransactions.DimProductKey = DimProduct.dim_mms_product_key
        --AND DimProduct.Package_product_flag = 'Y'    --   AND DimProduct.ActiveInd = 'Y'	
        JOIN [marketing].[v_dim_mms_member] DimCustomer --vDimCustomer DimCustomer
            ON #MMSTransactions.DimMemberKey = DimCustomer.dim_mms_member_key
        --   AND DimCustomer.ActiveInd = 'Y'
        JOIN [marketing].[v_dim_employee] TransactionDimEmployee --JOIN vDimEmployee TransactionDimEmployee
            ON #MMSTransactions.TransactionDimEmployeeKey = TransactionDimEmployee.dim_employee_key
        --  AND TransactionDimEmployee.hire_date <= PostDimDate.calendar_date  --comment verify
        --  AND TransactionDimEmployee.termination_date > PostDimDate.calendar_date  --comment verify
        JOIN [marketing].[v_dim_club] TransactionEmployeeDimLocation --vDimLocationActive TransactionEmployeeDimLocation
            ON TransactionDimEmployee.dim_club_key = TransactionEmployeeDimLocation.dim_club_key
        LEFT JOIN [marketing].[v_dim_employee] PrimaryDimEmployee --vDimEmployee PrimaryDimEmployee
            ON #MMSTransactions.PrimarySalesDimEmployeeKey = PrimaryDimEmployee.dim_employee_key
        LEFT JOIN [marketing].[v_dim_club] PrimaryEmployeeDimLocation --vDimLocationActive PrimaryEmployeeDimLocation
            ON PrimaryDimEmployee.dim_club_key = PrimaryEmployeeDimLocation.dim_club_key
        
        LEFT JOIN [marketing].[v_dim_description] SalesChannelDimDescription --vDimDescriptionActive SalesChannelDimDescription
            ON #MMSTransactions.SalesChannelDimDescriptionKey = SalesChannelDimDescription.dim_description_key
        LEFT JOIN #MMSDiscounts
            ON #MMSTransactions.TranItemID = #MMSDiscounts.TranItemID
		LEFT JOIN #MMSPaymentTypes
			ON #MMSTransactions.MMSTranID = #MMSPaymentTypes.MMSTranID

 
		
END;



IF 'Cafe' IN
   (
       SELECT SalesSource FROM #SalesSourceList
   )
BEGIN
    INSERT INTO #TransactionDetail
    SELECT --DimLocation.region_dim_description_key Region,
        DimLocation.MMSRegion Region,
        DimLocation.ClubCode,                             --DimClubInfo
        DimLocation.ClubName,
        CASE
            WHEN FactCafePOSSalesTransaction.order_refund_flag = 'Y' THEN
                'Refund'
            ELSE
                'Sale'
        END TransactionType,
        NULL MMSPostDate,
        NULL MMSDrawerCloseDate,
        TransactionCloseDimDate.full_date_description CafeCloseDate,
        PostedBusinessStartDimDate.full_date_description CafePostDate,
        NULL ECommerceOrderDate,
        NULL ECommerceShipmentDate,
        EDWDimDate.full_date_description EDWInsertedDate, 
        'Cafe' Source,
        CONVERT(VARCHAR, DimCafeProduct.menu_item_id) SourceProductID,
        DimCafeProduct.menu_item_name ProductDescription,
        NULL PaymentTypes,
        NULL MemberID,
        NULL MemberName,
        CASE
            WHEN FactCafePOSSalesTransaction.item_refund_flag = 'Y' THEN
                'Cafe Refund'
            ELSE
                'Cafe Sale'
        END TransactionReason,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployee.employee_id
        END TransactionTeamMemberID,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployee.employee_name_last_first
        END TransactionTeamMemberName,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployeeDimLocation.club_name
        END TransactionTeamMemberHomeClub,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployee.employee_id
        END CommissionedTeamMember1ID,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployee.employee_name_last_first
        END CommissionedTeamMember1Name,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployeeDimLocation.club_name
        END CommissionedTeamMember1HomeClub,
        NULL CommissionedTeamMember2ID,
        NULL CommissionedTeamMember2Name,
        NULL CommissionedTeamMember2HomeClub,
        FactCafePOSSalesTransaction.item_quantity SalesQuantity,
        NULL SNSFlag,
        SIGN(FactCafePOSSalesTransaction.item_quantity) * FactCafePOSSalesTransaction.item_sales_amount_gross GrossTransactionAmount,
        SIGN(FactCafePOSSalesTransaction.item_quantity) * FactCafePOSSalesTransaction.item_discount_amount TotalDiscountAmount,
        NULL SalesTax,
        SIGN(FactCafePOSSalesTransaction.item_quantity) * FactCafePOSSalesTransaction.item_sales_dollar_amount_excluding_tax TotalAmount,
        NULL DiscountAmount1,
        NULL DiscountAmount2,
        NULL DiscountAmount3,
        NULL DiscountAmount4,
        NULL DiscountAmount5,
        NULL Discount1,
        NULL Discount2,
        NULL Discount3,
        NULL Discount4,
        NULL Discount5,
        TransactionCloseDimDate.calendar_date TransactionDate,
        'Cafe' SalesChannel,
        NULL MembershipID,
        0 CorporateTransferAmount,
        NULL ECommerceShipmentNumber,
        NULL ECommerceOrderNumber,
        NULL ECommerceAutoShipFlag,
        NULL ECommerceOrderEntryTrackingNumber,
        NULL ECommerceProductCost,
        NULL ECommerceShipmentLineNumber,
        NULL ECommerceShippingAndHandlingAmount,
        DimLocation.GLClubID,
        NULL TransactionComment,
        NULL MemberRelationsAdjustmentCategory

    FROM [marketing].[v_fact_cafe_transaction_item] FactCafePOSSalesTransaction -- vFactCafePOSSalesTransaction FactCafePOSSalesTransaction
        JOIN #DimClubInfo DimLocation --#DimLocationKeyList DimLocation changed 
            ON FactCafePOSSalesTransaction.dim_club_key = DimLocation.DimClubKey --DimLocation.DimLocationKey
        JOIN [marketing].[v_dim_date] TransactionCloseDimDate --vDimDate TransactionCloseDimDate
            ON FactCafePOSSalesTransaction.order_close_dim_date_key = TransactionCloseDimDate.dim_date_key
        -- ON FactCafePOSSalesTransaction.TransactionCloseDimDateKey = TransactionCloseDimDate.DimDateKey
        JOIN [marketing].[v_dim_date] PostedBusinessStartDimDate --vDimDate PostedBusinessStartDimDate
            ON FactCafePOSSalesTransaction.posted_business_start_dim_date_key = PostedBusinessStartDimDate.dim_date_key
        JOIN [marketing].[v_dim_cafe_product] DimCafeProduct -- join [marketing].[v_fact_cafe_transaction_item] FactCafePOSSalesTransaction--vDimCafeProduct DimCafeProduct
            ON FactCafePOSSalesTransaction.dim_cafe_product_key = DimCafeProduct.dim_cafe_product_key
        --cut-off 1m20sec: AND DimCafeProduct.menu_item_active_flag = 'Y'   --ActiveInd = 'Y'                           

        JOIN [marketing].[v_dim_date] EDWDimDate --vDimDate EDWDimDate
            ON FactCafePOSSalesTransaction.udw_inserted_dim_date_key = EDWDimDate.dim_date_key
        --above here taken 28 seconds
        JOIN [marketing].[v_dim_employee] DimEmployee --vDimEmployee DimEmployee
            ON FactCafePOSSalesTransaction.order_commissionable_dim_employee_key = DimEmployee.dim_employee_key
        -- AND DimEmployee.employee_active_flag = 'Y' --ActiveInd = 'Y'
        JOIN [marketing].[v_dim_club] DimEmployeeDimLocation 
            ON DimEmployee.dim_club_key = DimEmployeeDimLocation.dim_club_key
    WHERE FactCafePOSSalesTransaction.order_close_dim_date_key >= @ReportStartDimDateKey 
          AND FactCafePOSSalesTransaction.order_close_dim_date_key <= @AdjustedEndDimDateKey 
          AND FactCafePOSSalesTransaction.item_voided_flag = 'N'
          AND
          (
              FactCafePOSSalesTransaction.order_void_flag = 'N'
              OR FactCafePOSSalesTransaction.order_refund_flag = 'Y'
          )
          AND
          (
              (
                  'Sale' IN
                  (
                      SELECT TransactionType FROM #TransactionTypeList
                  )
                  AND '0' IN
                      (
                          SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo
                      )
                  AND FactCafePOSSalesTransaction.order_refund_flag /*item_refund_flag*/ = 'N'
              )
              OR
              (
                  'Refund' IN
                  (
                      SELECT TransactionType FROM #TransactionTypeList
                  )
                  AND '-1' IN
                      (
                          SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo
                      )
                  AND FactCafePOSSalesTransaction.item_refund_flag = 'Y'
              )
          ); --FactCafePOSSalesTransaction.order_refund_flag = 'Y'))


END;


IF 'Magento' IN
   (
       SELECT SalesSource FROM #SalesSourceList
   )
BEGIN
    INSERT INTO #TransactionDetail
    SELECT 
        DimLocation.MMSRegion Region,
        DimLocation.ClubCode,
        DimLocation.ClubName,
        CASE
            WHEN FactMagentoTransaction.refund_flag = 'Y' THEN
                'Refund'
            ELSE
                'Sale'
        END TransactionType,
        NULL MMSPostDate,
        NULL MMSDrawerCloseDate,
        NULL CafeCloseDate,
        NULL CafePostDate,
        OrderDimDate.full_date_description ECommerceOrderDate,
        ShipmentDimDate.full_date_description ECommerceShipmentDate,
        EDWDimDate.full_date_description EDWInsertedDate,
        'Magento' AS Source,                                             -- DimSalesSource.DisplayDescription Source,   
                                                                         -- DimMagentoProduct.sku SourceProductID,
        DimMagentoProduct.product_id SourceProductID,
        DimMagentoProduct.product_name ProductDescription,
        NULL PaymentTypes,
        DimCustomer.member_id MemberID,
        DimCustomer.customer_name_last_first MemberName,
        CASE
            WHEN FactMagentoTransaction.refund_flag = 'Y' THEN
                'E-Commerce Refund' 
            ELSE
                'E-Commerce Sale'
        END TransactionReason,
        DimEmployee.employee_id TransactionTeamMemberID,
        DimEmployee.employee_name_last_first TransactionTeamMemberName,
        DimEmployeeDimLocation.club_name TransactionTeamMemberHomeClub,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployee.employee_id
        END CommissionedTeamMember1ID,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployee.employee_name_last_first
        END CommissionedTeamMember1Name,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployeeDimLocation.club_name
        END CommissionedTeamMember1HomeClub,
        NULL CommissionedTeamMember2ID,
        NULL CommissionedTeamMember2Name,
        NULL CommissionedTeamMember2HomeClub,
        FactMagentoTransaction.transaction_quantity SalesQuantity,       --updated
        NULL SNSFlag,
        (FactMagentoTransaction.product_price * FactMagentoTransaction.transaction_quantity) GrossTransactionAmount,
        (FactMagentoTransaction.transaction_discount_amount + FactMagentoTransaction.transaction_tax_amount)
        * SIGN(FactMagentoTransaction.transaction_quantity) TotalDiscountAmount,
        (FactMagentoTransaction.shipping_tax_amount + FactMagentoTransaction.transaction_tax_amount)
        * SIGN(FactMagentoTransaction.transaction_quantity) SalesTax,
        FactMagentoTransaction.transaction_amount * SIGN(FactMagentoTransaction.transaction_quantity) TotalAmount,
        NULL DiscountAmount1,
        NULL DiscountAmount2,
        NULL DiscountAmount3,
        NULL DiscountAmount4,
        NULL DiscountAmount5,
        NULL Discount1,
        NULL Discount2,
        NULL Discount3,
        NULL Discount4,
        NULL Discount5,
        ShipmentDimDate.calendar_date TransactionDate,
                                                                        
        'Magento' AS SalesChannel,                                       
        CASE
            WHEN DimCustomer.dim_mms_member_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimCustomer.member_id
        END MembershipID,
        0 CorporateTransferAmount,
        NULL ECommerceShipmentNumber,                                    
        FactMagentoTransaction.order_number ECommerceOrderNumber,
        NULL ECommerceAutoShipFlag,                                      
        NULL ECommerceOrderEntryTrackingNumber,                         
                                                                 
        FactMagentoTransaction.product_cost * SIGN(FactMagentoTransaction.transaction_amount) ECommerceProductCost,
        FactMagentoTransaction.batch_number ECommerceShipmentLineNumber,
        FactMagentoTransaction.shipping_amount * SIGN(FactMagentoTransaction.transaction_quantity) ECommerceShippingAndHandlingAmount,
        DimLocation.GLClubID,
        NULL TransactionComment,
        NULL MemberRelationsAdjustmentCategory
    FROM [marketing].[v_fact_magento_transaction_item] FactMagentoTransaction 
        JOIN #DimClubInfo DimLocation 
            ON FactMagentoTransaction.transaction_reporting_dim_club_key = DimLocation.DimClubKey
        JOIN [marketing].[v_dim_date] ShipmentDimDate 
            ON FactMagentoTransaction.invoice_dim_date_key = ShipmentDimDate.dim_date_key
     
        JOIN [marketing].[v_dim_date] OrderDimDate 
            ON FactMagentoTransaction.transaction_dim_date_key = OrderDimDate.dim_date_key
        JOIN [marketing].[v_dim_magento_product] DimMagentoProduct 
            ON FactMagentoTransaction.dim_magento_product_key = DimMagentoProduct.dim_magento_product_key 
      
        LEFT JOIN [marketing].[v_dim_date] EDWDimDate 
            ON FactMagentoTransaction.udw_inserted_dim_date_key = EDWDimDate.dim_date_key 
        JOIN [marketing].[v_dim_employee] DimEmployee 
            ON FactMagentoTransaction.dim_employee_key = DimEmployee.dim_employee_key
  
        JOIN [marketing].[v_dim_club] DimEmployeeDimLocation 
            ON DimEmployee.dim_club_key = DimEmployeeDimLocation.dim_club_key
        JOIN [marketing].[v_dim_mms_member] DimCustomer 
            ON FactMagentoTransaction.dim_mms_member_key = DimCustomer.dim_mms_member_key
               AND DimCustomer.member_active_flag = 'Y'
  
    WHERE FactMagentoTransaction.invoice_dim_date_key >= '20191001' --@ReportStartDimDateKey
          AND FactMagentoTransaction.invoice_dim_date_key <=  '20191201' --@AdjustedEndDimDateKey
          AND
          (
              (
                  'Refund' IN
                  (
                      SELECT TransactionType FROM #TransactionTypeList
                  )
             
				  AND '-5' IN
                      (
                          SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo
                      )
              
                  AND FactMagentoTransaction.transaction_item_amount <> 0
              )
              OR
              (
                  'Sale' IN
                  (
                      SELECT TransactionType FROM #TransactionTypeList
                  )
                  AND '-2' IN
                      (
                          SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo
                      )
         
                  AND FactMagentoTransaction.transaction_item_amount <> 0
              )
          );
END;


IF 'Hybris' IN
   (
       SELECT SalesSource FROM #SalesSourceList
   )
BEGIN
    INSERT INTO #TransactionDetail
    SELECT 
        DimLocation.MMSRegion Region,
        DimLocation.ClubCode,
        DimLocation.ClubName,
        CASE
            WHEN FactHybrisTransaction.refund_flag = 'Y' THEN
                'Refund'
            ELSE
                'Sale'
        END TransactionType,
        NULL MMSPostDate,
        NULL MMSDrawerCloseDate,
        NULL CafeCloseDate,
        NULL CafePostDate,
        OrderDimDate.full_date_description ECommerceOrderDate,
        ShipmentDimDate.full_date_description ECommerceShipmentDate,
        UDWDimDate.standard_date_name EDWInsertedDate,
        'Hybris' AS Source,
        'Hybris ' + DimHybrisProduct.code SourceProductID,
        DimHybrisProduct.description AS ProductDescription,
        NULL PaymentTypes,
        DimCustomer.member_id MemberID,
        DimCustomer.customer_name_last_first MemberName,
        CASE
            WHEN FactHybrisTransaction.refund_flag = 'Y' THEN
                'E-Commerce Refund' 
            ELSE
                'E-Commerce Sale'
        END TransactionReason,
        DimEmployee.employee_id TransactionTeamMemberID,
        DimEmployee.employee_name_last_first TransactionTeamMemberName,
        DimEmployeeDimLocation.club_name TransactionTeamMemberHomeClub,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployee.employee_id
        END CommissionedTeamMember1ID,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployee.employee_name_last_first
        END CommissionedTeamMember1Name,
        CASE
            WHEN DimEmployee.dim_employee_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimEmployeeDimLocation.club_name
        END CommissionedTeamMember1HomeClub,
        NULL CommissionedTeamMember2ID,
        NULL CommissionedTeamMember2Name,
        NULL CommissionedTeamMember2HomeClub,
        FactHybrisTransaction.transaction_quantity SalesQuantity,  --updated
        NULL SNSFlag,
        FactHybrisTransaction.transaction_amount_gross GrossTransactionAmount,
        FactHybrisTransaction.discount_amount TotalDiscountAmount, --verify this selection 
        FactHybrisTransaction.tax_amount SalesTax,
        (FactHybrisTransaction.transaction_amount) * SIGN(FactHybrisTransaction.transaction_quantity) TotalAmount,
        NULL DiscountAmount1,
        NULL DiscountAmount2,
        NULL DiscountAmount3,
        NULL DiscountAmount4,
        NULL DiscountAmount5,
        NULL Discount1,
        NULL Discount2,
        NULL Discount3,
        NULL Discount4,
        NULL Discount5,
        ShipmentDimDate.calendar_date TransactionDate,
                                                                   --  Cast('E-Commerce Vendor - ' + DimSalesSource.Description as Varchar(50)) SalesChannel,
        'Hybris' AS SalesChannel,                                  --  Cast('E-Commerce Vendor - Mangento ' + DimSalesSource.Description as Varchar(50)) SalesChannel,
        CASE
            WHEN DimCustomer.dim_mms_member_key IN ( '-997', '-998', '-999' ) THEN
                NULL
            ELSE
                DimCustomer.member_id
        END MembershipID,
        0 CorporateTransferAmount,
        FactHybrisTransaction.tracking_number ECommerceShipmentNumber,
        NULL ECommerceOrderNumber,                                 --FactMagentoTransaction.order_number  ECommerceOrderNumber,
        FactHybrisTransaction.auto_ship_flag ECommerceAutoShipFlag,
        NULL ECommerceOrderEntryTrackingNumber,                    -- FactMagentoTransaction.ordered_number  ECommerceOrderEntryTrackingNumber,
                                                                   -- DimECommerceProduct.ProductCost * Sign(FactECommerceSalesTransaction.transaction_amount)  ECommerceProductCost,
        (DimHybrisProduct.product_cost) * SIGN(FactHybrisTransaction.transaction_amount) ECommerceProductCost,
        NULL ECommerceShipmentLineNumber,                          -- FactHybrisTransaction.batch_number ECommerceShipmentLineNumber, -- FactECommerceSalesTransaction.ShipmentLineNumber  ECommerceShipmentLineNumber,
        (FactHybrisTransaction.shipping_and_handling_amount) * SIGN(FactHybrisTransaction.transaction_quantity) ECommerceShippingAndHandlingAmount,
        DimLocation.GLClubID,
        NULL TransactionComment,
        NULL MemberRelationsAdjustmentCategory
    FROM [marketing].[v_fact_hybris_transaction_item] FactHybrisTransaction
        JOIN #DimClubInfo DimLocation --#DimLocationKeyList DimLocation 
            ON FactHybrisTransaction.transaction_reporting_dim_club_key = DimLocation.DimClubKey
        JOIN [marketing].[v_dim_date] ShipmentDimDate --vDimDate ShipmentDimDate
            ON FactHybrisTransaction.settlement_dim_date_key = ShipmentDimDate.dim_date_key
        
        JOIN [marketing].[v_dim_date] OrderDimDate 
            ON FactHybrisTransaction.order_dim_date_key = OrderDimDate.dim_date_key
        JOIN [marketing].[v_dim_date] UDWDimDate
            ON FactHybrisTransaction.udw_inserted_dim_date_key = UDWDimDate.dim_date_key
        JOIN [marketing].[v_dim_hybris_product] DimHybrisProduct 
            ON FactHybrisTransaction.dim_hybris_product_key = DimHybrisProduct.dim_hybris_product_key 
       
        JOIN [marketing].[v_dim_employee] DimEmployee 
            ON FactHybrisTransaction.sales_dim_employee_key = DimEmployee.dim_employee_key
     
        JOIN [marketing].[v_dim_club] DimEmployeeDimLocation 
            ON DimEmployee.dim_club_key = DimEmployeeDimLocation.dim_club_key
        JOIN [marketing].[v_dim_mms_member] DimCustomer 
            ON FactHybrisTransaction.dim_mms_member_key = DimCustomer.dim_mms_member_key
  
    WHERE FactHybrisTransaction.settlement_dim_date_key >= @ReportStartDimDateKey
          AND FactHybrisTransaction.settlement_dim_date_key <= @AdjustedEndDimDateKey
          AND
          (
              (
                  'Sale' IN
                  (
                      SELECT TransactionType FROM #TransactionTypeList
                  )
                  AND '-2' IN
                      (
                          SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo
                      )
                
                  AND FactHybrisTransaction.refund_flag = 'N'
              )
              OR
              (
                  'Refund' IN
                  (
                      SELECT TransactionType FROM #TransactionTypeList
                  )
                  AND '-5' IN
                      (
                          SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo
                      )
            
                  AND FactHybrisTransaction.refund_flag = 'Y'
              )
          );
END;




IF 'HealthCheckUSA' IN
   (
       SELECT SalesSource FROM #SalesSourceList
   )
BEGIN
    INSERT INTO #TransactionDetail
    SELECT DimLocation.MMSRegion Region,
           DimLocation.ClubCode,
           DimLocation.ClubName,
           FactHealthCheckUSASalesTransactionItem.transaction_type TransactionType,
           NULL MMSPostDate,
           NULL MMSDrawerCloseDate,
           NULL CafeCloseDate,
           NULL CafePostDate,
           TransactionPostDimdate.standard_date_name ECommerceOrderDate,
           TransactionPostDimdate.standard_date_name ECommerceShipmentDate,
           EDWDimDate.standard_date_name EDWInsertedDate,
                                                                                                                          --   DimSalesSource.DisplayDescription Source,  --referred to below assumption column - verify
           'HealthCheckUSA' Source,
           DimHCUSAProduct.product_sku SourceProductID,
           DimHCUSAProduct.product_description ProductDescription,
           NULL PaymentTypes,
                                                                                                                          -- DimCustomer.MemberID,																				--ref below for assumption alias
           NULL MemberID,
                                                                                                                          -- DimCustomer.CustomerNameLastFirst MemberName,														--ref below for assumption alias
           '' AS MemberName,
           CASE
               WHEN FactHealthCheckUSASalesTransactionItem.refund_flag = 'Y' THEN
                   'HealthCheckUSA Refund'
               ELSE
                   'HealthCheckUSA Sale'
           END TransactionReason,
           DimEmployee.employee_id TransactionTeamMemberID,
           DimEmployee.employee_name_last_first TransactionTeamMemberName,
           DimEmployeeDimLocation.club_name TransactionTeamMemberHomeClub,
           CASE
               WHEN FactHealthCheckUSASalesTransactionItem.order_for_employee_flag = 'Y' THEN
                   DimEmployee.employee_id
               ELSE
                   NULL
           END CommissionedTeamMember1ID,
           CASE
               WHEN FactHealthCheckUSASalesTransactionItem.order_for_employee_flag = 'Y' THEN
                   DimEmployee.employee_name_last_first
               ELSE
                   NULL
           END CommissionedTeamMember1Name,
           CASE
               WHEN FactHealthCheckUSASalesTransactionItem.order_for_employee_flag = 'Y' THEN
                   DimEmployeeDimLocation.club_name
               ELSE
                   NULL
           END CommissionedTeamMember1HomeClub,
           NULL CommissionedTeamMember2ID,
           NULL CommissionedTeamMember2Name,
           NULL CommissionedTeamMember2HomeClub,
           FactHealthCheckUSASalesTransactionItem.sales_quantity,
           NULL SNSFlag,
                                                                                                                          -- FactHealthCheckUSASalesTransactionItem.SalesAmountGross  GrossTransactionAmount,     --below re-written to suffice logic of GrossTransactionAmount
           FactHealthCheckUSASalesTransactionItem.sales_amount
           + ISNULL(FactHealthCheckUSASalesTransactionItem.discount_amount, 0) AS GrossTransactionAmount,
           FactHealthCheckUSASalesTransactionItem.discount_amount TotalDiscountAmount,
           NULL SalesTax,
           FactHealthCheckUSASalesTransactionItem.sales_amount TotalAmount,
           NULL DiscountAmount1,
           NULL DiscountAmount2,
           NULL DiscountAmount3,
           NULL DiscountAmount4,
           NULL DiscountAmount5,
           NULL Discount1,
           NULL Discount2,
           NULL Discount3,
           NULL Discount4,
           NULL Discount5,
           TransactionPostDimdate.calendar_date TransactionDate,
                                                                                                                        
           CAST('E-Commerce Vendor - HealthCheckUSA ' + DimHCUSAProduct.product_description AS VARCHAR(50)) SalesChannel, 
           NULL MembershipID,
                                                                                                                          
           0 CorporateTransferAmount,
           NULL ECommerceShipmentNumber,
           NULL ECommerceOrderNumber,
           NULL ECommerceAutoShipFlag,
           NULL ECommerceOrderEntryTrackingNumber,
           NULL ECommerceProductCost,
           NULL ECommerceShipmentLineNumber,
           NULL ECommerceShippingAndHandlingAmount,
           DimLocation.GLClubID,
           NULL TransactionComment,
           NULL MemberRelationsAdjustmentCategory
    FROM [marketing].[v_fact_healthcheckusa_transaction_item] FactHealthCheckUSASalesTransactionItem
        LEFT JOIN #DimClubInfo DimLocation 
            ON FactHealthCheckUSASalesTransactionItem.transaction_reporting_dim_club_key = DimLocation.DimClubKey
        JOIN [marketing].[v_dim_date] TransactionPostDimdate --vDimDate TransactionPostDimDate
            ON FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key = TransactionPostDimdate.dim_date_key
        JOIN [marketing].[v_dim_healthcheckusa_product] DimHCUSAProduct --vDimEcommerceProduct DimECommerceProduct
            ON FactHealthCheckUSASalesTransactionItem.dim_healthcheckusa_product_key = DimHCUSAProduct.dim_healthcheckusa_product_key
      
        JOIN [marketing].[v_dim_date] EDWDimDate --vDimDate EDWDimDate
            ON FactHealthCheckUSASalesTransactionItem.udw_inserted_dim_date_key = EDWDimDate.dim_date_key
        JOIN [marketing].[v_dim_employee] DimEmployee --vDimEmployee DimEmployee
            ON FactHealthCheckUSASalesTransactionItem.sales_dim_employee_key = DimEmployee.dim_employee_key
   
        JOIN [marketing].[v_dim_club] DimEmployeeDimLocation 
            ON DimEmployee.dim_club_key = DimEmployeeDimLocation.dim_club_key
  

   
    WHERE FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key >= @ReportStartDimDateKey
          AND FactHealthCheckUSASalesTransactionItem.transaction_post_dim_date_key <= @AdjustedEndDimDateKey
          AND
          (
              (
                  'Sale' IN
                  (
                      SELECT TransactionType FROM #TransactionTypeList
                  )
                  AND '-3' IN
                      (
                          SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo
                      )
                  AND FactHealthCheckUSASalesTransactionItem.refund_flag = 'N'
              )
              OR
              (
                  'Refund' IN
                  (
                      SELECT TransactionType FROM #TransactionTypeList
                  )
                  AND '-4' IN
                      (
                          SELECT DimTransactionReasonKey FROM #DimTransactionReasonInfo
                      )
                  AND FactHealthCheckUSASalesTransactionItem.refund_flag = 'Y'
              )
          );
END;


IF OBJECT_ID('tempdb.dbo.#MembershipDues', 'U') IS NOT NULL
    DROP TABLE #MembershipDues;
--------*****************---------------

IF OBJECT_ID('tempdb.dbo.#TransactionMembershipIDs', 'U') IS NOT NULL
    DROP TABLE #TransactionMembershipIDs;
----- Find all transaction membershipIDs 
SELECT MembershipID
INTO #TransactionMembershipIDs
FROM #TransactionDetail
GROUP BY MembershipID;

---- find report end date month's dues assessment
SELECT MembershipDuesTransactions.membership_id MembershipID,
       SUM(MembershipDuesTransactions.sales_dollar_amount) AS MembershipDuesAssessed
INTO #MembershipDues
FROM [marketing].[v_fact_mms_transaction_item] MembershipDuesTransactions --vFactSalesTransaction MembershipDuesTransactions
    JOIN #TransactionMembershipIDs TranMembershipIDs
        ON MembershipDuesTransactions.membership_id = TranMembershipIDs.MembershipID
    JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason --vDimTransactionReasonActive DimTransactionReason
        ON MembershipDuesTransactions.dim_mms_transaction_reason_key = DimTransactionReason.dim_mms_transaction_reason_key
WHERE DimTransactionReason.reason_code_id = 28 ------ MonthlyDuesAssessment
      AND MembershipDuesTransactions.post_dim_date_key >= @AdjustedEndDate_1stOfMonthDimDateKey
      AND MembershipDuesTransactions.post_dim_date_key <= @AdjustedEndDate_2ndOfMonthDimDateKey
      AND @AdjustedEndDimDateKey >= @ReportStartDimDateKey
GROUP BY MembershipDuesTransactions.membership_id;

---- find report end date month's Jr. dues assessment
IF OBJECT_ID('tempdb.dbo.#MembershipJuniorDues', 'U') IS NOT NULL
    DROP TABLE #MembershipJuniorDues;

SELECT JuniorDuesTransactions.membership_id MembershipID,
       SUM(JuniorDuesTransactions.sales_dollar_amount) AS JuniorDuesAssessed
INTO #MembershipJuniorDues
FROM [marketing].[v_fact_mms_transaction_item] JuniorDuesTransactions --vFactSalesTransaction JuniorDuesTransactions
    JOIN #TransactionMembershipIDs TranMembershipIDs
        ON JuniorDuesTransactions.membership_id = TranMembershipIDs.MembershipID
    JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason --vDimTransactionReasonActive DimTransactionReason
        ON JuniorDuesTransactions.dim_mms_transaction_reason_key = DimTransactionReason.dim_mms_transaction_reason_key
WHERE DimTransactionReason.reason_code_id = 125 ------ JrDuesAssessment
      AND JuniorDuesTransactions.post_dim_date_key >= @AdjustedEndDate_1stOfMonthDimDateKey
      AND JuniorDuesTransactions.post_dim_date_key <= @AdjustedEndDate_2ndOfMonthDimDateKey
      AND @AdjustedEndDimDateKey >= @ReportStartDimDateKey
GROUP BY JuniorDuesTransactions.membership_id;




SELECT #TransactionDetail.Region,
       #TransactionDetail.ClubCode,
       #TransactionDetail.ClubName,
       #TransactionDetail.TransactionType,
       #TransactionDetail.MMSPostDate,
       #TransactionDetail.MMSDrawerCloseDate,
       #TransactionDetail.CafeCloseDate,
       #TransactionDetail.CafePostDate,
       #TransactionDetail.ECommerceOrderDate,
       #TransactionDetail.ECommerceShipmentDate,
       #TransactionDetail.EDWInsertedDate,
       #TransactionDetail.Source,
       #TransactionDetail.SourceProductID,
       #TransactionDetail.ProductDescription,
       #TransactionDetail.PaymentTypes,
       #TransactionDetail.MemberID,
       #TransactionDetail.MemberName,
       #TransactionDetail.TransactionReason,
       #TransactionDetail.TransactionTeamMemberID,
       #TransactionDetail.TransactionTeamMemberName,
       #TransactionDetail.TransactionTeamMemberHomeClub,
       #TransactionDetail.CommissionedTeamMember1ID,
       #TransactionDetail.CommissionedTeamMember1Name,
       #TransactionDetail.CommissionedTeamMember1HomeClub,
       #TransactionDetail.CommissionedTeamMember2ID,
       #TransactionDetail.CommissionedTeamMember2Name,
       #TransactionDetail.CommissionedTeamMember2HomeClub,
       #TransactionDetail.SalesQuantity,
       #TransactionDetail.SNSFlag,
       #TransactionDetail.GrossTransactionAmount,
       #TransactionDetail.TotalDiscountAmount,
       #TransactionDetail.SalesTax,
       #TransactionDetail.TotalAmount,
       #TransactionDetail.DiscountAmount1,
       #TransactionDetail.DiscountAmount2,
       #TransactionDetail.DiscountAmount3,
       #TransactionDetail.DiscountAmount4,
       #TransactionDetail.DiscountAmount5,
       #TransactionDetail.Discount1,
       #TransactionDetail.Discount2,
       #TransactionDetail.Discount3,
       #TransactionDetail.Discount4,
       #TransactionDetail.Discount5,
       'Local Currency' ReportingCurrencyCode,
       @ReportRunDateTime ReportRunDateTime,
       #TransactionDetail.TransactionDate,
       @HeaderDateRange HeaderDateRange,
       @SalesSourceCommaList HeaderSourceList,
       @TransactionTypeCommaList HeaderTransactionTypeList,
       @HeaderTransactionReason HeaderTransactionReason,
       1 RecordCount,
       CAST('' AS VARCHAR(71)) HeaderEmptyResult,
       SalesChannel,
       TotalAmount + ISNULL(SalesTax, 0) TotalAmountAfterTax,
       #TransactionDetail.CorporateTransferAmount,
       #TransactionDetail.ECommerceShipmentNumber,
       #TransactionDetail.ECommerceOrderNumber,
       #TransactionDetail.ECommerceAutoShipFlag,
       #TransactionDetail.ECommerceOrderEntryTrackingNumber,
       #TransactionDetail.ECommerceProductCost,
       #TransactionDetail.ECommerceShipmentLineNumber,
       #TransactionDetail.ECommerceShippingAndHandlingAmount,
       #TransactionDetail.GLClubID,
       #TransactionDetail.TransactionComment,
       MembershipDues.MembershipDuesAssessed,
       JuniorDues.JuniorDuesAssessed,
       #TransactionDetail.MemberRelationsAdjustmentCategory
FROM #TransactionDetail
    LEFT JOIN [marketing].[v_dim_mms_membership_history] FactMembership --vFactMembership FactMembership  note: used the membership_history due to having the effective and exp date time
        ON #TransactionDetail.MembershipID = FactMembership.membership_id
           AND FactMembership.effective_date_time <= @ReportEndDate
           AND FactMembership.expiration_date_time > @ReportEndDate
    LEFT JOIN #MembershipDues MembershipDues
        ON #TransactionDetail.MembershipID = MembershipDues.MembershipID
    LEFT JOIN #MembershipJuniorDues JuniorDues
        ON #TransactionDetail.MembershipID = JuniorDues.MembershipID
    LEFT JOIN [marketing].[v_dim_mms_membership_type] DimProduct --vDimProductActive DimProduct
       
        ON FactMembership.dim_mms_membership_type_key = DimProduct.dim_mms_product_key 
WHERE
(
    SELECT COUNT(*) FROM #TransactionDetail
)   > 0
AND
(
    #TransactionDetail.MembershipID IS NULL
    OR
    (
        @MembershipFilter = 'All Memberships'
        OR
        (
            @MembershipFilter = 'All Memberships - exclude Founders'
            AND DimProduct.attribute_founders_flag = 'N'
        )
        OR
        (
            @MembershipFilter = 'Employee Memberships'
            AND DimProduct.attribute_employee_membership_flag = 'Y'
        )
        OR
        (
            @MembershipFilter = 'Corporate Memberships'
            AND FactMembership.corporate_membership_flag = 'Y'
        )
    )
)
UNION ALL
SELECT CAST(NULL AS VARCHAR(50)) Region,
       CAST(NULL AS VARCHAR(18)) ClubCode,
       CAST(NULL AS VARCHAR(50)) ClubName,
       CAST(NULL AS VARCHAR(10)) TransactionType,
       CAST(NULL AS VARCHAR(12)) MMSPostDate,
       CAST(NULL AS VARCHAR(12)) MMSDrawerCloseDate,
       CAST(NULL AS VARCHAR(12)) CafeCloseDate,
       CAST(NULL AS VARCHAR(12)) CafePostDate,
       CAST(NULL AS VARCHAR(12)) ECommerceOrderDate,
       CAST(NULL AS VARCHAR(12)) ECommerceShipmentDate,
       CAST(NULL AS VARCHAR(12)) EDWInsertedDate,
       CAST(NULL AS VARCHAR(50)) Source,
       CAST(NULL AS VARCHAR(61)) SourceProductID,
       CAST(NULL AS VARCHAR(255)) ProductDescription,
       CAST(NULL AS VARCHAR(4000)) PaymentTypes,
       NULL MemberID,
       CAST(NULL AS VARCHAR(132)) MemberName,
       CAST(NULL AS VARCHAR(50)) TransactionReason,
       NULL TransactionTeamMemberID,
       CAST(NULL AS VARCHAR(102)) TransactionTeamMemberName,
       CAST(NULL AS VARCHAR(50)) TransactionTeamMemberHomeClub,
       NULL CommissionedTeamMember1ID,
       CAST(NULL AS VARCHAR(102)) CommissionedTeamMember1Name,
       CAST(NULL AS VARCHAR(50)) CommissionedTeamMember1HomeClub,
       NULL CommissionedTeamMember2ID,
       CAST(NULL AS VARCHAR(102)) CommissionedTeamMember2Name,
       CAST(NULL AS VARCHAR(50)) CommissionedTeamMember2HomeClub,
       NULL SalesQuantity,
       CAST(NULL AS CHAR(1)) SNSFlag,
       CAST(NULL AS DECIMAL(12, 2)) GrossTransactionAmount,
       CAST(NULL AS DECIMAL(12, 2)) TotalDiscountAmount,
       CAST(NULL AS DECIMAL(12, 2)) SalesTax,
       CAST(NULL AS DECIMAL(12, 2)) TotalAmount,
       CAST(NULL AS DECIMAL(12, 2)) DiscountAmount1,
       CAST(NULL AS DECIMAL(12, 2)) DiscountAmount2,
       CAST(NULL AS DECIMAL(12, 2)) DiscountAmount3,
       CAST(NULL AS DECIMAL(12, 2)) DiscountAmount4,
       CAST(NULL AS DECIMAL(12, 2)) DiscountAmount5,
       CAST(NULL AS VARCHAR(50)) Discount1,
       CAST(NULL AS VARCHAR(50)) Discount2,
       CAST(NULL AS VARCHAR(50)) Discount3,
       CAST(NULL AS VARCHAR(50)) Discount4,
       CAST(NULL AS VARCHAR(50)) Discount5,
       'Local Currency' ReportingCurrencyCode,
       CAST(@ReportRunDateTime AS VARCHAR(21)) ReportRunDateTime,
       CAST(NULL AS DATETIME) TransactionDate,
       CAST(@HeaderDateRange AS VARCHAR(51)) HeaderDateRange,
       CAST(@SalesSourceCommaList AS VARCHAR(4000)) HeaderSourceList,
       CAST(@TransactionTypeCommaList AS VARCHAR(4000)) HeaderTransactionTypeList,
       CAST(@HeaderTransactionReason AS VARCHAR(102)) HeaderTransactionReason,
       0 RecordCount,
       'There is no data available for the selected parameters.  Please re-try.' HeaderEmptyResult,
       CAST(NULL AS VARCHAR(50)) SalesChannel,
       CAST(NULL AS DECIMAL(12, 2)) TotalAmountAfterTax,
       CAST(NULL AS DECIMAL(12, 2)) CorporateTransferAmount,
       CAST(NULL AS VARCHAR(255)) ECommerceShipmentNumber,
       CAST(NULL AS INT) ECommerceOrderNumber,
       CAST(NULL AS CHAR(1)) ECommerceAutoShipFlag,
       CAST(NULL AS VARCHAR(255)) ECommerceOrderEntryTrackingNumber,
       CAST(NULL AS DECIMAL(12, 2)) ECommerceProductCost,
       CAST(NULL AS INT) ECommerceShipmentLineNumber,
       CAST(NULL AS DECIMAL(12, 2)) ECommerceShippingAndHandlingAmount,
       CAST(NULL AS INT) GLClubID,
       CAST(NULL AS VARCHAR(255)) TransactionComment,
       CAST(NULL AS DECIMAL(12, 2)) MembershipDuesAssessed,
       CAST(NULL AS DECIMAL(12, 2)) JuniorDuesAssessed,
       CAST(NULL AS VARCHAR(50)) MemberRelationsAdjustmentCategory
WHERE
(
    SELECT COUNT(*)  FROM #TransactionDetail
)   = 0; 
DROP TABLE #MMSTransactions
DROP TABLE #DimClubInfo
DROP TABLE #FactMMSPayment
DROP TABLE #MMSPaymentTypes
DROP TABLE #MMSDiscounts
DROP TABLE #DimLocationInfo
DROP TABLE #SalesSourceList
DROP TABLE #TransactionTypeList
DROP TABLE #DimTransactionReasonInfo
DROP TABLE #TransactionDetail
DROP TABLE #TransactionMembershipIDs
DROP TABLE #MembershipDues
DROP TABLE #MembershipJuniorDues
	




END
