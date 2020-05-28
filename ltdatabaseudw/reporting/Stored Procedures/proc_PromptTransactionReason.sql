CREATE PROC [reporting].[proc_PromptTransactionReason] AS  
BEGIN   
SET XACT_ABORT ON  
SET NOCOUNT ON  

IF OBJECT_ID('tempdb.dbo.#AvailableTranTypes', 'U') IS NOT NULL
 DROP TABLE #AvailableTranTypes; 
  
SELECT DISTINCT marketing.v_dim_mms_transaction_reason.Description TransactionReason,  
                marketing.v_dim_mms_transaction_reason.Reason_Code_ID  
  INTO #AvailableTranTypes
  FROM marketing.v_dim_mms_transaction_reason 
UNION  
SELECT 'Cafe Sale',  
       0  
UNION  
SELECT 'Cafe Refund',   
       -1  
UNION  
SELECT 'E-Commerce Sale', -2  
UNION  
SELECT 'HealthCheckUSA Sale', -3  
UNION  
SELECT 'HealthCheckUSA Refund', -4  
UNION  
SELECT 'E-Commerce Refund', -5  
  
DECLARE @TransactionReasonCount INT  
SELECT @TransactionReasonCount = COUNT(*)  
FROM #AvailableTranTypes
  
SELECT TransactionReason,   
       Reason_Code_ID TransactionReasonCodeID,   
       @TransactionReasonCount TotalTransactionReasonCount  
FROM #AvailableTranTypes

DROP TABLE #AvailableTranTypes
  
END


