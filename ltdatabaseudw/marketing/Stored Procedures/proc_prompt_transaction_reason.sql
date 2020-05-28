CREATE PROC [marketing].[proc_prompt_transaction_reason] AS  
BEGIN   
SET XACT_ABORT ON  
SET NOCOUNT ON  


if object_id('tempdb..#available_tran_types') is not null
drop table #available_tran_types

SELECT DISTINCT DimTransactionReason.Description transaction_reason,  
                DimTransactionReason.reason_code_id  
INTO #available_tran_types  
FROM marketing.v_dim_mms_transaction_reason DimTransactionReason
UNION  
SELECT 'Cafe Sale',   0  
UNION  
SELECT 'Cafe Refund',   -1  
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
FROM #available_tran_types  
  
SELECT transaction_reason,   
       Reason_Code_ID transaction_reason_code_id ,   
       @TransactionReasonCount total_transaction_reason_count  
FROM #available_tran_types  


END