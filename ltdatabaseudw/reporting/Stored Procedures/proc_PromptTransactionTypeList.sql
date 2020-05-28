CREATE PROC [reporting].[proc_PromptTransactionTypeList] AS  
BEGIN   
SET XACT_ABORT ON  
SET NOCOUNT ON  
  
SELECT 'Adjustment' TransactionType, 1 SortOrder  
UNION  
SELECT 'Charge' TransactionType, 2 SortOrder  
UNION  
SELECT 'Payment' TransactionType, 3 SortOrder  
UNION  
SELECT 'Refund' TransactionType, 4 SortOrder  
UNION  
SELECT 'Sale' TransactionType, 5 SortOrder  
ORDER BY SortOrder  
  
END 
