CREATE PROC [reporting].[proc_PromptSalesSourceList] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


----- Execution Sample
--- Exec [reporting].[proc_PromptSalesSourceList] 
-----


SELECT 'Cafe' SalesSource, 1 SortOrder
UNION
SELECT 'Hybris' SalesSource, 2 SortOrder
UNION
SELECT 'HealthCheckUSA' SalesSource, 3 SortOrder
UNION
SELECT 'Magento' SalesSource, 4 SortOrder
UNION
SELECT 'MMS' SalesSource, 5 SortOrder
ORDER BY SortOrder


END
