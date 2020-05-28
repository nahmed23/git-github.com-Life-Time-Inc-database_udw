CREATE PROC [reporting].[proc_PromptOperationsPostingRevenueCategory] AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END



SELECT DISTINCT [revenue_category] AS RevenueCategory
FROM [marketing].[v_dim_mms_product_history]
WHERE [revenue_category] <> ''

END

