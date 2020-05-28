CREATE PROC [reporting].[proc_PromptProductGLAccountNumber] AS
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
SET FMTONLY OFF
END

SELECT DISTINCT gl_account_number ProductGLAccountNumber
FROM marketing.v_dim_mms_product --old view and columnn vDimProduct.ProductGLAccount
WHERE gl_account_number <> ''

END
