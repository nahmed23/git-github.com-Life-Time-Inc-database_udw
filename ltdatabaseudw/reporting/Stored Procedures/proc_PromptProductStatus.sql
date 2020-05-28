CREATE PROC [reporting].[proc_PromptProductStatus] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


 -----------
 ------ This stored procedure returns the distinct list MMS product statuses from all the products in v_dim_mms_product.
 ------ Execution Sample:
 ------ Exec [reporting].[proc_PromptProductStatus]
 -----------


SELECT IsNull(product_status,'Unknown') AS product_status
FROM [marketing].[v_dim_mms_product]
GROUP BY product_status

END
