CREATE PROC [dbo].[proc_PromptInvoiceFlagCompany] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON


 SELECT Corporate_code CorporateCode,
		company_name + ' - ' + corporate_code CompanyNameCorporateCode
  FROM marketing.v_dim_mms_company
  WHERE invoice_flag = 'Y'

END
