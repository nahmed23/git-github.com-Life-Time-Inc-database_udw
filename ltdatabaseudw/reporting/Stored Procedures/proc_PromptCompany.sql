CREATE PROC [reporting].[proc_PromptCompany] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


---- JIRA : REP-5945
---- EXEC [reporting].[proc_PromptCompany] 

SELECT [company_id]
      ,[company_name] + '-' + [corporate_code] CompanyNameDashCorporateCode
       FROM [marketing].[v_dim_mms_company]
  where invoice_flag = 'Y'
  order by 1

END
