CREATE PROC [reporting].[proc_PromptOperationsCorporatePartnerProgramAndCompanyForPartnerTypeAndDateRange] @ReportStartDate [DATETIME],@ReportEndDate [DATETIME],@CorporatePartnerTypeList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


---- Execution Sample
-- Exec [reporting].[proc_PromptOperationsCorporatePartnerProgramAndCompanyForPartnerTypeAndDateRange] '1/1/2016','6/30/2016','Invoicing Program|Member Advantage Program'
----

DECLARE @StartDimDateKey Varchar(32)
DECLARE @EndDimDateKey Varchar(32)
SET @StartDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = CAST(@ReportStartDate AS DATE))
SET @EndDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date] WHERE calendar_date = CAST(@ReportEndDate AS DATE))

 ----- Create Program Type temp table   
IF OBJECT_ID('tempdb.dbo.#CorporatePartnerTypeDimDescriptionKeyList', 'U') IS NOT NULL DROP TABLE #CorporatePartnerTypeDimDescriptionKeyList;   
create table #CorporatePartnerTypeDimDescriptionKeyList with (distribution = round_robin, heap) as

SELECT DimDescription.dim_description_key CorporatePartnerTypeDimDescriptionKey,
DimDescription.description AS CorporatePartnerProgramType
FROM [marketing].[v_dim_description] DimDescription 
WHERE '|'+ @CorporatePartnerTypeList +'|' like '%|'+description+'|%'
  AND DimDescription.source_object = 'r_mms_val_reimbursement_program_type'
GROUP BY DimDescription.dim_description_key,DimDescription.description


SELECT DISTINCT
       #CorporatePartnerTypeDimDescriptionKeyList.CorporatePartnerProgramType AS CorporatePartnerProgramTypeDescription,
       DimCompany.company_name + ' - ' + DimCompany.corporate_code AS CompanyNameDashCorporateCode,
       DimCompany.company_name AS CompanyName,
       DimCompany.corporate_code AS CorporateCode,
       DimCompany.company_id AS CompanyID,
       DimCompany.dim_mms_company_key AS DimCompanyKey,
       DimReimbursementProgram.program_name AS ProgramName,
       DimReimbursementProgram.reimbursement_program_id AS ReimbursementProgramID,
       DimReimbursementProgram.dim_mms_reimbursement_program_key AS DimReimbursementProgramKey
  FROM [marketing].[v_fact_mms_member_reimbursement_program] FactMemberReimbursementProgram
  JOIN [marketing].[v_dim_mms_reimbursement_program] DimReimbursementProgram
    ON FactMemberReimbursementProgram.dim_mms_reimbursement_program_key = DimReimbursementProgram.dim_mms_reimbursement_program_key
  JOIN #CorporatePartnerTypeDimDescriptionKeyList
    ON DimReimbursementProgram.program_type_dim_description_key = #CorporatePartnerTypeDimDescriptionKeyList.CorporatePartnerTypeDimDescriptionKey
  JOIN [marketing].[v_dim_mms_company] DimCompany
    ON DimReimbursementProgram.dim_mms_company_key = DimCompany.dim_mms_company_key
 WHERE FactMemberReimbursementProgram.enrollment_dim_date_key <= @EndDimDateKey     ------ Only way to limit the programs to the ones active during the selected date range
   AND FactMemberReimbursementProgram.termination_dim_date_key > @StartDimDateKey 
         


DROP TABLE #CorporatePartnerTypeDimDescriptionKeyList


END
