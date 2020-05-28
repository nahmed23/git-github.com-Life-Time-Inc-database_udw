CREATE PROC [reporting].[proc_PromptOperationsCorporatePartnerProgramAndCompanyForPartnerTypeAndYearMonth] @FourDigitYearDashTwoDigitMonth [CHAR](7),@ProgramTypeDimDescriptionKeyList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON


DECLARE @StartDate DATETIME,
        @EndDate DATETIME,
        @StartDimDateKey INT,
        @EndDimDateKey INT
SELECT @StartDate = MIN(Calendar_Date),
       @EndDate = MAX(Calendar_Date),
       @StartDimDateKey = MIN(Dim_Date_Key),
       @EndDimDateKey = MAX(Dim_Date_Key)
 FROM [marketing].[v_Dim_Date] vDimDate --FROM vDimDate
 WHERE Four_Digit_Year_Dash_Two_Digit_Month = @FourDigitYearDashTwoDigitMonth

---------------------------------------------------------
--   PROGRAM TYPE DIM DESCRIPTION KEY LIST temp table ---
---------------------------------------------------------
IF OBJECT_ID('tempdb.dbo.#ProgramTypeDimDescriptionKeyList', 'U') IS NOT NULL 
	DROP TABLE #ProgramTypeDimDescriptionKeyList;

DECLARE @list_table VARCHAR(100) 
SET @list_table = 'partner_list'
EXEC [marketing].[proc_parse_pipe_list] @ProgramTypeDimDescriptionKeyList, @list_table

SELECT Item ProgramTypeDimDescriptionKey
  INTO #ProgramTypeDimDescriptionKeyList
FROM #partner_list 


SELECT DISTINCT
       DimCompany.Company_Name + ' - ' + DimCompany.Corporate_Code CompanyNameDashCorporateCode,
       Cast(DimCompany.Dim_MMS_Company_Key as Varchar) DimCompanyKey,
       DimReimbursementProgram.[dim_mms_reimbursement_program_key] DimReimbursementProgramKey,
       DimReimbursementProgram.[program_name] ProgramName,
       CorporatePartnerProgramTypeDimDescription.Description CorporatePartnerProgramTypeDescription
	   
  FROM [marketing].[v_fact_mms_member_reimbursement_program] FactMemberReimbursementProgram 
  JOIN [marketing].[v_dim_mms_reimbursement_program] DimReimbursementProgram 
    ON FactMemberReimbursementProgram.[dim_mms_reimbursement_program_key] = DimReimbursementProgram.[dim_mms_reimbursement_program_key]
   AND DimReimbursementProgram.program_active_flag = 'Y'  
  -- AND DimReimbursementProgram.EffectiveDate <= @EndDate
  -- AND DimReimbursementProgram.ExpirationDate > @StartDate
  JOIN  #ProgramTypeDimDescriptionKeyList  
    ON DimReimbursementProgram.Program_Type_Dim_Description_Key = #ProgramTypeDimDescriptionKeyList.ProgramTypeDimDescriptionKey

  JOIN [marketing].[v_dim_description] CorporatePartnerProgramTypeDimDescription 
    ON DimReimbursementProgram.Program_Type_Dim_Description_Key = CorporatePartnerProgramTypeDimDescription.Dim_Description_Key
  JOIN [marketing].[v_dim_mms_company] DimCompany 
    ON DimReimbursementProgram.Dim_MMS_Company_Key = DimCompany.Dim_MMS_Company_Key
 WHERE FactMemberReimbursementProgram.Enrollment_Dim_Date_Key <= @EndDimDateKey
   AND FactMemberReimbursementProgram.Termination_Dim_Date_Key > @StartDimDateKey

DROP TABLE #ProgramTypeDimDescriptionKeyList

END
