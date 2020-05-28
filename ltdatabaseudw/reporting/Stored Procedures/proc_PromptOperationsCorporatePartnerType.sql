CREATE PROC [reporting].[proc_PromptOperationsCorporatePartnerType] AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

SELECT [dim_description_key] CorporatePartnerProgramTypeDimDescriptionKey,
       [description] CorporatePartnerProgramTypeDescription
FROM [marketing].[v_dim_description]
WHERE source_object = 'r_mms_val_reimbursement_program_type'


END

