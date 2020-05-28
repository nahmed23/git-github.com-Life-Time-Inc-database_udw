CREATE PROC [reporting].[proc_PromptPartnerProgram] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

Select [program_name] ReimbursementProgramName
FROM [marketing].[v_dim_mms_reimbursement_program]
WHERE [program_active_flag] = 'Y'

END