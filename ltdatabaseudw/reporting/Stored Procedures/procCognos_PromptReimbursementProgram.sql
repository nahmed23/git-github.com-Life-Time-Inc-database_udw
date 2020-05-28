CREATE PROC [reporting].[procCognos_PromptReimbursementProgram] AS
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON

SELECT RP.reimbursement_program_id ReimbursementProgramID,
	   RP.[program_name] ReimbursementProgramName,
	   CASE WHEN
	   RP.program_active_flag = 'Y'
	   THEN 1 ELSE 0 
	   END as ActiveFlag,
	   dRP.dv_inserted_date_time InsertedDateTime,
	   dRP.dv_updated_date_time UpdatedDateTime,
	   case when CO.company_name ='' then 'None Designated' else CO.company_name end as PartnerProgramCompanyName,
	   ISNULL(CO.Company_ID,-998) PartnerProgramCompanyID,
	   Co.corporate_code CorporateCode

FROM marketing.v_dim_mms_reimbursement_program RP
  JOIN d_mms_reimbursement_program dRP
    ON dRP.dim_mms_reimbursement_program_key = RP.dim_mms_reimbursement_program_key
  LEFT JOIN marketing.v_dim_mms_company CO
    ON RP.dim_mms_company_key = CO.dim_mms_company_key
WHERE RP.reimbursement_program_id IS NOT NULL

END