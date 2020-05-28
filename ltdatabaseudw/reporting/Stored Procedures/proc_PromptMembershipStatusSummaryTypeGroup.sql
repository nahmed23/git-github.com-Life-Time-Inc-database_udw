CREATE PROC [reporting].[proc_PromptMembershipStatusSummaryTypeGroup] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


 -----------
 ------ This stored procedure returns the distinct list MMS membership status summary group descriptions from the v_dim_mms_membership_type table.
 ------ Execution Sample:
 ------ Exec [reporting].[proc_PromptMembershipStatusSummaryTypeGroup]
 -----------


SELECT attribute_membership_status_summary_group_description
FROM [marketing].[v_dim_mms_membership_type]
WHERE attribute_membership_status_summary_group_description <> ''
 AND attribute_membership_status_summary_group_description Is Not Null
GROUP BY attribute_membership_status_summary_group_description

END
