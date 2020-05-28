CREATE PROC [reporting].[proc_PromptMembershipStatus] AS  
BEGIN   
SET XACT_ABORT ON  
SET NOCOUNT ON  


IF 1=0 BEGIN
       SET FMTONLY OFF
     END

 ------ JIRA : REP-5954
 ------ Execution Sample: EXEC [reporting].[proc_PromptMembershipStatus]
  
  select description as MembershipStatusDescription  
  from marketing.v_dim_description 
  where source_object = 'r_mms_val_membership_status' and description <>'1'
  
END

