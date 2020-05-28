CREATE PROC [reporting].[proc_PromptCancellationReason] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


 ------ JIRA : REP-5942
 ------ Execution Sample: EXEC [reporting].[proc_PromptCancellationReason]


SELECT [dim_description_id]
      ,[description]
  FROM [marketing].[v_dim_description] where source_object = 'r_mms_val_termination_reason'

END
