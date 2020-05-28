CREATE PROC [reporting].[proc_PromptMembershipFilter] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


---- JIRA : REP-5951
---- EXEC [reporting].[proc_PromptMembershipFilter] 

SELECT 'All Memberships' MembershipFilter
UNION
SELECT 'All Memberships - Exclude Founders'
UNION
SELECT 'Corporate Memberships'
UNION
SELECT 'Employee Memberships'

END
