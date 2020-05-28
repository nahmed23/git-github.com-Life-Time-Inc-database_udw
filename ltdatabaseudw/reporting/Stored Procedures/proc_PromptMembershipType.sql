CREATE PROC [reporting].[proc_PromptMembershipType] AS  
BEGIN   
SET XACT_ABORT ON  
SET NOCOUNT ON  
 
 IF 1=0 BEGIN
	SET FMTONLY OFF
	END


SELECT 'All Memberships - Excluding Founders' MembershipTypePromptOption,  
       1 SortOrder  
UNION ALL  
SELECT 'All Memberships - Excluding House Account' MembershipTypePromptOption,  
       1 SortOrder  
UNION ALL  
SELECT 'Corporate Memberships' MembershipTypePromptOption,  
       1 SortOrder  
UNION ALL  
SELECT 'Corporate Flex Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Employee Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Flexible Pass Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Founders Type Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'House Account Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Investor Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'myHealthCheck Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Non-Access Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Pending Non-Access Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Short Term Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Student Flex Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'Trade Out Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT 'VIP Memberships' MembershipTypePromptOption,  
       2 SortOrder  
UNION ALL  
SELECT '26 and Under Memberships' MembershipTypePromptOption,  
       3 SortOrder  
UNION ALL  
SELECT 'Life Time Health Memberships' MembershipTypePromptOption,  
       2 SortOrder
UNION ALL
SELECT 'Access By Price Paid' MembershipTypePromptOption,
       2 SortOrder
  
END  
