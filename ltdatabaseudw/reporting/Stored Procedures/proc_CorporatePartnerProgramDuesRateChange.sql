CREATE PROC [reporting].[proc_CorporatePartnerProgramDuesRateChange] @FourDigitYearDashTwoDigitMonth [CHAR](7),@DimReimbursementProgramKeyList [VARCHAR](8000),@ProgramTypeDimDescriptionKeyList [VARCHAR](8000),@DimCompanyKeyList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON


IF 1=0 BEGIN
       SET FMTONLY OFF
     END


DECLARE @ReportRunDateTime VARCHAR(21)
SELECT @ReportRunDateTime = CAST(DATEADD(HH,-5,GETDATE()) AS nvarchar(30)) 

DECLARE @StartDimDateKey INT
      , @EndDimDateKey INT
      , @HeaderDateRange VARCHAR(33)
      , @PriorMonthStartingDimDateKey INT
      , @PriorMonthEndingDimDateKey INT
      , @StartDate DATETIME
	  , @EndDate DATETIME
SELECT @StartDimDateKey = [month_starting_dim_date_key]
      , @EndDimDateKey = [month_ending_dim_date_key]
      , @PriorMonthStartingDimDateKey = [prior_month_starting_dim_date_key]
	  , @PriorMonthEndingDimDateKey = [prior_month_ending_dim_date_key]
	  , @StartDate = [calendar_date]
	  , @EndDate = [month_ending_date]
FROM [marketing].[v_dim_date] 
WHERE [four_digit_year_dash_two_digit_month] = @FourDigitYearDashTwoDigitMonth
AND [day_number_in_month] = 1



SELECT @HeaderDateRange = StartDimDate.[standard_date_name] + ' through ' + EndDimDate.[standard_date_name]
  FROM [marketing].[v_dim_date] StartDimDate
      JOIN [marketing].[v_dim_date] EndDimDate
	  on StartDimDate.[month_ending_dim_date_key] = EndDimDate.[dim_date_key]
 WHERE StartDimDate.[four_digit_year_dash_two_digit_month] = @FourDigitYearDashTwoDigitMonth
AND StartDimDate.[day_number_in_month] = 1


--------------------------------------
-- REIMBURSEMENT PROGRAM temp table --
--------------------------------------
IF OBJECT_ID('tempdb.dbo.#DimProgramList', 'U') IS NOT NULL DROP TABLE #DimProgramList; 

DECLARE @list_table VARCHAR(100) 
SET @list_table = 'program_list' 

EXEC marketing.proc_parse_pipe_list @DimReimbursementProgramKeyList, @list_table

SELECT DISTINCT DimProgram.reimbursement_program_id AS ReimbursementProgramID,
                DimProgram.dim_mms_reimbursement_program_key AS DimReimbursementProgramKey,
                DimProgram.program_name AS ProgramName
  INTO  #DimProgramList
  
  FROM #program_list ProgramNameList
  JOIN   [marketing].[v_dim_mms_reimbursement_program] DimProgram 
    ON DimProgram.dim_mms_reimbursement_program_key = ProgramNameList.Item
	OR ProgramNameList.Item Like '< Ignore this prompt >'
	


------------------------
-- COMPANY temp table --
------------------------
IF OBJECT_ID('tempdb.dbo.#DimCompanyList', 'U') IS NOT NULL DROP TABLE #DimCompanyList; 


SET @list_table = 'company_list'

EXEC marketing.proc_parse_pipe_list @DimCompanyKeyList,@list_table

SELECT /*DISTINCT*/ DimCompany.[company_id]
			  , DimCompany.[dim_mms_company_key] 
			  , DimCompany.[company_name]
			  , DimCompany.[corporate_code]
			
  INTO #DimCompanyList 
   FROM #company_list CompanyList
  JOIN [marketing].[v_dim_mms_company] DimCompany
    ON DimCompany.[dim_mms_company_key] = CompanyList.Item
	OR CompanyList.Item Like '< Ignore this prompt >'


-----------------------------
-- PARTNER TYPE temp table --
-----------------------------
IF OBJECT_ID('tempdb.dbo.#DimPartnerList', 'U') IS NOT NULL DROP TABLE #DimPartnerList; 
SET @list_table = 'partner_list'

EXEC marketing.proc_parse_pipe_list @ProgramTypeDimDescriptionKeyList,@list_table

SELECT /*DISTINCT*/ DimPartner.[description]
			  , DimPartner.[dim_description_key]
  INTO #DimPartnerList
  
  FROM #partner_list PartnerList
  JOIN  [marketing].[v_dim_description] DimPartner   
    ON DimPartner.[dim_description_key] = PartnerList.Item
      OR PartnerList.Item Like '< Ignore this prompt >'
  WHERE DimPartner.source_object = 'r_mms_val_reimbursement_program_type'
  



IF OBJECT_ID('tempdb.dbo.#DimReimbursementProgram', 'U') IS NOT NULL DROP TABLE #DimReimbursementProgram; 
SELECT --DISTINCT
       DimReimbursementProgram.[dim_mms_reimbursement_program_key] DimReimbursementProgramKey
     , DimCompanyList.company_name + ' - ' + DimCompanyList.corporate_code PartnerCompany
     , DimPartnerList.description PartnerProgramType
     , DimReimbursementProgram.[program_name] PartnerProgram
     , DimReimbursementProgram.[subsidy_reimbursement_single_membership_dues_flag] SingleMembershipDuesFlag
     , DimReimbursementProgram.[subsidy_reimbursement_couple_membership_dues_flag] CoupleMembershipDuesFlag
     , DimReimbursementProgram.[subsidy_reimbursement_family_membership_dues_flag] FamilyMembershipDuesFlag
     , DimReimbursementProgram.[subsidy_reimbursement_junior_member_dues_flag] JuniorDuesFlag
     , DimReimbursementProgram.[subsidy_reimbursement_experience_life_magazine_flag] ELChargeFlag
  INTO #DimReimbursementProgram
  
  FROM [marketing].[v_dim_mms_reimbursement_program] DimReimbursementProgram
	JOIN #DimProgramList DimProgramList
		ON DimReimbursementProgram.dim_mms_reimbursement_program_key = DimProgramList.DimReimbursementProgramKey
	JOIN #DimCompanyList DimCompanyList
		ON DimReimbursementProgram.dim_mms_company_key = DimCompanyList.dim_mms_company_key
	JOIN [marketing].[v_dim_mms_company] DimCompany
		ON DimCompany.[dim_mms_company_key] = DimCompanyList.[dim_mms_company_key] 
	JOIN #DimPartnerList DimPartnerList
		ON DimReimbursementProgram.program_type_dim_description_key = DimPartnerList.dim_description_key
	

DECLARE @HeaderProgramList VARCHAR(MAX)

SET @HeaderProgramList = (SELECT STRING_AGG(CAST(/*ProgramName*/PartnerProgram AS VARCHAR(MAX)), ',' ) 
							  WITHIN GROUP (ORDER BY /*ProgramName*/ PartnerProgram ASC) AS ProgramNames
							  FROM #DimReimbursementProgram)
						
DECLARE @HeaderProgramTypeList VARCHAR(MAX)
SET @HeaderProgramTypeList = (SELECT STRING_AGG(CAST(PartnerProgramType AS VARCHAR(MAX)), ',')
								 WITHIN GROUP (ORDER BY PartnerProgramType ASC) AS ProgramType
								 FROM #DimReimbursementProgram)



DECLARE @HeaderCompanyList VARCHAR(MAX)
SET @HeaderCompanyList = (SELECT STRING_AGG(CAST(PartnerCompany AS VARCHAR(MAX)), ',')
							WITHIN GROUP (ORDER BY PartnerCompany ) AS CompanyName
							FROM #DimReimbursementProgram)
							



IF OBJECT_ID('tempdb.dbo.#DimCustomer', 'U') IS NOT NULL DROP TABLE #DimCustomer; 
SELECT  DimMember.[dim_mms_member_key]
     , DimMember.[member_id] MemberID
     , DimMember.[membership_id] MembershipID
     , DimMember.[customer_name_last_first] MemberName
	 , DimMember.description_member MemberType
     , CASE WHEN DimMember.[member_active_flag] = 'Y' THEN 'Active' ELSE 'Inactive' END MemberStatus
     , #DimReimbursementProgram.DimReimbursementProgramKey
     , #DimReimbursementProgram.PartnerCompany
     , #DimReimbursementProgram.PartnerProgram
     , CASE WHEN #DimReimbursementProgram.SingleMembershipDuesFlag = 'Y' AND DimMMSMembershipType.[family_status_description] = 'Single' THEN 'Y'
            WHEN #DimReimbursementProgram.CoupleMembershipDuesFlag = 'Y' AND DimMMSMembershipType.[family_status_description] = 'Couple' THEN 'Y'
            WHEN #DimReimbursementProgram.FamilyMembershipDuesFlag = 'Y' AND DimMMSMembershipType.[family_status_description]  = 'Family' THEN 'Y'
            ELSE 'N' END MembershipDuesFlag
     , #DimReimbursementProgram.JuniorDuesFlag
     , #DimReimbursementProgram.ELChargeFlag
     , FactReimbursementProgram.[enrollment_dim_date_key] EnrollmentDimDateKey
     , FactReimbursementProgram.[termination_dim_date_key] TerminationDimDateKey
     , PartnerProgramDescription1.description + ' - ' + FactReimbursementProgram.[identifier_field1_value] PartnerProgramID1
     , PartnerProgramDescription2.description + ' - ' + FactReimbursementProgram.[identifier_field2_value] PartnerProgramID2
     , PartnerProgramDescription3.description + ' - ' + FactReimbursementProgram.[identifier_field3_value] PartnerProgramID3
	 , DimMMSMembershipType.[attribute_membership_status_summary_group_description]  MembershipTypeMembershipStatusSummaryGroupDescription
  INTO   #DimCustomer

  FROM [marketing].[v_fact_mms_member_reimbursement_program] FactReimbursementProgram
  JOIN #DimReimbursementProgram
	ON FactReimbursementProgram.[dim_mms_reimbursement_program_key] = #DimReimbursementProgram.DimReimbursementProgramKey
	
	JOIN  [marketing].[v_dim_mms_member_history] DimMember
		ON FactReimbursementProgram.[dim_mms_member_key] = DimMember.[dim_mms_member_key] 
		 AND DimMember.[effective_date_time] <=  @EndDate
		 AND DimMember.[expiration_date_time] > @EndDate
	
		
	LEFT JOIN  [marketing].[v_dim_mms_membership_history] DimMembership
	
		ON DimMembership.dim_mms_membership_key = DimMember.dim_mms_membership_key
		AND DimMembership.[effective_date_time] <=   @EndDate
	    AND DimMembership.[expiration_date_time] > @EndDate
	

   left  JOIN marketing.v_dim_mms_membership_type DimMMSMembershipType
	    ON DimMMSMembershipType.[dim_mms_membership_type_key] = DimMembership.[dim_mms_membership_type_key] 		
		   --------------------------------------------------------------------------------------
	     AND DimMMSMembershipType.[attribute_membership_status_summary_group_description] IN
		  ('Membership Status Summary Group 2 Revenue',            -------- Group names prior to 10/2018
           'Membership Status Summary Group 3 Revenue LTHealth',   -------- Group names prior to 10/2018
		   'Membership Status Summary Group 2 Revenue - 1 Member',
		   'Membership Status Summary Group 3 Revenue - 2 Members',
		   'Membership Status Summary Group 4 Revenue - 3/3+ Members',
		   'Membership Status Summary Group 5 Revenue - 4+ Members',
		   'Membership Status Summary Group 9 Rev Student Flex',
		   'Membership Status Summary Group 10 Rev Student Access',
		   'Membership Status Summary Group 6 Revenue On-Hold & Non-Access') 
    LEFT JOIN [marketing].[v_dim_description] PartnerProgramDescription1
	     ON FactReimbursementProgram.[identifier_field1_name_dim_description_key] = PartnerProgramDescription1.dim_description_key
    LEFT JOIN [marketing].[v_dim_description] PartnerProgramDescription2
	     ON FactReimbursementProgram.[identifier_field2_name_dim_description_key] = PartnerProgramDescription2.dim_description_key
    LEFT JOIN [marketing].[v_dim_description] PartnerProgramDescription3
	     ON FactReimbursementProgram.[identifier_field3_name_dim_description_key] = PartnerProgramDescription3.dim_description_key


WHERE FactReimbursementProgram.[enrollment_dim_date_key] <= @EndDimDateKey
AND (FactReimbursementProgram.[termination_dim_date_key] > @EndDimDateKey 
	OR( FactReimbursementProgram.[termination_dim_date_key] = 99991231))



IF OBJECT_ID('tempdb.dbo.#DimCustomerMembershipData', 'U') IS NOT NULL DROP TABLE #DimCustomerMembershipData; 

SELECT DimMembership.[membership_id] MembershipID
	 , DimMMSMembershipType.[membership_type] MembershipType 
	 , DimMembershipFamilyStatus.[family_status_description] MembershipFamilyStatus
	 , MembershipStatusDescription.description MembershipStatus
	 , DimDate.[standard_date_name] MembershipCreatedDate
  INTO #DimCustomerMembershipData
from [marketing].[v_dim_mms_membership_history] DimMembership
	JOIN #DimCustomer
		ON DimMembership.[membership_id] = #DimCustomer.MembershipID
	LEFT JOIN [marketing].[v_dim_date] DimDate  -- In UDW Memberships created prior to 2000-11-03 have NULL membership_created_date_time
		ON DimDate.[calendar_date] = CONVERT(DATE,CONVERT(DATETIME,DimMembership.[membership_created_date_time],107))
	LEFT JOIN [marketing].[v_dim_description] MembershipStatusDescription
	    ON MembershipStatusDescription.dim_description_key = DimMembership.[membership_status_dim_description_key]
	LEFT JOIN marketing.v_dim_mms_membership_type DimMMSMembershipType
	    ON DimMMSMembershipType.[membership_type_id] = DimMembership.[membership_type_id]
	LEFT JOIN marketing.v_dim_mms_membership_type DimMembershipFamilyStatus
	    ON DimMembershipFamilyStatus.[dim_mms_membership_type_key] = DimMembership.[dim_mms_membership_type_key]
WHERE DimMembership.[effective_date_time] <= @EndDate
  AND DimMembership.[expiration_date_time] > @EndDate
    


IF OBJECT_ID('tempdb.dbo.#DuesFactSalesTransaction', 'U') IS NOT NULL DROP TABLE #DuesFactSalesTransaction; 

SELECT --DISTINCT
       FactSalesTransaction.[fact_mms_sales_transaction_key] FactSalesTransactionKey -- PERHAPS [fact_mms_sales_transaction_item_key]
--FactSalesTransaction.[fact_mms_sales_transaction_item_key] FactSalesTransactionItemKey
     , FactSalesTransaction.[membership_id] MembershipID
     , FactSalesTransaction.[dim_mms_member_key] DimMemberKey
     , FactSalesTransaction.[post_dim_date_key] PostDimDateKey
     , FactSalesTransaction.[dim_mms_product_key] DimProductKey --Note do not have corresponding dim_mms_product_key in v_dim_mms_Product
     , SIGN(FactSalesTransaction.[sales_quantity]) * FactSalesTransaction.[sales_dollar_amount] Amount
     , FactSalesTransaction.[original_currency_code] OriginalCurrencyCode
     , FactSalesTransaction.[dim_mms_transaction_reason_key] DimTransactionReasonKey
INTO #DuesFactSalesTransaction
FROM [marketing].[v_fact_mms_transaction_item] FactSalesTransaction
  JOIN #DimCustomer
    ON FactSalesTransaction.[membership_id] = #DimCustomer.MembershipID
  LEFT JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason
    ON FactSalesTransaction.[dim_mms_transaction_reason_key] = DimTransactionReason.[dim_mms_transaction_reason_key]
  JOIN [marketing].[v_dim_mms_product] DimProduct
    ON FactSalesTransaction.[dim_mms_product_key] = DimProduct.[dim_mms_product_key]
WHERE FactSalesTransaction.[post_dim_date_key] >= @PriorMonthStartingDimDateKey
  AND FactSalesTransaction.[post_dim_date_key] <= @EndDimDateKey
  AND FactSalesTransaction.[voided_flag] = 'N'
  AND (DimTransactionReason.[reason_code_id] = 28 -- Monthly Dues Assessment
      OR DimProduct.[junior_member_dues_flag] = 'Y'
      OR DimProduct.[product_id] in (1200,3121,3504,4937)) -- Experience Life


-- Prior Month Dues
IF OBJECT_ID('tempdb.dbo.#PriorMonthDues', 'U') IS NOT NULL DROP TABLE #PriorMonthDues; 
SELECT #DuesFactSalesTransaction.MembershipID,
       #DuesFactSalesTransaction.OriginalCurrencyCode,
       SUM(CASE WHEN DimTransactionReason.[reason_code_id] = 28 AND #DimCustomer.MembershipDuesFlag = 'Y' THEN Amount ELSE 0 END) MembershipDues,
       SUM(CASE WHEN DimProduct.[junior_member_dues_flag] = 'Y' AND #DimCustomer.JuniorDuesFlag = 'Y' THEN Amount ELSE 0 END) JuniorDues,
       SUM(CASE WHEN DimProduct.[product_id] in (1200,3121,3504,4937) AND #DimCustomer.ELChargeFlag = 'Y' THEN Amount ELSE 0 END) ELCharges,
       SUM(CASE WHEN DimTransactionReason.[reason_code_id] = 28 AND #DimCustomer.MembershipDuesFlag = 'Y' THEN Amount 
                WHEN DimProduct.[junior_member_dues_flag]  = 'Y' AND #DimCustomer.JuniorDuesFlag = 'Y' THEN Amount
                WHEN DimProduct.[product_id] in (1200,3121,3504,4937) AND #DimCustomer.ELChargeFlag = 'Y' THEN Amount
                ELSE 0 END) NetCharges
 
  INTO #PriorMonthDues
  FROM #DuesFactSalesTransaction
  JOIN #DimCustomer
    ON #DuesFactSalesTransaction.MembershipID = #DimCustomer.MembershipID
  JOIN  [marketing].[v_dim_mms_transaction_reason] DimTransactionReason 
    ON #DuesFactSalesTransaction.DimTransactionReasonKey = DimTransactionReason.[dim_mms_transaction_reason_key]
  JOIN  [marketing].[v_dim_mms_product] DimProduct
    ON #DuesFactSalesTransaction.DimProductKey = DimProduct.[dim_mms_product_key]
 WHERE #DuesFactSalesTransaction.PostDimDateKey >= @PriorMonthStartingDimDateKey
   AND #DuesFactSalesTransaction.PostDimDateKey <= @PriorMonthEndingDimDateKey
 --  and #DuesFactSalesTransaction.MembershipID = '3846966'
 GROUP BY #DuesFactSalesTransaction.MembershipID,
          #DuesFactSalesTransaction.OriginalCurrencyCode


-- Current Month Dues
IF OBJECT_ID('tempdb.dbo.#ReportMonthDues', 'U') IS NOT NULL DROP TABLE #ReportMonthDues; 
SELECT #DuesFactSalesTransaction.MembershipID,
       #DuesFactSalesTransaction.OriginalCurrencyCode,
       SUM(CASE WHEN DimTransactionReason.[reason_code_id] = 28 AND #DimCustomer.MembershipDuesFlag = 'Y' THEN Amount ELSE 0 END) MembershipDues,  -- Reason Code 28 = Monthly Dues Assessment
       SUM(CASE WHEN DimProduct.[junior_member_dues_flag] = 'Y' AND #DimCustomer.JuniorDuesFlag = 'Y' THEN Amount ELSE 0 END) JuniorDues,
       SUM(CASE WHEN DimProduct.[product_id] in (1200,3121,3504,4937) AND #DimCustomer.ELChargeFlag = 'Y' THEN Amount ELSE 0 END) ELCharges,
       SUM(CASE WHEN DimTransactionReason.[reason_code_id] = 28 AND #DimCustomer.MembershipDuesFlag = 'Y' THEN Amount 
                WHEN DimProduct.[junior_member_dues_flag]  = 'Y' AND #DimCustomer.JuniorDuesFlag = 'Y' THEN Amount
                WHEN DimProduct.[product_id] in (1200,3121,3504,4937) AND #DimCustomer.ELChargeFlag = 'Y' THEN Amount
                ELSE 0 END) NetCharges
  INTO #ReportMonthDues
  FROM #DuesFactSalesTransaction
  JOIN #DimCustomer
    ON #DuesFactSalesTransaction.MembershipID = #DimCustomer.MembershipID
  JOIN [marketing].[v_dim_mms_transaction_reason] DimTransactionReason
    ON #DuesFactSalesTransaction.DimTransactionReasonKey = DimTransactionReason.[dim_mms_transaction_reason_key]
  JOIN [marketing].[v_dim_mms_product] DimProduct
    ON #DuesFactSalesTransaction.DimProductKey = DimProduct.[dim_mms_product_key]
 WHERE #DuesFactSalesTransaction.PostDimDateKey >= @StartDimDateKey 
   AND #DuesFactSalesTransaction.PostDimDateKey <= @EndDimDateKey
 GROUP BY #DuesFactSalesTransaction.MembershipID,
          #DuesFactSalesTransaction.OriginalCurrencyCode



--Final result set
IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL DROP TABLE #Results; 
SELECT #DimCustomer.PartnerCompany,
       #DimCustomer.PartnerProgram,
       #DimCustomerMembershipData.MembershipID,
       #DimCustomerMembershipData.MembershipType,
       #DimCustomerMembershipData.MembershipFamilyStatus,
       #DimCustomerMembershipData.MembershipStatus,
       #DimCustomerMembershipData.MembershipCreatedDate,
       #DimCustomer.MemberID,
       #DimCustomer.MemberName,
       #DimCustomer.MemberType,
       #DimCustomer.MemberStatus,
       EnrollmentDimDate.[standard_date_name] ProgramJoinDate,
       #DimCustomer.PartnerProgramID1,
       #DimCustomer.PartnerProgramID2,
       #DimCustomer.PartnerProgramID3,
       #PriorMonthDues.OriginalCurrencyCode PriorMonthCurrencyCode,
       #PriorMonthDues.MembershipDues PriorMonthMemberDues,
       #PriorMonthDues.JuniorDues PriorMonthJuniorDues,
       #PriorMonthDues.ELCharges PriorMonthELCharges,
       #ReportMonthDues.OriginalCurrencyCode CurrencyCode,
       #ReportMonthDues.MembershipDues MemberDues,
       #ReportMonthDues.JuniorDues,
       #ReportMonthDues.ELCharges,
       #ReportMonthDues.NetCharges - #PriorMonthDues.NetCharges NetChange
  INTO  #Results
  
  FROM #DimCustomer   
  JOIN  [marketing].[v_dim_date] EnrollmentDimDate 
    ON #DimCustomer.EnrollmentDimDateKey = EnrollmentDimDate.[dim_date_key]
  JOIN  #DimCustomerMembershipData  
    ON #DimCustomer.MembershipID = #DimCustomerMembershipData.MembershipID
  JOIN  #PriorMonthDues  
    ON #DimCustomer.MembershipID = #PriorMonthDues.MembershipID
  JOIN #ReportMonthDues 
    ON #DimCustomer.MembershipID = #ReportMonthDues.MembershipID
 WHERE  #PriorMonthDues.OriginalCurrencyCode <> #ReportMonthDues.OriginalCurrencyCode
    OR  #PriorMonthDues.NetCharges <> #ReportMonthDues.NetCharges



SELECT PartnerCompany,  
       PartnerProgram,
       MembershipID,
       MembershipType,
       MembershipFamilyStatus,
       MembershipStatus,
       MembershipCreatedDate,
       MemberID,
       MemberName,
       MemberType,
       MemberStatus,
       ProgramJoinDate,
       PartnerProgramID1,
       PartnerProgramID2,
       PartnerProgramID3,
       PriorMonthCurrencyCode,
       PriorMonthMemberDues,
       PriorMonthJuniorDues,
       PriorMonthELCharges,
       CurrencyCode,
       MemberDues,
       JuniorDues,
       ELCharges,
       NetChange,
       @ReportRunDateTime ReportRunDateTime,
       @FourDigitYearDashTwoDigitMonth HeaderYearMonth,
       @HeaderProgramTypeList HeaderProgramTypeList,
       @HeaderCompanyList HeaderCompanyList,
       @HeaderProgramList HeaderProgramList,
       CAST(Null as VARCHAR(70)) HeaderEmptyResult
  FROM #Results
 WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(Null as VARCHAR(103)) PartnerCompany,
       CAST(Null as VARCHAR(50)) PartnerProgram,
       NULL MembershipID,
       CAST(Null as VARCHAR(50)) MembershipType,
       CAST(Null as VARCHAR(50)) MembershipFamilyStatus,
       CAST(Null as VARCHAR(50)) MembershipStatus,
       CAST(Null as VARCHAR(12)) MembershipCreatedDate,
       NULL MemberID,
       CAST(Null as VARCHAR(132)) MemberName,
       CAST(Null as VARCHAR(50)) MemberType,
       CAST(Null as VARCHAR(8)) MemberStatus,
       CAST(Null as VARCHAR(12)) ProgramJoinDate,
       CAST(Null as VARCHAR(153)) PartnerProgramID1,
       CAST(Null as VARCHAR(153)) PartnerProgramID2,
       CAST(Null as VARCHAR(153)) PartnerProgramID3,
       CAST(Null as VARCHAR(15)) PriorMonthCurrencyCode,
       CAST(Null as DECIMAL(12,2)) PriorMonthMemberDues,
       CAST(Null as DECIMAL(12,2)) PriorMonthJuniorDues,
       CAST(Null as DECIMAL(12,2)) PriorMonthELCharges,
       CAST(Null as VARCHAR(15)) CurrencyCode,
       CAST(Null as DECIMAL(12,2)) MemberDues,
       CAST(Null as DECIMAL(12,2)) JuniorDues,
       CAST(Null as DECIMAL(12,2)) ELCharges,
       CAST(Null as DECIMAL(12,2)) NetChange,
       @ReportRunDateTime ReportRunDateTime,
       @FourDigitYearDashTwoDigitMonth HeaderYearMonth,
       @HeaderProgramTypeList HeaderProgramTypeList,
       @HeaderCompanyList HeaderCompanyList,
       @HeaderProgramList HeaderProgramList,
       'There is no data available for the selected parameters. Please re-try.' HeaderEmptyResult
 WHERE (SELECT COUNT(*) FROM #Results) = 0


DROP TABLE #DimReimbursementProgram
DROP TABLE #DimProgramList
DROP TABLE #DimCompanyList
DROP TABLE #DimPartnerList
DROP TABLE #DimCustomer
DROP TABLE #DimCustomerMembershipData
DROP TABLE #DuesFactSalesTransaction
DROP TABLE #PriorMonthDues
DROP TABLE #ReportMonthDues
DROP TABLE #Results


END
