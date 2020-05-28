CREATE PROC [reporting].[proc_MembershipChangeDetails] @StartDate [DATETIME],@EndDate [DATETIME],@MembershipTypeList [VARCHAR](4000),@PartnerProgram [VARCHAR](50),@CompanyIDList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
 
 ----- Sample Execution
---   Exec [reporting].[proc_MembershipChangeDetails] '01/01/2020','01/31/2020','Non-access','All','0'
---   Exec [reporting].[proc_MembershipChangeDetails] '01/01/2020','01/31/2020','Non-access','All','16961'
------


DECLARE @ReportRunDateTime VARCHAR(21)
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
                           from map_utc_time_zone_conversion
                            where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')



DECLARE @StartDimDateKey INT,
        @EndDimDateKey INT,
        @HeaderDateRange VARCHAR(51)
SELECT @StartDimDateKey = StartDimDate.dim_date_key,
       @EndDimDateKey = EndDimDate.dim_date_key,
       @HeaderDateRange = StartDimDate.standard_date_name + ' through ' + EndDimDate.standard_date_name
FROM [marketing].[v_dim_date] StartDimDate
CROSS JOIN [marketing].[v_dim_date] EndDimDate
WHERE StartDimDate.[calendar_date] = @StartDate
  AND EndDimDate.[calendar_date] = @EndDate
 
 ---- create a temp table for the selected membership types
IF OBJECT_ID('tempdb.dbo.#MembershipTypeList', 'U') IS NOT NULL
DROP TABLE #MembershipTypeList;  

DECLARE @list_table VARCHAR(100) 
SET @list_table = 'MembershipType'

  EXEC marketing.proc_parse_pipe_list @MembershipTypeList,@list_table
SELECT Item MembershipType
INTO #MembershipTypeList
FROM #MembershipType


DECLARE @MembershipTypeCommaList VARCHAR(4000)
SET @MembershipTypeCommaList = REPLACE(@MembershipTypeList,'|',', ')


---- create a temp table for the selected company ids
IF OBJECT_ID('tempdb.dbo.#CompanyIDList', 'U') IS NOT NULL DROP TABLE #CompanyIDList;   
create table #CompanyIDList with (distribution = round_robin, heap) as    

SELECT  DimCompany.company_id,
         DimCompany.dim_mms_company_key
FROM [marketing].[v_dim_mms_company] DimCompany
WHERE '|'+@CompanyIDList+'|' like '%|'+ Cast(DimCompany.company_id as Varchar(10)) +'|%'
GROUP BY  DimCompany.company_id,DimCompany.dim_mms_company_key



  ---- to return all types of membership type changes - not yet limited to selected type
IF OBJECT_ID('tempdb.dbo.#FirstAndLastChangeOverPeriod', 'U') IS NOT NULL
DROP TABLE #FirstAndLastChangeOverPeriod;     

SELECT source_row_key, -------- DimMMSMembershipKey,
       MIN(membership_audit_id) MINMembershipAuditID,
	   MAX(membership_audit_id) MaxMembershipAuditID
INTO #FirstAndLastChangeOverPeriod
FROM [marketing].[v_fact_mms_membership_audit]
WHERE source_column_name = 'MembershipTypeID'
  AND modified_dim_employee_key <> '8F9DD6FB57CE2ADA820DC9BB267CB9B7'   ---- Automated Trigger 
  AND modified_dim_date_key >= @StartDimDateKey
  AND modified_dim_date_key <= @EndDimDateKey
GROUP BY source_row_key




  ---- to return information on the previous membership Type
  ---- for all memberships coming from the selected membership check-in group(s)
IF OBJECT_ID('tempdb.dbo.#FromMembershipType', 'U') IS NOT NULL
DROP TABLE #FromMembershipType;     

SELECT FromFactMembership.dim_mms_membership_key,
       FromFactMembership.membership_id,
	   MembershipType.product_id,
	   MembershipType.check_in_group_description,
	   MembershipType.val_check_in_group_id,
	   FromFactMembership.Current_Price,
	   MembershipAudit.modified_dim_date_key
INTO #FromMembershipType
FROM #FirstAndLastChangeOverPeriod Memberships
  JOIN [marketing].[v_fact_mms_membership_audit] MembershipAudit
    ON Memberships.MINMembershipAuditID = MembershipAudit.membership_audit_id
  JOIN [marketing].[v_dim_date]  AuditDate
    ON MembershipAudit.modified_dim_date_key = AuditDate.dim_date_key
  JOIN [marketing].[v_dim_mms_membership_history] FromFactMembership									
    ON Memberships.source_row_key = FromFactMembership.dim_mms_membership_key
  JOIN [marketing].[v_dim_mms_membership_type] MembershipType
    ON  FromFactMembership.dim_mms_membership_type_key = MembershipType.dim_mms_membership_type_key
  JOIN #MembershipTypeList FromMemberTypeList 
    ON MembershipType.check_in_group_description = FromMemberTypeList.MembershipType
WHERE MembershipAudit.old_value = CAST(MembershipType.product_id AS VARCHAR(1000))
   AND FromFactMembership.expiration_date_time < DateAdd(day,1,AuditDate.calendar_date)
   AND FromFactMembership.expiration_date_time  >= AuditDate.calendar_date
GROUP BY FromFactMembership.dim_mms_membership_key,
       FromFactMembership.membership_id,
	   MembershipType.product_id,
	   MembershipType.check_in_group_description,
	   MembershipType.val_check_in_group_id,
	   FromFactMembership.Current_Price,
	   MembershipAudit.modified_dim_date_key


  ---- to return information on the final new membership type
  ---- For all memberships which had a membership type change in the period
IF OBJECT_ID('tempdb.dbo.#ToMembershipType', 'U') IS NOT NULL
DROP TABLE #ToMembershipType;     

SELECT ToFactMembership.dim_mms_membership_key,
       ToFactMembership.membership_id,
	   MembershipType.product_id,
	   MembershipType.check_in_group_description,
	   MembershipType.val_check_in_group_id,
	   ToFactMembership.Current_Price,
	   MembershipAudit.modified_dim_date_key,
	   MembershipAudit.modified_dim_employee_key,
	   DimClub.club_name,
	   DimClub.club_id,
	   MembershipSalesChannelDimDescription.Description AS OriginalSalesChannel,
	   MembershipSourceDimDescription.Description AS membership_source,
	   DimCompany.company_name,
       DimCompany.corporate_code,
	   DimCompany.company_id
INTO #ToMembershipType
FROM #FirstAndLastChangeOverPeriod Memberships
  JOIN [marketing].[v_fact_mms_membership_audit] MembershipAudit
    ON Memberships.MAXMembershipAuditID = MembershipAudit.membership_audit_id
  JOIN [marketing].[v_dim_date]  AuditDate
    ON MembershipAudit.modified_dim_date_key = AuditDate.dim_date_key
  JOIN [marketing].[v_dim_mms_membership_history] ToFactMembership    
    ON Memberships.source_row_key = ToFactMembership.dim_mms_membership_key
  JOIN [marketing].[v_dim_mms_membership_type] MembershipType
    ON  ToFactMembership.dim_mms_membership_type_key = MembershipType.dim_mms_membership_type_key
  JOIN [marketing].[v_dim_club] DimClub  
    ON DimClub.dim_club_key = ToFactMembership.home_dim_club_key
  LEFT JOIN [marketing].[v_dim_description] MembershipSalesChannelDimDescription
    ON ToFactMembership.membership_sales_channel_dim_description_key = MembershipSalesChannelDimDescription.dim_description_key
  LEFT JOIN [marketing].[v_dim_description] MembershipSourceDimDescription
    ON ToFactMembership.membership_source_dim_description_key = MembershipSourceDimDescription.dim_description_key 
  LEFT JOIN [marketing].[v_dim_mms_company] DimCompany
    ON ToFactMembership.dim_mms_company_key = DimCompany.dim_mms_company_key
    AND DimCompany.dim_mms_company_key > '-998'

WHERE ToFactMembership.effective_date_time < DateAdd(day,1,AuditDate.calendar_date)
      AND CAST(ToFactMembership.expiration_date_time AS Date)  > AuditDate.calendar_date
GROUP BY ToFactMembership.dim_mms_membership_key,
       ToFactMembership.membership_id,
	   MembershipType.product_id,
	   MembershipType.check_in_group_description,
	   MembershipType.val_check_in_group_id,
	   ToFactMembership.Current_Price,
	   MembershipAudit.modified_dim_date_key,
	   MembershipAudit.modified_dim_employee_key,
	   DimClub.club_name,
	   DimClub.club_id,
	   MembershipSalesChannelDimDescription.Description,
	   MembershipSourceDimDescription.Description,
	   DimCompany.company_name,
       DimCompany.corporate_code,
	   DimCompany.company_id 



 ---- returns data on the most recent reactivation modification within the selected date range 
 ---  for memberships changing from the selected membership Types 

  IF OBJECT_ID('tempdb.dbo.#MembershipModification_ReactivationData', 'U') IS NOT NULL 
DROP TABLE #MembershipModification_ReactivationData;    

SELECT Request.dim_mms_membership_key DimMMSMembershipKey,
       Request.request_date_time AS RequestDateTime,
       Request.effective_date AS EffectiveDate,   
	   RequestType.Description As RequestType,
	   CASE WHEN RequestSource.Description Is Null
        THEN 'Club'
	    ELSE RequestSource.Description
	   END    RequestSource
  INTO #MembershipModification_ReactivationData    

  FROM (SELECT SelectedMemberships.dim_mms_membership_key AS DimMMSMembershipKey,
         MAX(MMR.membership_modification_request_id) MembershipModificationRequestID
          FROM #FromMembershipType  SelectedMemberships
		   JOIN [marketing].[v_fact_mms_membership_modification_request] MMR
		     ON SelectedMemberships.dim_mms_membership_key = MMR.dim_mms_membership_key
		   JOIN [marketing].[v_dim_description] RequestType
		     ON MMR.request_type_dim_description_key = RequestType.dim_description_key
		   JOIN [marketing].[v_dim_description] RequestStatus
		     ON MMR.request_status_dim_mms_description_key = RequestStatus.dim_description_key
           WHERE RequestType.description in('Full Access Conversion','Student Reactivation')
	        AND RequestStatus.description in('Pending','Completed')    ---- excluding Cancelled
			AND MMR.effective_dim_date_key >= @StartDimDateKey
			AND MMR.effective_dim_date_key <= @EndDimDateKey
	      GROUP BY SelectedMemberships.dim_mms_membership_key)   MostRecentModificationRequest
   JOIN [marketing].[v_fact_mms_membership_modification_request] Request
     ON MostRecentModificationRequest.MembershipModificationRequestID = Request.membership_modification_request_id
   LEFT JOIN [marketing].[v_dim_description] RequestSource
     ON Request.membership_modification_request_source_dim_description_key = RequestSource.dim_description_key
   JOIN [marketing].[v_dim_description] RequestType
     ON Request.request_type_dim_description_key = RequestType.dim_description_key
   JOIN [marketing].[v_dim_description] RequestStatus
     ON Request.request_status_dim_mms_description_key = RequestStatus.dim_description_key


      
SELECT DISTINCT
       FromMembership.membership_id AS MembershipID,
       PrimaryMember.member_id AS PrimaryMemberID,
       CASE WHEN @PartnerProgram = 'All' THEN NULL ELSE DimReimbursementProgram.program_name END PartnerProgram,
       FromDimProduct.product_id AS FromProductID,
       FromDimProduct.product_description AS FromProductDescription,
       FromMembership.check_in_group_description AS FromMembershipType,
       FromMembership.val_check_in_group_id AS FromCheckInGroupID,
	   FromMembership.Current_Price AS FromMembershipDues,
       --Cast(FromFactMembership.Current_Price * USDDimPlanExchangeRate.plan_rate as Decimal(12,2)) FromMembershipDues,
       ToDimProduct.product_id AS ToProductID,
       ToDimProduct.product_description AS ToProductDescription,
       ToMembership.check_in_group_description AS ToMembershipType,
       ToMembership.val_check_in_group_id AS ToCheckInGroupID,
	   ToMembership.Current_Price AS ToMembershipDues,
       --Cast(ToFactMembership.Current_Price * USDDimPlanExchangeRate.plan_rate as Decimal(12,2)) ToMembershipDues,
       ToDimDate.standard_date_name AS ToMembershipConvertedDate,
       DimEmployee.employee_id AS ModificationEmployeeID,
       DimEmployee.last_name + ', ' + DimEmployee.first_name AS ModificationEmployeeName,
       ToMembership.modified_dim_date_key AS ModifiedDimDateKey,
       ToMembership.club_name AS MembershipClubName,  
       ToMembership.club_id AS MembershipClubID,
       @MembershipTypeCommaList AS MembershipTypeCommaList,
       @ReportRunDateTime AS ReportRunDateTime,
       @HeaderDateRange AS HeaderDateRange,
       ToMembership.membership_source AS OriginalMembershipSource,
       ToMembership.OriginalSalesChannel,
       ToMembership.company_name AS CompanyName,
       ToMembership.corporate_code AS CorporateCode,
	   MembershipReactivation.RequestDateTime AS ReactivationRequestDate,
       MembershipReactivation.RequestSource AS ReactivationRequestSource,
	   MembershipReactivation.RequestType AS ReactivationRequestType,
	   MembershipReactivation.EffectiveDate AS ReactivationEffectiveDate  
	   
	   
  FROM #FromMembershipType FromMembership 
  JOIN [marketing].[v_dim_date] FromDimDate
    ON FromMembership.modified_dim_date_key = FromDimDate.dim_date_key
  JOIN [marketing].[v_dim_mms_product] FromDimProduct
    ON FromMembership.product_id = FromDimProduct.product_id
  JOIN #ToMembershipType  ToMembership
    ON FromMembership.dim_mms_membership_key = ToMembership.dim_mms_membership_key
  JOIN [marketing].[v_dim_date] ToDimDate
    ON ToMembership.modified_dim_date_key = ToDimDate.dim_date_key
  JOIN [marketing].[v_dim_mms_product] ToDimProduct
    ON ToMembership.product_id = ToDimProduct.product_id   
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON ToMembership.modified_dim_employee_key = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_mms_member] PrimaryMember
    ON ToMembership.dim_mms_membership_key = PrimaryMember.dim_mms_membership_key
   AND PrimaryMember.description_member = 'Primary'
  LEFT JOIN [marketing].[v_fact_mms_member_reimbursement_program] FactMemberReimbursementProgram
    ON PrimaryMember.dim_mms_member_key = FactMemberReimbursementProgram.dim_mms_member_key
  LEFT JOIN [marketing].[v_dim_mms_reimbursement_program] DimReimbursementProgram
    ON FactMemberReimbursementProgram.dim_mms_reimbursement_program_key = DimReimbursementProgram.dim_mms_reimbursement_program_key
  LEFT JOIN #MembershipModification_ReactivationData MembershipReactivation
    ON ToMembership.dim_mms_membership_key = MembershipReactivation.DimMMSMembershipKey

WHERE FromMembership.check_in_group_description <> ToMembership.check_in_group_description  
  AND (DimReimbursementProgram.program_name = @PartnerProgram OR @PartnerProgram = 'All')
  AND (ToMembership.company_id IN (SELECT company_id FROM #CompanyIDList) OR @CompanyIDList = '0')



END
