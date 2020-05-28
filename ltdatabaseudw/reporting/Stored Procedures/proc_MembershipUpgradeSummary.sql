CREATE PROC [reporting].[proc_MembershipUpgradeSummary] @CheckInGroupID [INT],@BeginDate [DATETIME],@EndDate [DATETIME],@MembershipFilter [VARCHAR](50) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



-------  Sample Execution
------- Exec [reporting].[proc_MembershipUpgradeSummary] 51, '9/1/2018', '12/1/2018', 'All Memberships'

DECLARE @ReportRunDateTime VARCHAR(21) 
DECLARE @ReportRunDate DateTime
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ---- UDW in UTC time
SET @ReportRunDate = CAST((DATEADD(HH,-5,GETDATE()))AS DATE)     ---- UDW in UTC time


DECLARE @StartDimDateKey INT,
        @EndDimDateKey INT,
        @HeaderDateRange VARCHAR(51)
SELECT @StartDimDateKey = StartDimDate.dim_date_key,
       @EndDimDateKey = EndDimDate.dim_date_key,
       @HeaderDateRange = StartDimDate.standard_date_name + ' through ' + EndDimDate.standard_date_name
FROM [marketing].[v_dim_date] StartDimDate
CROSS JOIN [marketing].[v_dim_date] EndDimDate
WHERE StartDimDate.calendar_date = @BeginDate
  AND EndDimDate.calendar_date = @EndDate

 ----- Create EarliestRecords temp table   
IF OBJECT_ID('tempdb.dbo.#EarliestRecords', 'U') IS NOT NULL
  DROP TABLE #EarliestRecords; 

SELECT FactAudit.source_row_key AS SourceRowKey,      -------- name Change
       FactAudit.modified_dim_date_key AS ModifiedDimDateKey, 
       FactAudit.modified_dim_time_key AS ModifiedDimTimeKey,
       MIN(FactAudit.membership_audit_id) MembershipAuditID
INTO #EarliestRecords
FROM [marketing].[v_fact_mms_membership_audit] FactAudit
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON FactAudit.modified_dim_employee_key = DimEmployee.dim_employee_key
WHERE FactAudit.source_column_name = 'MembershipTypeID'
  AND DimEmployee.employee_id <> -2   ----- "AUTOMATED TRIGGER"
  AND FactAudit.modified_dim_date_key >= @StartDimDateKey
  AND FactAudit.modified_dim_date_key <= @EndDimDateKey
GROUP BY FactAudit.source_row_key,
         FactAudit.modified_dim_date_key,
         FactAudit.modified_dim_time_key


 ----- Create LatestRecords temp table   
IF OBJECT_ID('tempdb.dbo.#LatestRecords', 'U') IS NOT NULL
  DROP TABLE #LatestRecords;     
   
SELECT FactAudit.source_row_key AS SourceRowKey,      -------- name Change
       FactAudit.modified_dim_date_key AS ModifiedDimDateKey, 
       FactAudit.modified_dim_time_key AS ModifiedDimTimeKey,
       MAX(FactAudit.membership_audit_id) MembershipAuditID
INTO #LatestRecords
FROM [marketing].[v_fact_mms_membership_audit] FactAudit
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON FactAudit.modified_dim_employee_key = DimEmployee.dim_employee_key
WHERE FactAudit.source_column_name = 'MembershipTypeID'
  AND DimEmployee.employee_id <> -2   ----- "AUTOMATED TRIGGER"
  AND FactAudit.modified_dim_date_key >= @StartDimDateKey
  AND FactAudit.modified_dim_date_key <= @EndDimDateKey
GROUP BY FactAudit.source_row_key,
         FactAudit.modified_dim_date_key,
         FactAudit.modified_dim_time_key

 ----- Create LatestRecords temp table   
IF OBJECT_ID('tempdb.dbo.#FirstAndLastChangeOverPeriod', 'U') IS NOT NULL
  DROP TABLE #FirstAndLastChangeOverPeriod;  

SELECT FactAudit.source_row_key AS MembershipKey,    ------ Name Change
       MIN(Convert(Varchar,FactAudit.modified_dim_date_key)+Convert(Varchar,FactAudit.modified_dim_time_key)) MinModifiedDimDateTimeKey,
       MAX(Convert(Varchar,FactAudit.modified_dim_date_key)+Convert(Varchar,FactAudit.modified_dim_time_key)) MaxModifiedDimDateTimeKey
INTO #FirstAndLastChangeOverPeriod
FROM [marketing].[v_fact_mms_membership_audit] FactAudit
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON FactAudit.modified_dim_employee_key = DimEmployee.dim_employee_key
WHERE FactAudit.source_column_name = 'MembershipTypeID'
  AND DimEmployee.employee_id <> -2   ----- "AUTOMATED TRIGGER"
  AND FactAudit.modified_dim_date_key >= @StartDimDateKey
  AND FactAudit.modified_dim_date_key <= @EndDimDateKey
GROUP BY FactAudit.source_row_key




SELECT DimLocation.club_name AS MembershipClubName,
       DimLocation.club_id AS MembershipClubID,
       FromDimProduct.check_in_group_description  AS FromMembershipType,
       ToDimProduct.check_in_group_description AS ToMembershipType,
       COUNT(*) NumberOfUpgrades,
       @ReportRunDateTime ReportRunDateTime,
       @HeaderDateRange HeaderDateRange,
	   @ReportRunDate AS ReportDate
FROM #FirstAndLastChangeOverPeriod
JOIN [marketing].[v_fact_mms_membership_audit] FromFactMembershipAudit
  ON #FirstAndLastChangeOverPeriod.MembershipKey = FromFactMembershipAudit.source_row_key
 AND #FirstAndLastChangeOverPeriod.MinModifiedDimDateTimeKey = (Convert(Varchar,FromFactMembershipAudit.modified_dim_date_key) + Convert(Varchar,FromFactMembershipAudit.modified_dim_time_key))
JOIN #EarliestRecords
  ON #EarliestRecords.MembershipAuditID = FromFactMembershipAudit.membership_audit_id
JOIN [marketing].[v_fact_mms_membership_audit] ToFactMembershipAudit
  ON #FirstAndLastChangeOverPeriod.MembershipKey = ToFactMembershipAudit.source_row_key
 AND #FirstAndLastChangeOverPeriod.MaxModifiedDimDateTimeKey = (Convert(Varchar,ToFactMembershipAudit.modified_dim_date_key) + Convert(Varchar,ToFactMembershipAudit.modified_dim_time_key))
JOIN #LatestRecords
  ON ToFactMembershipAudit.membership_audit_id = #LatestRecords.MembershipAuditID
JOIN [marketing].[v_dim_mms_membership_type] FromDimProduct
  ON FromFactMembershipAudit.old_value = FromDimProduct.product_id
  AND FromFactMembershipAudit.source_column_name = 'MembershipTypeID'
JOIN [marketing].[v_dim_mms_membership_type] ToDimProduct
  ON ToFactMembershipAudit.new_value = ToDimProduct.product_id
  AND ToFactMembershipAudit.source_column_name = 'MembershipTypeID'
JOIN [marketing].[v_dim_mms_membership_history] FactMembership
  ON #FirstAndLastChangeOverPeriod.MembershipKey = FactMembership.dim_mms_membership_key
  AND FactMembership.effective_date_time < @ReportRunDate
  AND FactMembership.expiration_date_time >= @ReportRunDate
JOIN [marketing].[v_dim_club] DimLocation
  ON FactMembership.home_dim_club_key = DimLocation.dim_club_key
LEFT JOIN [marketing].[v_dim_mms_company] DimCompany
    ON FactMembership.dim_mms_company_key = DimCompany.dim_mms_company_key
   AND DimCompany.dim_mms_company_key > '0'
WHERE ToDimProduct.val_check_in_group_id = @CheckInGroupID
  AND FromDimProduct.val_check_in_group_id < ToDimProduct.val_check_in_group_id 
  AND (@MembershipFilter = 'All Memberships' 
        OR (@MembershipFilter = 'All Memberships - exclude Founders' 
            AND (ToDimProduct.attribute_founders_flag = 'N' OR FromDimProduct.attribute_founders_flag = 'N'))
        OR (@MembershipFilter = 'Employee Memberships' 
            AND (ToDimProduct.attribute_employee_membership_flag = 'Y' OR FromDimProduct.attribute_employee_membership_flag = 'Y'))
        OR (@MembershipFilter = 'Corporate Memberships' AND FactMembership.corporate_membership_flag = 'Y'))
GROUP BY DimLocation.club_name,
       DimLocation.club_id,
       FromDimProduct.check_in_group_description,
       ToDimProduct.check_in_group_description


 DROP TABLE #FirstAndLastChangeOverPeriod
 DROP TABLE #LatestRecords
 DROP TABLE #EarliestRecords

END
