CREATE PROC [reporting].[proc_ChildCenterUsage_OperationalException] @ReportBeginDate [DATETIME],@ReportBeginTime [VARCHAR](15),@ReportEndDate [DATETIME],@ReportEndTime [VARCHAR](15),@MMSClubIDList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



------ Sample Execution
------ Exec [reporting].[proc_ChildCenterUsage_OperationalException] '9/1/2014','15:00:00.000','9/30/2014','10:00:00.000','151|8'
------

 ---- set needed datetime variables
DECLARE @ReportRunDateTime Datetime
SET @ReportRunDateTime = Replace(Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),1,6)+', '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),8,10)+' '+Substring(convert(varchar,DATEADD(HH,-5,GETDATE()) ,100),18,2),'  ',' ')   ----- UDW in UTC time


DECLARE @ReportBeginDateTime DATETIME
DECLARE @ReportEndDateTime   DATETIME
DECLARE @BeginDimDateKey CHAR(32)
DECLARE @BeginDimTimeKey INT
DECLARE @EndDimDateKey CHAR(32)
DECLARE @EndDimTimeKey INT
DECLARE @ReportBeginDate_Full Varchar(18)
DECLARE @ReportBeginTime_display_12_hour_time Varchar(8)
DECLARE @ReportBeginDateTime_Standard Varchar(50)
DECLARE @ReportEndDate_Full Varchar(18)
DECLARE @ReportEndTime_display_12_hour_time Varchar(8)
DECLARE @ReportEndDateTime_Standard Varchar(50)

SET @ReportBeginDateTime = CAST(CONVERT(VARCHAR(10),@ReportBeginDate,101) + ' ' + @ReportBeginTime AS DATETIME)
SET @ReportEndDateTime = CAST(CONVERT(VARCHAR(10),@ReportEndDate,101) + ' ' + @ReportEndTime AS DATETIME)
SET @BeginDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date]  WHERE calendar_date = @ReportBeginDate)
SET @EndDimDateKey = (SELECT dim_date_key FROM [marketing].[v_dim_date]  WHERE calendar_date = @ReportEndDate)
SET @BeginDimTimeKey = (SELECT dim_time_key FROM [marketing].[v_dim_time]  WHERE RTRIM(LTRIM(display_24_hour_time)) = SUBSTRING(@ReportBeginTime, 1, CHARINDEX(':', @ReportBeginTime, CHARINDEX(':', @ReportBeginTime)+1)-1))
SET @EndDimTimeKey = (SELECT dim_time_key  FROM [marketing].[v_dim_time]  WHERE RTRIM(LTRIM(display_24_hour_time)) = SUBSTRING(@ReportEndTime, 1, CHARINDEX(':', @ReportEndTime, CHARINDEX(':', @ReportEndTime)+1)-1))
SET @ReportBeginDate_Full = (SELECT full_date_description FROM [marketing].[v_dim_date]  WHERE dim_date_key = @BeginDimDateKey)
SET @ReportBeginTime_display_12_hour_time = (SELECT display_12_hour_time FROM [marketing].[v_dim_time]  WHERE dim_time_key = @BeginDimTimeKey)
SET @ReportBeginDateTime_Standard = @ReportBeginDate_Full +' '+@ReportBeginTime_display_12_hour_time
SET @ReportEndDate_Full = (SELECT full_date_description FROM [marketing].[v_dim_date]  WHERE dim_date_key = @EndDimDateKey)
SET @ReportEndTime_display_12_hour_time = (SELECT display_12_hour_time FROM [marketing].[v_dim_time]  WHERE dim_time_key = @EndDimTimeKey)
SET @ReportEndDateTime_Standard = @ReportEndDate_Full +' '+@ReportEndTime_display_12_hour_time

DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

  EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table
	
SELECT DimClub.dim_club_key, DimClub.club_id, DimClub.club_name,
       DimClub.club_code,
       @ReportBeginDateTime AS ReportBeginDateTime,
	   @ReportEndDateTime AS ReportEndDateTime,
	   @BeginDimDateKey AS BeginDimDateKey, 
	   @BeginDimTimeKey AS BeginDimTimeKey,
	   @EndDimDateKey AS EndDimDateKey, 
	   @EndDimTimeKey AS EndDimTimeKey
  INTO #Locations
  FROM #club_list MMSClubIDList
  JOIN [marketing].[v_dim_club] DimClub
    ON MMSClubIDList.Item = DimClub.club_id

SELECT Locations.club_code AS ClubCode,
       Locations.club_name AS ClubName,
       DimDescription.description AS ExceptionDescription,
       PrimaryMember.member_id PrimaryMemberID,
       PrimaryMember.customer_name_last_first PrimaryMemberName,
       ChildDimMember.member_id AS JuniorMemberID,
       ChildDimMember.customer_name_last_first AS JuniorMemberName,
       CheckInDimDate.full_date_description + ' ' + CheckInDimTime.display_12_hour_time CheckInDateTime,
       CheckInByDimMember.customer_name_last_first CheckedInByName,
       CheckOutDimDate.full_date_description + ' ' + CheckOutDimTime.display_12_hour_time CheckOutDateTime,
       DimEmployee.employee_name_last_first EmployeeName,
       Locations.club_id AS MMSClubID,       
       FactChildCenterUsage.check_in_dim_date_key AS CheckInDimDateKey,
       FactChildCenterUsage.check_in_dim_time_key AS CheckInDimTimeKey
  INTO #Results
  FROM [marketing].[v_fact_mms_child_center_usage_exception] FactChildCenterUsageException
  JOIN [marketing].[v_fact_mms_child_center_usage] FactChildCenterUsage
    ON FactChildCenterUsageException.fact_mms_child_center_usage_key = FactChildCenterUsage.fact_mms_child_center_usage_key
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON FactChildCenterUsageException.dim_employee_key = DimEmployee.dim_employee_key
  JOIN [marketing].[v_dim_description] DimDescription
    ON FactChildCenterUsageException.exception_dim_description_key = DimDescription.dim_description_key
  JOIN #Locations Locations
    ON FactChildCenterUsage.dim_club_key = Locations.dim_club_key
  JOIN [marketing].[v_dim_mms_member] ChildDimMember
    ON FactChildCenterUsage.child_dim_mms_member_key = ChildDimMember.dim_mms_member_key
  JOIN [marketing].[v_dim_mms_member] PrimaryMember
    ON ChildDimMember.dim_mms_membership_key = PrimaryMember.dim_mms_membership_key
	AND PrimaryMember.val_member_type_id = 1     ------ Primary Member    ------- Comment out for QA
	--AND PrimaryMember.ref_mms_val_member_type_id = 1     ------ Primary Member    ------- Comment out for DEV and PROD
  JOIN [marketing].[v_dim_mms_member] CheckInByDimMember
    ON FactChildCenterUsage.check_in_dim_mms_member_key = CheckInByDimMember.dim_mms_member_key
  JOIN [marketing].[v_dim_date] CheckInDimDate
    ON FactChildCenterUsage.check_in_dim_date_key = CheckInDimDate.dim_date_key
  JOIN [marketing].[v_dim_time] CheckInDimTime
    ON FactChildCenterUsage.check_in_dim_time_key = CheckInDimTime.dim_time_key
  JOIN [marketing].[v_dim_date] CheckOutDimDate
    ON FactChildCenterUsage.check_out_dim_date_key = CheckOutDimDate.dim_date_key
  JOIN [marketing].[v_dim_time] CheckOutDimTime
    ON FactChildCenterUsage.check_out_dim_time_key = CheckOutDimTime.dim_time_key
 WHERE (FactChildCenterUsage.check_in_dim_date_key= @BeginDimDateKey and FactChildCenterUsage.check_in_dim_date_key < @EndDimDateKey and FactChildCenterUsage.check_in_dim_time_key  >= @BeginDimTimeKey)
    OR (FactChildCenterUsage.check_in_dim_date_key = @EndDimDateKey and FactChildCenterUsage.check_in_dim_date_key >@BeginDimDateKey and FactChildCenterUsage.check_in_dim_time_key  <= @EndDimTimeKey)
    OR (FactChildCenterUsage.check_in_dim_date_key = @BeginDimDateKey and FactChildCenterUsage.check_in_dim_date_key = @EndDimDateKey and FactChildCenterUsage.check_in_dim_time_key  >= @BeginDimTimeKey and FactChildCenterUsage.check_in_dim_time_key  <= @EndDimTimeKey)
    OR (FactChildCenterUsage.check_in_dim_date_key > @BeginDimDateKey and FactChildCenterUsage.check_in_dim_date_key < @EndDimDateKey)
	


SELECT ClubCode,
       ClubName,
       ExceptionDescription,
       PrimaryMemberID,
       PrimaryMemberName,
       JuniorMemberID,
       JuniorMemberName,
       CheckInDateTime,
       CheckedInByName,
       CheckOutDateTime,
       EmployeeName,
       MMSClubID,
       @ReportBeginDateTime_Standard ReportBeginDateTime,
       @ReportEndDateTime_Standard ReportEndDateTime,
       CheckInDimDateKey,
       CheckInDimTimeKey,
       @ReportRunDateTime ReportRunDateTime,
       CAST(NULL AS VARCHAR(70)) HeaderEmptyResultSet
  FROM #Results
 WHERE (SELECT COUNT(*) FROM #Results) > 0
UNION ALL
SELECT CAST(NULL AS VARCHAR(18)) ClubCode,
       CAST(NULL AS VARCHAR(50)) ClubName,
       CAST(NULL AS VARCHAR(50)) ExceptionDescription,
       CAST(NULL AS INT) PrimaryMemberID,
       CAST(NULL AS VARCHAR(132)) PrimaryMemberName,
       CAST(NULL AS INT) JuniorMemberID,
       CAST(NULL AS VARCHAR(132)) JuniorMemberName,
       CAST(NULL AS VARCHAR(21)) CheckInDateTime,
       CAST(NULL AS VARCHAR(132)) CheckedInByName,
       CAST(NULL AS VARCHAR(21)) CheckOutDateTime,
       CAST(NULL AS VARCHAR(102)) EmployeeName,
       CAST(NULL AS INT) MMSClubID,
       @ReportBeginDateTime_Standard ReportBeginDateTime,
       @ReportEndDateTime_Standard ReportEndDateTime,
       CAST(NULL AS INT) CheckInDimDateKey,
       CAST(NULL AS INT) CheckInDimTimeKey,
       @ReportRunDateTime ReportRunDateTime,
       'There is no data available for the selected parameters. Please re-try.' HeaderEmptyResultSet
 WHERE (SELECT COUNT(*) FROM #Results) = 0

 ORDER BY ClubName,
          ExceptionDescription,
          EmployeeName,
          CheckInDimDateKey,
          CheckInDimTimeKey,
          JuniorMemberID


	DROP TABLE #Locations
	DROP TABLE #Results

END
