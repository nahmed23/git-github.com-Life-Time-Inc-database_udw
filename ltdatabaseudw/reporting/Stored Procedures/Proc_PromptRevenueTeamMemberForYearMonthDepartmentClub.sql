CREATE PROC [reporting].[Proc_PromptRevenueTeamMemberForYearMonthDepartmentClub] @StartFourDigitYearDashTwoDigitMonth [CHAR](7),@EndFourDigitYearDashTwoDigitMonth [CHAR](7),@DepartmentMinDimReportingHierarchyKeyList [VARCHAR](8000),@DimClubIDList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END

--DECLARE @StartFourDigitYearDashTwoDigitMonth CHAR(7) = '2019-01'
--DECLARE @EndFourDigitYearDashTwoDigitMonth CHAR(7) = '2019-01'
--DECLARE @DepartmentMinDimReportingHierarchyKeyList VARCHAR(8000) = 'CCE79F93AD19043ED2BCE68DB1B99ED6'
--DECLARE @DimClubIDList VARCHAR(8000) = '151|172'

---- Sample Execution
--EXEC [reporting].[Proc_PromptRevenueTeamMemberForYearMonthDepartmentClub] '2019-01','2019-01','CCE79F93AD19043ED2BCE68DB1B99ED6','151|172'
----

DECLARE @StartMonthStartingDimDateKey CHAR(32)
SELECT @StartMonthStartingDimDateKey = MIN(DimDate.month_starting_dim_date_key)
FROM marketing.v_dim_date DimDate
WHERE four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth

DECLARE @EndMonthEndingDimDateKey CHAR(32)
SELECT @EndMonthEndingDimDateKey = MAX(DimDate.month_ending_dim_date_key)
FROM marketing.v_dim_date DimDate
WHERE four_digit_year_dash_two_digit_month = @EndFourDigitYearDashTwoDigitMonth


 ----- Create Department Temp table

IF OBJECT_ID('tempdb.dbo.#Departments', 'U') IS NOT NULL
  DROP TABLE #Departments; 

 DECLARE @pipe_list varchar(8000) 
 SET @pipe_list = @DepartmentMinDimReportingHierarchyKeyList 

SELECT DISTINCT  Hier.reporting_department AS reporting_department
INTO #Departments
FROM [marketing].[v_dim_reporting_hierarchy_history] Hier
JOIN (SELECT @pipe_list pl) pl
  ON '|'+pl.pl+'|' like '%|'+Hier.dim_reporting_hierarchy_key+'|%'
    OR '|'+pl.pl+'|' like '%|All Departments|%'
WHERE Hier.effective_dim_date_key <= @EndMonthEndingDimDateKey
  AND Hier.expiration_dim_date_key >= @StartMonthStartingDimDateKey

  ---- Create the hierarchy key temp table
IF OBJECT_ID('tempdb.dbo.#DimReportingHierarchyKey', 'U') IS NOT NULL DROP TABLE #DimReportingHierarchyKey;

SELECT DimReportingHierarchy.dim_reporting_hierarchy_key AS DimReportingHierarchyKey

 INTO #DimReportingHierarchyKey  
  FROM [marketing].[v_dim_reporting_hierarchy_history] DimReportingHierarchy
  JOIN #Departments AS DepartmentList
    ON DimReportingHierarchy.reporting_department = DepartmentList.reporting_department
  WHERE DimReportingHierarchy.effective_dim_date_key  <= @EndMonthEndingDimDateKey   
     AND DimReportingHierarchy.expiration_dim_date_key >= @StartMonthStartingDimDateKey
  GROUP BY  DimReportingHierarchy.dim_reporting_hierarchy_key


 ----- let this stored procedure call stay because I could not make it faster 

 IF OBJECT_ID('tempdb.dbo.#clublist', 'U') IS NOT NULL DROP TABLE #clublist;

DECLARE @list_table VARCHAR(500)
SET @list_table = 'clublist'
EXEC marketing.proc_parse_pipe_list @DimClubIDList,@list_table

SELECT DISTINCT dim_employee.dim_employee_key DimEmployeeKey,
				dim_club.club_code + ' - ' + dim_employee.employee_name AS ClubCodeDashEmployeeName ,
				dim_employee.employee_id AS DimEmployeeID
FROM [marketing].[v_fact_mms_allocated_transaction_item] mms_allocated_tran_item
JOIN marketing.v_dim_employee dim_employee
  ON mms_allocated_tran_item.[primary_sales_dim_employee_key] = dim_employee.dim_employee_key
JOIN marketing.v_dim_club dim_club
  ON dim_club.dim_club_key = mms_allocated_tran_item.[dim_club_key]
JOIN #DimReportingHierarchyKey
  ON #DimReportingHierarchyKey.DimReportingHierarchyKey = mms_allocated_tran_item.[dim_reporting_hierarchy_key]
JOIN #clublist   
  ON #clublist.item = dim_club.club_ID

WHERE mms_allocated_tran_item.[allocated_month_starting_dim_date_key] >= @StartMonthStartingDimDateKey
  AND mms_allocated_tran_item.[allocated_month_starting_dim_date_key] <= @EndMonthEndingDimDateKey
  AND dim_employee.dim_employee_key <> '-998'

UNION

SELECT DISTINCT dim_employee.dim_employee_key DimEmployeeKey,
				dim_club.club_code + ' - ' + dim_employee.employee_name AS ClubCodeDashEmployeeName ,
				dim_employee.employee_id AS DimEmployeeID
FROM [marketing].[v_fact_cafe_allocated_transaction_item] cafe_allocated_tran_item
JOIN marketing.v_dim_employee dim_employee
  ON cafe_allocated_tran_item.[commissioned_sales_dim_employee_key] = dim_employee.dim_employee_key
JOIN marketing.v_dim_club dim_club
  ON dim_club.dim_club_key = cafe_allocated_tran_item.[dim_club_key]
JOIN #DimReportingHierarchyKey
  ON #DimReportingHierarchyKey.DimReportingHierarchyKey = cafe_allocated_tran_item.[dim_reporting_hierarchy_key]
JOIN #clublist
  ON #clublist.item = dim_club.club_ID

WHERE cafe_allocated_tran_item.[allocated_month_starting_dim_date_key] >= @StartMonthStartingDimDateKey
  AND cafe_allocated_tran_item.[allocated_month_starting_dim_date_key] <= @EndMonthEndingDimDateKey
  AND dim_employee.dim_employee_key <> '-998'

UNION 

SELECT DISTINCT dim_employee.dim_employee_key DimEmployeeKey,
				dim_club.club_code + ' - ' + dim_employee.employee_name AS ClubCodeDashEmployeeName,
				dim_employee.employee_id AS DimEmployeeID
FROM [marketing].[v_fact_hybris_allocated_transaction_item] hybris_allocated_tran_item
JOIN marketing.v_dim_employee dim_employee
  ON hybris_allocated_tran_item.[commissioned_sales_dim_employee_key] = dim_employee.dim_employee_key
JOIN marketing.v_dim_club dim_club
  ON dim_club.dim_club_key = hybris_allocated_tran_item.[dim_club_key]
JOIN #DimReportingHierarchyKey
  ON #DimReportingHierarchyKey.DimReportingHierarchyKey = hybris_allocated_tran_item.[dim_reporting_hierarchy_key]
JOIN #clublist
  ON #clublist.item = dim_club.club_ID

WHERE hybris_allocated_tran_item.[allocated_month_starting_dim_date_key] >= @StartMonthStartingDimDateKey
  AND hybris_allocated_tran_item.[allocated_month_starting_dim_date_key] <= @EndMonthEndingDimDateKey
  AND dim_employee.dim_employee_key <> '-998'
 
UNION 

SELECT DISTINCT dim_employee.dim_employee_key DimEmployeeKey,
				dim_club.club_code + ' - ' + dim_employee.employee_name AS ClubCodeDashEmployeeName,
				dim_employee.employee_id AS DimEmployeeID
FROM [marketing].[v_fact_magento_allocated_transaction_item] magento_allocated_tran_item				
JOIN marketing.v_dim_employee dim_employee
  ON magento_allocated_tran_item.[commissioned_sales_dim_employee_key] = dim_employee.dim_employee_key
JOIN marketing.v_dim_club dim_club
  ON dim_club.dim_club_key = magento_allocated_tran_item.[dim_club_key]
JOIN #DimReportingHierarchyKey
  ON #DimReportingHierarchyKey.DimReportingHierarchyKey = magento_allocated_tran_item.[dim_reporting_hierarchy_key]
JOIN #clublist
  ON #clublist.item = dim_club.club_ID

WHERE magento_allocated_tran_item.[allocated_month_starting_dim_date_key] >= @StartMonthStartingDimDateKey
  AND magento_allocated_tran_item.[allocated_month_starting_dim_date_key] <= @EndMonthEndingDimDateKey
  AND dim_employee.dim_employee_key <> '-998'

  DROP TABLE #clublist
  DROP TABLE #DimReportingHierarchyKey
  DROP TABLE #Departments

  END
