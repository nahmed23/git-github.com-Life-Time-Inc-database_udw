CREATE PROC [reporting].[proc_PTTrainer_DSSR_RevenueThreeMonthTrailingAverage] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END



------  Daily data - delete and reload the sandbox table [rep].[ThreeMonthTrailingAverage]
------  @ReportDate is automated to set report to yesterday's date
------  Table will hold daily data for just the report date



DECLARE @ReportDate DATETIME = '1/1/1900'
SET @ReportDate = CASE WHEN @ReportDate = 'Jan 1, 1900' 
                    THEN CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE()-1,101),101) 
					ELSE @ReportDate END



DECLARE @StartDate_FirstOf3MonthsPrior date 
DECLARE @EndDate_FirstOfReportMonth date

SET @EndDate_FirstOfReportMonth = (SELECT month_starting_date FROM [marketing].[v_dim_date] WHERE calendar_date = @ReportDate)

SET @StartDate_FirstOf3MonthsPrior = (DateAdd(Month,-3,@EndDate_FirstOfReportMonth))

                   

   IF OBJECT_ID('tempdb.dbo.#SalesAmount', 'U') IS NOT NULL
  DROP TABLE #SalesAmount;

SELECT DimClub.club_id,
       DimClub.club_name,
	   DimClub.club_code,
	   PTRCLArea.description AS RegionName,
	   DimEmployee.employee_id,
	   DimEmployee.first_name,
	   DimEmployee.last_name,
	   RevenueAndServiceEmployeeSummary.report_date_dim_date_key,
	   Sum(RevenueAndServiceEmployeeSummary.month_to_date_item_amount) AS SumSalesAmount,
	   DimClub.dim_club_key,
	   DimEmployee.dim_employee_key

INTO #SalesAmount      
  FROM dbo.fact_ptdssr_revenue_and_service_employee_summary  RevenueAndServiceEmployeeSummary     ------ Note: new UDW table
  JOIN [marketing].[v_dim_club] DimClub
    ON RevenueAndServiceEmployeeSummary.dim_club_key = DimClub.dim_club_key
  JOIN [marketing].[v_dim_description] PTRCLArea
    ON DimClub.pt_rcl_area_dim_description_key = PTRCLArea.dim_description_key
  JOIN [marketing].[v_dim_employee] DimEmployee
    ON RevenueAndServiceEmployeeSummary.dim_employee_key = DimEmployee.dim_employee_key 
  JOIN [marketing].[v_dim_date] ReportDate
    ON RevenueAndServiceEmployeeSummary.report_date_dim_date_key = ReportDate.dim_date_key

 WHERE ReportDate.calendar_date >= @StartDate_FirstOf3MonthsPrior
   AND ReportDate.calendar_date < @EndDate_FirstOfReportMonth
   AND RevenueAndServiceEmployeeSummary.report_date_is_last_day_in_month_indicator = 'Y'

 GROUP BY DimClub.club_id,
       DimClub.club_name,
	   DimClub.club_code,
	   PTRCLArea.description,
	   DimEmployee.employee_id,
	   DimEmployee.first_name,
	   DimEmployee.last_name,
	   RevenueAndServiceEmployeeSummary.report_date_dim_date_key,
	   DimClub.dim_club_key,
	   DimEmployee.dim_employee_key





   IF OBJECT_ID('tempdb.dbo.#EmployeeSales', 'U') IS NOT NULL
  DROP TABLE #EmployeeSales;

	SELECT
		SalesAmount.RegionName,
        SalesAmount.club_name AS ClubName,
	    SalesAmount.club_code AS ClubCode,
		SalesAmount.club_id AS MMSClubID,
        SalesAmount.dim_club_key AS DimclubKey,
        SalesAmount.dim_employee_key AS DimEmployeeKey,
		CASE
			WHEN Len(SalesAmount.employee_id) < 5 
			 THEN REPLICATE('0', 5 - LEN(SalesAmount.employee_id)) + CAST(SalesAmount.employee_id AS varchar(10))
			ELSE cast(SalesAmount.employee_id as varchar(10)) 
			END  EmployeeID,
		SalesAmount.first_name AS FirstName,
		SalesAmount.last_name AS LastName,
		COUNT(Distinct SalesAmount.report_date_dim_date_key) As MonthCount,
		SUM(SalesAmount.SumSalesAmount) AS SalesAmount,   ---- for unit testing
		SUM(SalesAmount.SumSalesAmount) / COUNT(Distinct SalesAmount.report_date_dim_date_key) AS ThreeMonthTrailingAverage
INTO #EmployeeSales    
	FROM #SalesAmount    SalesAmount

	GROUP BY SalesAmount.RegionName,
        SalesAmount.club_name,
	    SalesAmount.club_code,
		SalesAmount.club_id,
        SalesAmount.dim_club_key,
        SalesAmount.dim_employee_key,
		SalesAmount.first_name,
		SalesAmount.last_name,
		CASE
			WHEN Len(SalesAmount.employee_id) < 5 
			 THEN REPLICATE('0', 5 - LEN(SalesAmount.employee_id)) + CAST(SalesAmount.employee_id AS varchar(10))
			ELSE cast(SalesAmount.employee_id as varchar(10)) 
			END




 ---- The MER rating per trainer, the PT trainer level and Draw Balance indicator are gathered with the Framework manager query


Select EmployeeRevenue.RegionName,
       EmployeeRevenue.ClubName,
	   EmployeeRevenue.ClubCode,
	   EmployeeRevenue.MMSClubID,
       EmployeeRevenue.DimClubKey,
       EmployeeRevenue.DimEmployeeKey,
	   EmployeeRevenue.EmployeeID,
	   EmployeeRevenue.FirstName,
	   EmployeeRevenue.LastName,
	   EmployeeRevenue.ThreeMonthTrailingAverage,
	   NULL AS MERRate,
	   NULL AS TrainerLevel,
	   NULL AS HasDrawBalance,
	   @ReportDate AS ReportDate
FROM #EmployeeSales EmployeeRevenue
   

  DROP TABLE #SalesAmount
  DROP TABLE #EmployeeSales


END
