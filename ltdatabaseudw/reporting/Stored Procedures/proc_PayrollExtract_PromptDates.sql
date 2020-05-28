CREATE PROC [reporting].[proc_PayrollExtract_PromptDates] AS
BEGIN

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

----- Sample Execution
----- Exec [reporting].[proc_PayrollExtract_PromptDates]

DECLARE @StartDate AS DateTime
DECLARE @EndDate AS DateTime
SET @StartDate = DATEADD(MM,-18,Convert(Datetime,Convert(Varchar,GetDate(),101),101))
SET @EndDate = Convert(Datetime,Convert(Varchar,GetDate(),101),101)

IF OBJECT_ID('tempdb.dbo.#PayrollExtracts', 'U') IS NOT NULL
  DROP TABLE #PayrollExtracts; 

SELECT payroll_description AS PayrollExtractName
  INTO #PayrollExtracts
  FROM [marketing].[v_dim_magento_product_history]
 WHERE payroll_description <> ''
  AND effective_date_time <= @EndDate
  AND expiration_date_time > @StartDate
UNION
SELECT payroll_description AS PayrollExtractName
  FROM [marketing].[v_dim_mms_product_history]
 WHERE payroll_description <> ''
  AND effective_date_time <= @EndDate
  AND expiration_date_time > @StartDate
UNION
SELECT payroll_description AS PayrollExtractName
  FROM [marketing].[v_dim_cafe_product_history]
 WHERE payroll_description <> ''
  AND effective_date_time <= @EndDate
  AND expiration_date_time > @StartDate
UNION
SELECT payroll_description AS PayrollExtractName
  FROM [marketing].[v_dim_healthcheckusa_product_history]
 WHERE payroll_description <> ''
  AND effective_date_time <= @EndDate
  AND expiration_date_time > @StartDate
UNION
SELECT payroll_description AS PayrollExtractName
  FROM [marketing].[v_dim_hybris_product_history]
 WHERE payroll_description <> ''
  AND effective_date_time <= @EndDate
  AND expiration_date_time > @StartDate
UNION
SELECT 'Affiliate Programs Commissionable Sales and Service'


 ---- to return all payroll extract names for each Sunday date
SELECT DimDate.dim_date_key AS DimDateKey,
       DimDate.standard_date_name AS StandardDateDescription,
       DimDate.day_of_week_name AS DayOfWeekName,
       DimDate.day_number_in_week AS DayNumberInCalendarWeek,
	   #PayrollExtracts.PayrollExtractName,
	   Convert(INT,(SubString(Convert(varchar,Convert(Date,DateAdd(day,-7,DimDate.calendar_date))),1,4)+Substring(Convert(varchar,Convert(Date,DateAdd(day,-7,DimDate.calendar_date))),6,2)+Substring(Convert(varchar,Convert(Date,DateAdd(day,-7,DimDate.calendar_date))),9,2))) AS PriorSundayDimDateKey
FROM [marketing].[v_dim_date] DimDate
 CROSS JOIN #PayrollExtracts
WHERE DimDate.day_number_in_week = 1
  AND DimDate.calendar_date >= @StartDate  --- Offering 18 months of historical dates in prompting
  AND DimDate.calendar_date  <= @EndDate
ORDER BY DimDate.dim_date_key

END
