CREATE PROC [reporting].[proc_PromptYearMonth] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON
 
DECLARE @CurrentYear INT
DECLARE @CalendarMonthNumberInYear INT

Select @CurrentYear = dimDate.year, @CalendarMonthNumberinYear = dimdate.month_number_in_year
FROM marketing.v_dim_date DIMDATE
WHERE DimDate.Calendar_Date = CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101)

DECLARE @JuneFirst2007 DATETIME

SELECT @JuneFirst2007 = DimDate.calendar_date
From marketing.v_dim_date DimDate
WHERE dimdate.day_number_in_month = 1
  AND dimdate.month_number_in_year = 6
  AND dimdate.year = 2007

DECLARE @DecemberFirstCurrentYear DATETIME

SELECT @DecemberFirstCurrentYear = DimDate.Calendar_Date
FROM marketing.v_dim_date DimDate
WHERE DimDate.day_number_in_month = 1
  AND DimDate.month_number_in_year = 12
  AND DimDate.Year = @CurrentYear

DECLARE @DecemberFirstNextYear DATETIME

SELECT @DecemberFirstNextYear = DimDate.Calendar_Date
FROM marketing.v_dim_date DimDate
WHERE DimDate.day_number_in_month = 1
  AND DimDate.month_number_in_year = 12
  AND DimDate.Year = @CurrentYear + 1

SELECT DimDate.Four_Digit_Year_Dash_Two_Digit_Month FourDigitYearDashTwoDigitMonth,
       Convert(Varchar,DimDate.Year) + '-' + DimDate.Quarter_Name FourDigitYearDashCalendarQuarterName,
       YesterdayDimDate.Four_Digit_Year_Dash_Two_Digit_Month YesterdayFourDigitYearDashTwoDigitMonth,
	   DimDate.Month_Starting_Date CalendarMonthStartingDate /* Added by Arijit as per REP-5377 */
FROM marketing.v_dim_date DimDate
JOIN marketing.v_dim_date YesterdayDimDate
  ON YesterdayDimDate.Calendar_Date = Convert(Datetime,Convert(Varchar,GetDate()-1,101),101)
WHERE DimDate.Calendar_Date >= @JuneFirst2007
  AND DimDate.Calendar_Date <= CASE WHEN @CalendarMonthNumberInYear < 10 THEN @DecemberFirstCurrentYear
                                   ELSE @DecemberFirstNextYear END
  AND DimDate.Day_Number_In_Month = 1
  AND DimDate.Year >= 2007

  END