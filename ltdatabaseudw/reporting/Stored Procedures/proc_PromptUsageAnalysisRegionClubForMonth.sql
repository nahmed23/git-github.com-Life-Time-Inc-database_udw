CREATE PROC [reporting].[proc_PromptUsageAnalysisRegionClubForMonth] @StartFourDigitYearDashTwoDigitMonth [CHAR](7) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

------ JIRA : REP-5982
------ Sample Execution
------ Exec [reporting].[proc_PromptUsageAnalysisRegionClubForMonth] '2018-10'


DECLARE @FirstOfYearFourYearsPrior DATETIME,
        @EndOfMonthDate DATETIME

SELECT @EndOfMonthDate = ReportDimDate.month_ending_date,
       @FirstOfYearFourYearsPrior = FourYearsPriorDimDate.calendar_date
  FROM marketing.v_dim_date ReportDimDate
  JOIN marketing.v_dim_date FourYearsPriorDimDate
    ON ReportDimDate.year - 4 = FourYearsPriorDimDate.year
   AND FourYearsPriorDimDate.month_number_in_year = 1
   AND FourYearsPriorDimDate.day_number_in_month = 1
 WHERE ReportDimDate.four_digit_year_dash_two_digit_month = @StartFourDigitYearDashTwoDigitMonth
   AND ReportDimDate.day_number_in_month = 1

SELECT MMSRegion.description as MMSRegionName,
       DimLocation.club_name MMSClubName,
       DimLocation.club_code + ' - ' + DimLocation.club_name as ClubCodeDashClubName,
       DimLocation.club_id as DimLocationKey,
       DATEADD(MM,-1,ClubOpenDate.calendar_date) ClubPreSaleDate,
       ClubCloseDimDate.calendar_date ClubCloseDate,
       DimLocation.club_close_dim_date_key
  FROM marketing.v_dim_club DimLocation
  JOIN marketing.v_dim_description MMSRegion ON DimLocation.region_dim_description_key  = MMSRegion.dim_description_key
  JOIN marketing.v_dim_date ClubCloseDimDate ON DimLocation.club_close_dim_date_key = ClubCloseDimDate.dim_date_key
  JOIN marketing.v_dim_date ClubOpenDate on DimLocation.club_open_dim_date_key = ClubOpenDate.dim_date_key
 WHERE DimLocation.club_type = 'Club'
   AND ClubOpenDate.dim_date_key > -997
   AND DATEADD(MM,-1,ClubOpenDate.calendar_date) <= @EndOfMonthDate
   AND (ClubCloseDimDate.calendar_date > @FirstOfYearFourYearsPrior OR ClubCloseDimDate.dim_date_key = -998)


END


