CREATE PROC [reporting].[proc_PromptUsageRegionClubForMonthRange] @BeginningYearMonth [char](7),@EndingYearMonth [char](7) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

------ JIRA : REP-5984
------ Sample Execution
------ Exec [reporting].[proc_PromptUsageRegionClubForMonthRange] '2016-10','2018-10'




DECLARE @FirstOfMonth DATETIME,
        @LastOfMonth DATETIME

SELECT @FirstOfMonth = MIN(calendar_date)
  FROM marketing.v_dim_date
 WHERE four_digit_year_dash_two_digit_month = @BeginningYearMonth

SELECT @LastOfMonth = MAX(calendar_date)
  FROM marketing.v_dim_date
 WHERE four_digit_year_dash_two_digit_month = @EndingYearMonth


SELECT DISTINCT 
       DimLocation.club_id as DimLocationKey,
       MMSRegion.description as MMSRegionName,
       DimLocation.club_name as ClubName
  FROM marketing.v_dim_club DimLocation
  JOIN marketing.v_dim_date ClubOpenDate on DimLocation.club_open_dim_date_key = ClubOpenDate.dim_date_key
  JOIN marketing.v_dim_date ClubPreSaleDimDate
    ON DATEADD(MM,-1,ClubOpenDate.calendar_date) = ClubPreSaleDimDate.calendar_date
  JOIN marketing.v_dim_date ClubCloseDimDate
    ON DimLocation.club_close_dim_date_key = ClubCloseDimDate.dim_date_key
  JOIN marketing.v_dim_description MMSRegion
	ON DimLocation.region_dim_description_key  = MMSRegion.dim_description_key
 WHERE DimLocation.club_type = 'Club'
   AND ClubOpenDate.calendar_date IS NOT NULL
   AND ClubPreSaleDimDate.calendar_date <= @LastOfMonth
   AND (ClubCloseDimDate.calendar_date > @FirstOfMonth OR ClubCloseDimDate.dim_date_key = -998)

END
