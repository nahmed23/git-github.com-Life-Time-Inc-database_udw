CREATE PROC [reporting].[proc_PromptDelinquentUsageRegionClub] @ReportBeginDate [DATETIME],@ReportEndDate [DATETIME] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


 ------ JIRA : REP-5946
 ------ Execution Sample: EXEC [reporting].[proc_PromptDelinquentUsageRegionClub] '5/1/2018','5/5/2018'


SELECT DimClub.dim_club_key AS DimLocationKey,
       MMSRegionName.description  AS MMSRegionName,
       DimClub.club_id  AS MMSClubID,
       DimClub.club_code AS ClubCode,
       DimClub.club_name AS ClubName,
       CASE WHEN DimClub.club_status = 'Presale'
	        THEN 'Y'
			ELSE 'N'
			END PreSaleFlag,
       DimClub.club_code + ' - ' + DimClub.club_name AS ClubCodeDashClubName,
       ClubOpenDimDate.calendar_date AS ClubOpenDate,
       ClubCloseDimDate.calendar_date AS ClubCloseDate,
       DimClub.club_close_dim_date_key AS ClubCloseDimDateKey
  FROM marketing.v_dim_club DimClub
  JOIN marketing.v_dim_date ClubOpenDimDate ON DimClub.club_open_dim_date_key = ClubOpenDimDate.dim_date_key 
  JOIN marketing.v_dim_date ClubCloseDimDate ON DimClub.club_close_dim_date_key = ClubCloseDimDate.dim_date_key
  JOIN marketing.v_dim_description MMSRegionName  ON DimClub.region_dim_description_key = MMSRegionName.dim_description_key
 WHERE DimClub.club_type = 'Club'
   AND DimClub.club_open_dim_date_key > -997
   AND ClubOpenDimDate.calendar_date <= @ReportEndDate
   AND (ClubCloseDimDate.calendar_date > @ReportBeginDate OR DimClub.club_close_dim_date_key < 0)

END

