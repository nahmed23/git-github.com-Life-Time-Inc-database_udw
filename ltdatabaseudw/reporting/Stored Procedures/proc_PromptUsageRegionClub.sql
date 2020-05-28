CREATE PROC [reporting].[proc_PromptUsageRegionClub] @ReportBeginDate [DATETIME],@ReportEndDate [DATETIME] AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END


 -----------
 ------ This stored procedure returns clubs which were open within a selected date range, or within 1 month of opening.
 ------ Execution Sample:
 ------ Exec [reporting].[proc_PromptUsageRegionClub] '5/1/2018','5/5/2018'
 -----------


SELECT DimClub.dim_club_key AS DimClubKey,
       DimDescription.description  AS MMSRegionName,
       DimClub.club_id  AS MMSClubID,
       DimClub.club_code AS ClubCode,
       DimClub.club_name AS ClubName,
       CASE WHEN DimClub.club_status = 'Presale'
	        THEN 'Y'
			ELSE 'N'
			END PreSaleFlag,
       DimClub.club_code + ' - ' + DimClub.club_name AS ClubCodeDashClubName,
       ClubOpenDimDate.calendar_date AS ClubOpenDate,
       DATEADD(MM,-1,ClubOpenDimDate.calendar_date) AS ClubPreSaleDate,
       ClubCloseDimDate.calendar_date AS ClubCloseDate,
       DimClub.club_close_dim_date_key AS ClubCloseDimDateKey
  FROM marketing.v_dim_club DimClub
  JOIN marketing.v_dim_date ClubOpenDimDate
    ON DimClub.club_open_dim_date_key = ClubOpenDimDate.dim_date_key 
  JOIN marketing.v_dim_date ClubCloseDimDate
    ON DimClub.club_close_dim_date_key = ClubCloseDimDate.dim_date_key
  JOIN marketing.v_dim_description DimDescription
    ON DimClub.region_dim_description_key = DimDescription.dim_description_key
 WHERE DimClub.club_type = 'Club'
   AND IsNull(DimClub.club_code,'') <> ''
   AND IsNull(DimClub.club_open_dim_date_key,0) <> 0
   AND DATEADD(MM,-1,ClubOpenDimDate.calendar_date) <= @ReportEndDate
   AND (ClubCloseDimDate.calendar_date > @ReportBeginDate OR DimClub.club_close_dim_date_key < 0)

END
