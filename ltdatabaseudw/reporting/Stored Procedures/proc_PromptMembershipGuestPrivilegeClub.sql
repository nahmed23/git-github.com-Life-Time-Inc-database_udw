CREATE PROC [reporting].[proc_PromptMembershipGuestPrivilegeClub] @ReportDate [DATETIME],@MMSRegionNameList [VARCHAR](4000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON
--DECLARE @ReportDate DATETIME = '10/2/2018'
--DECLARE @MMSRegionNameList VARCHAR(4000) = 'Hall-MN-West'

DECLARE @list_table VARCHAR(4000)
SET @list_table = 'region_list'
EXEC marketing.proc_parse_pipe_list @MMSRegionNameList, @list_table

SELECT DimLocation.club_code + ' - ' + DimLocation.club_name ClubCodeDashClubName,
		DimLocation.dim_club_key DimLocationKey,
		MMSRegion.description MMSRegionName

FROM [marketing].[v_dim_club] DimLocation
  JOIN marketing.v_dim_description MMSRegion
    ON DimLocation.region_dim_description_key = MMSRegion.dim_description_key
  JOIN #region_list RegionList
    ON RegionList.Item = MMSRegion.description
  JOIN marketing.v_dim_date ClubCloseDate
    ON ClubCloseDate.dim_date_key = DimLocation.[club_close_dim_date_key]
  JOIN marketing.v_dim_date ClubOpenDate
    ON ClubOpenDate.dim_date_key = DimLocation.[club_open_dim_date_key]
WHERE (DimLocation.[club_close_dim_date_key] = -998 OR ClubCloseDate.Year >= YEAR(@ReportDate))
  AND (DimLocation.[club_open_dim_date_key] <> -998 AND ClubOpenDate.Calendar_Date <= @ReportDate)

END
