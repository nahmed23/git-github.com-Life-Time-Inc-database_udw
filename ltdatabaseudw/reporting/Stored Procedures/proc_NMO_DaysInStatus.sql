CREATE PROC [reporting].[proc_NMO_DaysInStatus] @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](8000),@MMSClubIDList [VARCHAR](8000) AS
BEGIN 
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END

--DECLARE @StartDate [DATETIME],@EndDate [DATETIME],@RegionList [VARCHAR](8000),@MMSClubIDList [VARCHAR](8000)

--SET @StartDate = '5/1/2019'
--SET @EndDate = '7/15/2019'
--SET @RegionList = 'All Regions'
--Set @MMSClubIDList = '151'

IF OBJECT_ID('tempdb.dbo.#ClubList', 'U') IS NOT NULL DROP TABLE #ClubList;
IF OBJECT_ID('tempdb.dbo.#RegionList', 'U') IS NOT NULL DROP TABLE #RegionList;
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL DROP TABLE #Clubs;


DECLARE @list_table VARCHAR(2000)
SET @list_table = 'club_list'
EXEC marketing.proc_parse_pipe_list @MMSClubIDList,@list_table

SET @list_table = 'region_list'
EXEC marketing.proc_parse_pipe_list @RegionList,@list_table

SELECT DimClub.dim_club_key AS DimClubKey, 
	   DimClub.club_name AS ClubName,
   	   MMSRegion.description AS MMSRegionName
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubKeyList
    ON ClubKeyList.Item = DimClub.club_id 
	   OR ClubKeyList.Item = -1
 JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
GROUP BY DimClub.dim_club_key, DimClub.club_name, MMSRegion.description

SELECT Clubs.DimClubKey, 
	   Clubs.ClubName,
   	   Clubs.MMSRegionName
  INTO #DimLocationKeyList
  FROM #Clubs Clubs
  JOIN #region_list RegionList
        ON Clubs.MMSRegionName = RegionList.Item
    OR @RegionList like '%All Regions%'

SELECT 
DimLocation.ClubName,
Member.member_id,
rtrim(member.first_name) + ' ' + member.last_name customer_name,
status.department,
right(task.title, len(task.title) - charindex('-', task.title)) Interest,
task.title,
[status],
CASE WHEN task.resolution_date IS NULL
	THEN DATEDIFF(DAY,task.created_date,GETDATE())
	ELSE DATEDIFF(DAY,task.created_date, resolution_date)
END as DaysInStatus,
[created_date],
[updated_date],
[due_date],
[resolution_date],
deleted_flag,
status.[Creator_Party_Id] Created_Employee_id,
[expiration_dim_date_key],
status.Party_Id

  FROM marketing.[v_dim_nmo_hub_task_status] status
  JOIN dbo.d_nmo_hub_task task
	ON task.dim_nmo_hub_task_status_key = status.dim_nmo_hub_task_status_key
  JOIN marketing.v_dim_mms_Member member
    ON member.dim_mms_member_key = status.dim_mms_member_key
  JOIN marketing.v_dim_club club
    on club.dim_club_key = status.dim_club_key
  JOIN #DimLocationKeyList DimLocation
    ON DimLocation.DimClubKey = club.dim_club_key

  WHERE CAST(task.created_date as DATE) BETWEEN @STartDate AND @EndDate

  ORDER BY status.[Party_Id]

END
