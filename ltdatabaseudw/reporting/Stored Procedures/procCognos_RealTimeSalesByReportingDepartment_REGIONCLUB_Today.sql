CREATE PROC [reporting].[procCognos_RealTimeSalesByReportingDepartment_REGIONCLUB_Today] @RegionList [VARCHAR](8000),@ClubIDList [VARCHAR](8000) AS
BEGIN 

SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
   SET FMTONLY OFF
END
 
DECLARE @StartDate Datetime
, @EndDate Datetime

SET @StartDate = CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101)  -- Returns Today's date
SET @EndDate = CONVERT(DATETIME,CONVERT(VARCHAR,GETDATE(),101),101)  -- Returns Today's date

DECLARE @HeaderDateRange Varchar(110)
SET @HeaderDateRange = Replace(Substring(convert(varchar, @StartDate, 100),1,6)+', '+Substring(convert(varchar, @StartDate, 100),8,4),'  ',' ')

-- Use map_utc_time_zone_conversion to determine correct 'current' time, factoring in daylight savings / non daylight savings 
DECLARE @ReportRunDateTime VARCHAR(21) 
SET @ReportRunDateTime = (select Replace(Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),1,6)+', '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),8,10)+' '+Substring(convert(varchar,dateadd(hh,-1 * offset ,getdate()) ,100),18,2),'  ',' ') get_date_varchar
						  from map_utc_time_zone_conversion
						  where getdate() between utc_start_date_time and utc_end_date_time and description = 'central time')

DECLARE @StartMonthStartingDimDateKey INT
SELECT @StartMonthStartingDimDateKey = DimDate.[dim_date_key]
FROM [marketing].[v_dim_date] DimDate
WHERE DimDate.[calendar_date] = @StartDate

IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL DROP TABLE #Clubs;

-- Create club temp table
DECLARE @list_table VARCHAR(100)
SET @list_table = 'club_list'

EXEC marketing.proc_parse_pipe_list @ClubIDList,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey,  ------- new name
	   DimClub.club_id AS MMSClubID,
	   PTRegion.description AS Region,
	   DimClub.club_status AS ClubStatus,
	   ClubActivationDate.calendar_date AS ClubActivationDate,
	   DimClub.local_currency_code AS CurrencyCode,
	   DimClub.club_name AS ClubName,
       DimClub.club_code AS ClubCode
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubKeyList
    ON ClubKeyList.Item = DimClub.club_id 
	   OR ClubKeyList.Item = -1 -- All Clubs
 JOIN [marketing].[v_dim_description]  PTRegion
   ON PTRegion.dim_description_key = DimClub.pt_rcl_area_dim_description_key
 JOIN [marketing].[v_dim_date] ClubActivationDate
   ON DimClub.[club_open_dim_date_key] = ClubActivationDate.[dim_date_key]
 WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_id < 900
  AND DimClub.club_type = 'Club'
  AND (DimClub.club_close_dim_date_key < '-997' OR DimClub.club_close_dim_date_key > @StartMonthStartingDimDateKey)  
GROUP BY DimClub.dim_club_key, DimClub.club_id, PTRegion.description, DimClub.club_status, ClubActivationDate.calendar_date,
	   DimClub.local_currency_code, DimClub.club_name, DimClub.club_code 

IF OBJECT_ID('tempdb.dbo.#DimLocation', 'U') IS NOT NULL DROP TABLE #DimLocation;

-- Create region temp table
SET @list_table = 'region_list'

EXEC marketing.proc_parse_pipe_list @RegionList,@list_table

SELECT Clubs.DimClubKey, 
	   Clubs.MMSClubID,
	   Clubs.Region,
	   Clubs.ClubStatus,
	   Clubs.ClubActivationDate,
	   Clubs.CurrencyCode
  INTO #DimLocation
  FROM #Clubs Clubs
  JOIN #region_list RegionList
        ON Clubs.Region = RegionList.Item
    OR @RegionList like '%All Regions%'
  

SELECT * FROM #DimLocation
DROP TABLE #Clubs   
DROP TABLE #DimLocation  


END

