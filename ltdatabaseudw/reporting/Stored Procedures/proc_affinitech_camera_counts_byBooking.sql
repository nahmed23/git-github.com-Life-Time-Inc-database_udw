CREATE PROC [reporting].[proc_affinitech_camera_counts_byBooking] @ClubCodeList [VARCHAR](5000),@StartDate [DATETIME],@EndDate [DATETIME] AS
BEGIN
SET XACT_ABORT ON
SET NOCOUNT ON

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
--EXEC [reporting].[proc_affinitech_camera_counts_byBooking] '151', '4/22/2019', '4/25/2019'
--DECLARE @ClubCodeList VARCHAR(10) = 'MNCN'
--DECLARE @StartDate DATETIME = '4/22/2019'
--DECLARE @EndDate DATETIME = '4/25/2019'

IF OBJECT_ID('tempdb.dbo.#TempTable', 'U') IS NOT NULL DROP TABLE #TempTable;
IF OBJECT_ID('tempdb.dbo.#Participation', 'U') IS NOT NULL DROP TABLE #Participation;
IF OBJECT_ID('tempdb.dbo.#ClassTimes', 'U') IS NOT NULL DROP TABLE #ClassTimes;
IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL DROP TABLE #Results;
IF OBJECT_ID('tempdb.dbo.#Clubs', 'U') IS NOT NULL DROP TABLE #Clubs;


DECLARE @list_table VARCHAR(2000)
SET @list_table = 'club_list'

EXEC marketing.proc_parse_pipe_list @ClubCodeList,@list_table
	
SELECT DimClub.dim_club_key AS DimClubKey, 
       DimClub.club_id, 
	   Dimclub.domain_name_prefix,
	   DimClub.club_name AS ClubName,
       DimClub.club_code,
	   MMSRegion.description AS MMSRegion
  INTO #Clubs   
  FROM [marketing].[v_dim_club] DimClub
  JOIN #club_list ClubKeyList
    ON ClubKeyList.Item = DimClub.Club_ID or @ClubCodeList = 'All Clubs'
  JOIN [marketing].[v_dim_description]  MMSRegion
   ON MMSRegion.dim_description_key = DimClub.region_dim_description_key 
WHERE DimClub.club_id Not In (-1,99,100)
  AND DimClub.club_type = 'Club'
GROUP BY DimClub.dim_club_key, 
       DimClub.club_id,
	   Dimclub.domain_name_prefix, 
	   DimClub.club_name,
       DimClub.club_code,
	   MMSRegion.description

SELECT
	CAST(c.Start_Range as DATE) ClassDate,
	c.start_range FiveMin,
	SUM(CASE WHEN cam.cam_inverted = 1 THEN c.Exits-c.Enters ELSE c.Enters-c.Exits END) OVER (PARTITION BY cam.studio, convert(DATE,Start_Range) ORDER BY start_range) As 'Count',
	RIGHT(cam.studio, LEN(cam.Studio) - 4)  AS Studio,
	cam.cam_club_it club_indicator,
	cam.Cam_ip

Into #TempTable
FROM dbo.d_affinitech_camera_count c
  LEFT JOIN dbo.d_affinitech_cameras cam 
    ON c.Source_IP = cam.CAM_IP
  JOIN #Clubs Clubs
    ON Clubs.domain_name_prefix = cam.cam_club_it

WHERE CAST(c.Start_Range as DATE) >= @StartDate
    AND CAST(c.Start_Range as DATE) <= @EndDate

SELECT
booking.booking_id reservation
, ROW_NUMBER() OVER (ORDER BY club.domain_name_prefix, participation_date.calendar_date, resource_usage.resource_name, StartTime.minutes_after_midnight) Rownum
, participation_date.calendar_date participation_date
, booking.booking_state
, resource_usage.booking_resource_usage_state
, activity.activity_state
, p.participation_state
, NULL MOD_count
, report_part.number_of_participants no_participants  --12/19: adding the participation from the reporting schema
, club.club_name ClubName
, club.domain_name_prefix club_indicator
, employee.first_name + ' ' + employee.last_name Instructor	
, resource_usage.resource_name [resource]
, StartDate.calendar_date StartDate
, activity.activity_name upc_desc
, resource_usage.resource_id as resource_id
, CAST(Dateadd(mi,StartTime.minutes_after_midnight,0)as Time) as StartTime
, CAST(Dateadd(mi,EndTime.minutes_after_midnight,0)as Time) as EndTime
, MIN(CAST(DateAdd(Mi,StartTime.minutes_after_midnight,0)as TIME)) OVER (PARTITION BY club.domain_name_prefix , participation_date.calendar_date, resource_usage.resource_id) FirstClassStart
, MAX(CAST(DateAdd(Mi,EndTime.minutes_after_midnight,0)as TIME)) OVER (PARTITION BY club.domain_name_prefix , participation_date.calendar_date, resource_usage.resource_id) LastClassEnd
, CAST(DATEADD(mi,-30,starttime.display_12_hour_time)AS TIME) ThirtyBeforeStart
, CAST(DATEADD(mi,+15,starttime.display_12_hour_time)AS TIME) FifteenAfterStart
, CAST(DateAdd(Mi,+10,EndTime.display_12_hour_time)AS TIME) TenAfterEnd
, CAST(DATEADD(mi,+30,EndTime.display_12_hour_time)AS TIME) ThirtyAfterEnd
, StartDate.day_of_week_name [weekday]
, 'Exerp' as source_system

INTO #Participation
FROM marketing.v_dim_exerp_booking_recurrence booking_recurrence 
  JOIN marketing.v_dim_exerp_booking booking
    ON booking.dim_exerp_booking_recurrence_key = booking_recurrence.dim_exerp_booking_recurrence_key
  LEFT Join marketing.v_fact_exerp_participation p 
    ON booking.dim_exerp_booking_key = p.dim_exerp_booking_key 		
	  AND p.participation_state <> 'CANCELLED'
  JOIN marketing.v_dim_date participation_date
    ON participation_date.dim_date_key = booking.start_dim_date_key
      AND (participation_date.calendar_date BETWEEN @StartDate AND @EndDate OR participation_date.calendar_date IS NULL)
  LEFT JOIN marketing.v_dim_date StartDate
    ON Startdate.dim_date_key = booking_recurrence.recurrence_start_dim_date_key
  LEFT Join marketing.v_dim_time StartTime
    ON Starttime.dim_time_key = booking_recurrence.recurrence_start_dim_time_key
	AND Starttime.display_12_hour_time <> 'N/A'
  LEFT Join marketing.v_dim_time EndTime
    ON Endtime.dim_time_key = booking_recurrence.recurrence_end_dim_time_key
	AND Endtime.display_12_hour_time <> 'N/A'
  LEFT JOIN marketing.v_Dim_club club
    ON club.dim_club_key = booking.dim_club_key 
  JOIN #Clubs Clubs
    ON Clubs.domain_name_prefix = club.domain_name_prefix
  Join marketing.v_dim_exerp_activity activity
    ON activity.dim_exerp_activity_key = booking.dim_exerp_activity_key
	  AND activity.activity_state = 'ACTIVE'
  Left JOIN marketing.v_dim_exerp_booking_resource_usage resource_usage
    ON resource_usage.booking_id = booking.booking_id
	  AND resource_usage.booking_resource_usage_state = 'ACTIVE'
	  AND resource_usage.resource_name NOT LIKE '%Outdoor%' --12/19: adding a filter to eliminate classes that are outside and do not have camera counts
  LEFT JOIN marketing.v_dim_boss_product boss_product
    ON boss_product.dim_boss_product_key = activity.dim_boss_product_key
  LEFT Join marketing.v_dim_exerp_staff_usage staff_usage_main_booking	
    ON staff_usage_main_booking.booking_id = booking_recurrence.main_booking_id
      AND staff_usage_main_booking.staff_usage_state ='ACTIVE'
  LEFT join marketing.v_dim_employee employee_main_booking
    ON employee_main_booking.dim_employee_key = staff_usage_main_booking.dim_employee_key	
  LEFT Join marketing.v_dim_exerp_staff_usage staff_usage_booking	
    ON staff_usage_booking.dim_exerp_booking_key = booking.dim_exerp_booking_key
      AND staff_usage_booking.staff_usage_state ='ACTIVE'
  LEFT join marketing.v_dim_employee employee
    ON employee.dim_employee_key = staff_usage_booking.dim_employee_key
  JOIN reporting.v_exerp_participation report_part
	ON report_part.booking_id = booking.booking_id

Where 
  booking.booking_state != 'CANCELLED'
  AND boss_product.department_description IN ('GROUP FITNESS                 ', 'MARTIAL ARTS                  ', 'MY EVENT                      ')
  AND resource_usage.resource_name NOT LIKE '%Pool%'
  AND club.domain_name_prefix IN (select distinct club_indicator from #TEMPTABLE) --12/19: Adding a clause to filter out clubs that do not have cameras
  


Group BY 
	participation_date.calendar_date 
    , club.club_name
    , club.domain_name_prefix
    , employee.first_name + ' ' + employee.last_name
    , resource_usage.resource_name
    , StartDate.calendar_date
    , activity.activity_name
    , resource_usage.resource_id
    , StartTime.display_12_hour_time
    , EndTime.display_12_hour_time
    , StartTime.minutes_after_midnight
    , EndTime.minutes_after_midnight
    , StartDate.day_of_week_name
    , booking.booking_id
    , booking.booking_state
    , resource_usage.booking_resource_usage_state
    , activity.activity_state
    , p.participation_state
	, report_part.number_of_participants

	

SELECT
	P.*,
	prev_class.PrevClassEnd,
	next_class.NextClassStart
INTO #ClassTimes
FROM #Participation P
  LEFT JOIN (Select p.EndTime as PrevclassEnd, p.rownum, p.resource_id, p.participation_date from #participation p) prev_class --Previous Class EndTime
    ON prev_class.Rownum + 1 = P.Rownum
  LEFT JOIN (Select p.StartTime as NextclassStart, p.rownum, p.resource_id, p.participation_date from #participation p) next_class --Next Class starttime
    ON next_class.Rownum - 1 = P.Rownum

----Scenario 1: No class within 60 of start time
Select
 p.ClubName,
 p.club_indicator,
 t.ClassDate,
 t.Studio,
 p.resource,
 p.upc_desc,
 CT.NextclassStart,
 CT.PrevclassEnd,
 CAST(p.StartTime as DATETIME) StartTime,
 CAST(p.EndTime as DATETIME) EndTime,
 p.MOD_count,
 p.source_system,
 MIN(t2.count) as ExitCount,   
 MIN(t2.count) as MinCount,  --Between 30 minutes before class and end of class
 MAX(t.count) - MIN(t2.count) As CameraCount,
 p.no_participants as InstructorCount,
 --MAX(t.count) As CameraCount,
 Max(t.count) as MaxCount, ---Max count between start and end of class
 DATEDIFF(MINUTE, p.startTime, CT.PrevclassEnd) MinutesFromLastClass,
 DATEDIFF(MINUTE, p.EndTime, CT.NextclassStart) MinutesToNextClass,
 'Max - Min' as Logic

INTO #Results
FROM #Participation p
  LEFT JOIN #ClassTimes CT
    ON CT.Rownum = P.Rownum
  LEFT JOIN #TempTable t
    ON p.participation_date = t.ClassDate ---Calc for Max Count
	 AND p.Resource = t.Studio
	  AND P.club_indicator = t.club_indicator
	  AND CAST(t.fivemin as time) BETWEEN p.StartTime AND p.EndTime
  LEFT JOIN #TempTable t2  ---Calc for Min Count
    ON p.participation_date = t2.ClassDate 
	  AND p.Resource = t2.Studio
	  AND P.club_indicator = t2.club_indicator
	  AND CAST(t2.fivemin as time) BETWEEN p.ThirtyBeforeStart AND p.EndTime   

WHERE (ABS(DATEDIFF(MINUTE,p.startTime, CT.PrevclassEnd)) >= 60 OR CT.PrevclassEnd IS NULL) --AND  club_indicator NOT IN (select distinct club_indicator from #TEMPTABLE) AND Avg_INSTRUCTORCount <> 0

GROUP BY 

p.ClubName,
p.club_indicator,
t.ClassDate,
t.Studio,
p.resource,
p.upc_desc,
CT.NextclassStart,
CT.PrevclassEnd,
p.StartTime,
p.EndTime,
p.no_participants,
p.MOD_count,
p.source_system

UNION
--Scenario 2: Class within 60 of start time, and No class within 60 after
Select

 p.ClubName,
 p.club_indicator,
 t.ClassDate,
 t.Studio,
 p.resource,
 p.upc_desc,
 CT.NextclassStart,
 CT.PrevclassEnd,
 CAST(p.StartTime as DATETIME) StartTime,
 CAST(p.EndTime as DATETIME) EndTime,
 p.MOD_count,
 p.source_system,
-- MAX(t.exits) - MIN(t.exits) as Exits,
 MIN(t2.count) as ExitCount,   --Exit Count = Min count between 15 mins into class and 30 mins after
 MIN(t.count) as MinCount,
 MAX(t.count) - MIN(t3.count) As CameraCount,
 p.no_participants as InstructorCount,
 --Max(t.count)  As CameraCount,
 Max(t.count) as MaxCount, ---Max count between start and end of class
 DATEDIFF(MINUTE, p.startTime, CT.PrevclassEnd) MinutesFromLastClass,
 DATEDIFF(MINUTE, p.EndTime, CT.NextclassStart) MinutesToNextClass,
 'Max - ExitCount' as Logic
 
FROM #Participation p
  LEFT JOIN #ClassTimes CT
    ON CT.Rownum = P.Rownum
  LEFT JOIN #Temptable t
    ON p.participation_date = t.ClassDate ---Calc for Max Count
	AND p.Resource = t.Studio
	AND P.club_indicator = t.club_indicator
	AND CAST(t.fivemin as time) BETWEEN DATEADD(minute,5,p.StartTime) AND p.EndTime
  LEFT JOIN #Temptable t2  ---Calc for Min Count
    ON p.participation_date = t2.ClassDate 
	AND p.Resource = t2.Studio
	AND P.club_indicator = t2.club_indicator
	AND CAST(t2.fivemin as time) BETWEEN DATEADD(minute,-30,p.FirstClassStart) AND p.EndTime
  LEFT JOIN #Temptable  t3  ---Calc for Exit Count
    ON p.participation_date = t3.ClassDate 
	AND p.Resource = t3.Studio
	AND P.club_indicator = t3.club_indicator
	AND CAST(t3.fivemin as time) BETWEEN p.FifteenAfterStart AND DATEADD(minute,30,p.EndTime)   
	      
WHERE ABS(DATEDIFF(MINUTE, CAST(p.startTime as TIME), CT.PrevclassEnd)) < 60
  AND (ABS(DATEDIFF(MINUTE, CAST(p.EndTime as TIME), CT.NextclassStart)) >= 60 OR CT.NextclassStart IS NULL)

GROUP BY

p.ClubName,
p.club_indicator,
t.ClassDate,
t.Studio,
p.resource,
p.upc_desc,
CT.NextclassStart,
CT.PrevclassEnd,
p.StartTime,
p.EndTime,
p.no_participants,
p.MOD_count,
p.source_system 		

UNION
----Scenario 3: Class Within 60 of StartTime and Within 60 of EndTime
Select

 p.ClubName,
 p.club_indicator,
t.ClassDate,
 t.Studio,
 p.resource,
 p.upc_desc,
 CT.NextclassStart,
 CT.PrevclassEnd,
 CAST(p.StartTime as DATETIME) StartTime,
 CAST(p.EndTime as DATETIME) EndTime,
 p.MOD_count,
 p.source_system,
 MIN(t3.count) as ExitCount,   
 MIN(t2.count) as MinCount,  --Between FirstClass of day start and end of class
 --(MAX(t.count))  As CameraCount,
 (MAX(t.count) - MIN(t2.count) + Max(t.count) - MIN(t3.count)) / 2  As CameraCount,
 p.no_participants as InstructorCount,
 Max(t.count) as MaxCount, ---Max count between start and end of class
 DATEDIFF(MINUTE, p.startTime, CT.PrevclassEnd) MinutesFromLastClass,
 DATEDIFF(MINUTE, p.EndTime, CT.NextclassStart) MinutesToNextClass,
 '((Max-Min)+(Max-Exit))/2' as Logic

FROM #Participation p
 LEFT JOIN #ClassTimes CT
    ON CT.Rownum = P.Rownum
  LEFT JOIN #Temptable t
    ON p.participation_date = t.ClassDate ---Calc for Max Count
	AND p.Resource = t.Studio
	AND P.club_indicator = t.club_indicator
	AND CAST(t.fivemin as time) BETWEEN p.StartTime AND p.EndTime
  LEFT JOIN #Temptable t2  ---Calc for Min Count
    ON p.participation_date = t2.ClassDate 
	AND p.Resource = t2.Studio
	AND P.club_indicator = t2.club_indicator
	AND CAST(t2.fivemin as time) BETWEEN DATEADD(minute,-30,p.FirstClassStart) AND p.EndTime
  LEFT JOIN #Temptable t3  ---Calc for Exit Count
    ON p.participation_date = t3.ClassDate 
	AND p.Resource = t3.Studio
	AND P.club_indicator = t3.club_indicator
	AND CAST(t3.fivemin as time) BETWEEN p.FifteenAfterStart AND DATEADD(minute,30,p.LastClassEnd)	   
	  
WHERE (ABS(DATEDIFF(MINUTE, p.startTime, CT.PrevclassEnd)) < 60)
  AND (ABS(DATEDIFF(MINUTE, p.EndTime, CT.NextclassStart)) <= 60)

GROUP BY 

p.ClubName,
p.club_indicator,
t.ClassDate,
t.Studio,
p.resource,
p.upc_desc,
CT.NextclassStart,
CT.PrevclassEnd,
p.StartTime,
p.EndTime,
p.no_participants,
p.MOD_count,
p.source_system



SELECT r.*,
ABS(r.instructorCount - r.cameracount) as CameraCountVariance,
CASE WHEN r.instructorCount = 0 THEN 100 ELSE (ABS(r.instructorCount - r.cameracount) / REPLACE(r.instructorCount,0,1)) * 10 END as CamVarPercent
--[audit].Accuracy,
--[audit].Count,
--[audit].transactions

--, (SELECT top 1 CCA.entersforStudio from TemporaryImport..CameraCountAudit CCA WHERE RIGHT(cca.Studio, LEN(cca.Studio) - 4) = r.studio AND CCA.[Date] = r.ClassDate AND cca.cam_club_IT = r.club_indicator) EntersForDay
--, (SELECT top 1 CCA.exitsforStudio from TemporaryImport..CameraCountAudit CCA WHERE RIGHT(cca.Studio, LEN(cca.Studio) - 4) = r.studio AND CCA.[Date] = r.ClassDate AND cca.cam_club_IT = r.club_indicator) ExitsForDay
FROM #Results r
--JOIN [marketing].[v_fact_affinitech_accuracy_audit] [audit]
--  ON RIGHT([audit].studio, LEN([audit].Studio) - 4) = r.studio
--    AND [audit].[date] = r.ClassDate 
ORDER by r.clubname, r.classDate, r.studio, r.StartTime

END
