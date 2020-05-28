CREATE PROC [reporting].[proc_GFScheduleOptimization] @StartDate [DateTime],@EndDate [DateTime],@Club [INT],@Age [INT] AS
begin

set nocount on
set xact_abort on

IF 1=0 BEGIN
       SET FMTONLY OFF
     END
----select * from marketing.v_dim_club where club_name like 'north%'
--DECLARE @StartDate DATETIME = '5/19/2019'
--DECLARE @EndDate DATETIME = '5/25/2019'
--DECLARE @Club INT = 151
--DECLARE @Gender VARCHAR(5) = 'M'
--DECLARE @Age INT = 1

DECLARE @StartDimDateKey INT
DECLARE @EndDimDateKey INT

IF OBJECT_ID('tempdb.dbo.#Clublist', 'U') IS NOT NULL DROP TABLE #clublist;
IF OBJECT_ID('tempdb.dbo.#Genderlist', 'U') IS NOT NULL DROP TABLE #Genderlist;
IF OBJECT_ID('tempdb.dbo.#Usage', 'U') IS NOT NULL DROP TABLE #Usage; 
IF OBJECT_ID('tempdb.dbo.#Participation', 'U') IS NOT NULL DROP TABLE #Participation;
IF OBJECT_ID('tempdb.dbo.#WeekDayUsage', 'U') IS NOT NULL DROP TABLE #WeekDayUsage;
IF OBJECT_ID('tempdb.dbo.#USort', 'U') IS NOT NULL DROP TABLE #USort;  
IF OBJECT_ID('tempdb.dbo.#Results', 'U') IS NOT NULL DROP TABLE #Results;  
IF OBJECT_ID('tempdb.dbo.#SwipesPer15MinsRank', 'U') IS NOT NULL DROP TABLE #SwipesPer15MinsRank;  
IF OBJECT_ID('tempdb.dbo.#PenetrationTemp', 'U') IS NOT NULL DROP TABLE #PenetrationTemp;
IF OBJECT_ID('tempdb.dbo.#ResultDistinctClasses', 'U') IS NOT NULL DROP TABLE #ResultDistinctClasses;
IF OBJECT_ID('tempdb.dbo.#TempNewResults', 'U') IS NOT NULL DROP TABLE #TempNewResults;



SELECT @StartDimDateKey = ReportBeginDimDate.dim_date_key,
       @EndDimDateKey = ReportEndDimDate.dim_date_key
  FROM marketing.v_dim_date ReportBeginDimDate
 CROSS JOIN marketing.v_dim_date ReportEndDimDate
 WHERE ReportBeginDimDate.calendar_date = @StartDate
   AND ReportEndDimDate.calendar_date = @EndDate

DECLARE @list_table VARCHAR(500)
SET @list_table = 'Clublist'
EXEC marketing.proc_parse_pipe_list @Club,@list_table

--DECLARE @Gender_list VARCHAR(10)
--SET @Gender_list = 'GenderList'
--EXEC marketing.proc_parse_pipe_list @Gender,@Gender_list

--Exerp group fitness participation for range
SELECT
	--  CAST(booking_recurrence.main_booking_id as VARCHAR(15)) as reservation
	 participation_date.calendar_date as participation_date
	, COUNT(Distinct p.participation_ID) as no_participants
    , LEFT(StartTime.display_12_hour_quarter_group,8) as FifteenMinInterval
    , LEFT(Starttime.display_12_hour_half_group,8) as HH
	, club.club_id ClubID
	, employee_main_booking.first_name + ' ' + LEFT(employee_main_booking.last_name,1) + '.' Instructor
	, booking.class_capacity ReservationLimit
	, resource_usage.resource_name [resource]
	, activity.activity_name upc_desc
	, activity.dim_exerp_activity_key upccode
	, booking.class_capacity capacity
	, StartTime.[display_12_hour_time] StartTime
	, EndTime.[display_12_hour_time] EndTime
	, StartDate.day_of_week_name [weekday]
	, prodkit.parent_upc
--	, DENSE_RANK() OVER (PARTITION BY DATEPART(dw,participation_date.calendar_date), resource.name, LEFT(StartTime.display_12_hour_quarter_group,8) order by StartDate.calendar_date desc) as DoubleClass
	, 'Exerp' as datasource

INTO #Participation
FROM 
marketing.v_dim_exerp_booking_recurrence booking_recurrence 
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
 Join marketing.v_dim_time StartTime
    ON Starttime.dim_time_key = booking.start_dim_time_key
  Join marketing.v_dim_time EndTime
    ON Endtime.dim_time_key = booking.stop_dim_time_key
  JOIN marketing.v_Dim_club club
    ON club.dim_club_key = booking.dim_club_key
  JOIN #Clublist cl
    ON club.club_id = cl.Item
  Join marketing.v_dim_exerp_activity activity
    ON activity.dim_exerp_activity_key = booking.dim_exerp_activity_key
	  AND activity.activity_state = 'ACTIVE'
  Left JOIN marketing.v_dim_exerp_booking_resource_usage resource_usage
    ON resource_usage.booking_id = booking.booking_id
	  AND resource_usage.booking_resource_usage_state = 'ACTIVE'
  LEFT JOIN marketing.v_dim_boss_product boss_product
    ON boss_product.dim_boss_product_key = activity.dim_boss_product_key
  LEFT JOIN marketing.v_dim_boss_product_bridge_dim_boss_prod_kit prodkit
    ON prodkit.child_dim_boss_product_key = boss_product.dim_boss_product_key  
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

Where booking.booking_state <> 'CANCELLED'
	and booking_recurrence.main_booking_id IS NOT NULL
	  AND boss_product.department_description IN ('GROUP FITNESS                 ', 'MARTIAL ARTS                  ', 'MY EVENT                      ')

Group BY 
  participation_date.calendar_date
 ,activity.activity_name
, booking_recurrence.main_booking_id 
, boss_product.department_description
, employee_main_booking.first_name + ' ' + LEFT(employee_main_booking.last_name,1)
, club.club_id
, resource_usage.resource_name
, StartDate.day_of_week_name
, Starttime.display_12_hour_half_group
, StartTime.display_12_hour_quarter_group
, StartTime.display_12_hour_time
, EndTime.display_12_hour_time
, booking.class_capacity 
, activity.dim_exerp_activity_key
, prodkit.parent_upc

UNION
--Boss Data
SELECT 
Participation_date.calendar_date,
participation.number_of_participants,
LEFT(StartTime.display_12_hour_quarter_group,8) as FifteenMinInterval,
LEFT(Starttime.display_12_hour_half_group,8) as HH,
club.Club_id,
employee_main_reservation.employee_name,
reservation.capacity,
rtrim(reservation.resource) [Resource],
rtrim(Product.product_description),
Product.upc_code upccode,
reservation.capacity,
StartTime.[display_12_hour_time],
EndTime.[display_12_hour_time],
StartDate.day_of_week_name, 
prodkit.parent_upc,
'Boss'     

FROM marketing.v_fact_boss_participation participation
JOIN marketing.v_dim_boss_reservation reservation
  ON reservation.dim_boss_reservation_key = participation.dim_boss_reservation_key
JOIN marketing.v_Dim_Date StartDate
  ON StartDate.dim_date_key = reservation.start_dim_date_key
Join marketing.v_Dim_Date EndDate
  ON EndDate.dim_date_key = reservation.end_dim_date_key
JOIN marketing.v_dim_time StartTime
  ON StartTime.dim_time_key = reservation.start_dim_time_key
JOIN marketing.v_dim_time EndTime
  ON EndTime.dim_time_key = reservation.end_dim_time_key
JOIN marketing.v_dim_club Club
  ON Club.dim_club_key = reservation.dim_club_key
    JOIN #Clublist cl
    ON club.club_id = cl.Item
JOIN marketing.v_dim_employee employee_main_reservation
  ON employee_main_reservation.dim_employee_key = reservation.dim_employee_key
JOIN marketing.v_dim_employee employee
  ON employee.dim_employee_key = participation.primary_dim_employee_key
JOIN marketing.v_dim_boss_product Product
  ON Product.dim_boss_product_key = reservation.dim_boss_product_key
  LEFT JOIN marketing.v_dim_boss_product_bridge_dim_boss_prod_kit prodkit
    ON prodkit.child_dim_boss_product_key = product.dim_boss_product_key  
JOIN marketing.v_dim_date Participation_Date
  ON Participation_Date.dim_date_key = participation.participation_dim_date_key
  AND Participation_Date.calendar_date BETWEEN @StartDate AND @EndDAte  

Where product.department_description IN ('MARTIAL ARTS', 'Run','Group fitness') 
AND reservation.resource != 'OUTDOORS'

--WeekDay usage for Period
SELECT
DATENAME(DW,MU.checkin_date_time) as WeekdayName,
COUNT(*) as Swipes,
SUM(CASE WHEN MU.gender_abbreviation = 'M' Then 1 Else 0 End) as Males,
SUM(CASE WHEN MU.gender_abbreviation = 'F' Then 1 Else 0 End) as Females,
Club.Club_id ClubID,
CAST(CAST(COUNT(*) as decimal) / CASE WHEN DATEDIFF(week, @StartDate, @EndDate) = 0 THEN 1 ELSE DATEDIFF(week, @StartDate, @EndDate) END as INT) as WeekdayAvgSwipes,
DENSE_RANK() OVER (ORDER BY COUNT(*) desc) as weekdayswipeRank

INTO #WeekDayUsage
FROM marketing.v_fact_mms_member_usage MU
JOIN marketing.v_dim_club Club
  ON Club.dim_club_key = MU.dim_club_key
JOIN #Clublist cl
  ON club.club_id = cl.Item
--JOIN #Genderlist gl
--  ON gl.item = MU.gender_abbreviation

WHERE CAST(checkin_date_time as DATE) BETWEEN @StartDate AND @EndDate
  --AND MU.gender_abbreviation IN (@Gender)
  AND MU.member_age_years >= (@Age)
GROUP BY Club.Club_id
  , DATENAME(DW,MU.checkin_date_time)


--Club Usage for HalfHour and 15min intervals: Field is adjusted to Display previous halfhour: 
--Participation data is to reflect previous half hours swipe count
SELECT
--  DATEADD(mi,DATEDIFF(mi,30,CAST(MU.checkin_date_time as TIME))/30*30,30) as HalfHour
  DATEADD(mi,DATEDIFF(mi,15,CAST(MU.checkin_date_time as TIME))/15*15,15)  as FifteenMinInterval
, dim_time.display_12_hour_time
, dim_time.display_24_hour_time
, DATENAME(DW,MU.checkin_date_time) as [Weekday]
, COUNT(*) as Swipes
, SUM(CASE WHEN MU.gender_abbreviation = 'M' Then 1 Else 0 End) as Males
, SUM(CASE WHEN MU.gender_abbreviation = 'F' Then 1 Else 0 End) as Females
, SUM(case WHEN MU.member_age_years >= 65 THEN 1 Else 0 END) as '65+'
, CAST(CAST(COUNT(*) as decimal) / CASE WHEN DATEDIFF(week, @StartDate, @EndDate) = 0 THEN 1 ELSE DATEDIFF(week, @StartDate, @EndDate) END as INT) as WeekDayHHAvgSwipes
, Club.Club_id ClubID

INTO #Usage
FROM marketing.v_fact_mms_member_usage MU
JOIN marketing.v_dim_club Club
  ON Club.dim_club_key = MU.dim_club_key
JOIN #Clublist cl
  ON club.club_id = cl.Item
JOIN marketing.v_dim_time dim_time
  ON dim_time.display_24_hour_time = CONVERT(CHAR(5),DATEADD(mi,DATEDIFF(mi,15,CAST(MU.checkin_date_time as TIME))/15*15,15),114)

WHERE CAST(checkin_date_time as DATE) BETWEEN @StartDate AND @EndDate
 -- AND MU.gender_abbreviation IN (@Gender)
	AND MU.member_age_years >= (@Age)
GROUP BY Club.Club_id
 , DATENAME(DW,MU.checkin_date_time)
-- , DATEADD(mi,DATEDIFF(mi,30,CAST(MU.checkin_date_time as TIME))/30*30,30)
 , DATEADD(mi,DATEDIFF(mi,15,CAST(MU.checkin_date_time as TIME))/15*15,15) 
 , dim_time.display_24_hour_time
 , dim_time.display_12_hour_time

--------UsageSorting For HalfHour Shading-----
SELECT MAX(u.Swipes) as MaxSwipes,
u.ClubID
INTO #USort
FROM #Usage U
GROUP BY u.clubid

----Results-----
SELECT
--  p.reservation
ROW_Number() OVER (ORDER BY CAST(u.FifteenMinInterval as TIME)) as [Rownumber]
, ISNULL(avg(p.no_participants),0) as avgParticipation
, CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) as Capacitypercent 
--, p.startdate
, CASE 
  WHEN CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) >= .9 Then 1
  WHEN CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) <= .899 AND CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) >= .8 THEN 2
  WHEN CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) <= .799 AND CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) >= .7 THEN 3
  WHEN CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) <= .699 AND CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) >= .6 THEN 4
  WHEN CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) <= .599 THEN 5
  WHEN CAST(avg(p.no_participants) as float) / CAST(NullIf(p.capacity,0) as Float) IS NULL THEN 6
  Else 10
  END as capacitySort 
, u.[WeekDay]
, CASE WHEN u.Weekday = 'Sunday' THEN 7
		WHEN u.WeekDay = 'Monday' Then 1
		WHEN u.WeekDay = 'Tuesday' Then 2
		WHEN u.WeekDay = 'Wednesday' Then 3
		WHEN u.WeekDay = 'Thursday' Then 4
		WHEN u.WeekDay = 'Friday' Then 5
		WHEN u.WeekDay = 'Saturday' Then 6
		Else 99 END as weekdaySort
--, DENSE_RANK() OVER (PARTITION BY DATEPART(dw,p.participation_date), p.resource, p.hh order by p.startdate desc) as DoubleClass
, p.startTime
, p.EndTime
, p.hh
--, CAST(u.HalfHour as TIME) HalfHour
, CAST(u.FifteenMinInterval as TIME) FifteenMinInterval
, CAST(u.FifteenMinInterval as TIME) FifteenMinInterval_Fact
, CAST(u.display_12_hour_time as VARCHAR(5)) display_12_hour_time
, u.display_24_hour_time
, u.Swipes
, u.WeekDayHHAvgSwipes
, p.clubID
, p.instructor
, p.capacity
, p.resource
, p.upc_desc
, p.upccode
, CASE
	WHEN p.parent_upc = '701592762528'
	THEN 'Y'
	ELSE 'N' 
	END as Signature 
, CAST(NULLIF(u.WeekdayHHAvgSwipes,0) as decimal) / CAST(wu.WeekDayAvgSwipes as decimal) * 100 as hhUsePercent 
, CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 as UsageSortPercent
, CAST(wu.Swipes as decimal) / CAST(weekdaySwipeSum.weekdaySwipeSum as decimal) * 100 as WeekdayUsePercent 
, wu.weekdayswipeRank
, CAST(CASE WHEN u.swipes = 0 Then 5
	WHEN CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 >= 80.001 THEN 1
	WHEN CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 >= 60.001 AND CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 <= 80 THEN 2
	WHEN CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 >= 40.001 AND CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 <= 60 THEN 3
	WHEN CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 >= 20.001 AND CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 <= 40 THEN 4
	WHEN CAST(NULLIF(u.swipes,0) as decimal) / CAST(us.maxSwipes as decimal) * 100 <= 20 THEN 5
	Else 10
  END as INT) as UsageRankSort   ---HalfHour
--, p.Ramping
, Dense_RANK() OVER (PARTITION BY u.[weekday], p.FifteenMinInterval order by p.upc_desc) as AvgSwipeFix
, CAST(u.WeekDayHHAvgSwipes as VARCHAR) + '  ' + CAST(CAST(CAST(NULLIF(u.WeekdayHHAvgSwipes,0) as decimal) / CAST(wu.WeekDayAvgSwipes as decimal) * 100 as decimal(2,0))as VARCHAR) + '%' as HHUseIntAndPercent
, u.[WeekDay] + '  ' + CAST(SUM(CASE WHEN p.parent_upc = '701592762528'THEN 1 ELSE 0 End) OVER (PARTITION BY u.[weekday]) as VARCHAR) + ' Signature of ' + CAST(COUNT(p.upc_desc) OVER (Partition BY u.[weekday]) as VARCHAR) + ' Classes' as WeekdayHeader
--, CAST(SUM(avg(NullIF(p.no_participants,0))) OVER (Partition By u.[weekday], p.hh) as decimal) / CAST(SUM(NULLIF(u.WeekDayHHAvgSwipes,0)) OVER (PARTITION BY u.[weekday], p.hh) as decimal) as PenDecimal
, p.datasource

INTO #Results
FRom #Usage u 
  JOIN #USort us 
    ON us.ClubID = u.ClubID 
  LEFT JOIN #Participation p
    ON u.ClubID = p.clubID
      AND u.FifteenMinInterval = p.FifteenMinInterval
      AND u.[WeekDay] = p.[weekday]
  LEFT JOIN #WeekdayUsage wu
    ON wu.ClubID = u.clubID
      AND wu.Weekdayname = u.[WeekDay]
  LEFT JOIN (SELECT SUM(u.swipes) as swipeSum, u.ClubID
			 FROM #Usage u
			 GROUP BY u.clubID) 
			 as swipeSum ON swipeSum.clubID = u.ClubID
  LEFT JOIN (SELECT SUM(wu.swipes) as weekdaySwipeSum, wu.clubid
			FROM #WeekdayUsage wu
			GROUP BY wu.ClubID)
			as weekdaySwipeSum ON weekdaySwipeSum.clubid = u.clubid
							   

GROUP BY
--  p.reservation
-- p.startdate
 u.[WeekDay]
, DATEPART(dw,p.participation_date)
--, u.halfhour
, u.FifteenMinInterval
, p.HH
, p.FifteenMinInterval
, u.WeekDayHHAvgSwipes
, u.display_24_hour_time
, u.display_12_hour_time
, p.startTime
, p.endTime
, u.swipes
, wu.Swipes
, p.clubID
, p.instructor
, p.capacity
, p.resource
, p.upc_desc
, p.upccode
, p.parent_upc
, wu.weekdayswipeRank
, swipeSum.swipeSum
, us.maxSwipes
, weekdaySwipeSum.weekdaySwipeSum
, p.datasource
, wu.weekdayavgswipes

--Fix for Cognos report
UPDATE #Results
SET weekdayhhavgswipes = 0 where avgswipefix > 1


---Here we are trying a few steps to correctly calculate the Penetration Percentage for a Given Day and 15 MIn Time Interval.
-- First we are trying to find the Number of Swipes for each 15 min interval for the Weekdays, and sort them for later use
select [weekday],display_24_hour_time, avg(Swipes) avg_swipes, DENSE_RANK() OVER ( ORDER BY [weekday],display_24_hour_time) rnk  
INTO #SwipesPer15MinsRank
from #Results 
group by [weekday],display_24_hour_time


-- Here, we are finding, for a given 15 min interval, the Total Number of Swipes 30 mins prior to it through 30 mins after that
-- That way, for a given 15 min interval, we would know the net Swipes that would contribute to our Penetration % Calculation

select [weekday],display_24_hour_time, 
ISNULL(avg_swipes,0) + 
ISNULL((lag(avg_swipes,1) over (order by rnk)),0) +
ISNULL((lead(avg_swipes,1) over (order by rnk)),0) +
ISNULL((lead(avg_swipes,2) over (order by rnk)),0) net_swipes
INTO #PenetrationTemp
 from #SwipesPer15MinsRank

 -- There are some duplicates in the #Results temp table, primarily arising when a UPC code is associated with more than 1 kits.
 -- That is causing the Penetration % Calculation is go wrong. So this Temp table is to get the distinct list of Participations 
 -- for a given 15 min timeslot,weekday, trainer

 select distinct avgParticipation,display_24_hour_time, weekday, startTime,instructor 
INTO #ResultDistinctClasses
from #Results 

-- Here we are calculating the Penetration %
 select distinct a.weekday,
 a.display_24_hour_time,
 CAST(SUM(NullIF(a.avgParticipation,0)) OVER (Partition By a.[weekday],a.display_24_hour_time) as float) / b.net_swipes as PenDecimal
 INTO #TempNewResults
 from #ResultDistinctClasses a 
 INNER JOIN #PenetrationTemp b on a.display_24_hour_time = b.display_24_hour_time
 and a.weekday = b.weekday

 --- FINAL OUTPUT
SELECT
a.[Rownumber]
, a.avgParticipation
, a.Capacitypercent 
, a.capacitySort 
, a.[WeekDay]
, a.weekdaySort
, a.startTime
, a.EndTime
, a.hh
, a.FifteenMinInterval
, a.FifteenMinInterval_Fact
, a.display_12_hour_time
, a.display_24_hour_time
, a.Swipes
, a.WeekDayHHAvgSwipes
, a.clubID
, a.instructor
, a.capacity
, a.resource
, a.upc_desc
, a.upccode
, a.Signature 
, a.hhUsePercent 
, a.UsageSortPercent
, a.WeekdayUsePercent 
, a.weekdayswipeRank
, a.UsageRankSort
, a.AvgSwipeFix
, a.HHUseIntAndPercent
, a.WeekdayHeader
, b.PenDecimal
, a.datasource
 from #Results a 
 JOIN  #TempNewResults b on  a.display_24_hour_time = b.display_24_hour_time and a.weekday = b.weekday


END
GRANT EXECUTE ON reporting.proc_GFScheduleOptimization TO CognosAnalyticsUser
