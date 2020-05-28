CREATE PROC [dbo].[proc_affinitech_camera_count_user] @StartDate [DATETIME] AS
BEGIN

DECLARE @EndDate DATETIME = CONVERT(CHAR(10), GETDATE(), 101) 

truncate table fact_affinitech_camera_count_user


if object_id('tempdb..#camera_count_temp') is not null drop table #camera_count_temp
create table #camera_count_temp with(distribution=hash(bk_hash),location=user_db) as
	SELECT 
		convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(c.Source_IP,'z#@$k%&P'))),2)  bk_hash,
		c.Source_IP,
		CAST(c.Start_Range as DATE) ClassDate,
		c.start_range FiveMin,
		SUM(CASE WHEN cam.cam_inverted = 1 THEN c.Exits-c.Enters ELSE c.Enters-c.Exits END) OVER (PARTITION BY cam.studio, 
		convert(DATE,Start_Range) ORDER BY start_range) As 'Count',
		RIGHT(cam.studio, LEN(cam.Studio) - 4) Studio,
		cam.cam_club_it club_indicator
	FROM dbo.d_affinitech_camera_count c
	LEFT JOIN dbo.d_affinitech_cameras cam 
		ON c.Source_IP = cam.CAM_IP
	WHERE CAST(c.Start_Range as DATE) >= @StartDate
		AND CAST(c.Start_Range as DATE) <= @EndDate
		and c.bk_hash not in ('-997','-998','-999') and cam.bk_hash not in ('-997','-998','-999') 

if object_id('tempdb..#Participation') is not null drop table #Participation
create table #Participation with(distribution=hash(bk_hash),location=user_db) as
	SELECT
	
/*  booking_recurrence.dim_exerp_booking_recurrence_key can be used instead of creating bk_hash as below */
		convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(booking.booking_id,'z#@$k%&P'))),2)  bk_hash,
		
		booking_recurrence.main_booking_id Booking_Recurrence_ID
		, booking.booking_id Booking_Instance_ID
		, ROW_NUMBER() OVER (ORDER BY club.domain_name_prefix, participation_date.calendar_date, resource_usage.resource_name, StartTime.minutes_after_midnight) Rownum
		, participation_date.calendar_date participation_date
		, booking.booking_state
		, resource_usage.booking_resource_usage_state
		, activity.activity_state
		, p.participation_state
		, NULL MOD_count
		
		, sum(case when p.participation_state ='PARTICIPATION' then 1 else 0 end) no_participants
		
		, club.club_name ClubName
		, club.club_code ClubCode
		, club.domain_name_prefix club_indicator
		, employee.first_name + ' ' + employee.last_name Instructor	
		, resource_usage.resource_name [resource]
		, StartDate.calendar_date StartDate
		, activity.activity_name upc_desc
		, activity.external_ID upc_Code
		, resource_usage.resource_id as resource_id
		, CAST(Dateadd(mi,StartTime.minutes_after_midnight,0)as Time) as StartTime
		, CAST(Dateadd(mi,EndTime.minutes_after_midnight,0)as Time) as EndTime

/*I am not sure of the time columns created below but if its date, then the concept is different. For ex date of prev class or date of next class or date of first and last class of the recurrence then recurrence date can be used with window functions*/
		
		,LAG(CAST(Dateadd(mi,EndTime.minutes_after_midnight,0)as Time),1,NULL) 
			OVER (ORDER BY club.domain_name_prefix, participation_date.calendar_date, resource_usage.resource_name, StartTime.minutes_after_midnight) as PrevClassEnd
		,LEAD(CAST(Dateadd(mi,StartTime.minutes_after_midnight,0)as Time),1,NULL) 
			OVER (ORDER BY club.domain_name_prefix, participation_date.calendar_date, resource_usage.resource_name, StartTime.minutes_after_midnight) as NextClassStart
		, MIN(CAST(DateAdd(Mi,StartTime.minutes_after_midnight,0)as TIME)) OVER (PARTITION BY club.domain_name_prefix , participation_date.calendar_date, resource_usage.resource_id) FirstClassStart
		, MAX(CAST(DateAdd(Mi,EndTime.minutes_after_midnight,0)as TIME)) OVER (PARTITION BY club.domain_name_prefix , participation_date.calendar_date, resource_usage.resource_id) LastClassEnd
		, CAST(DATEADD(mi,-30,starttime.display_12_hour_time)AS TIME) ThirtyBeforeStart
		, CAST(DATEADD(mi,+15,starttime.display_12_hour_time)AS TIME) FifteenAfterStart
		, CAST(DateAdd(Mi,+10,EndTime.display_12_hour_time)AS TIME) TenAfterEnd
		, CAST(DATEADD(mi,+30,EndTime.display_12_hour_time)AS TIME) ThirtyAfterEnd
		, StartDate.day_of_week_name [weekday]
		, 'Exerp' as source_system
/*COUNT(*) OVER (PARTITION BY club.domain_name_prefix, participation_date.calendar_date, StartTime.minutes_after_midnight,activity.activity_name ) as doubles*/
	FROM 
		marketing.v_dim_exerp_booking_recurrence booking_recurrence 
	JOIN 
		marketing.v_dim_exerp_booking booking
			ON booking.dim_exerp_booking_recurrence_key = booking_recurrence.dim_exerp_booking_recurrence_key
	LEFT Join 
		marketing.v_fact_exerp_participation p 
			ON booking.dim_exerp_booking_key = p.dim_exerp_booking_key 		
			AND p.participation_state <> 'CANCELLED'

/*I have commented the date filter here and included it in where condition		*/
	JOIN marketing.v_dim_date participation_date
		ON participation_date.dim_date_key = booking.start_dim_date_key
			/*AND (participation_date.calendar_date BETWEEN @StartDate AND @EndDate OR participation_date.calendar_date IS NULL) */
		and booking.start_dim_date_key not in ('-997','-998','-999')

	LEFT JOIN marketing.v_dim_date StartDate
		ON Startdate.dim_date_key = booking_recurrence.recurrence_start_dim_date_key 
			and booking_recurrence.recurrence_start_dim_date_key not in ('-997','-998','-999')
	LEFT Join marketing.v_dim_time StartTime
		ON Starttime.dim_time_key = booking_recurrence.recurrence_start_dim_time_key 
		and booking_recurrence.recurrence_start_dim_time_key not in ('-997','-998','-999')
	LEFT Join marketing.v_dim_time EndTime
		ON Endtime.dim_time_key = booking_recurrence.recurrence_end_dim_time_key 
		and booking_recurrence.recurrence_end_dim_time_key not in ('-997','-998','-999')
	LEFT JOIN marketing.v_Dim_club club
		ON club.dim_club_key = booking.dim_club_key 
	Join marketing.v_dim_exerp_activity activity
		ON activity.dim_exerp_activity_key = booking.dim_exerp_activity_key
		AND activity.activity_state = 'ACTIVE'
		
/*use dim_exerp_booking_key instead of booking_id*/
	Left JOIN marketing.v_dim_exerp_booking_resource_usage resource_usage
		/*ON resource_usage.booking_id = booking.booking_id*/
		ON resource_usage.dim_exerp_booking_key = booking.dim_exerp_booking_key
		AND resource_usage.booking_resource_usage_state = 'ACTIVE'
	
	LEFT JOIN marketing.v_dim_boss_product boss_product
		ON boss_product.dim_boss_product_key = activity.dim_boss_product_key

	LEFT Join marketing.v_dim_exerp_staff_usage staff_usage_booking	
		ON staff_usage_booking.dim_exerp_booking_key = booking.dim_exerp_booking_key
		AND staff_usage_booking.staff_usage_state ='ACTIVE'
	LEFT join marketing.v_dim_employee employee
		ON employee.dim_employee_key = staff_usage_booking.dim_employee_key		

	Where 

/*club.domain_name_prefix IN (@Club)*/
		booking.booking_state != 'CANCELLED'
		AND boss_product.department_description IN ('GROUP FITNESS                 ', 'MARTIAL ARTS                  ', 'MY EVENT                      ')
		AND resource_usage.resource_name NOT LIKE '%Pool%'
		and booking_recurrence.dim_exerp_booking_recurrence_key not in ('-997','-998','-999') 
		
/*filter condition taken from join above*/
		and participation_date.calendar_date BETWEEN @StartDate AND @EndDate
		
	Group BY 
		participation_date.calendar_date 
		, club.club_name
		, club.club_code
		, club.domain_name_prefix
		, employee.first_name + ' ' + employee.last_name
		, resource_usage.resource_name
		, StartDate.calendar_date
		, activity.activity_name
		, activity.external_ID
		, resource_usage.resource_id
		, StartTime.display_12_hour_time
		, EndTime.display_12_hour_time
		, StartTime.minutes_after_midnight
		, EndTime.minutes_after_midnight
		, StartDate.day_of_week_name
		, booking_recurrence.main_booking_id
		, booking.booking_id
		, booking.booking_state
		, resource_usage.booking_resource_usage_state
		, activity.activity_state
		, p.participation_state



if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table #etl_step_1 with(distribution=hash(bk_hash),location=user_db) as
	Select distinct
		convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(p.Booking_Recurrence_ID,'z#@$k%&P'))),2)  bk_hash,
		p.ClubName,
		p.ClubCode,
		p.club_indicator,
		t.ClassDate,
		t.Studio,
		p.resource,
		p.upc_desc,
		p.upc_Code,
		p.Booking_Recurrence_ID,
		p.Booking_Instance_ID,
		P.NextclassStart,
		P.PrevclassEnd,
		CAST(p.StartTime as DATETIME) StartTime,
		CAST(p.EndTime as DATETIME) EndTime,
		p.no_participants as InstructorCount,
		p.MOD_count,
		p.source_system,
		MIN(case when p.ThirtyBeforeStart<= CAST(t.fivemin as time) then t.count  end )  as ExitCount,   
		MIN(case when p.ThirtyBeforeStart<= CAST(t.fivemin as time) then t.count  end ) as MinCount,  /*Between 30 minutes before class and end of class*/
		MAX(case when p.StartTime<= CAST(t.fivemin as time) then t.count end) - MIN(case when p.ThirtyBeforeStart<= CAST(t.fivemin as time) then t.count end ) As CameraCount,
		Max(case when p.StartTime<= CAST(t.fivemin as time) then t.count end) as MaxCount, /*-Max count between start and end of class*/
		DATEDIFF(MINUTE, p.startTime, P.PrevclassEnd) MinutesFromLastClass,
		DATEDIFF(MINUTE, p.EndTime, P.NextclassStart) MinutesToNextClass,
		'Max - Min' as Logic
	FROM #Participation p
		LEFT JOIN #camera_count_temp t
			ON p.participation_date = t.ClassDate /*-Calc for Max Count*/
			AND p.Resource = t.Studio
			AND P.club_indicator = t.club_indicator
			and (p.StartTime<= CAST(t.fivemin as time) or p.ThirtyBeforeStart<= CAST(t.fivemin as time))
			and p.EndTime >= CAST(t.fivemin as time) 

		WHERE (ABS(DATEDIFF(MINUTE,p.startTime, P.PrevclassEnd)) >= 60 OR P.PrevclassEnd IS NULL)

	GROUP BY 
	p.ClubName,
	p.ClubCode,
	p.club_indicator,
	t.ClassDate,
	t.Studio,
	p.resource,
	p.upc_desc,
	p.upc_Code,
	p.Booking_Recurrence_ID,
	p.Booking_Instance_ID,
	P.NextclassStart,
	P.PrevclassEnd,
	p.StartTime,
	p.EndTime,
	p.no_participants,
	p.MOD_count,
	p.source_system
	

begin tran	
	insert into fact_affinitech_camera_count_user
		(club_name,club_code,class_date,resource,upc_code,upc_desc,booking_reference_id,booking_instance_id,start_time,end_time,instructor_count,camera_count)
	SELECT 
	r.ClubName club_name,
	r.ClubCode club_code,
	r.classdate class_date,
	r.resource,
	r.upc_code,
	r.upc_desc,
	r.Booking_Recurrence_ID booking_reference_id,
	r.Booking_Instance_ID booking_instance_id,
	cast(r.starttime as time) start_time,
	cast(r.endtime as time) end_time,
	r.instructorcount instructor_count,
	r.CameraCount camera_count
	FROM #etl_step_1 r
commit tran		

if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table #etl_step_2 with(distribution=hash(bk_hash),location=user_db) as
	/*Scenario 2: Class within 60 of start time, and No class within 60 after*/
	Select  distinct 
		convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(p.Booking_Recurrence_ID,'z#@$k%&P'))),2)  bk_hash,
		p.ClubName,
		p.ClubCode,
		p.club_indicator,
		t.ClassDate,
		t.Studio,
		p.resource,
		p.upc_desc,
		p.upc_Code,
		p.Booking_Recurrence_ID,
		p.Booking_Instance_ID,
		P.NextclassStart,
		P.PrevclassEnd,
		CAST(p.StartTime as DATETIME) StartTime,
		CAST(p.EndTime as DATETIME) EndTime,
		p.no_participants as InstructorCount,
		p.MOD_count,
		p.source_system,
		MIN(case when DATEADD(minute,-30,p.FirstClassStart) <= CAST(t.fivemin as time) then t.count end ) as ExitCount,
		MIN(case when p.StartTime<= CAST(t.fivemin as time) then t.count end) as MinCount,
		MAX(case when p.StartTime<= CAST(t.fivemin as time) then t.count end) - 
			MIN(case when DATEADD(minute,-30,p.FirstClassStart) <= CAST(t.fivemin as time) then t.count end )  As CameraCount,
		Max(case when p.StartTime<= CAST(t.fivemin as time) then t.count end) as MaxCount, /*-Max count between start and end of class*/
		DATEDIFF(MINUTE, p.startTime, P.PrevclassEnd) MinutesFromLastClass,
		DATEDIFF(MINUTE, p.EndTime, P.NextclassStart) MinutesToNextClass,
		'Max - ExitCount' as Logic
 
	FROM #Participation p
		LEFT JOIN #camera_count_temp t
		ON p.participation_date = t.ClassDate /*-Calc for Max Count*/
		AND p.Resource = t.Studio
		AND P.club_indicator = t.club_indicator
		
		and (p.StartTime<= CAST(t.fivemin as time) or DATEADD(minute,-30,p.FirstClassStart) <= CAST(t.fivemin as time))
		and p.EndTime >= CAST(t.fivemin as time)
		
	WHERE (ABS(DATEDIFF(MINUTE, p.startTime, P.PrevclassEnd)) < 60)
		AND (ABS(DATEDIFF(MINUTE, p.EndTime, P.NextclassStart)) <= 60)

	GROUP BY 
	p.ClubName,
	p.ClubCode,
	p.club_indicator,
	t.ClassDate,
	t.Studio,
	p.resource,
	p.upc_desc,
	p.upc_Code,
		p.Booking_Recurrence_ID,
		p.Booking_Instance_ID,
	P.NextclassStart,
	P.PrevclassEnd,
	p.StartTime,
	p.EndTime,
	p.no_participants,
	p.MOD_count,
	p.source_system 		
	
	
begin tran
	insert into fact_affinitech_camera_count_user
		(club_name,club_code,class_date,resource,upc_code,upc_desc,booking_reference_id,booking_instance_id,start_time,end_time,instructor_count,camera_count)
	SELECT 
	r.ClubName club_name,
	r.ClubCode club_code,
	r.classdate class_date,
	r.resource,
	r.upc_code,
	r.upc_desc,
	r.Booking_Recurrence_ID booking_reference_id,
	r.Booking_Instance_ID booking_instance_id,
	cast(r.starttime as time) start_time,
	cast(r.endtime as time) end_time,
	r.instructorcount instructor_count,
	r.CameraCount camera_count
	FROM #etl_step_2 r
commit tran	

if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table #etl_step_3 with(distribution=hash(bk_hash),location=user_db) as
	/*Scenario 3: Class Within 60 of StartTime and Within 60 of EndTime*/
	Select distinct
		convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(p.Booking_Recurrence_ID,'z#@$k%&P'))),2)  bk_hash,	
		p.ClubName,
		p.ClubCode,
		p.club_indicator,
		t.ClassDate,
		t.Studio,
		p.resource,
		p.upc_desc,
		p.upc_Code,
		p.Booking_Recurrence_ID,
		p.Booking_Instance_ID,
		P.NextclassStart,
		P.PrevclassEnd,
		CAST(p.StartTime as DATETIME) StartTime,
		CAST(p.EndTime as DATETIME) EndTime,
		p.no_participants as InstructorCount,
		p.MOD_count,
		p.source_system,
		MIN(case when p.FifteenAfterStart <= CAST(t.fivemin as time) and DATEADD(minute,30,p.LastClassEnd) >= CAST(t.fivemin as time)  then t.count end ) as ExitCount,
		MIN(case when DATEADD(minute,-30,p.FirstClassStart) <= CAST(t.fivemin as time) and p.EndTime >= CAST(t.fivemin as time) then t.count end ) as MinCount,  /*Between 30 minutes before class and end of class*/
		(
			MAX(case when p.StartTime<= CAST(t.fivemin as time) and  p.EndTime >= CAST(t.fivemin as time) then t.count end)-
			MIN(case when DATEADD(minute,-30,p.FirstClassStart) <= CAST(t.fivemin as time) and p.EndTime >= CAST(t.fivemin as time) then t.count end )
			+
			MAX(case when p.StartTime<= CAST(t.fivemin as time) and  p.EndTime >= CAST(t.fivemin as time) then t.count end)-
			MIN(case when p.FifteenAfterStart <= CAST(t.fivemin as time) and DATEADD(minute,30,p.LastClassEnd) >= CAST(t.fivemin as time) then t.count end)
		) / 2  As CameraCount,
		
		MAX(case when p.StartTime<= CAST(t.fivemin as time) and  p.EndTime >= CAST(t.fivemin as time) then t.count end) as MaxCount, /*-Max count between start and end of class*/
		DATEDIFF(MINUTE, p.startTime, P.PrevclassEnd) MinutesFromLastClass,
		DATEDIFF(MINUTE, p.EndTime, P.NextclassStart) MinutesToNextClass,
		'((Max-Min)+(Max-Exit))/2' as Logic

	FROM #Participation p
		LEFT JOIN #camera_count_temp t
		ON p.participation_date = t.ClassDate /*-Calc for Max Count*/
		AND p.Resource = t.Studio
		AND P.club_indicator = t.club_indicator
		
		and (
		((p.StartTime<= CAST(t.fivemin as time) or DATEADD(minute,-30,p.FirstClassStart) <= CAST(t.fivemin as time))
		and p.EndTime >= CAST(t.fivemin as time))
		or
		(p.FifteenAfterStart <= CAST(t.fivemin as time) and DATEADD(minute,30,p.LastClassEnd) >= CAST(t.fivemin as time))
		)
	      
	WHERE (ABS(DATEDIFF(MINUTE, p.startTime, P.PrevclassEnd)) < 60)
		AND (ABS(DATEDIFF(MINUTE, p.EndTime, P.NextclassStart)) <= 60)

	GROUP BY 
	p.ClubName,
	p.ClubCode,
	p.club_indicator,
	t.ClassDate,
	t.Studio,
	p.resource,
	p.upc_desc,
	p.upc_Code,
	p.Booking_Recurrence_ID,
	p.Booking_Instance_ID,
	P.NextclassStart,
	P.PrevclassEnd,
	p.StartTime,
	p.EndTime,
	p.no_participants,
	p.MOD_count,
	p.source_system


begin tran
	insert into fact_affinitech_camera_count_user
		(club_name,club_code,class_date,resource,upc_code,upc_desc,booking_reference_id,booking_instance_id,start_time,end_time,instructor_count,camera_count)
	SELECT 
	r.ClubName club_name,
	r.ClubCode club_code,
	r.classdate class_date,
	r.resource,
	r.upc_code,
	r.upc_desc,
	r.Booking_Recurrence_ID booking_reference_id,
	r.Booking_Instance_ID booking_instance_id,
	cast(r.starttime as time) start_time,
	cast(r.endtime as time) end_time,
	r.instructorcount instructor_count,
	r.CameraCount camera_count
	FROM #etl_step_3 r
commit tran	

			
END
