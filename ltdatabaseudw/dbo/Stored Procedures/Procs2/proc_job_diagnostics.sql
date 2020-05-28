CREATE PROC [dbo].[proc_job_diagnostics] AS
declare @begindate datetime
declare @now datetime
set @now = getdate()
set @begindate = dateadd(day,-7,@now)

BEGIN TRY
/* get all jobs in history */
select distinct job_name into #all_job_names from dbo.dv_job_status_history 

/* 90 day job runtimes */
select *, datediff(second,job_start_date_time, job_end_date_time) as runtime into #runtimes_90 from dbo.dv_job_status_history
where job_start_date_time >= dateadd(day,-90,getdate())
and job_status like 'Complete'

/* 30 day job runtimes */
select *, datediff(second,job_start_date_time, job_end_date_time) as runtime into #runtimes_30 from dbo.dv_job_status_history
where job_start_date_time >= dateadd(day,-30,getdate())
and job_status like 'Complete'

/* 7 day job runtimes */
select job_name, job_start_date_time, job_end_date_time, datediff(second,job_start_date_time, job_end_date_time) as runtime, job_status into #runtimes_7 from dbo.dv_job_status_history
where job_start_date_time between @begindate and @now
and job_status like 'Complete'

/* day of the week job runtimes */
select * into #runtimes_DOW from(
select job_name, job_status, job_start_date_time, job_end_date_time, datediff(second,job_start_date_time, job_end_date_time) as runtime, DATENAME(dw,job_start_date_time) as day_of_week 
from dbo.dv_job_status_history) t1
where day_of_week like DATENAME(dw,@now)
and year(job_start_date_time) >= year(@now)-1
and job_status like 'Complete'

/* day of the month job runtimes */
select *, datediff(second,job_start_date_time, job_end_date_time) as runtime into #runtimes_DOM from dbo.dv_job_status_history
where year(job_start_date_time) >= year(@now)-2
and day(job_start_date_time) = day(@now)
and job_status like 'Complete'
END TRY
BEGIN CATCH  
Print 'Error Occurred in Initial Data Collection:'  
Print Error_Message()  
END CATCH 

BEGIN TRY
/* aggregate functions for each timeframe */
select job_name, STDEV(runtime) as standard_deviation, AVG(runtime) as average_runtime into #jobstats_90 from #runtimes_90 group by job_name
select job_name, STDEV(runtime) as standard_deviation, AVG(runtime) as average_runtime into #jobstats_30 from #runtimes_30 group by job_name
select job_name, STDEV(runtime) as standard_deviation, AVG(runtime) as average_runtime into #jobstats_7 from #runtimes_7 group by job_name
select job_name, STDEV(runtime) as standard_deviation, AVG(runtime) as average_runtime into #jobstats_DOW from #runtimes_DOW group by job_name
select job_name, STDEV(runtime) as standard_deviation, AVG(runtime) as average_runtime into #jobstats_DOM from #runtimes_DOM group by job_name
END TRY
BEGIN CATCH  
Print 'Error Occurred in Initial Data Aggregation:'  
Print Error_Message()  
END CATCH 

BEGIN TRY
select #runtimes_90.job_name, #runtimes_90.runtime, #jobstats_90.standard_deviation, #jobstats_90.average_runtime into #allstats_90 from #runtimes_90
inner join #jobstats_90
on #runtimes_90.job_name = #jobstats_90.job_name

select #runtimes_30.job_name, #runtimes_30.runtime, #jobstats_30.standard_deviation, #jobstats_30.average_runtime into #allstats_30 from #runtimes_30
inner join #jobstats_30
on #runtimes_30.job_name = #jobstats_30.job_name

select #runtimes_7.job_name, #runtimes_7.runtime, #jobstats_7.standard_deviation, #jobstats_7.average_runtime into #allstats_7 from #runtimes_7
inner join #jobstats_7
on #runtimes_7.job_name = #jobstats_7.job_name

select #runtimes_DOW.job_name, #runtimes_DOW.runtime, #jobstats_DOW.standard_deviation, #jobstats_DOW.average_runtime into #allstats_DOW from #runtimes_DOW
inner join #jobstats_DOW
on #runtimes_DOW.job_name = #jobstats_DOW.job_name

select #runtimes_DOM.job_name, #runtimes_DOM.runtime, #jobstats_DOM.standard_deviation, #jobstats_DOM.average_runtime into #allstats_DOM from #runtimes_DOM
inner join #jobstats_DOM
on #runtimes_DOM.job_name = #jobstats_DOM.job_name 

select *, ((runtime - average_runtime) / (standard_deviation)) as zscore into #zscores_90 from #allstats_90 where standard_deviation != 0
select *, ((runtime - average_runtime) / (standard_deviation)) as zscore into #zscores_30 from #allstats_30 where standard_deviation != 0
select *, ((runtime - average_runtime) / (standard_deviation)) as zscore into #zscores_7 from #allstats_7 where standard_deviation != 0
select *, ((runtime - average_runtime) / (standard_deviation)) as zscore into #zscores_DOW from #allstats_DOW where standard_deviation != 0
select *, ((runtime - average_runtime) / (standard_deviation)) as zscore into #zscores_DOM from #allstats_DOM where standard_deviation != 0
END TRY
BEGIN CATCH  
Print 'Error Occurred in Z-Score Creation:'  
Print Error_Message()  
END CATCH

BEGIN TRY
select job_name, AVG(runtime) as average_runtime_90, CAST(STDEV(runtime) as INT) as standard_deviation_90 into #final_90
from #zscores_90
where ABS(zscore) < 1
group by job_name

select job_name, AVG(runtime) as average_runtime_30, CAST(STDEV(runtime) as INT) as standard_deviation_30 into #final_30
from #zscores_30
where ABS(zscore) < 1
group by job_name

select job_name, AVG(runtime) as average_runtime_7, CAST(STDEV(runtime) as INT) as standard_deviation_7 into #final_7
from #zscores_7
where ABS(zscore) < 1
group by job_name

select job_name, AVG(runtime) as average_runtime_DOW, CAST(STDEV(runtime) as INT) as standard_deviation_DOW into #final_DOW
from #zscores_DOW
where ABS(zscore) < 1
group by job_name

select job_name, AVG(runtime) as average_runtime_DOM, CAST(STDEV(runtime) as INT) as standard_deviation_DOM into #final_DOM
from #zscores_DOM
where ABS(zscore) < 1
group by job_name
END TRY
BEGIN CATCH  
Print 'Error Occurred in Z-Score Selection:'  
Print Error_Message()  
END CATCH

IF OBJECT_ID('dbo.dv_job_diagnostics', 'U') IS NULL 
BEGIN
BEGIN TRY
select job_name,average_runtime_7,standard_deviation_7,average_runtime_30,standard_deviation_30,average_runtime_90,standard_deviation_90,average_runtime_DOW,standard_deviation_DOW,average_runtime_DOM,standard_deviation_DOM,volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM,
(COALESCE(volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM) + COALESCE(volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM) + COALESCE(volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM) + COALESCE(volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM)
+ COALESCE(volatility_30,volatility_90,volatility_DOW,volatility_DOM,volatility_7) + COALESCE(volatility_30,volatility_90,volatility_DOW,volatility_DOM,volatility_7) + COALESCE(volatility_30,volatility_90,volatility_DOW,volatility_DOM,volatility_7)
+ COALESCE(volatility_90,volatility_DOW,volatility_DOM,volatility_7,volatility_30) + COALESCE(volatility_90,volatility_DOW,volatility_DOM,volatility_7,volatility_30)
+ COALESCE(volatility_DOW,volatility_DOM,volatility_7,volatility_30,volatility_90) 
+ COALESCE(volatility_DOM,volatility_7,volatility_30,volatility_90,volatility_DOW))/11 as average_volatility, date_evaluated into dbo.dv_job_diagnostics
from
(select #all_job_names.job_name,
#final_7.average_runtime_7, #final_7.standard_deviation_7, CASE WHEN #final_7.standard_deviation_7 = 0 THEN NULL ELSE CAST(#final_7.standard_deviation_7 as float)/#final_7.average_runtime_7 END as volatility_7,
#final_30.average_runtime_30,#final_30.standard_deviation_30, CASE WHEN #final_30.standard_deviation_30 = 0 THEN NULL ELSE CAST(#final_30.standard_deviation_30 as float)/#final_30.average_runtime_30 END as volatility_30,
#final_90.average_runtime_90,#final_90.standard_deviation_90, CASE WHEN #final_90.standard_deviation_90 = 0 THEN NULL ELSE CAST(#final_90.standard_deviation_90 as float)/#final_90.average_runtime_90 END as volatility_90,
#final_DOW.average_runtime_DOW,#final_DOW.standard_deviation_DOW, CASE WHEN standard_deviation_DOW = 0 THEN NULL ELSE CAST(standard_deviation_DOW as float)/average_runtime_DOW END as volatility_DOW,
#final_DOM.average_runtime_DOM,#final_DOM.standard_deviation_DOM, CASE WHEN standard_deviation_DOM = 0 THEN NULL ELSE CAST(standard_deviation_DOM as float)/average_runtime_DOM END as volatility_DOM,
CAST(getdate() AS date) as date_evaluated from #all_job_names
left join #final_7 on #final_7.job_name = #all_job_names.job_name
left join #final_30 on #final_30.job_name = #all_job_names.job_name
left join #final_90 on #final_90.job_name = #all_job_names.job_name
left join #final_DOW on #final_DOW.job_name = #all_job_names.job_name
left join #final_DOM on #final_DOM.job_name = #all_job_names.job_name) as t1
END TRY
BEGIN CATCH  
Print 'Error Occurred in Statistical Conditional Table Creation:'  
Print Error_Message()  
END CATCH
END
ELSE
BEGIN TRY
insert into dbo.dv_job_diagnostics
select job_name,average_runtime_7,standard_deviation_7,average_runtime_30,standard_deviation_30,average_runtime_90,standard_deviation_90,average_runtime_DOW,standard_deviation_DOW,average_runtime_DOM,standard_deviation_DOM,volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM,
(COALESCE(volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM) + COALESCE(volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM) + COALESCE(volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM) + COALESCE(volatility_7,volatility_30,volatility_90,volatility_DOW,volatility_DOM)
+ COALESCE(volatility_30,volatility_90,volatility_DOW,volatility_DOM,volatility_7) + COALESCE(volatility_30,volatility_90,volatility_DOW,volatility_DOM,volatility_7) + COALESCE(volatility_30,volatility_90,volatility_DOW,volatility_DOM,volatility_7)
+ COALESCE(volatility_90,volatility_DOW,volatility_DOM,volatility_7,volatility_30) + COALESCE(volatility_90,volatility_DOW,volatility_DOM,volatility_7,volatility_30)
+ COALESCE(volatility_DOW,volatility_DOM,volatility_7,volatility_30,volatility_90) 
+ COALESCE(volatility_DOM,volatility_7,volatility_30,volatility_90,volatility_DOW))/11 as average_volatility, date_evaluated
from
(select #all_job_names.job_name,
#final_7.average_runtime_7, #final_7.standard_deviation_7, CASE WHEN #final_7.standard_deviation_7 = 0 THEN NULL ELSE CAST(#final_7.standard_deviation_7 as float)/#final_7.average_runtime_7 END as volatility_7,
#final_30.average_runtime_30,#final_30.standard_deviation_30, CASE WHEN #final_30.standard_deviation_30 = 0 THEN NULL ELSE CAST(#final_30.standard_deviation_30 as float)/#final_30.average_runtime_30 END as volatility_30,
#final_90.average_runtime_90,#final_90.standard_deviation_90, CASE WHEN #final_90.standard_deviation_90 = 0 THEN NULL ELSE CAST(#final_90.standard_deviation_90 as float)/#final_90.average_runtime_90 END as volatility_90,
#final_DOW.average_runtime_DOW,#final_DOW.standard_deviation_DOW, CASE WHEN standard_deviation_DOW = 0 THEN NULL ELSE CAST(standard_deviation_DOW as float)/average_runtime_DOW END as volatility_DOW,
#final_DOM.average_runtime_DOM,#final_DOM.standard_deviation_DOM, CASE WHEN standard_deviation_DOM = 0 THEN NULL ELSE CAST(standard_deviation_DOM as float)/average_runtime_DOM END as volatility_DOM,
CAST(getdate() AS date) as date_evaluated from #all_job_names
left join #final_7 on #final_7.job_name = #all_job_names.job_name
left join #final_30 on #final_30.job_name = #all_job_names.job_name
left join #final_90 on #final_90.job_name = #all_job_names.job_name
left join #final_DOW on #final_DOW.job_name = #all_job_names.job_name
left join #final_DOM on #final_DOM.job_name = #all_job_names.job_name) as t1
END TRY
BEGIN CATCH  
Print 'Error Occurred in Statistical Conditional Existed Table Insertion:'  
Print Error_Message()  
END CATCH

IF OBJECT_ID('dbo.dv_job_diagnostics_labeled', 'U') IS NOT NULL 
BEGIN
insert into dbo.dv_job_diagnostics_labeled
select job_name,
CASE 
WHEN (average_runtime_7 > 100)
THEN
	CASE
		WHEN average_runtime_7 IS NULL or average_runtime_30 IS NULL or average_runtime_90 IS NULL THEN 'NOT ENOUGH HISTORY'
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 > 1.1 and CAST(average_runtime_7 as float)/average_runtime_30 < 1.2 and CAST(average_runtime_7 as float)/average_runtime_90 > 1.15 and CAST(average_runtime_7 as float)/average_runtime_90 < 1.25) THEN 'LONG' 
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 >= 1.2 and CAST(average_runtime_7 as float)/average_runtime_90 >= 1.25) THEN 'NEEDS ATTENTION'
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 < 0.8 and CAST(average_runtime_7 as float)/average_runtime_90 < 0.75) THEN 'SHORT' 
		ELSE 'NORMAL' 
	END	
ELSE
CASE
		WHEN average_runtime_7 IS NULL or average_runtime_30 = NULL or average_runtime_90 IS NULL THEN 'NOT ENOUGH HISTORY'
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 > 1.3 and CAST(average_runtime_7 as float)/average_runtime_30 < 1.4 and CAST(average_runtime_7 as float)/average_runtime_90 > 1.35 and CAST(average_runtime_7 as float)/average_runtime_90 < 1.45) THEN 'LONG' 
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 >= 1.4 and CAST(average_runtime_7 as float)/average_runtime_90 >= 1.45) THEN 'NEEDS ATTENTION'
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 < 0.6 and CAST(average_runtime_7 as float)/average_runtime_90 < 0.55) THEN 'SHORT' 
		ELSE 'NORMAL' 
	END	
END	
as ninety_day_runtime_trend,
CASE 
WHEN (average_runtime_7 > 100)
THEN
	CASE
		WHEN (average_runtime_DOW IS NULL or average_runtime_DOW = 0 or average_runtime_7 IS NULL or average_runtime_7 = 0) THEN 'NOT ENOUGH HISTORY'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 > 1.2 and CAST(average_runtime_DOW as float)/average_runtime_7 < 1.3 THEN 'LONG'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 >= 1.3 THEN 'NEEDS ATTENTION'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 < 0.8 THEN 'SHORT'
		ELSE 'NORMAL'
	END
ELSE
	CASE
		WHEN (average_runtime_DOW IS NULL or average_runtime_DOW = 0 or average_runtime_7 IS NULL or average_runtime_7 = 0) THEN 'NOT ENOUGH HISTORY'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 > 1.3 and CAST(average_runtime_DOW as float)/average_runtime_7 < 1.4 THEN 'LONG'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 >= 1.4 THEN 'NEEDS ATTENTION'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 < 0.70 THEN 'SHORT'
		ELSE 'NORMAL'
	END
END
as weekday_runtime_trend,
CASE 
WHEN (average_runtime_7 > 100)
THEN
	CASE
		WHEN average_runtime_DOM IS NULL or average_runtime_DOM = 0 or average_runtime_7 IS NULL or average_runtime_7 = 0 THEN 'NOT ENOUGH HISTORY'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 > 1.2 and CAST(average_runtime_DOM as float)/average_runtime_7 < 1.3 THEN 'LONG'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 >= 1.3 THEN 'NEEDS ATTENTION'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 < 0.8 THEN 'SHORT'
		ELSE 'NORMAL'
	END
ELSE
	CASE
		WHEN average_runtime_DOM IS NULL or average_runtime_DOM = 0 or average_runtime_7 IS NULL or average_runtime_7 = 0 THEN 'NOT ENOUGH HISTORY'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 > 1.3 and CAST(average_runtime_DOM as float)/average_runtime_7 < 1.4 THEN 'LONG'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 >= 1.4 THEN 'NEEDS ATTENTION'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 < 0.70 THEN 'SHORT'
		ELSE 'NORMAL'
	END
END
as day_of_month_runtime_trend,
CASE 
WHEN (average_runtime_7 > 100)
THEN
	CASE
		WHEN average_runtime_7 IS NULL or average_runtime_7 = 0 THEN 'NOT ENOUGH HISTORY'
		WHEN average_volatility < 0.15 THEN 'LOW'
		WHEN average_volatility >= 0.15 and average_volatility < 0.30 THEN 'MODERTATE'
		ELSE 'HIGH'
	END
ELSE
	CASE
		WHEN average_runtime_7 IS NULL or average_runtime_7 = 0 THEN 'NOT ENOUGH HISTORY'
		WHEN average_volatility < 0.2 THEN 'LOW'
		WHEN average_volatility >= 0.2 and average_volatility < 0.4 THEN 'MODERTATE'
		ELSE 'HIGH'
	END
END
as volatility_rating,
CAST(getdate() AS date) as date_evaluated
from dbo.dv_job_diagnostics
where date_evaluated = CAST(getdate() as date)
END
ELSE
BEGIN
select job_name,
CASE 
WHEN (average_runtime_7 > 100)
THEN
	CASE
		WHEN average_runtime_7 IS NULL or average_runtime_30 IS NULL or average_runtime_90 IS NULL THEN 'NOT ENOUGH HISTORY'
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 > 1.1 and CAST(average_runtime_7 as float)/average_runtime_30 < 1.2 and CAST(average_runtime_7 as float)/average_runtime_90 > 1.15 and CAST(average_runtime_7 as float)/average_runtime_90 < 1.25) THEN 'LONG' 
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 >= 1.2 and CAST(average_runtime_7 as float)/average_runtime_90 >= 1.25) THEN 'NEEDS ATTENTION'
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 < 0.8 and CAST(average_runtime_7 as float)/average_runtime_90 < 0.75) THEN 'SHORT' 
		ELSE 'NORMAL' 
	END	
ELSE
CASE
		WHEN average_runtime_7 IS NULL or average_runtime_30 = NULL or average_runtime_90 IS NULL THEN 'NOT ENOUGH HISTORY'
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 > 1.3 and CAST(average_runtime_7 as float)/average_runtime_30 < 1.4 and CAST(average_runtime_7 as float)/average_runtime_90 > 1.35 and CAST(average_runtime_7 as float)/average_runtime_90 < 1.45) THEN 'LONG' 
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 >= 1.4 and CAST(average_runtime_7 as float)/average_runtime_90 >= 1.45) THEN 'NEEDS ATTENTION'
		WHEN (CAST(average_runtime_7 as float)/average_runtime_30 < 0.6 and CAST(average_runtime_7 as float)/average_runtime_90 < 0.55) THEN 'SHORT' 
		ELSE 'NORMAL' 
	END	
END	
as ninety_day_runtime_trend,
CASE 
WHEN (average_runtime_7 > 100)
THEN
	CASE
		WHEN (average_runtime_DOW IS NULL or average_runtime_DOW = 0 or average_runtime_7 IS NULL or average_runtime_7 = 0) THEN 'NOT ENOUGH HISTORY'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 > 1.2 and CAST(average_runtime_DOW as float)/average_runtime_7 < 1.3 THEN 'LONG'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 >= 1.3 THEN 'NEEDS ATTENTION'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 < 0.8 THEN 'SHORT'
		ELSE 'NORMAL'
	END
ELSE
	CASE
		WHEN (average_runtime_DOW IS NULL or average_runtime_DOW = 0 or average_runtime_7 IS NULL or average_runtime_7 = 0) THEN 'NOT ENOUGH HISTORY'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 > 1.3 and CAST(average_runtime_DOW as float)/average_runtime_7 < 1.4 THEN 'LONG'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 >= 1.4 THEN 'NEEDS ATTENTION'
		WHEN CAST(average_runtime_DOW as float)/average_runtime_7 < 0.70 THEN 'SHORT'
		ELSE 'NORMAL'
	END
END
as weekday_runtime_trend,
CASE 
WHEN (average_runtime_7 > 100)
THEN
	CASE
		WHEN average_runtime_DOM IS NULL or average_runtime_DOM = 0 or average_runtime_7 IS NULL or average_runtime_7 = 0 THEN 'NOT ENOUGH HISTORY'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 > 1.2 and CAST(average_runtime_DOM as float)/average_runtime_7 < 1.3 THEN 'LONG'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 >= 1.3 THEN 'NEEDS ATTENTION'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 < 0.8 THEN 'SHORT'
		ELSE 'NORMAL'
	END
ELSE
	CASE
		WHEN average_runtime_DOM IS NULL or average_runtime_DOM = 0 or average_runtime_7 IS NULL or average_runtime_7 = 0 THEN 'NOT ENOUGH HISTORY'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 > 1.3 and CAST(average_runtime_DOM as float)/average_runtime_7 < 1.4 THEN 'LONG'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 >= 1.4 THEN 'NEEDS ATTENTION'
		WHEN CAST(average_runtime_DOM as float)/average_runtime_7 < 0.70 THEN 'SHORT'
		ELSE 'NORMAL'
	END
END
as day_of_month_runtime_trend,
CASE 
WHEN (average_runtime_7 > 100)
THEN
	CASE
		WHEN average_runtime_7 IS NULL or average_runtime_7 = 0 THEN 'NOT ENOUGH HISTORY'
		WHEN average_volatility < 0.15 THEN 'LOW'
		WHEN average_volatility >= 0.15 and average_volatility < 0.30 THEN 'MODERTATE'
		ELSE 'HIGH'
	END
ELSE
	CASE
		WHEN average_runtime_7 IS NULL or average_runtime_7 = 0 THEN 'NOT ENOUGH HISTORY'
		WHEN average_volatility < 0.2 THEN 'LOW'
		WHEN average_volatility >= 0.2 and average_volatility < 0.4 THEN 'MODERTATE'
		ELSE 'HIGH'
	END
END
as volatility_rating,
CAST(getdate() AS date) as date_evaluated into dbo.dv_job_diagnostics_labeled
from dbo.dv_job_diagnostics
where date_evaluated = CAST(getdate() as date)
END
