CREATE PROC [dbo].[proc_humanity_update_workday] AS
begin
--truncate table dbo.fact_humanity_workday_employees
--exec proc_humanity_update_workday 
if object_id ('tempdb..#stage_Workday_Employee_Full_File') is not null
drop table #stage_Workday_Employee_Full_File


---update Effective_date_end


update dbo.d_humanity_workday_employees
set Effective_date_end=cast('9999-12-31' as date)
where bk_hash not in ('-997', '-998','-999')

----------------------------------------------
--exec proc_humanity_update_workday
------------------------------
CREATE TABLE #stage_Workday_Employee_Full_File
(
	[Cost_Center] [varchar](255) NULL,
	[Hourly_Amount] [varchar](255) NULL,
	[Job_Code] [varchar](255) NULL,
	[Offering] [varchar](255) NULL,
	[Region] [varchar](255) NULL,
	[Position_ID] [varchar](255) NULL,
	[Primary_Job] [varchar](255) NULL,
	[Employee_ID] [varchar](255) NULL,
	[created_user] [varchar](255) NULL,
	[created_time] [datetime] NULL,
	[Effective_date_begin] [date] NULL,
	[Effective_date_end] [date] NULL,
	[File_arrive_date] [varchar](10) NULL,
	[Employee_position_hashkey] [varbinary](8000) NULL,
	[Cost_Hour_Job_Offer_Region_hashkey] [varbinary](8000) NULL,
	[CurrentlyProcessedFileName] [varchar](255) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)


DECLARE @min_date date
DECLARE @max_date date
set @max_date=(select max(file_arrive_date) from dbo.d_humanity_workday_employees where bk_hash not in ('-997', '-998','-999'))

--check the fact file for existance of records for first full file
if exists (select 1 from dbo.fact_humanity_workday_employees)
begin
set @min_date=(select max(file_arrive_date) from dbo.fact_humanity_workday_employees)

insert into #stage_Workday_Employee_Full_File
(
Cost_Center,
Hourly_Amount,
Job_Code,
Offering,
Region,
Position_ID,
Primary_Job,
Employee_ID,
created_user,
created_time,
Effective_date_begin,
Effective_date_end,
File_arrive_date,
Employee_position_hashkey,
Cost_Hour_Job_Offer_Region_hashkey
)
select
Cost_Center,
Hourly_Amount,
Job_Code,
Offering,
Region,
Position_ID,
Primary_Job,
Employee_ID,
created_user,
created_time,
Effective_date_begin,
Effective_date_end,
File_arrive_date,
Employee_position_hashkey,
Cost_Hour_Job_Offer_Region_hashkey
from dbo.fact_humanity_workday_employees
end
if not exists (select 1 from dbo.fact_humanity_workday_employees)
begin
set @min_date=(select min(file_arrive_date) from dbo.d_humanity_workday_employees where bk_hash not in ('-997', '-998','-999'))

insert into #stage_Workday_Employee_Full_File
(
Cost_Center,
Hourly_Amount,
Job_Code,
Offering,
Region,
Position_ID,
Primary_Job,
Employee_ID,
created_user,
created_time,
Effective_date_begin,
Effective_date_end,
File_arrive_date,
Employee_position_hashkey,
Cost_Hour_Job_Offer_Region_hashkey
)
select
Cost_Center,
Hourly_Amount,
Job_Code,
Offering,
Region,
Position_ID,
Primary_Job,
Employee_ID,
dv_insert_user,
dv_inserted_date_time,
Effective_date_begin,
Effective_date_end,
File_arrive_date,
Employee_position_hashkey,
Cost_Hour_Job_Offer_Region_hashkey
from dbo.d_humanity_workday_employees 
where bk_hash not in ('-997', '-998','-999')
and file_arrive_date = (select min(file_arrive_date) from dbo.d_humanity_workday_employees where bk_hash not in ('-997', '-998','-999'))
end
----------------------------------------------------------
WHILE @min_date <= @max_date
BEGIN
SET @min_date = DATEADD(day, 1, @min_date) 
----------------------------------------------
if object_id('tempdb..#stage_Workday_Employee_changed_file_load') is not null
drop table #stage_Workday_Employee_changed_file_load

CREATE TABLE #stage_Workday_Employee_changed_file_load
(
	[Cost_Center] [varchar](255) NULL,
	[Hourly_Amount] [varchar](255) NULL,
	[Job_Code] [varchar](255) NULL,
	[Offering] [varchar](255) NULL,
	[Region] [varchar](255) NULL,
	[Position_ID] [varchar](255) NULL,
	[Primary_Job] [varchar](255) NULL,
	[Employee_ID] [varchar](255) NULL,
	[created_user] [varchar](255) NULL,
	[created_time] [datetime] NULL,
	[Effective_date_begin] [date] NULL,
	[Effective_date_end] [date] NULL,
	[File_arrive_date] [varchar](10) NULL,
	[Employee_position_hashkey] [varbinary](8000) NULL,
	[Cost_Hour_Job_Offer_Region_hashkey] [varbinary](8000) NULL,
	[CurrentlyProcessedFileName] [varchar](255) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)


insert into #stage_Workday_Employee_changed_file_load
(
Cost_Center,
Hourly_Amount,
Job_Code,
Offering,
Region,
Position_ID,
Primary_Job,
Employee_ID,
created_user,
created_time,
Effective_date_begin,
Effective_date_end,
File_arrive_date,
Employee_position_hashkey,
Cost_Hour_Job_Offer_Region_hashkey
)
select
Cost_Center,
Hourly_Amount,
Job_Code,
Offering,
Region,
Position_ID,
Primary_Job,
Employee_ID,
dv_insert_user,
dv_inserted_date_time,
Effective_date_begin,
Effective_date_end,
cast(File_arrive_date as date) as File_arrive_date,
Employee_position_hashkey,
Cost_Hour_Job_Offer_Region_hashkey
from dbo.d_humanity_workday_employees
Where bk_hash not in ('-997', '-998','-999')
and cast(File_arrive_date as date) = @min_date

---get new records to be inserted from changed file to temp file
if object_id('tempdb..#stage_Workday_Employee_FullFile_newrec') is not null
drop table #stage_Workday_Employee_FullFile_newrec


select 
stage_Workday_Employee_changedfile.Cost_Center,
stage_Workday_Employee_changedfile.Hourly_Amount,
stage_Workday_Employee_changedfile.Job_Code,
stage_Workday_Employee_changedfile.Offering,
stage_Workday_Employee_changedfile.Region,
stage_Workday_Employee_changedfile.Position_ID,
stage_Workday_Employee_changedfile.Primary_Job,
stage_Workday_Employee_changedfile.Employee_ID,
stage_Workday_Employee_changedfile.created_user,
stage_Workday_Employee_changedfile.created_time,
stage_Workday_Employee_changedfile.Effective_date_begin,
stage_Workday_Employee_changedfile.Effective_date_end,
stage_Workday_Employee_changedfile.File_arrive_date,
stage_Workday_Employee_changedfile.Employee_position_hashkey,
stage_Workday_Employee_changedfile.Cost_Hour_Job_Offer_Region_hashkey
into #stage_Workday_Employee_FullFile_newrec
from #stage_Workday_Employee_changed_file_load stage_Workday_Employee_changedfile
join #stage_Workday_Employee_Full_File stage_Workday_Employee_FullFile
on stage_Workday_Employee_FullFile.Employee_position_hashkey=stage_Workday_Employee_changedfile.Employee_position_hashkey
and 
stage_Workday_Employee_FullFile.Cost_Hour_Job_Offer_Region_hashkey<>stage_Workday_Employee_changedfile.Cost_Hour_Job_Offer_Region_hashkey
and stage_Workday_Employee_FullFile.Effective_date_end='9999-12-31' 

update #stage_Workday_Employee_Full_File
set #stage_Workday_Employee_Full_File.Effective_date_end=cast(DATEADD(day,-1,stage_Workday_Employee_changedfile.Effective_date_begin) as date)
from #stage_Workday_Employee_changed_file_load stage_Workday_Employee_changedfile
where 
#stage_Workday_Employee_Full_File.Employee_position_hashkey=stage_Workday_Employee_changedfile.Employee_position_hashkey
and #stage_Workday_Employee_Full_File.Cost_Hour_Job_Offer_Region_hashkey<>stage_Workday_Employee_changedfile.Cost_Hour_Job_Offer_Region_hashkey
and #stage_Workday_Employee_Full_File.Effective_date_end='9999-12-31' 

---insert new record with effective end date '9999-12-31' 
insert into #stage_Workday_Employee_Full_File
(Cost_Center,	Hourly_Amount,	Job_Code	,Offering,	Region,	Position_ID	,Primary_Job	,Employee_ID,	created_user,	created_time,	Effective_date_begin	,Effective_date_end,	File_arrive_date	,Employee_position_hashkey	,Cost_Hour_Job_Offer_Region_hashkey)
select
Cost_Center,	Hourly_Amount,	Job_Code	,Offering,	Region,	Position_ID	,Primary_Job	,Employee_ID,	created_user,	created_time,	Effective_date_begin	,Effective_date_end,	File_arrive_date	,Employee_position_hashkey	,Cost_Hour_Job_Offer_Region_hashkey
from #stage_Workday_Employee_FullFile_newrec


-----insert new records from new file which is not present in old file
insert into #stage_Workday_Employee_Full_File
(Cost_Center,	Hourly_Amount,	Job_Code	,Offering,	Region,	Position_ID	,Primary_Job	,Employee_ID,	created_user,	created_time,	Effective_date_begin	,Effective_date_end,	File_arrive_date	,Employee_position_hashkey	,Cost_Hour_Job_Offer_Region_hashkey)
select
Cost_Center,	Hourly_Amount,	Job_Code	,Offering,	Region,	Position_ID	,Primary_Job	,Employee_ID,	created_user,	created_time,	Effective_date_begin	,Effective_date_end,	File_arrive_date	,Employee_position_hashkey	,Cost_Hour_Job_Offer_Region_hashkey
from #stage_Workday_Employee_changed_file_load where Employee_position_hashkey 
not in (select Employee_position_hashkey from #stage_Workday_Employee_Full_File)
 


if object_id('tempdb..#stage_Workday_Employee_changed_file_load') is not null
drop table #stage_Workday_Employee_changed_file_load
if object_id('tempdb..#stage_Workday_Employee_FullFile_newrec') is not null
drop table #stage_Workday_Employee_FullFile_newrec

------------------------------------------------------------

END


truncate table dbo.fact_humanity_workday_employees

INSERT INTO [dbo].[fact_humanity_workday_employees]
           ([Cost_Center]
           ,[Hourly_Amount]
           ,[Job_Code]
           ,[Offering]
           ,[Region]
           ,[Position_ID]
           ,[Primary_Job]
           ,[Employee_ID]
           ,[created_user]
           ,[created_time]           
           ,[Effective_date_begin]
           ,[Effective_date_end]
           ,[File_arrive_date]
           ,[Employee_position_hashkey]
           ,[Cost_Hour_Job_Offer_Region_hashkey]
            )
select 
            [Cost_Center]
           ,[Hourly_Amount]
           ,[Job_Code]
           ,[Offering]
           ,[Region]
           ,[Position_ID]
           ,[Primary_Job]
           ,[Employee_ID]
           ,[created_user]
           ,[created_time]
           ,[Effective_date_begin]
           ,[Effective_date_end]
           ,[File_arrive_date]
           ,[Employee_position_hashkey]
           ,[Cost_Hour_Job_Offer_Region_hashkey]
           
from #stage_Workday_Employee_Full_File


if object_id ('tempdb..#stage_Workday_Employee_Full_File') is not null
drop table #stage_Workday_Employee_Full_File

end
