CREATE PROC [dbo].[proc_etips_update_opt_in_employees] AS
begin

--exec proc_etips_update_opt_in_employees
--select * from dbo.d_etips_opt_in_employees
--select * from dbo.fact_etips_opt_in_employees


if object_id ('tempdb..#etips_opt_in_employees_Full_File') is not null
drop table #etips_opt_in_employees_Full_File

CREATE TABLE #etips_opt_in_employees_Full_File
(    
    [d_etips_opt_in_employees_key] [char] (32) null,
	[employee_id] [varchar](255) NULL,
	[file_arrive_date] [date] NULL,
	[pay_card_end_date] [varchar](255) NULL,
	[pay_card_start_date] [varchar](255) NULL,
	[pay_card_status] [varchar](255) NULL,
	[employee_id_pay_start_date_status_key] [varbinary](8000) NULL,
	[dv_load_date_time] [datetime] NULL,
	[dv_load_end_date_time] [datetime] NULL,
	[dv_batch_id] [bigint] NOT NULL,
	[dv_inserted_date_time] [datetime] NOT NULL,
	[dv_insert_user] [varchar](50) NOT NULL,
	[dv_updated_date_time] [datetime] NULL,
	[dv_update_user] [varchar](50) NULL

)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
---------------------------------
---------------------------------
--First time only below truncate needed
truncate table dbo.fact_etips_opt_in_employees
---------Get latest records --from full file process 
DECLARE @min_date date
DECLARE @max_date date
set @max_date=(select max(file_arrive_date) from dbo.d_etips_opt_in_employees where bk_hash not in ('-997', '-998','-999'))
--set @max_date='2019-11-30'

--check the fact file for existance of records for first full file
if exists (select 1 from dbo.fact_etips_opt_in_employees)
begin
set @min_date=(select max(file_arrive_date) from dbo.fact_etips_opt_in_employees)

insert into #etips_opt_in_employees_Full_File
(
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
)
select
fact_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
from dbo.fact_etips_opt_in_employees 
end
if not exists (select 1 from dbo.fact_etips_opt_in_employees)
begin

set @min_date=(select min(file_arrive_date) from dbo.d_etips_opt_in_employees where bk_hash not in ('-997', '-998','-999'))


insert into #etips_opt_in_employees_Full_File
(
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
)
select
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
from dbo.d_etips_opt_in_employees 
where file_arrive_date = (select min(file_arrive_date) from dbo.d_etips_opt_in_employees where bk_hash not in ('-997', '-998','-999'))
end

WHILE @min_date <= @max_date
BEGIN
SET @min_date = DATEADD(day, 1, @min_date) 
----------------------------------------------
if object_id('tempdb..#etips_opt_in_employees_change_File') is not null
drop table #etips_opt_in_employees_change_File

CREATE TABLE #etips_opt_in_employees_change_File
(
    [d_etips_opt_in_employees_key] [char] (32) null,
	[employee_id] [varchar](255) NULL,
	[file_arrive_date] [date] NULL,
	[pay_card_end_date] [varchar](255) NULL,
	[pay_card_start_date] [varchar](255) NULL,
	[pay_card_status] [varchar](255) NULL,
	[employee_id_pay_start_date_status_key] [varbinary](8000) NULL,
	[dv_load_date_time] [datetime] NULL,
	[dv_load_end_date_time] [datetime] NULL,
	[dv_batch_id] [bigint] NOT NULL,
	[dv_inserted_date_time] [datetime] NOT NULL,
	[dv_insert_user] [varchar](50) NOT NULL,
	[dv_updated_date_time] [datetime] NULL,
	[dv_update_user] [varchar](50) NULL


)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)

insert into #etips_opt_in_employees_change_File
(
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
)
select
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
from dbo.d_etips_opt_in_employees
Where bk_hash not in ('-997', '-998','-999')
and cast(File_arrive_date as date) = @min_date

update #etips_opt_in_employees_full_File
set #etips_opt_in_employees_full_File.file_arrive_date=etips_opt_in_employees_change_File.file_arrive_date 
from #etips_opt_in_employees_change_File etips_opt_in_employees_change_File
where 
#etips_opt_in_employees_full_File.Employee_id=etips_opt_in_employees_change_File.Employee_id
and #etips_opt_in_employees_full_File.employee_id_pay_start_date_status_key=etips_opt_in_employees_change_File.employee_id_pay_start_date_status_key
and #etips_opt_in_employees_full_File.file_arrive_date<>etips_opt_in_employees_change_File.file_arrive_date


---get new records to be inserted from changed file to temp file
if object_id('tempdb..#etips_opt_in_employees_Full_File_newrec') is not null
drop table #etips_opt_in_employees_Full_File_newrec

select 
etips_opt_in_employees_change_File.d_etips_opt_in_employees_key,
etips_opt_in_employees_change_File.employee_id,
etips_opt_in_employees_change_File.file_arrive_date,
etips_opt_in_employees_change_File.pay_card_end_date,
etips_opt_in_employees_change_File.pay_card_start_date,
etips_opt_in_employees_change_File.pay_card_status,
etips_opt_in_employees_change_File.employee_id_pay_start_date_status_key,
etips_opt_in_employees_change_File.dv_load_date_time,
etips_opt_in_employees_change_File.dv_load_end_date_time,
etips_opt_in_employees_change_File.dv_batch_id,
etips_opt_in_employees_change_File.dv_inserted_date_time,
etips_opt_in_employees_change_File.dv_insert_user,
etips_opt_in_employees_change_File.dv_updated_date_time,
etips_opt_in_employees_change_File.dv_update_user
into #etips_opt_in_employees_Full_File_newrec
from #etips_opt_in_employees_change_File etips_opt_in_employees_change_File
join #etips_opt_in_employees_full_File etips_opt_in_employees_full_File
on etips_opt_in_employees_full_File.Employee_id=etips_opt_in_employees_change_File.Employee_id
and 
etips_opt_in_employees_full_File.employee_id_pay_start_date_status_key<>etips_opt_in_employees_change_File.employee_id_pay_start_date_status_key
and etips_opt_in_employees_full_File.pay_card_end_date='12/31/9999' 
--where etips_opt_in_employees_full_File.employee_id='31442'

update #etips_opt_in_employees_full_File
set #etips_opt_in_employees_full_File.pay_card_end_date=convert(varchar,DATEADD(day,-1,etips_opt_in_employees_change_File.pay_card_start_date),101) 
from #etips_opt_in_employees_change_File etips_opt_in_employees_change_File
where 
#etips_opt_in_employees_full_File.Employee_id=etips_opt_in_employees_change_File.Employee_id
and #etips_opt_in_employees_full_File.employee_id_pay_start_date_status_key<>etips_opt_in_employees_change_File.employee_id_pay_start_date_status_key
and #etips_opt_in_employees_full_File.pay_card_end_date='12/31/9999'  



---insert new record with pay_card_end_date '9999-12-31' 
insert into #etips_opt_in_employees_full_File
(
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user

)
select 
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
from #etips_opt_in_employees_Full_File_newrec
-----insert new records from new file which is not present in old file
insert into #etips_opt_in_employees_full_File
(
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user

)
select 
d_etips_opt_in_employees_key,
employee_id,
file_arrive_date,
pay_card_end_date,
pay_card_start_date,
pay_card_status,
employee_id_pay_start_date_status_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
from #etips_opt_in_employees_change_File
where Employee_id not in (select Employee_id from #etips_opt_in_employees_full_File)

if object_id('tempdb..#etips_opt_in_employees_Full_File_newrec') is not null
drop table #etips_opt_in_employees_Full_File_newrec

if object_id('tempdb..#etips_opt_in_employees_change_File') is not null
drop table #etips_opt_in_employees_change_File

end
-----------------------------------------------------------
insert into dbo.fact_etips_opt_in_employees
(
    [fact_etips_opt_in_employees_key],
 	[employee_id],
	[pay_card_status],
	[pay_card_start_date] ,
	[pay_card_end_date] ,
	[file_arrive_date],	
	[employee_id_pay_start_date_status_key],
	dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user

		 
)
select
    [d_etips_opt_in_employees_key], 
 	[employee_id],
	[pay_card_status],
	[pay_card_start_date] ,
	[pay_card_end_date] ,
	[file_arrive_date],	
	[employee_id_pay_start_date_status_key],
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user,
dv_updated_date_time,
dv_update_user
	
from #etips_opt_in_employees_full_File 
--select * from #etips_opt_in_employees_full_File
if object_id ('tempdb..#etips_opt_in_employees_Full_File') is not null
drop table #etips_opt_in_employees_Full_File


end


--select * from d_etips_opt_in_employees
--select * from fact_etips_opt_in_employees where pay_card_end_date='12/08/2019'
--select * from fact_etips_opt_in_employees where pay_card_status='Yes'
--select * from #etips_opt_in_employees_Full_File_newrec
--select * from #etips_opt_in_employees_change_File where employee_id='31442'
--select * from #etips_opt_in_employees_full_File where employee_id='31442'

--select distinct pay_card_end_date from fact_etips_opt_in_employees 

---------------------------This is query which needs to be placed in existing etipts logic----------------------------
--select employee_id, pay_card_start_date, pay_card_end_date from fact_etips_opt_in_employees fact_etips_opt_in_employees
--where pay_card_status='Yes' 
--and file_arrive_date in (select max(file_arrive_date) from fact_etips_opt_in_employees where employee_id=fact_etips_opt_in_employees.employee_id)
--and employee_id='233775'
---------------------------This is query which needs to be placed in existing etipts logic----------------------------
--select * from fact_etips_opt_in_employees 
--where employee_id in (
--select a.employee_id from
--(
--(select employee_id,count(employee_id) as cnt from fact_etips_opt_in_employees fact_etips_opt_in_employees
--where pay_card_status='Yes' 
--group by employee_id
--having count(employee_id)>1)
--) a)
--order by employee_id 

--employee_id
--233775
--235299
--251859
