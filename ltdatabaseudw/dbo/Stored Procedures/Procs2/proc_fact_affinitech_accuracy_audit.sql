CREATE PROC [dbo].[proc_fact_affinitech_accuracy_audit] @begin_extract_date_time [varchar](500) AS
  /* Input parameter: @begin_extract_date_time varchar(100) /*$$begin_extract_date_time*/*/

 begin

 set xact_abort on
 set nocount on

 /* Date portion of @begin_extract_date_time*/
declare @begin_extract_dim_date date =  convert(date, @begin_extract_date_time, 120)
Declare @StudioQty int, @date_range int,@start int,@end int
Declare @cur_date date

/*Extract distinct date from d_affinitech_camera_count table to process data in daily basis.(since future data from 2032 present in source)*/
if object_id('tempdb..#etl_step_0') is not null drop table #etl_step_0
	create table #etl_step_0 with(distribution=hash(temp_start_range),location=user_db) as
		select temp_start_range,row_number() over( order by temp_start_range) as rank_date from(
			select  distinct cast(Start_Range as date)  as temp_start_range from  d_affinitech_camera_count c
				where cast(Start_Range as date)>= @begin_extract_dim_date)A

set @start = 1
set @end = (select max(rank_date) from #etl_step_0)

while @start <= @end
begin

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table #etl_step_1 with(distribution=hash(Studio),location=user_db) as
	SELECT distinct cam.Studio as Studio
        ,c.Start_Range as Start_Range
        ,sum(case when cam.cam_inverted = 1 then c.Exits-c.Enters else c.Enters-c.Exits end)
		over (partition by cam.studio, convert(date,c.Start_Range) order by c.Start_Range) as 'Count'
		,sum([Enters] + [Exits]) over (partition by cam.studio, convert(date,c.Start_Range) order by c.Start_Range) as 'Transactions',
		c.dv_batch_id as dv_batch_id,
		max(c.dv_load_date_time) over(partition by  cam.studio, convert(date,c.Start_Range) order by c.Start_Range) as dv_load_date_time
	FROM  d_affinitech_camera_count as c join #etl_step_0 as t on t.temp_start_range= convert(date,c.Start_Range)
		left join d_affinitech_cameras as cam on c.Source_IP = cam.CAM_IP
		where t.rank_date=@start and cast(c.Start_Range as date) = t.temp_start_range and cam.studio is not null

 Set @StudioQty=(Select count (Distinct studio) from  #etl_step_1)
 Set @cur_date =(Select max(temp_start_range) from  #etl_step_0 where rank_date= @start)


if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table #etl_step_2 with(distribution=hash(studio),location=user_db) as
select top (@StudioQty)
		convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_1.studio,'z#@$k%&P')+
		'P%#&z$@k'+isnull((convert(varchar(32), convert(datetime, #etl_step_1.Start_Range, 120), 112)),'z#@$k%&P'))),2) as  fact_affinitech_accuracy_audit_key
		,@cur_date as 'Date'
		,studio as studio
        ,[Count] as 'Count'
        ,[transactions]
		,case when convert(float,[transactions]) = 0 then 0 else 1-(ABS(convert(float,[count]))/convert(float,[transactions])) end as 'Accuracy'
		,dv_batch_id
		,dv_load_date_time
		from #etl_step_1 order by Start_Range desc




 begin tran

   delete dbo.fact_affinitech_accuracy_audit
    where date >= @cur_date

   insert into fact_affinitech_accuracy_audit
        (
	 fact_affinitech_accuracy_audit_key
	,date
	,studio
	,Count
	,transactions
	,Accuracy
	,dv_load_date_time
	,dv_load_end_date_time
	,dv_batch_id
	,dv_inserted_date_time
	,dv_insert_user
  )
  select
  fact_affinitech_accuracy_audit_key
  ,date
  ,studio
  ,Count
  ,transactions
  ,Accuracy
  ,dv_load_date_time
  ,convert(datetime, '99991231', 112)
  ,dv_batch_id
  ,getdate()
  ,suser_sname()
  from #etl_step_2
 commit tran

set @start = @start+1
end

 end

