CREATE PROC [dbo].[fact_loc_log_detail] @batchid [bigint] AS
begin

/*declare @batchid bigint =20200207080116*/

declare @start_num int = 1
declare @end_num int  =3
/* (select max(row_num) from #etl_step1)*/
if object_id('tempdb..#record_cnt_hist') is not null drop table #record_cnt_hist
create  table #record_cnt_hist (workflow_name varchar(255),record_count int) with(distribution=round_robin)

while @start_num <= @end_num
begin
		
insert into #record_cnt_hist
(workflow_name,record_count)
  select A.workflow_name ,B.record_count from stage_loc_log_detail A,stage_loc_log_detail B 
   where A.workflow_name=B.workflow_name and  cast(B.begin_extract_date_time as date)=cast(DATEADD(month,-(@start_num), A.begin_extract_date_time)  as date) 
   and A.dv_batch_id=@batchid and   cast(A.begin_extract_date_time as date)<>'1753-01-01'
   
insert into #record_cnt_hist
(workflow_name,record_count)
  select A.workflow_name ,B.record_count from stage_loc_log_detail A,stage_loc_log_detail B 
   where A.workflow_name=B.workflow_name and  cast(B.begin_extract_date_time as date)=cast(DATEADD(year,-(@start_num), A.begin_extract_date_time)  as date) 
   and A.dv_batch_id=@batchid and   cast(A.begin_extract_date_time as date)<>'1753-01-01'
 
insert into #record_cnt_hist
(workflow_name,record_count)
   select A.workflow_name ,B.record_count from stage_loc_log_detail A,stage_loc_log_detail B 
   where A.workflow_name=B.workflow_name and  cast(B.begin_extract_date_time as date)=cast(DATEADD(week,-(@start_num), A.begin_extract_date_time)  as date) 
   and A.dv_batch_id=@batchid and   cast(A.begin_extract_date_time as date)<>'1753-01-01'
   
insert into #record_cnt_hist
(workflow_name,record_count)
   select A.workflow_name ,B.record_count from stage_loc_log_detail A,stage_loc_log_detail B 
   where A.workflow_name=B.workflow_name and  cast(B.begin_extract_date_time as date)=cast(DATEADD(day,-(@start_num), A.begin_extract_date_time)  as date) 
   and A.dv_batch_id=@batchid and   cast(A.begin_extract_date_time as date)<>'1753-01-01'
 
 
    set @start_num = @start_num+1
end

if object_id('tempdb..#std_dev') is not null drop table #std_dev
create table #std_dev with(distribution=round_robin, location=user_db, clustered index(workflow_name)) as
select  workflow_name,stdev(record_count) as Standard_deviation,avg(record_count) as average from #record_cnt_hist group by workflow_name

update stage_loc_log_detail set stage_loc_log_detail.Standard_deviation=#std_dev.Standard_deviation,stage_loc_log_detail.average=#std_dev.average from  #std_dev 
where stage_loc_log_detail.workflow_name=#std_dev.workflow_name and stage_loc_log_detail.dv_batch_id=@batchid


end


