CREATE PROC [dbo].[proc_tmp_identity_conversion] @source_object [varchar](500),@job_group [varchar](500),@informatica_folder [varchar](500) AS
begin

--declare @source_object varchar(500) = 'mms_child_center_usage_exception'
--declare @job_group varchar(500) = 'dv_main_azure'
--declare @informatica_folder varchar(500) = 'dv_mms_azure'
declare @sql varchar(max)

declare @dv_job_status_id int = (select dv_job_status_id from dv_job_status where job_name = 'wf_dv_'+@source_object and job_group = @job_group)
declare @current_dv_job_status_extract_date_time datetime = (select begin_extract_date_time from dv_job_status where dv_job_status_id = @dv_job_status_id)

if object_id('tempdb..#rebuild') is not null drop table #rebuild
create table dbo.#rebuild with(distribution=round_robin, location=user_db) as 
select distinct source_table dv_table
from dv_etl_map
where dv_table like '[h|l|s|p]_'+@source_object
union
select distinct dv_table
from dv_etl_map
where dv_table like '[h|l|s|p]_'+@source_object


---create _bkp tables
if object_id('tempdb..#bkp') is not null drop table #bkp
create table dbo.#bkp with(distribution=round_robin, location=user_db) as 
select 'if object_id('''+dv_table+'_bkp'') is null '+
       'create table dbo.'+dv_table+'_bkp with (distribution = '+case when dv_table like 'stage[_]%' then 'round_robin' else 'hash(bk_hash)' end+') as select * from '+dv_table s,
	   rank() over (order by dv_table) r
from #rebuild
where dv_table <> 'p_'+@source_object

declare @s int = 1
declare @e int = (select max(r) from #bkp)

while @s <= @e
begin

	set @sql = (select s from #bkp where r = @s)
	exec(@sql)
	--print @sql

	set @s = @s + 1
end

--truncate tables
if object_id('tempdb..#truncate') is not null drop table #truncate
create table dbo.#truncate with(distribution=round_robin, location=user_db) as 
select 'truncate table '+dv_table s,
	   rank() over (order by dv_table) r
from #rebuild

set @s = 1
set @e = (select max(r) from #truncate)

while @s <= @e
begin

	set @sql = (select s from #truncate where r = @s)
	exec(@sql)
	--print @sql

	set @s = @s + 1
end


drop table #bkp
drop table #truncate


exec proc_util_generate_structures @source_object,@job_group,@informatica_folder


if object_id('tempdb..#dv_etl_map') is not null drop table #dv_etl_map
create table dbo.#dv_etl_map with(distribution=round_robin, location=user_db) as 
select t.name tname,case when c.name in ('delete','order','backup','percent') then '['+c.name+']' else c.name end cname, dense_rank() over (order by t.name) table_rank, dense_rank() over (partition by t.name order by c.column_id) column_rank
from sys.tables t
join sys.columns c on t.object_id = c.object_id
where t.name in (select dv_table from #rebuild)
and c.is_identity = 0
and t.name not like 'p[_]%'
and c.name <> 'dv_deleted'
--order by 3,4


declare @ts int = 1
declare @te int = (select max(table_rank) from #dv_etl_map)
declare @cs int, @ce int
declare @dv_table varchar(500)
declare @col_sql varchar(max)

while @ts <= @te
begin
	set @dv_table = (select distinct tname from #dv_etl_map where table_rank = @ts)
	set @col_sql = ''
	

	set @cs = 1
	set @ce = (Select max(column_rank) from #dv_etl_map where table_rank = @ts)

	while @cs <= @ce
	begin
		
		set @col_sql = @col_sql+case when @cs <> 1 then ',' else '' end+(select cname from #dv_etl_map where table_rank = @ts and column_rank = @cs)


		set @cs = @cs+1
	end

	set @sql = 'truncate table '+@dv_table+' insert into '+@dv_table+'('+@col_sql+') select '+@col_sql+' from '+@dv_table+'_bkp'

	exec(@sql)
    --print @sql


	set @ts = @ts + 1
end

exec('proc_p_'+@source_object+' -1')
--print 'proc_p_'+@source_object+' -1'

declare @new_dv_job_status_id int = (select dv_job_status_id from dv_job_status where job_name = 'wf_dv_'+@source_object and job_group = @job_group)


update dv_job_status
set begin_extract_date_time = @current_dv_job_status_extract_date_time,
    utc_begin_extract_date_time = @current_dv_job_status_extract_date_time,
    next_begin_extract_date_time = @current_dv_job_status_extract_date_time,
    next_utc_begin_extract_date_time = @current_dv_job_status_extract_date_time,
    enabled_flag = 1
where dv_job_status_id = @new_dv_job_status_id

end