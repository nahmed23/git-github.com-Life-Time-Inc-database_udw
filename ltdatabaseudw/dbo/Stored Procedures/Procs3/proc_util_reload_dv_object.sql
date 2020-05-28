CREATE PROC [dbo].[proc_util_reload_dv_object] @base_object [varchar](500) AS
begin

set nocount on
set xact_abort on

declare @dv_job_status_id bigint = (select dv_job_status_id from dv_job_status where job_name like 'wf_dv_'+@base_object)

if object_id('tempdb..#tables') is not null drop table #tables
create table dbo.#tables with(distribution=round_robin, location=user_db, heap) as
select sql_cmd, dense_rank() over (order by sort_order, sql_cmd) r
  from (select 'truncate table '+dv_table sql_cmd, --dv_tables
               1 sort_order
          from dv_etl_map
         where dv_table like '[d|h|l|s|r|p][_]'+@base_object
        union
         select 'truncate table '+source_table,  --staging table
                1 sort_order
           from dv_etl_map
          where dv_table like 'h_'+@base_object
        union
         select 'truncate table '+target_object,  --d tables
                1 sort_order 
           from dv_d_etl_map
          where source_sql like 'p[_]'+@base_object+'.bk_hash'
            and target_object not like 'v[_]%'
        union
         select 'exec proc_util_create_base_records '''+dv_table+'''' sql_cmd,  --base records for h/l/s
                2 sort_order
           from dv_etl_map
          where dv_table like '[h|l|s|r][_]%'+@base_object
            
        union
         select 'exec proc_'+dv_table+' -1' sql_cmd,  --run the pit proc
                3 sort_order
           from dv_etl_map
          where dv_table like 'p[_]%'+@base_object
        union
         select 'exec proc_'+dv_table+' -1' sql_cmd,  --run the pit proc
                3 sort_order
           from dv_etl_map
          where dv_table like 'd[_]%'+@base_object
        union
         select 'update dv_job_status set begin_extract_date_time = ''Jan 1, 1753'', utc_begin_extract_date_time = ''Jan 1, 1753'', next_begin_extract_date_time = ''jan 1, 1753'', next_utc_begin_extract_date_time = ''jan 1, 1753'' where dv_job_status_id = '+cast(@dv_job_status_id as varchar) sql_cmd,  --reset the dv_job_status record
                4 sort_order)x


declare @start int = 1
declare @end int = (select max(r) from #tables)
declare @sql varchar(1000)

while @start <= @end
begin
    set @sql = (select sql_cmd from #tables where r = @start)
    exec(@sql)
    --print @sql
    set @start = @start + 1
end


if (select count(*) from dv_job_dependency where dv_job_status_id in (select dv_job_status_id from dv_job_dependency where dependent_on_dv_job_status_id = @dv_job_status_id)) > 1
begin
    raiserror('**Warning** Please check the dv_job_dependency table for additional objects that need to be reset',10,1,@base_object)  
    raiserror('**Warning** Please check the dv_job_dependency table for additional objects that need to be reset',10,1,@base_object)  
    raiserror('**Warning** Please check the dv_job_dependency table for additional objects that need to be reset',10,1,@base_object)  
end

end
