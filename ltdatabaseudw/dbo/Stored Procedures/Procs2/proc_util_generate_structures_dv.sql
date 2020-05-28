CREATE PROC [dbo].[proc_util_generate_structures_dv] @source_object [varchar](500),@job_group [varchar](500),@informatica_folder [varchar](256) AS

begin

set xact_abort on
set nocount on
set ansi_nulls on
set quoted_identifier on

--print '************************ proc_util_generate_structures_dv start ************************'

--when we fully cut over the azure, remove this; "_azure" will be removed 
declare @informatica_folder_name varchar(500) = replace(@informatica_folder,'_azure','')
declare @pit_table varchar(500) = 'p_'+@source_object
declare @hub_table varchar(500) = 'h_'+@source_object
declare @ref_table varchar(500) = 'r_'+@source_object

--First query grabs satellites, links, and stage table
--Second query grabs the PIT table and Hub
--Third query manufactures the stage_hash table - specific handling in proc_util_generate_table_dv
if object_id('tempdb..#all_tables') is not null drop table #all_tables
create table dbo.#all_tables with(distribution=round_robin, location=user_db) as 
select dv_table,
       generate_delete_proc_flag,
       rank() over (order by dv_table) r
from (select source_table dv_table, --sats, links, stage
             max(generate_delete_proc_flag) generate_delete_proc_flag
        from dv_etl_map
       where dv_table = @pit_table
          or dv_table = @hub_table
          or dv_table = @ref_table
       group by source_table
       union
      select dv_table, --pit, hub, ref
             max(generate_delete_proc_flag) generate_delete_proc_flag
       from dv_etl_map
      where dv_table = @pit_table
         or dv_table = @hub_table
         or dv_table = @ref_table
       group by dv_table
       union
      select 'stage_hash_'+substring(source_table,7,len(source_table)), --manufactured stage_hash
             max(generate_delete_proc_flag) generate_delete_proc_flag
       from dv_etl_map
      where dv_table = @hub_table
       group by 'stage_hash_'+substring(source_table,7,len(source_table))
       ) x

declare @pit_type varchar(1) = case when (select min(historical_pit_flag) from dv_etl_map where dv_table = @pit_table) = 1 then 'h' else 's' end --history/simple

declare @table_start int, @table_end int, @sql varchar(max)
declare @dv_table_name varchar(500), @dv_table_type varchar(500), @is_truncated_staging bit, @partition_scheme varchar(500), @job_status_insert_sql varchar(max)
declare @pk_sql varchar(500)

set @table_start = 1
set @table_end = (select max(r) from #all_tables)

--create tables
while @table_start <= @table_end
begin

    set @dv_table_name = (select dv_table from #all_tables where r = @table_start)

    if @dv_table_name like 'stage[_]hash[_]%' and exists(select 1 from sys.columns where object_id = object_id(@dv_table_name) and name = 'dv_inserted_date_time')
    begin
        set @sql = 'alter table '+@dv_table_name+'  drop column dv_inserted_date_time'
        exec(@sql)
    end
    if @dv_table_name like 'stage[_]hash[_]%' and exists(select 1 from sys.columns where object_id = object_id(@dv_table_name) and name = 'dv_insert_user')
    begin
        set @sql = 'alter table '+@dv_table_name+'  drop column dv_insert_user'
        exec(@sql)
    end

    exec proc_util_generate_table_dv @dv_table_name

    set @table_start = @table_start+1

end

set @sql = ''

--generate delete proc
if exists(select top 1 1 from #all_tables where generate_delete_proc_flag = 1)
begin
    set @sql = 'exec dbo.proc_util_generate_procedure_dv_deleted '''+@source_object+''''+char(13)+char(10)
    exec(@sql)
end


--generator proc calls
if exists(select top 1 1 from #all_tables where dv_table like 'r[_]%')
begin
    set @sql = 'exec dbo.proc_util_generate_procedure_ref '''+@source_object+''''+char(13)+char(10)
end
else
begin
    set @sql = case when @pit_type = 'h' then 'exec dbo.proc_util_generate_procedure_pit_historical ''' --generate pit proc
                    else 'exec dbo.proc_util_generate_procedure_pit '''
                end +@pit_table+''''+char(13)+char(10)
              +char(13)+char(10)
              +'exec dbo.proc_util_generate_procedure_dv_etl '''+@source_object+''''+char(13)+char(10) --generate the dv etl proc
end

exec(@sql)
set @sql = 'exec proc_etl_'+@source_object+' -1,''jan 1, 1753''' --run the etl proc to make sure it works, also runs the pit proc
exec(@sql)

declare @source varchar(500) = (select min(source) from dv_etl_map where dv_table = @hub_table or dv_table = @ref_table)

--add dv_job_status record
set @sql = 'exec dbo.proc_util_job_status_insert'+char(13)+char(10)
              +'     @job_name = ''wf_dv_'+@source_object+''','+char(13)+char(10)
              +'     @begin_extract_date_time = ''Jan 1, 1753'','+char(13)+char(10)
              +'     @source_name = '+isnull(''''+@source+'''','''''')+','+char(13)+char(10)
              +'     @job_group = '''+@job_group+''','+char(13)+char(10)
              +'     @informatica_folder_name = '''+@informatica_folder+''''+char(13)+char(10)
              +char(13)+char(10)
              +@sql

--print @sql 
exec(@sql)


--print '************************ proc_util_generate_structures_dv end ************************'

end
