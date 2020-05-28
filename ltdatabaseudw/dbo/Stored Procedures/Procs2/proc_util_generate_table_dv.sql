CREATE PROC [dbo].[proc_util_generate_table_dv] @dv_table_name [varchar](500) AS
begin

set xact_abort on
set nocount on
set ansi_nulls on
set quoted_identifier on

--print '    ************************ proc_util_generate_table_dv '+@dv_table_name+' start ************************'

--stage_hash handling so we don't have to insert a "duplicate" set of dv_etl_map records
declare @process_as_dv_table_name varchar(1000) = case when @dv_table_name like 'stage[_]hash[_]%' then 'stage_'+substring(@dv_table_name,12,len(@dv_table_name)) else @dv_table_name end

--only stage needs this special handling, but clashes with stage_hash naming
declare  @dv_table_type varchar(500) = case when @dv_table_name like 'stage[_]hash[_]%' then 'stage_hash' else substring(@dv_table_name,1,charindex('_',@dv_table_name)-1) end

--only initial stage_ table shouldn't have identity columns (informatica does not handle them yet - 1/9/2018)
declare @pk_sql varchar(500) = (select case when @dv_table_type != 'stage' then 'bigint identity(1,1) not null,' else 'bigint not null,' end)

 --Grab table metadata
if object_id('tempdb..#table') is not null drop table #table
create table dbo.#table with(distribution=round_robin, location=user_db) as
select distinct dv_table, dv_column, data_type, sort_order, is_truncated_staging,partition_scheme,historical_pit_flag,
       dense_rank() over (order by sort_order) column_rank
  from dbo.dv_etl_map
 where dv_table = @process_as_dv_table_name
   and dv_column <> 'dv_load_date_time' --hardcoded below, dv_etl_map record is for the dv etl proc logic

declare @start int, @end int, @sql varchar(max)

declare @is_truncated_staging int = (select max(cast(is_truncated_staging as int)) from #table)
set @sql = (select 'if exists(select top 1 1 from dbo.'+@dv_table_name+case when @dv_table_type <> 'stage' then ' where bk_hash not in (''-997'',''-998'',''-999'')' else '' end + ')'+char(13)+char(10)
                  +'begin'+char(13)+char(10)
                  +'raiserror(''Records exist in %s, skipping object recreation.  Truncate the table to force recreation.'',10,1,'''+@dv_table_name+''')'+char(13)+char(10)
                  +'end'+char(13)+char(10)
                  +'else'+char(13)+char(10)
             where exists(select 1 from sys.tables where name = @dv_table_name))

set @sql = isnull(@sql,'')+'begin'+char(13)+char(10)
                          +'    if exists(select 1 from sys.tables where name = '''+@dv_table_name+''')'+char(13)+char(10)
                          +'    drop table '+@dv_table_name+char(13)+char(10)
                          +char(13)+char(10)
                          +'    create table dbo.'+@dv_table_name+' ('+char(13)+char(10)
                          +'        '+@dv_table_name+'_id '+@pk_sql+char(13)+char(10) --PK
                          +case when @dv_table_type <> 'stage' then '        bk_hash char(32) not null,'+char(13)+char(10) else '' end --bk_hash if not stage

--add dv_etl_map columns
set @start = 1
set @end = (select max(column_rank) from #table)
while @start <= @end
begin

    set @sql = (select @sql + '        '+dv_column+' '+data_type+','+char(13)+char(10)
                    from #table
                    where column_rank = @start)



    set @start = @start+1
end

--add standard columns
set @sql = @sql   +case when @dv_table_type <> 'stage'                                then '        dv_load_date_time datetime not null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type in ('r','p','b')                          then '        dv_load_end_date_time datetime not null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type in ('p')                                  then '        dv_greatest_satellite_date_time datetime null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type in ('p')                                  then '        dv_next_greatest_satellite_date_time datetime null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type in ('p')                                  then '        dv_first_in_key_series int null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type not in('p','stage','stage_hash')          then '        dv_r_load_source_id bigint not null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type not in ('stage','stage_hash') or @is_truncated_staging = 0   then '        dv_inserted_date_time datetime not null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type not in ('stage','stage_hash') or @is_truncated_staging = 0   then '        dv_insert_user varchar(50) not null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type not in ('stage','stage_hash') or @is_truncated_staging = 0   then '        dv_updated_date_time datetime null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type not in ('stage','stage_hash') or @is_truncated_staging = 0   then '        dv_update_user varchar(50) null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type in ('l','s','r')                          then '        dv_hash char(32) not null,'+char(13)+char(10) else '' end
                  +case when @dv_table_type in ('h','s','l')                          then '        dv_deleted bit default 0 not null,'+char(13)+char(10) else '' end
                                                                                                                            +'        dv_batch_id bigint not null'+char(13)+char(10)
                                                                                                                            +'    ) with '+case when @dv_table_type in ('stage') then '(heap, distribution = round_robin)'
                                                                                                                                                else '(heap, distribution = hash(bk_hash))' end+char(13)+char(10)
set @sql = @sql + 'end'
--create table
exec(@sql)
--print @sql

set @sql = 'exec dbo.proc_util_create_base_records '''+@dv_table_name+''''+char(13)+char(10)
if @dv_table_type not in ('stage','stage_hash','p') exec(@sql)

--print '    ************************ proc_util_generate_table_dv '+@dv_table_name+' end ************************'

end
