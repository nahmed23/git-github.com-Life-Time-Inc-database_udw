CREATE PROC [dbo].[proc_util_data_profile] @table_name [varchar](500),@top_count [varchar](5) AS
begin

set nocount on
set xact_abort on

if object_id('tempdb..#res') is not null drop table #res
create table dbo.#res
       (column_name varchar(500),
        ordinal int,
        datatype varchar(500),
        metric varchar(500),
        val varchar(500))
  with (heap)


if object_id('tempdb..#t') is not null drop table #t
create table dbo.#t with(distribution=round_robin, location=user_db, heap) as
select column_name, ordinal_position r, data_type, numeric_precision, data_type + case when data_type in ('char', 'varchar', 'nchar', 'nvarchar','binary') then '(' + cast(character_maximum_length as varchar)+')'
                                                                                       when data_type in ('decimal','numeric') then '(' + cast(numeric_precision as varchar)+','+cast(numeric_scale as varchar)+')'
                                                                                       else '' end full_data_type
from information_schema.columns
where table_name = @table_name
and column_name not in ('dv_load_date_time','dv_batch_id','dv_r_load_source_id','dv_inserted_date_time','dv_insert_user','dv_updated_date_time','dv_update_user','dv_hash','dv_load_end_date_time') --no dv columns
and ordinal_position <> 1
and column_name not in ('bk_hash')

declare @start int = (select min(r) from #t)
declare @end int = (select max(r) from #t)
declare @sql varchar(8000)
declare @column_name varchar(500)
declare @data_type varchar(500), @isnumeric int, @full_data_type varchar(500)
declare @top_start int, @top_end int

while @start <= @end
begin
    set @column_name = (select column_name from #t where r = @start)
    set @data_type = (select data_type from #t where r= @start)
    set @full_data_type = (select full_data_type from #t where r = @start)
    set @isnumeric = (select case when numeric_precision is not null then 1 else 0 end from #t where r= @start)
    
    --top counts
    set @sql = 'if object_id(''tempdb..#top'') is not null drop table #top'+char(13)+char(10)
              +'create table dbo.#top with(distribution=round_robin, location=user_db, heap) as'+char(13)+char(10)
              +'select cast(col as varchar(500)) col, c, rank() over (order by c desc, col asc) r from (select top ('+@top_count+') '+@column_name+' col, count(*) c from '+@table_name+case when exists(select 1 from information_schema.columns where table_name = @table_name and column_name = 'bk_hash') then ' where bk_hash not in (''-999'',''-998'',''-997'')' else '' end+' group by '+@column_name+' order by c desc) x'
    
    exec(@sql)

    --select * from #top

    set @top_start = (select min(r) from #top)
    set @top_end = (select max(r) from #top)

    --stack top values for pivot
    while @top_start <= @top_end
    begin
    
        insert into #res
        select @column_name,
               @start ordinal,
               @full_data_type,
               'val'+cast(r as varchar) metric,
               case when col is null then 'null' else col end
           from #top
          where r = @top_start
        union all
        select @column_name,
               @start ordinal,
               @full_data_type,
               'val'+cast(r as varchar)+'_count' metric,
               cast(c as varchar(500))
           from #top
          where r = @top_start
          
        set @top_start = @top_start+1
    end

    if @data_type <> 'bit'
    begin
        --aggregate metrics
        set @sql = 'if object_id(''tempdb..#agg'') is not null drop table #agg'+char(13)+char(10)
                  +'create table dbo.#agg with(distribution=round_robin, location=user_db, heap) as'+char(13)+char(10)
                  +'select count(*) record_count,'+char(13)+char(10)
                  +'count(distinct '+@column_name+') + sum(distinct case when '+@column_name+' is null then 1 else 0 end) distinct_value_count,'+char(13)+char(10)
                  +'min('+@column_name+') min,'+char(13)+char(10)
                  +'max('+@column_name+') max,'+char(13)+char(10)
                  +'sum(case when '+case when @isnumeric = 1 then @column_name else 'cast('+@column_name+' as varchar(500))' end+' = ''0'' then 1 else 0 end) zero_count,'+char(13)+char(10)
                  +'sum(case when '+@column_name+' is null then 1 else 0 end) null_count'+char(13)+char(10)
                  +'  from '+@table_name+char(13)+char(10)
                  +case when exists(select 1 from information_schema.columns where table_name = @table_name and column_name = 'bk_hash') then ' where bk_hash not in (''-999'',''-998'',''-997'')' else '' end
    
        exec(@sql)
 
        --stack aggregate metrics for pivot
        insert into #res
        select @column_name,
               @start ordinal,
               @full_data_type,
               'record_count',
               cast(record_count as varchar(500))
          from #agg 
        union all
        select @column_name,
               @start ordinal,
               @full_data_type,
               'distinct_value_count',
               cast(distinct_value_count as varchar(500))
          from #agg 
        union all
        select @column_name,
               @start ordinal,
               @full_data_type,
               'min',
               cast(min as varchar(500))
          from #agg 
        union all
        select @column_name,
               @start ordinal,
               @full_data_type,
               'max',
               cast(max as varchar(500))
          from #agg 
        union all
        select @column_name,
               @start ordinal,
               @full_data_type,
               'zero_count',
               cast(zero_count as varchar(500))
          from #agg 
        union all
        select @column_name,
               @start ordinal,
               @full_data_type,
               'null_count',
               cast(null_count as varchar(500))
          from #agg 
          
    end

    set @start = @start+1
end

declare @top_x_sql varchar(8000)=''
set @start = 1
set @end = @top_count

while @start <= @top_count
begin
    set @top_x_sql = @top_x_sql+'[val'+cast(@start as varchar)+'],[val'+cast(@start as varchar)+'_count]'+case when @start = @end then '' else ',' end
    set @start = @start + 1
end

set @sql = 'if object_id(''tempdb..#profile'') is not null drop table #profile'+char(13)+char(10)
          +'create table dbo.#profile with(distribution=round_robin, location=user_db, heap) as'+char(13)+char(10)
          +'select column_name, ordinal,datatype, [record_count],[distinct_value_count],[zero_count],[null_count],[min],[max],'+@top_x_sql+char(13)+char(10)
          +'  from (select column_name,ordinal,datatype, metric, val'+char(13)+char(10)
          +'          from #res) x'+char(13)+char(10)
          +'pivot(max(val)'+char(13)+char(10)
          +'      for metric in ([record_count],[distinct_value_count],[min],[max],[zero_count],[null_count],'+@top_x_sql+')) y'

exec(@sql)

--select 'you can access the results in #profile'
select * from #profile order by column_name



drop table #t
drop table #top
drop table #res
drop table #agg

end

