CREATE PROC [dbo].[proc_util_regenerate_h_etl_procedures] AS
begin

set nocount on
set xact_abort on

if object_id('tempdb..#hs') is not null drop table #hs
create table dbo.#hs with(distribution=round_robin, location=user_db, heap) as
select distinct dv_table, dv_column, source
  from dv_etl_map
 where dv_table like 'h[_]%' 
   and business_key_sort_order is not null
   and source <> 'manual' 

if object_id('tempdb..#ls') is not null drop table #ls
create table dbo.#ls with(distribution=round_robin, location=user_db, heap) as
select distinct d1.dv_table, d1.dv_column, d1.source
  from dv_etl_map d1
  left join #hs on d1.dv_column = #hs.dv_column and d1.source = #hs.source
 where d1.dv_table like 'l[_]%' 
   and d1.business_key_sort_order is not null
   and d1.source <> 'manual' 
   and #hs.dv_column is null

if object_id('tempdb..#base_objects') is not null drop table #base_objects
create table dbo.#base_objects with(distribution=round_robin, location=user_db, heap) as
select dv_table, dv_column, source
  from #hs
union
select dv_table, dv_column, source
  from #ls

if object_id('tempdb..#sqls') is not null drop table #sqls
create table dbo.#sqls with(distribution=round_robin, location=user_db, heap) as
select dv_table,
       'exec proc_util_generate_h_etl_procedure '''+
       --dv_table+''', '''+
       dv_column +''', '+
       case when count(distinct source) = 1 then ''''+min(source)+'''' else 'null' end regen_sql,
       row_number() over (order by dv_table) r
  from #base_objects
 group by dv_table, dv_column


declare @start int = (select min(r) from #sqls)
declare @end int = (select max(r) from #sqls)
declare @sql varchar(max), @table varchar(max)

while @start <= @end
begin

    set @sql = (select regen_sql from #sqls where r = @start)
    set @table = (select dv_table from #sqls where r = @start)
    print @table
    exec(@sql)
    --select @sql

    set @start = @start + 1

end

end
