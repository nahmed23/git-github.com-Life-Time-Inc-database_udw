CREATE PROC [dbo].[proc_util_regenerate_pit_procedures] AS
begin

set nocount on
set xact_abort on

if object_id('tempdb..#pits') is not null drop table #pits
create table dbo.#pits with(distribution=round_robin, location=user_db, heap) as
select dv_table,source_table
  from dv_etl_map 
 where dv_table like 'p[_]%'
   and source_column is not null

if object_id('tempdb..#dv') is not null drop table #dv
create table dbo.#dv with(distribution=round_robin, location=user_db, heap) as
select dv_table, dv_column, source
  from dv_etl_map 
 where dv_table in (select source_table from #pits)
   and business_key_sort_order is not null

if object_id('tempdb..#sqls') is not null drop table #sqls
create table dbo.#sqls with(distribution=round_robin, location=user_db, heap) as
select #pits.dv_table pit_table,
       'exec proc_util_generate_pit_procedure '''+
       #pits.dv_table+''', '''+
       #dv.dv_column +''', '+
       case when count(distinct source) = 1 then ''''+min(#dv.source)+'''' else 'null' end regen_sql,
       row_number() over (order by #pits.dv_table) r
  from #pits
  join #dv on #pits.source_table = #dv.dv_table
 group by #pits.dv_table, #dv.dv_column


declare @start int = (select min(r) from #sqls)
declare @end int = (select max(r) from #sqls)
declare @sql varchar(max), @table varchar(max)

while @start <= @end
begin

    set @sql = (select regen_sql from #sqls where r = @start)
    set @table = (select pit_table from #sqls where r = @start)
    print @table
    exec(@sql)

    set @start = @start + 1

end

end
