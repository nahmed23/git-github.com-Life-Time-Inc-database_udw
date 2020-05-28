CREATE PROC [dbo].[proc_util_regenerate_r_etl_procedures] AS
begin

set nocount on
set xact_abort on

if object_id('tempdb..#rs') is not null drop table #rs
create table dbo.#rs with(distribution=round_robin, location=user_db, heap) as
select dv_table, 
       dv_column, 
       source,
       row_number() over (order by dv_table) r,
       'exec proc_util_generate_r_etl_procedure '''+dv_column+''','''+source+'''' regen_sql
from dv_etl_map 
where dv_table like 'r[_]%' 
and business_key_sort_order is not null
and source <> 'manual' 

declare @start int = (select min(r) from #rs)
declare @end int = (select max(r) from #rs)
declare @sql varchar(max), @table varchar(max)

while @start <= @end
begin

    set @sql = (select regen_sql from #rs where r = @start)
    set @table = (select dv_table from #rs where r = @start)
    print @table
    exec(@sql)

    set @start = @start + 1

end

end
