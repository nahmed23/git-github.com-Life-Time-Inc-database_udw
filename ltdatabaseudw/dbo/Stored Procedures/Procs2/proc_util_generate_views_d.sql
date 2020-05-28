CREATE PROC [dbo].[proc_util_generate_views_d] @d_table_name [varchar](500) AS
begin

set xact_abort on
set nocount on
--view dependencies
--assumes the base d-table has been built


--print '    ************************ Building views ************************'

--we want these columns first.  see #cols join
if object_id('tempdb..#lead_columns') is not null drop table #lead_columns
create table dbo.#lead_columns with(distribution=round_robin, location=user_db, heap) as
select target_column, 
       rank() over (order by column_rank,target_column) r
  from dbo.v_d_etl_map_table_column_order
 where target_object = @d_table_name
   and (business_key_sort_order is not null
        or target_column in ('effective_date_time','expiration_date_time')
        or column_rank in (1))

declare @start int, @end int
declare @sql varchar(max)
declare @schema varchar(500)
declare @base_obj varchar(500) = @d_table_name
declare @base_schema varchar(50)= 'dbo'


        --This assumes the dim/fact_key, business keys, and effective/expiration dates won't ever be modified in anyway
    if object_id('tempdb..#cols') is not null drop table #cols
    create table dbo.#cols with(distribution=round_robin, location=user_db, heap) as
    select d.target_object,
           d.target_column,
           d.source_sql,
           d.source_sql + ' ' + d.target_column def, 
           d.view_schema,
           dense_rank() over (order by d.target_object) object_rank,
           rank() over (partition by d.target_object
                        order by case when k.r is not null then 'aaaaaaaaaa'+cast(r as varchar(4))
                                      else replace(replace(d.target_column,'[',''),']','') end) column_rank
      from dv_d_etl_map d
      join (select target_object from dv_d_etl_map where source_sql like '%'+@d_table_name+'.%' and source_sql not like '%[_]'+@d_table_name+'.%'group by target_object) v on d.target_object = v.target_object
      left join #lead_columns k on d.target_column = k.target_column
     where d.target_column <> 'd_where_clause' --this is handled specially after the loop (with the from)
	   and d.target_column <> 'v_clause'
       and d.target_object like 'v%'

declare @obj_start int = 1
declare @obj_end int = (select max(object_rank) from #cols)
declare @target_object varchar(1000)
while @obj_start <= @obj_end
begin

    set @schema = isnull((select max(view_schema) from #cols where object_rank = @obj_start),'marketing')
    set @target_object = (select max(target_object) from #cols where object_rank = @obj_start)
    set @start = 1
    set @end = (select max(column_rank) from #cols where object_rank = @obj_start)
    
    
    --if exists drop
    set @sql = (select distinct 
                       'if exists(select 1 from sys.views where name = '''+target_object+''' and schema_id = (select schema_id from sys.schemas where name = '''+@schema+'''))'+char(13)+char(10)
                      +'drop view '+@schema+'.'+target_object+char(13)+char(10)+char(13)+char(10)
                  from #cols
                 where object_rank = @obj_start)
    exec(@sql)
    
    --create view
    set @sql = (select distinct
                       'create view '+@schema+'.'+target_object+' as'+char(13)+char(10)
                      +'select '
                  from #cols
                 where object_rank = @obj_start)
    
    while @start <= @end
    begin

        --apped current column
        set @sql = @sql + case when @start <> 1 then '       ' else '' end + (select def from #cols where object_rank = @obj_start and column_rank = @start)+case when @start = @end then '' else ',' end+char(13)+char(10)


        set @start = @start+1
    end

	declare @v_clause varchar(max) = (select source_sql from dv_d_etl_map where target_object = @target_object and target_column = 'v_clause')
	declare @d_where_clause varchar(max) = (select source_sql from dv_d_etl_map where target_object = @target_object and target_column = 'd_where_clause')

    --from and WHERE clause
    set @sql = @sql + case when @v_clause is not null then @v_clause
	                       else '  from '+@base_schema+'.'+@base_obj+char(13)+char(10)
                                +isnull(@d_where_clause,'')+char(13)+char(10)
					   end
    
    --execute view creation
    exec(@sql)
    
    set @obj_start = @obj_start + 1
end






--print '    ************************ Views have been built ************************'

drop table #lead_columns
if object_id('tempdb..#cols') is not null drop table #cols
end

