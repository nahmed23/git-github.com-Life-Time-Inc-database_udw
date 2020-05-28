﻿CREATE PROC [dbo].[proc_util_generate_procedure_d] @source_object [varchar](500) AS
begin

set nocount on
set xact_abort on

--print '    ************************ proc_util_generate_procedure_d start ************************'

declare @sql varchar(max),
        @start int,
        @end int,
        @d_table_column_list varchar(max)

declare @d_table_name varchar(1000) = 'd_'+@source_object

-- Create temp table #d_table_def with the SQL statement for each target column.
if object_id('tempdb..#d_table_def') is not null drop table #d_table_def
create table dbo.#d_table_def with(distribution=round_robin, location=user_db, heap) as
select target_object,
       target_column,
	   source_sql,
       rank() over (order by column_rank) column_rank,
       data_type
  from dbo.v_d_etl_map_table_column_order
 where target_object = @d_table_name

declare @pit_table_name varchar(1000) = 'p_'+@source_object
declare @deleted_flag_exists char(1)= isnull((select case when target_column = 'deleted_flag' then 'y' else 'n' end from #d_table_def where target_column = 'deleted_flag'),'N')

--Assumption: the PIT table is the source for at least one column.  This should always be true for the *_key column.
--Assumption: one PIT table per d-table
if object_id('tempdb..#p') is not null drop table #p
create table dbo.#p with(distribution=round_robin, location=user_db, heap) as
select e.dv_table table_name,
       e.source_table,
       e.source_column,
       rank() over (order by e.source_table) r
  from dv_etl_map e
 where e.dv_table = @pit_table_name
   and e.business_key_sort_order is null --exclude business keys, only returning links and sats
 group by e.dv_table,
          e.source_table,
          e.source_column

delete from #d_table_def where target_column = @pit_table_name+'_id'
 
set @sql = case when exists(select 1 from sys.procedures where name = 'proc_'+@d_table_name) then 'alter' else 'create' end + ' procedure dbo.proc_'+@d_table_name+' @current_dv_batch_id bigint'+char(13)+char(10)
           +'as'+char(13)+char(10)
           +'begin'+char(13)+char(10)
           +char(13)+char(10)
           +'set nocount on'+char(13)+char(10)
           +'set xact_abort on'+char(13)+char(10)
           +char(13)+char(10)
           +'--Start!'+char(13)+char(10)
           +'-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren''t any records.'+char(13)+char(10)
           +'declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from '+@d_table_name+')'+char(13)+char(10)
           +char(13)+char(10)
           +'-- Find the PIT records with dv_batch_id > @max_dv_batch_id'+char(13)+char(10)
           +'-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry'+char(13)+char(10)
           +'if object_id(''tempdb..#'+@pit_table_name+'_insert'') is not null drop table #'+@pit_table_name+'_insert'+char(13)+char(10)
           +'create table dbo.#'+@pit_table_name+'_insert with(distribution=hash(bk_hash), location=user_db) as'+char(13)+char(10)
           +'select '+@pit_table_name+'.'+@pit_table_name+'_id,'+char(13)+char(10)
           +'       '+@pit_table_name+'.bk_hash'+char(13)+char(10)
           +'  from dbo.'+@pit_table_name+char(13)+char(10)
           +' where '+@pit_table_name+'.dv_load_end_date_time = convert(datetime,''9999.12.31'',102)'+char(13)+char(10)
           +'   and ('+@pit_table_name+'.dv_batch_id > @max_dv_batch_id'+char(13)+char(10)
           +'        or '+@pit_table_name+'.dv_batch_id = @current_dv_batch_id)'+char(13)+char(10)
           +char(13)+char(10)
           +'-- calculate all values of the records to be inserted to make the actual update go as fast as possible'+char(13)+char(10)
           +'if object_id(''tempdb..#insert'') is not null drop table #insert'+char(13)+char(10)
           +'create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as'+char(13)+char(10)
           +'select '+@pit_table_name+'.bk_hash,'+char(13)+char(10)

set @start = 1
set @end = (select max(column_rank) from #d_table_def)
declare @source_sql varchar(max), @target_column varchar(500)
declare @parse_start_index int, @parse_end_index int, @parse_call varchar(max), @source_sql_copy varchar(max)
--loop through the d_table's columns
while @start <= @end
begin

    set @source_sql = (select source_sql from #d_table_def where column_rank = @start)
    set @target_column = (select target_column from #d_table_def where column_rank = @start)

    --function parsing
    set @source_sql_copy = @source_sql
    while(patindex('%util_bk_hash[[]%',@source_sql_copy) > 0)
    begin
        
        --find the position of the earliest instance of the function call
        set @parse_start_index = patindex('%util_bk_hash[[]%',@source_sql_copy)
        --find the position of the matching end bracket
        set @parse_end_index = charindex(']',substring(@source_sql_copy,@parse_start_index,len(@source_sql_copy)))
        --substring out the entire function call
        set @parse_call  = substring(@source_sql_copy,@parse_start_index,@parse_end_index)
        --submit the function call to be parsed
        exec proc_util_d_bk_hash @parse_call
        
        --ready @source_sql_copy for the next iteration by trimming off up through the previous parsed out function call
        set @source_sql_copy = substring(@source_sql_copy,@parse_end_index+1,len(@source_sql_copy))
        --replace the function call with the generated hash string from the proc call
        set @source_sql = replace(@source_sql,@parse_call,(select d_bk_hash from #proc_util_d_bk_hash))
    end

    --attach and alias the source sql
    set @sql = @sql
               + '       ' +replace(@source_sql,char(13)+char(10),char(13)+char(10)+'       ')
               +' '+@target_column + ',' + char(13)+char(10)
    
    --assemble a string of the d-table columns for the final insert statement
    set @d_table_column_list = isnull(@d_table_column_list,'')
                               +case when @start = 1 then '' else '         ' end
                               +@target_column+','+char(13)+char(10)

    set @start = @start + 1

end

set @sql = @sql 
           +'       isnull(h_'+@source_object+'.dv_deleted,0) dv_deleted,'+char(13)+char(10)
           +'       '+@pit_table_name+'.'+@pit_table_name+'_id,'+char(13)+char(10)
           +'       '+@pit_table_name+'.dv_batch_id,'+char(13)+char(10)
           +'       '+@pit_table_name+'.dv_load_date_time,'+char(13)+char(10)
           +'       '+@pit_table_name+'.dv_load_end_date_time'+char(13)+char(10)
           +'  from dbo.h_'+@source_object+char(13)+char(10)
           +'  join dbo.'+@pit_table_name+char(13)+char(10)
           +'    on h_'+@source_object+'.bk_hash = '+@pit_table_name+'.bk_hash'+char(13)+char(10)
           +'  join #'+@pit_table_name+'_insert'+char(13)+char(10)
           +'    on '+@pit_table_name+'.bk_hash = #'+@pit_table_name+'_insert.bk_hash'+char(13)+char(10)
           +'   and '+@pit_table_name+'.'+@pit_table_name+'_id = #'+@pit_table_name+'_insert.'+@pit_table_name+'_id'+char(13)+char(10)
           

set @start = 1
set @end = (select max(r) from #p)

--loop through the pit table's source tables (links and sats) and add joins
while @start <= @end
begin

    set @sql = (select @sql +'  join dbo.'+source_table+char(13)+char(10)
                             +'    on '+table_name+'.bk_hash = '+source_table+'.bk_hash'+char(13)+char(10)
                             +'   and '+table_name+'.'+source_column+' = '+source_table+'.'+source_column+char(13)+char(10)
                   from #p
                  where r = @start)

    set @start = @start + 1
end

declare @d_where_clause varchar(8000) = (select ' '+source_sql+char(13)+char(10) from dv_d_etl_map where target_object = @d_table_name and target_column = 'd_where_clause')

set @sql =  @sql+isnull(@d_where_clause,'')

set @sql = @sql 
            +char(13)+char(10)
            +'-- do as a single transaction'+char(13)+char(10)
            +'--   delete records from the dimensional table where the business_key is in #p_*_insert temp table'+char(13)+char(10)
            +'--   insert records from all of the joins to the pit table and to #p_*_insert temp table'+char(13)+char(10)
            +'begin tran'+char(13)+char(10)
            +'  delete dbo.'+@d_table_name+char(13)+char(10)
            +'   where '+@d_table_name+'.bk_hash in (select bk_hash from #'+@pit_table_name+'_insert)'+char(13)+char(10)
            +char(13)+char(10)
            +'  insert dbo.'+@d_table_name+'('+char(13)+char(10)
            +'             bk_hash,'+char(13)+char(10)
            +'             '+replace(@d_table_column_list,'         ','             ')
            +case when @deleted_flag_exists = 'n' then '             deleted_flag,'+char(13)+char(10) else '' end
            +'             '+@pit_table_name+'_id,'+char(13)+char(10)
            +'             dv_load_date_time,'+char(13)+char(10)
            +'             dv_load_end_date_time,'+char(13)+char(10)
            +'             dv_batch_id,'+char(13)+char(10)
            +'             dv_inserted_date_time,'+char(13)+char(10)
            +'             dv_insert_user)'+char(13)+char(10)
            +'  select bk_hash,'+char(13)+char(10)
            +'         '+@d_table_column_list
            +case when @deleted_flag_exists = 'n' then '         dv_deleted,'+char(13)+char(10) else '' end
            +'         '+@pit_table_name+'_id,'+char(13)+char(10)
            +'         dv_load_date_time,'+char(13)+char(10)
            +'         dv_load_end_date_time,'+char(13)+char(10)
            +'         dv_batch_id,'+char(13)+char(10)
            +'         getdate(),'+char(13)+char(10)
            +'         suser_sname()'+char(13)+char(10)
            +'    from #insert'+char(13)+char(10)
            +'commit tran'+char(13)+char(10)
            +char(13)+char(10)
            +'--force replication'+char(13)+char(10)
            +'set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from '+@d_table_name+')'+char(13)+char(10)
            +'--Done!'+char(13)+char(10)
            +'end'+char(13)+char(10)

---Modified by Offshore
/*
declare @sql1 varchar(8000)
set @sql1 = substring(@sql,1,8000)
print @sql1
set @sql1 = substring(@sql,8001,8000)
print @sql1
set @sql1 = substring(@sql,16001,8000)
print @sql1
*/
exec (@sql)


set @sql = 'grant execute on dbo.proc_'+@d_table_name+' to informaticauser'+char(13)+char(10)
exec (@sql)

drop table #d_table_def
drop table #p


--print '    ************************ proc_util_generate_procedure_d end ************************'

end

