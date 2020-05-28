CREATE PROC [dbo].[proc_util_generate_table_d] @source_object [varchar](500),@history_table_flag [int] AS
begin

set xact_abort on
set nocount on

--print '    ************************ proc_util_generate_d_table start ************************'

declare @d_table_name varchar(1000) = 'd_'+@source_object+case when @history_table_flag = 1 then '_history' else '' end

--Populate temp table #table_def with the dimensional transformations from the dv_d_etl_map table for tables in #build_these.  column_rank is to determine order of columns on the table
if object_id('tempdb..#table_def') is not null drop table #table_def
create table dbo.#table_def with(distribution=round_robin, location=user_db, heap) as
select target_object,
       target_column,
	   source_sql,
       rank() over (order by column_rank) column_rank,
       data_type,
       distribution_type
  from dbo.v_d_etl_map_table_column_order
 where target_object = @d_table_name

declare @deleted_flag_exists char(1)= (select case when exists(select 1 from #table_def where target_column = 'deleted_flag') then 'Y' else 'N' end)

--declare @key varchar(1000) = (select target_column from #table_def where target_column like '%'+@source_object+'_key')
--declare @distribution_type varchar(1000) = case when @key like 'dim[_]%' then 'replicate' else 'hash(bk_hash)' end
declare @distribution_type varchar(1000) = (select distinct
                                                   case when distribution_type is not null then distribution_type
                                                        --when @key like 'dim[_]%' then 'replicate'
                                                        else 'hash(bk_hash)' end
                                              from #table_def)

--For each table in #table_def,  assemble and execute the necessary create table SQL 
declare @sql varchar(max)

declare @pit_table_id varchar(500) = 'p_'+@source_object+'_id'


--Populate local variable @sql with the check for atypical records
if exists(select 1 from sys.tables where name = @d_table_name)
begin
    set @sql = 'if exists(select top 1 1 from dbo.'+@d_table_name+' where '+@d_table_name+'_id > ''0'')'+char(13)+char(10)
               +'begin'+char(13)+char(10)
               --+'    raiserror(''Records exist in %s, skipping object recreation.  Truncate the table to force recreation.'',10,1,'''+@d_table_name+''')'+char(13)+char(10)
               +'    declare @var varchar(50) = ''do nothing here'''+char(13)+char(10)
               +'end'+char(13)+char(10)
               +'else'+char(13)+char(10)
			   
end

--Append to @sql the if exists/drop table and the create table statement
set @sql = isnull(@sql,'')
           +'begin'+char(13)+char(10)
           +'    if exists(select 1 from sys.tables where name ='''+@d_table_name+''')'+char(13)+char(10)
           +'    drop table dbo.'+@d_table_name+char(13)+char(10)
           +char(13)+char(10)
           +'    create table dbo.'+@d_table_name+'('+char(13)+char(10)
		   +'        '+@d_table_name+'_id bigint identity not null,'+char(13)+char(10)
           +'        bk_hash char(32) not null,'+char(13)+char(10)

--Loop through the columns for @d_table_name, appending to @sql the column and data type info
declare @column_start int = 1
declare @column_end int = (select max(column_rank) from #table_def)
 
while @column_start <= @column_end
begin
        
    set @sql = (select @sql +'        '+target_column+' '+data_type+' null,'+char(13)+char(10)
                    from #table_def
                    where column_rank = @column_start)

    set @column_start = @column_start+1
end

--Append to @sql the default standard DV columns, driving PIT id,  clustered columnstore indexing, and dv_sequence_number insert
set @sql = @sql +'        '+@pit_table_id+' bigint not null,'+char(13)+char(10)
                +case when @deleted_flag_exists = 'n' then '        deleted_flag int null,'+char(13)+char(10) else '' end
                +'        dv_load_date_time datetime null,' +char(13)+char(10)
                +'        dv_load_end_date_time datetime null,' +char(13)+char(10)
                +'        dv_batch_id bigint not null,' +char(13)+char(10)
                +'        dv_inserted_date_time datetime not null,' +char(13)+char(10)
                +'        dv_insert_user varchar(50) not null,' +char(13)+char(10)
                +'        dv_updated_date_time datetime null,' +char(13)+char(10)
                +'        dv_update_user varchar(50) null' +char(13)+char(10)
                +'    )  with (clustered columnstore index, distribution = '+@distribution_type+')'+char(13)+char(10)
                +char(13)+char(10)
                +'end'+char(13)+char(10)

--print @sql
exec(@sql)

--print '    ************************ proc_util_generate_d_table end ************************'

drop table #table_def

end
