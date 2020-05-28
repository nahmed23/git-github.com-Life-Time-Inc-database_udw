CREATE PROC [dbo].[proc_util_generate_table_mfd] @manual_fact_dim [varchar](500) AS
begin

set xact_abort on
set nocount on

--print '    ************************ proc_util_generate_d_table start ************************'



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
 where target_object = @manual_fact_dim

 
declare @key_flag int = isnull((select max(1) from #table_def where target_column = @manual_fact_dim+'_key'),0)
declare @distribution_type varchar(1000) = (select distinct
                                                   case --when distribution_type is null and @manual_fact_dim like 'dim[_]%' then 'replicate'
                                                        --when distribution_type is null and @manual_fact_dim like 'fact[_]%' then isnull(distribution_type,'hash('+@manual_fact_dim+'_key)') 
                                                        when distribution_type is not null then distribution_type
                                                        when @key_flag = 0 then 'replicate'
                                                        else isnull(distribution_type,'hash('+@manual_fact_dim+'_key)')  end
                                              from #table_def)

--For each table in #table_def,  assemble and execute the necessary create table SQL 
declare @sql varchar(max)

--Populate local variable @sql with the check for atypical records
if exists(select 1 from sys.tables where name = @manual_fact_dim)
begin
    set @sql = 'if exists(select top 1 1 from dbo.'+@manual_fact_dim+' where '+@manual_fact_dim+'_id > ''0'')'+char(13)+char(10)
               +'begin'+char(13)+char(10)
               --+'    raiserror(''Records exist in %s, skipping object recreation.  Truncate the table to force recreation.'',10,1,'''+@manual_fact_dim+''')'+char(13)+char(10)
               +'    declare @var varchar(50) = ''do nothing here'''+char(13)+char(10)
               +'end'+char(13)+char(10)
               +'else'+char(13)+char(10)
			   
end

--Append to @sql the if exists/drop table and the create table statement
set @sql = isnull(@sql,'')
           +'begin'+char(13)+char(10)
           +'    if exists(select 1 from sys.tables where name ='''+@manual_fact_dim+''')'+char(13)+char(10)
           +'    drop table dbo.'+@manual_fact_dim+char(13)+char(10)
           +char(13)+char(10)
           +'    create table dbo.'+@manual_fact_dim+'('+char(13)+char(10)
		   +'        '+@manual_fact_dim+'_id bigint identity not null,'+char(13)+char(10)

--Loop through the columns for @manual_fact_dim, appending to @sql the column and data type info
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
set @sql = @sql +'        dv_load_date_time datetime null,' +char(13)+char(10)
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
