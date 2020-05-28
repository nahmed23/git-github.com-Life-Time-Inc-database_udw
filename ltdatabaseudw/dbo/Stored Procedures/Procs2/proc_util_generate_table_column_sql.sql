CREATE PROC [dbo].[proc_util_generate_table_column_sql] @table_name [varchar](500) AS
begin

set nocount on
set xact_abort on
/*
This procedure generates some useful values.
It is only used the hub and ref generating procedures.
*/
declare @dv_bk_hash varchar(max) = 'convert(char(32),hashbytes(''md5'',(', @source_hash varchar(max)= 'convert(char(32),hashbytes(''md5'',(', @source_bk_hash varchar(max) = 'convert(char(32),hashbytes(''md5'',('
declare @column_start int, @column_end int, @source_table varchar(500)
declare @bk_null_filter varchar(max), @column_list varchar(max), @alias_list varchar(max)

if object_id('tempdb..#proc_util_generate_table_column_sql') is not null drop table #proc_util_generate_table_column_sql
create table dbo.#proc_util_generate_table_column_sql
        (source_bk_hash varchar(max),
         source_hash varchar(max),
         bk_null_filter varchar(max),
         column_list varchar(max),
         alias_list varchar(max))
  with (heap)

if object_id('tempdb..#hash_etl_map') is not null drop table #hash_etl_map
create table dbo.#hash_etl_map with(distribution=round_robin, location=user_db) as
select dv_table,
       dv_column, 
       data_type, 
       business_key_sort_order, 
       case when @table_name like 'p[_]%' then dv_table else source_table end source_table,
       source_column,
       dense_rank() over (order by sort_order) column_rank,
       isnull(dv_hash_ignore_flag,0) dv_hash_ignore_flag
  from dv_etl_map 
 where dv_table = @table_name 
   and dv_column <> 'dv_load_date_time' 
     
set @source_table = (select min(source_table) from #hash_etl_map)

set @column_start = 1
set @column_end = (select max(column_rank) from #hash_etl_map)

while @column_start <= @column_end
begin
            
    if (select business_key_sort_order from #hash_etl_map where column_rank = @column_start) is not null
    begin
        --generates the bk hash calculation out of the source staging table
        set @source_bk_hash = isnull(@source_bk_hash,'') + (select case when @column_start > 1 then '+' else '' end
                                                                    +'''P%#&z$@k''+'+case when data_type = 'bigint' then 'isnull(cast('+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type = 'bit' then 'isnull(cast('+source_column+' as varchar(42)),''z#@$k%&P'')'
                                                                                        when data_type like 'char%' then 'isnull('+source_column + ',''z#@$k%&P'')'
                                                                                        when data_type like 'datetime%' then 'isnull(convert(varchar,'+source_column+',120),''z#@$k%&P'')'
                                                                                        when data_type like 'decimal%' then 'isnull(cast('+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type like 'float' then 'isnull(cast('+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type = 'int' then 'isnull(cast('+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type like 'nchar%' then 'isnull('+source_column +',''z#@$k%&P'')'
                                                                                        when data_type like 'numeric%' then 'isnull(cast('+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type like 'nvarchar%' then 'isnull('+source_column + ',''z#@$k%&P'')'
                                                                                        when data_type like 'smalldatetime%' then 'isnull(convert(varchar,'+source_column+',120),''z#@$k%&P'')'
                                                                                        when data_type = 'smallint' then 'isnull(cast('+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type like 'uniqueidentifier%' then 'isnull(cast('+source_column + ' as char(36)),''z#@$k%&P'')'
                                                                                        when data_type like 'varchar%' then 'isnull('+source_column+ ',''z#@$k%&P'')'
                                                                                        when data_type like '%binary%' then 'isnull(convert(varchar(500), '+source_column+', 2),''z#@$k%&P'')'
                                                                                        else 'isnull(cast('+source_column+' as varchar(500)),''z#@$k%&P'')' end
                                                                from #hash_etl_map
                                                                where column_rank = @column_start)
        
        --This generates the necessary WHERE clause logic for filtering out records that come across with no valid [partial] business key
        set @bk_null_filter = isnull(@bk_null_filter,'') + (select case when @column_start = 1 then ' where ('+source_column+' is not null'
                                                                        else char(13)+char(10)+'        or '+source_column+' is not null' end
                                                                from #hash_etl_map
                                                                where column_rank = @column_start)
    end

    --generates the record hash value out of the stage table into the link/satellite/ref table
    set @source_hash = isnull(@source_hash,'') + isnull((select case when @column_start > 1 then char(13)+char(10)+'                                        +' else '' end
                                                                  +'''P%#&z$@k''+'+case when data_type = 'bigint' then 'isnull(cast('+source_table+'.'+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type = 'bit' then 'isnull(cast('+source_table+'.'+source_column+' as varchar(42)),''z#@$k%&P'')'
                                                                                        when data_type like 'char%' then 'isnull('+source_table+'.'+source_column + ',''z#@$k%&P'')'
                                                                                        when data_type like 'datetime%' then 'isnull(convert(varchar,'+source_table+'.'+source_column+',120),''z#@$k%&P'')'
                                                                                        when data_type like 'decimal%' then 'isnull(cast('+source_table+'.'+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type like 'float' then 'isnull(cast('+source_table+'.'+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type = 'int' then 'isnull(cast('+source_table+'.'+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type like 'nchar%' then 'isnull('+source_table+'.'+source_column +',''z#@$k%&P'')'
                                                                                        when data_type like 'numeric%' then 'isnull(cast('+source_table+'.'+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type like 'nvarchar%' then 'isnull('+source_table+'.'+source_column + ',''z#@$k%&P'')'
                                                                                        when data_type like 'smalldatetime%' then 'isnull(convert(varchar,'+source_table+'.'+source_column+',120),''z#@$k%&P'')'
                                                                                        when data_type = 'smallint' then 'isnull(cast('+source_table+'.'+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                        when data_type like 'uniqueidentifier%' then 'isnull(cast('+source_table+'.'+source_column + ' as char(36)),''z#@$k%&P'')'
                                                                                        when data_type like 'varchar%' then 'isnull('+source_table+'.'+source_column+ ',''z#@$k%&P'')'
                                                                                        when data_type like '%binary%' then 'isnull(convert(varchar(500), '+source_table+'.'+source_column+', 2),''z#@$k%&P'')'
                                                                                        else 'isnull(cast('+source_table+'.'+source_column+' as varchar(500)),''z#@$k%&P'')'
                                                                                    end --+ case when @column_start < @column_end then '+'+char(13)+char(10) else '' end
                                                                        from #hash_etl_map
                                                                       where column_rank = @column_start
                                                                         and dv_hash_ignore_flag <> 1)
                                                    ,'')

    --generates a simple list of the target table columns
    --for pit tables
    set @column_list = isnull(@column_list,'') + (select '       '+case when @table_name like 'p[_]%' then dv_table+'.'+dv_column+case when @column_start = @column_end then '' else ',' end
                                                                        else dv_column+',' end+char(13)+char(10)
                                                    from #hash_etl_map
                                                   where column_rank = @column_start)

    --generates a list 
    set @alias_list = isnull(@alias_list,'')+ (select '       '+source_table+'.'+source_column+case when @table_name like 'p[_]%' then '' else ' '+dv_column end +','+char(13)+char(10)
                                                 from #hash_etl_map
                                                where column_rank = @column_start)

    set @column_start = @column_start + 1
end

set @bk_null_filter = @bk_null_filter+')'
set @dv_bk_hash = @dv_bk_hash+')),2)'
set @source_bk_hash = @source_bk_hash+')),2)'
set @source_hash = @source_hash+')),2)'

insert into #proc_util_generate_table_column_sql
select @source_bk_hash,@source_hash, @bk_null_filter, @column_list, @alias_list





end

