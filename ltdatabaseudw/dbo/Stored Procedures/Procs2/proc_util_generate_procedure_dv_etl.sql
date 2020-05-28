CREATE PROC [dbo].[proc_util_generate_procedure_dv_etl] @source_object [varchar](500) AS
begin

set nocount on
set xact_abort on

--print '    ************************ proc_util_generate_procedure_dv start ************************'

declare @sql varchar(max)

declare @hub_table varchar(500) = 'h_'+@source_object
declare @pit_table varchar(500) = 'p_'+@source_object
    
--this only preps the link and satellites, since the hub and pit pretty much only need the table name
if object_id('tempdb..#map') is not null drop table #map
create table dbo.#map with(distribution=round_robin, location=user_db, clustered index(dv_table)) as
select dv_table, 
       dv_column, 
       source_column,source_table,source, business_key_sort_order,sort_order,generate_delete_proc_flag,
       is_truncated_staging,
       case when dv_column = 'dv_load_date_time' then null
            else dense_rank() over (partition by dv_table order by sort_order) end column_rank,
       dense_rank() over (order by dv_table) ls_rank
from dv_etl_map
where dv_table in (select source_table from dv_etl_map where dv_table = @pit_table and business_key_sort_order is null group by source_table)

declare @stage_table varchar(500) = (select min(source_table) from #map)
declare @stage_hash_table varchar(500) = 'stage_hash_'+substring(@stage_table,7,len(@stage_table))
declare @is_truncated_staging int = (select max(cast(is_truncated_staging as int)) from #map)

--need this for the full list of staging columns
exec proc_util_generate_table_column_sql @stage_table
declare @staging_columns varchar(max) = (select column_list from #proc_util_generate_table_column_sql)

--everything else comes from the hub
exec proc_util_generate_table_column_sql @hub_table

declare @source_dv_load_date_time_column varchar(500) = (select source_column from dv_etl_map where dv_table = @hub_table and dv_column = 'dv_load_date_time')
declare @r_data_source_id int = (select r_data_source_id from dbo.r_data_source where source_name = (select min(source) from dv_etl_map where dv_table = @hub_table))

set @sql = case when exists(select 1 from sys.procedures where name = 'proc_etl_'+@source_object) then 'alter' else 'create' end 
           +' procedure dbo.proc_etl_'+@source_object+' ('+char(13)+char(10)
           +'  @current_dv_batch_id bigint,'+char(13)+char(10)
           +'  @job_start_date_time_varchar varchar(19)'+char(13)+char(10)
           +')'+char(13)+char(10)
           +'as'+char(13)+char(10)
           +'begin'+char(13)+char(10)
           +char(13)+char(10)
           +'set nocount on'+char(13)+char(10)
           +'set xact_abort on'+char(13)+char(10)
           +char(13)+char(10)
           +'--Start!'+char(13)+char(10)
           +'declare @job_start_date_time datetime'+char(13)+char(10)
           +'set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)'+char(13)+char(10)
           +char(13)+char(10)
           +'declare @user varchar(50) = suser_sname()'+char(13)+char(10)
           +'declare @insert_date_time datetime'+char(13)+char(10)
           +char(13)+char(10)
           +case when @is_truncated_staging = 0 then 'delete from '+@stage_hash_table+' where dv_batch_id = @current_dv_batch_id'
                 else 'truncate table '+@stage_hash_table
                 end+char(13)+char(10)
           +char(13)+char(10)
           +'set @insert_date_time = getdate()'+char(13)+char(10)
           +'insert into dbo.'+@stage_hash_table+' ('+char(13)+char(10)
           +'       bk_hash,'+char(13)+char(10)
           +@staging_columns
           +'       dv_load_date_time,'+char(13)+char(10)
           --+'       dv_inserted_date_time,'+char(13)+char(10) --removed
           --+'       dv_insert_user,'+char(13)+char(10) --removed
           +'       dv_batch_id)'+char(13)+char(10)
           +'select '+(select source_bk_hash from #proc_util_generate_table_column_sql)+' bk_hash,'+char(13)+char(10)
           +@staging_columns
           +'       isnull(cast('+@stage_table+'.'+@source_dv_load_date_time_column+' as datetime),''Jan 1, 1753'') dv_load_date_time,'+char(13)+char(10)
           --+'       @insert_date_time,'+char(13)+char(10) --removed
           --+'       @user,'+char(13)+char(10) --removed
           +'       dv_batch_id'+char(13)+char(10)
           +'  from '+@stage_table+char(13)+char(10)
           +' where dv_batch_id = @current_dv_batch_id'+char(13)+char(10)
           +char(13)+char(10)
           +'--Run PIT proc for retry logic'+char(13)+char(10)
           +'exec dbo.proc_'+@pit_table+' @current_dv_batch_id'+char(13)+char(10)
           +char(13)+char(10)
           +'--Insert/update new hub business keys'+char(13)+char(10)
           +'set @insert_date_time = getdate()'+char(13)+char(10)
           +'insert into '+@hub_table+' ('+char(13)+char(10)
           +'       bk_hash,'+char(13)+char(10)---------------
           +(select column_list from #proc_util_generate_table_column_sql)
           +'       dv_load_date_time,'+char(13)+char(10)
           +'       dv_batch_id,'+char(13)+char(10)
           +'       dv_r_load_source_id,'+char(13)+char(10)
           +'       dv_inserted_date_time,'+char(13)+char(10)
           +'       dv_insert_user)'+char(13)+char(10)
           +'select distinct '+@stage_hash_table+'.bk_hash,'+char(13)+char(10)
           +(select replace(alias_list,@stage_table,@stage_hash_table)  from #proc_util_generate_table_column_sql)
           +'       isnull(cast('+@stage_hash_table+'.'+@source_dv_load_date_time_column+' as datetime),''Jan 1, 1753'') dv_load_date_time,'+char(13)+char(10)
           +'       @current_dv_batch_id,'+char(13)+char(10)
           +'       '+cast(@r_data_source_id as varchar)+','+char(13)+char(10)
           +'       @insert_date_time,'+char(13)+char(10)
           +'       @user'+char(13)+char(10)
           +'  from '+@stage_hash_table+char(13)+char(10)
           +'  left join '+@hub_table+char(13)+char(10)
           +'    on '+@stage_hash_table+'.bk_hash = '+@hub_table+'.bk_hash'+char(13)+char(10)
           +' where '+@hub_table+'_id is null'+char(13)+char(10)
           +'   and '+@stage_hash_table+'.dv_batch_id = @current_dv_batch_id'+char(13)+char(10)
           +char(13)+char(10)

--link and satellite section - handled equivalently
declare @ls_table varchar(500)
declare @table_start int = (select min(ls_rank) from #map)
declare @table_end int  = (select max(ls_rank) from #map)

while @table_start <= @table_end
begin

    set @ls_table = (select max(dv_table) from #map where ls_rank = @table_start)
    set @source_dv_load_date_time_column = (select source_column from #map where ls_rank = @table_start and dv_column = 'dv_load_date_time')
    set @r_data_source_id = (select r_data_source_id from dbo.r_data_source where source_name = (select min(source) from #map where ls_rank = @table_start))
    exec proc_util_generate_table_column_sql @ls_table
    
    set @sql = @sql 

            +'--calculate hash and lookup to current '+@ls_table+char(13)+char(10)
            +'if object_id(''tempdb..#'+@ls_table+'_inserts'') is not null drop table #'+@ls_table+'_inserts'+char(13)+char(10)
            +'create table #'+@ls_table+'_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as '+char(13)+char(10)
            + 'select '+@stage_hash_table+'.bk_hash,'+char(13)+char(10)
            +(select replace(alias_list,@stage_table,@stage_hash_table)  from #proc_util_generate_table_column_sql)
            +'       isnull(cast('+@stage_hash_table+'.'+@source_dv_load_date_time_column+' as datetime),''Jan 1, 1753'') dv_load_date_time,'+char(13)+char(10)
            +'       '+(select replace(source_hash,@stage_table,@stage_hash_table) from #proc_util_generate_table_column_sql)+' source_hash'+char(13)+char(10)
            +'  from dbo.'+@stage_hash_table+char(13)+char(10)
            +' where '+@stage_hash_table+'.dv_batch_id = @current_dv_batch_id'+char(13)+char(10)
            +char(13)+char(10)
            +'--Insert all updated and new '+@ls_table+' records'+char(13)+char(10)
            +'set @insert_date_time = getdate()'+char(13)+char(10)
            +'insert into '+@ls_table+' ('+char(13)+char(10)
            +'       bk_hash,'+char(13)+char(10)
            +(select column_list from #proc_util_generate_table_column_sql)
            +'       dv_load_date_time,'+char(13)+char(10)
            +'       dv_batch_id,'+char(13)+char(10)
            +'       dv_r_load_source_id,'+char(13)+char(10)
            +'       dv_hash,'+char(13)+char(10)
            +'       dv_inserted_date_time,'+char(13)+char(10)
            +'       dv_insert_user)'+char(13)+char(10)
            +'select #'+@ls_table+'_inserts.bk_hash,'+char(13)+char(10)
            +(select replace(column_list,'       ','       #'+@ls_table+'_inserts.') from #proc_util_generate_table_column_sql)
            +'       case when '+@ls_table+'.'+@ls_table+'_id is null then isnull(#'+@ls_table+'_inserts.dv_load_date_time,convert(datetime,''jan 1, 1753'',120))'+char(13)+char(10)
            +'            else @job_start_date_time end,'+char(13)+char(10)
            +'       @current_dv_batch_id,'+char(13)+char(10)
            +'       '+cast(@r_data_source_id as varchar)+','+char(13)+char(10)
            +'       #'+@ls_table+'_inserts.source_hash,'+char(13)+char(10)
            +'       @insert_date_time,'+char(13)+char(10)
            +'       @user'+char(13)+char(10)
            +'  from #'+@ls_table+'_inserts'+char(13)+char(10)
            +'  left join '+@pit_table+char(13)+char(10)
            +'    on #'+@ls_table+'_inserts.bk_hash = '+@pit_table+'.bk_hash'+char(13)+char(10)
            +'   and '+@pit_table+'.dv_load_end_date_time = ''Dec 31, 9999'''+char(13)+char(10)
            +'  left join '+@ls_table+char(13)+char(10)
            +'    on '+@pit_table+'.bk_hash = '+@ls_table+'.bk_hash'+char(13)+char(10)
            +'   and '+@pit_table+'.'+@ls_table+'_id = '+@ls_table+'.'+@ls_table+'_id'+char(13)+char(10)
            +' where '+@ls_table+'.'+@ls_table+'_id is null'+char(13)+char(10)
            +'    or ('+@ls_table+'.'+@ls_table+'_id is not null'+char(13)+char(10)
            +'        and '+@ls_table+'.dv_hash <> #'+@ls_table+'_inserts.source_hash)'+char(13)+char(10)
            +char(13)+char(10)
    
    set @table_start = @table_start+1

end

--Find all d_ tables that have sql referencing any of the objects in #map
if object_id('tempdb..#d_tables') is not null drop table #d_tables
create table dbo.#d_tables with(distribution=round_robin, location=user_db, clustered index (target_object)) as
select distinct target_object, dense_rank() over (order by target_object) r
  from dbo.dv_d_etl_map
 where target_object = 'd_'+@source_object
    or target_object = 'd_'+@source_object+'_history'

declare @start int = 1
declare @end int = (select max(r) from #d_tables)
declare @d_sql varchar(max) = '--run dimensional procs'+char(13)+char(10)

--Each identified d_table gets its proc called
while @start <= @end
begin

    set @d_sql =(select isnull(@d_sql,'')) 
               +isnull((select 'exec dbo.proc_'+target_object+' @current_dv_batch_id'+char(13)+char(10)
                   from #d_tables
                   join sys.procedures p on 'proc_'+target_object = p.name
                  where r = @start),'')

    set @start = @start+1
end

if exists(select top 1 1 from #map where generate_delete_proc_flag = 1)
begin
set @sql = (select isnull(@sql,'')
                   +'--Run the dv_deleted proc'+char(13)+char(10)
                   +'exec dbo.proc_dv_deleted_'+@source_object+' @current_dv_batch_id, @job_start_date_time_varchar'+char(13)+char(10)
                   +char(13)+char(10))
end

set @sql = (select isnull(@sql, '') +'--Run the PIT proc'+char(13)+char(10)
                   +'exec dbo.proc_'+@pit_table+' @current_dv_batch_id'+char(13)+char(10)
                   +char(13)+char(10)
                   + case when exists(select 1 from #d_tables) then @d_sql+char(13)+char(10)
                          else '' end
                   +'end'+char(13)+char(10))

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
--print @sql
      
--exec('grant execute on proc_etl_'+@source_object+' to InformaticaUser')


drop table #map
drop table #d_tables

--print '    ************************ proc_util_generate_procedure_dv end ************************'

end
