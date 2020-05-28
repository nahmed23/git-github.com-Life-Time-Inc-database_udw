CREATE PROC [dbo].[proc_util_generate_procedure_dv_deleted] @source_object [varchar](500) AS
begin

set nocount on
set xact_abort on

if object_id('tempdb..#dv') is not null drop table #dv
create table #dv with (distribution = round_robin) as
select distinct
       dv_table,
       --dv_column,
       --business_key_sort_order,
       dense_rank() over (order by dv_table) table_rank--,
       --rank() over (partition by dv_table order by sort_order) column_rank
from dv_etl_map
where dv_table like 's[_]'+@source_object
or dv_table like 'l[_]'+@source_object

declare @sql varchar(max)
declare @hub_table varchar(500) = 'h_'+@source_object
declare @pit_table varchar(500) = 'p_'+@source_object

set @sql = case when exists(select 1 from sys.procedures where name = 'proc_dv_deleted_'+@source_object) then 'alter' else 'create' end 
           +' procedure dbo.proc_dv_deleted_'+@source_object+' ('+char(13)+char(10)
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
           +'declare @insert_date_time datetime'+char(13)+char(10)
           +'declare @user varchar(500) = suser_sname()'+char(13)+char(10)
           +char(13)+char(10)
           +'--THIS LOGIC (#bk_hash) IS ONLY A PLACEHOLDER AS AN EXAMPLE'+char(13)+char(10)
           +'--It may need to be manually updated for each individual object'+char(13)+char(10)
           +'--More logic than a simple query may be required, but the end result should be a #bk_hash table populated with bk_hashes, deleted times, and deleted batchids'+char(13)+char(10)
           +'if object_id(''tempdb..#bk_hash'') is not null drop table #bk_hash'+char(13)+char(10)
           +'create table #bk_hash with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as '+char(13)+char(10)
           +'select d.deleted_bk_hash bk_hash,'+char(13)+char(10)
           +'       d.dv_load_date_time,'+char(13)+char(10)
           +'       d.dv_batch_id'+char(13)+char(10)
           +'  from d_mms_deleted_data d --someday this needs a d_table'+char(13)+char(10)
           +' where d.dv_batch_id >= @current_dv_batch_id'+char(13)+char(10)
           +'   and d.table_name = ''product'''+char(13)+char(10)
           +'   and 0=1'+char(13)+char(10)
           +char(13)+char(10)
           +'set @insert_date_time = getdate()'+char(13)+char(10)
           +'update '+@hub_table+char(13)+char(10)
           +'   set dv_deleted = 1,'+char(13)+char(10)
           +'       dv_updated_date_time = @insert_date_time,'+char(13)+char(10)
           +'       dv_update_user = @user'+char(13)+char(10)
           +'  from #bk_hash'+char(13)+char(10)
           +' where '+@hub_table+'.bk_hash = #bk_hash.bk_hash'+char(13)+char(10)
           +char(13)+char(10)

--link and satellite section - handled equivalently
declare @ls_table varchar(500)
declare @table_start int = (select min(table_rank) from #dv)
declare @table_end int  = (select max(table_rank) from #dv)

while @table_start <= @table_end
begin

    set @ls_table = (select max(dv_table) from #dv where table_rank = @table_start)
    --set @source_dv_load_date_time_column = (select source_column from #dv where ls_rank = @table_start and dv_column = 'dv_load_date_time')
    --set @r_data_source_id = (select r_data_source_id from dbo.r_data_source where source_name = (select min(source) from #dv where ls_rank = @table_start))
    exec proc_util_generate_table_column_sql @ls_table
    
    set @sql = @sql 

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
            +'       dv_insert_user,'+char(13)+char(10)
            +'       dv_deleted)'+char(13)+char(10)
            +'select '+@ls_table+'.bk_hash,'+char(13)+char(10)
            +(select replace(column_list,'       ','       '+@ls_table+'.') from #proc_util_generate_table_column_sql)
            +'       #bk_hash.dv_load_date_time,'+char(13)+char(10)
            +'       #bk_hash.dv_batch_id,'+char(13)+char(10)
            +'       '+@ls_table+'.dv_r_load_source_id,'+char(13)+char(10)
            +'       '+@ls_table+'.dv_hash,'+char(13)+char(10)
            +'       @insert_date_time,'+char(13)+char(10)
            +'       @user,'+char(13)+char(10)
            +'       1 --deleted'+char(13)+char(10)
            +'  from '+@pit_table+char(13)+char(10)
            +'  join '+@ls_table+char(13)+char(10)
            +'    on '+@pit_table+'.bk_hash = '+@ls_table+'.bk_hash'+char(13)+char(10)
            +'   and '+@pit_table+'.'+@ls_table+'_id = '+@ls_table+'.'+@ls_table+'_id'+char(13)+char(10)
            +'  join #bk_hash'+char(13)+char(10)
            +'    on '+@pit_table+'.bk_hash = #bk_hash.bk_hash'+char(13)+char(10)
            +' where '+@pit_table+'.bk_hash in (select bk_hash from #bk_hash)'+char(13)+char(10)
            +'   and '+@pit_table+'.dv_load_end_date_time = ''dec 31, 9999'''+char(13)+char(10)
            +'   and isnull('+@ls_table+'.dv_deleted,0) != 1'+char(13)+char(10)
            +char(13)+char(10)
    
    set @table_start = @table_start+1

end

set @sql =@sql + 'end'

exec(@sql)

end
