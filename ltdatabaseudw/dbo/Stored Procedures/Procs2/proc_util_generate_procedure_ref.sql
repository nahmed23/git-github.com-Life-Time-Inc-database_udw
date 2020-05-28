CREATE PROC [dbo].[proc_util_generate_procedure_ref] @source_object [varchar](500) AS
begin

set nocount on
set xact_abort on

print '    ************************ proc_util_generate_procedure_ref start ************************'
declare @ref_table varchar(500) = 'r_'+@source_object
declare @source_dv_load_date_time_column varchar(500)= (select case when source_column = 'Jan 1, 1980' then '''Jan 1, 1980''' else source_column end from dbo.dv_etl_map where dv_table = @ref_table and dv_column = 'dv_load_date_time')

--grab dv_etl_map info for this table
if object_id('tempdb..#map') is not null drop table #map
create table #map with (distribution=round_robin, location=user_db) as 
select dv_table, dv_column, source_table, source_column, data_type,
       rank() over (order by sort_order) r
  from dbo.dv_etl_map
 where dv_table = @ref_table
   and dv_column <> 'dv_load_date_time'

declare @batch_hash_select varchar(max), @batch_hash_hash varchar(max), @todo_select varchar(max), @final_insert varchar(max), @column_start int, @column_end int, @bk_hash_list varchar(max), @source_stage_table varchar(500), @bk_where varchar(max)
set @source_stage_table = (select min(source_table) from #map)

exec proc_util_generate_table_column_sql @ref_table

declare @sql varchar(max)

--drop if exists
set @sql = 'if exists(select 1 from sys.procedures where name = ''proc_etl_'+@source_object+''')
drop proc proc_etl_'+@source_object

--print @sql
exec(@sql)

set @sql = case when exists(select 1 from sys.procedures where object_id = object_id('dbo.proc_etl_'+@source_object)) then 'alter' else 'create' end + ' procedure dbo.proc_etl_'+@source_object+'(
@current_dv_batch_id bigint,
@job_start_date_time_varchar varchar(19)
)
as
begin

set nocount on
set xact_abort on

declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

--join up incremental and current
if object_id(''tempdb..#process'') is not null drop table #process
create table dbo.#process with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as
select row_number() over (order by s.bk_hash) rownum,
       s.bk_hash,'+char(13)+char(10)
      +replace((select column_list from #proc_util_generate_table_column_sql),'       ','       s.')
      +'       case when r.'+@ref_table+'_id is null then s.dv_load_date_time'+char(13)+char(10)		
      +'            else @job_start_date_time end dv_load_date_time,
       convert(datetime,''Dec 31, 9999'',120) dv_load_end_date_time,
       @current_dv_batch_id dv_batch_id,
       2 dv_r_load_source_id,
       s.source_hash dv_hash,
       r.'+@ref_table+'_id
  from (select '+(select source_bk_hash from #proc_util_generate_table_column_sql)+' bk_hash,'+char(13)+char(10)
                +'               isnull('+@source_dv_load_date_time_column+', convert(datetime,''jan 1, 1980'',120)) dv_load_date_time,'+char(13)+char(10)
                +replace((select alias_list from #proc_util_generate_table_column_sql),'       ','               ')
                +'               '+(select source_hash from #proc_util_generate_table_column_sql)+' source_hash,'+char(13)+char(10)
                +'               dv_batch_id
          from dbo.'+@source_stage_table+char(13)+char(10)
  +'        '+(select bk_null_filter from #proc_util_generate_table_column_sql)+') s 
  left join '+@ref_table+' r
    on s.bk_hash = r.bk_hash
   and r.dv_load_end_date_time = convert(varchar,''dec 31, 9999'',120)
 where r.'+@ref_table+'_id is null
    or (r.'+@ref_table+'_id is not null
        and s.source_hash <> r.dv_hash)

declare @start_r_id bigint, @c int, @user varchar(50)   
--set @c = isnull((select max(rownum) from #process),0)

--exec dbo.proc_util_sequence_number_get_next @table_name = '''+@ref_table+''', @id_count = @c, @start_id = @start_r_id out

begin tran
--end date existing business keys that have a new record with a different hash coming in
set @user = suser_sname()
update dbo.'+@ref_table+'
   set dv_load_end_date_time = @job_start_date_time,
       dv_updated_date_time = getdate(),
	   dv_update_user = @user
  from #process
 where '+@ref_table+'.'+@ref_table+'_id = #process.'+@ref_table+'_id

--insert incremental changes 
insert into dbo.'+@ref_table+' (
       bk_hash,'+char(13)+char(10)
       +(select column_list from #proc_util_generate_table_column_sql)
       +'       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
	   dv_inserted_date_time,
	   dv_insert_user,
	   dv_updated_date_time,
	   dv_update_user)
select bk_hash,'+char(13)+char(10)
       +(select column_list from #proc_util_generate_table_column_sql)
       +'       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
	   getdate(),
	   suser_sname(),
	   null,
	   null
  from #process
commit tran

end
'

--print @sql
exec(@sql)

set @sql = 'grant execute on dbo.proc_etl_'+@source_object+' to InformaticaUser'

exec(@sql)

drop table #map

print '    ************************ proc_util_generate_procedure_ref end ************************'

end
