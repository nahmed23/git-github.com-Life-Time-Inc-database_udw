CREATE PROC [dbo].[proc_util_generate_structures_d] @source_object [varchar](500),@history_table_flag [int] AS
begin

set xact_abort on
set nocount on

--print '************************ proc_util_generate_structures_d start ************************'
--print ''

declare @manual_object_flag int = case when (select count(*) from dv_d_etl_map where target_object = @source_object and source_sql is null) > 0 then 1 else 0 end
declare @d_table_name varchar(1000), @sql varchar(8000)

declare @stage_table_name varchar(1000) = (select replace(source_table,'stage_','stage_hash_') from dv_etl_map where dv_table = 'h_'+@source_object group by replace(source_table,'stage_','stage_hash_'))

if exists(select 1 from sys.columns where object_id = object_id(@stage_table_name) and name = 'dv_inserted_date_time')
begin
    set @sql = 'alter table '+@stage_table_name+'  drop column dv_inserted_date_time'
    exec(@sql)
end
if exists(select 1 from sys.columns where object_id = object_id(@stage_table_name) and name = 'dv_insert_user')
begin
    set @sql = 'alter table '+@stage_table_name+'  drop column dv_insert_user'
    exec(@sql)
end


if @manual_object_flag = 0
begin

--d_table processing
    --make table
    exec proc_util_generate_table_d @source_object, @history_table_flag

    --make procedure
    if @history_table_flag = 0 exec proc_util_generate_procedure_d @source_object
    if @history_table_flag = 1 exec proc_util_generate_procedure_d_history @source_object

    --regenerate source_object etl proc
    exec proc_util_generate_procedure_dv_etl @source_object

    set @d_table_name = 'd_'+@source_object+case when @history_table_flag = 1 then '_history' else '' end
end
else
begin

--mfd processing
    exec proc_util_generate_table_mfd @source_object
    set @d_table_name = @source_object

end

--recreate any related views
exec proc_util_generate_views_d @d_table_name


--print ''
--print '************************ proc_util_generate_structures_d end ************************'

end
