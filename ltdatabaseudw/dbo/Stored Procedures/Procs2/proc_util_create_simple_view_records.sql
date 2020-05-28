CREATE PROC [dbo].[proc_util_create_simple_view_records] @d_table_name [varchar](500),@release [varchar](50),@schema [varchar](50),@view_name [varchar](500) AS
begin

set xact_abort on
set nocount on

--clear out existing records
delete from dv_d_etl_map where target_object = @view_name

if object_id('tempdb..#base_table') is not null drop table #base_table
create table dbo.#base_table with(distribution=round_robin, location=user_db, heap) as
select target_object,
       target_column, 
	   data_type, 
	   rank() over (order by column_rank) column_rank
  from v_d_etl_map_table_column_order  
 where target_object = @d_table_name
   and target_column not in (select distinct dv_table+'_'+dv_column from dv_etl_map)

if object_id('tempdb..#d_etl_map') is not null drop table #d_etl_map
create table dbo.#d_etl_map(target_object varchar(500) not null, target_column varchar(500) not null, data_type varchar(500) not null, source_sql varchar(8000) not null, release varchar(500) not null,view_schema varchar(50) null)
  with (location=user_db,heap)

declare @start int = 1
declare @end int = (select max(column_rank) from #base_table)
declare @column varchar(500), @data_type varchar(50), @definition varchar(max)
while @start <= @end
begin
    
    insert #d_etl_map 
    select @view_name, 
            target_column,
            data_type,
            target_object+'.'+target_column,
            @release,
            @schema
      from #base_table
     where column_rank = @start

    set @start = @start+1
end

declare @d_etl_map_count int
declare @start_d_etl_map_id bigint
set @d_etl_map_count = (select count(*) from #d_etl_map)
--exec proc_util_sequence_number_get_next @table_name = 'dv_d_etl_map', @id_count = @d_etl_map_count, @start_id = @start_d_etl_map_id out

insert dv_d_etl_map(target_object, target_column, data_type, source_sql, partition_scheme, release, dv_inserted_date_time, dv_insert_user,view_schema)
select --@start_d_etl_map_id - 1 + row_number() over (order by target_column) d_etl_map_id, 
       target_object, target_column, data_type, source_sql, null, release, getdate() inserted_date_time, suser_sname() insert_user, view_schema
  from #d_etl_map


drop table #base_table
drop table #d_etl_map

end


 













