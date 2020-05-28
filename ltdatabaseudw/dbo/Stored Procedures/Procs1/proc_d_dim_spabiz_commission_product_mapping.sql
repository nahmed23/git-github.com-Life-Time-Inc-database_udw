CREATE PROC [dbo].[proc_d_dim_spabiz_commission_product_mapping] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_commission_product_mapping','proc_d_dim_spabiz_commission_product_mapping start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_commission_product_mapping','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_dim_spabiz_commission_product_mapping

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_commission_product_mapping','#p_spabiz_commission_product_mapping_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_commission_product_mapping_insert') is not null drop table #p_spabiz_commission_product_mapping_insert
create table dbo.#p_spabiz_commission_product_mapping_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_commission_product_mapping.p_spabiz_commission_product_mapping_id,
       p_spabiz_commission_product_mapping.bk_hash,
       row_number() over (order by p_spabiz_commission_product_mapping_id) row_num
  from dbo.p_spabiz_commission_product_mapping
  join #batch_id
    on p_spabiz_commission_product_mapping.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_commission_product_mapping.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_commission_product_mapping.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_commission_product_mapping','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_commission_product_mapping_insert.row_num,
       p_spabiz_commission_product_mapping.bk_hash dim_spabiz_commission_product_mapping_key,
       p_spabiz_commission_product_mapping.product_name product_name,
       s_spabiz_commission_product_mapping.mapping_group_name mapping_group_name,
       s_spabiz_commission_product_mapping.product_mapping_type product_mapping_type,
       p_spabiz_commission_product_mapping.p_spabiz_commission_product_mapping_id,
       p_spabiz_commission_product_mapping.dv_batch_id,
       p_spabiz_commission_product_mapping.dv_load_date_time,
       p_spabiz_commission_product_mapping.dv_load_end_date_time
  from dbo.p_spabiz_commission_product_mapping
  join #p_spabiz_commission_product_mapping_insert
    on p_spabiz_commission_product_mapping.p_spabiz_commission_product_mapping_id = #p_spabiz_commission_product_mapping_insert.p_spabiz_commission_product_mapping_id
  join dbo.s_spabiz_commission_product_mapping
    on p_spabiz_commission_product_mapping.s_spabiz_commission_product_mapping_id = s_spabiz_commission_product_mapping.s_spabiz_commission_product_mapping_id

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_dim_spabiz_commission_product_mapping', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_commission_product_mapping',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_dim_spabiz_commission_product_mapping
       where d_dim_spabiz_commission_product_mapping.dim_spabiz_commission_product_mapping_key in (select bk_hash from #p_spabiz_commission_product_mapping_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_dim_spabiz_commission_product_mapping(
                 d_dim_spabiz_commission_product_mapping_id,
                 dim_spabiz_commission_product_mapping_key,
                 product_name,
                 mapping_group_name,
                 product_mapping_type,
                 p_spabiz_commission_product_mapping_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             dim_spabiz_commission_product_mapping_key,
             product_name,
             mapping_group_name,
             product_mapping_type,
             p_spabiz_commission_product_mapping_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
       where row_num >= @start
         and row_num < @start+1000000
    commit tran

    set @start = @start+1000000
end

--Done!
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_commission_product_mapping','proc_d_dim_spabiz_commission_product_mapping end',@current_dv_batch_id
end
