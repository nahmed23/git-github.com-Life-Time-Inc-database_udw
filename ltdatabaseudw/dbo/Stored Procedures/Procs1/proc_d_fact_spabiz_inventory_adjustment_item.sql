CREATE PROC [dbo].[proc_d_fact_spabiz_inventory_adjustment_item] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_adjustment_item','proc_d_fact_spabiz_inventory_adjustment_item start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_adjustment_item','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_inventory_adjustment_item

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_adjustment_item','#p_spabiz_inv_adj_data_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_inv_adj_data_insert') is not null drop table #p_spabiz_inv_adj_data_insert
create table dbo.#p_spabiz_inv_adj_data_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_inv_adj_data.p_spabiz_inv_adj_data_id,
       p_spabiz_inv_adj_data.bk_hash,
       row_number() over (order by p_spabiz_inv_adj_data_id) row_num
  from dbo.p_spabiz_inv_adj_data
  join #batch_id
    on p_spabiz_inv_adj_data.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_inv_adj_data.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_inv_adj_data.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_adjustment_item','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_inv_adj_data_insert.row_num,
       p_spabiz_inv_adj_data.bk_hash fact_spabiz_inventory_adjustment_item_key,
       p_spabiz_inv_adj_data.inv_adj_data_id inv_adj_data_id,
       p_spabiz_inv_adj_data.store_number store_number,
       s_spabiz_inv_adj_data.cost cost,
       case when p_spabiz_inv_adj_data.bk_hash in ('-997','-998','-999') then null
            else s_spabiz_inv_adj_data.date
        end created_date_time,
       case
            when p_spabiz_inv_adj_data.bk_hash in ('-997','-998','-999') then p_spabiz_inv_adj_data.bk_hash
            when l_spabiz_inv_adj_data.cat_id is null then '-998'
            when l_spabiz_inv_adj_data.cat_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.cat_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_category_key,
       case
            when p_spabiz_inv_adj_data.bk_hash in ('-997','-998','-999') then p_spabiz_inv_adj_data.bk_hash
            when l_spabiz_inv_adj_data.reason_id is null then '-998'
            when l_spabiz_inv_adj_data.reason_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.reason_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_inventory_adjustment_reason_key,
       case
            when p_spabiz_inv_adj_data.bk_hash in ('-997','-998','-999') then p_spabiz_inv_adj_data.bk_hash
            when l_spabiz_inv_adj_data.product_id is null then '-998'
            when l_spabiz_inv_adj_data.product_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.product_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_product_key,
       case
            when p_spabiz_inv_adj_data.bk_hash in ('-997','-998','-999') then p_spabiz_inv_adj_data.bk_hash
            when l_spabiz_inv_adj_data.staff_id is null then '-998'
            when l_spabiz_inv_adj_data.staff_id in (0, -1) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case
            when p_spabiz_inv_adj_data.bk_hash in ('-997','-998','-999') then p_spabiz_inv_adj_data.bk_hash
            when l_spabiz_inv_adj_data.store_number is null then '-998'
            when l_spabiz_inv_adj_data.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_inv_adj_data.edit_time edit_date_time,
       s_spabiz_inv_adj_data.qty quantity,
       case
            when s_spabiz_inv_adj_data.source_type = 86 then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.source_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.store_number as varchar(500)),'z#@$k%&P'))),2)
            else '-998'
        end source_fact_spabiz_inventory_count_key,
       case
            when s_spabiz_inv_adj_data.source_type = 1 then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.source_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_inv_adj_data.store_number as varchar(500)),'z#@$k%&P'))),2)
            else '-998'
        end source_fact_spabiz_receiving_order_key,
       's_spabiz_inv_adj_data.source_type_' + convert(varchar,convert(int,s_spabiz_inv_adj_data.source_type)) source_type_dim_description_key,
       convert(int,s_spabiz_inv_adj_data.source_type) source_type_id,
       's_spabiz_inv_adj_data.status_' + convert(varchar,convert(int,s_spabiz_inv_adj_data.status)) status_dim_description_key,
       convert(int,s_spabiz_inv_adj_data.status) status_id,
       l_spabiz_inv_adj_data.adj_id l_spabiz_inv_adj_data_adj_id,
       l_spabiz_inv_adj_data.cat_id l_spabiz_inv_adj_data_cat_id,
       l_spabiz_inv_adj_data.product_id l_spabiz_inv_adj_data_product_id,
       l_spabiz_inv_adj_data.reason_id l_spabiz_inv_adj_data_reason_id,
       l_spabiz_inv_adj_data.source_id l_spabiz_inv_adj_data_source_id,
       l_spabiz_inv_adj_data.staff_id l_spabiz_inv_adj_data_staff_id,
       s_spabiz_inv_adj_data.source_type s_spabiz_inv_adj_data_source_type,
       s_spabiz_inv_adj_data.status s_spabiz_inv_adj_data_status,
       p_spabiz_inv_adj_data.p_spabiz_inv_adj_data_id,
       p_spabiz_inv_adj_data.dv_batch_id,
       p_spabiz_inv_adj_data.dv_load_date_time,
       p_spabiz_inv_adj_data.dv_load_end_date_time
  from dbo.p_spabiz_inv_adj_data
  join #p_spabiz_inv_adj_data_insert
    on p_spabiz_inv_adj_data.p_spabiz_inv_adj_data_id = #p_spabiz_inv_adj_data_insert.p_spabiz_inv_adj_data_id
  join dbo.l_spabiz_inv_adj_data
    on p_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id = l_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id
  join dbo.s_spabiz_inv_adj_data
    on p_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id = s_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id
 where l_spabiz_inv_adj_data.store_number not in (1,100,999) OR p_spabiz_inv_adj_data.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_inventory_adjustment_item', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_adjustment_item',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_inventory_adjustment_item
       where d_fact_spabiz_inventory_adjustment_item.fact_spabiz_inventory_adjustment_item_key in (select bk_hash from #p_spabiz_inv_adj_data_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_spabiz_inventory_adjustment_item(
                 d_fact_spabiz_inventory_adjustment_item_id,
                 fact_spabiz_inventory_adjustment_item_key,
                 inv_adj_data_id,
                 store_number,
                 cost,
                 created_date_time,
                 dim_spabiz_category_key,
                 dim_spabiz_inventory_adjustment_reason_key,
                 dim_spabiz_product_key,
                 dim_spabiz_staff_key,
                 dim_spabiz_store_key,
                 edit_date_time,
                 quantity,
                 source_fact_spabiz_inventory_count_key,
                 source_fact_spabiz_receiving_order_key,
                 source_type_dim_description_key,
                 source_type_id,
                 status_dim_description_key,
                 status_id,
                 l_spabiz_inv_adj_data_adj_id,
                 l_spabiz_inv_adj_data_cat_id,
                 l_spabiz_inv_adj_data_product_id,
                 l_spabiz_inv_adj_data_reason_id,
                 l_spabiz_inv_adj_data_source_id,
                 l_spabiz_inv_adj_data_staff_id,
                 s_spabiz_inv_adj_data_source_type,
                 s_spabiz_inv_adj_data_status,
                 p_spabiz_inv_adj_data_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_inventory_adjustment_item_key,
             inv_adj_data_id,
             store_number,
             cost,
             created_date_time,
             dim_spabiz_category_key,
             dim_spabiz_inventory_adjustment_reason_key,
             dim_spabiz_product_key,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             edit_date_time,
             quantity,
             source_fact_spabiz_inventory_count_key,
             source_fact_spabiz_receiving_order_key,
             source_type_dim_description_key,
             source_type_id,
             status_dim_description_key,
             status_id,
             l_spabiz_inv_adj_data_adj_id,
             l_spabiz_inv_adj_data_cat_id,
             l_spabiz_inv_adj_data_product_id,
             l_spabiz_inv_adj_data_reason_id,
             l_spabiz_inv_adj_data_source_id,
             l_spabiz_inv_adj_data_staff_id,
             s_spabiz_inv_adj_data_source_type,
             s_spabiz_inv_adj_data_status,
             p_spabiz_inv_adj_data_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_adjustment_item','proc_d_fact_spabiz_inventory_adjustment_item end',@current_dv_batch_id
end
