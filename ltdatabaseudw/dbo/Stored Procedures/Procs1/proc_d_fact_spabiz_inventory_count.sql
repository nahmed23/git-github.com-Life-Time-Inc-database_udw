CREATE PROC [dbo].[proc_d_fact_spabiz_inventory_count] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_count','proc_d_fact_spabiz_inventory_count start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_count','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_inventory_count

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_count','#p_spabiz_inv_count_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_inv_count_insert') is not null drop table #p_spabiz_inv_count_insert
create table dbo.#p_spabiz_inv_count_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_inv_count.p_spabiz_inv_count_id,
       p_spabiz_inv_count.bk_hash,
       row_number() over (order by p_spabiz_inv_count_id) row_num
  from dbo.p_spabiz_inv_count
  join #batch_id
    on p_spabiz_inv_count.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_inv_count.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_inv_count.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_count','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_inv_count_insert.row_num,
       p_spabiz_inv_count.bk_hash fact_spabiz_inventory_count_key,
       p_spabiz_inv_count.inv_count_id inv_count_id,
       p_spabiz_inv_count.store_number store_number,
       case when p_spabiz_inv_count.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_inv_count.date = convert(date, '18991230', 112) then null
            else s_spabiz_inv_count.date
        end created_date_time,
       case when p_spabiz_inv_count.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_inv_count.date_expected = convert(date, '18991230', 112) then null
            else s_spabiz_inv_count.date_expected
        end date_expected_date_time,
       case when p_spabiz_inv_count.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_inv_count.date_started = convert(date, '18991230', 112) then null
            else s_spabiz_inv_count.date_started
        end date_started_date_time,
       case
            when p_spabiz_inv_count.bk_hash in ('-997','-998','-999') then p_spabiz_inv_count.bk_hash
            when l_spabiz_inv_count.staff_id is null then '-998'
            when l_spabiz_inv_count.staff_id in (0, -1) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_count.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_inv_count.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case
            when p_spabiz_inv_count.bk_hash in ('-997','-998','-999') then p_spabiz_inv_count.bk_hash
            when l_spabiz_inv_count.store_number is null then '-998'
            when l_spabiz_inv_count.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_inv_count.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_inv_count.edit_time edit_date_time,
       case
            when s_spabiz_inv_count.end_range is null then ''
            else s_spabiz_inv_count.end_range
       end end_range,
       case
            when s_spabiz_inv_count.extra is null then ''
            else s_spabiz_inv_count.extra
       end extra_query_text_filter,
       case
            when s_spabiz_inv_count.num is null then 0
            when isnumeric(s_spabiz_inv_count.num) = 0 then 0
            else s_spabiz_inv_count.num
       end inventory_count,
       case
            when s_spabiz_inv_count.num_adjusted is null then 0
            else s_spabiz_inv_count.num_adjusted
       end inventory_count_adjustment,
       s_spabiz_inv_count.inv_effect inventory_effect,
       's_spabiz_inv_count.item_type_' + convert(varchar,convert(int,s_spabiz_inv_count.item_type)) item_type_dim_description_key,
       convert(int,s_spabiz_inv_count.item_type) item_type_id,
       case
            when s_spabiz_inv_count.name is null then ''
            else s_spabiz_inv_count.name
       end name,
       's_spabiz_inv_count.sort_count_by_' + convert(varchar,convert(int,s_spabiz_inv_count.sort_count_by)) sort_count_by_dim_description_key,
       convert(int,s_spabiz_inv_count.sort_count_by) sort_count_by_id,
       case
            when s_spabiz_inv_count.start_range is null then ''
            else s_spabiz_inv_count.start_range
       end start_range,
       's_spabiz_inv_count.status_' + convert(varchar,convert(int,s_spabiz_inv_count.status)) status_dim_description_key,
       convert(int,s_spabiz_inv_count.status) status_id,
       case
            when s_spabiz_inv_count.total_skus is null then 0
            else s_spabiz_inv_count.total_skus
       end total_skus,
       l_spabiz_inv_count.staff_id l_spabiz_inv_count_staff_id,
       p_spabiz_inv_count.p_spabiz_inv_count_id,
       p_spabiz_inv_count.dv_batch_id,
       p_spabiz_inv_count.dv_load_date_time,
       p_spabiz_inv_count.dv_load_end_date_time
  from dbo.p_spabiz_inv_count
  join #p_spabiz_inv_count_insert
    on p_spabiz_inv_count.p_spabiz_inv_count_id = #p_spabiz_inv_count_insert.p_spabiz_inv_count_id
  join dbo.l_spabiz_inv_count
    on p_spabiz_inv_count.l_spabiz_inv_count_id = l_spabiz_inv_count.l_spabiz_inv_count_id
  join dbo.s_spabiz_inv_count
    on p_spabiz_inv_count.s_spabiz_inv_count_id = s_spabiz_inv_count.s_spabiz_inv_count_id
 where l_spabiz_inv_count.store_number not in (1,100,999) OR p_spabiz_inv_count.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_inventory_count', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_count',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_inventory_count
       where d_fact_spabiz_inventory_count.fact_spabiz_inventory_count_key in (select bk_hash from #p_spabiz_inv_count_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_spabiz_inventory_count(
                 d_fact_spabiz_inventory_count_id,
                 fact_spabiz_inventory_count_key,
                 inv_count_id,
                 store_number,
                 created_date_time,
                 date_expected_date_time,
                 date_started_date_time,
                 dim_spabiz_staff_key,
                 dim_spabiz_store_key,
                 edit_date_time,
                 end_range,
                 extra_query_text_filter,
                 inventory_count,
                 inventory_count_adjustment,
                 inventory_effect,
                 item_type_dim_description_key,
                 item_type_id,
                 name,
                 sort_count_by_dim_description_key,
                 sort_count_by_id,
                 start_range,
                 status_dim_description_key,
                 status_id,
                 total_skus,
                 l_spabiz_inv_count_staff_id,
                 p_spabiz_inv_count_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_inventory_count_key,
             inv_count_id,
             store_number,
             created_date_time,
             date_expected_date_time,
             date_started_date_time,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             edit_date_time,
             end_range,
             extra_query_text_filter,
             inventory_count,
             inventory_count_adjustment,
             inventory_effect,
             item_type_dim_description_key,
             item_type_id,
             name,
             sort_count_by_dim_description_key,
             sort_count_by_id,
             start_range,
             status_dim_description_key,
             status_id,
             total_skus,
             l_spabiz_inv_count_staff_id,
             p_spabiz_inv_count_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_inventory_count','proc_d_fact_spabiz_inventory_count end',@current_dv_batch_id
end
