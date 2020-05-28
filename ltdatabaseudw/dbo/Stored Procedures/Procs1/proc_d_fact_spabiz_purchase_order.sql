CREATE PROC [dbo].[proc_d_fact_spabiz_purchase_order] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_purchase_order','proc_d_fact_spabiz_purchase_order start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_purchase_order','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_purchase_order

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_purchase_order','#p_spabiz_po_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_po_insert') is not null drop table #p_spabiz_po_insert
create table dbo.#p_spabiz_po_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_po.p_spabiz_po_id,
       p_spabiz_po.bk_hash,
       row_number() over (order by p_spabiz_po_id) row_num
  from dbo.p_spabiz_po
  join #batch_id
    on p_spabiz_po.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_po.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_po.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_purchase_order','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_po_insert.row_num,
       p_spabiz_po.bk_hash fact_spabiz_purchase_order_key,
       p_spabiz_po.po_id po_id,
       p_spabiz_po.store_number store_number,
       case when p_spabiz_po.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_po.date = convert(date, '18991230', 112) then null
            else s_spabiz_po.date
        end created_date_time,
       case
            when p_spabiz_po.bk_hash in ('-997','-998','-999') then p_spabiz_po.bk_hash
            when l_spabiz_po.staff_id is null then '-998'
            when l_spabiz_po.staff_id in (0, -1) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_po.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_po.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case
            when p_spabiz_po.bk_hash in ('-997','-998','-999') then p_spabiz_po.bk_hash
            when l_spabiz_po.store_number is null then '-998'
            when l_spabiz_po.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_po.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       case
            when p_spabiz_po.bk_hash in ('-997','-998','-999') then p_spabiz_po.bk_hash
            when l_spabiz_po.vendor_id is null then '-998'
            when l_spabiz_po.vendor_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_po.vendor_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_po.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_vendor_key,
       s_spabiz_po.discount discount,
       s_spabiz_po.edit_time edit_date_time,
       s_spabiz_po.payment payment,
       s_spabiz_po.retail_total retail_total,
       's_spabiz_po.status_' + convert(varchar,convert(int,s_spabiz_po.status)) status_dim_description_key,
       convert(int,s_spabiz_po.status) status_id,
       s_spabiz_po.sub_total sub_total,
       s_spabiz_po.tax tax,
       s_spabiz_po.total total,
       l_spabiz_po.staff_id l_spabiz_po_staff_id,
       l_spabiz_po.vendor_id l_spabiz_po_vendor_id,
       s_spabiz_po.status s_spabiz_po_status,
       p_spabiz_po.p_spabiz_po_id,
       p_spabiz_po.dv_batch_id,
       p_spabiz_po.dv_load_date_time,
       p_spabiz_po.dv_load_end_date_time
  from dbo.p_spabiz_po
  join #p_spabiz_po_insert
    on p_spabiz_po.p_spabiz_po_id = #p_spabiz_po_insert.p_spabiz_po_id
  join dbo.l_spabiz_po
    on p_spabiz_po.l_spabiz_po_id = l_spabiz_po.l_spabiz_po_id
  join dbo.s_spabiz_po
    on p_spabiz_po.s_spabiz_po_id = s_spabiz_po.s_spabiz_po_id
 where l_spabiz_po.store_number not in (1,100,999) OR p_spabiz_po.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_purchase_order', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_purchase_order',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_purchase_order
       where d_fact_spabiz_purchase_order.fact_spabiz_purchase_order_key in (select bk_hash from #p_spabiz_po_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_spabiz_purchase_order(
                 d_fact_spabiz_purchase_order_id,
                 fact_spabiz_purchase_order_key,
                 po_id,
                 store_number,
                 created_date_time,
                 dim_spabiz_staff_key,
                 dim_spabiz_store_key,
                 dim_spabiz_vendor_key,
                 discount,
                 edit_date_time,
                 payment,
                 retail_total,
                 status_dim_description_key,
                 status_id,
                 sub_total,
                 tax,
                 total,
                 l_spabiz_po_staff_id,
                 l_spabiz_po_vendor_id,
                 s_spabiz_po_status,
                 p_spabiz_po_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_purchase_order_key,
             po_id,
             store_number,
             created_date_time,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             dim_spabiz_vendor_key,
             discount,
             edit_date_time,
             payment,
             retail_total,
             status_dim_description_key,
             status_id,
             sub_total,
             tax,
             total,
             l_spabiz_po_staff_id,
             l_spabiz_po_vendor_id,
             s_spabiz_po_status,
             p_spabiz_po_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_purchase_order','proc_d_fact_spabiz_purchase_order end',@current_dv_batch_id
end
