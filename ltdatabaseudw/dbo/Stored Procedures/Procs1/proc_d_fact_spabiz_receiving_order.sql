﻿CREATE PROC [dbo].[proc_d_fact_spabiz_receiving_order] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_receiving_order','proc_d_fact_spabiz_receiving_order start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_receiving_order','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_receiving_order

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_receiving_order','#p_spabiz_ro_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_ro_insert') is not null drop table #p_spabiz_ro_insert
create table dbo.#p_spabiz_ro_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_ro.p_spabiz_ro_id,
       p_spabiz_ro.bk_hash,
       row_number() over (order by p_spabiz_ro_id) row_num
  from dbo.p_spabiz_ro
  join #batch_id
    on p_spabiz_ro.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_ro.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_ro.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_receiving_order','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_ro_insert.row_num,
       p_spabiz_ro.bk_hash fact_spabiz_receiving_order_key,
       p_spabiz_ro.ro_id receiving_order_id,
       p_spabiz_ro.store_number store_number,
       case when p_spabiz_ro.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ro.date = convert(date, '18991230', 112) then null
            else s_spabiz_ro.date
        end created_date_time,
       case
            when p_spabiz_ro.bk_hash in ('-997','-998','-999') then p_spabiz_ro.bk_hash
            when l_spabiz_ro.staff_id is null then '-998'
            when l_spabiz_ro.staff_id in (0, -1) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ro.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ro.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case
            when p_spabiz_ro.bk_hash in ('-997','-998','-999') then p_spabiz_ro.bk_hash
            when l_spabiz_ro.store_number is null then '-998'
            when l_spabiz_ro.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ro.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       case
            when p_spabiz_ro.bk_hash in ('-997','-998','-999') then p_spabiz_ro.bk_hash
            when l_spabiz_ro.vendor_id is null then '-998'
            when l_spabiz_ro.vendor_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ro.vendor_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ro.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_vendor_key,
       s_spabiz_ro.discount discount,
       s_spabiz_ro.edit_time edit_date_time,
       case
            when p_spabiz_ro.bk_hash in ('-997','-998','-999') then p_spabiz_ro.bk_hash
            when l_spabiz_ro.po_id is null then '-998'
            when l_spabiz_ro.po_id in (0, -1) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ro.po_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ro.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_purchase_order_key,
       s_spabiz_ro.freight freight,
       s_spabiz_ro.inv_date invoice_date_time,
       s_spabiz_ro.inv_number invoice_number,
       s_spabiz_ro.payment payment,
       s_spabiz_ro.retail_total retail_total,
       's_spabiz_ro.status_' + convert(varchar,convert(int,s_spabiz_ro.status)) status_dim_description_key,
       convert(int,s_spabiz_ro.status) status_id,
       s_spabiz_ro.sub_total sub_total,
       s_spabiz_ro.tax tax,
       s_spabiz_ro.total total,
       l_spabiz_ro.po_id l_spabiz_ro_po_id,
       l_spabiz_ro.staff_id l_spabiz_ro_staff_id,
       l_spabiz_ro.vendor_id l_spabiz_ro_vendor_id,
       s_spabiz_ro.status s_spabiz_ro_status,
       p_spabiz_ro.p_spabiz_ro_id,
       p_spabiz_ro.dv_batch_id,
       p_spabiz_ro.dv_load_date_time,
       p_spabiz_ro.dv_load_end_date_time
  from dbo.p_spabiz_ro
  join #p_spabiz_ro_insert
    on p_spabiz_ro.p_spabiz_ro_id = #p_spabiz_ro_insert.p_spabiz_ro_id
  join dbo.l_spabiz_ro
    on p_spabiz_ro.l_spabiz_ro_id = l_spabiz_ro.l_spabiz_ro_id
  join dbo.s_spabiz_ro
    on p_spabiz_ro.s_spabiz_ro_id = s_spabiz_ro.s_spabiz_ro_id
 where l_spabiz_ro.store_number not in (1,100,999) OR p_spabiz_ro.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_receiving_order', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_receiving_order',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_receiving_order
       where d_fact_spabiz_receiving_order.fact_spabiz_receiving_order_key in (select bk_hash from #p_spabiz_ro_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_spabiz_receiving_order(
                 d_fact_spabiz_receiving_order_id,
                 fact_spabiz_receiving_order_key,
                 receiving_order_id,
                 store_number,
                 created_date_time,
                 dim_spabiz_staff_key,
                 dim_spabiz_store_key,
                 dim_spabiz_vendor_key,
                 discount,
                 edit_date_time,
                 fact_spabiz_purchase_order_key,
                 freight,
                 invoice_date_time,
                 invoice_number,
                 payment,
                 retail_total,
                 status_dim_description_key,
                 status_id,
                 sub_total,
                 tax,
                 total,
                 l_spabiz_ro_po_id,
                 l_spabiz_ro_staff_id,
                 l_spabiz_ro_vendor_id,
                 s_spabiz_ro_status,
                 p_spabiz_ro_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_receiving_order_key,
             receiving_order_id,
             store_number,
             created_date_time,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             dim_spabiz_vendor_key,
             discount,
             edit_date_time,
             fact_spabiz_purchase_order_key,
             freight,
             invoice_date_time,
             invoice_number,
             payment,
             retail_total,
             status_dim_description_key,
             status_id,
             sub_total,
             tax,
             total,
             l_spabiz_ro_po_id,
             l_spabiz_ro_staff_id,
             l_spabiz_ro_vendor_id,
             s_spabiz_ro_status,
             p_spabiz_ro_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_receiving_order','proc_d_fact_spabiz_receiving_order end',@current_dv_batch_id
end
