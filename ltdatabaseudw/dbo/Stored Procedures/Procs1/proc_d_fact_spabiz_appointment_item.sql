CREATE PROC [dbo].[proc_d_fact_spabiz_appointment_item] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment_item','proc_d_fact_spabiz_appointment_item start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment_item','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_appointment_item

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment_item','#p_spabiz_ap_data_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_ap_data_insert') is not null drop table #p_spabiz_ap_data_insert
create table dbo.#p_spabiz_ap_data_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_ap_data.p_spabiz_ap_data_id,
       p_spabiz_ap_data.bk_hash,
       row_number() over (order by p_spabiz_ap_data_id) row_num
  from dbo.p_spabiz_ap_data
  join #batch_id
    on p_spabiz_ap_data.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_ap_data.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_ap_data.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment_item','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_ap_data_insert.row_num,
       p_spabiz_ap_data.bk_hash fact_spabiz_appointment_item_key,
       p_spabiz_ap_data.ap_data_id ap_data_id,
       p_spabiz_ap_data.store_number store_number,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.by_staff_id is null then '-998'
            when l_spabiz_ap_data.by_staff_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.by_staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end booked_by_dim_staff_key,
       case when s_spabiz_ap_data.booked_on_web in (1) then 'Y'
            else 'N'
        end booked_on_web_flag,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ap_data.check_in = convert(date, '18991230', 112) then null
            else s_spabiz_ap_data.check_in
        end check_in_date_time,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ap_data.check_out = convert(date, '18991230', 112) then null
            else s_spabiz_ap_data.check_out
        end check_out_date_time,
       's_spabiz_ap_data.data_type_' + convert(varchar,s_spabiz_ap_data.data_type) data_type_dim_description_key,
       s_spabiz_ap_data.data_type data_type_id,
       case when s_spabiz_ap_data.ap_data_delete in (-1) then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.service_id is null then '-998'
            when l_spabiz_ap_data.service_id in (0,-1) then '-998'
            when s_spabiz_ap_data.data_type in (44) then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.service_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
            else '-998'
        end dim_spabiz_block_time_key,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.cust_id is null then '-998'
            when l_spabiz_ap_data.cust_id in (0,-1) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.cust_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_customer_key,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.service_id is null then '-998'
            when l_spabiz_ap_data.service_id in (0,-1) then '-998'
            when s_spabiz_ap_data.data_type in (1,2,5) then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.service_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
            else '-998'
        end dim_spabiz_service_key,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.store_number is null then '-998'
            when l_spabiz_ap_data.store_number in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_ap_data.edit_time edit_date_time,
       s_spabiz_ap_data.end_time end_date_time,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.ap_id is null then '-998'
            when l_spabiz_ap_data.ap_id in (0,-1) then '-998'
            when l_spabiz_ap_data.resource_id in (0,-1) then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.ap_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
            else '-998'
        end fact_spabiz_appointment_key,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.ticket_data_id is null then '-998'
            when l_spabiz_ap_data.ticket_data_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.ticket_data_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_item_key,
       s_spabiz_ap_data.note note,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.parent_id is null then '-998'
            when l_spabiz_ap_data.parent_id in (0,-2) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.parent_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end parent_fact_spabiz_appointment_item_key,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.ap_id is null then '-998'
            when l_spabiz_ap_data.ap_id in (0,-1) then '-998'
            when l_spabiz_ap_data.resource_id not in (0,-1) then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.ap_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
            else '-998'
        end related_fact_spabiz_appointment_item_key,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_ap_data.res_block is null then 'N'
            when s_spabiz_ap_data.res_block in (0) then 'N'
            else 'Y'
        end resource_block_flag,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.resource_id is null then '-998'
            when l_spabiz_ap_data.resource_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.resource_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end resource_dim_spabiz_staff_key,
       case when p_spabiz_ap_data.bk_hash in ('-997','-998','-999') then p_spabiz_ap_data.bk_hash
            when l_spabiz_ap_data.staff_id is null then '-998'
            when l_spabiz_ap_data.staff_id in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap_data.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap_data.store_number as varchar(500)),'z#@$k%&P'))),2)
        end service_dim_spabiz_staff_key,
       case when s_spabiz_ap_data.standing in (1) then 'Y'
            else 'N'
        end standing_appointment_flag,
       s_spabiz_ap_data.start_time start_date_time,
       's_spabiz_ap_data.status_' + convert(varchar,s_spabiz_ap_data.status) status_dim_description_key,
       s_spabiz_ap_data.status status_id,
       l_spabiz_ap_data.ap_id l_spabiz_ap_data_ap_id,
       l_spabiz_ap_data.by_staff_id l_spabiz_ap_data_by_staff_id,
       l_spabiz_ap_data.cust_id l_spabiz_ap_data_cust_id,
       l_spabiz_ap_data.parent_id l_spabiz_ap_data_parent_id,
       l_spabiz_ap_data.resource_id l_spabiz_ap_data_resource_id,
       l_spabiz_ap_data.service_id l_spabiz_ap_data_service_id,
       l_spabiz_ap_data.staff_id l_spabiz_ap_data_staff_id,
       l_spabiz_ap_data.ticket_data_id l_spabiz_ap_data_ticket_data_id,
       s_spabiz_ap_data.data_type s_spabiz_ap_data_data_type,
       s_spabiz_ap_data.status s_spabiz_ap_data_status,
       p_spabiz_ap_data.p_spabiz_ap_data_id,
       p_spabiz_ap_data.dv_batch_id,
       p_spabiz_ap_data.dv_load_date_time,
       p_spabiz_ap_data.dv_load_end_date_time
  from dbo.p_spabiz_ap_data
  join #p_spabiz_ap_data_insert
    on p_spabiz_ap_data.p_spabiz_ap_data_id = #p_spabiz_ap_data_insert.p_spabiz_ap_data_id
  join dbo.l_spabiz_ap_data
    on p_spabiz_ap_data.l_spabiz_ap_data_id = l_spabiz_ap_data.l_spabiz_ap_data_id
  join dbo.s_spabiz_ap_data
    on p_spabiz_ap_data.s_spabiz_ap_data_id = s_spabiz_ap_data.s_spabiz_ap_data_id
 where p_spabiz_ap_data.store_number not in (1,100,999) OR p_spabiz_ap_data.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_appointment_item', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment_item',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_appointment_item
       where d_fact_spabiz_appointment_item.fact_spabiz_appointment_item_key in (select bk_hash from #p_spabiz_ap_data_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_spabiz_appointment_item(
                 d_fact_spabiz_appointment_item_id,
                 fact_spabiz_appointment_item_key,
                 ap_data_id,
                 store_number,
                 booked_by_dim_staff_key,
                 booked_on_web_flag,
                 check_in_date_time,
                 check_out_date_time,
                 data_type_dim_description_key,
                 data_type_id,
                 deleted_flag,
                 dim_spabiz_block_time_key,
                 dim_spabiz_customer_key,
                 dim_spabiz_service_key,
                 dim_spabiz_store_key,
                 edit_date_time,
                 end_date_time,
                 fact_spabiz_appointment_key,
                 fact_spabiz_ticket_item_key,
                 note,
                 parent_fact_spabiz_appointment_item_key,
                 related_fact_spabiz_appointment_item_key,
                 resource_block_flag,
                 resource_dim_spabiz_staff_key,
                 service_dim_spabiz_staff_key,
                 standing_appointment_flag,
                 start_date_time,
                 status_dim_description_key,
                 status_id,
                 l_spabiz_ap_data_ap_id,
                 l_spabiz_ap_data_by_staff_id,
                 l_spabiz_ap_data_cust_id,
                 l_spabiz_ap_data_parent_id,
                 l_spabiz_ap_data_resource_id,
                 l_spabiz_ap_data_service_id,
                 l_spabiz_ap_data_staff_id,
                 l_spabiz_ap_data_ticket_data_id,
                 s_spabiz_ap_data_data_type,
                 s_spabiz_ap_data_status,
                 p_spabiz_ap_data_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_appointment_item_key,
             ap_data_id,
             store_number,
             booked_by_dim_staff_key,
             booked_on_web_flag,
             check_in_date_time,
             check_out_date_time,
             data_type_dim_description_key,
             data_type_id,
             deleted_flag,
             dim_spabiz_block_time_key,
             dim_spabiz_customer_key,
             dim_spabiz_service_key,
             dim_spabiz_store_key,
             edit_date_time,
             end_date_time,
             fact_spabiz_appointment_key,
             fact_spabiz_ticket_item_key,
             note,
             parent_fact_spabiz_appointment_item_key,
             related_fact_spabiz_appointment_item_key,
             resource_block_flag,
             resource_dim_spabiz_staff_key,
             service_dim_spabiz_staff_key,
             standing_appointment_flag,
             start_date_time,
             status_dim_description_key,
             status_id,
             l_spabiz_ap_data_ap_id,
             l_spabiz_ap_data_by_staff_id,
             l_spabiz_ap_data_cust_id,
             l_spabiz_ap_data_parent_id,
             l_spabiz_ap_data_resource_id,
             l_spabiz_ap_data_service_id,
             l_spabiz_ap_data_staff_id,
             l_spabiz_ap_data_ticket_data_id,
             s_spabiz_ap_data_data_type,
             s_spabiz_ap_data_status,
             p_spabiz_ap_data_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment_item','proc_d_fact_spabiz_appointment_item end',@current_dv_batch_id
end
