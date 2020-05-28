CREATE PROC [dbo].[proc_d_fact_spabiz_appointment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment','proc_d_fact_spabiz_appointment start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_fact_spabiz_appointment

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment','#p_spabiz_ap_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_ap_insert') is not null drop table #p_spabiz_ap_insert
create table dbo.#p_spabiz_ap_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_ap.p_spabiz_ap_id,
       p_spabiz_ap.bk_hash,
       row_number() over (order by p_spabiz_ap_id) row_num
  from dbo.p_spabiz_ap
  join #batch_id
    on p_spabiz_ap.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_ap.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_ap.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_ap_insert.row_num,
       p_spabiz_ap.bk_hash fact_spabiz_appointment_key,
       p_spabiz_ap.ap_id appointment_id,
       p_spabiz_ap.store_number store_number,
       case when p_spabiz_ap.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ap.date = convert(date, '18991230', 112) then null
            else s_spabiz_ap.date
        end appointment_date_time,
       case when p_spabiz_ap.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ap.start_time = convert(date, '18991230', 112) then null
            else s_spabiz_ap.start_time
        end appointment_start_date_time,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.book_staff_id is null then '-998'
            when l_spabiz_ap.book_staff_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap.book_staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap.store_number as varchar(500)),'z#@$k%&P'))),2)
        end booked_by_dim_spabiz_staff_key,
       case when p_spabiz_ap.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ap.checkin_time = convert(date, '18991230', 112) then null
            else s_spabiz_ap.checkin_time
        end checkin_date_time,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.confirm_id is null then '-998'
            when l_spabiz_ap.confirm_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap.confirm_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap.store_number as varchar(500)),'z#@$k%&P'))),2)
        end confirmed_by_dim_spabiz_staff_key,
       case when p_spabiz_ap.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ap.book_time = convert(date, '18991230', 112) then null
            else s_spabiz_ap.book_time
        end created_date_time,
       case when s_spabiz_ap.ap_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.cust_id is null then '-998'
            when l_spabiz_ap.cust_id = 0 then '-998'
            when l_spabiz_ap.cust_id = -1 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap.cust_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_customer_key,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.staff_id is null then '-998'
            when l_spabiz_ap.staff_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap.staff_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.store_number is null then '-998'
            when l_spabiz_ap.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_ap.edit_time edit_date_time,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.ticket_id is null then '-998'
            when l_spabiz_ap.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_ap.ticket_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_ap.store_number as varchar(500)),'z#@$k%&P'))),2)
        end fact_spabiz_ticket_key,
       s_spabiz_ap.status_old l_spabiz_ap_previous_status,
       s_spabiz_ap.status l_spabiz_ap_status,
       case when s_spabiz_ap.late = -1 then 'Y'
            else 'N'
        end late_flag,
       case when s_spabiz_ap.memo is null then ''
            else s_spabiz_ap.memo
        end memo,
       case when s_spabiz_ap.no_show = 1 then 'Y'
            else 'N'
        end no_show_flag,
       's_spabiz_ap.status_old_' + convert(varchar,s_spabiz_ap.status_old) previous_status_dim_description_key,
       s_spabiz_ap.status_old previous_status_id,
       case when s_spabiz_ap.standing = 1 then 'Y'
            else 'N'
        end standing_appointment_flag,
       's_spabiz_ap.status_' + convert(varchar,s_spabiz_ap.status) status_dim_description_key,
       s_spabiz_ap.status status_id,
       l_spabiz_ap.book_staff_id l_spabiz_ap_book_staff_id,
       l_spabiz_ap.confirm_id l_spabiz_ap_confirm_id,
       l_spabiz_ap.cust_id l_spabiz_ap_cust_id,
       l_spabiz_ap.staff_id l_spabiz_ap_staff_id,
       l_spabiz_ap.ticket_id l_spabiz_ap_ticket_id,
       p_spabiz_ap.p_spabiz_ap_id,
       p_spabiz_ap.dv_batch_id,
       p_spabiz_ap.dv_load_date_time,
       p_spabiz_ap.dv_load_end_date_time
  from dbo.p_spabiz_ap
  join #p_spabiz_ap_insert
    on p_spabiz_ap.p_spabiz_ap_id = #p_spabiz_ap_insert.p_spabiz_ap_id
  join dbo.l_spabiz_ap
    on p_spabiz_ap.l_spabiz_ap_id = l_spabiz_ap.l_spabiz_ap_id
  join dbo.s_spabiz_ap
    on p_spabiz_ap.s_spabiz_ap_id = s_spabiz_ap.s_spabiz_ap_id
 where l_spabiz_ap.store_number not in (1,100,999) OR p_spabiz_ap.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_fact_spabiz_appointment', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_fact_spabiz_appointment
       where d_fact_spabiz_appointment.fact_spabiz_appointment_key in (select bk_hash from #p_spabiz_ap_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_fact_spabiz_appointment(
                 d_fact_spabiz_appointment_id,
                 fact_spabiz_appointment_key,
                 appointment_id,
                 store_number,
                 appointment_date_time,
                 appointment_start_date_time,
                 booked_by_dim_spabiz_staff_key,
                 checkin_date_time,
                 confirmed_by_dim_spabiz_staff_key,
                 created_date_time,
                 deleted_flag,
                 dim_spabiz_customer_key,
                 dim_spabiz_staff_key,
                 dim_spabiz_store_key,
                 edit_date_time,
                 fact_spabiz_ticket_key,
                 l_spabiz_ap_previous_status,
                 l_spabiz_ap_status,
                 late_flag,
                 memo,
                 no_show_flag,
                 previous_status_dim_description_key,
                 previous_status_id,
                 standing_appointment_flag,
                 status_dim_description_key,
                 status_id,
                 l_spabiz_ap_book_staff_id,
                 l_spabiz_ap_confirm_id,
                 l_spabiz_ap_cust_id,
                 l_spabiz_ap_staff_id,
                 l_spabiz_ap_ticket_id,
                 p_spabiz_ap_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             fact_spabiz_appointment_key,
             appointment_id,
             store_number,
             appointment_date_time,
             appointment_start_date_time,
             booked_by_dim_spabiz_staff_key,
             checkin_date_time,
             confirmed_by_dim_spabiz_staff_key,
             created_date_time,
             deleted_flag,
             dim_spabiz_customer_key,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             edit_date_time,
             fact_spabiz_ticket_key,
             l_spabiz_ap_previous_status,
             l_spabiz_ap_status,
             late_flag,
             memo,
             no_show_flag,
             previous_status_dim_description_key,
             previous_status_id,
             standing_appointment_flag,
             status_dim_description_key,
             status_id,
             l_spabiz_ap_book_staff_id,
             l_spabiz_ap_confirm_id,
             l_spabiz_ap_cust_id,
             l_spabiz_ap_staff_id,
             l_spabiz_ap_ticket_id,
             p_spabiz_ap_id,
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
exec dbo.proc_util_task_status_insert 'proc_d_fact_spabiz_appointment','proc_d_fact_spabiz_appointment end',@current_dv_batch_id
end
