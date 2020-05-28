CREATE PROC [dbo].[proc_d_spabiz_ap] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_ap)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_ap_insert') is not null drop table #p_spabiz_ap_insert
create table dbo.#p_spabiz_ap_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_ap.p_spabiz_ap_id,
       p_spabiz_ap.bk_hash
  from dbo.p_spabiz_ap
 where p_spabiz_ap.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_ap.dv_batch_id > @max_dv_batch_id
        or p_spabiz_ap.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_ap.bk_hash,
       p_spabiz_ap.bk_hash fact_spabiz_appointment_key,
       p_spabiz_ap.ap_id appointment_id,
       p_spabiz_ap.store_number store_number,
       convert(datetime,convert(varchar,s_spabiz_ap.date, 110) +' '+convert(varchar,s_spabiz_ap.start_time, 108)) appointment_date_time,
       case when p_spabiz_ap.bk_hash in ('-999','-998','-997') then p_spabiz_ap.bk_hash else convert(varchar,convert(datetime,convert(varchar,s_spabiz_ap.date, 110) +' '+convert(varchar,s_spabiz_ap.start_time, 108)),112) end appointment_dim_date_key,
       case when p_spabiz_ap.bk_hash in ('-999','-998','-997') then p_spabiz_ap.bk_hash else '1' + replace(substring(convert(varchar,convert(datetime,convert(varchar,s_spabiz_ap.date, 110) +' '+convert(varchar,s_spabiz_ap.start_time, 108)),114), 1, 5),':','') end appointment_dim_time_key,
       case when p_spabiz_ap.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_ap.start_time = convert(date, '18991230', 112) then null
            else s_spabiz_ap.start_time
        end appointment_start_date_time,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.book_staff_id is null then '-998'
            when l_spabiz_ap.book_staff_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.book_staff_id as decimal(26,6)) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
        end booked_by_dim_spabiz_staff_key,
       convert(datetime,convert(varchar,s_spabiz_ap.date, 110) +' '+convert(varchar,s_spabiz_ap.checkin_time, 108)) checkin_date_time,
       case when p_spabiz_ap.bk_hash in ('-999','-998','-997') then p_spabiz_ap.bk_hash else convert(varchar,convert(datetime,convert(varchar,s_spabiz_ap.date, 110) +' '+convert(varchar,s_spabiz_ap.checkin_time, 108)),112) end checkin_dim_date_key,
       case when p_spabiz_ap.bk_hash in ('-999','-998','-997') then p_spabiz_ap.bk_hash else '1' + replace(substring(convert(varchar,convert(datetime,convert(varchar,s_spabiz_ap.date, 110) +' '+convert(varchar,s_spabiz_ap.checkin_time, 108)),114), 1, 5),':','') end checkin_dim_time_key,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.confirm_id is null then '-998'
            when l_spabiz_ap.confirm_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.confirm_id as decimal(26,6)) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
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
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.cust_id as decimal(26,6)) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_customer_key,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.staff_id is null then '-998'
            when l_spabiz_ap.staff_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.staff_id as decimal(26,6)) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_staff_key,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.store_number is null then '-998'
            when l_spabiz_ap.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_ap.edit_time edit_date_time,
       case
            when p_spabiz_ap.bk_hash in ('-997','-998','-999') then p_spabiz_ap.bk_hash
            when l_spabiz_ap.ticket_id is null then '-998'
            when l_spabiz_ap.ticket_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.ticket_id as decimal(26,6)) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(l_spabiz_ap.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
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
       h_spabiz_ap.dv_deleted,
       p_spabiz_ap.p_spabiz_ap_id,
       p_spabiz_ap.dv_batch_id,
       p_spabiz_ap.dv_load_date_time,
       p_spabiz_ap.dv_load_end_date_time
  from dbo.h_spabiz_ap
  join dbo.p_spabiz_ap
    on h_spabiz_ap.bk_hash = p_spabiz_ap.bk_hash
  join #p_spabiz_ap_insert
    on p_spabiz_ap.bk_hash = #p_spabiz_ap_insert.bk_hash
   and p_spabiz_ap.p_spabiz_ap_id = #p_spabiz_ap_insert.p_spabiz_ap_id
  join dbo.l_spabiz_ap
    on p_spabiz_ap.bk_hash = l_spabiz_ap.bk_hash
   and p_spabiz_ap.l_spabiz_ap_id = l_spabiz_ap.l_spabiz_ap_id
  join dbo.s_spabiz_ap
    on p_spabiz_ap.bk_hash = s_spabiz_ap.bk_hash
   and p_spabiz_ap.s_spabiz_ap_id = s_spabiz_ap.s_spabiz_ap_id
 where l_spabiz_ap.store_number not in (1,100,999) OR p_spabiz_ap.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_ap
   where d_spabiz_ap.bk_hash in (select bk_hash from #p_spabiz_ap_insert)

  insert dbo.d_spabiz_ap(
             bk_hash,
             fact_spabiz_appointment_key,
             appointment_id,
             store_number,
             appointment_date_time,
             appointment_dim_date_key,
             appointment_dim_time_key,
             appointment_start_date_time,
             booked_by_dim_spabiz_staff_key,
             checkin_date_time,
             checkin_dim_date_key,
             checkin_dim_time_key,
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
  select bk_hash,
         fact_spabiz_appointment_key,
         appointment_id,
         store_number,
         appointment_date_time,
         appointment_dim_date_key,
         appointment_dim_time_key,
         appointment_start_date_time,
         booked_by_dim_spabiz_staff_key,
         checkin_date_time,
         checkin_dim_date_key,
         checkin_dim_time_key,
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
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_ap)
--Done!
end
