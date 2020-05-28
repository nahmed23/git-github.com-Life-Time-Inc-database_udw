CREATE PROC [dbo].[proc_d_spabiz_store] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_store)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_store_insert') is not null drop table #p_spabiz_store_insert
create table dbo.#p_spabiz_store_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_store.p_spabiz_store_id,
       p_spabiz_store.bk_hash
  from dbo.p_spabiz_store
 where p_spabiz_store.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_store.dv_batch_id > @max_dv_batch_id
        or p_spabiz_store.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_store.bk_hash,
       p_spabiz_store.bk_hash dim_spabiz_store_key,
       p_spabiz_store.store_number store_number,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_store.delete_date = convert(date, '18991230', 112) then null
            else delete_date
        end deleted_date_time,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.store_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       s_spabiz_store.edit_time edit_date_time,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.open_1 = 1 then 'Y'
            else 'N'
        end open_day_1_sunday_flag,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.open_2 = 1 then 'Y'
            else 'N'
        end open_day_2_monday_flag,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.open_3 = 1 then 'Y'
            else 'N'
        end open_day_3_tuesday_flag,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.open_4 = 1 then 'Y'
            else 'N'
        end open_day_4_wednesday_flag,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.open_5 = 1 then 'Y'
            else 'N'
        end open_day_5_thursday_flag,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.open_6 = 1 then 'Y'
            else 'N'
        end open_day_6_friday_flag,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.open_7 = 1 then 'Y'
            else 'N'
        end open_day_7_saturday_flag,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_store.power_booking = 1 then 'Y'
            else 'N'
        end power_booking_flag,
       s_spabiz_store.quick_id quick_id,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_store.city is null then ''
            else s_spabiz_store.city
        end store_address_city,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_store.country is null then ''
            else s_spabiz_store.country
        end store_address_country,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_store.address_1 is null then ''
            else s_spabiz_store.address_1
        end store_address_line_1,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_store.address_2 is null then ''
            else s_spabiz_store.address_2
        end store_address_line_2,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_store.zip is null then ''
            else s_spabiz_store.zip
        end store_address_postal_code,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_store.store_state is null then ''
            else s_spabiz_store.store_state
        end store_address_state_or_province,
       s_spabiz_store.store_id store_id,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_store.name is null then ''
            else s_spabiz_store.name
        end store_name,
       case when p_spabiz_store.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_store.telephone is null then ''
            else s_spabiz_store.telephone
        end store_phone_number,
       p_spabiz_store.p_spabiz_store_id,
       p_spabiz_store.dv_batch_id,
       p_spabiz_store.dv_load_date_time,
       p_spabiz_store.dv_load_end_date_time
  from dbo.p_spabiz_store
  join #p_spabiz_store_insert
    on p_spabiz_store.bk_hash = #p_spabiz_store_insert.bk_hash
   and p_spabiz_store.p_spabiz_store_id = #p_spabiz_store_insert.p_spabiz_store_id
  join dbo.s_spabiz_store
    on p_spabiz_store.bk_hash = s_spabiz_store.bk_hash
   and p_spabiz_store.s_spabiz_store_id = s_spabiz_store.s_spabiz_store_id
 where p_spabiz_store.store_number not in (1,100,999) OR p_spabiz_store.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_store
   where d_spabiz_store.bk_hash in (select bk_hash from #p_spabiz_store_insert)

  insert dbo.d_spabiz_store(
             bk_hash,
             dim_spabiz_store_key,
             store_number,
             deleted_date_time,
             deleted_flag,
             edit_date_time,
             open_day_1_sunday_flag,
             open_day_2_monday_flag,
             open_day_3_tuesday_flag,
             open_day_4_wednesday_flag,
             open_day_5_thursday_flag,
             open_day_6_friday_flag,
             open_day_7_saturday_flag,
             power_booking_flag,
             quick_id,
             store_address_city,
             store_address_country,
             store_address_line_1,
             store_address_line_2,
             store_address_postal_code,
             store_address_state_or_province,
             store_id,
             store_name,
             store_phone_number,
             p_spabiz_store_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_spabiz_store_key,
         store_number,
         deleted_date_time,
         deleted_flag,
         edit_date_time,
         open_day_1_sunday_flag,
         open_day_2_monday_flag,
         open_day_3_tuesday_flag,
         open_day_4_wednesday_flag,
         open_day_5_thursday_flag,
         open_day_6_friday_flag,
         open_day_7_saturday_flag,
         power_booking_flag,
         quick_id,
         store_address_city,
         store_address_country,
         store_address_line_1,
         store_address_line_2,
         store_address_postal_code,
         store_address_state_or_province,
         store_id,
         store_name,
         store_phone_number,
         p_spabiz_store_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_store)
--Done!
end
