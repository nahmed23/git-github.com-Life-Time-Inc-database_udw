CREATE PROC [dbo].[proc_d_dim_spabiz_store] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_store','proc_d_dim_spabiz_store start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_store','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_dim_spabiz_store

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_store','#p_spabiz_store_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_store_insert') is not null drop table #p_spabiz_store_insert
create table dbo.#p_spabiz_store_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_store.p_spabiz_store_id,
       p_spabiz_store.bk_hash,
       row_number() over (order by p_spabiz_store_id) row_num
  from dbo.p_spabiz_store
  join #batch_id
    on p_spabiz_store.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_store.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_store.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_store','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_store_insert.row_num,
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
    on p_spabiz_store.p_spabiz_store_id = #p_spabiz_store_insert.p_spabiz_store_id
  join dbo.s_spabiz_store
    on p_spabiz_store.s_spabiz_store_id = s_spabiz_store.s_spabiz_store_id
 where p_spabiz_store.store_number not in (1,100,999) OR p_spabiz_store.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_dim_spabiz_store', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_store',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_dim_spabiz_store
       where d_dim_spabiz_store.dim_spabiz_store_key in (select bk_hash from #p_spabiz_store_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_dim_spabiz_store(
                 d_dim_spabiz_store_id,
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
      select @start_p_id + row_num,
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
       where row_num >= @start
         and row_num < @start+1000000
    commit tran

    set @start = @start+1000000
end

--Done!
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_store','proc_d_dim_spabiz_store end',@current_dv_batch_id
end
