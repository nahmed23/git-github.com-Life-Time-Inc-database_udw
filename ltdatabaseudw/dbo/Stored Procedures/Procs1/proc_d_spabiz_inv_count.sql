CREATE PROC [dbo].[proc_d_spabiz_inv_count] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_inv_count)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_inv_count_insert') is not null drop table #p_spabiz_inv_count_insert
create table dbo.#p_spabiz_inv_count_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_inv_count.p_spabiz_inv_count_id,
       p_spabiz_inv_count.bk_hash
  from dbo.p_spabiz_inv_count
 where p_spabiz_inv_count.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_inv_count.dv_batch_id > @max_dv_batch_id
        or p_spabiz_inv_count.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_inv_count.bk_hash,
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
    on p_spabiz_inv_count.bk_hash = #p_spabiz_inv_count_insert.bk_hash
   and p_spabiz_inv_count.p_spabiz_inv_count_id = #p_spabiz_inv_count_insert.p_spabiz_inv_count_id
  join dbo.l_spabiz_inv_count
    on p_spabiz_inv_count.bk_hash = l_spabiz_inv_count.bk_hash
   and p_spabiz_inv_count.l_spabiz_inv_count_id = l_spabiz_inv_count.l_spabiz_inv_count_id
  join dbo.s_spabiz_inv_count
    on p_spabiz_inv_count.bk_hash = s_spabiz_inv_count.bk_hash
   and p_spabiz_inv_count.s_spabiz_inv_count_id = s_spabiz_inv_count.s_spabiz_inv_count_id
 where l_spabiz_inv_count.store_number not in (1,100,999) OR p_spabiz_inv_count.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_inv_count
   where d_spabiz_inv_count.bk_hash in (select bk_hash from #p_spabiz_inv_count_insert)

  insert dbo.d_spabiz_inv_count(
             bk_hash,
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
  select bk_hash,
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
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_inv_count)
--Done!
end
