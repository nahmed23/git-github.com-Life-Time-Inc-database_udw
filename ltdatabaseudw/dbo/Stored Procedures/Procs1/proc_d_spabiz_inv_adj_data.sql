CREATE PROC [dbo].[proc_d_spabiz_inv_adj_data] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_inv_adj_data)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_inv_adj_data_insert') is not null drop table #p_spabiz_inv_adj_data_insert
create table dbo.#p_spabiz_inv_adj_data_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_inv_adj_data.p_spabiz_inv_adj_data_id,
       p_spabiz_inv_adj_data.bk_hash
  from dbo.p_spabiz_inv_adj_data
 where p_spabiz_inv_adj_data.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_inv_adj_data.dv_batch_id > @max_dv_batch_id
        or p_spabiz_inv_adj_data.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_inv_adj_data.bk_hash,
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
    on p_spabiz_inv_adj_data.bk_hash = #p_spabiz_inv_adj_data_insert.bk_hash
   and p_spabiz_inv_adj_data.p_spabiz_inv_adj_data_id = #p_spabiz_inv_adj_data_insert.p_spabiz_inv_adj_data_id
  join dbo.l_spabiz_inv_adj_data
    on p_spabiz_inv_adj_data.bk_hash = l_spabiz_inv_adj_data.bk_hash
   and p_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id = l_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id
  join dbo.s_spabiz_inv_adj_data
    on p_spabiz_inv_adj_data.bk_hash = s_spabiz_inv_adj_data.bk_hash
   and p_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id = s_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id
 where l_spabiz_inv_adj_data.store_number not in (1,100,999) OR p_spabiz_inv_adj_data.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_inv_adj_data
   where d_spabiz_inv_adj_data.bk_hash in (select bk_hash from #p_spabiz_inv_adj_data_insert)

  insert dbo.d_spabiz_inv_adj_data(
             bk_hash,
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
  select bk_hash,
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
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_inv_adj_data)
--Done!
end
