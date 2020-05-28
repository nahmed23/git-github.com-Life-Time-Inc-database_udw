CREATE PROC [dbo].[proc_d_spabiz_po] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_po)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_po_insert') is not null drop table #p_spabiz_po_insert
create table dbo.#p_spabiz_po_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_po.p_spabiz_po_id,
       p_spabiz_po.bk_hash
  from dbo.p_spabiz_po
 where p_spabiz_po.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_po.dv_batch_id > @max_dv_batch_id
        or p_spabiz_po.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_po.bk_hash,
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
    on p_spabiz_po.bk_hash = #p_spabiz_po_insert.bk_hash
   and p_spabiz_po.p_spabiz_po_id = #p_spabiz_po_insert.p_spabiz_po_id
  join dbo.l_spabiz_po
    on p_spabiz_po.bk_hash = l_spabiz_po.bk_hash
   and p_spabiz_po.l_spabiz_po_id = l_spabiz_po.l_spabiz_po_id
  join dbo.s_spabiz_po
    on p_spabiz_po.bk_hash = s_spabiz_po.bk_hash
   and p_spabiz_po.s_spabiz_po_id = s_spabiz_po.s_spabiz_po_id
 where l_spabiz_po.store_number not in (1,100,999) OR p_spabiz_po.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_po
   where d_spabiz_po.bk_hash in (select bk_hash from #p_spabiz_po_insert)

  insert dbo.d_spabiz_po(
             bk_hash,
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
  select bk_hash,
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
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_po)
--Done!
end
