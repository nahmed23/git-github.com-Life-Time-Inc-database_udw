CREATE PROC [dbo].[proc_d_spabiz_payment_types] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_payment_types)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_payment_types_insert') is not null drop table #p_spabiz_payment_types_insert
create table dbo.#p_spabiz_payment_types_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_payment_types.p_spabiz_payment_types_id,
       p_spabiz_payment_types.bk_hash
  from dbo.p_spabiz_payment_types
 where p_spabiz_payment_types.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_payment_types.dv_batch_id > @max_dv_batch_id
        or p_spabiz_payment_types.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_payment_types.bk_hash,
       p_spabiz_payment_types.bk_hash dim_spabiz_payment_type_key,
       p_spabiz_payment_types.payment_types_id payment_type_id,
       p_spabiz_payment_types.store_number store_number,
       case when s_spabiz_payment_types.depositable = 1 then 'Y'
            else 'N'
        end bank_depositable_flag,
       case when p_spabiz_payment_types.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_payment_types.date_time = convert(date, '18991230', 112) then null
            else date_time
        end created_date_time,
       case when p_spabiz_payment_types.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_payment_types.delete_date = convert(date, '18991230', 112) then null
            else delete_date
        end deleted_date_time,
       case when s_spabiz_payment_types.payment_types_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_payment_types.bk_hash in ('-997','-998','-999') then p_spabiz_payment_types.bk_hash
            when l_spabiz_payment_types.store_number is null then '-998'
            when l_spabiz_payment_types.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_payment_types.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       s_spabiz_payment_types.edit_time edit_date_time,
       case when s_spabiz_payment_types.enabled = 1 then 'Y'
            else 'N'
        end enabled_flag,
       case when s_spabiz_payment_types.name is null then ''
            else s_spabiz_payment_types.name
        end name,
       case when s_spabiz_payment_types.non_revenue = 0 then 'N'
            else 'Y'
        end non_revenue_flag,
       case when s_spabiz_payment_types.pop_drawer = 1 then 'Y'
            else 'N'
        end pop_drawer_flag,
       l_spabiz_payment_types.order_num sort_order,
       case when s_spabiz_payment_types.verify = 1 then 'Y'
            else 'N'
        end verify_credit_card_flag,
       p_spabiz_payment_types.p_spabiz_payment_types_id,
       p_spabiz_payment_types.dv_batch_id,
       p_spabiz_payment_types.dv_load_date_time,
       p_spabiz_payment_types.dv_load_end_date_time
  from dbo.p_spabiz_payment_types
  join #p_spabiz_payment_types_insert
    on p_spabiz_payment_types.bk_hash = #p_spabiz_payment_types_insert.bk_hash
   and p_spabiz_payment_types.p_spabiz_payment_types_id = #p_spabiz_payment_types_insert.p_spabiz_payment_types_id
  join dbo.l_spabiz_payment_types
    on p_spabiz_payment_types.bk_hash = l_spabiz_payment_types.bk_hash
   and p_spabiz_payment_types.l_spabiz_payment_types_id = l_spabiz_payment_types.l_spabiz_payment_types_id
  join dbo.s_spabiz_payment_types
    on p_spabiz_payment_types.bk_hash = s_spabiz_payment_types.bk_hash
   and p_spabiz_payment_types.s_spabiz_payment_types_id = s_spabiz_payment_types.s_spabiz_payment_types_id
 where l_spabiz_payment_types.store_number not in (1,100,999) OR p_spabiz_payment_types.bk_hash in ('-999','-998','-997')

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_payment_types
   where d_spabiz_payment_types.bk_hash in (select bk_hash from #p_spabiz_payment_types_insert)

  insert dbo.d_spabiz_payment_types(
             bk_hash,
             dim_spabiz_payment_type_key,
             payment_type_id,
             store_number,
             bank_depositable_flag,
             created_date_time,
             deleted_date_time,
             deleted_flag,
             dim_spabiz_store_key,
             edit_date_time,
             enabled_flag,
             name,
             non_revenue_flag,
             pop_drawer_flag,
             sort_order,
             verify_credit_card_flag,
             p_spabiz_payment_types_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_spabiz_payment_type_key,
         payment_type_id,
         store_number,
         bank_depositable_flag,
         created_date_time,
         deleted_date_time,
         deleted_flag,
         dim_spabiz_store_key,
         edit_date_time,
         enabled_flag,
         name,
         non_revenue_flag,
         pop_drawer_flag,
         sort_order,
         verify_credit_card_flag,
         p_spabiz_payment_types_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_payment_types)
--Done!
end
