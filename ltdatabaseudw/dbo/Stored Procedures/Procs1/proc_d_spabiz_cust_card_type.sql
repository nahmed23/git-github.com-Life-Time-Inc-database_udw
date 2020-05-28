CREATE PROC [dbo].[proc_d_spabiz_cust_card_type] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_cust_card_type)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_cust_card_type_insert') is not null drop table #p_spabiz_cust_card_type_insert
create table dbo.#p_spabiz_cust_card_type_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_cust_card_type.p_spabiz_cust_card_type_id,
       p_spabiz_cust_card_type.bk_hash
  from dbo.p_spabiz_cust_card_type
 where p_spabiz_cust_card_type.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_cust_card_type.dv_batch_id > @max_dv_batch_id
        or p_spabiz_cust_card_type.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_cust_card_type.bk_hash,
       p_spabiz_cust_card_type.cust_card_type_id cust_card_type_id,
       p_spabiz_cust_card_type.store_number store_number,
       s_spabiz_cust_card_type.counter_id counter_id,
       case when s_spabiz_cust_card_type.cust_card_type_delete = -1 then 'Y' else 'N' end cust_card_type_delete_flag,
       case when p_spabiz_cust_card_type.bk_hash in('-997', '-998', '-999') then p_spabiz_cust_card_type.bk_hash
           when l_spabiz_cust_card_type.discount_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_spabiz_cust_card_type.discount_id as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)   end d_spabiz_cust_card_type_discount_key,
       s_spabiz_cust_card_type.days_good_for days_good_for,
       case when p_spabiz_cust_card_type.bk_hash in('-997', '-998', '-999') then p_spabiz_cust_card_type.bk_hash
           when s_spabiz_cust_card_type.delete_date is null then '-998'
        else convert(varchar,s_spabiz_cust_card_type.delete_date, 112)    end delete_dim_date_key,
       s_spabiz_cust_card_type.disp_color disp_color,
       case when p_spabiz_cust_card_type.bk_hash in ('-997','-998','-999') then p_spabiz_cust_card_type.bk_hash
       when s_spabiz_cust_card_type.edit_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_spabiz_cust_card_type.edit_time,114), 1, 5),':','') end edit_dim_time_key,
       s_spabiz_cust_card_type.initial_price initial_price,
       s_spabiz_cust_card_type.name name,
       s_spabiz_cust_card_type.payment_interval payment_interval,
       s_spabiz_cust_card_type.prod_disc prod_disc,
       s_spabiz_cust_card_type.retail_price retail_price,
       s_spabiz_cust_card_type.service_disc service_disc,
       isnull(h_spabiz_cust_card_type.dv_deleted,0) dv_deleted,
       p_spabiz_cust_card_type.p_spabiz_cust_card_type_id,
       p_spabiz_cust_card_type.dv_batch_id,
       p_spabiz_cust_card_type.dv_load_date_time,
       p_spabiz_cust_card_type.dv_load_end_date_time
  from dbo.h_spabiz_cust_card_type
  join dbo.p_spabiz_cust_card_type
    on h_spabiz_cust_card_type.bk_hash = p_spabiz_cust_card_type.bk_hash
  join #p_spabiz_cust_card_type_insert
    on p_spabiz_cust_card_type.bk_hash = #p_spabiz_cust_card_type_insert.bk_hash
   and p_spabiz_cust_card_type.p_spabiz_cust_card_type_id = #p_spabiz_cust_card_type_insert.p_spabiz_cust_card_type_id
  join dbo.l_spabiz_cust_card_type
    on p_spabiz_cust_card_type.bk_hash = l_spabiz_cust_card_type.bk_hash
   and p_spabiz_cust_card_type.l_spabiz_cust_card_type_id = l_spabiz_cust_card_type.l_spabiz_cust_card_type_id
  join dbo.s_spabiz_cust_card_type
    on p_spabiz_cust_card_type.bk_hash = s_spabiz_cust_card_type.bk_hash
   and p_spabiz_cust_card_type.s_spabiz_cust_card_type_id = s_spabiz_cust_card_type.s_spabiz_cust_card_type_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_cust_card_type
   where d_spabiz_cust_card_type.bk_hash in (select bk_hash from #p_spabiz_cust_card_type_insert)

  insert dbo.d_spabiz_cust_card_type(
             bk_hash,
             cust_card_type_id,
             store_number,
             counter_id,
             cust_card_type_delete_flag,
             d_spabiz_cust_card_type_discount_key,
             days_good_for,
             delete_dim_date_key,
             disp_color,
             edit_dim_time_key,
             initial_price,
             name,
             payment_interval,
             prod_disc,
             retail_price,
             service_disc,
             deleted_flag,
             p_spabiz_cust_card_type_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         cust_card_type_id,
         store_number,
         counter_id,
         cust_card_type_delete_flag,
         d_spabiz_cust_card_type_discount_key,
         days_good_for,
         delete_dim_date_key,
         disp_color,
         edit_dim_time_key,
         initial_price,
         name,
         payment_interval,
         prod_disc,
         retail_price,
         service_disc,
         dv_deleted,
         p_spabiz_cust_card_type_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_cust_card_type)
--Done!
end
