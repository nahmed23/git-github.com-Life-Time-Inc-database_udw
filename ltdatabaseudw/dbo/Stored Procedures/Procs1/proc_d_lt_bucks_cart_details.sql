CREATE PROC [dbo].[proc_d_lt_bucks_cart_details] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_cart_details)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_lt_bucks_cart_details_insert') is not null drop table #p_lt_bucks_cart_details_insert
create table dbo.#p_lt_bucks_cart_details_insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_cart_details.p_lt_bucks_cart_details_id,
       p_lt_bucks_cart_details.bk_hash
  from dbo.p_lt_bucks_cart_details
 where p_lt_bucks_cart_details.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_lt_bucks_cart_details.dv_batch_id > @max_dv_batch_id
        or p_lt_bucks_cart_details.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_cart_details.bk_hash,
       p_lt_bucks_cart_details.bk_hash fact_lt_bucks_cart_details_key,
       p_lt_bucks_cart_details.cdetail_id cdetail_id,
       l_lt_bucks_cart_details.cdetail_cart cart_id,
       s_lt_bucks_cart_details.cdetail_delivery_date delivery_date_time,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_club is null then '-998'
            when l_lt_bucks_cart_details.cdetail_club in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_club as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_poption is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_poption as varchar(500)),'z#@$k%&P'))),2)
        end dim_lt_bucks_product_options_key,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_cart is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_cart as varchar(500)),'z#@$k%&P'))),2)
        end fact_lt_bucks_shopping_cart_key,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_package is null then '-998'
            when l_lt_bucks_cart_details.cdetail_package in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_package as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_package_key,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_transaction_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_transaction_key as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_sales_transaction_key,
       s_lt_bucks_cart_details.cdetail_expiration_date transaction_expiration_date_time,
       p_lt_bucks_cart_details.p_lt_bucks_cart_details_id,
       p_lt_bucks_cart_details.dv_batch_id,
       p_lt_bucks_cart_details.dv_load_date_time,
       p_lt_bucks_cart_details.dv_load_end_date_time
  from dbo.h_lt_bucks_cart_details
  join dbo.p_lt_bucks_cart_details
    on h_lt_bucks_cart_details.bk_hash = p_lt_bucks_cart_details.bk_hash  join #p_lt_bucks_cart_details_insert
    on p_lt_bucks_cart_details.bk_hash = #p_lt_bucks_cart_details_insert.bk_hash
   and p_lt_bucks_cart_details.p_lt_bucks_cart_details_id = #p_lt_bucks_cart_details_insert.p_lt_bucks_cart_details_id
  join dbo.l_lt_bucks_cart_details
    on p_lt_bucks_cart_details.bk_hash = l_lt_bucks_cart_details.bk_hash
   and p_lt_bucks_cart_details.l_lt_bucks_cart_details_id = l_lt_bucks_cart_details.l_lt_bucks_cart_details_id
  join dbo.s_lt_bucks_cart_details
    on p_lt_bucks_cart_details.bk_hash = s_lt_bucks_cart_details.bk_hash
   and p_lt_bucks_cart_details.s_lt_bucks_cart_details_id = s_lt_bucks_cart_details.s_lt_bucks_cart_details_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_lt_bucks_cart_details
   where d_lt_bucks_cart_details.bk_hash in (select bk_hash from #p_lt_bucks_cart_details_insert)

  insert dbo.d_lt_bucks_cart_details(
             bk_hash,
             fact_lt_bucks_cart_details_key,
             cdetail_id,
             cart_id,
             delivery_date_time,
             dim_club_key,
             dim_lt_bucks_product_options_key,
             fact_lt_bucks_shopping_cart_key,
             fact_mms_package_key,
             fact_mms_sales_transaction_key,
             transaction_expiration_date_time,
             p_lt_bucks_cart_details_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_lt_bucks_cart_details_key,
         cdetail_id,
         cart_id,
         delivery_date_time,
         dim_club_key,
         dim_lt_bucks_product_options_key,
         fact_lt_bucks_shopping_cart_key,
         fact_mms_package_key,
         fact_mms_sales_transaction_key,
         transaction_expiration_date_time,
         p_lt_bucks_cart_details_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_cart_details)
--Done!
end
