CREATE PROC [dbo].[proc_d_lt_bucks_shopping_cart] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_shopping_cart)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_lt_bucks_shopping_cart_insert') is not null drop table #p_lt_bucks_shopping_cart_insert
create table dbo.#p_lt_bucks_shopping_cart_insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_shopping_cart.p_lt_bucks_shopping_cart_id,
       p_lt_bucks_shopping_cart.bk_hash
  from dbo.p_lt_bucks_shopping_cart
 where p_lt_bucks_shopping_cart.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_lt_bucks_shopping_cart.dv_batch_id > @max_dv_batch_id
        or p_lt_bucks_shopping_cart.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_shopping_cart.bk_hash,
       p_lt_bucks_shopping_cart.bk_hash fact_lt_bucks_shopping_cart_key,
       p_lt_bucks_shopping_cart.cart_id cart_id,
       isnull(s_lt_bucks_shopping_cart.cart_amount,0) bucks_amount,
       case when p_lt_bucks_shopping_cart.bk_hash in ('-997','-998','-999') then p_lt_bucks_shopping_cart.bk_hash
            when l_lt_bucks_shopping_cart.cart_product is null then '-998'
            when l_lt_bucks_shopping_cart.cart_product in (0) then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_shopping_cart.cart_product as varchar(500)),'z#@$k%&P'))),2)
        end dim_lt_bucks_products_key,
       s_lt_bucks_shopping_cart.cart_name name,
       isnull(s_lt_bucks_shopping_cart.cart_point_amount,0) point_amount,
       s_lt_bucks_shopping_cart.cart_sku product_sku,
       s_lt_bucks_shopping_cart.cart_qty quantity,
       s_lt_bucks_shopping_cart.cart_status status,
       s_lt_bucks_shopping_cart.cart_timestamp transaction_date_time,
       p_lt_bucks_shopping_cart.p_lt_bucks_shopping_cart_id,
       p_lt_bucks_shopping_cart.dv_batch_id,
       p_lt_bucks_shopping_cart.dv_load_date_time,
       p_lt_bucks_shopping_cart.dv_load_end_date_time
  from dbo.p_lt_bucks_shopping_cart
  join #p_lt_bucks_shopping_cart_insert
    on p_lt_bucks_shopping_cart.bk_hash = #p_lt_bucks_shopping_cart_insert.bk_hash
   and p_lt_bucks_shopping_cart.p_lt_bucks_shopping_cart_id = #p_lt_bucks_shopping_cart_insert.p_lt_bucks_shopping_cart_id
  join dbo.l_lt_bucks_shopping_cart
    on p_lt_bucks_shopping_cart.bk_hash = l_lt_bucks_shopping_cart.bk_hash
   and p_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id = l_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id
  join dbo.s_lt_bucks_shopping_cart
    on p_lt_bucks_shopping_cart.bk_hash = s_lt_bucks_shopping_cart.bk_hash
   and p_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id = s_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_lt_bucks_shopping_cart
   where d_lt_bucks_shopping_cart.bk_hash in (select bk_hash from #p_lt_bucks_shopping_cart_insert)

  insert dbo.d_lt_bucks_shopping_cart(
             bk_hash,
             fact_lt_bucks_shopping_cart_key,
             cart_id,
             bucks_amount,
             dim_lt_bucks_products_key,
             name,
             point_amount,
             product_sku,
             quantity,
             status,
             transaction_date_time,
             p_lt_bucks_shopping_cart_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_lt_bucks_shopping_cart_key,
         cart_id,
         bucks_amount,
         dim_lt_bucks_products_key,
         name,
         point_amount,
         product_sku,
         quantity,
         status,
         transaction_date_time,
         p_lt_bucks_shopping_cart_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_shopping_cart)
--Done!
end
