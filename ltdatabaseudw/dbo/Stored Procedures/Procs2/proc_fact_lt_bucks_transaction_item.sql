CREATE PROC [dbo].[proc_fact_lt_bucks_transaction_item] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

/*
Notes:
    Each p_lt_bucks_shopping_cart record is associated with exactly one p_lt_bucks_cart_details record via l_lt_bucks_cart_details.cdetail_cart
    Shopping cart records are limited to having cart_status = 3, which means the user checked out.  Other status are for removed or abandoned items
*/

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @dv_batch_id as current_dv_batch_id
  from dbo.fact_lt_bucks_transaction_item

if object_id('tempdb..#cart_id') is not null drop table #cart_id
create table dbo.#cart_id with(distribution=hash(cart_id), location=user_db, heap) as
select cart_id,
       rank() over (order by cart_id) r
from (select l_lt_bucks_cart_details.cdetail_cart cart_id
      from p_lt_bucks_cart_details
      join l_lt_bucks_cart_details
        on p_lt_bucks_cart_details.l_lt_bucks_cart_details_id = l_lt_bucks_cart_details.l_lt_bucks_cart_details_id
      join #dv_batch_id
        on p_lt_bucks_cart_details.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_lt_bucks_cart_details.dv_batch_id = #dv_batch_id.current_dv_batch_id
     where p_lt_bucks_cart_details.dv_load_end_date_time = 'Dec 31, 9999'
      union
      select p_lt_bucks_shopping_cart.cart_id
      from p_lt_bucks_shopping_cart 
      join #dv_batch_id
        on p_lt_bucks_shopping_cart.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or p_lt_bucks_shopping_cart.dv_batch_id = #dv_batch_id.current_dv_batch_id
      where p_lt_bucks_shopping_cart.dv_load_end_date_time = 'Dec 31, 9999'
) x

--d_fact_lt_bucks_cart_details
if object_id('tempdb..#p_lt_bucks_cart_details') is not null drop table #p_lt_bucks_cart_details
create table dbo.#p_lt_bucks_cart_details with(distribution=hash(fact_lt_bucks_shopping_cart_key), location=user_db, heap) as
select p_lt_bucks_cart_details.cdetail_id cart_detail_id,
       l_lt_bucks_cart_details.cdetail_cart cart_id,
       s_lt_bucks_cart_details.cdetail_delivery_date delivery_date_time,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_club is null then '-998'
            when l_lt_bucks_cart_details.cdetail_club in (0) then '-998'
            --util_bk_hash[l_lt_bucks_cart_details.cdetail_club,h_mms_club.club_id]
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_club as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_poption is null then '-998'
            --util_bk_hash[l_lt_bucks_cart_details.cdetail_poption,h_lt_bucks_product_options.poption_id]
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_poption as varchar(500)),'z#@$k%&P'))),2)
        end dim_lt_bucks_product_option_key,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_cart is null then '-998'
            --util_bk_hash[l_lt_bucks_cart_details.cdetail_cart,h_lt_bucks_shopping_cart.cart_id]
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_cart as varchar(500)),'z#@$k%&P'))),2)
        end fact_lt_bucks_shopping_cart_key,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_package is null then '-998'
            when l_lt_bucks_cart_details.cdetail_package in (0) then '-998'
            --util_bk_hash[l_lt_bucks_cart_details.cdetail_package,h_spabiz_package.package_id]
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_package as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_package_key,
       case when p_lt_bucks_cart_details.bk_hash in ('-997','-998','-999') then p_lt_bucks_cart_details.bk_hash
            when l_lt_bucks_cart_details.cdetail_transaction_key is null then '-998'
            --util_bk_hash[l_lt_bucks_cart_details.cdetail_transaction_key,h_mms_club.club_id]
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_cart_details.cdetail_transaction_key as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_sales_transaction_key,
       s_lt_bucks_cart_details.cdetail_expiration_date transaction_expiration_date_time,
       p_lt_bucks_cart_details.p_lt_bucks_cart_details_id,
       p_lt_bucks_cart_details.dv_batch_id,
       p_lt_bucks_cart_details.dv_load_date_time,
       p_lt_bucks_cart_details.dv_load_end_date_time
  from dbo.p_lt_bucks_cart_details
  join dbo.l_lt_bucks_cart_details
    on p_lt_bucks_cart_details.l_lt_bucks_cart_details_id = l_lt_bucks_cart_details.l_lt_bucks_cart_details_id
  join dbo.s_lt_bucks_cart_details
    on p_lt_bucks_cart_details.s_lt_bucks_cart_details_id = s_lt_bucks_cart_details.s_lt_bucks_cart_details_id
  join #cart_id
    on l_lt_bucks_cart_details.cdetail_cart = #cart_id.cart_id
 where p_lt_bucks_cart_details.dv_load_end_date_time = 'Dec 31, 9999'

if object_id('tempdb..#p_lt_bucks_shopping_cart') is not null drop table #p_lt_bucks_shopping_cart
create table dbo.#p_lt_bucks_shopping_cart with(distribution=hash(fact_lt_bucks_shopping_cart_key), location=user_db, heap) as
select p_lt_bucks_shopping_cart.bk_hash fact_lt_bucks_shopping_cart_key,
       p_lt_bucks_shopping_cart.cart_id,
       l_lt_bucks_shopping_cart.cart_session session_id,
       s_lt_bucks_shopping_cart.cart_amount bucks_amount,
       s_lt_bucks_shopping_cart.cart_sku product_sku,
       s_lt_bucks_shopping_cart.cart_qty quantity,
       s_lt_bucks_shopping_cart.cart_name,
       s_lt_bucks_shopping_cart.cart_timestamp transaction_date_time,
       p_lt_bucks_shopping_cart.p_lt_bucks_shopping_cart_id,
       p_lt_bucks_shopping_cart.dv_batch_id,
       p_lt_bucks_shopping_cart.dv_load_date_time,
       p_lt_bucks_shopping_cart.dv_load_end_date_time,
       #cart_id.r
from p_lt_bucks_shopping_cart 
join s_lt_bucks_shopping_cart
  on p_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id = s_lt_bucks_shopping_cart.s_lt_bucks_shopping_cart_id
join l_lt_bucks_shopping_cart
  on p_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id = l_lt_bucks_shopping_cart.l_lt_bucks_shopping_cart_id
join #cart_id
  on p_lt_bucks_shopping_cart.cart_id = #cart_id.cart_id
where p_lt_bucks_shopping_cart.dv_load_end_date_time = 'Dec 31, 9999'
  and s_lt_bucks_shopping_cart.cart_status = 3 --User checked out with all items in the cart.  Other status are for removed or abandoned items

--delete and re-insert
declare @start int, @end int
set @start = 1
set @end = (select max(r) from #cart_id)

while @start <= @end
begin
-- do as a single transaction
--   delete records from the fact table that exist
--   insert records from records from current and missing batches
    begin tran
      delete dbo.fact_lt_bucks_transaction_item
       where cart_id in (select cart_id from #cart_id where r >= @start and r < @start+60000000)

      insert dbo.fact_lt_bucks_transaction_item(
                fact_lt_bucks_transaction_item_key,
                cart_id,
                bucks_amount,
                cart_name,
                cart_detail_id,
                delivery_date_time,
                dim_club_key,
                dim_lt_bucks_product_option_key,
                fact_mms_package_key,
                fact_mms_sales_transaction_key,
                product_sku,
                quantity,
                session_id,
                transaction_date_time,
                transaction_expiration_date_time,
                p_lt_bucks_shopping_cart_id,
                p_lt_bucks_cart_details_id,
                dv_load_date_time,
                dv_load_end_date_time,
                dv_batch_id,
                dv_inserted_date_time,
                dv_insert_user)
      select #p_lt_bucks_shopping_cart.fact_lt_bucks_shopping_cart_key,
             #p_lt_bucks_shopping_cart.cart_id,
             isnull(#p_lt_bucks_shopping_cart.bucks_amount,0) bucks_amount,
             isnull(#p_lt_bucks_shopping_cart.cart_name,'') cart_name,
             #p_lt_bucks_cart_details.cart_detail_id,
             #p_lt_bucks_cart_details.delivery_date_time,
             #p_lt_bucks_cart_details.dim_club_key,
             #p_lt_bucks_cart_details.dim_lt_bucks_product_option_key,
             #p_lt_bucks_cart_details.fact_mms_package_key,
             #p_lt_bucks_cart_details.fact_mms_sales_transaction_key,
             isnull(#p_lt_bucks_shopping_cart.product_sku,'') product_sku,
             isnull(#p_lt_bucks_shopping_cart.quantity,0) quantity,
             #p_lt_bucks_shopping_cart.session_id,
             #p_lt_bucks_shopping_cart.transaction_date_time,
             #p_lt_bucks_cart_details.transaction_expiration_date_time,
             #p_lt_bucks_shopping_cart.p_lt_bucks_shopping_cart_id,
             #p_lt_bucks_cart_details.p_lt_bucks_cart_details_id,
             case when #p_lt_bucks_shopping_cart.dv_load_date_time >= #p_lt_bucks_cart_details.dv_load_date_time then #p_lt_bucks_shopping_cart.dv_load_date_time
                  else #p_lt_bucks_cart_details.dv_load_date_time end dv_load_date_time,
             case when #p_lt_bucks_shopping_cart.dv_load_end_date_time >= #p_lt_bucks_cart_details.dv_load_end_date_time then #p_lt_bucks_shopping_cart.dv_load_end_date_time
                  else #p_lt_bucks_cart_details.dv_load_end_date_time end dv_load_end_date_time,
             case when #p_lt_bucks_shopping_cart.dv_batch_id >= #p_lt_bucks_cart_details.dv_batch_id then #p_lt_bucks_shopping_cart.dv_batch_id
                  else #p_lt_bucks_cart_details.dv_batch_id end dv_batch_id,
             getdate(),
             suser_sname()
        from #p_lt_bucks_shopping_cart
        join #p_lt_bucks_cart_details
          on #p_lt_bucks_cart_details.fact_lt_bucks_shopping_cart_key = #p_lt_bucks_shopping_cart.fact_lt_bucks_shopping_cart_key
       where #p_lt_bucks_shopping_cart.r >= @start
         and #p_lt_bucks_shopping_cart.r < @start+60000000
    commit tran

    set @start = @start+60000000
end
end
