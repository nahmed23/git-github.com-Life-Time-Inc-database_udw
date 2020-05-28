CREATE PROC [dbo].[proc_d_exerp_product] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_product)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_product_insert') is not null drop table #p_exerp_product_insert
create table dbo.#p_exerp_product_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_product.p_exerp_product_id,
       p_exerp_product.bk_hash
  from dbo.p_exerp_product
 where p_exerp_product.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_product.dv_batch_id > @max_dv_batch_id
        or p_exerp_product.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_product.bk_hash,
       p_exerp_product.product_id product_id,
       case when p_exerp_product.bk_hash in('-997', '-998', '-999') then p_exerp_product.bk_hash
           when l_exerp_product.product_group_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_product.product_group_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_product_group_bk_hash,
       case when p_exerp_product.bk_hash in('-997', '-998', '-999') then p_exerp_product.bk_hash     
        when l_exerp_product.center_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_product.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_exerp_product.bk_hash in('-997', '-998', '-999') then p_exerp_product.bk_hash
           when l_exerp_product.external_id  is null then '-998'
       	when l_exerp_product.external_id  like 'F' then '-998'
       	when l_exerp_product.external_id  like '%M' then '-998'
       	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(substring(
       substring(external_id,patindex('%[_]%',external_id)+1,500),
       patindex('%[_]%',substring(external_id,patindex('%[_]%',external_id)+1,500))+1,500) as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_product_key,
       s_exerp_product.ets ets,
       l_exerp_product.external_id external_id,
       case when p_exerp_product.bk_hash in('-997', '-998', '-999') then p_exerp_product.bk_hash    
        when l_exerp_product.master_product_id is null then '-998'  
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_product.master_product_id as int) as varchar(500)),'z#@$k%&P'))),2)   end master_dim_exerp_product_key,
       case when s_exerp_product.blocked = 1 then 'Y'        else 'N'  end product_blocked_flag,
       s_exerp_product.cost_price product_cost_price,
       s_exerp_product.flat_rate_commission product_flat_rate_commission,
       l_exerp_product.product_group_id product_group_id,
       s_exerp_product.included_member_count product_included_member_count,
       s_exerp_product.minimum_price product_minimum_price,
       s_exerp_product.name product_name,
       s_exerp_product.period_commission product_period_commission,
       s_exerp_product.sales_commission product_sales_commission,
       s_exerp_product.sales_price product_sales_price,
       s_exerp_product.sales_units product_sales_units,
       s_exerp_product.type product_type,
       isnull(h_exerp_product.dv_deleted,0) dv_deleted,
       p_exerp_product.p_exerp_product_id,
       p_exerp_product.dv_batch_id,
       p_exerp_product.dv_load_date_time,
       p_exerp_product.dv_load_end_date_time
  from dbo.h_exerp_product
  join dbo.p_exerp_product
    on h_exerp_product.bk_hash = p_exerp_product.bk_hash
  join #p_exerp_product_insert
    on p_exerp_product.bk_hash = #p_exerp_product_insert.bk_hash
   and p_exerp_product.p_exerp_product_id = #p_exerp_product_insert.p_exerp_product_id
  join dbo.l_exerp_product
    on p_exerp_product.bk_hash = l_exerp_product.bk_hash
   and p_exerp_product.l_exerp_product_id = l_exerp_product.l_exerp_product_id
  join dbo.s_exerp_product
    on p_exerp_product.bk_hash = s_exerp_product.bk_hash
   and p_exerp_product.s_exerp_product_id = s_exerp_product.s_exerp_product_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_product
   where d_exerp_product.bk_hash in (select bk_hash from #p_exerp_product_insert)

  insert dbo.d_exerp_product(
             bk_hash,
             product_id,
             d_exerp_product_group_bk_hash,
             dim_club_key,
             dim_mms_product_key,
             ets,
             external_id,
             master_dim_exerp_product_key,
             product_blocked_flag,
             product_cost_price,
             product_flat_rate_commission,
             product_group_id,
             product_included_member_count,
             product_minimum_price,
             product_name,
             product_period_commission,
             product_sales_commission,
             product_sales_price,
             product_sales_units,
             product_type,
             deleted_flag,
             p_exerp_product_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         product_id,
         d_exerp_product_group_bk_hash,
         dim_club_key,
         dim_mms_product_key,
         ets,
         external_id,
         master_dim_exerp_product_key,
         product_blocked_flag,
         product_cost_price,
         product_flat_rate_commission,
         product_group_id,
         product_included_member_count,
         product_minimum_price,
         product_name,
         product_period_commission,
         product_sales_commission,
         product_sales_price,
         product_sales_units,
         product_type,
         dv_deleted,
         p_exerp_product_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_product)
--Done!
end
