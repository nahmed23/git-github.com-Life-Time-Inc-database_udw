CREATE PROC [dbo].[proc_d_exerp_master_product] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_master_product)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_master_product_insert') is not null drop table #p_exerp_master_product_insert
create table dbo.#p_exerp_master_product_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_master_product.p_exerp_master_product_id,
       p_exerp_master_product.bk_hash
  from dbo.p_exerp_master_product
 where p_exerp_master_product.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_master_product.dv_batch_id > @max_dv_batch_id
        or p_exerp_master_product.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_master_product.bk_hash,
       p_exerp_master_product.master_product_id master_product_id,
       s_exerp_master_product.global_id master_product_global_id,
       s_exerp_master_product.name master_product_name,
       s_exerp_master_product.state master_product_state,
       isnull(h_exerp_master_product.dv_deleted,0) dv_deleted,
       p_exerp_master_product.p_exerp_master_product_id,
       p_exerp_master_product.dv_batch_id,
       p_exerp_master_product.dv_load_date_time,
       p_exerp_master_product.dv_load_end_date_time
  from dbo.h_exerp_master_product
  join dbo.p_exerp_master_product
    on h_exerp_master_product.bk_hash = p_exerp_master_product.bk_hash
  join #p_exerp_master_product_insert
    on p_exerp_master_product.bk_hash = #p_exerp_master_product_insert.bk_hash
   and p_exerp_master_product.p_exerp_master_product_id = #p_exerp_master_product_insert.p_exerp_master_product_id
  join dbo.s_exerp_master_product
    on p_exerp_master_product.bk_hash = s_exerp_master_product.bk_hash
   and p_exerp_master_product.s_exerp_master_product_id = s_exerp_master_product.s_exerp_master_product_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_master_product
   where d_exerp_master_product.bk_hash in (select bk_hash from #p_exerp_master_product_insert)

  insert dbo.d_exerp_master_product(
             bk_hash,
             master_product_id,
             master_product_global_id,
             master_product_name,
             master_product_state,
             deleted_flag,
             p_exerp_master_product_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         master_product_id,
         master_product_global_id,
         master_product_name,
         master_product_state,
         dv_deleted,
         p_exerp_master_product_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_master_product)
--Done!
end
