CREATE PROC [dbo].[proc_d_boss_asi_prod_kit] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_prod_kit)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_prod_kit_insert') is not null drop table #p_boss_asi_prod_kit_insert
create table dbo.#p_boss_asi_prod_kit_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_prod_kit.p_boss_asi_prod_kit_id,
       p_boss_asi_prod_kit.bk_hash
  from dbo.p_boss_asi_prod_kit
 where p_boss_asi_prod_kit.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_prod_kit.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_prod_kit.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_prod_kit.bk_hash,
       p_boss_asi_prod_kit.parent_upc parent_upc,
       p_boss_asi_prod_kit.child_upc child_upc,
       case when p_boss_asi_prod_kit.bk_hash in ('-997', '-998', '-999') then p_boss_asi_prod_kit.bk_hash
           when p_boss_asi_prod_kit.child_upc is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ltrim(rtrim(p_boss_asi_prod_kit.child_upc)) as char(15)),'z#@$k%&P'))),2) end child_dim_boss_product_key,
       s_boss_asi_prod_kit.duration duration,
       case when p_boss_asi_prod_kit.bk_hash in ('-997', '-998', '-999') then p_boss_asi_prod_kit.bk_hash
           when p_boss_asi_prod_kit.parent_upc is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ltrim(rtrim(p_boss_asi_prod_kit.parent_upc)) as char(15)),'z#@$k%&P'))),2) end parent_dim_boss_product_key,
       s_boss_asi_prod_kit.sort_order sort_order,
       isnull(h_boss_asi_prod_kit.dv_deleted,0) dv_deleted,
       p_boss_asi_prod_kit.p_boss_asi_prod_kit_id,
       p_boss_asi_prod_kit.dv_batch_id,
       p_boss_asi_prod_kit.dv_load_date_time,
       p_boss_asi_prod_kit.dv_load_end_date_time
  from dbo.h_boss_asi_prod_kit
  join dbo.p_boss_asi_prod_kit
    on h_boss_asi_prod_kit.bk_hash = p_boss_asi_prod_kit.bk_hash
  join #p_boss_asi_prod_kit_insert
    on p_boss_asi_prod_kit.bk_hash = #p_boss_asi_prod_kit_insert.bk_hash
   and p_boss_asi_prod_kit.p_boss_asi_prod_kit_id = #p_boss_asi_prod_kit_insert.p_boss_asi_prod_kit_id
  join dbo.s_boss_asi_prod_kit
    on p_boss_asi_prod_kit.bk_hash = s_boss_asi_prod_kit.bk_hash
   and p_boss_asi_prod_kit.s_boss_asi_prod_kit_id = s_boss_asi_prod_kit.s_boss_asi_prod_kit_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_prod_kit
   where d_boss_asi_prod_kit.bk_hash in (select bk_hash from #p_boss_asi_prod_kit_insert)

  insert dbo.d_boss_asi_prod_kit(
             bk_hash,
             parent_upc,
             child_upc,
             child_dim_boss_product_key,
             duration,
             parent_dim_boss_product_key,
             sort_order,
             deleted_flag,
             p_boss_asi_prod_kit_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         parent_upc,
         child_upc,
         child_dim_boss_product_key,
         duration,
         parent_dim_boss_product_key,
         sort_order,
         dv_deleted,
         p_boss_asi_prod_kit_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_prod_kit)
--Done!
end
