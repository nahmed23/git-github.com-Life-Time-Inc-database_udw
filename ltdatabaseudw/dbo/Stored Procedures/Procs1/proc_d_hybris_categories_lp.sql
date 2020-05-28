CREATE PROC [dbo].[proc_d_hybris_categories_lp] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_hybris_categories_lp)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_hybris_categories_lp_insert') is not null drop table #p_hybris_categories_lp_insert
create table dbo.#p_hybris_categories_lp_insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_categories_lp.p_hybris_categories_lp_id,
       p_hybris_categories_lp.bk_hash
  from dbo.p_hybris_categories_lp
 where p_hybris_categories_lp.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_hybris_categories_lp.dv_batch_id > @max_dv_batch_id
        or p_hybris_categories_lp.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_categories_lp.bk_hash,
       p_hybris_categories_lp.bk_hash d_hybris_categories_lp_key,
       p_hybris_categories_lp.item_pk item_pk,
       p_hybris_categories_lp.lang_pk lang_pk,
       l_hybris_categories_lp.item_type_pk item_type_pk,
       s_hybris_categories_lp.p_description p_description,
       s_hybris_categories_lp.p_name p_name,
       p_hybris_categories_lp.p_hybris_categories_lp_id,
       p_hybris_categories_lp.dv_batch_id,
       p_hybris_categories_lp.dv_load_date_time,
       p_hybris_categories_lp.dv_load_end_date_time
  from dbo.h_hybris_categories_lp
  join dbo.p_hybris_categories_lp
    on h_hybris_categories_lp.bk_hash = p_hybris_categories_lp.bk_hash  join #p_hybris_categories_lp_insert
    on p_hybris_categories_lp.bk_hash = #p_hybris_categories_lp_insert.bk_hash
   and p_hybris_categories_lp.p_hybris_categories_lp_id = #p_hybris_categories_lp_insert.p_hybris_categories_lp_id
  join dbo.l_hybris_categories_lp
    on p_hybris_categories_lp.bk_hash = l_hybris_categories_lp.bk_hash
   and p_hybris_categories_lp.l_hybris_categories_lp_id = l_hybris_categories_lp.l_hybris_categories_lp_id
  join dbo.s_hybris_categories_lp
    on p_hybris_categories_lp.bk_hash = s_hybris_categories_lp.bk_hash
   and p_hybris_categories_lp.s_hybris_categories_lp_id = s_hybris_categories_lp.s_hybris_categories_lp_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_hybris_categories_lp
   where d_hybris_categories_lp.bk_hash in (select bk_hash from #p_hybris_categories_lp_insert)

  insert dbo.d_hybris_categories_lp(
             bk_hash,
             d_hybris_categories_lp_key,
             item_pk,
             lang_pk,
             item_type_pk,
             p_description,
             p_name,
             p_hybris_categories_lp_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_hybris_categories_lp_key,
         item_pk,
         lang_pk,
         item_type_pk,
         p_description,
         p_name,
         p_hybris_categories_lp_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_hybris_categories_lp)
--Done!
end
