CREATE PROC [dbo].[proc_d_boss_product_format] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_product_format)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_product_format_insert') is not null drop table #p_boss_product_format_insert
create table dbo.#p_boss_product_format_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_product_format.p_boss_product_format_id,
       p_boss_product_format.bk_hash
  from dbo.p_boss_product_format
 where p_boss_product_format.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_product_format.dv_batch_id > @max_dv_batch_id
        or p_boss_product_format.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_product_format.bk_hash,
       p_boss_product_format.product_format_id product_format_id,
       s_boss_product_format.help_text product_format_help_text,
       s_boss_product_format.long_desc product_format_long_desc,
       s_boss_product_format.short_desc product_format_short_desc,
       isnull(h_boss_product_format.dv_deleted,0) dv_deleted,
       p_boss_product_format.p_boss_product_format_id,
       p_boss_product_format.dv_batch_id,
       p_boss_product_format.dv_load_date_time,
       p_boss_product_format.dv_load_end_date_time
  from dbo.h_boss_product_format
  join dbo.p_boss_product_format
    on h_boss_product_format.bk_hash = p_boss_product_format.bk_hash
  join #p_boss_product_format_insert
    on p_boss_product_format.bk_hash = #p_boss_product_format_insert.bk_hash
   and p_boss_product_format.p_boss_product_format_id = #p_boss_product_format_insert.p_boss_product_format_id
  join dbo.s_boss_product_format
    on p_boss_product_format.bk_hash = s_boss_product_format.bk_hash
   and p_boss_product_format.s_boss_product_format_id = s_boss_product_format.s_boss_product_format_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_product_format
   where d_boss_product_format.bk_hash in (select bk_hash from #p_boss_product_format_insert)

  insert dbo.d_boss_product_format(
             bk_hash,
             product_format_id,
             product_format_help_text,
             product_format_long_desc,
             product_format_short_desc,
             deleted_flag,
             p_boss_product_format_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         product_format_id,
         product_format_help_text,
         product_format_long_desc,
         product_format_short_desc,
         dv_deleted,
         p_boss_product_format_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_product_format)
--Done!
end
