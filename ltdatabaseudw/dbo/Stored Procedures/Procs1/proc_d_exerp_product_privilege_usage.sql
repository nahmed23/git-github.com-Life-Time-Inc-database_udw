CREATE PROC [dbo].[proc_d_exerp_product_privilege_usage] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_product_privilege_usage)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_product_privilege_usage_insert') is not null drop table #p_exerp_product_privilege_usage_insert
create table dbo.#p_exerp_product_privilege_usage_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_product_privilege_usage.p_exerp_product_privilege_usage_id,
       p_exerp_product_privilege_usage.bk_hash
  from dbo.p_exerp_product_privilege_usage
 where p_exerp_product_privilege_usage.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_product_privilege_usage.dv_batch_id > @max_dv_batch_id
        or p_exerp_product_privilege_usage.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_product_privilege_usage.bk_hash,
       p_exerp_product_privilege_usage.product_privilege_usage_id product_privilege_usage_id,
       s_exerp_product_privilege_usage.campaign_code campaign_code,
       case when p_exerp_product_privilege_usage.bk_hash in ('-997','-998','-999') then p_exerp_product_privilege_usage.bk_hash     
         when l_exerp_product_privilege_usage.center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_product_privilege_usage.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_exerp_center_bk_hash,
       s_exerp_product_privilege_usage.ets ets,
       s_exerp_product_privilege_usage.state product_privilege_usage_state,
       l_exerp_product_privilege_usage.source_id source_id,
       s_exerp_product_privilege_usage.source_type source_type,
       l_exerp_product_privilege_usage.target_id target_id,
       s_exerp_product_privilege_usage.target_type target_type,
       isnull(h_exerp_product_privilege_usage.dv_deleted,0) dv_deleted,
       p_exerp_product_privilege_usage.p_exerp_product_privilege_usage_id,
       p_exerp_product_privilege_usage.dv_batch_id,
       p_exerp_product_privilege_usage.dv_load_date_time,
       p_exerp_product_privilege_usage.dv_load_end_date_time
  from dbo.h_exerp_product_privilege_usage
  join dbo.p_exerp_product_privilege_usage
    on h_exerp_product_privilege_usage.bk_hash = p_exerp_product_privilege_usage.bk_hash
  join #p_exerp_product_privilege_usage_insert
    on p_exerp_product_privilege_usage.bk_hash = #p_exerp_product_privilege_usage_insert.bk_hash
   and p_exerp_product_privilege_usage.p_exerp_product_privilege_usage_id = #p_exerp_product_privilege_usage_insert.p_exerp_product_privilege_usage_id
  join dbo.l_exerp_product_privilege_usage
    on p_exerp_product_privilege_usage.bk_hash = l_exerp_product_privilege_usage.bk_hash
   and p_exerp_product_privilege_usage.l_exerp_product_privilege_usage_id = l_exerp_product_privilege_usage.l_exerp_product_privilege_usage_id
  join dbo.s_exerp_product_privilege_usage
    on p_exerp_product_privilege_usage.bk_hash = s_exerp_product_privilege_usage.bk_hash
   and p_exerp_product_privilege_usage.s_exerp_product_privilege_usage_id = s_exerp_product_privilege_usage.s_exerp_product_privilege_usage_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_product_privilege_usage
   where d_exerp_product_privilege_usage.bk_hash in (select bk_hash from #p_exerp_product_privilege_usage_insert)

  insert dbo.d_exerp_product_privilege_usage(
             bk_hash,
             product_privilege_usage_id,
             campaign_code,
             d_exerp_center_bk_hash,
             ets,
             product_privilege_usage_state,
             source_id,
             source_type,
             target_id,
             target_type,
             deleted_flag,
             p_exerp_product_privilege_usage_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         product_privilege_usage_id,
         campaign_code,
         d_exerp_center_bk_hash,
         ets,
         product_privilege_usage_state,
         source_id,
         source_type,
         target_id,
         target_type,
         dv_deleted,
         p_exerp_product_privilege_usage_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_product_privilege_usage)
--Done!
end
