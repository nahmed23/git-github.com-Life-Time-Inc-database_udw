CREATE PROC [dbo].[proc_d_exerp_product_group] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_product_group)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_product_group_insert') is not null drop table #p_exerp_product_group_insert
create table dbo.#p_exerp_product_group_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_product_group.p_exerp_product_group_id,
       p_exerp_product_group.bk_hash
  from dbo.p_exerp_product_group
 where p_exerp_product_group.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_product_group.dv_batch_id > @max_dv_batch_id
        or p_exerp_product_group.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_product_group.bk_hash,
       p_exerp_product_group.product_group_id product_group_id,
       case when p_exerp_product_group.bk_hash in('-997', '-998', '-999') then p_exerp_product_group.bk_hash
           when l_exerp_product_group.dimension_product_group_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_product_group.dimension_product_group_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dimension_d_exerp_product_group_bk_hash,
       l_exerp_product_group.dimension_product_group_id dimension_product_group_id,
       l_exerp_product_group.external_id external_id,
       case when p_exerp_product_group.bk_hash in('-997', '-998', '-999') then p_exerp_product_group.bk_hash
           when l_exerp_product_group.parent_product_group_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_product_group.parent_product_group_id as int) as varchar(500)),'z#@$k%&P'))),2)   end parent_d_exerp_product_group_bk_hash,
       l_exerp_product_group.parent_product_group_id parent_product_group_id,
       s_exerp_product_group.name product_group_name,
       isnull(h_exerp_product_group.dv_deleted,0) dv_deleted,
       p_exerp_product_group.p_exerp_product_group_id,
       p_exerp_product_group.dv_batch_id,
       p_exerp_product_group.dv_load_date_time,
       p_exerp_product_group.dv_load_end_date_time
  from dbo.h_exerp_product_group
  join dbo.p_exerp_product_group
    on h_exerp_product_group.bk_hash = p_exerp_product_group.bk_hash
  join #p_exerp_product_group_insert
    on p_exerp_product_group.bk_hash = #p_exerp_product_group_insert.bk_hash
   and p_exerp_product_group.p_exerp_product_group_id = #p_exerp_product_group_insert.p_exerp_product_group_id
  join dbo.l_exerp_product_group
    on p_exerp_product_group.bk_hash = l_exerp_product_group.bk_hash
   and p_exerp_product_group.l_exerp_product_group_id = l_exerp_product_group.l_exerp_product_group_id
  join dbo.s_exerp_product_group
    on p_exerp_product_group.bk_hash = s_exerp_product_group.bk_hash
   and p_exerp_product_group.s_exerp_product_group_id = s_exerp_product_group.s_exerp_product_group_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_product_group
   where d_exerp_product_group.bk_hash in (select bk_hash from #p_exerp_product_group_insert)

  insert dbo.d_exerp_product_group(
             bk_hash,
             product_group_id,
             dimension_d_exerp_product_group_bk_hash,
             dimension_product_group_id,
             external_id,
             parent_d_exerp_product_group_bk_hash,
             parent_product_group_id,
             product_group_name,
             deleted_flag,
             p_exerp_product_group_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         product_group_id,
         dimension_d_exerp_product_group_bk_hash,
         dimension_product_group_id,
         external_id,
         parent_d_exerp_product_group_bk_hash,
         parent_product_group_id,
         product_group_name,
         dv_deleted,
         p_exerp_product_group_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_product_group)
--Done!
end
