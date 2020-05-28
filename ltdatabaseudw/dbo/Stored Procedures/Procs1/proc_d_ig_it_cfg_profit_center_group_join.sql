CREATE PROC [dbo].[proc_d_ig_it_cfg_profit_center_group_join] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_cfg_profit_center_group_join)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_cfg_profit_center_group_join_insert') is not null drop table #p_ig_it_cfg_profit_center_group_join_insert
create table dbo.#p_ig_it_cfg_profit_center_group_join_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_cfg_profit_center_group_join.p_ig_it_cfg_profit_center_group_join_id,
       p_ig_it_cfg_profit_center_group_join.bk_hash
  from dbo.p_ig_it_cfg_profit_center_group_join
 where p_ig_it_cfg_profit_center_group_join.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_cfg_profit_center_group_join.dv_batch_id > @max_dv_batch_id
        or p_ig_it_cfg_profit_center_group_join.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_cfg_profit_center_group_join.bk_hash,
       p_ig_it_cfg_profit_center_group_join.bk_hash dummy_bk_hash_key,
       p_ig_it_cfg_profit_center_group_join.profit_ctr_grp_id profit_ctr_grp_id,
       p_ig_it_cfg_profit_center_group_join.profit_center_id profit_center_id,
       case when p_ig_it_cfg_profit_center_group_join.profit_ctr_grp_id = 2 then 'Y'
                   else 'N'
               end profit_center_group_bistro_flag,
       case when p_ig_it_cfg_profit_center_group_join.profit_ctr_grp_id = 1 then 'Y'
                   else 'N'
               end profit_center_group_cafe_flag,
       p_ig_it_cfg_profit_center_group_join.p_ig_it_cfg_profit_center_group_join_id,
       p_ig_it_cfg_profit_center_group_join.dv_batch_id,
       p_ig_it_cfg_profit_center_group_join.dv_load_date_time,
       p_ig_it_cfg_profit_center_group_join.dv_load_end_date_time
  from dbo.h_ig_it_cfg_profit_center_group_join
  join dbo.p_ig_it_cfg_profit_center_group_join
    on h_ig_it_cfg_profit_center_group_join.bk_hash = p_ig_it_cfg_profit_center_group_join.bk_hash  join #p_ig_it_cfg_profit_center_group_join_insert
    on p_ig_it_cfg_profit_center_group_join.bk_hash = #p_ig_it_cfg_profit_center_group_join_insert.bk_hash
   and p_ig_it_cfg_profit_center_group_join.p_ig_it_cfg_profit_center_group_join_id = #p_ig_it_cfg_profit_center_group_join_insert.p_ig_it_cfg_profit_center_group_join_id
  join dbo.s_ig_it_cfg_profit_center_group_join
    on p_ig_it_cfg_profit_center_group_join.bk_hash = s_ig_it_cfg_profit_center_group_join.bk_hash
   and p_ig_it_cfg_profit_center_group_join.s_ig_it_cfg_profit_center_group_join_id = s_ig_it_cfg_profit_center_group_join.s_ig_it_cfg_profit_center_group_join_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_cfg_profit_center_group_join
   where d_ig_it_cfg_profit_center_group_join.bk_hash in (select bk_hash from #p_ig_it_cfg_profit_center_group_join_insert)

  insert dbo.d_ig_it_cfg_profit_center_group_join(
             bk_hash,
             dummy_bk_hash_key,
             profit_ctr_grp_id,
             profit_center_id,
             profit_center_group_bistro_flag,
             profit_center_group_cafe_flag,
             p_ig_it_cfg_profit_center_group_join_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dummy_bk_hash_key,
         profit_ctr_grp_id,
         profit_center_id,
         profit_center_group_bistro_flag,
         profit_center_group_cafe_flag,
         p_ig_it_cfg_profit_center_group_join_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_cfg_profit_center_group_join)
--Done!
end
