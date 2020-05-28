CREATE PROC [dbo].[proc_d_mart_dim_seg_member_goal] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mart_dim_seg_member_goal)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mart_dim_seg_member_goal_insert') is not null drop table #p_mart_dim_seg_member_goal_insert
create table dbo.#p_mart_dim_seg_member_goal_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mart_dim_seg_member_goal.p_mart_dim_seg_member_goal_id,
       p_mart_dim_seg_member_goal.bk_hash
  from dbo.p_mart_dim_seg_member_goal
 where p_mart_dim_seg_member_goal.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mart_dim_seg_member_goal.dv_batch_id > @max_dv_batch_id
        or p_mart_dim_seg_member_goal.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mart_dim_seg_member_goal.bk_hash,
       p_mart_dim_seg_member_goal.bk_hash dim_seg_member_goal_key,
       p_mart_dim_seg_member_goal.dim_seg_member_goal_id dim_seg_member_goal_id,
       case when s_mart_dim_seg_member_goal.active_flag = 1 then 'Y' else 'N' end active_flag,
       s_mart_dim_seg_member_goal.goal goal,
       s_mart_dim_seg_member_goal.goal_segment goal_segment,
       s_mart_dim_seg_member_goal.row_add_date row_add_date,
       case when p_mart_dim_seg_member_goal.bk_hash in('-997', '-998', '-999') then p_mart_dim_seg_member_goal.bk_hash
           when s_mart_dim_seg_member_goal.row_add_date is null then '-998'
        else convert(varchar, s_mart_dim_seg_member_goal.row_add_date, 112) end row_add_dim_date_key,
       case when p_mart_dim_seg_member_goal.bk_hash in ('-997','-998','-999') then p_mart_dim_seg_member_goal.bk_hash
       when s_mart_dim_seg_member_goal.row_add_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mart_dim_seg_member_goal.row_add_date,114), 1, 5),':','') end row_add_dim_time_key,
       isnull(h_mart_dim_seg_member_goal.dv_deleted,0) dv_deleted,
       p_mart_dim_seg_member_goal.p_mart_dim_seg_member_goal_id,
       p_mart_dim_seg_member_goal.dv_batch_id,
       p_mart_dim_seg_member_goal.dv_load_date_time,
       p_mart_dim_seg_member_goal.dv_load_end_date_time
  from dbo.h_mart_dim_seg_member_goal
  join dbo.p_mart_dim_seg_member_goal
    on h_mart_dim_seg_member_goal.bk_hash = p_mart_dim_seg_member_goal.bk_hash
  join #p_mart_dim_seg_member_goal_insert
    on p_mart_dim_seg_member_goal.bk_hash = #p_mart_dim_seg_member_goal_insert.bk_hash
   and p_mart_dim_seg_member_goal.p_mart_dim_seg_member_goal_id = #p_mart_dim_seg_member_goal_insert.p_mart_dim_seg_member_goal_id
  join dbo.s_mart_dim_seg_member_goal
    on p_mart_dim_seg_member_goal.bk_hash = s_mart_dim_seg_member_goal.bk_hash
   and p_mart_dim_seg_member_goal.s_mart_dim_seg_member_goal_id = s_mart_dim_seg_member_goal.s_mart_dim_seg_member_goal_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mart_dim_seg_member_goal
   where d_mart_dim_seg_member_goal.bk_hash in (select bk_hash from #p_mart_dim_seg_member_goal_insert)

  insert dbo.d_mart_dim_seg_member_goal(
             bk_hash,
             dim_seg_member_goal_key,
             dim_seg_member_goal_id,
             active_flag,
             goal,
             goal_segment,
             row_add_date,
             row_add_dim_date_key,
             row_add_dim_time_key,
             deleted_flag,
             p_mart_dim_seg_member_goal_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_seg_member_goal_key,
         dim_seg_member_goal_id,
         active_flag,
         goal,
         goal_segment,
         row_add_date,
         row_add_dim_date_key,
         row_add_dim_time_key,
         dv_deleted,
         p_mart_dim_seg_member_goal_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mart_dim_seg_member_goal)
--Done!
end
