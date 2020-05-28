CREATE PROC [dbo].[proc_d_mart_seg_member_juniors_on_account_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mart_seg_member_juniors_on_account_history);

if object_id('tempdb..#p_mart_seg_member_juniors_on_account_id_list') is not null drop table #p_mart_seg_member_juniors_on_account_id_list
create table dbo.#p_mart_seg_member_juniors_on_account_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_mart_seg_member_juniors_on_account_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_mart_seg_member_juniors_on_account_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_mart_seg_member_juniors_on_account_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_mart_seg_member_juniors_on_account_id,bk_hash) as
(
    select d_mart_seg_member_juniors_on_account_history.p_mart_seg_member_juniors_on_account_id,
           d_mart_seg_member_juniors_on_account_history.bk_hash
      from dbo.d_mart_seg_member_juniors_on_account_history
      join undo_delete
        on d_mart_seg_member_juniors_on_account_history.bk_hash = undo_delete.bk_hash
       and d_mart_seg_member_juniors_on_account_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_mart_seg_member_juniors_on_account_insert (p_mart_seg_member_juniors_on_account_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_mart_seg_member_juniors_on_account_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_mart_seg_member_juniors_on_account
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_mart_seg_member_juniors_on_account_update (p_mart_seg_member_juniors_on_account_id,bk_hash) as
(
    select p_mart_seg_member_juniors_on_account.p_mart_seg_member_juniors_on_account_id,
           p_mart_seg_member_juniors_on_account.bk_hash
      from dbo.p_mart_seg_member_juniors_on_account
      join p_mart_seg_member_juniors_on_account_insert
        on p_mart_seg_member_juniors_on_account.bk_hash = p_mart_seg_member_juniors_on_account_insert.bk_hash
       and p_mart_seg_member_juniors_on_account.dv_load_end_date_time = p_mart_seg_member_juniors_on_account_insert.dv_load_date_time
)
select undo_delete.p_mart_seg_member_juniors_on_account_id,
       bk_hash
  from undo_delete
union
select undo_update.p_mart_seg_member_juniors_on_account_id,
       bk_hash
  from undo_update
union
select p_mart_seg_member_juniors_on_account_insert.p_mart_seg_member_juniors_on_account_id,
       bk_hash
  from p_mart_seg_member_juniors_on_account_insert
union
select p_mart_seg_member_juniors_on_account_update.p_mart_seg_member_juniors_on_account_id,
       bk_hash
  from p_mart_seg_member_juniors_on_account_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_mart_seg_member_juniors_on_account_id_list.bk_hash,
       p_mart_seg_member_juniors_on_account.bk_hash dim_juniors_on_account_segment_key,
       p_mart_seg_member_juniors_on_account.juniors_on_account_segment_id juniors_on_account_segment_id,
       isnull(p_mart_seg_member_juniors_on_account.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102)) effective_date_time,
       case when p_mart_seg_member_juniors_on_account.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) 
       then p_mart_seg_member_juniors_on_account.dv_load_end_date_time                
       else p_mart_seg_member_juniors_on_account.dv_next_greatest_satellite_date_time         
       end expiration_date_time,
       case when p_mart_seg_member_juniors_on_account.dv_next_greatest_satellite_date_time is null then '1'else '0' end active_flag,
       s_mart_seg_member_juniors_on_account.juniors_on_account juniors_on_account,
       h_mart_seg_member_juniors_on_account.dv_deleted,
       p_mart_seg_member_juniors_on_account.p_mart_seg_member_juniors_on_account_id,
       p_mart_seg_member_juniors_on_account.dv_batch_id,
       p_mart_seg_member_juniors_on_account.dv_load_date_time,
       p_mart_seg_member_juniors_on_account.dv_load_end_date_time
  from dbo.h_mart_seg_member_juniors_on_account
  join dbo.p_mart_seg_member_juniors_on_account
    on h_mart_seg_member_juniors_on_account.bk_hash = p_mart_seg_member_juniors_on_account.bk_hash  join #p_mart_seg_member_juniors_on_account_id_list
    on p_mart_seg_member_juniors_on_account.p_mart_seg_member_juniors_on_account_id = #p_mart_seg_member_juniors_on_account_id_list.p_mart_seg_member_juniors_on_account_id
   and p_mart_seg_member_juniors_on_account.bk_hash = #p_mart_seg_member_juniors_on_account_id_list.bk_hash
  join dbo.s_mart_seg_member_juniors_on_account
    on p_mart_seg_member_juniors_on_account.bk_hash = s_mart_seg_member_juniors_on_account.bk_hash
   and p_mart_seg_member_juniors_on_account.s_mart_seg_member_juniors_on_account_id = s_mart_seg_member_juniors_on_account.s_mart_seg_member_juniors_on_account_id
 where isnull(p_mart_seg_member_juniors_on_account.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102))!= case when p_mart_seg_member_juniors_on_account.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) 
then p_mart_seg_member_juniors_on_account.dv_load_end_date_time                
else p_mart_seg_member_juniors_on_account.dv_next_greatest_satellite_date_time         
end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_mart_seg_member_juniors_on_account_history
       where d_mart_seg_member_juniors_on_account_history.p_mart_seg_member_juniors_on_account_id in (select p_mart_seg_member_juniors_on_account_id from #p_mart_seg_member_juniors_on_account_id_list)

      insert dbo.d_mart_seg_member_juniors_on_account_history(
                 bk_hash,
                 dim_juniors_on_account_segment_key,
                 juniors_on_account_segment_id,
                 effective_date_time,
                 expiration_date_time,
                 active_flag,
                 juniors_on_account,
                 deleted_flag,
                 p_mart_seg_member_juniors_on_account_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             dim_juniors_on_account_segment_key,
             juniors_on_account_segment_id,
             effective_date_time,
             expiration_date_time,
             active_flag,
             juniors_on_account,
             dv_deleted,
             p_mart_seg_member_juniors_on_account_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mart_seg_member_juniors_on_account_history)
--Done!
end
