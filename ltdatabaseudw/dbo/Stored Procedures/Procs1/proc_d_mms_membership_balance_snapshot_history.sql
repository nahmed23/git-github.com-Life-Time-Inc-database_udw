CREATE PROC [dbo].[proc_d_mms_membership_balance_snapshot_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_membership_balance_snapshot_history);

if object_id('tempdb..#p_mms_membership_balance_snapshot_id_list') is not null drop table #p_mms_membership_balance_snapshot_id_list
create table dbo.#p_mms_membership_balance_snapshot_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_mms_membership_balance_snapshot_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_mms_membership_balance_snapshot_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_mms_membership_balance_snapshot_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_mms_membership_balance_snapshot_id,bk_hash) as
(
    select d_mms_membership_balance_snapshot_history.p_mms_membership_balance_snapshot_id,
           d_mms_membership_balance_snapshot_history.bk_hash
      from dbo.d_mms_membership_balance_snapshot_history
      join undo_delete
        on d_mms_membership_balance_snapshot_history.bk_hash = undo_delete.bk_hash
       and d_mms_membership_balance_snapshot_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_mms_membership_balance_snapshot_insert (p_mms_membership_balance_snapshot_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_mms_membership_balance_snapshot_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_mms_membership_balance_snapshot
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_mms_membership_balance_snapshot_update (p_mms_membership_balance_snapshot_id,bk_hash) as
(
    select p_mms_membership_balance_snapshot.p_mms_membership_balance_snapshot_id,
           p_mms_membership_balance_snapshot.bk_hash
      from dbo.p_mms_membership_balance_snapshot
      join p_mms_membership_balance_snapshot_insert
        on p_mms_membership_balance_snapshot.bk_hash = p_mms_membership_balance_snapshot_insert.bk_hash
       and p_mms_membership_balance_snapshot.dv_load_end_date_time = p_mms_membership_balance_snapshot_insert.dv_load_date_time
)
select undo_delete.p_mms_membership_balance_snapshot_id,
       bk_hash
  from undo_delete
union
select undo_update.p_mms_membership_balance_snapshot_id,
       bk_hash
  from undo_update
union
select p_mms_membership_balance_snapshot_insert.p_mms_membership_balance_snapshot_id,
       bk_hash
  from p_mms_membership_balance_snapshot_insert
union
select p_mms_membership_balance_snapshot_update.p_mms_membership_balance_snapshot_id,
       bk_hash
  from p_mms_membership_balance_snapshot_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_mms_membership_balance_snapshot_id_list.bk_hash,
       p_mms_membership_balance_snapshot.bk_hash dim_mms_membership_key,
       p_mms_membership_balance_snapshot.membership_id membership_id,
       isnull(p_mms_membership_balance_snapshot.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102)) effective_date_time ,
       case when p_mms_membership_balance_snapshot.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_mms_membership_balance_snapshot.dv_load_end_date_time
            else p_mms_membership_balance_snapshot.dv_next_greatest_satellite_date_time
            end expiration_date_time,
       s_mms_membership_balance_snapshot.committed_balance_products committed_balance_products ,
       s_mms_membership_balance_snapshot.current_balance_products current_balance_products ,
       s_mms_membership_balance_snapshot.committed_balance end_of_day_committed_balance ,
       s_mms_membership_balance_snapshot.current_balance end_of_day_current_balance ,
       s_mms_membership_balance_snapshot.statement_balance end_of_day_statement_balance ,
       l_mms_membership_balance_snapshot.membership_balance_id membership_balance_id,
       'Y' processing_complete_flag ,
       p_mms_membership_balance_snapshot.p_mms_membership_balance_snapshot_id,
       p_mms_membership_balance_snapshot.dv_batch_id,
       p_mms_membership_balance_snapshot.dv_load_date_time,
       p_mms_membership_balance_snapshot.dv_load_end_date_time
  from dbo.p_mms_membership_balance_snapshot
  join #p_mms_membership_balance_snapshot_id_list
    on p_mms_membership_balance_snapshot.p_mms_membership_balance_snapshot_id = #p_mms_membership_balance_snapshot_id_list.p_mms_membership_balance_snapshot_id
   and p_mms_membership_balance_snapshot.bk_hash = #p_mms_membership_balance_snapshot_id_list.bk_hash
  join dbo.l_mms_membership_balance_snapshot
    on p_mms_membership_balance_snapshot.bk_hash = l_mms_membership_balance_snapshot.bk_hash
   and p_mms_membership_balance_snapshot.l_mms_membership_balance_snapshot_id = l_mms_membership_balance_snapshot.l_mms_membership_balance_snapshot_id
  join dbo.s_mms_membership_balance_snapshot
    on p_mms_membership_balance_snapshot.bk_hash = s_mms_membership_balance_snapshot.bk_hash
   and p_mms_membership_balance_snapshot.s_mms_membership_balance_snapshot_id = s_mms_membership_balance_snapshot.s_mms_membership_balance_snapshot_id
 where isnull(p_mms_membership_balance_snapshot.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102))!= case when p_mms_membership_balance_snapshot.dv_load_end_date_time = convert(datetime, '9999.12.31', 102) then p_mms_membership_balance_snapshot.dv_load_end_date_time
     else p_mms_membership_balance_snapshot.dv_next_greatest_satellite_date_time
     end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_mms_membership_balance_snapshot_history
       where d_mms_membership_balance_snapshot_history.p_mms_membership_balance_snapshot_id in (select p_mms_membership_balance_snapshot_id from #p_mms_membership_balance_snapshot_id_list)

      insert dbo.d_mms_membership_balance_snapshot_history(
                 bk_hash,
                 dim_mms_membership_key,
                 membership_id,
                 effective_date_time ,
                 expiration_date_time,
                 committed_balance_products ,
                 current_balance_products ,
                 end_of_day_committed_balance ,
                 end_of_day_current_balance ,
                 end_of_day_statement_balance ,
                 membership_balance_id,
                 processing_complete_flag ,
                 p_mms_membership_balance_snapshot_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             dim_mms_membership_key,
             membership_id,
             effective_date_time ,
             expiration_date_time,
             committed_balance_products ,
             current_balance_products ,
             end_of_day_committed_balance ,
             end_of_day_current_balance ,
             end_of_day_statement_balance ,
             membership_balance_id,
             processing_complete_flag ,
             p_mms_membership_balance_snapshot_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_membership_balance_snapshot_history)
--Done!
end
