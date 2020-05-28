CREATE PROC [dbo].[proc_d_healthcheckusa_transactions_history] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_healthcheckusa_transactions_history);

if object_id('tempdb..#p_healthcheckusa_transactions_id_list') is not null drop table #p_healthcheckusa_transactions_id_list
create table dbo.#p_healthcheckusa_transactions_id_list with(distribution=hash(bk_hash), location=user_db, heap) as
with undo_delete (p_healthcheckusa_transactions_id,bk_hash,dv_load_date_time) as 
(
-- Find any updates with the current batch ID to undo in case of retry - just like in the PIT Proc
--   Find the records in the current batch
--   Find the records related to the current batch
--   Note that this needs to be done using the PIT ids within the fact/dimension base table since a workflow retry would have deleted those IDs from the PIT table and reinserted with new IDs
    select p_healthcheckusa_transactions_id,
           bk_hash,
           dv_load_date_time
      from dbo.d_healthcheckusa_transactions_history
     where dv_batch_id = @current_dv_batch_id
),
undo_update (p_healthcheckusa_transactions_id,bk_hash) as
(
    select d_healthcheckusa_transactions_history.p_healthcheckusa_transactions_id,
           d_healthcheckusa_transactions_history.bk_hash
      from dbo.d_healthcheckusa_transactions_history
      join undo_delete
        on d_healthcheckusa_transactions_history.bk_hash = undo_delete.bk_hash
       and d_healthcheckusa_transactions_history.dv_load_end_date_time = undo_delete.dv_load_date_time
),
p_healthcheckusa_transactions_insert (p_healthcheckusa_transactions_id,bk_hash,dv_load_date_time) as 
(
-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those have not been physically deleted yet - if they exist
-- Then find the PIT ids in the PIT table that correspond to the dimension/fact records to end-date
    select p_healthcheckusa_transactions_id,
           bk_hash,
           dv_load_date_time
      from dbo.p_healthcheckusa_transactions
     where dv_batch_id > @max_dv_batch_id
        or dv_batch_id = @current_dv_batch_id
),
p_healthcheckusa_transactions_update (p_healthcheckusa_transactions_id,bk_hash) as
(
    select p_healthcheckusa_transactions.p_healthcheckusa_transactions_id,
           p_healthcheckusa_transactions.bk_hash
      from dbo.p_healthcheckusa_transactions
      join p_healthcheckusa_transactions_insert
        on p_healthcheckusa_transactions.bk_hash = p_healthcheckusa_transactions_insert.bk_hash
       and p_healthcheckusa_transactions.dv_load_end_date_time = p_healthcheckusa_transactions_insert.dv_load_date_time
)
select undo_delete.p_healthcheckusa_transactions_id,
       bk_hash
  from undo_delete
union
select undo_update.p_healthcheckusa_transactions_id,
       bk_hash
  from undo_update
union
select p_healthcheckusa_transactions_insert.p_healthcheckusa_transactions_id,
       bk_hash
  from p_healthcheckusa_transactions_insert
union
select p_healthcheckusa_transactions_update.p_healthcheckusa_transactions_id,
       bk_hash
  from p_healthcheckusa_transactions_update

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db, heap) as
select #p_healthcheckusa_transactions_id_list.bk_hash,
       p_healthcheckusa_transactions.bk_hash d_healthcheckusa_transactions_bk_hash,
       p_healthcheckusa_transactions.order_number order_number,
       p_healthcheckusa_transactions.sku product_sku,
       isnull(p_healthcheckusa_transactions.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102)) effective_date_time,
       case when p_healthcheckusa_transactions.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)       then p_healthcheckusa_transactions.dv_load_end_date_time  else p_healthcheckusa_transactions.dv_next_greatest_satellite_date_time end expiration_date_time,
       case when p_healthcheckusa_transactions.bk_hash in ('-997','-998','-999') then p_healthcheckusa_transactions.bk_hash   when l_healthcheckusa_transactions.ltf_employee_id is not null then  convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_healthcheckusa_transactions.ltf_employee_id as int) as varchar(500)),'z#@$k%&P'))),2) else '-998'   end dim_employee_key,
       l_healthcheckusa_transactions.ltf_employee_id employee_id,
       l_healthcheckusa_transactions.ltf_gl_club_id gl_club_id,
       s_healthcheckusa_transactions.item_amount item_amount,
       s_healthcheckusa_transactions.item_discount item_discount,
       s_healthcheckusa_transactions.order_for_employee_flag order_for_employee_flag,
       s_healthcheckusa_transactions.quantity quantity,
       s_healthcheckusa_transactions.transaction_date transaction_date,
       s_healthcheckusa_transactions.transaction_type transaction_type,
       h_healthcheckusa_transactions.dv_deleted,
       p_healthcheckusa_transactions.p_healthcheckusa_transactions_id,
       p_healthcheckusa_transactions.dv_batch_id,
       p_healthcheckusa_transactions.dv_load_date_time,
       p_healthcheckusa_transactions.dv_load_end_date_time
  from dbo.h_healthcheckusa_transactions
  join dbo.p_healthcheckusa_transactions
    on h_healthcheckusa_transactions.bk_hash = p_healthcheckusa_transactions.bk_hash  join #p_healthcheckusa_transactions_id_list
    on p_healthcheckusa_transactions.p_healthcheckusa_transactions_id = #p_healthcheckusa_transactions_id_list.p_healthcheckusa_transactions_id
   and p_healthcheckusa_transactions.bk_hash = #p_healthcheckusa_transactions_id_list.bk_hash
  join dbo.l_healthcheckusa_transactions
    on p_healthcheckusa_transactions.bk_hash = l_healthcheckusa_transactions.bk_hash
   and p_healthcheckusa_transactions.l_healthcheckusa_transactions_id = l_healthcheckusa_transactions.l_healthcheckusa_transactions_id
  join dbo.s_healthcheckusa_transactions
    on p_healthcheckusa_transactions.bk_hash = s_healthcheckusa_transactions.bk_hash
   and p_healthcheckusa_transactions.s_healthcheckusa_transactions_id = s_healthcheckusa_transactions.s_healthcheckusa_transactions_id
 where isnull(p_healthcheckusa_transactions.dv_greatest_satellite_date_time, convert(datetime, '2000.01.01', 102))!= case when p_healthcheckusa_transactions.dv_load_end_date_time = convert(datetime, '9999.12.31', 102)       then p_healthcheckusa_transactions.dv_load_end_date_time  else p_healthcheckusa_transactions.dv_next_greatest_satellite_date_time end


-- do as a single transaction
--   delete records from dimension where PIT_id = #PIT.PIT_id
--     Note that this also gets rid of any records where the existing effective_date_time equals the soon to be newly calculated expiration_date_time
--   insert records from all of the joins to the pit table and to #PIT.PIT_id
    begin tran
      delete dbo.d_healthcheckusa_transactions_history
       where d_healthcheckusa_transactions_history.p_healthcheckusa_transactions_id in (select p_healthcheckusa_transactions_id from #p_healthcheckusa_transactions_id_list)

      insert dbo.d_healthcheckusa_transactions_history(
                 bk_hash,
                 d_healthcheckusa_transactions_bk_hash,
                 order_number,
                 product_sku,
                 effective_date_time,
                 expiration_date_time,
                 dim_employee_key,
                 employee_id,
                 gl_club_id,
                 item_amount,
                 item_discount,
                 order_for_employee_flag,
                 quantity,
                 transaction_date,
                 transaction_type,
                 deleted_flag,
                 p_healthcheckusa_transactions_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select bk_hash,
             d_healthcheckusa_transactions_bk_hash,
             order_number,
             product_sku,
             effective_date_time,
             expiration_date_time,
             dim_employee_key,
             employee_id,
             gl_club_id,
             item_amount,
             item_discount,
             order_for_employee_flag,
             quantity,
             transaction_date,
             transaction_type,
             dv_deleted,
             p_healthcheckusa_transactions_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
    commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_healthcheckusa_transactions_history)
--Done!
end
