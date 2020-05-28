CREATE PROC [dbo].[proc_d_mms_tran_item_gift_card_issuance] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_tran_item_gift_card_issuance)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_tran_item_gift_card_issuance_insert') is not null drop table #p_mms_tran_item_gift_card_issuance_insert
create table dbo.#p_mms_tran_item_gift_card_issuance_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_tran_item_gift_card_issuance.p_mms_tran_item_gift_card_issuance_id,
       p_mms_tran_item_gift_card_issuance.bk_hash
  from dbo.p_mms_tran_item_gift_card_issuance
 where p_mms_tran_item_gift_card_issuance.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_tran_item_gift_card_issuance.dv_batch_id > @max_dv_batch_id
        or p_mms_tran_item_gift_card_issuance.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_tran_item_gift_card_issuance.bk_hash,
       p_mms_tran_item_gift_card_issuance.bk_hash d_mms_tran_item_gift_card_issuance_bk_hash,
       p_mms_tran_item_gift_card_issuance.tran_item_gift_card_issuance_id tran_item_gift_card_issuance_id,
       s_mms_tran_item_gift_card_issuance.issuance_amount issuance_amount,
       l_mms_tran_item_gift_card_issuance.tran_item_id tran_item_id,
       h_mms_tran_item_gift_card_issuance.dv_deleted,
       p_mms_tran_item_gift_card_issuance.p_mms_tran_item_gift_card_issuance_id,
       p_mms_tran_item_gift_card_issuance.dv_batch_id,
       p_mms_tran_item_gift_card_issuance.dv_load_date_time,
       p_mms_tran_item_gift_card_issuance.dv_load_end_date_time
  from dbo.h_mms_tran_item_gift_card_issuance
  join dbo.p_mms_tran_item_gift_card_issuance
    on h_mms_tran_item_gift_card_issuance.bk_hash = p_mms_tran_item_gift_card_issuance.bk_hash
  join #p_mms_tran_item_gift_card_issuance_insert
    on p_mms_tran_item_gift_card_issuance.bk_hash = #p_mms_tran_item_gift_card_issuance_insert.bk_hash
   and p_mms_tran_item_gift_card_issuance.p_mms_tran_item_gift_card_issuance_id = #p_mms_tran_item_gift_card_issuance_insert.p_mms_tran_item_gift_card_issuance_id
  join dbo.l_mms_tran_item_gift_card_issuance
    on p_mms_tran_item_gift_card_issuance.bk_hash = l_mms_tran_item_gift_card_issuance.bk_hash
   and p_mms_tran_item_gift_card_issuance.l_mms_tran_item_gift_card_issuance_id = l_mms_tran_item_gift_card_issuance.l_mms_tran_item_gift_card_issuance_id
  join dbo.s_mms_tran_item_gift_card_issuance
    on p_mms_tran_item_gift_card_issuance.bk_hash = s_mms_tran_item_gift_card_issuance.bk_hash
   and p_mms_tran_item_gift_card_issuance.s_mms_tran_item_gift_card_issuance_id = s_mms_tran_item_gift_card_issuance.s_mms_tran_item_gift_card_issuance_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_tran_item_gift_card_issuance
   where d_mms_tran_item_gift_card_issuance.bk_hash in (select bk_hash from #p_mms_tran_item_gift_card_issuance_insert)

  insert dbo.d_mms_tran_item_gift_card_issuance(
             bk_hash,
             d_mms_tran_item_gift_card_issuance_bk_hash,
             tran_item_gift_card_issuance_id,
             issuance_amount,
             tran_item_id,
             p_mms_tran_item_gift_card_issuance_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_mms_tran_item_gift_card_issuance_bk_hash,
         tran_item_gift_card_issuance_id,
         issuance_amount,
         tran_item_id,
         p_mms_tran_item_gift_card_issuance_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_tran_item_gift_card_issuance)
--Done!
end
