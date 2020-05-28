CREATE PROC [dbo].[proc_d_lt_bucks_transaction_fifo] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_transaction_fifo)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_lt_bucks_transaction_fifo_insert') is not null drop table #p_lt_bucks_transaction_fifo_insert
create table dbo.#p_lt_bucks_transaction_fifo_insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_transaction_fifo.p_lt_bucks_transaction_fifo_id,
       p_lt_bucks_transaction_fifo.bk_hash
  from dbo.p_lt_bucks_transaction_fifo
 where p_lt_bucks_transaction_fifo.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_lt_bucks_transaction_fifo.dv_batch_id > @max_dv_batch_id
        or p_lt_bucks_transaction_fifo.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_transaction_fifo.bk_hash,
       p_lt_bucks_transaction_fifo.bk_hash fact_lt_bucks_award_spend_key,
       p_lt_bucks_transaction_fifo.tfifo_id tfifo_id,
       case when p_lt_bucks_transaction_fifo.bk_hash in ('-997','-998','-999') then p_lt_bucks_transaction_fifo.bk_hash
            when l_lt_bucks_transaction_fifo.tfifo_transaction_1 is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_transaction_fifo.tfifo_transaction_1 as varchar(500)),'z#@$k%&P'))),2)
        end award_fact_lt_bucks_transaction_key,
        isnull(s_lt_bucks_transaction_fifo.tfifo_amount,0) bucks_amount,
       case when p_lt_bucks_transaction_fifo.bk_hash in ('-997','-998','-999') then p_lt_bucks_transaction_fifo.bk_hash
            when l_lt_bucks_transaction_fifo.tfifo_transaction_2 is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_transaction_fifo.tfifo_transaction_2 as varchar(500)),'z#@$k%&P'))),2)
        end spend_fact_lt_bucks_transaction_key,
       s_lt_bucks_transaction_fifo.tfifo_timestamp transaction_date_time,
       p_lt_bucks_transaction_fifo.p_lt_bucks_transaction_fifo_id,
       p_lt_bucks_transaction_fifo.dv_batch_id,
       p_lt_bucks_transaction_fifo.dv_load_date_time,
       p_lt_bucks_transaction_fifo.dv_load_end_date_time
  from dbo.p_lt_bucks_transaction_fifo
  join #p_lt_bucks_transaction_fifo_insert
    on p_lt_bucks_transaction_fifo.bk_hash = #p_lt_bucks_transaction_fifo_insert.bk_hash
   and p_lt_bucks_transaction_fifo.p_lt_bucks_transaction_fifo_id = #p_lt_bucks_transaction_fifo_insert.p_lt_bucks_transaction_fifo_id
  join dbo.l_lt_bucks_transaction_fifo
    on p_lt_bucks_transaction_fifo.bk_hash = l_lt_bucks_transaction_fifo.bk_hash
   and p_lt_bucks_transaction_fifo.l_lt_bucks_transaction_fifo_id = l_lt_bucks_transaction_fifo.l_lt_bucks_transaction_fifo_id
  join dbo.s_lt_bucks_transaction_fifo
    on p_lt_bucks_transaction_fifo.bk_hash = s_lt_bucks_transaction_fifo.bk_hash
   and p_lt_bucks_transaction_fifo.s_lt_bucks_transaction_fifo_id = s_lt_bucks_transaction_fifo.s_lt_bucks_transaction_fifo_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_lt_bucks_transaction_fifo
   where d_lt_bucks_transaction_fifo.bk_hash in (select bk_hash from #p_lt_bucks_transaction_fifo_insert)

  insert dbo.d_lt_bucks_transaction_fifo(
             bk_hash,
             fact_lt_bucks_award_spend_key,
             tfifo_id,
             award_fact_lt_bucks_transaction_key,
             bucks_amount,
             spend_fact_lt_bucks_transaction_key,
             transaction_date_time,
             p_lt_bucks_transaction_fifo_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_lt_bucks_award_spend_key,
         tfifo_id,
         award_fact_lt_bucks_transaction_key,
         bucks_amount,
         spend_fact_lt_bucks_transaction_key,
         transaction_date_time,
         p_lt_bucks_transaction_fifo_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_transaction_fifo)
--Done!
end
