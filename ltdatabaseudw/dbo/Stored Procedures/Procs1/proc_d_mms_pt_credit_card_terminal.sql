CREATE PROC [dbo].[proc_d_mms_pt_credit_card_terminal] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_pt_credit_card_terminal)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_pt_credit_card_terminal_insert') is not null drop table #p_mms_pt_credit_card_terminal_insert
create table dbo.#p_mms_pt_credit_card_terminal_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_pt_credit_card_terminal.p_mms_pt_credit_card_terminal_id,
       p_mms_pt_credit_card_terminal.bk_hash
  from dbo.p_mms_pt_credit_card_terminal
 where p_mms_pt_credit_card_terminal.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_pt_credit_card_terminal.dv_batch_id > @max_dv_batch_id
        or p_mms_pt_credit_card_terminal.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_pt_credit_card_terminal.bk_hash,
       p_mms_pt_credit_card_terminal.bk_hash fact_mms_pt_credit_card_terminal_key,
       p_mms_pt_credit_card_terminal.pt_credit_card_terminal_id pt_credit_card_terminal_id,
       case when p_mms_pt_credit_card_terminal.bk_hash in ('-997','-998','-999') then p_mms_pt_credit_card_terminal.bk_hash
             when l_mms_pt_credit_card_terminal.club_id is null then '-998' 
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pt_credit_card_terminal.club_id as varchar(500)),'z#@$k%&P'))),2)
         end dim_club_key,
       l_mms_pt_credit_card_terminal.merchant_number merchant_number,
       p_mms_pt_credit_card_terminal.p_mms_pt_credit_card_terminal_id,
       p_mms_pt_credit_card_terminal.dv_batch_id,
       p_mms_pt_credit_card_terminal.dv_load_date_time,
       p_mms_pt_credit_card_terminal.dv_load_end_date_time
  from dbo.p_mms_pt_credit_card_terminal
  join #p_mms_pt_credit_card_terminal_insert
    on p_mms_pt_credit_card_terminal.bk_hash = #p_mms_pt_credit_card_terminal_insert.bk_hash
   and p_mms_pt_credit_card_terminal.p_mms_pt_credit_card_terminal_id = #p_mms_pt_credit_card_terminal_insert.p_mms_pt_credit_card_terminal_id
  join dbo.l_mms_pt_credit_card_terminal
    on p_mms_pt_credit_card_terminal.bk_hash = l_mms_pt_credit_card_terminal.bk_hash
   and p_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id = l_mms_pt_credit_card_terminal.l_mms_pt_credit_card_terminal_id
  join dbo.s_mms_pt_credit_card_terminal
    on p_mms_pt_credit_card_terminal.bk_hash = s_mms_pt_credit_card_terminal.bk_hash
   and p_mms_pt_credit_card_terminal.s_mms_pt_credit_card_terminal_id = s_mms_pt_credit_card_terminal.s_mms_pt_credit_card_terminal_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_pt_credit_card_terminal
   where d_mms_pt_credit_card_terminal.bk_hash in (select bk_hash from #p_mms_pt_credit_card_terminal_insert)

  insert dbo.d_mms_pt_credit_card_terminal(
             bk_hash,
             fact_mms_pt_credit_card_terminal_key,
             pt_credit_card_terminal_id,
             dim_club_key,
             merchant_number,
             p_mms_pt_credit_card_terminal_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_pt_credit_card_terminal_key,
         pt_credit_card_terminal_id,
         dim_club_key,
         merchant_number,
         p_mms_pt_credit_card_terminal_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_pt_credit_card_terminal)
--Done!
end
