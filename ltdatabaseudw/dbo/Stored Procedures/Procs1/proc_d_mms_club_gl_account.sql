CREATE PROC [dbo].[proc_d_mms_club_gl_account] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_club_gl_account)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_club_gl_account_insert') is not null drop table #p_mms_club_gl_account_insert
create table dbo.#p_mms_club_gl_account_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_gl_account.p_mms_club_gl_account_id,
       p_mms_club_gl_account.bk_hash
  from dbo.p_mms_club_gl_account
 where p_mms_club_gl_account.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_club_gl_account.dv_batch_id > @max_dv_batch_id
        or p_mms_club_gl_account.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_gl_account.bk_hash,
       p_mms_club_gl_account.bk_hash club_gl_account_key,
       p_mms_club_gl_account.club_gl_account_id club_gl_account_id,
       l_mms_club_gl_account.club_id club_id,
       s_mms_club_gl_account.gl_cash_entry_account gl_cash_entry_account,
       s_mms_club_gl_account.gl_cash_entry_cash_sub_account gl_cash_entry_cash_sub_account,
       s_mms_club_gl_account.gl_cash_entry_company_name gl_cash_entry_company_name,
       s_mms_club_gl_account.gl_cash_entry_credit_card_sub_account gl_cash_entry_credit_card_sub_account,
       s_mms_club_gl_account.gl_receivables_entry_account gl_receivables_entry_account,
       s_mms_club_gl_account.gl_receivables_entry_company_name gl_receivables_entry_company_name,
       s_mms_club_gl_account.gl_receivables_entry_sub_account gl_receivables_entry_sub_account,
       l_mms_club_gl_account.val_currency_code_id val_currency_code_id,
       p_mms_club_gl_account.p_mms_club_gl_account_id,
       p_mms_club_gl_account.dv_batch_id,
       p_mms_club_gl_account.dv_load_date_time,
       p_mms_club_gl_account.dv_load_end_date_time
  from dbo.p_mms_club_gl_account
  join #p_mms_club_gl_account_insert
    on p_mms_club_gl_account.bk_hash = #p_mms_club_gl_account_insert.bk_hash
   and p_mms_club_gl_account.p_mms_club_gl_account_id = #p_mms_club_gl_account_insert.p_mms_club_gl_account_id
  join dbo.l_mms_club_gl_account
    on p_mms_club_gl_account.bk_hash = l_mms_club_gl_account.bk_hash
   and p_mms_club_gl_account.l_mms_club_gl_account_id = l_mms_club_gl_account.l_mms_club_gl_account_id
  join dbo.s_mms_club_gl_account
    on p_mms_club_gl_account.bk_hash = s_mms_club_gl_account.bk_hash
   and p_mms_club_gl_account.s_mms_club_gl_account_id = s_mms_club_gl_account.s_mms_club_gl_account_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_club_gl_account
   where d_mms_club_gl_account.bk_hash in (select bk_hash from #p_mms_club_gl_account_insert)

  insert dbo.d_mms_club_gl_account(
             bk_hash,
             club_gl_account_key,
             club_gl_account_id,
             club_id,
             gl_cash_entry_account,
             gl_cash_entry_cash_sub_account,
             gl_cash_entry_company_name,
             gl_cash_entry_credit_card_sub_account,
             gl_receivables_entry_account,
             gl_receivables_entry_company_name,
             gl_receivables_entry_sub_account,
             val_currency_code_id,
             p_mms_club_gl_account_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         club_gl_account_key,
         club_gl_account_id,
         club_id,
         gl_cash_entry_account,
         gl_cash_entry_cash_sub_account,
         gl_cash_entry_company_name,
         gl_cash_entry_credit_card_sub_account,
         gl_receivables_entry_account,
         gl_receivables_entry_company_name,
         gl_receivables_entry_sub_account,
         val_currency_code_id,
         p_mms_club_gl_account_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_club_gl_account)
--Done!
end
