CREATE PROC [dbo].[proc_d_mms_pt_credit_card_transaction] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_pt_credit_card_transaction)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_pt_credit_card_transaction_insert') is not null drop table #p_mms_pt_credit_card_transaction_insert
create table dbo.#p_mms_pt_credit_card_transaction_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_pt_credit_card_transaction.p_mms_pt_credit_card_transaction_id,
       p_mms_pt_credit_card_transaction.bk_hash
  from dbo.p_mms_pt_credit_card_transaction
 where p_mms_pt_credit_card_transaction.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_pt_credit_card_transaction.dv_batch_id > @max_dv_batch_id
        or p_mms_pt_credit_card_transaction.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_pt_credit_card_transaction.bk_hash,
       p_mms_pt_credit_card_transaction.bk_hash fact_mms_pt_credit_card_transaction_key,
       p_mms_pt_credit_card_transaction.pt_credit_card_transaction_id pt_credit_card_transaction_id,
       s_mms_pt_credit_card_transaction.authorization_code authorization_code,
       s_mms_pt_credit_card_transaction.card_type card_type,
       substring(ltrim(rtrim(s_mms_pt_credit_card_transaction.masked_account_number)),len(s_mms_pt_credit_card_transaction.masked_account_number)-3,4)  credit_card_last_four_digits,
       case when l_mms_pt_credit_card_transaction.bk_hash in ('-997','-998','-999') then l_mms_pt_credit_card_transaction.bk_hash
             when l_mms_pt_credit_card_transaction.member_id is null then '-998' 
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pt_credit_card_transaction.member_id as varchar(500)),'z#@$k%&P'))),2)
         end dim_mms_member_key,
       case when l_mms_pt_credit_card_transaction.bk_hash in ('-997','-998','-999') then l_mms_pt_credit_card_transaction.bk_hash
             when l_mms_pt_credit_card_transaction.payment_id is null then '-998' 
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pt_credit_card_transaction.payment_id as varchar(500)),'z#@$k%&P'))),2)
         end fact_mms_payment_key,
       case when l_mms_pt_credit_card_transaction.bk_hash in ('-997','-998','-999') then l_mms_pt_credit_card_transaction.bk_hash
             when l_mms_pt_credit_card_transaction.ptcreditcardbatchid is null then '-998' 
             else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_pt_credit_card_transaction.ptcreditcardbatchid as varchar(500)),'z#@$k%&P'))),2)
         end fact_mms_pt_credit_card_batch_key,
       s_mms_pt_credit_card_transaction.masked_account_number masked_account_number,
       case when p_mms_pt_credit_card_transaction.bk_hash in ('-997','-998','-999') then null
            else isnull(s_mms_pt_credit_card_transaction.tran_amount,0) 
       end transaction_amount,
       s_mms_pt_credit_card_transaction.transaction_code transaction_code,
       s_mms_pt_credit_card_transaction.transaction_date_time transaction_date_time,
       case when p_mms_pt_credit_card_transaction.bk_hash in ('-997', '-998', '-999') then p_mms_pt_credit_card_transaction.bk_hash
            when s_mms_pt_credit_card_transaction.transaction_date_time is null then '-998'
            else convert(varchar, s_mms_pt_credit_card_transaction.transaction_date_time, 112)
         end transaction_dim_date_key,
       case when p_mms_pt_credit_card_transaction.bk_hash in ('-997', '-998', '-999') then p_mms_pt_credit_card_transaction.bk_hash
            when s_mms_pt_credit_card_transaction.transaction_date_time is null then '-998'
            else '1' + replace(substring(convert(varchar,s_mms_pt_credit_card_transaction.transaction_date_time,114), 1, 5),':','')
        end transaction_dim_time_key,
       case when p_mms_pt_credit_card_transaction.bk_hash in ('-997','-998','-999') then null
             when s_mms_pt_credit_card_transaction.voided_flag = '1' then 'Y' 
             else 'N'
       end voided_flag,
       p_mms_pt_credit_card_transaction.p_mms_pt_credit_card_transaction_id,
       p_mms_pt_credit_card_transaction.dv_batch_id,
       p_mms_pt_credit_card_transaction.dv_load_date_time,
       p_mms_pt_credit_card_transaction.dv_load_end_date_time
  from dbo.h_mms_pt_credit_card_transaction
  join dbo.p_mms_pt_credit_card_transaction
    on h_mms_pt_credit_card_transaction.bk_hash = p_mms_pt_credit_card_transaction.bk_hash  join #p_mms_pt_credit_card_transaction_insert
    on p_mms_pt_credit_card_transaction.bk_hash = #p_mms_pt_credit_card_transaction_insert.bk_hash
   and p_mms_pt_credit_card_transaction.p_mms_pt_credit_card_transaction_id = #p_mms_pt_credit_card_transaction_insert.p_mms_pt_credit_card_transaction_id
  join dbo.l_mms_pt_credit_card_transaction
    on p_mms_pt_credit_card_transaction.bk_hash = l_mms_pt_credit_card_transaction.bk_hash
   and p_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id = l_mms_pt_credit_card_transaction.l_mms_pt_credit_card_transaction_id
  join dbo.s_mms_pt_credit_card_transaction
    on p_mms_pt_credit_card_transaction.bk_hash = s_mms_pt_credit_card_transaction.bk_hash
   and p_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id = s_mms_pt_credit_card_transaction.s_mms_pt_credit_card_transaction_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_pt_credit_card_transaction
   where d_mms_pt_credit_card_transaction.bk_hash in (select bk_hash from #p_mms_pt_credit_card_transaction_insert)

  insert dbo.d_mms_pt_credit_card_transaction(
             bk_hash,
             fact_mms_pt_credit_card_transaction_key,
             pt_credit_card_transaction_id,
             authorization_code,
             card_type,
             credit_card_last_four_digits,
             dim_mms_member_key,
             fact_mms_payment_key,
             fact_mms_pt_credit_card_batch_key,
             masked_account_number,
             transaction_amount,
             transaction_code,
             transaction_date_time,
             transaction_dim_date_key,
             transaction_dim_time_key,
             voided_flag,
             p_mms_pt_credit_card_transaction_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_pt_credit_card_transaction_key,
         pt_credit_card_transaction_id,
         authorization_code,
         card_type,
         credit_card_last_four_digits,
         dim_mms_member_key,
         fact_mms_payment_key,
         fact_mms_pt_credit_card_batch_key,
         masked_account_number,
         transaction_amount,
         transaction_code,
         transaction_date_time,
         transaction_dim_date_key,
         transaction_dim_time_key,
         voided_flag,
         p_mms_pt_credit_card_transaction_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_pt_credit_card_transaction)
--Done!
end
