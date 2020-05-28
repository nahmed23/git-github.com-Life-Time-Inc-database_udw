CREATE PROC [dbo].[proc_d_orbital_transaction_data_report] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_orbital_transaction_data_report)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_orbital_transaction_data_report_insert') is not null drop table #p_orbital_transaction_data_report_insert
create table dbo.#p_orbital_transaction_data_report_insert with(distribution=hash(bk_hash), location=user_db) as
select p_orbital_transaction_data_report.p_orbital_transaction_data_report_id,
       p_orbital_transaction_data_report.bk_hash
  from dbo.p_orbital_transaction_data_report
 where p_orbital_transaction_data_report.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (--p_orbital_transaction_data_report.dv_batch_id > @max_dv_batch_id
        --or 
		p_orbital_transaction_data_report.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_orbital_transaction_data_report.bk_hash,
       p_orbital_transaction_data_report.bk_hash fact_orbital_transaction_data_report_key,
       s_orbital_transaction_data_report.MerchantNumber Merchant_Number,
       s_orbital_transaction_data_report.BatchNumber Batch_Number,
       p_orbital_transaction_data_report.TransactionId Transaction_Id,
       s_orbital_transaction_data_report.Amount Amount,
       case when s_orbital_transaction_data_report.TranCode='DP' then 'Y' else 'N' end deposit_flag,
       s_orbital_transaction_data_report.MOPCode MOP_Code,
       case when s_orbital_transaction_data_report.MOPCode IN('VI','MC') then 'VMC'
           when s_orbital_transaction_data_report.MOPCode='AX' then 'AMEX'
       	when s_orbital_transaction_data_report.MOPCode='DI' then 'DISC' else 'ADJUSTMENT' end tender_type_id,
       s_orbital_transaction_data_report.TransactionDate Transaction_Date,
       isnull(h_orbital_transaction_data_report.dv_deleted,0) dv_deleted,
       p_orbital_transaction_data_report.p_orbital_transaction_data_report_id,
       p_orbital_transaction_data_report.dv_batch_id,
       p_orbital_transaction_data_report.dv_load_date_time,
       p_orbital_transaction_data_report.dv_load_end_date_time
  from dbo.h_orbital_transaction_data_report
  join dbo.p_orbital_transaction_data_report
    on h_orbital_transaction_data_report.bk_hash = p_orbital_transaction_data_report.bk_hash
  join #p_orbital_transaction_data_report_insert
    on p_orbital_transaction_data_report.bk_hash = #p_orbital_transaction_data_report_insert.bk_hash
   and p_orbital_transaction_data_report.p_orbital_transaction_data_report_id = #p_orbital_transaction_data_report_insert.p_orbital_transaction_data_report_id
  join dbo.l_orbital_transaction_data_report
    on p_orbital_transaction_data_report.bk_hash = l_orbital_transaction_data_report.bk_hash
   and p_orbital_transaction_data_report.l_orbital_transaction_data_report_id = l_orbital_transaction_data_report.l_orbital_transaction_data_report_id
  join dbo.s_orbital_transaction_data_report
    on p_orbital_transaction_data_report.bk_hash = s_orbital_transaction_data_report.bk_hash
   and p_orbital_transaction_data_report.s_orbital_transaction_data_report_id = s_orbital_transaction_data_report.s_orbital_transaction_data_report_id

truncate table dbo.d_orbital_transaction_data_report;

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  --delete dbo.d_orbital_transaction_data_report
   --where d_orbital_transaction_data_report.bk_hash in (select bk_hash from #p_orbital_transaction_data_report_insert)

  insert dbo.d_orbital_transaction_data_report(
             bk_hash,
             fact_orbital_transaction_data_report_key,
             Merchant_Number,
             Batch_Number,
             Transaction_Id,
             Amount,
             deposit_flag,
             MOP_Code,
             tender_type_id,
             Transaction_Date,
             deleted_flag,
             p_orbital_transaction_data_report_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_orbital_transaction_data_report_key,
         Merchant_Number,
         Batch_Number,
         Transaction_Id,
         Amount,
         deposit_flag,
         MOP_Code,
         tender_type_id,
         Transaction_Date,
         dv_deleted,
         p_orbital_transaction_data_report_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_orbital_transaction_data_report)
--Done!
end
