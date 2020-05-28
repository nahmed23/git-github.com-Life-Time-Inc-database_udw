CREATE PROC [dbo].[proc_d_mms_ACH_charge_back_detail] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_ACH_charge_back_detail)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_ACH_charge_back_detail_insert') is not null drop table #p_mms_ACH_charge_back_detail_insert
create table dbo.#p_mms_ACH_charge_back_detail_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_ACH_charge_back_detail.p_mms_ACH_charge_back_detail_id,
       p_mms_ACH_charge_back_detail.bk_hash
  from dbo.p_mms_ACH_charge_back_detail
 where p_mms_ACH_charge_back_detail.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_ACH_charge_back_detail.dv_batch_id = @current_dv_batch_id) --OR
  -- p_mms_ACH_charge_back_detail.dv_batch_id > @max_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_ACH_charge_back_detail.bk_hash,
       p_mms_ACH_charge_back_detail.bk_hash fact_mms_ach_charge_back_detail_key,
       p_mms_ACH_charge_back_detail.charge_back_mms_tran_id charge_back_mms_tran_id,
       s_mms_ACH_charge_back_detail.club_name club_name,
       case when p_mms_ACH_charge_back_detail.bk_hash in ('-997', '-998', '-999') then p_mms_ACH_charge_back_detail.bk_hash  
           when l_mms_ACH_charge_back_detail.member_id is null then '-998'  
       	    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_ACH_charge_back_detail.member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_ach_charge_back_detail_member_key,
       s_mms_ACH_charge_back_detail.local_currency_code local_currency_code,
       l_mms_ACH_charge_back_detail.member_id member_id,
       cast(cast(s_mms_ACH_charge_back_detail.charge_back_post_date_time as date) as varchar) posted_date,
       s_mms_ACH_charge_back_detail.reporting_currency_code reporting_currency_code,
       s_mms_ACH_charge_back_detail.local_currency_charge_back_tran_amount transaction_amount,
       cast(cast(s_mms_ACH_charge_back_detail.charge_back_post_date_time as date) as varchar) transaction_date,
       s_mms_ACH_charge_back_detail.local_currency_charge_back_tran_amount transaction_line_amount,
       h_mms_ACH_charge_back_detail.dv_deleted,
       p_mms_ACH_charge_back_detail.p_mms_ACH_charge_back_detail_id,
       p_mms_ACH_charge_back_detail.dv_batch_id,
       p_mms_ACH_charge_back_detail.dv_load_date_time,
       p_mms_ACH_charge_back_detail.dv_load_end_date_time
  from dbo.h_mms_ACH_charge_back_detail
  join dbo.p_mms_ACH_charge_back_detail
    on h_mms_ACH_charge_back_detail.bk_hash = p_mms_ACH_charge_back_detail.bk_hash
  join #p_mms_ACH_charge_back_detail_insert
    on p_mms_ACH_charge_back_detail.bk_hash = #p_mms_ACH_charge_back_detail_insert.bk_hash
   and p_mms_ACH_charge_back_detail.p_mms_ACH_charge_back_detail_id = #p_mms_ACH_charge_back_detail_insert.p_mms_ACH_charge_back_detail_id
  join dbo.l_mms_ACH_charge_back_detail
    on p_mms_ACH_charge_back_detail.bk_hash = l_mms_ACH_charge_back_detail.bk_hash
   and p_mms_ACH_charge_back_detail.l_mms_ACH_charge_back_detail_id = l_mms_ACH_charge_back_detail.l_mms_ACH_charge_back_detail_id
  join dbo.s_mms_ACH_charge_back_detail
    on p_mms_ACH_charge_back_detail.bk_hash = s_mms_ACH_charge_back_detail.bk_hash
   and p_mms_ACH_charge_back_detail.s_mms_ACH_charge_back_detail_id = s_mms_ACH_charge_back_detail.s_mms_ACH_charge_back_detail_id

   truncate table dbo.d_mms_ACH_charge_back_detail
-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran

  insert dbo.d_mms_ACH_charge_back_detail(
             bk_hash,
             fact_mms_ach_charge_back_detail_key,
             charge_back_mms_tran_id,
             club_name,
             dim_mms_ach_charge_back_detail_member_key,
             local_currency_code,
             member_id,
             posted_date,
             reporting_currency_code,
             transaction_amount,
             transaction_date,
             transaction_line_amount,
             p_mms_ACH_charge_back_detail_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_ach_charge_back_detail_key,
         charge_back_mms_tran_id,
         club_name,
         dim_mms_ach_charge_back_detail_member_key,
         local_currency_code,
         member_id,
         posted_date,
         reporting_currency_code,
         transaction_amount,
         transaction_date,
         transaction_line_amount,
         p_mms_ACH_charge_back_detail_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_ACH_charge_back_detail)
--Done!
end
