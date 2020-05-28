CREATE PROC [dbo].[proc_d_forexintegration_exchange_rate] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_forexintegration_exchange_rate)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_forexintegration_exchange_rate_insert') is not null drop table #p_forexintegration_exchange_rate_insert
create table dbo.#p_forexintegration_exchange_rate_insert with(distribution=hash(bk_hash), location=user_db) as
select p_forexintegration_exchange_rate.p_forexintegration_exchange_rate_id,
       p_forexintegration_exchange_rate.bk_hash
  from dbo.p_forexintegration_exchange_rate
 where p_forexintegration_exchange_rate.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_forexintegration_exchange_rate.dv_batch_id > @max_dv_batch_id
        or p_forexintegration_exchange_rate.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_forexintegration_exchange_rate.bk_hash,
       p_forexintegration_exchange_rate.exchange_rate_id exchange_rate_id,
       s_forexintegration_exchange_rate.effective_date effective_date,
       case when p_forexintegration_exchange_rate.bk_hash in ('-997', '-998', '-999') then p_forexintegration_exchange_rate.bk_hash              when s_forexintegration_exchange_rate.effective_date is null then '-998'               else convert(varchar, s_forexintegration_exchange_rate.effective_date, 112)           end effective_dim_date_key,
       s_forexintegration_exchange_rate.currency_rate exchange_rate,
       s_forexintegration_exchange_rate.rate_type exchange_rate_type_description,
       s_forexintegration_exchange_rate.from_exchange_rate_iso_code from_currency_code,
       s_forexintegration_exchange_rate.daily_average_date source_daily_average_date,
       s_forexintegration_exchange_rate.to_exchange_rate_iso_code to_currency_code,
       h_forexintegration_exchange_rate.dv_deleted,
       p_forexintegration_exchange_rate.p_forexintegration_exchange_rate_id,
       p_forexintegration_exchange_rate.dv_batch_id,
       p_forexintegration_exchange_rate.dv_load_date_time,
       p_forexintegration_exchange_rate.dv_load_end_date_time
  from dbo.h_forexintegration_exchange_rate
  join dbo.p_forexintegration_exchange_rate
    on h_forexintegration_exchange_rate.bk_hash = p_forexintegration_exchange_rate.bk_hash
  join #p_forexintegration_exchange_rate_insert
    on p_forexintegration_exchange_rate.bk_hash = #p_forexintegration_exchange_rate_insert.bk_hash
   and p_forexintegration_exchange_rate.p_forexintegration_exchange_rate_id = #p_forexintegration_exchange_rate_insert.p_forexintegration_exchange_rate_id
  join dbo.l_forexintegration_exchange_rate
    on p_forexintegration_exchange_rate.bk_hash = l_forexintegration_exchange_rate.bk_hash
   and p_forexintegration_exchange_rate.l_forexintegration_exchange_rate_id = l_forexintegration_exchange_rate.l_forexintegration_exchange_rate_id
  join dbo.s_forexintegration_exchange_rate
    on p_forexintegration_exchange_rate.bk_hash = s_forexintegration_exchange_rate.bk_hash
   and p_forexintegration_exchange_rate.s_forexintegration_exchange_rate_id = s_forexintegration_exchange_rate.s_forexintegration_exchange_rate_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_forexintegration_exchange_rate
   where d_forexintegration_exchange_rate.bk_hash in (select bk_hash from #p_forexintegration_exchange_rate_insert)

  insert dbo.d_forexintegration_exchange_rate(
             bk_hash,
             exchange_rate_id,
             effective_date,
             effective_dim_date_key,
             exchange_rate,
             exchange_rate_type_description,
             from_currency_code,
             source_daily_average_date,
             to_currency_code,
             p_forexintegration_exchange_rate_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         exchange_rate_id,
         effective_date,
         effective_dim_date_key,
         exchange_rate,
         exchange_rate_type_description,
         from_currency_code,
         source_daily_average_date,
         to_currency_code,
         p_forexintegration_exchange_rate_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_forexintegration_exchange_rate)
--Done!
end
