CREATE PROC [dbo].[proc_d_humanity_trades] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_humanity_trades)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_humanity_trades_insert') is not null drop table #p_humanity_trades_insert
create table dbo.#p_humanity_trades_insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_trades.p_humanity_trades_id,
       p_humanity_trades.bk_hash
  from dbo.p_humanity_trades
 where p_humanity_trades.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_humanity_trades.dv_batch_id > @max_dv_batch_id
        or p_humanity_trades.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_trades.bk_hash,
       p_humanity_trades.bk_hash d_humanity_trades_key,
       l_humanity_trades.shift_id shift_id,
       l_humanity_trades.trade_reason trade_reason,
       l_humanity_trades.trade_requested_datetime_utc trade_requested_datetime_utc,
       l_humanity_trades.trade_status trade_status,
       l_humanity_trades.swap swap,
       l_humanity_trades.shift_start_datetime_utc shift_start_datetime_utc,
       l_humanity_trades.shift_end_datetime_utc shift_end_datetime_utc,
       l_humanity_trades.hours hours,
       l_humanity_trades.shift_type shift_type,
       l_humanity_trades.position_id position_id,
       l_humanity_trades.workday_position_id workday_position_id,
       l_humanity_trades.company_id company_id,
       l_humanity_trades.position_name position_name,
       l_humanity_trades.location_id location_id,
       l_humanity_trades.location_name location_name,
       s_humanity_trades.ltf_file_name ltf_file_name,
       l_humanity_trades.company_name company_name,
       l_humanity_trades.trade_requested_employee_id trade_requested_employee_id,
       l_humanity_trades.trade_requested_employee_eid trade_requested_employee_eid,
       l_humanity_trades.trade_requested_employee_name trade_requested_employee_name,
       l_humanity_trades.traded_to_employee_id traded_to_employee_id,
       l_humanity_trades.traded_to_employee_eid traded_to_employee_eid,
       l_humanity_trades.traded_to_employee_name traded_to_employee_name,
       cast(substring(s_humanity_trades.ltf_file_name,charindex('.csv',(s_humanity_trades.ltf_file_name))-8,8) as date) file_arrive_date,
       isnull(h_humanity_trades.dv_deleted,0) dv_deleted,
       p_humanity_trades.p_humanity_trades_id,
       p_humanity_trades.dv_batch_id,
       p_humanity_trades.dv_load_date_time,
       p_humanity_trades.dv_load_end_date_time
  from dbo.h_humanity_trades
  join dbo.p_humanity_trades
    on h_humanity_trades.bk_hash = p_humanity_trades.bk_hash
  join #p_humanity_trades_insert
    on p_humanity_trades.bk_hash = #p_humanity_trades_insert.bk_hash
   and p_humanity_trades.p_humanity_trades_id = #p_humanity_trades_insert.p_humanity_trades_id
  join dbo.l_humanity_trades
    on p_humanity_trades.bk_hash = l_humanity_trades.bk_hash
   and p_humanity_trades.l_humanity_trades_id = l_humanity_trades.l_humanity_trades_id
  join dbo.s_humanity_trades
    on p_humanity_trades.bk_hash = s_humanity_trades.bk_hash
   and p_humanity_trades.s_humanity_trades_id = s_humanity_trades.s_humanity_trades_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_humanity_trades
   where d_humanity_trades.bk_hash in (select bk_hash from #p_humanity_trades_insert)

  insert dbo.d_humanity_trades(
             bk_hash,
             d_humanity_trades_key,
             shift_id,
             trade_reason,
             trade_requested_datetime_utc,
             trade_status,
             swap,
             shift_start_datetime_utc,
             shift_end_datetime_utc,
             hours,
             shift_type,
             position_id,
             workday_position_id,
             company_id,
             position_name,
             location_id,
             location_name,
             ltf_file_name,
             company_name,
             trade_requested_employee_id,
             trade_requested_employee_eid,
             trade_requested_employee_name,
             traded_to_employee_id,
             traded_to_employee_eid,
             traded_to_employee_name,
             file_arrive_date,
             deleted_flag,
             p_humanity_trades_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_humanity_trades_key,
         shift_id,
         trade_reason,
         trade_requested_datetime_utc,
         trade_status,
         swap,
         shift_start_datetime_utc,
         shift_end_datetime_utc,
         hours,
         shift_type,
         position_id,
         workday_position_id,
         company_id,
         position_name,
         location_id,
         location_name,
         ltf_file_name,
         company_name,
         trade_requested_employee_id,
         trade_requested_employee_eid,
         trade_requested_employee_name,
         traded_to_employee_id,
         traded_to_employee_eid,
         traded_to_employee_name,
         file_arrive_date,
         dv_deleted,
         p_humanity_trades_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_humanity_trades)
--Done!
end
