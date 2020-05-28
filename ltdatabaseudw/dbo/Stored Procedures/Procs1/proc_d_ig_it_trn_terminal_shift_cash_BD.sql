CREATE PROC [dbo].[proc_d_ig_it_trn_terminal_shift_cash_BD] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_terminal_shift_cash_BD)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_trn_terminal_shift_cash_BD_insert') is not null drop table #p_ig_it_trn_terminal_shift_cash_BD_insert
create table dbo.#p_ig_it_trn_terminal_shift_cash_BD_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_terminal_shift_cash_BD.p_ig_it_trn_terminal_shift_cash_BD_id,
       p_ig_it_trn_terminal_shift_cash_BD.bk_hash
  from dbo.p_ig_it_trn_terminal_shift_cash_BD
 where p_ig_it_trn_terminal_shift_cash_BD.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_trn_terminal_shift_cash_BD.dv_batch_id > @max_dv_batch_id
        or p_ig_it_trn_terminal_shift_cash_BD.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_terminal_shift_cash_BD.bk_hash,
       p_ig_it_trn_terminal_shift_cash_BD.bk_hash fact_cafe_terminal_shift_cash_key,
       p_ig_it_trn_terminal_shift_cash_BD.bus_day_id bus_day_id,
       p_ig_it_trn_terminal_shift_cash_BD.cash_shift_id cash_shift_id,
       p_ig_it_trn_terminal_shift_cash_BD.tender_id tender_id,
       p_ig_it_trn_terminal_shift_cash_BD.term_id term_id,
       s_ig_it_trn_terminal_shift_cash_BD.breakage_amt breakage_amount,
       s_ig_it_trn_terminal_shift_cash_BD_1.bd_end_dt_time business_day_end_date_time,
       case when p_ig_it_trn_terminal_shift_cash_BD.bk_hash in('-997', '-998', '-999') then p_ig_it_trn_terminal_shift_cash_BD.bk_hash
           when s_ig_it_trn_terminal_shift_cash_BD_1.bd_end_dt_time is null then '-998'
        else convert(varchar, s_ig_it_trn_terminal_shift_cash_BD_1.bd_end_dt_time, 112)  end    business_day_end_dim_date_key,
       case when p_ig_it_trn_terminal_shift_cash_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_terminal_shift_cash_BD.bk_hash
       when s_ig_it_trn_terminal_shift_cash_BD_1.bd_end_dt_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ig_it_trn_terminal_shift_cash_BD_1.bd_end_dt_time,114), 1, 5),':','') end business_day_end_dim_time_key,
       s_ig_it_trn_terminal_shift_cash_BD_1.bd_start_dt_time business_day_start_date_time,
       case when p_ig_it_trn_terminal_shift_cash_BD.bk_hash in('-997', '-998', '-999') then p_ig_it_trn_terminal_shift_cash_BD.bk_hash
           when s_ig_it_trn_terminal_shift_cash_BD_1.bd_start_dt_time is null then '-998'
        else convert(varchar, s_ig_it_trn_terminal_shift_cash_BD_1.bd_start_dt_time, 112)    end business_day_start_dim_date_key,
       case when p_ig_it_trn_terminal_shift_cash_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_terminal_shift_cash_BD.bk_hash
       when s_ig_it_trn_terminal_shift_cash_BD_1.bd_start_dt_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ig_it_trn_terminal_shift_cash_BD_1.bd_start_dt_time,114), 1, 5),':','') end business_day_start_dim_time_key,
       s_ig_it_trn_terminal_shift_cash_BD.cash_drop_amt cash_drop_amount,
       s_ig_it_trn_terminal_shift_cash_BD.change_amt change_amount,
       case when p_ig_it_trn_terminal_shift_cash_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_terminal_shift_cash_BD.bk_hash     
         when p_ig_it_trn_terminal_shift_cash_BD.bus_day_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_terminal_shift_cash_BD.bus_day_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_cafe_business_day_dates_key,
       case when p_ig_it_trn_terminal_shift_cash_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_terminal_shift_cash_BD.bk_hash     
         when p_ig_it_trn_terminal_shift_cash_BD.tender_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_terminal_shift_cash_BD.tender_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_payment_type_key,
       case when p_ig_it_trn_terminal_shift_cash_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_terminal_shift_cash_BD.bk_hash     
         when p_ig_it_trn_terminal_shift_cash_BD.term_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_terminal_shift_cash_BD.term_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_terminal_key,
       s_ig_it_trn_terminal_shift_cash_BD.loan_amt loan_amount,
       s_ig_it_trn_terminal_shift_cash_BD.num_tendered_checks number_tendered_checks,
       s_ig_it_trn_terminal_shift_cash_BD.paidout_amt paid_out_amount,
       s_ig_it_trn_terminal_shift_cash_BD.received_curr_amt received_current_amount,
       s_ig_it_trn_terminal_shift_cash_BD.tender_amt tender_amount,
       s_ig_it_trn_terminal_shift_cash_BD.tender_qty tender_quantity,
       s_ig_it_trn_terminal_shift_cash_BD.withdrawal_amt withdrawal_amount,
       isnull(h_ig_it_trn_terminal_shift_cash_BD.dv_deleted,0) dv_deleted,
       p_ig_it_trn_terminal_shift_cash_BD.p_ig_it_trn_terminal_shift_cash_BD_id,
       p_ig_it_trn_terminal_shift_cash_BD.dv_batch_id,
       p_ig_it_trn_terminal_shift_cash_BD.dv_load_date_time,
       p_ig_it_trn_terminal_shift_cash_BD.dv_load_end_date_time
  from dbo.h_ig_it_trn_terminal_shift_cash_BD
  join dbo.p_ig_it_trn_terminal_shift_cash_BD
    on h_ig_it_trn_terminal_shift_cash_BD.bk_hash = p_ig_it_trn_terminal_shift_cash_BD.bk_hash
  join #p_ig_it_trn_terminal_shift_cash_BD_insert
    on p_ig_it_trn_terminal_shift_cash_BD.bk_hash = #p_ig_it_trn_terminal_shift_cash_BD_insert.bk_hash
   and p_ig_it_trn_terminal_shift_cash_BD.p_ig_it_trn_terminal_shift_cash_BD_id = #p_ig_it_trn_terminal_shift_cash_BD_insert.p_ig_it_trn_terminal_shift_cash_BD_id
  join dbo.s_ig_it_trn_terminal_shift_cash_BD
    on p_ig_it_trn_terminal_shift_cash_BD.bk_hash = s_ig_it_trn_terminal_shift_cash_BD.bk_hash
   and p_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_id = s_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_id
  join dbo.s_ig_it_trn_terminal_shift_cash_BD_1
    on p_ig_it_trn_terminal_shift_cash_BD.bk_hash = s_ig_it_trn_terminal_shift_cash_BD_1.bk_hash
   and p_ig_it_trn_terminal_shift_cash_BD.s_ig_it_trn_terminal_shift_cash_BD_1_id = s_ig_it_trn_terminal_shift_cash_BD_1.s_ig_it_trn_terminal_shift_cash_BD_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_trn_terminal_shift_cash_BD
   where d_ig_it_trn_terminal_shift_cash_BD.bk_hash in (select bk_hash from #p_ig_it_trn_terminal_shift_cash_BD_insert)

  insert dbo.d_ig_it_trn_terminal_shift_cash_BD(
             bk_hash,
             fact_cafe_terminal_shift_cash_key,
             bus_day_id,
             cash_shift_id,
             tender_id,
             term_id,
             breakage_amount,
             business_day_end_date_time,
             business_day_end_dim_date_key,
             business_day_end_dim_time_key,
             business_day_start_date_time,
             business_day_start_dim_date_key,
             business_day_start_dim_time_key,
             cash_drop_amount,
             change_amount,
             dim_cafe_business_day_dates_key,
             dim_cafe_payment_type_key,
             dim_cafe_terminal_key,
             loan_amount,
             number_tendered_checks,
             paid_out_amount,
             received_current_amount,
             tender_amount,
             tender_quantity,
             withdrawal_amount,
             deleted_flag,
             p_ig_it_trn_terminal_shift_cash_BD_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_cafe_terminal_shift_cash_key,
         bus_day_id,
         cash_shift_id,
         tender_id,
         term_id,
         breakage_amount,
         business_day_end_date_time,
         business_day_end_dim_date_key,
         business_day_end_dim_time_key,
         business_day_start_date_time,
         business_day_start_dim_date_key,
         business_day_start_dim_time_key,
         cash_drop_amount,
         change_amount,
         dim_cafe_business_day_dates_key,
         dim_cafe_payment_type_key,
         dim_cafe_terminal_key,
         loan_amount,
         number_tendered_checks,
         paid_out_amount,
         received_current_amount,
         tender_amount,
         tender_quantity,
         withdrawal_amount,
         dv_deleted,
         p_ig_it_trn_terminal_shift_cash_BD_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_terminal_shift_cash_BD)
--Done!
end
