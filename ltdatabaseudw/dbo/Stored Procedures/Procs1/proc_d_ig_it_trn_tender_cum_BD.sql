CREATE PROC [dbo].[proc_d_ig_it_trn_tender_cum_BD] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_tender_cum_BD)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_trn_tender_cum_BD_insert') is not null drop table #p_ig_it_trn_tender_cum_BD_insert
create table dbo.#p_ig_it_trn_tender_cum_BD_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_tender_cum_BD.p_ig_it_trn_tender_cum_BD_id,
       p_ig_it_trn_tender_cum_BD.bk_hash
  from dbo.p_ig_it_trn_tender_cum_BD
 where p_ig_it_trn_tender_cum_BD.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_trn_tender_cum_BD.dv_batch_id > @max_dv_batch_id
        or p_ig_it_trn_tender_cum_BD.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_tender_cum_BD.bk_hash,
       p_ig_it_trn_tender_cum_BD.bk_hash fact_cafe_cumulative_tender_key,
       p_ig_it_trn_tender_cum_BD.bus_day_id bus_day_id,
       p_ig_it_trn_tender_cum_BD.check_type_id check_type_id,
       p_ig_it_trn_tender_cum_BD.meal_period_id meal_period_id,
       p_ig_it_trn_tender_cum_BD.cashier_emp_id cashier_emp_id,
       p_ig_it_trn_tender_cum_BD.PMS_post_code PMS_post_code,
       p_ig_it_trn_tender_cum_BD.profit_center_id profit_center_id,
       p_ig_it_trn_tender_cum_BD.tax_removed_code tax_removed_code,
       p_ig_it_trn_tender_cum_BD.tender_id tender_id,
       p_ig_it_trn_tender_cum_BD.void_type_id void_type_id,
       s_ig_it_trn_tender_cum_BD.base_tender_amt base_tender_amount,
       s_ig_it_trn_tender_cum_BD.breakage_amt breakage_amount,
       case when p_ig_it_trn_tender_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_tender_cum_BD.bk_hash     
         when p_ig_it_trn_tender_cum_BD.cashier_emp_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_tender_cum_BD.cashier_emp_id as int) as varchar(500)),'z#@$k%&P'))),2) end cashier_dim_cafe_employee_key,
       s_ig_it_trn_tender_cum_BD.change_amt change_amount,
       case when p_ig_it_trn_tender_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_tender_cum_BD.bk_hash     
         when p_ig_it_trn_tender_cum_BD.bus_day_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_tender_cum_BD.bus_day_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_cafe_business_day_dates_key,
       case when p_ig_it_trn_tender_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_tender_cum_BD.bk_hash     
         when p_ig_it_trn_tender_cum_BD.check_type_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_tender_cum_BD.check_type_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_cafe_check_type_key,
       case when p_ig_it_trn_tender_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_tender_cum_BD.bk_hash     
         when p_ig_it_trn_tender_cum_BD.meal_period_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_tender_cum_BD.meal_period_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_cafe_meal_period_key,
       case when p_ig_it_trn_tender_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_tender_cum_BD.bk_hash     
         when p_ig_it_trn_tender_cum_BD.tender_id is null then '-998' 
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_tender_cum_BD.tender_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_payment_type_key,
       case when p_ig_it_trn_tender_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_tender_cum_BD.bk_hash     
         when p_ig_it_trn_tender_cum_BD.profit_center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_tender_cum_BD.profit_center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end  dim_cafe_profit_center_key ,
       s_ig_it_trn_tender_cum_BD.received_curr_amt - s_ig_it_trn_tender_cum_BD.change_amt net_tender_amount,
       s_ig_it_trn_tender_cum_BD.received_curr_amt received_curr_amount,
       s_ig_it_trn_tender_cum_BD.tender_qty tender_quantity,
       s_ig_it_trn_tender_cum_BD.tip_amt tip_amount,
       isnull(h_ig_it_trn_tender_cum_BD.dv_deleted,0) dv_deleted,
       p_ig_it_trn_tender_cum_BD.p_ig_it_trn_tender_cum_BD_id,
       p_ig_it_trn_tender_cum_BD.dv_batch_id,
       p_ig_it_trn_tender_cum_BD.dv_load_date_time,
       p_ig_it_trn_tender_cum_BD.dv_load_end_date_time
  from dbo.h_ig_it_trn_tender_cum_BD
  join dbo.p_ig_it_trn_tender_cum_BD
    on h_ig_it_trn_tender_cum_BD.bk_hash = p_ig_it_trn_tender_cum_BD.bk_hash
  join #p_ig_it_trn_tender_cum_BD_insert
    on p_ig_it_trn_tender_cum_BD.bk_hash = #p_ig_it_trn_tender_cum_BD_insert.bk_hash
   and p_ig_it_trn_tender_cum_BD.p_ig_it_trn_tender_cum_BD_id = #p_ig_it_trn_tender_cum_BD_insert.p_ig_it_trn_tender_cum_BD_id
  join dbo.s_ig_it_trn_tender_cum_BD
    on p_ig_it_trn_tender_cum_BD.bk_hash = s_ig_it_trn_tender_cum_BD.bk_hash
   and p_ig_it_trn_tender_cum_BD.s_ig_it_trn_tender_cum_BD_id = s_ig_it_trn_tender_cum_BD.s_ig_it_trn_tender_cum_BD_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_trn_tender_cum_BD
   where d_ig_it_trn_tender_cum_BD.bk_hash in (select bk_hash from #p_ig_it_trn_tender_cum_BD_insert)

  insert dbo.d_ig_it_trn_tender_cum_BD(
             bk_hash,
             fact_cafe_cumulative_tender_key,
             bus_day_id,
             check_type_id,
             meal_period_id,
             cashier_emp_id,
             PMS_post_code,
             profit_center_id,
             tax_removed_code,
             tender_id,
             void_type_id,
             base_tender_amount,
             breakage_amount,
             cashier_dim_cafe_employee_key,
             change_amount,
             dim_cafe_business_day_dates_key,
             dim_cafe_check_type_key,
             dim_cafe_meal_period_key,
             dim_cafe_payment_type_key,
             dim_cafe_profit_center_key ,
             net_tender_amount,
             received_curr_amount,
             tender_quantity,
             tip_amount,
             deleted_flag,
             p_ig_it_trn_tender_cum_BD_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_cafe_cumulative_tender_key,
         bus_day_id,
         check_type_id,
         meal_period_id,
         cashier_emp_id,
         PMS_post_code,
         profit_center_id,
         tax_removed_code,
         tender_id,
         void_type_id,
         base_tender_amount,
         breakage_amount,
         cashier_dim_cafe_employee_key,
         change_amount,
         dim_cafe_business_day_dates_key,
         dim_cafe_check_type_key,
         dim_cafe_meal_period_key,
         dim_cafe_payment_type_key,
         dim_cafe_profit_center_key ,
         net_tender_amount,
         received_curr_amount,
         tender_quantity,
         tip_amount,
         dv_deleted,
         p_ig_it_trn_tender_cum_BD_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_tender_cum_BD)
--Done!
end
