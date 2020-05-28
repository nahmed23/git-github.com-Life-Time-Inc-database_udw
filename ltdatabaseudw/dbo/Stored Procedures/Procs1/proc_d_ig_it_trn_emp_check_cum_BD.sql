CREATE PROC [dbo].[proc_d_ig_it_trn_emp_check_cum_BD] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_emp_check_cum_BD)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_trn_emp_check_cum_BD_insert') is not null drop table #p_ig_it_trn_emp_check_cum_BD_insert
create table dbo.#p_ig_it_trn_emp_check_cum_BD_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_emp_check_cum_BD.p_ig_it_trn_emp_check_cum_BD_id,
       p_ig_it_trn_emp_check_cum_BD.bk_hash
  from dbo.p_ig_it_trn_emp_check_cum_BD
 where p_ig_it_trn_emp_check_cum_BD.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_trn_emp_check_cum_BD.dv_batch_id > @max_dv_batch_id
        or p_ig_it_trn_emp_check_cum_BD.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_emp_check_cum_BD.bk_hash,
       p_ig_it_trn_emp_check_cum_BD.bk_hash fact_cafe_employee_check_cumlative_key,
       p_ig_it_trn_emp_check_cum_BD.bus_day_id bus_day_id,
       p_ig_it_trn_emp_check_cum_BD.check_type_id check_type_id,
       p_ig_it_trn_emp_check_cum_BD.meal_period_id meal_period_id,
       p_ig_it_trn_emp_check_cum_BD.profit_center_id profit_center_id,
       p_ig_it_trn_emp_check_cum_BD.server_emp_id server_emp_id,
       p_ig_it_trn_emp_check_cum_BD.void_type_id void_type_id,
       case when p_ig_it_trn_emp_check_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_emp_check_cum_BD.bk_hash     
         when p_ig_it_trn_emp_check_cum_BD.bus_day_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_emp_check_cum_BD.bus_day_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_cafe_business_day_dates_key,
       case when p_ig_it_trn_emp_check_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_emp_check_cum_BD.bk_hash     
         when p_ig_it_trn_emp_check_cum_BD.check_type_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_emp_check_cum_BD.check_type_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_check_type_key,
       case when p_ig_it_trn_emp_check_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_emp_check_cum_BD.bk_hash     
         when p_ig_it_trn_emp_check_cum_BD.meal_period_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_emp_check_cum_BD.meal_period_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_cafe_meal_period_key,
       case when p_ig_it_trn_emp_check_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_emp_check_cum_BD.bk_hash     
         when p_ig_it_trn_emp_check_cum_BD.profit_center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_emp_check_cum_BD.profit_center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end  dim_cafe_profit_center_key,
       s_ig_it_trn_emp_check_cum_BD.num_checks number_checks,
       s_ig_it_trn_emp_check_cum_BD.num_covers number_covers,
       case when p_ig_it_trn_emp_check_cum_BD.bk_hash in ('-997','-998','-999') then p_ig_it_trn_emp_check_cum_BD.bk_hash     
         when p_ig_it_trn_emp_check_cum_BD.server_emp_id is null then '-998' 
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_ig_it_trn_emp_check_cum_BD.server_emp_id as int) as varchar(500)),'z#@$k%&P'))),2) end server_dim_employee_key,
       isnull(h_ig_it_trn_emp_check_cum_BD.dv_deleted,0) dv_deleted,
       p_ig_it_trn_emp_check_cum_BD.p_ig_it_trn_emp_check_cum_BD_id,
       p_ig_it_trn_emp_check_cum_BD.dv_batch_id,
       p_ig_it_trn_emp_check_cum_BD.dv_load_date_time,
       p_ig_it_trn_emp_check_cum_BD.dv_load_end_date_time
  from dbo.h_ig_it_trn_emp_check_cum_BD
  join dbo.p_ig_it_trn_emp_check_cum_BD
    on h_ig_it_trn_emp_check_cum_BD.bk_hash = p_ig_it_trn_emp_check_cum_BD.bk_hash
  join #p_ig_it_trn_emp_check_cum_BD_insert
    on p_ig_it_trn_emp_check_cum_BD.bk_hash = #p_ig_it_trn_emp_check_cum_BD_insert.bk_hash
   and p_ig_it_trn_emp_check_cum_BD.p_ig_it_trn_emp_check_cum_BD_id = #p_ig_it_trn_emp_check_cum_BD_insert.p_ig_it_trn_emp_check_cum_BD_id
  join dbo.s_ig_it_trn_emp_check_cum_BD
    on p_ig_it_trn_emp_check_cum_BD.bk_hash = s_ig_it_trn_emp_check_cum_BD.bk_hash
   and p_ig_it_trn_emp_check_cum_BD.s_ig_it_trn_emp_check_cum_BD_id = s_ig_it_trn_emp_check_cum_BD.s_ig_it_trn_emp_check_cum_BD_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_trn_emp_check_cum_BD
   where d_ig_it_trn_emp_check_cum_BD.bk_hash in (select bk_hash from #p_ig_it_trn_emp_check_cum_BD_insert)

  insert dbo.d_ig_it_trn_emp_check_cum_BD(
             bk_hash,
             fact_cafe_employee_check_cumlative_key,
             bus_day_id,
             check_type_id,
             meal_period_id,
             profit_center_id,
             server_emp_id,
             void_type_id,
             dim_cafe_business_day_dates_key,
             dim_cafe_check_type_key,
             dim_cafe_meal_period_key,
             dim_cafe_profit_center_key,
             number_checks,
             number_covers,
             server_dim_employee_key,
             deleted_flag,
             p_ig_it_trn_emp_check_cum_BD_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_cafe_employee_check_cumlative_key,
         bus_day_id,
         check_type_id,
         meal_period_id,
         profit_center_id,
         server_emp_id,
         void_type_id,
         dim_cafe_business_day_dates_key,
         dim_cafe_check_type_key,
         dim_cafe_meal_period_key,
         dim_cafe_profit_center_key,
         number_checks,
         number_covers,
         server_dim_employee_key,
         dv_deleted,
         p_ig_it_trn_emp_check_cum_BD_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_emp_check_cum_BD)
--Done!
end
