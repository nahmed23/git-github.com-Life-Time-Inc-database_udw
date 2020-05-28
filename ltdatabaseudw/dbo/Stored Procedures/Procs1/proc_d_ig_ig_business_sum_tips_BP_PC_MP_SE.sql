CREATE PROC [dbo].[proc_d_ig_ig_business_sum_tips_BP_PC_MP_SE] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_ig_business_sum_tips_BP_PC_MP_SE)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_ig_business_sum_tips_BP_PC_MP_SE_insert') is not null drop table #p_ig_ig_business_sum_tips_BP_PC_MP_SE_insert
create table dbo.#p_ig_ig_business_sum_tips_BP_PC_MP_SE_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_business_sum_tips_BP_PC_MP_SE.p_ig_ig_business_sum_tips_BP_PC_MP_SE_id,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash
  from dbo.p_ig_ig_business_sum_tips_BP_PC_MP_SE
 where p_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_batch_id > @max_dv_batch_id
        or p_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash dim_ig_ig_business_sum_tips_bp_pc_mp_se_key,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.tendered_business_period_dim_id tendered_business_period_dim_id,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.posted_business_period_dim_id posted_business_period_dim_id,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.event_dim_id event_dim_id,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.profit_center_dim_id profit_center_dim_id,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.meal_period_dim_id meal_period_dim_id,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.server_emp_dim_id server_emp_dim_id,
       s_ig_ig_business_sum_tips_BP_PC_MP_SE.charged_gratuity_amount charged_gratuity_amount,
       s_ig_ig_business_sum_tips_BP_PC_MP_SE.charged_tip_amount charged_tip_amount,
       h_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_deleted,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.p_ig_ig_business_sum_tips_BP_PC_MP_SE_id,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_batch_id,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_load_date_time,
       p_ig_ig_business_sum_tips_BP_PC_MP_SE.dv_load_end_date_time
  from dbo.h_ig_ig_business_sum_tips_BP_PC_MP_SE
  join dbo.p_ig_ig_business_sum_tips_BP_PC_MP_SE
    on h_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash = p_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash
  join #p_ig_ig_business_sum_tips_BP_PC_MP_SE_insert
    on p_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash = #p_ig_ig_business_sum_tips_BP_PC_MP_SE_insert.bk_hash
   and p_ig_ig_business_sum_tips_BP_PC_MP_SE.p_ig_ig_business_sum_tips_BP_PC_MP_SE_id = #p_ig_ig_business_sum_tips_BP_PC_MP_SE_insert.p_ig_ig_business_sum_tips_BP_PC_MP_SE_id
  join dbo.s_ig_ig_business_sum_tips_BP_PC_MP_SE
    on p_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash = s_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash
   and p_ig_ig_business_sum_tips_BP_PC_MP_SE.s_ig_ig_business_sum_tips_BP_PC_MP_SE_id = s_ig_ig_business_sum_tips_BP_PC_MP_SE.s_ig_ig_business_sum_tips_BP_PC_MP_SE_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_ig_business_sum_tips_BP_PC_MP_SE
   where d_ig_ig_business_sum_tips_BP_PC_MP_SE.bk_hash in (select bk_hash from #p_ig_ig_business_sum_tips_BP_PC_MP_SE_insert)

  insert dbo.d_ig_ig_business_sum_tips_BP_PC_MP_SE(
             bk_hash,
             dim_ig_ig_business_sum_tips_bp_pc_mp_se_key,
             tendered_business_period_dim_id,
             posted_business_period_dim_id,
             event_dim_id,
             profit_center_dim_id,
             meal_period_dim_id,
             server_emp_dim_id,
             charged_gratuity_amount,
             charged_tip_amount,
             p_ig_ig_business_sum_tips_BP_PC_MP_SE_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_ig_ig_business_sum_tips_bp_pc_mp_se_key,
         tendered_business_period_dim_id,
         posted_business_period_dim_id,
         event_dim_id,
         profit_center_dim_id,
         meal_period_dim_id,
         server_emp_dim_id,
         charged_gratuity_amount,
         charged_tip_amount,
         p_ig_ig_business_sum_tips_BP_PC_MP_SE_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_ig_business_sum_tips_BP_PC_MP_SE)
--Done!
end
