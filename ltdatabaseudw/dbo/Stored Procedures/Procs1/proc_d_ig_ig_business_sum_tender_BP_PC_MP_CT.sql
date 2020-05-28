CREATE PROC [dbo].[proc_d_ig_ig_business_sum_tender_BP_PC_MP_CT] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_ig_business_sum_tender_BP_PC_MP_CT)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_ig_business_sum_tender_BP_PC_MP_CT_insert') is not null drop table #p_ig_ig_business_sum_tender_BP_PC_MP_CT_insert
create table dbo.#p_ig_ig_business_sum_tender_BP_PC_MP_CT_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_business_sum_tender_BP_PC_MP_CT.p_ig_ig_business_sum_tender_BP_PC_MP_CT_id,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
  from dbo.p_ig_ig_business_sum_tender_BP_PC_MP_CT
 where p_ig_ig_business_sum_tender_BP_PC_MP_CT.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_ig_business_sum_tender_BP_PC_MP_CT.dv_batch_id > @max_dv_batch_id
        or p_ig_ig_business_sum_tender_BP_PC_MP_CT.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash fact_cafe_tender_by_check_type_summary_key,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.event_dim_id event_dim_id,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.meal_period_dim_id meal_period_dim_id,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.check_type_dim_id check_type_dim_id,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.credit_type_id credit_type_id,
       isnull(s_ig_ig_business_sum_tender_BP_PC_MP_CT.change_amount,0) new_change_amount,
       isnull(s_ig_ig_business_sum_tender_BP_PC_MP_CT.tender_amount,0) new_tender_amount,
       case when p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash in ('-997', '-998', '-999') then p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
             when p_ig_ig_business_sum_tender_BP_PC_MP_CT.posted_business_period_dim_id is null then '-998'
       	  else p_ig_ig_business_sum_tender_BP_PC_MP_CT.posted_business_period_dim_id end posted_business_period_dim_id,
       case when p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash in ('-997', '-998', '-999') then p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
             when p_ig_ig_business_sum_tender_BP_PC_MP_CT.profit_center_dim_id is null then '-998'
       	  else p_ig_ig_business_sum_tender_BP_PC_MP_CT.profit_center_dim_id end profit_center_dim_id,
       case when p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash in ('-997', '-998', '-999') then p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
             when p_ig_ig_business_sum_tender_BP_PC_MP_CT.tender_dim_id is null then '-998'
       	  else p_ig_ig_business_sum_tender_BP_PC_MP_CT.tender_dim_id end tender_dim_id,
       isnull(s_ig_ig_business_sum_tender_BP_PC_MP_CT.tender_amount,0) - isnull(s_ig_ig_business_sum_tender_BP_PC_MP_CT.change_amount,0) tender_net_amount,
       case when p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash in ('-997', '-998', '-999') then p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
             when p_ig_ig_business_sum_tender_BP_PC_MP_CT.tendered_business_period_dim_id is null then '-998'
       	  else p_ig_ig_business_sum_tender_BP_PC_MP_CT.tendered_business_period_dim_id end tendered_business_period_dim_id,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.p_ig_ig_business_sum_tender_BP_PC_MP_CT_id,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.dv_batch_id,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.dv_load_date_time,
       p_ig_ig_business_sum_tender_BP_PC_MP_CT.dv_load_end_date_time
  from dbo.p_ig_ig_business_sum_tender_BP_PC_MP_CT
  join #p_ig_ig_business_sum_tender_BP_PC_MP_CT_insert
    on p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash = #p_ig_ig_business_sum_tender_BP_PC_MP_CT_insert.bk_hash
   and p_ig_ig_business_sum_tender_BP_PC_MP_CT.p_ig_ig_business_sum_tender_BP_PC_MP_CT_id = #p_ig_ig_business_sum_tender_BP_PC_MP_CT_insert.p_ig_ig_business_sum_tender_BP_PC_MP_CT_id
  join dbo.s_ig_ig_business_sum_tender_BP_PC_MP_CT
    on p_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash = s_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash
   and p_ig_ig_business_sum_tender_BP_PC_MP_CT.s_ig_ig_business_sum_tender_BP_PC_MP_CT_id = s_ig_ig_business_sum_tender_BP_PC_MP_CT.s_ig_ig_business_sum_tender_BP_PC_MP_CT_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_ig_business_sum_tender_BP_PC_MP_CT
   where d_ig_ig_business_sum_tender_BP_PC_MP_CT.bk_hash in (select bk_hash from #p_ig_ig_business_sum_tender_BP_PC_MP_CT_insert)

  insert dbo.d_ig_ig_business_sum_tender_BP_PC_MP_CT(
             bk_hash,
             fact_cafe_tender_by_check_type_summary_key,
             event_dim_id,
             meal_period_dim_id,
             check_type_dim_id,
             credit_type_id,
             new_change_amount,
             new_tender_amount,
             posted_business_period_dim_id,
             profit_center_dim_id,
             tender_dim_id,
             tender_net_amount,
             tendered_business_period_dim_id,
             p_ig_ig_business_sum_tender_BP_PC_MP_CT_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_cafe_tender_by_check_type_summary_key,
         event_dim_id,
         meal_period_dim_id,
         check_type_dim_id,
         credit_type_id,
         new_change_amount,
         new_tender_amount,
         posted_business_period_dim_id,
         profit_center_dim_id,
         tender_dim_id,
         tender_net_amount,
         tendered_business_period_dim_id,
         p_ig_ig_business_sum_tender_BP_PC_MP_CT_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_ig_business_sum_tender_BP_PC_MP_CT)
--Done!
end
