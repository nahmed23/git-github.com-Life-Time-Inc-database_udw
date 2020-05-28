CREATE PROC [dbo].[proc_d_ig_it_trn_business_day_dates] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_business_day_dates)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_trn_business_day_dates_insert') is not null drop table #p_ig_it_trn_business_day_dates_insert
create table dbo.#p_ig_it_trn_business_day_dates_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_business_day_dates.p_ig_it_trn_business_day_dates_id,
       p_ig_it_trn_business_day_dates.bk_hash
  from dbo.p_ig_it_trn_business_day_dates
 where p_ig_it_trn_business_day_dates.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_trn_business_day_dates.dv_batch_id > @max_dv_batch_id
        or p_ig_it_trn_business_day_dates.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_business_day_dates.bk_hash,
       p_ig_it_trn_business_day_dates.bk_hash dim_cafe_business_day_dates_key,
       p_ig_it_trn_business_day_dates.bus_day_id bus_day_id,
       case when p_ig_it_trn_business_day_dates.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_business_day_dates.bk_hash 
     when s_ig_it_trn_business_day_dates.bd_end_dttime is null then '-998'    
   else convert(varchar, s_ig_it_trn_business_day_dates.bd_end_dttime, 112) end business_day_end_dim_date_key,
       case when p_ig_it_trn_business_day_dates.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_business_day_dates.bk_hash 
     when s_ig_it_trn_business_day_dates.bd_end_dttime is null then '-998'
   else '1' + replace(substring(convert(varchar,s_ig_it_trn_business_day_dates.bd_end_dttime,114), 1, 5),':','') end business_day_end_dim_time_key,
       case when p_ig_it_trn_business_day_dates.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_business_day_dates.bk_hash 
     when s_ig_it_trn_business_day_dates.bd_start_dttime is null then '-998'    
   else convert(varchar, s_ig_it_trn_business_day_dates.bd_start_dttime, 112) end business_day_start_dim_date_key,
       case when p_ig_it_trn_business_day_dates.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_business_day_dates.bk_hash 
     when s_ig_it_trn_business_day_dates.bd_start_dttime is null then '-998'
   else '1' + replace(substring(convert(varchar,s_ig_it_trn_business_day_dates.bd_start_dttime,114), 1, 5),':','') end business_day_start_dim_time_key,
       isnull(h_ig_it_trn_business_day_dates.dv_deleted,0) dv_deleted,
       p_ig_it_trn_business_day_dates.p_ig_it_trn_business_day_dates_id,
       p_ig_it_trn_business_day_dates.dv_batch_id,
       p_ig_it_trn_business_day_dates.dv_load_date_time,
       p_ig_it_trn_business_day_dates.dv_load_end_date_time
  from dbo.h_ig_it_trn_business_day_dates
  join dbo.p_ig_it_trn_business_day_dates
    on h_ig_it_trn_business_day_dates.bk_hash = p_ig_it_trn_business_day_dates.bk_hash
  join #p_ig_it_trn_business_day_dates_insert
    on p_ig_it_trn_business_day_dates.bk_hash = #p_ig_it_trn_business_day_dates_insert.bk_hash
   and p_ig_it_trn_business_day_dates.p_ig_it_trn_business_day_dates_id = #p_ig_it_trn_business_day_dates_insert.p_ig_it_trn_business_day_dates_id
  join dbo.s_ig_it_trn_business_day_dates
    on p_ig_it_trn_business_day_dates.bk_hash = s_ig_it_trn_business_day_dates.bk_hash
   and p_ig_it_trn_business_day_dates.s_ig_it_trn_business_day_dates_id = s_ig_it_trn_business_day_dates.s_ig_it_trn_business_day_dates_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_trn_business_day_dates
   where d_ig_it_trn_business_day_dates.bk_hash in (select bk_hash from #p_ig_it_trn_business_day_dates_insert)

  insert dbo.d_ig_it_trn_business_day_dates(
             bk_hash,
             dim_cafe_business_day_dates_key,
             bus_day_id,
             business_day_end_dim_date_key,
             business_day_end_dim_time_key,
             business_day_start_dim_date_key,
             business_day_start_dim_time_key,
             deleted_flag,
             p_ig_it_trn_business_day_dates_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_cafe_business_day_dates_key,
         bus_day_id,
         business_day_end_dim_date_key,
         business_day_end_dim_time_key,
         business_day_start_dim_date_key,
         business_day_start_dim_time_key,
         dv_deleted,
         p_ig_it_trn_business_day_dates_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_business_day_dates)
--Done!
end
