CREATE PROC [dbo].[proc_d_ig_ig_dimension_business_period_dimension] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_ig_dimension_business_period_dimension)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_ig_dimension_business_period_dimension_insert') is not null drop table #p_ig_ig_dimension_business_period_dimension_insert
create table dbo.#p_ig_ig_dimension_business_period_dimension_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_dimension_business_period_dimension.p_ig_ig_dimension_business_period_dimension_id,
       p_ig_ig_dimension_business_period_dimension.bk_hash
  from dbo.p_ig_ig_dimension_business_period_dimension
 where p_ig_ig_dimension_business_period_dimension.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_ig_dimension_business_period_dimension.dv_batch_id > @max_dv_batch_id
        or p_ig_ig_dimension_business_period_dimension.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_ig_dimension_business_period_dimension.bk_hash,
       p_ig_ig_dimension_business_period_dimension.bk_hash dim_business_period_dim_key,
       case when p_ig_ig_dimension_business_period_dimension.bk_hash in ('-997', '-998', '-999') then p_ig_ig_dimension_business_period_dimension.bk_hash
             when p_ig_ig_dimension_business_period_dimension.business_period_dim_id is null then '-998'
       	  else p_ig_ig_dimension_business_period_dimension.business_period_dim_id end business_period_dim_id,
       case when p_ig_ig_dimension_business_period_dimension.bk_hash in ('-997', '-998', '-999') then p_ig_ig_dimension_business_period_dimension.bk_hash   
            when s_ig_ig_dimension_business_period_dimension.end_date_time is null then '-998'   
              else convert(varchar, s_ig_ig_dimension_business_period_dimension.end_date_time, 112) end business_period_end_dim_date_key,
       case when p_ig_ig_dimension_business_period_dimension.bk_hash in ('-997', '-998', '-999') then p_ig_ig_dimension_business_period_dimension.bk_hash
             when s_ig_ig_dimension_business_period_dimension.start_date_time is null then '-998'   
             else convert(varchar, s_ig_ig_dimension_business_period_dimension.start_date_time, 112) end business_period_start_dim_date_key,
       s_ig_ig_dimension_business_period_dimension.end_date_time end_date_time,
       case when p_ig_ig_dimension_business_period_dimension.bk_hash in ('-997', '-998', '-999') then p_ig_ig_dimension_business_period_dimension.bk_hash   
            when s_ig_ig_dimension_business_period_dimension.start_date_time is null then '-998'   
              else d_date.month_ending_dim_date_key end month_ending_dim_date_key,
       s_ig_ig_dimension_business_period_dimension.start_date_time start_date_time,
       p_ig_ig_dimension_business_period_dimension.p_ig_ig_dimension_business_period_dimension_id,
       p_ig_ig_dimension_business_period_dimension.dv_batch_id,
       p_ig_ig_dimension_business_period_dimension.dv_load_date_time,
       p_ig_ig_dimension_business_period_dimension.dv_load_end_date_time
  from dbo.p_ig_ig_dimension_business_period_dimension
  join #p_ig_ig_dimension_business_period_dimension_insert
    on p_ig_ig_dimension_business_period_dimension.bk_hash = #p_ig_ig_dimension_business_period_dimension_insert.bk_hash
   and p_ig_ig_dimension_business_period_dimension.p_ig_ig_dimension_business_period_dimension_id = #p_ig_ig_dimension_business_period_dimension_insert.p_ig_ig_dimension_business_period_dimension_id
  join dbo.l_ig_ig_dimension_business_period_dimension
    on p_ig_ig_dimension_business_period_dimension.bk_hash = l_ig_ig_dimension_business_period_dimension.bk_hash
   and p_ig_ig_dimension_business_period_dimension.l_ig_ig_dimension_business_period_dimension_id = l_ig_ig_dimension_business_period_dimension.l_ig_ig_dimension_business_period_dimension_id
  join dbo.s_ig_ig_dimension_business_period_dimension
    on p_ig_ig_dimension_business_period_dimension.bk_hash = s_ig_ig_dimension_business_period_dimension.bk_hash
   and p_ig_ig_dimension_business_period_dimension.s_ig_ig_dimension_business_period_dimension_id = s_ig_ig_dimension_business_period_dimension.s_ig_ig_dimension_business_period_dimension_id
 left join dim_date d_date      on CAST(s_ig_ig_dimension_business_period_dimension.start_date_time AS DATE) = d_date.calendar_date 

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_ig_dimension_business_period_dimension
   where d_ig_ig_dimension_business_period_dimension.bk_hash in (select bk_hash from #p_ig_ig_dimension_business_period_dimension_insert)

  insert dbo.d_ig_ig_dimension_business_period_dimension(
             bk_hash,
             dim_business_period_dim_key,
             business_period_dim_id,
             business_period_end_dim_date_key,
             business_period_start_dim_date_key,
             end_date_time,
             month_ending_dim_date_key,
             start_date_time,
             p_ig_ig_dimension_business_period_dimension_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_business_period_dim_key,
         business_period_dim_id,
         business_period_end_dim_date_key,
         business_period_start_dim_date_key,
         end_date_time,
         month_ending_dim_date_key,
         start_date_time,
         p_ig_ig_dimension_business_period_dimension_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_ig_dimension_business_period_dimension)
--Done!
end
