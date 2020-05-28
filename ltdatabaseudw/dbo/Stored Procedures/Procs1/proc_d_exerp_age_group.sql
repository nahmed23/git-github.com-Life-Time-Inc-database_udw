CREATE PROC [dbo].[proc_d_exerp_age_group] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_age_group)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_age_group_insert') is not null drop table #p_exerp_age_group_insert
create table dbo.#p_exerp_age_group_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_age_group.p_exerp_age_group_id,
       p_exerp_age_group.bk_hash
  from dbo.p_exerp_age_group
 where p_exerp_age_group.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_age_group.dv_batch_id > @max_dv_batch_id
        or p_exerp_age_group.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_age_group.bk_hash,
       p_exerp_age_group.bk_hash dim_exerp_age_group_key,
       p_exerp_age_group.age_group_id age_group_id,
       s_exerp_age_group.name age_group_name,
       s_exerp_age_group.state age_group_state,
       s_exerp_age_group.external_id external_id,
       s_exerp_age_group.maximum_age maximum_age,
       s_exerp_age_group.minimum_age minimum_age,
       s_exerp_age_group.strict_age_limit strict_age_limit,
       isnull(h_exerp_age_group.dv_deleted,0) dv_deleted,
       p_exerp_age_group.p_exerp_age_group_id,
       p_exerp_age_group.dv_batch_id,
       p_exerp_age_group.dv_load_date_time,
       p_exerp_age_group.dv_load_end_date_time
  from dbo.h_exerp_age_group
  join dbo.p_exerp_age_group
    on h_exerp_age_group.bk_hash = p_exerp_age_group.bk_hash
  join #p_exerp_age_group_insert
    on p_exerp_age_group.bk_hash = #p_exerp_age_group_insert.bk_hash
   and p_exerp_age_group.p_exerp_age_group_id = #p_exerp_age_group_insert.p_exerp_age_group_id
  join dbo.s_exerp_age_group
    on p_exerp_age_group.bk_hash = s_exerp_age_group.bk_hash
   and p_exerp_age_group.s_exerp_age_group_id = s_exerp_age_group.s_exerp_age_group_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_age_group
   where d_exerp_age_group.bk_hash in (select bk_hash from #p_exerp_age_group_insert)

  insert dbo.d_exerp_age_group(
             bk_hash,
             dim_exerp_age_group_key,
             age_group_id,
             age_group_name,
             age_group_state,
             external_id,
             maximum_age,
             minimum_age,
             strict_age_limit,
             deleted_flag,
             p_exerp_age_group_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_exerp_age_group_key,
         age_group_id,
         age_group_name,
         age_group_state,
         external_id,
         maximum_age,
         minimum_age,
         strict_age_limit,
         dv_deleted,
         p_exerp_age_group_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_age_group)
--Done!
end
