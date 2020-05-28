CREATE PROC [dbo].[proc_d_fitmetrix_api_activities] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_activities)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_fitmetrix_api_activities_insert') is not null drop table #p_fitmetrix_api_activities_insert
create table dbo.#p_fitmetrix_api_activities_insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_activities.p_fitmetrix_api_activities_id,
       p_fitmetrix_api_activities.bk_hash
  from dbo.p_fitmetrix_api_activities
 where p_fitmetrix_api_activities.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_fitmetrix_api_activities.dv_batch_id > @max_dv_batch_id
        or p_fitmetrix_api_activities.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_activities.bk_hash,
       p_fitmetrix_api_activities.bk_hash dim_fitmetrix_activity_key,
       p_fitmetrix_api_activities.activity_id activity_id,
       case when p_fitmetrix_api_activities.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_activities.bk_hash
      when s_fitmetrix_api_activities.activity_added is null then '-998'
 else convert(varchar, CAST(activity_added as DATETIMEOFFSET), 112)
end activity_added_dim_date_key,
       isnull(s_fitmetrix_api_activities.activity_name, '') activity_name,
       l_fitmetrix_api_activities.activity_type_id activity_type_id,
       l_fitmetrix_api_activities.external_id external_id,
       case when s_fitmetrix_api_activities.is_deleted='true' then 'Y'   else 'N'  end is_deleted_flag,
       s_fitmetrix_api_activities.position position,
       h_fitmetrix_api_activities.dv_deleted,
       p_fitmetrix_api_activities.p_fitmetrix_api_activities_id,
       p_fitmetrix_api_activities.dv_batch_id,
       p_fitmetrix_api_activities.dv_load_date_time,
       p_fitmetrix_api_activities.dv_load_end_date_time
  from dbo.h_fitmetrix_api_activities
  join dbo.p_fitmetrix_api_activities
    on h_fitmetrix_api_activities.bk_hash = p_fitmetrix_api_activities.bk_hash  join #p_fitmetrix_api_activities_insert
    on p_fitmetrix_api_activities.bk_hash = #p_fitmetrix_api_activities_insert.bk_hash
   and p_fitmetrix_api_activities.p_fitmetrix_api_activities_id = #p_fitmetrix_api_activities_insert.p_fitmetrix_api_activities_id
  join dbo.l_fitmetrix_api_activities
    on p_fitmetrix_api_activities.bk_hash = l_fitmetrix_api_activities.bk_hash
   and p_fitmetrix_api_activities.l_fitmetrix_api_activities_id = l_fitmetrix_api_activities.l_fitmetrix_api_activities_id
  join dbo.s_fitmetrix_api_activities
    on p_fitmetrix_api_activities.bk_hash = s_fitmetrix_api_activities.bk_hash
   and p_fitmetrix_api_activities.s_fitmetrix_api_activities_id = s_fitmetrix_api_activities.s_fitmetrix_api_activities_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_fitmetrix_api_activities
   where d_fitmetrix_api_activities.bk_hash in (select bk_hash from #p_fitmetrix_api_activities_insert)

  insert dbo.d_fitmetrix_api_activities(
             bk_hash,
             dim_fitmetrix_activity_key,
             activity_id,
             activity_added_dim_date_key,
             activity_name,
             activity_type_id,
             external_id,
             is_deleted_flag,
             position,
             deleted_flag,
             p_fitmetrix_api_activities_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_fitmetrix_activity_key,
         activity_id,
         activity_added_dim_date_key,
         activity_name,
         activity_type_id,
         external_id,
         is_deleted_flag,
         position,
         dv_deleted,
         p_fitmetrix_api_activities_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_activities)
--Done!
end
