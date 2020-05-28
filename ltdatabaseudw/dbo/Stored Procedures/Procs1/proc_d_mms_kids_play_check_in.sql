CREATE PROC [dbo].[proc_d_mms_kids_play_check_in] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_kids_play_check_in)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_kids_play_check_in_insert') is not null drop table #p_mms_kids_play_check_in_insert
create table dbo.#p_mms_kids_play_check_in_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_kids_play_check_in.p_mms_kids_play_check_in_id,
       p_mms_kids_play_check_in.bk_hash
  from dbo.p_mms_kids_play_check_in
 where p_mms_kids_play_check_in.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_kids_play_check_in.dv_batch_id > @max_dv_batch_id
        or p_mms_kids_play_check_in.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_kids_play_check_in.bk_hash,
       p_mms_kids_play_check_in.bk_hash fact_mms_kids_play_check_in_key,
       p_mms_kids_play_check_in.kids_play_check_in_id kids_play_check_in_id,
       case when p_mms_kids_play_check_in.bk_hash in ('-997','-998','-999')
          then p_mms_kids_play_check_in.bk_hash 
		when s_mms_kids_play_check_in.kids_play_check_in_date_time is null then '-998'
          else isnull(convert(varchar, s_mms_kids_play_check_in.kids_play_check_in_date_time, 112),'-998') end check_in_dim_date_key,
       case when p_mms_kids_play_check_in.bk_hash in ('-997','-998','-999')
          then p_mms_kids_play_check_in.bk_hash 
		when s_mms_kids_play_check_in.kids_play_check_in_date_time is null then '-998'
          else '1' + replace(substring(convert(varchar,s_mms_kids_play_check_in.kids_play_check_in_date_time,114),1,5),':','') end check_in_dim_time_key,
       l_mms_kids_play_check_in.child_center_usage_id child_center_usage_id,
       s_mms_kids_play_check_in.kids_play_check_in_date_time kids_play_check_in_date_time,
       s_mms_kids_play_check_in.kids_play_check_in_date_time_zone kids_play_check_in_date_time_zone,
       s_mms_kids_play_check_in.utckids_play_check_in_date_time utc_kids_play_check_in_date_time,
       p_mms_kids_play_check_in.p_mms_kids_play_check_in_id,
       p_mms_kids_play_check_in.dv_batch_id,
       p_mms_kids_play_check_in.dv_load_date_time,
       p_mms_kids_play_check_in.dv_load_end_date_time
  from dbo.h_mms_kids_play_check_in
  join dbo.p_mms_kids_play_check_in
    on h_mms_kids_play_check_in.bk_hash = p_mms_kids_play_check_in.bk_hash  join #p_mms_kids_play_check_in_insert
    on p_mms_kids_play_check_in.bk_hash = #p_mms_kids_play_check_in_insert.bk_hash
   and p_mms_kids_play_check_in.p_mms_kids_play_check_in_id = #p_mms_kids_play_check_in_insert.p_mms_kids_play_check_in_id
  join dbo.l_mms_kids_play_check_in
    on p_mms_kids_play_check_in.bk_hash = l_mms_kids_play_check_in.bk_hash
   and p_mms_kids_play_check_in.l_mms_kids_play_check_in_id = l_mms_kids_play_check_in.l_mms_kids_play_check_in_id
  join dbo.s_mms_kids_play_check_in
    on p_mms_kids_play_check_in.bk_hash = s_mms_kids_play_check_in.bk_hash
   and p_mms_kids_play_check_in.s_mms_kids_play_check_in_id = s_mms_kids_play_check_in.s_mms_kids_play_check_in_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_kids_play_check_in
   where d_mms_kids_play_check_in.bk_hash in (select bk_hash from #p_mms_kids_play_check_in_insert)

  insert dbo.d_mms_kids_play_check_in(
             bk_hash,
             fact_mms_kids_play_check_in_key,
             kids_play_check_in_id,
             check_in_dim_date_key,
             check_in_dim_time_key,
             child_center_usage_id,
             kids_play_check_in_date_time,
             kids_play_check_in_date_time_zone,
             utc_kids_play_check_in_date_time,
             p_mms_kids_play_check_in_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_kids_play_check_in_key,
         kids_play_check_in_id,
         check_in_dim_date_key,
         check_in_dim_time_key,
         child_center_usage_id,
         kids_play_check_in_date_time,
         kids_play_check_in_date_time_zone,
         utc_kids_play_check_in_date_time,
         p_mms_kids_play_check_in_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_kids_play_check_in)
--Done!
end
