CREATE PROC [dbo].[proc_d_mms_club_activity_area_member_usage] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_club_activity_area_member_usage)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_club_activity_area_member_usage_insert') is not null drop table #p_mms_club_activity_area_member_usage_insert
create table dbo.#p_mms_club_activity_area_member_usage_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_activity_area_member_usage.p_mms_club_activity_area_member_usage_id,
       p_mms_club_activity_area_member_usage.bk_hash
  from dbo.p_mms_club_activity_area_member_usage
 where p_mms_club_activity_area_member_usage.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_club_activity_area_member_usage.dv_batch_id > @max_dv_batch_id
        or p_mms_club_activity_area_member_usage.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_club_activity_area_member_usage.bk_hash,
       p_mms_club_activity_area_member_usage.bk_hash fact_mms_club_activity_area_member_usage_key,
       p_mms_club_activity_area_member_usage.club_activity_area_member_usage_id club_activity_area_member_usage_id,
       case when p_mms_club_activity_area_member_usage.bk_hash in ('-997','-998','-999') then p_mms_club_activity_area_member_usage.bk_hash
            when l_mms_club_activity_area_member_usage.val_activity_area_id is null then '-998'
            else 'r_mms_val_activity_area_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club_activity_area_member_usage.val_activity_area_id as bigint) as varchar(500)),'z#@$k%&P'))),2)
       end club_activity_area_member_usage_dim_description_key,
       case when p_mms_club_activity_area_member_usage.bk_hash in ('-997','-998','-999') then p_mms_club_activity_area_member_usage.bk_hash
            when l_mms_club_activity_area_member_usage.club_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club_activity_area_member_usage.club_id as int) as varchar(500)),'z#@$k%&P'))),2)
       	end dim_club_key,
       case when  p_mms_club_activity_area_member_usage.bk_hash in ('-997','-998','-999') then p_mms_club_activity_area_member_usage.bk_hash
            when l_mms_club_activity_area_member_usage.member_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_club_activity_area_member_usage.member_id as int) as varchar(500)),'z#@$k%&P'))),2)
       	end dim_mms_member_key,
       s_mms_club_activity_area_member_usage.inserted_date_time inserted_date_time,
       s_mms_club_activity_area_member_usage.updated_date_time updated_date_time,
       isnull(l_mms_club_activity_area_member_usage.val_activity_area_id,'-998') val_activity_area_id,
       isnull(h_mms_club_activity_area_member_usage.dv_deleted,0) dv_deleted,
       p_mms_club_activity_area_member_usage.p_mms_club_activity_area_member_usage_id,
       p_mms_club_activity_area_member_usage.dv_batch_id,
       p_mms_club_activity_area_member_usage.dv_load_date_time,
       p_mms_club_activity_area_member_usage.dv_load_end_date_time
  from dbo.h_mms_club_activity_area_member_usage
  join dbo.p_mms_club_activity_area_member_usage
    on h_mms_club_activity_area_member_usage.bk_hash = p_mms_club_activity_area_member_usage.bk_hash
  join #p_mms_club_activity_area_member_usage_insert
    on p_mms_club_activity_area_member_usage.bk_hash = #p_mms_club_activity_area_member_usage_insert.bk_hash
   and p_mms_club_activity_area_member_usage.p_mms_club_activity_area_member_usage_id = #p_mms_club_activity_area_member_usage_insert.p_mms_club_activity_area_member_usage_id
  join dbo.l_mms_club_activity_area_member_usage
    on p_mms_club_activity_area_member_usage.bk_hash = l_mms_club_activity_area_member_usage.bk_hash
   and p_mms_club_activity_area_member_usage.l_mms_club_activity_area_member_usage_id = l_mms_club_activity_area_member_usage.l_mms_club_activity_area_member_usage_id
  join dbo.s_mms_club_activity_area_member_usage
    on p_mms_club_activity_area_member_usage.bk_hash = s_mms_club_activity_area_member_usage.bk_hash
   and p_mms_club_activity_area_member_usage.s_mms_club_activity_area_member_usage_id = s_mms_club_activity_area_member_usage.s_mms_club_activity_area_member_usage_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_club_activity_area_member_usage
   where d_mms_club_activity_area_member_usage.bk_hash in (select bk_hash from #p_mms_club_activity_area_member_usage_insert)

  insert dbo.d_mms_club_activity_area_member_usage(
             bk_hash,
             fact_mms_club_activity_area_member_usage_key,
             club_activity_area_member_usage_id,
             club_activity_area_member_usage_dim_description_key,
             dim_club_key,
             dim_mms_member_key,
             inserted_date_time,
             updated_date_time,
             val_activity_area_id,
             deleted_flag,
             p_mms_club_activity_area_member_usage_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_club_activity_area_member_usage_key,
         club_activity_area_member_usage_id,
         club_activity_area_member_usage_dim_description_key,
         dim_club_key,
         dim_mms_member_key,
         inserted_date_time,
         updated_date_time,
         val_activity_area_id,
         dv_deleted,
         p_mms_club_activity_area_member_usage_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_club_activity_area_member_usage)
--Done!
end
