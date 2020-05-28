CREATE PROC [dbo].[proc_d_affinitech_camera_count] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_affinitech_camera_count)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_affinitech_camera_count_insert') is not null drop table #p_affinitech_camera_count_insert
create table dbo.#p_affinitech_camera_count_insert with(distribution=hash(bk_hash), location=user_db) as
select p_affinitech_camera_count.p_affinitech_camera_count_id,
       p_affinitech_camera_count.bk_hash
  from dbo.p_affinitech_camera_count
 where p_affinitech_camera_count.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_affinitech_camera_count.dv_batch_id > @max_dv_batch_id
        or p_affinitech_camera_count.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_affinitech_camera_count.bk_hash,
       p_affinitech_camera_count.Door_Description Door_Description,
       p_affinitech_camera_count.Start_Range Start_Range,
       p_affinitech_camera_count.Source_IP Source_IP,
       s_affinitech_camera_count.Cumulative_Enters Cumulative_Enters,
       s_affinitech_camera_count.Cumulative_Exits Cumulative_Exits,
       l_affinitech_camera_count.Division_ID Division_ID,
       l_affinitech_camera_count.Door_ID Door_ID,
       s_affinitech_camera_count.Door_Type Door_Type,
       s_affinitech_camera_count.Enters Enters,
       s_affinitech_camera_count.Event_Type Event_Type,
       s_affinitech_camera_count.Exits Exits,
       l_affinitech_camera_count.Site_ID Site_ID,
       case when p_affinitech_camera_count.bk_hash in('-997', '-998', '-999') then p_affinitech_camera_count.bk_hash
           when s_affinitech_camera_count.Start_Range is null then '-998'
        else convert(varchar, s_affinitech_camera_count.Start_Range, 112)    end Start_Range_dim_date_key,
       case when p_affinitech_camera_count.bk_hash in ('-997','-998','-999') then p_affinitech_camera_count.bk_hash
       when s_affinitech_camera_count.Start_Range is null then '-998'
       else '1' + replace(substring(convert(varchar,s_affinitech_camera_count.Start_Range,114), 1, 5),':','') end Start_Range_dim_time_key,
       isnull(h_affinitech_camera_count.dv_deleted,0) dv_deleted,
       p_affinitech_camera_count.p_affinitech_camera_count_id,
       p_affinitech_camera_count.dv_batch_id,
       p_affinitech_camera_count.dv_load_date_time,
       p_affinitech_camera_count.dv_load_end_date_time
  from dbo.h_affinitech_camera_count
  join dbo.p_affinitech_camera_count
    on h_affinitech_camera_count.bk_hash = p_affinitech_camera_count.bk_hash
  join #p_affinitech_camera_count_insert
    on p_affinitech_camera_count.bk_hash = #p_affinitech_camera_count_insert.bk_hash
   and p_affinitech_camera_count.p_affinitech_camera_count_id = #p_affinitech_camera_count_insert.p_affinitech_camera_count_id
  join dbo.l_affinitech_camera_count
    on p_affinitech_camera_count.bk_hash = l_affinitech_camera_count.bk_hash
   and p_affinitech_camera_count.l_affinitech_camera_count_id = l_affinitech_camera_count.l_affinitech_camera_count_id
  join dbo.s_affinitech_camera_count
    on p_affinitech_camera_count.bk_hash = s_affinitech_camera_count.bk_hash
   and p_affinitech_camera_count.s_affinitech_camera_count_id = s_affinitech_camera_count.s_affinitech_camera_count_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_affinitech_camera_count
   where d_affinitech_camera_count.bk_hash in (select bk_hash from #p_affinitech_camera_count_insert)

  insert dbo.d_affinitech_camera_count(
             bk_hash,
             Door_Description,
             Start_Range,
             Source_IP,
             Cumulative_Enters,
             Cumulative_Exits,
             Division_ID,
             Door_ID,
             Door_Type,
             Enters,
             Event_Type,
             Exits,
             Site_ID,
             Start_Range_dim_date_key,
             Start_Range_dim_time_key,
             deleted_flag,
             p_affinitech_camera_count_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         Door_Description,
         Start_Range,
         Source_IP,
         Cumulative_Enters,
         Cumulative_Exits,
         Division_ID,
         Door_ID,
         Door_Type,
         Enters,
         Event_Type,
         Exits,
         Site_ID,
         Start_Range_dim_date_key,
         Start_Range_dim_time_key,
         dv_deleted,
         p_affinitech_camera_count_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_affinitech_camera_count)
--Done!
end
