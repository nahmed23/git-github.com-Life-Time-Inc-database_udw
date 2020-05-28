CREATE PROC [dbo].[proc_d_affinitech_cameras] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_affinitech_cameras)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_affinitech_cameras_insert') is not null drop table #p_affinitech_cameras_insert
create table dbo.#p_affinitech_cameras_insert with(distribution=hash(bk_hash), location=user_db) as
select p_affinitech_cameras.p_affinitech_cameras_id,
       p_affinitech_cameras.bk_hash
  from dbo.p_affinitech_cameras
 where p_affinitech_cameras.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_affinitech_cameras.dv_batch_id > @max_dv_batch_id
        or p_affinitech_cameras.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_affinitech_cameras.bk_hash,
       p_affinitech_cameras.cam_id cam_id,
       s_affinitech_cameras.cam_club_it cam_club_it,
       case when l_affinitech_cameras.cam_club is null then '-998'
       when isnumeric(l_affinitech_cameras.cam_club) =0  then '-998'
       else convert(varchar(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_affinitech_cameras.cam_club as varchar(8000)),'z#@$k%&P'))),2) end cam_dim_club_key,
       s_affinitech_cameras.cam_inverted cam_inverted,
       s_affinitech_cameras.cam_ip cam_ip,
       s_affinitech_cameras.cam_name cam_name,
       s_affinitech_cameras.studio studio,
       isnull(h_affinitech_cameras.dv_deleted,0) dv_deleted,
       p_affinitech_cameras.p_affinitech_cameras_id,
       p_affinitech_cameras.dv_batch_id,
       p_affinitech_cameras.dv_load_date_time,
       p_affinitech_cameras.dv_load_end_date_time
  from dbo.h_affinitech_cameras
  join dbo.p_affinitech_cameras
    on h_affinitech_cameras.bk_hash = p_affinitech_cameras.bk_hash
  join #p_affinitech_cameras_insert
    on p_affinitech_cameras.bk_hash = #p_affinitech_cameras_insert.bk_hash
   and p_affinitech_cameras.p_affinitech_cameras_id = #p_affinitech_cameras_insert.p_affinitech_cameras_id
  join dbo.l_affinitech_cameras
    on p_affinitech_cameras.bk_hash = l_affinitech_cameras.bk_hash
   and p_affinitech_cameras.l_affinitech_cameras_id = l_affinitech_cameras.l_affinitech_cameras_id
  join dbo.s_affinitech_cameras
    on p_affinitech_cameras.bk_hash = s_affinitech_cameras.bk_hash
   and p_affinitech_cameras.s_affinitech_cameras_id = s_affinitech_cameras.s_affinitech_cameras_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_affinitech_cameras
   where d_affinitech_cameras.bk_hash in (select bk_hash from #p_affinitech_cameras_insert)

  insert dbo.d_affinitech_cameras(
             bk_hash,
             cam_id,
             cam_club_it,
             cam_dim_club_key,
             cam_inverted,
             cam_ip,
             cam_name,
             studio,
             deleted_flag,
             p_affinitech_cameras_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         cam_id,
         cam_club_it,
         cam_dim_club_key,
         cam_inverted,
         cam_ip,
         cam_name,
         studio,
         dv_deleted,
         p_affinitech_cameras_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_affinitech_cameras)
--Done!
end
