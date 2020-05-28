CREATE PROC [dbo].[proc_d_mms_guest] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_guest)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_guest_insert') is not null drop table #p_mms_guest_insert
create table dbo.#p_mms_guest_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_guest.p_mms_guest_id,
       p_mms_guest.bk_hash
  from dbo.p_mms_guest
 where p_mms_guest.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_guest.dv_batch_id > @max_dv_batch_id
        or p_mms_guest.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_guest.bk_hash,
       p_mms_guest.bk_hash dim_club_guest_key,
       p_mms_guest.guest_id guest_id,
       isnull(s_mms_guest.first_name,'') first_name,
       isnull(s_mms_guest.last_name,'') last_name,
       p_mms_guest.p_mms_guest_id,
       p_mms_guest.dv_batch_id,
       p_mms_guest.dv_load_date_time,
       p_mms_guest.dv_load_end_date_time
  from dbo.p_mms_guest
  join #p_mms_guest_insert
    on p_mms_guest.bk_hash = #p_mms_guest_insert.bk_hash
   and p_mms_guest.p_mms_guest_id = #p_mms_guest_insert.p_mms_guest_id
  join dbo.l_mms_guest
    on p_mms_guest.bk_hash = l_mms_guest.bk_hash
   and p_mms_guest.l_mms_guest_id = l_mms_guest.l_mms_guest_id
  join dbo.s_mms_guest
    on p_mms_guest.bk_hash = s_mms_guest.bk_hash
   and p_mms_guest.s_mms_guest_id = s_mms_guest.s_mms_guest_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_guest
   where d_mms_guest.bk_hash in (select bk_hash from #p_mms_guest_insert)

  insert dbo.d_mms_guest(
             bk_hash,
             dim_club_guest_key,
             guest_id,
             first_name,
             last_name,
             p_mms_guest_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_club_guest_key,
         guest_id,
         first_name,
         last_name,
         p_mms_guest_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_guest)
--Done!
end
