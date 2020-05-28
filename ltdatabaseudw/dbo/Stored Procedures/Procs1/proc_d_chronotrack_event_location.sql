CREATE PROC [dbo].[proc_d_chronotrack_event_location] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_chronotrack_event_location)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_chronotrack_event_location_insert') is not null drop table #p_chronotrack_event_location_insert
create table dbo.#p_chronotrack_event_location_insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_event_location.p_chronotrack_event_location_id,
       p_chronotrack_event_location.bk_hash
  from dbo.p_chronotrack_event_location
 where p_chronotrack_event_location.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_chronotrack_event_location.dv_batch_id > @max_dv_batch_id
        or p_chronotrack_event_location.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_event_location.bk_hash,
       p_chronotrack_event_location.event_location_id event_location_id,
       s_chronotrack_event_location.create_time create_time,
       case when p_chronotrack_event_location.bk_hash in('-997', '-998', '-999') then p_chronotrack_event_location.bk_hash
           when l_chronotrack_event_location.event_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_event_location.event_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end d_chronotrack_event_bk_hash,
       case when p_chronotrack_event_location.bk_hash in('-997', '-998', '-999') then p_chronotrack_event_location.bk_hash
           when l_chronotrack_event_location.location_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_chronotrack_event_location.location_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end d_chronotrack_location_bk_hash,
       l_chronotrack_event_location.event_id event_id,
       l_chronotrack_event_location.location_id location_id,
       s_chronotrack_event_location.modified_time modified_time,
       isnull(h_chronotrack_event_location.dv_deleted,0) dv_deleted,
       p_chronotrack_event_location.p_chronotrack_event_location_id,
       p_chronotrack_event_location.dv_batch_id,
       p_chronotrack_event_location.dv_load_date_time,
       p_chronotrack_event_location.dv_load_end_date_time
  from dbo.h_chronotrack_event_location
  join dbo.p_chronotrack_event_location
    on h_chronotrack_event_location.bk_hash = p_chronotrack_event_location.bk_hash
  join #p_chronotrack_event_location_insert
    on p_chronotrack_event_location.bk_hash = #p_chronotrack_event_location_insert.bk_hash
   and p_chronotrack_event_location.p_chronotrack_event_location_id = #p_chronotrack_event_location_insert.p_chronotrack_event_location_id
  join dbo.l_chronotrack_event_location
    on p_chronotrack_event_location.bk_hash = l_chronotrack_event_location.bk_hash
   and p_chronotrack_event_location.l_chronotrack_event_location_id = l_chronotrack_event_location.l_chronotrack_event_location_id
  join dbo.s_chronotrack_event_location
    on p_chronotrack_event_location.bk_hash = s_chronotrack_event_location.bk_hash
   and p_chronotrack_event_location.s_chronotrack_event_location_id = s_chronotrack_event_location.s_chronotrack_event_location_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_chronotrack_event_location
   where d_chronotrack_event_location.bk_hash in (select bk_hash from #p_chronotrack_event_location_insert)

  insert dbo.d_chronotrack_event_location(
             bk_hash,
             event_location_id,
             create_time,
             d_chronotrack_event_bk_hash,
             d_chronotrack_location_bk_hash,
             event_id,
             location_id,
             modified_time,
             deleted_flag,
             p_chronotrack_event_location_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         event_location_id,
         create_time,
         d_chronotrack_event_bk_hash,
         d_chronotrack_location_bk_hash,
         event_id,
         location_id,
         modified_time,
         dv_deleted,
         p_chronotrack_event_location_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_chronotrack_event_location)
--Done!
end
