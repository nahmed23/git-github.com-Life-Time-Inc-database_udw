CREATE PROC [dbo].[proc_d_ec_measurement_recordings] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_measurement_recordings)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_measurement_recordings_insert') is not null drop table #p_ec_measurement_recordings_insert
create table dbo.#p_ec_measurement_recordings_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_measurement_recordings.p_ec_measurement_recordings_id,
       p_ec_measurement_recordings.bk_hash
  from dbo.p_ec_measurement_recordings
 where p_ec_measurement_recordings.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_measurement_recordings.dv_batch_id > @max_dv_batch_id
        or p_ec_measurement_recordings.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_measurement_recordings.bk_hash,
       p_ec_measurement_recordings.measurement_recording_id measurement_recording_id,
       case when s_ec_measurement_recordings.active=1 then 'Y'
              else 'N' end active_flag,
       case when s_ec_measurement_recordings.certified=1 then 'Y'
              else 'N' end certified_flag,
       s_ec_measurement_recordings.created_by created_by,
       case when p_ec_measurement_recordings.bk_hash in ('-997', '-998', '-999') then p_ec_measurement_recordings.bk_hash   
           when s_ec_measurement_recordings.created_date is null then '-998'   
       	 else convert(char(8), s_ec_measurement_recordings.created_date, 112)  end created_dim_date_key,
       case when p_ec_measurement_recordings.bk_hash in ('-997', '-998', '-999') then p_ec_measurement_recordings.bk_hash   
           when l_ec_measurement_recordings.party_id is null then '-998'   
       	 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_measurement_recordings.party_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_ltfeb_ltf_user_identity_bk_hash,
       case when p_ec_measurement_recordings.bk_hash in ('-997', '-998', '-999') then p_ec_measurement_recordings.bk_hash   
           when l_ec_measurement_recordings.club_id is null then '-998'   
       	 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_ec_measurement_recordings.club_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       case when p_ec_measurement_recordings.bk_hash in ('-997', '-998', '-999') then p_ec_measurement_recordings.bk_hash   
           when s_ec_measurement_recordings.measure_date is null then '-998'   
       	 else convert(char(8), s_ec_measurement_recordings.measure_date, 112)  end measurement_dim_date_key,
       case when p_ec_measurement_recordings.bk_hash in ('-997','-998','-999') then p_ec_measurement_recordings.bk_hash
       when s_ec_measurement_recordings.measure_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_ec_measurement_recordings.measure_date,114), 1, 5),':','') end measurement_dim_time_key,
       isnull(s_ec_measurement_recordings.metadata, '') metadata,
       s_ec_measurement_recordings.modified_by modified_by,
       case when p_ec_measurement_recordings.bk_hash in ('-997', '-998', '-999') then p_ec_measurement_recordings.bk_hash   
           when s_ec_measurement_recordings.modified_date is null then '-998'   
       	 else convert(char(8), s_ec_measurement_recordings.modified_date, 112)  end modified_dim_date_key,
       isnull(s_ec_measurement_recordings.notes, '') notes,
       l_ec_measurement_recordings.party_id party_id,
       isnull(s_ec_measurement_recordings.source, '') source,
       isnull(l_ec_measurement_recordings.user_program_status_id, '') user_program_status_id,
       isnull(h_ec_measurement_recordings.dv_deleted,0) dv_deleted,
       p_ec_measurement_recordings.p_ec_measurement_recordings_id,
       p_ec_measurement_recordings.dv_batch_id,
       p_ec_measurement_recordings.dv_load_date_time,
       p_ec_measurement_recordings.dv_load_end_date_time
  from dbo.h_ec_measurement_recordings
  join dbo.p_ec_measurement_recordings
    on h_ec_measurement_recordings.bk_hash = p_ec_measurement_recordings.bk_hash
  join #p_ec_measurement_recordings_insert
    on p_ec_measurement_recordings.bk_hash = #p_ec_measurement_recordings_insert.bk_hash
   and p_ec_measurement_recordings.p_ec_measurement_recordings_id = #p_ec_measurement_recordings_insert.p_ec_measurement_recordings_id
  join dbo.l_ec_measurement_recordings
    on p_ec_measurement_recordings.bk_hash = l_ec_measurement_recordings.bk_hash
   and p_ec_measurement_recordings.l_ec_measurement_recordings_id = l_ec_measurement_recordings.l_ec_measurement_recordings_id
  join dbo.s_ec_measurement_recordings
    on p_ec_measurement_recordings.bk_hash = s_ec_measurement_recordings.bk_hash
   and p_ec_measurement_recordings.s_ec_measurement_recordings_id = s_ec_measurement_recordings.s_ec_measurement_recordings_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_measurement_recordings
   where d_ec_measurement_recordings.bk_hash in (select bk_hash from #p_ec_measurement_recordings_insert)

  insert dbo.d_ec_measurement_recordings(
             bk_hash,
             measurement_recording_id,
             active_flag,
             certified_flag,
             created_by,
             created_dim_date_key,
             d_ltfeb_ltf_user_identity_bk_hash,
             dim_club_key,
             measurement_dim_date_key,
             measurement_dim_time_key,
             metadata,
             modified_by,
             modified_dim_date_key,
             notes,
             party_id,
             source,
             user_program_status_id,
             deleted_flag,
             p_ec_measurement_recordings_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         measurement_recording_id,
         active_flag,
         certified_flag,
         created_by,
         created_dim_date_key,
         d_ltfeb_ltf_user_identity_bk_hash,
         dim_club_key,
         measurement_dim_date_key,
         measurement_dim_time_key,
         metadata,
         modified_by,
         modified_dim_date_key,
         notes,
         party_id,
         source,
         user_program_status_id,
         dv_deleted,
         p_ec_measurement_recordings_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_measurement_recordings)
--Done!
end
