CREATE PROC [dbo].[proc_d_ec_measurements] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_measurements)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_measurements_insert') is not null drop table #p_ec_measurements_insert
create table dbo.#p_ec_measurements_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_measurements.p_ec_measurements_id,
       p_ec_measurements.bk_hash
  from dbo.p_ec_measurements
 where p_ec_measurements.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_measurements.dv_batch_id > @max_dv_batch_id
        or p_ec_measurements.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_measurements.bk_hash,
       p_ec_measurements.bk_hash fact_trainerize_measurement_key,
       p_ec_measurements.measurement_id measurement_id,
       case when p_ec_measurements.bk_hash in ('-997', '-998', '-999') then p_ec_measurements.bk_hash      
        when l_ec_measurements.measurement_recording_id is null then '-998'   
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_ec_measurements.measurement_recording_id as varchar(36)),'z#@$k%&P'))),2) end d_ec_measurement_recordings_bk_hash,
       case when p_ec_measurements.bk_hash in ('-997','-998', '-999') then p_ec_measurements.bk_hash      
        when l_ec_measurements.measures_id is null then '-998'   
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_ec_measurements.measures_id as varchar(36)),'z#@$k%&P'))),2) end dim_trainerize_measure_key,
       isnull(s_ec_measurements.measure_value, '') measure_value,
       isnull(s_ec_measurements.unit, '') unit,
       isnull(h_ec_measurements.dv_deleted,0) dv_deleted,
       p_ec_measurements.p_ec_measurements_id,
       p_ec_measurements.dv_batch_id,
       p_ec_measurements.dv_load_date_time,
       p_ec_measurements.dv_load_end_date_time
  from dbo.h_ec_measurements
  join dbo.p_ec_measurements
    on h_ec_measurements.bk_hash = p_ec_measurements.bk_hash
  join #p_ec_measurements_insert
    on p_ec_measurements.bk_hash = #p_ec_measurements_insert.bk_hash
   and p_ec_measurements.p_ec_measurements_id = #p_ec_measurements_insert.p_ec_measurements_id
  join dbo.l_ec_measurements
    on p_ec_measurements.bk_hash = l_ec_measurements.bk_hash
   and p_ec_measurements.l_ec_measurements_id = l_ec_measurements.l_ec_measurements_id
  join dbo.s_ec_measurements
    on p_ec_measurements.bk_hash = s_ec_measurements.bk_hash
   and p_ec_measurements.s_ec_measurements_id = s_ec_measurements.s_ec_measurements_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_measurements
   where d_ec_measurements.bk_hash in (select bk_hash from #p_ec_measurements_insert)

  insert dbo.d_ec_measurements(
             bk_hash,
             fact_trainerize_measurement_key,
             measurement_id,
             d_ec_measurement_recordings_bk_hash,
             dim_trainerize_measure_key,
             measure_value,
             unit,
             deleted_flag,
             p_ec_measurements_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_trainerize_measurement_key,
         measurement_id,
         d_ec_measurement_recordings_bk_hash,
         dim_trainerize_measure_key,
         measure_value,
         unit,
         dv_deleted,
         p_ec_measurements_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_measurements)
--Done!
end
