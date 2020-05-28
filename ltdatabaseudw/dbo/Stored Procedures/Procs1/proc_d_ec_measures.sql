CREATE PROC [dbo].[proc_d_ec_measures] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ec_measures)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ec_measures_insert') is not null drop table #p_ec_measures_insert
create table dbo.#p_ec_measures_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_measures.p_ec_measures_id,
       p_ec_measures.bk_hash
  from dbo.p_ec_measures
 where p_ec_measures.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ec_measures.dv_batch_id > @max_dv_batch_id
        or p_ec_measures.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ec_measures.bk_hash,
       p_ec_measures.bk_hash dim_trainerize_measure_key,
       p_ec_measures.measures_id measures_id,
       s_ec_measures.created_by created_by,
       case when p_ec_measures.bk_hash in ('-997', '-998', '-999') then p_ec_measures.bk_hash   
           when s_ec_measures.created_date is null then '-998'   
       	 else convert(char(8), s_ec_measures.created_date, 112)  end created_dim_date_key,
       isnull(s_ec_measures.description, '') description,
       isnull(s_ec_measures.diagonostic_range_female, '') diagonostic_range_female,
       isnull(s_ec_measures.diagonostic_range_male, '') diagonostic_range_male,
       isnull(s_ec_measures.extended_metadata, '') extended_metadata,
       isnull(s_ec_measures.gender, '') gender,
       s_ec_measures.measure_value_type measure_value_type,
       isnull(s_ec_measures.measurement_instructions_location, '') measurement_instructions_location,
       s_ec_measures.measurement_type measurement_type,
       s_ec_measures.modified_by modified_by,
       case when p_ec_measures.bk_hash in ('-997', '-998', '-999') then p_ec_measures.bk_hash   
           when s_ec_measures.modified_date is null then '-998'   
       	 else convert(char(8), s_ec_measures.modified_date, 112)  end modified_dim_date_key,
       isnull(s_ec_measures.optimum_range_female, '') optimum_range_female,
       isnull(s_ec_measures.optimum_range_male, '') optimum_range_male,
       isnull(s_ec_measures.slug, '') slug,
       isnull(s_ec_measures.tags, '') tags,
       isnull(s_ec_measures.title, '') title,
       isnull(s_ec_measures.unit, '') unit,
       isnull(h_ec_measures.dv_deleted,0) dv_deleted,
       p_ec_measures.p_ec_measures_id,
       p_ec_measures.dv_batch_id,
       p_ec_measures.dv_load_date_time,
       p_ec_measures.dv_load_end_date_time
  from dbo.h_ec_measures
  join dbo.p_ec_measures
    on h_ec_measures.bk_hash = p_ec_measures.bk_hash
  join #p_ec_measures_insert
    on p_ec_measures.bk_hash = #p_ec_measures_insert.bk_hash
   and p_ec_measures.p_ec_measures_id = #p_ec_measures_insert.p_ec_measures_id
  join dbo.s_ec_measures
    on p_ec_measures.bk_hash = s_ec_measures.bk_hash
   and p_ec_measures.s_ec_measures_id = s_ec_measures.s_ec_measures_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ec_measures
   where d_ec_measures.bk_hash in (select bk_hash from #p_ec_measures_insert)

  insert dbo.d_ec_measures(
             bk_hash,
             dim_trainerize_measure_key,
             measures_id,
             created_by,
             created_dim_date_key,
             description,
             diagonostic_range_female,
             diagonostic_range_male,
             extended_metadata,
             gender,
             measure_value_type,
             measurement_instructions_location,
             measurement_type,
             modified_by,
             modified_dim_date_key,
             optimum_range_female,
             optimum_range_male,
             slug,
             tags,
             title,
             unit,
             deleted_flag,
             p_ec_measures_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_trainerize_measure_key,
         measures_id,
         created_by,
         created_dim_date_key,
         description,
         diagonostic_range_female,
         diagonostic_range_male,
         extended_metadata,
         gender,
         measure_value_type,
         measurement_instructions_location,
         measurement_type,
         modified_by,
         modified_dim_date_key,
         optimum_range_female,
         optimum_range_male,
         slug,
         tags,
         title,
         unit,
         dv_deleted,
         p_ec_measures_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ec_measures)
--Done!
end
