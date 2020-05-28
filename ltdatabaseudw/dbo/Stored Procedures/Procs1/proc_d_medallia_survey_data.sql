CREATE PROC [dbo].[proc_d_medallia_survey_data] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_medallia_survey_data)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_medallia_survey_data_insert') is not null drop table #p_medallia_survey_data_insert
create table dbo.#p_medallia_survey_data_insert with(distribution=hash(bk_hash), location=user_db) as
select p_medallia_survey_data.p_medallia_survey_data_id,
       p_medallia_survey_data.bk_hash
  from dbo.p_medallia_survey_data
 where p_medallia_survey_data.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_medallia_survey_data.dv_batch_id > @max_dv_batch_id
        or p_medallia_survey_data.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_medallia_survey_data.bk_hash,
       p_medallia_survey_data.bk_hash d_medallia_survey_data_key,
       s_medallia_survey_data.survey_id survey_id,
       s_medallia_survey_data.field_name field_name,
       s_medallia_survey_data.field_value field_value,
       s_medallia_survey_data.file_name file_name,
       isnull(h_medallia_survey_data.dv_deleted,0) dv_deleted,
       p_medallia_survey_data.p_medallia_survey_data_id,
       p_medallia_survey_data.dv_batch_id,
       p_medallia_survey_data.dv_load_date_time,
       p_medallia_survey_data.dv_load_end_date_time
  from dbo.h_medallia_survey_data
  join dbo.p_medallia_survey_data
    on h_medallia_survey_data.bk_hash = p_medallia_survey_data.bk_hash
  join #p_medallia_survey_data_insert
    on p_medallia_survey_data.bk_hash = #p_medallia_survey_data_insert.bk_hash
   and p_medallia_survey_data.p_medallia_survey_data_id = #p_medallia_survey_data_insert.p_medallia_survey_data_id
  join dbo.s_medallia_survey_data
    on p_medallia_survey_data.bk_hash = s_medallia_survey_data.bk_hash
   and p_medallia_survey_data.s_medallia_survey_data_id = s_medallia_survey_data.s_medallia_survey_data_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_medallia_survey_data
   where d_medallia_survey_data.bk_hash in (select bk_hash from #p_medallia_survey_data_insert)

  insert dbo.d_medallia_survey_data(
             bk_hash,
             d_medallia_survey_data_key,
             survey_id,
             field_name,
             field_value,
             file_name,
             deleted_flag,
             p_medallia_survey_data_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_medallia_survey_data_key,
         survey_id,
         field_name,
         field_value,
         file_name,
         dv_deleted,
         p_medallia_survey_data_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_medallia_survey_data)
--Done!
end
