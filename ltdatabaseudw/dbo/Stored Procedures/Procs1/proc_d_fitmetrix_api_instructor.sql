CREATE PROC [dbo].[proc_d_fitmetrix_api_instructor] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_instructor)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_fitmetrix_api_instructor_insert') is not null drop table #p_fitmetrix_api_instructor_insert
create table dbo.#p_fitmetrix_api_instructor_insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_instructor.p_fitmetrix_api_instructor_id,
       p_fitmetrix_api_instructor.bk_hash
  from dbo.p_fitmetrix_api_instructor
 where p_fitmetrix_api_instructor.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_fitmetrix_api_instructor.dv_batch_id > @max_dv_batch_id
        or p_fitmetrix_api_instructor.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_instructor.bk_hash,
       p_fitmetrix_api_instructor.bk_hash dim_fitmetrix_instructor_key,
       p_fitmetrix_api_instructor.instructor_id instructor_id,
       case when p_fitmetrix_api_instructor.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_instructor.bk_hash       
     when l_fitmetrix_api_instructor.external_id is null then '-998'      
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_fitmetrix_api_instructor.external_id as varchar(500)),'z#@$k%&P'))),2)   
end dim_employee_key,
       case when p_fitmetrix_api_instructor.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_instructor.bk_hash       
     when l_fitmetrix_api_instructor.facility_location_id is null then '-998'      
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_fitmetrix_api_instructor.facility_location_id as varchar(500)),'z#@$k%&P'))),2)     
end dim_fitmetrix_location_key,
       isnull(s_fitmetrix_api_instructor.email, '') email,
       isnull(s_fitmetrix_api_instructor.gender, '') gender,
       isnull(s_fitmetrix_api_instructor.first_name, '') name,
       h_fitmetrix_api_instructor.dv_deleted,
       p_fitmetrix_api_instructor.p_fitmetrix_api_instructor_id,
       p_fitmetrix_api_instructor.dv_batch_id,
       p_fitmetrix_api_instructor.dv_load_date_time,
       p_fitmetrix_api_instructor.dv_load_end_date_time
  from dbo.h_fitmetrix_api_instructor
  join dbo.p_fitmetrix_api_instructor
    on h_fitmetrix_api_instructor.bk_hash = p_fitmetrix_api_instructor.bk_hash  join #p_fitmetrix_api_instructor_insert
    on p_fitmetrix_api_instructor.bk_hash = #p_fitmetrix_api_instructor_insert.bk_hash
   and p_fitmetrix_api_instructor.p_fitmetrix_api_instructor_id = #p_fitmetrix_api_instructor_insert.p_fitmetrix_api_instructor_id
  join dbo.l_fitmetrix_api_instructor
    on p_fitmetrix_api_instructor.bk_hash = l_fitmetrix_api_instructor.bk_hash
   and p_fitmetrix_api_instructor.l_fitmetrix_api_instructor_id = l_fitmetrix_api_instructor.l_fitmetrix_api_instructor_id
  join dbo.s_fitmetrix_api_instructor
    on p_fitmetrix_api_instructor.bk_hash = s_fitmetrix_api_instructor.bk_hash
   and p_fitmetrix_api_instructor.s_fitmetrix_api_instructor_id = s_fitmetrix_api_instructor.s_fitmetrix_api_instructor_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_fitmetrix_api_instructor
   where d_fitmetrix_api_instructor.bk_hash in (select bk_hash from #p_fitmetrix_api_instructor_insert)

  insert dbo.d_fitmetrix_api_instructor(
             bk_hash,
             dim_fitmetrix_instructor_key,
             instructor_id,
             dim_employee_key,
             dim_fitmetrix_location_key,
             email,
             gender,
             name,
             deleted_flag,
             p_fitmetrix_api_instructor_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_fitmetrix_instructor_key,
         instructor_id,
         dim_employee_key,
         dim_fitmetrix_location_key,
         email,
         gender,
         name,
         dv_deleted,
         p_fitmetrix_api_instructor_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_instructor)
--Done!
end
