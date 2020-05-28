CREATE PROC [dbo].[proc_d_athlinks_api_vw_course] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_course)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_athlinks_api_vw_course_insert') is not null drop table #p_athlinks_api_vw_course_insert
create table dbo.#p_athlinks_api_vw_course_insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_course.p_athlinks_api_vw_course_id,
       p_athlinks_api_vw_course.bk_hash
  from dbo.p_athlinks_api_vw_course
 where p_athlinks_api_vw_course.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_athlinks_api_vw_course.dv_batch_id > @max_dv_batch_id
        or p_athlinks_api_vw_course.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_course.bk_hash,
       p_athlinks_api_vw_course.course_id course_id,
       s_athlinks_api_vw_course.course_name course_name,
       s_athlinks_api_vw_course.course_pattern course_pattern,
       case when p_athlinks_api_vw_course.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_course.bk_hash
           when l_athlinks_api_vw_course.course_pattern_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_course.course_pattern_id as int) as varchar(500)),'z#@$k%&P'))),2)   end course_pattern_d_athlinks_api_vw_course_bk_hash,
       l_athlinks_api_vw_course.course_pattern_id course_pattern_id,
       case when p_athlinks_api_vw_course.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_course.bk_hash
           when l_athlinks_api_vw_course.course_pattern_outer_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_course.course_pattern_outer_id as int) as varchar(500)),'z#@$k%&P'))),2)   end course_pattern_outer_d_athlinks_api_vw_course_bk_hash,
       l_athlinks_api_vw_course.course_pattern_outer_id course_pattern_outer_id,
       s_athlinks_api_vw_course.course_pattern_outer_name course_pattern_outer_name,
       s_athlinks_api_vw_course.create_date create_date,
       case when p_athlinks_api_vw_course.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_course.bk_hash
           when s_athlinks_api_vw_course.create_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_course.create_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_course.create_date, 112)< 19000101 then '19000101'  
        else convert(varchar, s_athlinks_api_vw_course.create_date, 112)    end create_dim_date_key,
       case when p_athlinks_api_vw_course.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_course.bk_hash
       when s_athlinks_api_vw_course.create_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_course.create_date,114), 1, 5),':','') end create_dim_time_key,
       case when p_athlinks_api_vw_course.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_course.bk_hash    
       when l_athlinks_api_vw_course.race_id is null then '-998'  
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_course.race_id as int) as varchar(500)),'z#@$k%&P'))),2)  end d_athlinks_api_vw_race_ltf_data_bk_hash,
       s_athlinks_api_vw_course.dist_type_id dist_type_id,
       s_athlinks_api_vw_course.dist_unit dist_unit,
       case when p_athlinks_api_vw_course.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_course.bk_hash
           when l_athlinks_api_vw_course.event_course_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_course.event_course_id as int) as varchar(500)),'z#@$k%&P'))),2)   end event_course_d_athlinks_api_vw_course_bk_hash,
       l_athlinks_api_vw_course.event_course_id event_course_id,
       s_athlinks_api_vw_course.gallery_id gallery_id,
       s_athlinks_api_vw_course.overall_count overall_count,
       case when p_athlinks_api_vw_course.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_course.bk_hash      
       when l_athlinks_api_vw_course.race_cat_id is null then '-998'   
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_course.race_cat_id as int) as varchar(500)),'z#@$k%&P'))),2)   end race_cat_d_athlinks_api_vw_race_ltf_data_bk_hash,
       s_athlinks_api_vw_course.race_cat_desc race_cat_desc,
       l_athlinks_api_vw_course.race_cat_id race_cat_id,
       l_athlinks_api_vw_course.race_id race_id,
       s_athlinks_api_vw_course.results_date results_date,
       case when p_athlinks_api_vw_course.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_course.bk_hash
           when s_athlinks_api_vw_course.results_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_course.results_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_course.results_date, 112)< 19000101 then '19000101' 
        else convert(varchar, s_athlinks_api_vw_course.results_date, 112)    end results_dim_date_key,
       case when p_athlinks_api_vw_course.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_course.bk_hash
       when s_athlinks_api_vw_course.results_date is null then '-998' 
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_course.results_date,114), 1, 5),':','') end results_dim_time_key,
       s_athlinks_api_vw_course.results_user results_user,
       s_athlinks_api_vw_course.settings settings,
       isnull(h_athlinks_api_vw_course.dv_deleted,0) dv_deleted,
       p_athlinks_api_vw_course.p_athlinks_api_vw_course_id,
       p_athlinks_api_vw_course.dv_batch_id,
       p_athlinks_api_vw_course.dv_load_date_time,
       p_athlinks_api_vw_course.dv_load_end_date_time
  from dbo.h_athlinks_api_vw_course
  join dbo.p_athlinks_api_vw_course
    on h_athlinks_api_vw_course.bk_hash = p_athlinks_api_vw_course.bk_hash
  join #p_athlinks_api_vw_course_insert
    on p_athlinks_api_vw_course.bk_hash = #p_athlinks_api_vw_course_insert.bk_hash
   and p_athlinks_api_vw_course.p_athlinks_api_vw_course_id = #p_athlinks_api_vw_course_insert.p_athlinks_api_vw_course_id
  join dbo.l_athlinks_api_vw_course
    on p_athlinks_api_vw_course.bk_hash = l_athlinks_api_vw_course.bk_hash
   and p_athlinks_api_vw_course.l_athlinks_api_vw_course_id = l_athlinks_api_vw_course.l_athlinks_api_vw_course_id
  join dbo.s_athlinks_api_vw_course
    on p_athlinks_api_vw_course.bk_hash = s_athlinks_api_vw_course.bk_hash
   and p_athlinks_api_vw_course.s_athlinks_api_vw_course_id = s_athlinks_api_vw_course.s_athlinks_api_vw_course_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_athlinks_api_vw_course
   where d_athlinks_api_vw_course.bk_hash in (select bk_hash from #p_athlinks_api_vw_course_insert)

  insert dbo.d_athlinks_api_vw_course(
             bk_hash,
             course_id,
             course_name,
             course_pattern,
             course_pattern_d_athlinks_api_vw_course_bk_hash,
             course_pattern_id,
             course_pattern_outer_d_athlinks_api_vw_course_bk_hash,
             course_pattern_outer_id,
             course_pattern_outer_name,
             create_date,
             create_dim_date_key,
             create_dim_time_key,
             d_athlinks_api_vw_race_ltf_data_bk_hash,
             dist_type_id,
             dist_unit,
             event_course_d_athlinks_api_vw_course_bk_hash,
             event_course_id,
             gallery_id,
             overall_count,
             race_cat_d_athlinks_api_vw_race_ltf_data_bk_hash,
             race_cat_desc,
             race_cat_id,
             race_id,
             results_date,
             results_dim_date_key,
             results_dim_time_key,
             results_user,
             settings,
             deleted_flag,
             p_athlinks_api_vw_course_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         course_id,
         course_name,
         course_pattern,
         course_pattern_d_athlinks_api_vw_course_bk_hash,
         course_pattern_id,
         course_pattern_outer_d_athlinks_api_vw_course_bk_hash,
         course_pattern_outer_id,
         course_pattern_outer_name,
         create_date,
         create_dim_date_key,
         create_dim_time_key,
         d_athlinks_api_vw_race_ltf_data_bk_hash,
         dist_type_id,
         dist_unit,
         event_course_d_athlinks_api_vw_course_bk_hash,
         event_course_id,
         gallery_id,
         overall_count,
         race_cat_d_athlinks_api_vw_race_ltf_data_bk_hash,
         race_cat_desc,
         race_cat_id,
         race_id,
         results_date,
         results_dim_date_key,
         results_dim_time_key,
         results_user,
         settings,
         dv_deleted,
         p_athlinks_api_vw_course_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_course)
--Done!
end
