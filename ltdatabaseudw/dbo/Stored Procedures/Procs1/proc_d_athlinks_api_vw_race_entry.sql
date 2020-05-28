CREATE PROC [dbo].[proc_d_athlinks_api_vw_race_entry] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_race_entry)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_athlinks_api_vw_race_entry_insert') is not null drop table #p_athlinks_api_vw_race_entry_insert
create table dbo.#p_athlinks_api_vw_race_entry_insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_race_entry.p_athlinks_api_vw_race_entry_id,
       p_athlinks_api_vw_race_entry.bk_hash
  from dbo.p_athlinks_api_vw_race_entry
 where p_athlinks_api_vw_race_entry.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_athlinks_api_vw_race_entry.dv_batch_id > @max_dv_batch_id
        or p_athlinks_api_vw_race_entry.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_race_entry.bk_hash,
       p_athlinks_api_vw_race_entry.entry_id entry_id,
       s_athlinks_api_vw_race_entry.age age,
       s_athlinks_api_vw_race_entry.bib_num bib_num,
       s_athlinks_api_vw_race_entry.city city,
       l_athlinks_api_vw_race_entry.class_id class_id,
       s_athlinks_api_vw_race_entry.class_name class_name,
       l_athlinks_api_vw_race_entry.course_id course_id,
       s_athlinks_api_vw_race_entry.create_date create_date,
       case when p_athlinks_api_vw_race_entry.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_entry.bk_hash
           when s_athlinks_api_vw_race_entry.create_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_race_entry.create_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_race_entry.create_date, 112)< 19000101 then '19000101' 
        else convert(varchar, s_athlinks_api_vw_race_entry.create_date, 112)    end create_dim_date_key,
       case when p_athlinks_api_vw_race_entry.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_race_entry.bk_hash
       when s_athlinks_api_vw_race_entry.create_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_race_entry.create_date,114), 1, 5),':','') end create_dim_time_key,
       case when p_athlinks_api_vw_race_entry.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_entry.bk_hash
           when l_athlinks_api_vw_race_entry.racer_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_race_entry.racer_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_athlinks_api_vw_athlete_non_member_bk_hash,
       case when p_athlinks_api_vw_race_entry.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_entry.bk_hash
           when l_athlinks_api_vw_race_entry.course_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_race_entry.course_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_athlinks_api_vw_course_bk_hash,
       case when p_athlinks_api_vw_race_entry.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_entry.bk_hash
           when l_athlinks_api_vw_race_entry.master_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_race_entry.master_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_athlinks_api_vw_master_event_bk_hash,
       case when p_athlinks_api_vw_race_entry.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_entry.bk_hash     
       when l_athlinks_api_vw_race_entry.race_id is null then '-998'   
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_race_entry.race_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_athlinks_api_vw_race_ltf_data_bk_hash,
       s_athlinks_api_vw_race_entry.display_name display_name,
       l_athlinks_api_vw_race_entry.entry_state_prov_id entry_state_prov_id,
       l_athlinks_api_vw_race_entry.event_course_id event_course_id,
       case when p_athlinks_api_vw_race_entry.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_entry.bk_hash
           when l_athlinks_api_vw_race_entry.event_course_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_race_entry.event_course_id as int) as varchar(500)),'z#@$k%&P'))),2)   end event_d_athlinks_api_vw_course_bk_hash,
       s_athlinks_api_vw_race_entry.gender gender,
       case when s_athlinks_api_vw_race_entry.is_member = 1 then 'Y' else 'N' end is_member_flag,
       l_athlinks_api_vw_race_entry.master_id master_id,
       s_athlinks_api_vw_race_entry.master_name master_name,
       s_athlinks_api_vw_race_entry.photo_path photo_path,
       s_athlinks_api_vw_race_entry.race_date race_date,
       case when p_athlinks_api_vw_race_entry.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_entry.bk_hash
           when s_athlinks_api_vw_race_entry.race_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_race_entry.race_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_race_entry.race_date, 112)< 19000101 then '19000101'  
        else convert(varchar, s_athlinks_api_vw_race_entry.race_date, 112)    end race_dim_date_key,
       case when p_athlinks_api_vw_race_entry.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_race_entry.bk_hash
       when s_athlinks_api_vw_race_entry.race_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_race_entry.race_date,114), 1, 5),':','') end race_dim_time_key,
       l_athlinks_api_vw_race_entry.race_id race_id,
       l_athlinks_api_vw_race_entry.racer_id racer_id,
       s_athlinks_api_vw_race_entry.rank_a rank_a,
       s_athlinks_api_vw_race_entry.rank_g rank_g,
       s_athlinks_api_vw_race_entry.rank_o rank_o,
       s_athlinks_api_vw_race_entry.result_count result_count,
       s_athlinks_api_vw_race_entry.results_date results_date,
       case when p_athlinks_api_vw_race_entry.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_entry.bk_hash
           when s_athlinks_api_vw_race_entry.results_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_race_entry.results_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_race_entry.results_date, 112)< 19000101 then '19000101'  
        else convert(varchar, s_athlinks_api_vw_race_entry.results_date, 112)    end results_dim_date_key,
       case when p_athlinks_api_vw_race_entry.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_race_entry.bk_hash
       when s_athlinks_api_vw_race_entry.results_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_race_entry.results_date,114), 1, 5),':','') end results_dim_time_key,
       s_athlinks_api_vw_race_entry.ticks ticks,
       s_athlinks_api_vw_race_entry.ticks_string ticks_string,
       s_athlinks_api_vw_race_entry.total_a total_a,
       s_athlinks_api_vw_race_entry.total_g total_g,
       s_athlinks_api_vw_race_entry.total_o total_o,
       isnull(h_athlinks_api_vw_race_entry.dv_deleted,0) dv_deleted,
       p_athlinks_api_vw_race_entry.p_athlinks_api_vw_race_entry_id,
       p_athlinks_api_vw_race_entry.dv_batch_id,
       p_athlinks_api_vw_race_entry.dv_load_date_time,
       p_athlinks_api_vw_race_entry.dv_load_end_date_time
  from dbo.h_athlinks_api_vw_race_entry
  join dbo.p_athlinks_api_vw_race_entry
    on h_athlinks_api_vw_race_entry.bk_hash = p_athlinks_api_vw_race_entry.bk_hash
  join #p_athlinks_api_vw_race_entry_insert
    on p_athlinks_api_vw_race_entry.bk_hash = #p_athlinks_api_vw_race_entry_insert.bk_hash
   and p_athlinks_api_vw_race_entry.p_athlinks_api_vw_race_entry_id = #p_athlinks_api_vw_race_entry_insert.p_athlinks_api_vw_race_entry_id
  join dbo.l_athlinks_api_vw_race_entry
    on p_athlinks_api_vw_race_entry.bk_hash = l_athlinks_api_vw_race_entry.bk_hash
   and p_athlinks_api_vw_race_entry.l_athlinks_api_vw_race_entry_id = l_athlinks_api_vw_race_entry.l_athlinks_api_vw_race_entry_id
  join dbo.s_athlinks_api_vw_race_entry
    on p_athlinks_api_vw_race_entry.bk_hash = s_athlinks_api_vw_race_entry.bk_hash
   and p_athlinks_api_vw_race_entry.s_athlinks_api_vw_race_entry_id = s_athlinks_api_vw_race_entry.s_athlinks_api_vw_race_entry_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_athlinks_api_vw_race_entry
   where d_athlinks_api_vw_race_entry.bk_hash in (select bk_hash from #p_athlinks_api_vw_race_entry_insert)

  insert dbo.d_athlinks_api_vw_race_entry(
             bk_hash,
             entry_id,
             age,
             bib_num,
             city,
             class_id,
             class_name,
             course_id,
             create_date,
             create_dim_date_key,
             create_dim_time_key,
             d_athlinks_api_vw_athlete_non_member_bk_hash,
             d_athlinks_api_vw_course_bk_hash,
             d_athlinks_api_vw_master_event_bk_hash,
             d_athlinks_api_vw_race_ltf_data_bk_hash,
             display_name,
             entry_state_prov_id,
             event_course_id,
             event_d_athlinks_api_vw_course_bk_hash,
             gender,
             is_member_flag,
             master_id,
             master_name,
             photo_path,
             race_date,
             race_dim_date_key,
             race_dim_time_key,
             race_id,
             racer_id,
             rank_a,
             rank_g,
             rank_o,
             result_count,
             results_date,
             results_dim_date_key,
             results_dim_time_key,
             ticks,
             ticks_string,
             total_a,
             total_g,
             total_o,
             deleted_flag,
             p_athlinks_api_vw_race_entry_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         entry_id,
         age,
         bib_num,
         city,
         class_id,
         class_name,
         course_id,
         create_date,
         create_dim_date_key,
         create_dim_time_key,
         d_athlinks_api_vw_athlete_non_member_bk_hash,
         d_athlinks_api_vw_course_bk_hash,
         d_athlinks_api_vw_master_event_bk_hash,
         d_athlinks_api_vw_race_ltf_data_bk_hash,
         display_name,
         entry_state_prov_id,
         event_course_id,
         event_d_athlinks_api_vw_course_bk_hash,
         gender,
         is_member_flag,
         master_id,
         master_name,
         photo_path,
         race_date,
         race_dim_date_key,
         race_dim_time_key,
         race_id,
         racer_id,
         rank_a,
         rank_g,
         rank_o,
         result_count,
         results_date,
         results_dim_date_key,
         results_dim_time_key,
         ticks,
         ticks_string,
         total_a,
         total_g,
         total_o,
         dv_deleted,
         p_athlinks_api_vw_race_entry_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_race_entry)
--Done!
end
