CREATE PROC [dbo].[proc_d_athlinks_api_vw_athlete_non_member] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_athlete_non_member)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_athlinks_api_vw_athlete_non_member_insert') is not null drop table #p_athlinks_api_vw_athlete_non_member_insert
create table dbo.#p_athlinks_api_vw_athlete_non_member_insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_athlete_non_member.p_athlinks_api_vw_athlete_non_member_id,
       p_athlinks_api_vw_athlete_non_member.bk_hash
  from dbo.p_athlinks_api_vw_athlete_non_member
 where p_athlinks_api_vw_athlete_non_member.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_athlinks_api_vw_athlete_non_member.dv_batch_id > @max_dv_batch_id
        or p_athlinks_api_vw_athlete_non_member.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_athlete_non_member.bk_hash,
       p_athlinks_api_vw_athlete_non_member.racer_id racer_id,
       s_athlinks_api_vw_athlete_non_member.age age,
       s_athlinks_api_vw_athlete_non_member.city city,
       s_athlinks_api_vw_athlete_non_member.country_id country_id,
       s_athlinks_api_vw_athlete_non_member.country_id_3 country_id_3,
       s_athlinks_api_vw_athlete_non_member.country_name country_name,
       s_athlinks_api_vw_athlete_non_member.create_date create_date,
       case when p_athlinks_api_vw_athlete_non_member.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_athlete_non_member.bk_hash
           when s_athlinks_api_vw_athlete_non_member.create_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_athlete_non_member.create_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_athlete_non_member.create_date, 112)< 19000101 then '19000101'  
        else convert(varchar, s_athlinks_api_vw_athlete_non_member.create_date, 112)    end create_dim_date_key,
       case when p_athlinks_api_vw_athlete_non_member.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_athlete_non_member.bk_hash
       when s_athlinks_api_vw_athlete_non_member.create_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_athlete_non_member.create_date,114), 1, 5),':','') end create_dim_time_key,
       s_athlinks_api_vw_athlete_non_member.display_name display_name,
       s_athlinks_api_vw_athlete_non_member.f_name full_name,
       s_athlinks_api_vw_athlete_non_member.gender gender,
       s_athlinks_api_vw_athlete_non_member.is_member is_member,
       s_athlinks_api_vw_athlete_non_member.join_date join_date,
       case when p_athlinks_api_vw_athlete_non_member.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_athlete_non_member.bk_hash
           when s_athlinks_api_vw_athlete_non_member.join_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_athlete_non_member.join_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_athlete_non_member.join_date, 112)< 19000101 then '19000101'  
        else convert(varchar, s_athlinks_api_vw_athlete_non_member.join_date, 112)    end join_dim_date_key,
       case when p_athlinks_api_vw_athlete_non_member.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_athlete_non_member.bk_hash
       when s_athlinks_api_vw_athlete_non_member.join_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_athlete_non_member.join_date,114), 1, 5),':','') end join_dim_time_key,
       s_athlinks_api_vw_athlete_non_member.l_name last_name,
       s_athlinks_api_vw_athlete_non_member.notes notes,
       s_athlinks_api_vw_athlete_non_member.owner_id owner_id,
       s_athlinks_api_vw_athlete_non_member.photo_path photo_path,
       s_athlinks_api_vw_athlete_non_member.result_count result_count,
       s_athlinks_api_vw_athlete_non_member.state_prov_abbrev state_prov_abbrev,
       s_athlinks_api_vw_athlete_non_member.state_prov_id state_prov_id,
       s_athlinks_api_vw_athlete_non_member.state_prov_name state_prov_name,
       isnull(h_athlinks_api_vw_athlete_non_member.dv_deleted,0) dv_deleted,
       p_athlinks_api_vw_athlete_non_member.p_athlinks_api_vw_athlete_non_member_id,
       p_athlinks_api_vw_athlete_non_member.dv_batch_id,
       p_athlinks_api_vw_athlete_non_member.dv_load_date_time,
       p_athlinks_api_vw_athlete_non_member.dv_load_end_date_time
  from dbo.h_athlinks_api_vw_athlete_non_member
  join dbo.p_athlinks_api_vw_athlete_non_member
    on h_athlinks_api_vw_athlete_non_member.bk_hash = p_athlinks_api_vw_athlete_non_member.bk_hash
  join #p_athlinks_api_vw_athlete_non_member_insert
    on p_athlinks_api_vw_athlete_non_member.bk_hash = #p_athlinks_api_vw_athlete_non_member_insert.bk_hash
   and p_athlinks_api_vw_athlete_non_member.p_athlinks_api_vw_athlete_non_member_id = #p_athlinks_api_vw_athlete_non_member_insert.p_athlinks_api_vw_athlete_non_member_id
  join dbo.s_athlinks_api_vw_athlete_non_member
    on p_athlinks_api_vw_athlete_non_member.bk_hash = s_athlinks_api_vw_athlete_non_member.bk_hash
   and p_athlinks_api_vw_athlete_non_member.s_athlinks_api_vw_athlete_non_member_id = s_athlinks_api_vw_athlete_non_member.s_athlinks_api_vw_athlete_non_member_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_athlinks_api_vw_athlete_non_member
   where d_athlinks_api_vw_athlete_non_member.bk_hash in (select bk_hash from #p_athlinks_api_vw_athlete_non_member_insert)

  insert dbo.d_athlinks_api_vw_athlete_non_member(
             bk_hash,
             racer_id,
             age,
             city,
             country_id,
             country_id_3,
             country_name,
             create_date,
             create_dim_date_key,
             create_dim_time_key,
             display_name,
             full_name,
             gender,
             is_member,
             join_date,
             join_dim_date_key,
             join_dim_time_key,
             last_name,
             notes,
             owner_id,
             photo_path,
             result_count,
             state_prov_abbrev,
             state_prov_id,
             state_prov_name,
             deleted_flag,
             p_athlinks_api_vw_athlete_non_member_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         racer_id,
         age,
         city,
         country_id,
         country_id_3,
         country_name,
         create_date,
         create_dim_date_key,
         create_dim_time_key,
         display_name,
         full_name,
         gender,
         is_member,
         join_date,
         join_dim_date_key,
         join_dim_time_key,
         last_name,
         notes,
         owner_id,
         photo_path,
         result_count,
         state_prov_abbrev,
         state_prov_id,
         state_prov_name,
         dv_deleted,
         p_athlinks_api_vw_athlete_non_member_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_athlete_non_member)
--Done!
end
