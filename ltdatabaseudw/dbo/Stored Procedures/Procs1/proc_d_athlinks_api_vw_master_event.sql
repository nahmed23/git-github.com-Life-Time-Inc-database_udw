CREATE PROC [dbo].[proc_d_athlinks_api_vw_master_event] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_master_event)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_athlinks_api_vw_master_event_insert') is not null drop table #p_athlinks_api_vw_master_event_insert
create table dbo.#p_athlinks_api_vw_master_event_insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_master_event.p_athlinks_api_vw_master_event_id,
       p_athlinks_api_vw_master_event.bk_hash
  from dbo.p_athlinks_api_vw_master_event
 where p_athlinks_api_vw_master_event.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_athlinks_api_vw_master_event.dv_batch_id > @max_dv_batch_id
        or p_athlinks_api_vw_master_event.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_master_event.bk_hash,
       p_athlinks_api_vw_master_event.master_id master_id,
       s_athlinks_api_vw_master_event.city city,
       l_athlinks_api_vw_master_event.company_id company_id,
       s_athlinks_api_vw_master_event.company_name company_name,
       s_athlinks_api_vw_master_event.contact_address contact_address,
       s_athlinks_api_vw_master_event.contact_name contact_name,
       s_athlinks_api_vw_master_event.country_id country_id,
       s_athlinks_api_vw_master_event.create_date create_date,
       case when p_athlinks_api_vw_master_event.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_master_event.bk_hash
           when s_athlinks_api_vw_master_event.create_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_master_event.create_date, 112) > 20991231 then '99991231' 
        when convert(varchar, s_athlinks_api_vw_master_event.create_date, 112)< 19000101 then '19000101'
        else convert(varchar, s_athlinks_api_vw_master_event.create_date, 112)    end create_dim_date_key,
       case when p_athlinks_api_vw_master_event.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_master_event.bk_hash
       when s_athlinks_api_vw_master_event.create_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_master_event.create_date,114), 1, 5),':','') end create_dim_time_key,
       s_athlinks_api_vw_master_event.curated_desc curated_desc,
       s_athlinks_api_vw_master_event.elevation elevation,
       case when s_athlinks_api_vw_master_event.featured = 1 then 'Y' else 'N' end  featured_flag,
       s_athlinks_api_vw_master_event.geo geo,
       s_athlinks_api_vw_master_event.latitude latitude,
       s_athlinks_api_vw_master_event.logo_path logo_path,
       s_athlinks_api_vw_master_event.longitude longitude,
       s_athlinks_api_vw_master_event.name master_event_name,
       case when p_athlinks_api_vw_master_event.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_master_event.bk_hash     
        when l_athlinks_api_vw_master_event.next_race_id is null then '-998'   
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_master_event.next_race_id as int) as varchar(500)),'z#@$k%&P'))),2)   end next_d_athlinks_api_vw_race_ltf_data_bk_hash,
       s_athlinks_api_vw_master_event.next_date next_date,
       case when p_athlinks_api_vw_master_event.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_master_event.bk_hash
           when s_athlinks_api_vw_master_event.next_date is null then '-998'
       		when  convert(varchar, s_athlinks_api_vw_master_event.next_date, 112) > 20991231 then '99991231' 
        when convert(varchar, s_athlinks_api_vw_master_event.next_date, 112)< 19000101 then '19000101'
        else convert(varchar, s_athlinks_api_vw_master_event.next_date, 112)    end next_dim_date_key,
       case when p_athlinks_api_vw_master_event.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_master_event.bk_hash
       when s_athlinks_api_vw_master_event.next_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_master_event.next_date,114), 1, 5),':','') end next_dim_time_key,
       l_athlinks_api_vw_master_event.next_race_id next_race_id,
       s_athlinks_api_vw_master_event.phone phone,
       case when p_athlinks_api_vw_master_event.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_master_event.bk_hash     
        when l_athlinks_api_vw_master_event.prev_race_id is null then '-998'  
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_master_event.prev_race_id as int) as varchar(500)),'z#@$k%&P'))),2)   end prev_d_athlinks_api_vw_race_ltf_data_bk_hash,
       s_athlinks_api_vw_master_event.prev_date prev_date,
       case when p_athlinks_api_vw_master_event.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_master_event.bk_hash
           when s_athlinks_api_vw_master_event.prev_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_master_event.prev_date, 112) > 20991231 then '99991231' 
        when convert(varchar, s_athlinks_api_vw_master_event.prev_date, 112)< 19000101 then '19000101'
        else convert(varchar, s_athlinks_api_vw_master_event.prev_date, 112)    end prev_dim_date_key,
       case when p_athlinks_api_vw_master_event.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_master_event.bk_hash
       when s_athlinks_api_vw_master_event.prev_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_master_event.prev_date,114), 1, 5),':','') end prev_dim_time_key,
       l_athlinks_api_vw_master_event.prev_race_id prev_race_id,
       s_athlinks_api_vw_master_event.race_count race_count,
       s_athlinks_api_vw_master_event.result_count result_count,
       s_athlinks_api_vw_master_event.short_url short_url,
       s_athlinks_api_vw_master_event.state_prov_abbrev state_prov_abbrev,
       s_athlinks_api_vw_master_event.state_prov_id state_prov_id,
       isnull(h_athlinks_api_vw_master_event.dv_deleted,0) dv_deleted,
       p_athlinks_api_vw_master_event.p_athlinks_api_vw_master_event_id,
       p_athlinks_api_vw_master_event.dv_batch_id,
       p_athlinks_api_vw_master_event.dv_load_date_time,
       p_athlinks_api_vw_master_event.dv_load_end_date_time
  from dbo.h_athlinks_api_vw_master_event
  join dbo.p_athlinks_api_vw_master_event
    on h_athlinks_api_vw_master_event.bk_hash = p_athlinks_api_vw_master_event.bk_hash
  join #p_athlinks_api_vw_master_event_insert
    on p_athlinks_api_vw_master_event.bk_hash = #p_athlinks_api_vw_master_event_insert.bk_hash
   and p_athlinks_api_vw_master_event.p_athlinks_api_vw_master_event_id = #p_athlinks_api_vw_master_event_insert.p_athlinks_api_vw_master_event_id
  join dbo.l_athlinks_api_vw_master_event
    on p_athlinks_api_vw_master_event.bk_hash = l_athlinks_api_vw_master_event.bk_hash
   and p_athlinks_api_vw_master_event.l_athlinks_api_vw_master_event_id = l_athlinks_api_vw_master_event.l_athlinks_api_vw_master_event_id
  join dbo.s_athlinks_api_vw_master_event
    on p_athlinks_api_vw_master_event.bk_hash = s_athlinks_api_vw_master_event.bk_hash
   and p_athlinks_api_vw_master_event.s_athlinks_api_vw_master_event_id = s_athlinks_api_vw_master_event.s_athlinks_api_vw_master_event_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_athlinks_api_vw_master_event
   where d_athlinks_api_vw_master_event.bk_hash in (select bk_hash from #p_athlinks_api_vw_master_event_insert)

  insert dbo.d_athlinks_api_vw_master_event(
             bk_hash,
             master_id,
             city,
             company_id,
             company_name,
             contact_address,
             contact_name,
             country_id,
             create_date,
             create_dim_date_key,
             create_dim_time_key,
             curated_desc,
             elevation,
             featured_flag,
             geo,
             latitude,
             logo_path,
             longitude,
             master_event_name,
             next_d_athlinks_api_vw_race_ltf_data_bk_hash,
             next_date,
             next_dim_date_key,
             next_dim_time_key,
             next_race_id,
             phone,
             prev_d_athlinks_api_vw_race_ltf_data_bk_hash,
             prev_date,
             prev_dim_date_key,
             prev_dim_time_key,
             prev_race_id,
             race_count,
             result_count,
             short_url,
             state_prov_abbrev,
             state_prov_id,
             deleted_flag,
             p_athlinks_api_vw_master_event_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         master_id,
         city,
         company_id,
         company_name,
         contact_address,
         contact_name,
         country_id,
         create_date,
         create_dim_date_key,
         create_dim_time_key,
         curated_desc,
         elevation,
         featured_flag,
         geo,
         latitude,
         logo_path,
         longitude,
         master_event_name,
         next_d_athlinks_api_vw_race_ltf_data_bk_hash,
         next_date,
         next_dim_date_key,
         next_dim_time_key,
         next_race_id,
         phone,
         prev_d_athlinks_api_vw_race_ltf_data_bk_hash,
         prev_date,
         prev_dim_date_key,
         prev_dim_time_key,
         prev_race_id,
         race_count,
         result_count,
         short_url,
         state_prov_abbrev,
         state_prov_id,
         dv_deleted,
         p_athlinks_api_vw_master_event_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_master_event)
--Done!
end
