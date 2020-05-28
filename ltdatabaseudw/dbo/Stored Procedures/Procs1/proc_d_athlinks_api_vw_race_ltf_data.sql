CREATE PROC [dbo].[proc_d_athlinks_api_vw_race_ltf_data] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_race_ltf_data)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_athlinks_api_vw_race_ltf_data_insert') is not null drop table #p_athlinks_api_vw_race_ltf_data_insert
create table dbo.#p_athlinks_api_vw_race_ltf_data_insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_race_ltf_data.p_athlinks_api_vw_race_ltf_data_id,
       p_athlinks_api_vw_race_ltf_data.bk_hash
  from dbo.p_athlinks_api_vw_race_ltf_data
 where p_athlinks_api_vw_race_ltf_data.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_athlinks_api_vw_race_ltf_data.dv_batch_id > @max_dv_batch_id
        or p_athlinks_api_vw_race_ltf_data.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_athlinks_api_vw_race_ltf_data.bk_hash,
       p_athlinks_api_vw_race_ltf_data.race_id race_id,
       s_athlinks_api_vw_race_ltf_data.city city,
       case when p_athlinks_api_vw_race_ltf_data.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_ltf_data.bk_hash
           when l_athlinks_api_vw_race_ltf_data.race_company_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_race_ltf_data.race_company_id as int) as varchar(500)),'z#@$k%&P'))),2)   end company_d_athlinks_api_vw_race_ltf_data_bk_hash,
       s_athlinks_api_vw_race_ltf_data.country_id country_id,
       s_athlinks_api_vw_race_ltf_data.country_id_3 country_id_3,
       s_athlinks_api_vw_race_ltf_data.country_name country_name,
       s_athlinks_api_vw_race_ltf_data.create_date create_date,
       case when p_athlinks_api_vw_race_ltf_data.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_ltf_data.bk_hash
           when s_athlinks_api_vw_race_ltf_data.create_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_race_ltf_data.create_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_race_ltf_data.create_date, 112)< 19000101 then '19000101'  
        else convert(varchar, s_athlinks_api_vw_race_ltf_data.create_date, 112)    end create_dim_date_key,
       case when p_athlinks_api_vw_race_ltf_data.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_race_ltf_data.bk_hash
       when s_athlinks_api_vw_race_ltf_data.create_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_race_ltf_data.create_date,114), 1, 5),':','') end create_dim_time_key,
       case when p_athlinks_api_vw_race_ltf_data.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_ltf_data.bk_hash
           when l_athlinks_api_vw_race_ltf_data.master_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_athlinks_api_vw_race_ltf_data.master_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_athlinks_api_vw_master_event_bk_hash,
       s_athlinks_api_vw_race_ltf_data.date_sort date_sort,
       s_athlinks_api_vw_race_ltf_data.elevation elevation,
       s_athlinks_api_vw_race_ltf_data.latitude latitude,
       s_athlinks_api_vw_race_ltf_data.longitude longitude,
       l_athlinks_api_vw_race_ltf_data.master_id master_id,
       l_athlinks_api_vw_race_ltf_data.race_company_id race_company_id,
       s_athlinks_api_vw_race_ltf_data.race_date race_date,
       case when p_athlinks_api_vw_race_ltf_data.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_ltf_data.bk_hash
           when s_athlinks_api_vw_race_ltf_data.race_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_race_ltf_data.race_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_race_ltf_data.race_date, 112)< 19000101 then '19000101'  
        else convert(varchar, s_athlinks_api_vw_race_ltf_data.race_date, 112)    end race_dim_date_key,
       case when p_athlinks_api_vw_race_ltf_data.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_race_ltf_data.bk_hash
       when s_athlinks_api_vw_race_ltf_data.race_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_race_ltf_data.race_date,114), 1, 5),':','') end race_dim_time_key,
       s_athlinks_api_vw_race_ltf_data.race_end_date race_end_date,
       case when p_athlinks_api_vw_race_ltf_data.bk_hash in('-997', '-998', '-999') then p_athlinks_api_vw_race_ltf_data.bk_hash
           when s_athlinks_api_vw_race_ltf_data.race_end_date is null then '-998'
       	when  convert(varchar, s_athlinks_api_vw_race_ltf_data.race_end_date, 112) > 20991231 then '99991231' 
           when convert(varchar, s_athlinks_api_vw_race_ltf_data.race_end_date, 112)< 19000101 then '19000101'  
        else convert(varchar, s_athlinks_api_vw_race_ltf_data.race_end_date, 112)    end race_end_dim_date_key,
       case when p_athlinks_api_vw_race_ltf_data.bk_hash in ('-997','-998','-999') then p_athlinks_api_vw_race_ltf_data.bk_hash
       when s_athlinks_api_vw_race_ltf_data.race_end_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_athlinks_api_vw_race_ltf_data.race_end_date,114), 1, 5),':','') end race_end_dim_time_key,
       s_athlinks_api_vw_race_ltf_data.race_name race_name,
       s_athlinks_api_vw_race_ltf_data.result_count result_count,
       s_athlinks_api_vw_race_ltf_data.state_prov_abbrev state_prov_abbrev,
       s_athlinks_api_vw_race_ltf_data.state_prov_id state_prov_id,
       s_athlinks_api_vw_race_ltf_data.state_prov_name state_prov_name,
       s_athlinks_api_vw_race_ltf_data.status status,
       s_athlinks_api_vw_race_ltf_data.temperature temperature,
       s_athlinks_api_vw_race_ltf_data.weather_notes weather_notes,
       s_athlinks_api_vw_race_ltf_data.website website,
       isnull(h_athlinks_api_vw_race_ltf_data.dv_deleted,0) dv_deleted,
       p_athlinks_api_vw_race_ltf_data.p_athlinks_api_vw_race_ltf_data_id,
       p_athlinks_api_vw_race_ltf_data.dv_batch_id,
       p_athlinks_api_vw_race_ltf_data.dv_load_date_time,
       p_athlinks_api_vw_race_ltf_data.dv_load_end_date_time
  from dbo.h_athlinks_api_vw_race_ltf_data
  join dbo.p_athlinks_api_vw_race_ltf_data
    on h_athlinks_api_vw_race_ltf_data.bk_hash = p_athlinks_api_vw_race_ltf_data.bk_hash
  join #p_athlinks_api_vw_race_ltf_data_insert
    on p_athlinks_api_vw_race_ltf_data.bk_hash = #p_athlinks_api_vw_race_ltf_data_insert.bk_hash
   and p_athlinks_api_vw_race_ltf_data.p_athlinks_api_vw_race_ltf_data_id = #p_athlinks_api_vw_race_ltf_data_insert.p_athlinks_api_vw_race_ltf_data_id
  join dbo.l_athlinks_api_vw_race_ltf_data
    on p_athlinks_api_vw_race_ltf_data.bk_hash = l_athlinks_api_vw_race_ltf_data.bk_hash
   and p_athlinks_api_vw_race_ltf_data.l_athlinks_api_vw_race_ltf_data_id = l_athlinks_api_vw_race_ltf_data.l_athlinks_api_vw_race_ltf_data_id
  join dbo.s_athlinks_api_vw_race_ltf_data
    on p_athlinks_api_vw_race_ltf_data.bk_hash = s_athlinks_api_vw_race_ltf_data.bk_hash
   and p_athlinks_api_vw_race_ltf_data.s_athlinks_api_vw_race_ltf_data_id = s_athlinks_api_vw_race_ltf_data.s_athlinks_api_vw_race_ltf_data_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_athlinks_api_vw_race_ltf_data
   where d_athlinks_api_vw_race_ltf_data.bk_hash in (select bk_hash from #p_athlinks_api_vw_race_ltf_data_insert)

  insert dbo.d_athlinks_api_vw_race_ltf_data(
             bk_hash,
             race_id,
             city,
             company_d_athlinks_api_vw_race_ltf_data_bk_hash,
             country_id,
             country_id_3,
             country_name,
             create_date,
             create_dim_date_key,
             create_dim_time_key,
             d_athlinks_api_vw_master_event_bk_hash,
             date_sort,
             elevation,
             latitude,
             longitude,
             master_id,
             race_company_id,
             race_date,
             race_dim_date_key,
             race_dim_time_key,
             race_end_date,
             race_end_dim_date_key,
             race_end_dim_time_key,
             race_name,
             result_count,
             state_prov_abbrev,
             state_prov_id,
             state_prov_name,
             status,
             temperature,
             weather_notes,
             website,
             deleted_flag,
             p_athlinks_api_vw_race_ltf_data_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         race_id,
         city,
         company_d_athlinks_api_vw_race_ltf_data_bk_hash,
         country_id,
         country_id_3,
         country_name,
         create_date,
         create_dim_date_key,
         create_dim_time_key,
         d_athlinks_api_vw_master_event_bk_hash,
         date_sort,
         elevation,
         latitude,
         longitude,
         master_id,
         race_company_id,
         race_date,
         race_dim_date_key,
         race_dim_time_key,
         race_end_date,
         race_end_dim_date_key,
         race_end_dim_time_key,
         race_name,
         result_count,
         state_prov_abbrev,
         state_prov_id,
         state_prov_name,
         status,
         temperature,
         weather_notes,
         website,
         dv_deleted,
         p_athlinks_api_vw_race_ltf_data_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_athlinks_api_vw_race_ltf_data)
--Done!
end
