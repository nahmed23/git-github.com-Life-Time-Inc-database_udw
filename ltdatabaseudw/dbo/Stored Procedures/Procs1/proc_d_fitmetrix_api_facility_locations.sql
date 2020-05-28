CREATE PROC [dbo].[proc_d_fitmetrix_api_facility_locations] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_facility_locations)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_fitmetrix_api_facility_locations_insert') is not null drop table #p_fitmetrix_api_facility_locations_insert
create table dbo.#p_fitmetrix_api_facility_locations_insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_facility_locations.p_fitmetrix_api_facility_locations_id,
       p_fitmetrix_api_facility_locations.bk_hash
  from dbo.p_fitmetrix_api_facility_locations
 where p_fitmetrix_api_facility_locations.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_fitmetrix_api_facility_locations.dv_batch_id > @max_dv_batch_id
        or p_fitmetrix_api_facility_locations.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_facility_locations.bk_hash,
       p_fitmetrix_api_facility_locations.bk_hash dim_fitmetrix_location_key,
       p_fitmetrix_api_facility_locations.facility_location_id facility_location_id,
       s_fitmetrix_api_facility_locations.city address_city,
       s_fitmetrix_api_facility_locations.country address_country_abbreviation,
       s_fitmetrix_api_facility_locations.street_1 address_line_1,
       isnull(s_fitmetrix_api_facility_locations.street_2,'') address_line_2,
       s_fitmetrix_api_facility_locations.zip address_postal_code,
       s_fitmetrix_api_facility_locations.state address_state_or_province_abbreviation,
       case when p_fitmetrix_api_facility_locations.bk_hash in ('-997', '-998', '-999') then p_fitmetrix_api_facility_locations.bk_hash
     when l_fitmetrix_api_facility_locations.external_id is null then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_fitmetrix_api_facility_locations.external_id as varchar(500)),'z#@$k%&P'))),2)
 end dim_club_key,
       isnull(s_fitmetrix_api_facility_locations.email_from_name,'') email_from_name,
       s_fitmetrix_api_facility_locations.latitude latitude,
       s_fitmetrix_api_facility_locations.name location_name,
       s_fitmetrix_api_facility_locations.longitude longitude,
       s_fitmetrix_api_facility_locations.phone phone,
       h_fitmetrix_api_facility_locations.dv_deleted,
       p_fitmetrix_api_facility_locations.p_fitmetrix_api_facility_locations_id,
       p_fitmetrix_api_facility_locations.dv_batch_id,
       p_fitmetrix_api_facility_locations.dv_load_date_time,
       p_fitmetrix_api_facility_locations.dv_load_end_date_time
  from dbo.h_fitmetrix_api_facility_locations
  join dbo.p_fitmetrix_api_facility_locations
    on h_fitmetrix_api_facility_locations.bk_hash = p_fitmetrix_api_facility_locations.bk_hash  join #p_fitmetrix_api_facility_locations_insert
    on p_fitmetrix_api_facility_locations.bk_hash = #p_fitmetrix_api_facility_locations_insert.bk_hash
   and p_fitmetrix_api_facility_locations.p_fitmetrix_api_facility_locations_id = #p_fitmetrix_api_facility_locations_insert.p_fitmetrix_api_facility_locations_id
  join dbo.l_fitmetrix_api_facility_locations
    on p_fitmetrix_api_facility_locations.bk_hash = l_fitmetrix_api_facility_locations.bk_hash
   and p_fitmetrix_api_facility_locations.l_fitmetrix_api_facility_locations_id = l_fitmetrix_api_facility_locations.l_fitmetrix_api_facility_locations_id
  join dbo.s_fitmetrix_api_facility_locations
    on p_fitmetrix_api_facility_locations.bk_hash = s_fitmetrix_api_facility_locations.bk_hash
   and p_fitmetrix_api_facility_locations.s_fitmetrix_api_facility_locations_id = s_fitmetrix_api_facility_locations.s_fitmetrix_api_facility_locations_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_fitmetrix_api_facility_locations
   where d_fitmetrix_api_facility_locations.bk_hash in (select bk_hash from #p_fitmetrix_api_facility_locations_insert)

  insert dbo.d_fitmetrix_api_facility_locations(
             bk_hash,
             dim_fitmetrix_location_key,
             facility_location_id,
             address_city,
             address_country_abbreviation,
             address_line_1,
             address_line_2,
             address_postal_code,
             address_state_or_province_abbreviation,
             dim_club_key,
             email_from_name,
             latitude,
             location_name,
             longitude,
             phone,
             deleted_flag,
             p_fitmetrix_api_facility_locations_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_fitmetrix_location_key,
         facility_location_id,
         address_city,
         address_country_abbreviation,
         address_line_1,
         address_line_2,
         address_postal_code,
         address_state_or_province_abbreviation,
         dim_club_key,
         email_from_name,
         latitude,
         location_name,
         longitude,
         phone,
         dv_deleted,
         p_fitmetrix_api_facility_locations_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_facility_locations)
--Done!
end
