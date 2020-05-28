CREATE PROC [dbo].[proc_d_fitmetrix_api_appointments] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_appointments)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_fitmetrix_api_appointments_insert') is not null drop table #p_fitmetrix_api_appointments_insert
create table dbo.#p_fitmetrix_api_appointments_insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_appointments.p_fitmetrix_api_appointments_id,
       p_fitmetrix_api_appointments.bk_hash
  from dbo.p_fitmetrix_api_appointments
 where p_fitmetrix_api_appointments.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_fitmetrix_api_appointments.dv_batch_id > @max_dv_batch_id
        or p_fitmetrix_api_appointments.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_fitmetrix_api_appointments.bk_hash,
       p_fitmetrix_api_appointments.bk_hash dim_fitmetrix_appointment_key,
       p_fitmetrix_api_appointments.appointment_id appointment_id,
       case when s_fitmetrix_api_appointments.is_cancelled = 'true' then 'Y'
     else 'N'
 end cancelled_flag,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') 	then p_fitmetrix_api_appointments.bk_hash      
       when l_fitmetrix_api_appointments.external_id_base64_decoded is null then '-998'
       else 
       	case when charindex('boss:', convert(varchar,l_fitmetrix_api_appointments.external_id_base64_decoded)) > 0 
       		then convert(varchar(32),hashbytes('md5',('P%#&z$@k'+substring(replace(convert(varchar,l_fitmetrix_api_appointments.external_id_base64_decoded), 'boss:', ''), 1, 
       		charindex(':', replace(convert(varchar,l_fitmetrix_api_appointments.external_id_base64_decoded), 'boss:', '')) - 1))),2) else '-998' 
       	end
       end dim_boss_reservation_key,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash      
       when l_fitmetrix_api_appointments.external_id_base64_decoded is null then '-998'
       else 
       	case when charindex('exerp:', convert(varchar,l_fitmetrix_api_appointments.external_id_base64_decoded)) > 0 
       		then convert(varchar(32),hashbytes('md5',('P%#&z$@k'+substring(replace(convert(varchar,l_fitmetrix_api_appointments.external_id_base64_decoded), 'exerp:', ''), 1, 
       		charindex(':', replace(convert(varchar,l_fitmetrix_api_appointments.external_id_base64_decoded), 'exerp:', '')) - 1))),2) else '-998' 
       	end 
       end dim_exerp_booking_key,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash
     when l_fitmetrix_api_appointments.activity_id is null then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_fitmetrix_api_appointments.activity_id as int) as varchar(500)),'z#@$k%&P'))),2)
 end dim_fitmetrix_activity_key,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash
     when l_fitmetrix_api_appointments.instructor_id is null then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_fitmetrix_api_appointments.instructor_id as int) as varchar(500)),'z#@$k%&P'))),2)
 end dim_fitmetrix_instructor_key,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash
     when l_fitmetrix_api_appointments.facility_location_id is null then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_fitmetrix_api_appointments.facility_location_id as int) as varchar(500)),'z#@$k%&P'))),2)
 end dim_fitmetrix_location_key,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash
     when l_fitmetrix_api_appointments.facility_location_resource_id is null then '-998'
     else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_fitmetrix_api_appointments.facility_location_resource_id as int) as varchar(500)),'z#@$k%&P'))),2)
 end dim_fitmetrix_location_resource_key,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash
     when s_fitmetrix_api_appointments.end_date_time is null then '-998'
     else convert(varchar, convert(datetime,s_fitmetrix_api_appointments.end_date_time,126), 112)
 end end_dim_date_key,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash
     when s_fitmetrix_api_appointments.end_date_time is null then '-998'
     else '1' + replace(substring(convert(varchar,convert(datetime,s_fitmetrix_api_appointments.end_date_time,126),114), 1, 5),':','')
 end end_dim_time_key,
       s_fitmetrix_api_appointments.instructor_first_name instructor_name,
       s_fitmetrix_api_appointments.max_capacity max_capacity,
       s_fitmetrix_api_appointments.name name,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash
     when s_fitmetrix_api_appointments.start_date_time is null then '-998'
     else convert(varchar, convert(datetime,s_fitmetrix_api_appointments.start_date_time,126), 112)
 end start_dim_date_key,
       case when p_fitmetrix_api_appointments.bk_hash in ('-997','-998','-999') then p_fitmetrix_api_appointments.bk_hash
     when s_fitmetrix_api_appointments.start_date_time is null then '-998'
     else '1' + replace(substring(convert(varchar,convert(datetime,s_fitmetrix_api_appointments.start_date_time,126),114), 1, 5),':','')
 end start_dim_time_key,
       s_fitmetrix_api_appointments.total_booked total_booked,
       case when s_fitmetrix_api_appointments.is_wait_list_available = 'true' then 'Y'
     else 'N'
 end wait_list_available_flag,
       isnull(h_fitmetrix_api_appointments.dv_deleted,0) dv_deleted,
       p_fitmetrix_api_appointments.p_fitmetrix_api_appointments_id,
       p_fitmetrix_api_appointments.dv_batch_id,
       p_fitmetrix_api_appointments.dv_load_date_time,
       p_fitmetrix_api_appointments.dv_load_end_date_time
  from dbo.h_fitmetrix_api_appointments
  join dbo.p_fitmetrix_api_appointments
    on h_fitmetrix_api_appointments.bk_hash = p_fitmetrix_api_appointments.bk_hash
  join #p_fitmetrix_api_appointments_insert
    on p_fitmetrix_api_appointments.bk_hash = #p_fitmetrix_api_appointments_insert.bk_hash
   and p_fitmetrix_api_appointments.p_fitmetrix_api_appointments_id = #p_fitmetrix_api_appointments_insert.p_fitmetrix_api_appointments_id
  join dbo.l_fitmetrix_api_appointments
    on p_fitmetrix_api_appointments.bk_hash = l_fitmetrix_api_appointments.bk_hash
   and p_fitmetrix_api_appointments.l_fitmetrix_api_appointments_id = l_fitmetrix_api_appointments.l_fitmetrix_api_appointments_id
  join dbo.s_fitmetrix_api_appointments
    on p_fitmetrix_api_appointments.bk_hash = s_fitmetrix_api_appointments.bk_hash
   and p_fitmetrix_api_appointments.s_fitmetrix_api_appointments_id = s_fitmetrix_api_appointments.s_fitmetrix_api_appointments_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_fitmetrix_api_appointments
   where d_fitmetrix_api_appointments.bk_hash in (select bk_hash from #p_fitmetrix_api_appointments_insert)

  insert dbo.d_fitmetrix_api_appointments(
             bk_hash,
             dim_fitmetrix_appointment_key,
             appointment_id,
             cancelled_flag,
             dim_boss_reservation_key,
             dim_exerp_booking_key,
             dim_fitmetrix_activity_key,
             dim_fitmetrix_instructor_key,
             dim_fitmetrix_location_key,
             dim_fitmetrix_location_resource_key,
             end_dim_date_key,
             end_dim_time_key,
             instructor_name,
             max_capacity,
             name,
             start_dim_date_key,
             start_dim_time_key,
             total_booked,
             wait_list_available_flag,
             deleted_flag,
             p_fitmetrix_api_appointments_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_fitmetrix_appointment_key,
         appointment_id,
         cancelled_flag,
         dim_boss_reservation_key,
         dim_exerp_booking_key,
         dim_fitmetrix_activity_key,
         dim_fitmetrix_instructor_key,
         dim_fitmetrix_location_key,
         dim_fitmetrix_location_resource_key,
         end_dim_date_key,
         end_dim_time_key,
         instructor_name,
         max_capacity,
         name,
         start_dim_date_key,
         start_dim_time_key,
         total_booked,
         wait_list_available_flag,
         dv_deleted,
         p_fitmetrix_api_appointments_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_fitmetrix_api_appointments)
--Done!
end
