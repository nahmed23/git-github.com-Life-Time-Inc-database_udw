CREATE PROC [dbo].[proc_d_crmcloudsync_appointment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_appointment)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_appointment_insert') is not null drop table #p_crmcloudsync_appointment_insert
create table dbo.#p_crmcloudsync_appointment_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_appointment.p_crmcloudsync_appointment_id,
       p_crmcloudsync_appointment.bk_hash
  from dbo.p_crmcloudsync_appointment
 where p_crmcloudsync_appointment.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_appointment.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_appointment.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_appointment.bk_hash,
       p_crmcloudsync_appointment.bk_hash fact_crm_appointment_key,
       p_crmcloudsync_appointment.activity_id activity_id,
       s_crmcloudsync_appointment.activity_type_code activity_type_code,
       isnull(s_crmcloudsync_appointment.activity_type_code_name,'') activity_type_code_name,
       s_crmcloudsync_appointment.actual_duration_minutes actual_duration_minutes,
       s_crmcloudsync_appointment.actual_end actual_end,
       case when p_crmcloudsync_appointment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
          when s_crmcloudsync_appointment.actual_end is null then '-998'
       else convert(varchar, s_crmcloudsync_appointment.actual_end, 112)    end actual_end_dim_date_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_appointment.bk_hash
       when s_crmcloudsync_appointment.actual_end is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_appointment.actual_end,114), 1, 5),':','') end actual_end_dim_time_key,
       s_crmcloudsync_appointment.actual_start actual_start,
       case when p_crmcloudsync_appointment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
          when s_crmcloudsync_appointment.actual_start is null then '-998'
       else convert(varchar, s_crmcloudsync_appointment.actual_start, 112)    end actual_start_dim_date_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_appointment.bk_hash
       when s_crmcloudsync_appointment.actual_start is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_appointment.actual_start,114), 1, 5),':','') end actual_start_dim_time_key,
       isnull(s_crmcloudsync_appointment.category,'') category,
       case when p_crmcloudsync_appointment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
    when l_crmcloudsync_appointment.created_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_appointment.created_by as varchar(36)),'z#@$k%&P'))),2)  end created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_appointment.created_by_name,'') created_by_name,
       case when p_crmcloudsync_appointment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
          when s_crmcloudsync_appointment.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_appointment.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_appointment.bk_hash
       when s_crmcloudsync_appointment.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_appointment.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_appointment.created_on created_on,
       case when p_crmcloudsync_appointment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
    when l_crmcloudsync_appointment.created_on_behalf_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_appointment.created_on_behalf_by as varchar(36)),'z#@$k%&P'))),2)  end created_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_appointment.created_on_behalf_by_name,'') created_on_behalf_by_name,
       isnull(s_crmcloudsync_appointment.description,'') description,
       case when p_crmcloudsync_appointment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
    when l_crmcloudsync_appointment.ltf_club_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_appointment.ltf_club_id as varchar(36)),'z#@$k%&P'))),2)  end dim_crm_ltf_club_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
    when l_crmcloudsync_appointment.owner_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_appointment.owner_id as varchar(36)),'z#@$k%&P'))),2)  end dim_crm_owner_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
    when l_crmcloudsync_appointment.regarding_object_id is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_appointment.regarding_object_id as varchar(36)),'z#@$k%&P'))),2)  end dim_crm_regarding_object_key,
       s_crmcloudsync_appointment.insert_user insert_user,
       s_crmcloudsync_appointment.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_appointment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
          when s_crmcloudsync_appointment.inserted_date_time is null then '-998'
       else convert(varchar, s_crmcloudsync_appointment.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_appointment.bk_hash
       when s_crmcloudsync_appointment.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_appointment.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_crmcloudsync_appointment.instance_type_code instance_type_code,
       isnull(s_crmcloudsync_appointment.instance_type_code_name,'') instance_type_code_name,
       s_crmcloudsync_appointment.ltf_appointment_type ltf_appointment_type,
       isnull(s_crmcloudsync_appointment.ltf_appointment_type_name,'') ltf_appointment_type_name,
       s_crmcloudsync_appointment.ltf_check_in_flag ltf_check_in_flag,
       isnull(s_crmcloudsync_appointment.ltf_check_in_flag_name,'') ltf_check_in_flag_name,
       isnull(s_crmcloudsync_appointment.ltf_club_id_name,'') ltf_club_id_name,
       s_crmcloudsync_appointment.ltf_program ltf_program,
       isnull(s_crmcloudsync_appointment.ltf_program_name,'') ltf_program_name,
       s_crmcloudsync_appointment.ltf_qr_code ltf_qr_code,
       s_crmcloudsync_appointment.ltf_udw_id ltf_udw_id,
       s_crmcloudsync_appointment.ltf_web_booking_source ltf_web_booking_source,
       isnull(s_crmcloudsync_appointment.ltf_web_booking_source_name,'') ltf_web_booking_source_name,
       case when p_crmcloudsync_appointment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
    when l_crmcloudsync_appointment.modified_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_appointment.modified_by as varchar(36)),'z#@$k%&P'))),2)  end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_appointment.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_appointment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
          when s_crmcloudsync_appointment.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_appointment.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_appointment.bk_hash
       when s_crmcloudsync_appointment.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_appointment.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_appointment.modified_on modified_on,
       case when p_crmcloudsync_appointment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
    when l_crmcloudsync_appointment.modified_on_behalf_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_appointment.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2)  end modified_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_appointment.modified_on_behalf_by_name,'') modified_on_behalf_by_name,
       isnull(s_crmcloudsync_appointment.owner_id_name,'') owner_id_name,
       s_crmcloudsync_appointment.owner_id_type owner_id_type,
       l_crmcloudsync_appointment.owning_business_unit owning_business_unit,
       l_crmcloudsync_appointment.owning_team owning_team,
       case when p_crmcloudsync_appointment.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
    when l_crmcloudsync_appointment.owning_user is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_appointment.owning_user as varchar(36)),'z#@$k%&P'))),2)  end owning_user_dim_crm_system_user_key,
       s_crmcloudsync_appointment.priority_code priority_code,
       isnull(s_crmcloudsync_appointment.priority_code_name,'') priority_code_name,
       isnull(s_crmcloudsync_appointment.regarding_object_id_name,'') regarding_object_id_name,
       isnull(s_crmcloudsync_appointment.regarding_object_type_code,'') regarding_object_type_code,
       s_crmcloudsync_appointment.scheduled_duration_minutes scheduled_duration_minutes,
       s_crmcloudsync_appointment.scheduled_end scheduled_end,
       case when p_crmcloudsync_appointment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
          when s_crmcloudsync_appointment.scheduled_end is null then '-998'
       else convert(varchar, s_crmcloudsync_appointment.scheduled_end, 112)    end scheduled_end_dim_date_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_appointment.bk_hash
       when s_crmcloudsync_appointment.scheduled_end is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_appointment.scheduled_end,114), 1, 5),':','') end scheduled_end_dim_time_key,
       s_crmcloudsync_appointment.scheduled_start scheduled_start,
       case when p_crmcloudsync_appointment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
          when s_crmcloudsync_appointment.scheduled_start is null then '-998'
       else convert(varchar, s_crmcloudsync_appointment.scheduled_start, 112)    end scheduled_start_dim_date_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_appointment.bk_hash
       when s_crmcloudsync_appointment.scheduled_start is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_appointment.scheduled_start,114), 1, 5),':','') end scheduled_start_dim_time_key,
       s_crmcloudsync_appointment.state_code state_code,
       isnull(s_crmcloudsync_appointment.state_code_name,'') state_code_name,
       s_crmcloudsync_appointment.status_code status_code,
       isnull(s_crmcloudsync_appointment.status_code_name,'') status_code_name,
       isnull(s_crmcloudsync_appointment.subject,'') subject,
       s_crmcloudsync_appointment.time_zone_rule_version_number time_zone_rule_version_number,
       isnull(s_crmcloudsync_appointment.update_user,'') update_user,
       s_crmcloudsync_appointment.updated_date_time updated_date_time,
       case when p_crmcloudsync_appointment.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_appointment.bk_hash
          when s_crmcloudsync_appointment.updated_date_time is null then '-998'
       else convert(varchar, s_crmcloudsync_appointment.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_appointment.bk_hash in ('-997','-998','-999') then p_crmcloudsync_appointment.bk_hash
       when s_crmcloudsync_appointment.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_appointment.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_crmcloudsync_appointment.utc_conversion_time_zone_code utc_conversion_time_zone_code,
       isnull(h_crmcloudsync_appointment.dv_deleted,0) dv_deleted,
       p_crmcloudsync_appointment.p_crmcloudsync_appointment_id,
       p_crmcloudsync_appointment.dv_batch_id,
       p_crmcloudsync_appointment.dv_load_date_time,
       p_crmcloudsync_appointment.dv_load_end_date_time
  from dbo.h_crmcloudsync_appointment
  join dbo.p_crmcloudsync_appointment
    on h_crmcloudsync_appointment.bk_hash = p_crmcloudsync_appointment.bk_hash
  join #p_crmcloudsync_appointment_insert
    on p_crmcloudsync_appointment.bk_hash = #p_crmcloudsync_appointment_insert.bk_hash
   and p_crmcloudsync_appointment.p_crmcloudsync_appointment_id = #p_crmcloudsync_appointment_insert.p_crmcloudsync_appointment_id
  join dbo.l_crmcloudsync_appointment
    on p_crmcloudsync_appointment.bk_hash = l_crmcloudsync_appointment.bk_hash
   and p_crmcloudsync_appointment.l_crmcloudsync_appointment_id = l_crmcloudsync_appointment.l_crmcloudsync_appointment_id
  join dbo.s_crmcloudsync_appointment
    on p_crmcloudsync_appointment.bk_hash = s_crmcloudsync_appointment.bk_hash
   and p_crmcloudsync_appointment.s_crmcloudsync_appointment_id = s_crmcloudsync_appointment.s_crmcloudsync_appointment_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_appointment
   where d_crmcloudsync_appointment.bk_hash in (select bk_hash from #p_crmcloudsync_appointment_insert)

  insert dbo.d_crmcloudsync_appointment(
             bk_hash,
             fact_crm_appointment_key,
             activity_id,
             activity_type_code,
             activity_type_code_name,
             actual_duration_minutes,
             actual_end,
             actual_end_dim_date_key,
             actual_end_dim_time_key,
             actual_start,
             actual_start_dim_date_key,
             actual_start_dim_time_key,
             category,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             created_on_behalf_by_dim_crm_system_user_key,
             created_on_behalf_by_name,
             description,
             dim_crm_ltf_club_key,
             dim_crm_owner_key,
             dim_crm_regarding_object_key,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             instance_type_code,
             instance_type_code_name,
             ltf_appointment_type,
             ltf_appointment_type_name,
             ltf_check_in_flag,
             ltf_check_in_flag_name,
             ltf_club_id_name,
             ltf_program,
             ltf_program_name,
             ltf_qr_code,
             ltf_udw_id,
             ltf_web_booking_source,
             ltf_web_booking_source_name,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             modified_on_behalf_by_dim_crm_system_user_key,
             modified_on_behalf_by_name,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_team,
             owning_user_dim_crm_system_user_key,
             priority_code,
             priority_code_name,
             regarding_object_id_name,
             regarding_object_type_code,
             scheduled_duration_minutes,
             scheduled_end,
             scheduled_end_dim_date_key,
             scheduled_end_dim_time_key,
             scheduled_start,
             scheduled_start_dim_date_key,
             scheduled_start_dim_time_key,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             subject,
             time_zone_rule_version_number,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             utc_conversion_time_zone_code,
             deleted_flag,
             p_crmcloudsync_appointment_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_crm_appointment_key,
         activity_id,
         activity_type_code,
         activity_type_code_name,
         actual_duration_minutes,
         actual_end,
         actual_end_dim_date_key,
         actual_end_dim_time_key,
         actual_start,
         actual_start_dim_date_key,
         actual_start_dim_time_key,
         category,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         created_on_behalf_by_dim_crm_system_user_key,
         created_on_behalf_by_name,
         description,
         dim_crm_ltf_club_key,
         dim_crm_owner_key,
         dim_crm_regarding_object_key,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         instance_type_code,
         instance_type_code_name,
         ltf_appointment_type,
         ltf_appointment_type_name,
         ltf_check_in_flag,
         ltf_check_in_flag_name,
         ltf_club_id_name,
         ltf_program,
         ltf_program_name,
         ltf_qr_code,
         ltf_udw_id,
         ltf_web_booking_source,
         ltf_web_booking_source_name,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         modified_on_behalf_by_dim_crm_system_user_key,
         modified_on_behalf_by_name,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_team,
         owning_user_dim_crm_system_user_key,
         priority_code,
         priority_code_name,
         regarding_object_id_name,
         regarding_object_type_code,
         scheduled_duration_minutes,
         scheduled_end,
         scheduled_end_dim_date_key,
         scheduled_end_dim_time_key,
         scheduled_start,
         scheduled_start_dim_date_key,
         scheduled_start_dim_time_key,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         subject,
         time_zone_rule_version_number,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         utc_conversion_time_zone_code,
         dv_deleted,
         p_crmcloudsync_appointment_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_appointment)
--Done!
end
