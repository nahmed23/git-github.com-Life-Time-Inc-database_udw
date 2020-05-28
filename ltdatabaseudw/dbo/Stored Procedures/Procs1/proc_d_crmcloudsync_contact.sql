CREATE PROC [dbo].[proc_d_crmcloudsync_contact] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_contact)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_contact_insert') is not null drop table #p_crmcloudsync_contact_insert
create table dbo.#p_crmcloudsync_contact_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_contact.p_crmcloudsync_contact_id,
       p_crmcloudsync_contact.bk_hash
  from dbo.p_crmcloudsync_contact
 where p_crmcloudsync_contact.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_contact.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_contact.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_contact.bk_hash,
       p_crmcloudsync_contact.bk_hash dim_crm_contact_key,
       p_crmcloudsync_contact.contact_id contact_id,
       isnull(s_crmcloudsync_contact.address_1_city,'') address_1_city,
       isnull(s_crmcloudsync_contact.address_1_composite,'') address_1_composite,
       isnull(s_crmcloudsync_contact.address_1_country,'') address_1_country,
       isnull(s_crmcloudsync_contact.address_1_line_1,'') address_1_line_1,
       isnull(s_crmcloudsync_contact.address_1_line_2,'') address_1_line_2,
       isnull(s_crmcloudsync_contact.address_1_line_3,'') address_1_line_3,
       isnull(s_crmcloudsync_contact.address_1_postal_code,'') address_1_postal_code,
       isnull(s_crmcloudsync_contact.address_1_state_or_province,'') address_1_state_or_province,
       isnull(s_crmcloudsync_contact.address_1_telephone_1,'') address_1_telephone_1,
       s_crmcloudsync_contact.birth_date birth_date,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash    
         when s_crmcloudsync_contact.birth_date is null then '-998' 
         when  convert(varchar, s_crmcloudsync_contact.birth_date, 112) > '20991231' then '99991231' 
         when convert(varchar, s_crmcloudsync_contact.birth_date, 112)< '19000101' then '19000101'  
          else convert(varchar, s_crmcloudsync_contact.birth_date, 112)    end birth_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
           when l_crmcloudsync_contact.created_by is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.created_by as varchar(36)),'z#@$k%&P'))),2)
        end created_by_dim_crm_system_user_key,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
          when s_crmcloudsync_contact.created_on is null then '-998'
       else convert(varchar, s_crmcloudsync_contact.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997','-998','-999') then p_crmcloudsync_contact.bk_hash
       when s_crmcloudsync_contact.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_contact.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_contact.created_on created_on,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash 
               when l_crmcloudsync_contact.created_on_behalf_by is null then '-998' 
               else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.created_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end created_on_behalf_by_dim_crm_system_user_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
        when l_crmcloudsync_contact.address_1_address_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.address_1_address_id as varchar(36)),'z#@$k%&P'))),2) 
        end dim_crm_address_1_address_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
           when l_crmcloudsync_contact.ltf_club_id is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.ltf_club_id as varchar(36)),'z#@$k%&P'))),2)
       	end dim_crm_ltf_club_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash      
       when l_crmcloudsync_contact.ltf_employer_id    is  null then '-998'
       when isnumeric(l_crmcloudsync_contact.ltf_employer_id) = 0  then '-999'         
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.ltf_employer_id as varchar(36)),'z#@$k%&P'))),2)   end dim_crm_ltf_employer_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
           when l_crmcloudsync_contact.ltf_ltf_party_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.ltf_ltf_party_id as varchar(36)),'z#@$k%&P'))),2)
        end dim_crm_ltf_ltf_party_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash      
       when l_crmcloudsync_contact.owner_id is null then '-998'      
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.owner_id as varchar(36)),'z#@$k%&P'))),2)   end dim_crm_owner_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
           when l_crmcloudsync_contact.owning_team is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.owning_team as varchar(36)),'z#@$k%&P'))),2)
       	end dim_crm_team_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash    
         when l_crmcloudsync_contact.ltf_referring_member_id is null then '-998'  
        when (isnumeric(l_crmcloudsync_contact.ltf_referring_member_id) = 0 or 
        l_crmcloudsync_contact.ltf_referring_member_id not like '%[0-9]%') or
        len(l_crmcloudsync_contact.ltf_referring_member_id) >=10  then '-999'      
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_contact.ltf_referring_member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_member_key,
       s_crmcloudsync_contact.do_not_email do_not_email,
       case when s_crmcloudsync_contact.do_not_email = 1 then 'Y' else 'N' end do_not_email_flag,
       isnull(s_crmcloudsync_contact.do_not_email_name,'') do_not_email_name,
       s_crmcloudsync_contact.do_not_phone do_not_phone,
       case when s_crmcloudsync_contact.do_not_phone = 1 then 'Y' else 'N' end do_not_phone_flag,
       isnull(s_crmcloudsync_contact.do_not_phone_name,'') do_not_phone_name,
       isnull(s_crmcloudsync_contact.do_not_postal_mail_name,'') do_not_postal_mail_name,
       isnull(s_crmcloudsync_contact.do_not_send_marketing_material_name,'') do_not_send_marketing_material_name,
       isnull(s_crmcloudsync_contact.email_address_1,'') email_address_1,
       isnull(s_crmcloudsync_contact.email_address_2,'') email_address_2,
       isnull(s_crmcloudsync_contact.email_address_3,'') email_address_3,
       l_crmcloudsync_contact.employee_id employee_id,
       isnull(s_crmcloudsync_contact.first_name,'') first_name,
       isnull(s_crmcloudsync_contact.full_name,'') full_name,
       s_crmcloudsync_contact.gender_code gender_code,
       isnull(s_crmcloudsync_contact.gender_code_name,'') gender_code_name,
       s_crmcloudsync_contact.insert_user insert_user,
       s_crmcloudsync_contact.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
          when s_crmcloudsync_contact.inserted_date_time is null then '-998'
       else convert(varchar, s_crmcloudsync_contact.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997','-998','-999') then p_crmcloudsync_contact.bk_hash
       when s_crmcloudsync_contact.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_contact.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       isnull(s_crmcloudsync_contact.last_name,'') last_name,
       isnull(s_crmcloudsync_contact.ltf_age,'') ltf_age,
       isnull(s_crmcloudsync_contact.ltf_alternate_full_name,'') ltf_alternate_full_name,
       s_crmcloudsync_contact.ltf_anniversary_call ltf_anniversary_call,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
          when s_crmcloudsync_contact.ltf_anniversary_call is null then '-998'
       else convert(varchar, s_crmcloudsync_contact.ltf_anniversary_call, 112)    end ltf_anniversary_call_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997','-998','-999') then p_crmcloudsync_contact.bk_hash
       when s_crmcloudsync_contact.ltf_anniversary_call is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_contact.ltf_anniversary_call,114), 1, 5),':','') end ltf_anniversary_call_dim_time_key,
       s_crmcloudsync_contact.ltf_bday_years_difference ltf_bday_years_difference,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
          when s_crmcloudsync_contact.ltf_bday_years_difference is null then '-998'
       else convert(varchar, s_crmcloudsync_contact.ltf_bday_years_difference, 112)    end ltf_bday_years_difference_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997','-998','-999') then p_crmcloudsync_contact.bk_hash
       when s_crmcloudsync_contact.ltf_bday_years_difference is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_contact.ltf_bday_years_difference,114), 1, 5),':','') end ltf_bday_years_difference_dim_time_key,
       isnull(s_crmcloudsync_contact.ltf_birth_year,'') ltf_birth_year,
       s_crmcloudsync_contact.ltf_calculated_age ltf_calculated_age,
       isnull(s_crmcloudsync_contact.ltf_club_id_name,'') ltf_club_id_name,
       s_crmcloudsync_contact.ltf_club_proximity ltf_club_proximity,
       isnull(s_crmcloudsync_contact.ltf_club_proximity_name,'') ltf_club_proximity_name,
       s_crmcloudsync_contact.ltf_commitment_level ltf_commitment_level,
       isnull(s_crmcloudsync_contact.ltf_commitment_level_name,'') ltf_commitment_level_name,
       isnull(s_crmcloudsync_contact.ltf_commitment_reason,'') ltf_commitment_reason,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash     
        when l_crmcloudsync_contact.ltf_connect_member_id is null then '-998'    
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.ltf_connect_member_id as varchar(36)),'z#@$k%&P'))),2)   end ltf_connect_member_dim_mms_member_key,
       isnull(s_crmcloudsync_contact.ltf_connect_member_id_name,'') ltf_connect_member_id_name,
       s_crmcloudsync_contact.ltf_days_since_join_date ltf_days_since_join_date,
       case when s_crmcloudsync_contact.ltf_dn_cover_ride = 1 then 'Y' else 'N' end ltf_dn_cover_ride_flag,
       s_crmcloudsync_contact.ltf_dnc_dne_update_triggered_by ltf_dnc_dne_update_triggered_by,
       isnull(s_crmcloudsync_contact.ltf_dnc_dne_update_triggered_by_name,'') ltf_dnc_dne_update_triggered_by_name,
       s_crmcloudsync_contact.ltf_dn_cover_ride ltf_dnc_over_ride,
       isnull(s_crmcloudsync_contact.ltf_dn_cover_ride_name,'') ltf_dnc_over_ride_name,
       s_crmcloudsync_contact.ltf_dnc_temporary_release_expiration ltf_dnc_temporary_release_expiration,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
          when s_crmcloudsync_contact.ltf_dnc_temporary_release_expiration is null then '-998'
       else convert(varchar, s_crmcloudsync_contact.ltf_dnc_temporary_release_expiration, 112)    end ltf_dnc_temporary_release_expiration_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997','-998','-999') then p_crmcloudsync_contact.bk_hash
       when s_crmcloudsync_contact.ltf_dnc_temporary_release_expiration is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_contact.ltf_dnc_temporary_release_expiration,114), 1, 5),':','') end ltf_dnc_temporary_release_expiration_dim_time_key,
       s_crmcloudsync_contact.ltf_do_not_email_address_1 ltf_do_not_email_address_1,
       case when s_crmcloudsync_contact.ltf_do_not_email_address_1 = 1 then 'Y' else 'N' end ltf_do_not_email_address_1_flag,
       isnull(s_crmcloudsync_contact.ltf_do_not_email_address_1_name,'') ltf_do_not_email_address_1_name,
       s_crmcloudsync_contact.ltf_do_not_email_address_2 ltf_do_not_email_address_2,
       case when s_crmcloudsync_contact.ltf_do_not_email_address_2 = 1 then 'Y' else 'N' end ltf_do_not_email_address_2_flag,
       isnull(s_crmcloudsync_contact.ltf_do_not_email_address_2_name,'') ltf_do_not_email_address_2_name,
       s_crmcloudsync_contact.ltf_do_not_phone_mobile_phone ltf_do_not_phone_mobile_phone,
       case when s_crmcloudsync_contact.ltf_do_not_phone_mobile_phone = 1 then 'Y' else 'N' end ltf_do_not_phone_mobile_phone_flag,
       isnull(s_crmcloudsync_contact.ltf_do_not_phone_mobile_phone_name,'') ltf_do_not_phone_mobile_phone_name,
       s_crmcloudsync_contact.ltf_do_not_phone_telephone_1 ltf_do_not_phone_telephone_1,
       case when s_crmcloudsync_contact.ltf_do_not_phone_telephone_1 = 1 then 'Y' else 'N' end ltf_do_not_phone_telephone_1_flag,
       isnull(s_crmcloudsync_contact.ltf_do_not_phone_telephone_1_name,'') ltf_do_not_phone_telephone_1_name,
       s_crmcloudsync_contact.ltf_do_not_phone_telephone_2 ltf_do_not_phone_telephone_2,
       case when s_crmcloudsync_contact.ltf_do_not_phone_telephone_2 = 1 then 'Y' else 'N' end ltf_do_not_phone_telephone_2_flag,
       isnull(s_crmcloudsync_contact.ltf_do_not_phone_telephone_2_name,'') ltf_do_not_phone_telephone_2_name,
       s_crmcloudsync_contact.ltf_duplicate_over_ride ltf_duplicate_over_ride,
       case when s_crmcloudsync_contact.ltf_duplicate_over_ride = 1 then 'Y' else 'N' end ltf_duplicate_over_ride_flag,
       isnull(s_crmcloudsync_contact.ltf_duplicate_over_ride_name,'') ltf_duplicate_over_ride_name,
       isnull(s_crmcloudsync_contact.ltf_employer_id_name,'') ltf_employer_id_name,
       s_crmcloudsync_contact.ltf_employer_wellness_program ltf_employer_wellness_program,
       isnull(s_crmcloudsync_contact.ltf_employer_wellness_program_name,'') ltf_employer_wellness_program_name,
       s_crmcloudsync_contact.ltf_exercise_history ltf_exercise_history,
       isnull(s_crmcloudsync_contact.ltf_exercise_history_name,'') ltf_exercise_history_name,
       s_crmcloudsync_contact.ltf_injuries_or_limitations ltf_injuries_or_limitations,
       isnull(s_crmcloudsync_contact.ltf_injuries_or_limitations_description,'') ltf_injuries_or_limitations_description,
       case when s_crmcloudsync_contact.ltf_injuries_or_limitations = 1 then 'Y' else 'N' end ltf_injuries_or_limitations_flag,
       isnull(s_crmcloudsync_contact.ltf_injuries_or_limitations_name,'') ltf_injuries_or_limitations_name,
       s_crmcloudsync_contact.ltf_inserted_by_system ltf_inserted_by_system,
       case when s_crmcloudsync_contact.ltf_inserted_by_system = 1 then 'Y' else 'N' end ltf_inserted_by_system_flag,
       isnull(s_crmcloudsync_contact.ltf_inserted_by_system_name,'') ltf_inserted_by_system_name,
       s_crmcloudsync_contact.ltf_is_employee ltf_is_employee,
       case when s_crmcloudsync_contact.ltf_is_employee = 1 then 'Y' else 'N' end ltf_is_employee_flag,
       isnull(s_crmcloudsync_contact.ltf_is_employee_name,'') ltf_is_employee_name,
       s_crmcloudsync_contact.ltf_is_life_time_close_to ltf_is_life_time_close_to,
       isnull(s_crmcloudsync_contact.ltf_is_life_time_close_to_name,'') ltf_is_life_time_close_to_name,
       s_crmcloudsync_contact.ltf_join_date ltf_join_date,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
          when s_crmcloudsync_contact.ltf_join_date is null then '-998'
       else convert(varchar, s_crmcloudsync_contact.ltf_join_date, 112)    end ltf_join_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997','-998','-999') then p_crmcloudsync_contact.bk_hash
       when s_crmcloudsync_contact.ltf_join_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_contact.ltf_join_date,114), 1, 5),':','') end ltf_join_dim_time_key,
       isnull(l_crmcloudsync_contact.ltf_last_contacted_by,'-998') ltf_last_contacted_by,
       isnull(s_crmcloudsync_contact.ltf_last_contacted_by_name,'') ltf_last_contacted_by_name,
       s_crmcloudsync_contact.ltf_lead_source ltf_lead_source,
       isnull(s_crmcloudsync_contact.ltf_lead_source_name,'') ltf_lead_source_name,
       s_crmcloudsync_contact.ltf_lead_type ltf_lead_type,
       isnull(s_crmcloudsync_contact.ltf_lead_type_name,'') ltf_lead_type_name,
       isnull(s_crmcloudsync_contact.ltf_legacy,'') ltf_legacy,
       s_crmcloudsync_contact.ltf_lt_bucks ltf_lt_bucks,
       s_crmcloudsync_contact.ltf_measurable_goal ltf_measurable_goal,
       isnull(s_crmcloudsync_contact.ltf_measurable_goal_name,'') ltf_measurable_goal_name,
       isnull(s_crmcloudsync_contact.ltf_member_type_list,'-998') ltf_member_type_list,
       isnull(s_crmcloudsync_contact.ltf_member_type_list_name,'') ltf_member_type_list_name,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
            when l_crmcloudsync_contact.ltf_most_recent_member_id is null then '-998'
            when (isnumeric(l_crmcloudsync_contact.ltf_most_recent_member_id) = 0 
                  or l_crmcloudsync_contact.ltf_most_recent_member_id not like '%[0-9]%') 
                  or len(l_crmcloudsync_contact.ltf_most_recent_member_id) >=10  then '-999'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_crmcloudsync_contact.ltf_most_recent_member_id as int) as varchar(500)),'z#@$k%&P'))),2)
       	end ltf_most_recent_member_dim_mms_member_key,
       isnull(s_crmcloudsync_contact.ltf_nugget,'') ltf_nugget,
       s_crmcloudsync_contact.ltf_past_trainer_or_coach ltf_past_trainer_or_coach,
       case when s_crmcloudsync_contact.ltf_past_trainer_or_coach = 1 then 'Y' else 'N' end ltf_past_trainer_or_coach_flag,
       isnull(s_crmcloudsync_contact.ltf_past_trainer_or_coach_name,'') ltf_past_trainer_or_coach_name,
       s_crmcloudsync_contact.ltf_primary_objective ltf_primary_objective,
       isnull(s_crmcloudsync_contact.ltf_primary_objective_name,'') ltf_primary_objective_name,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash   
          when l_crmcloudsync_contact.ltf_referring_contact_id is null then '-998'    
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.ltf_referring_contact_id as varchar(36)),'z#@$k%&P'))),2)   end ltf_referring_contact_dim_contact_key,
       isnull(s_crmcloudsync_contact.ltf_referring_contact_id_name,'') ltf_referring_contact_id_name,
       isnull(s_crmcloudsync_contact.ltf_referring_contact_id_yomi_name,'') ltf_referring_contact_id_yomi_name,
       s_crmcloudsync_contact.ltf_risk_score ltf_risk_score,
       s_crmcloudsync_contact.ltf_specific_goal ltf_specific_goal,
       isnull(s_crmcloudsync_contact.ltf_specific_goal_name,'') ltf_specific_goal_name,
       s_crmcloudsync_contact.ltf_star_value ltf_star_value,
       s_crmcloudsync_contact.ltf_time_goal ltf_time_goal,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
          when s_crmcloudsync_contact.ltf_time_goal is null then '-998'
       else convert(varchar, s_crmcloudsync_contact.ltf_time_goal, 112)    end ltf_time_goal_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997','-998','-999') then p_crmcloudsync_contact.bk_hash
       when s_crmcloudsync_contact.ltf_time_goal is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_contact.ltf_time_goal,114), 1, 5),':','') end ltf_time_goal_dim_time_key,
       s_crmcloudsync_contact.ltf_todays_action ltf_todays_action,
       isnull(s_crmcloudsync_contact.ltf_todays_action_name,'') ltf_todays_action_name,
       s_crmcloudsync_contact.ltf_trainer_or_coach_preference ltf_trainer_or_coach_preference,
       isnull(s_crmcloudsync_contact.ltf_trainer_or_coach_preference_name,'') ltf_trainer_or_coach_preference_name,
       isnull(l_crmcloudsync_contact.ltf_udw_id,'-998') ltf_udw_id,
       s_crmcloudsync_contact.ltf_volatile_contact ltf_volatile_contact,
       case when s_crmcloudsync_contact.ltf_volatile_contact = 1 then 'Y' else 'N' end ltf_volatile_contact_flag,
       isnull(s_crmcloudsync_contact.ltf_volatile_contact_name,'') ltf_volatile_contact_name,
       s_crmcloudsync_contact.ltf_workout_preference ltf_workout_preference,
       isnull(s_crmcloudsync_contact.ltf_workout_preference_name,'') ltf_workout_preference_name,
       s_crmcloudsync_contact.ltf_years_of_membership ltf_years_of_membership,
       isnull(s_crmcloudsync_contact.middle_name,'') middle_name,
       isnull(s_crmcloudsync_contact.mobile_phone,'') mobile_phone,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
           when l_crmcloudsync_contact.modified_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.modified_by as varchar(36)),'z#@$k%&P'))),2)
       	end modified_by_dim_crm_system_user_key,
       case when p_crmcloudsync_contact.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
          when s_crmcloudsync_contact.modified_on is null then '-998'
       else convert(varchar, s_crmcloudsync_contact.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_contact.bk_hash in ('-997','-998','-999') then p_crmcloudsync_contact.bk_hash
       when s_crmcloudsync_contact.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_contact.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_contact.modified_on modified_on,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
           when l_crmcloudsync_contact.modified_on_behalf_by is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2)
       	end modified_on_behalf_by_dim_crm_system_user_key,
       isnull(l_crmcloudsync_contact.originating_lead_id,'-998') originating_lead_id,
       isnull(s_crmcloudsync_contact.owner_id_name,'') owner_id_name,
       isnull(s_crmcloudsync_contact.owner_id_type,'') owner_id_type,
       isnull(l_crmcloudsync_contact.owning_business_unit,'-998') owning_business_unit,
       case when p_crmcloudsync_contact.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_contact.bk_hash
           when l_crmcloudsync_contact.owning_user is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_contact.owning_user as varchar(36)),'z#@$k%&P'))),2)
       	end owning_user_dim_crm_system_user_key,
       isnull(s_crmcloudsync_contact.salutation,'') salutation,
       s_crmcloudsync_contact.state_code state_code,
       isnull(s_crmcloudsync_contact.state_code_name,'') state_code_name,
       isnull(s_crmcloudsync_contact.telephone_1,'') telephone_1,
       isnull(s_crmcloudsync_contact.telephone_2,'') telephone_2,
       isnull(s_crmcloudsync_contact.update_user,'') update_user,
       isnull(h_crmcloudsync_contact.dv_deleted,0) dv_deleted,
       p_crmcloudsync_contact.p_crmcloudsync_contact_id,
       p_crmcloudsync_contact.dv_batch_id,
       p_crmcloudsync_contact.dv_load_date_time,
       p_crmcloudsync_contact.dv_load_end_date_time
  from dbo.h_crmcloudsync_contact
  join dbo.p_crmcloudsync_contact
    on h_crmcloudsync_contact.bk_hash = p_crmcloudsync_contact.bk_hash
  join #p_crmcloudsync_contact_insert
    on p_crmcloudsync_contact.bk_hash = #p_crmcloudsync_contact_insert.bk_hash
   and p_crmcloudsync_contact.p_crmcloudsync_contact_id = #p_crmcloudsync_contact_insert.p_crmcloudsync_contact_id
  join dbo.l_crmcloudsync_contact
    on p_crmcloudsync_contact.bk_hash = l_crmcloudsync_contact.bk_hash
   and p_crmcloudsync_contact.l_crmcloudsync_contact_id = l_crmcloudsync_contact.l_crmcloudsync_contact_id
  join dbo.s_crmcloudsync_contact
    on p_crmcloudsync_contact.bk_hash = s_crmcloudsync_contact.bk_hash
   and p_crmcloudsync_contact.s_crmcloudsync_contact_id = s_crmcloudsync_contact.s_crmcloudsync_contact_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_contact
   where d_crmcloudsync_contact.bk_hash in (select bk_hash from #p_crmcloudsync_contact_insert)

  insert dbo.d_crmcloudsync_contact(
             bk_hash,
             dim_crm_contact_key,
             contact_id,
             address_1_city,
             address_1_composite,
             address_1_country,
             address_1_line_1,
             address_1_line_2,
             address_1_line_3,
             address_1_postal_code,
             address_1_state_or_province,
             address_1_telephone_1,
             birth_date,
             birth_dim_date_key,
             created_by_dim_crm_system_user_key,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             created_on_behalf_by_dim_crm_system_user_key,
             dim_crm_address_1_address_key,
             dim_crm_ltf_club_key,
             dim_crm_ltf_employer_key,
             dim_crm_ltf_ltf_party_key,
             dim_crm_owner_key,
             dim_crm_team_key,
             dim_mms_member_key,
             do_not_email,
             do_not_email_flag,
             do_not_email_name,
             do_not_phone,
             do_not_phone_flag,
             do_not_phone_name,
             do_not_postal_mail_name,
             do_not_send_marketing_material_name,
             email_address_1,
             email_address_2,
             email_address_3,
             employee_id,
             first_name,
             full_name,
             gender_code,
             gender_code_name,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             last_name,
             ltf_age,
             ltf_alternate_full_name,
             ltf_anniversary_call,
             ltf_anniversary_call_dim_date_key,
             ltf_anniversary_call_dim_time_key,
             ltf_bday_years_difference,
             ltf_bday_years_difference_dim_date_key,
             ltf_bday_years_difference_dim_time_key,
             ltf_birth_year,
             ltf_calculated_age,
             ltf_club_id_name,
             ltf_club_proximity,
             ltf_club_proximity_name,
             ltf_commitment_level,
             ltf_commitment_level_name,
             ltf_commitment_reason,
             ltf_connect_member_dim_mms_member_key,
             ltf_connect_member_id_name,
             ltf_days_since_join_date,
             ltf_dn_cover_ride_flag,
             ltf_dnc_dne_update_triggered_by,
             ltf_dnc_dne_update_triggered_by_name,
             ltf_dnc_over_ride,
             ltf_dnc_over_ride_name,
             ltf_dnc_temporary_release_expiration,
             ltf_dnc_temporary_release_expiration_dim_date_key,
             ltf_dnc_temporary_release_expiration_dim_time_key,
             ltf_do_not_email_address_1,
             ltf_do_not_email_address_1_flag,
             ltf_do_not_email_address_1_name,
             ltf_do_not_email_address_2,
             ltf_do_not_email_address_2_flag,
             ltf_do_not_email_address_2_name,
             ltf_do_not_phone_mobile_phone,
             ltf_do_not_phone_mobile_phone_flag,
             ltf_do_not_phone_mobile_phone_name,
             ltf_do_not_phone_telephone_1,
             ltf_do_not_phone_telephone_1_flag,
             ltf_do_not_phone_telephone_1_name,
             ltf_do_not_phone_telephone_2,
             ltf_do_not_phone_telephone_2_flag,
             ltf_do_not_phone_telephone_2_name,
             ltf_duplicate_over_ride,
             ltf_duplicate_over_ride_flag,
             ltf_duplicate_over_ride_name,
             ltf_employer_id_name,
             ltf_employer_wellness_program,
             ltf_employer_wellness_program_name,
             ltf_exercise_history,
             ltf_exercise_history_name,
             ltf_injuries_or_limitations,
             ltf_injuries_or_limitations_description,
             ltf_injuries_or_limitations_flag,
             ltf_injuries_or_limitations_name,
             ltf_inserted_by_system,
             ltf_inserted_by_system_flag,
             ltf_inserted_by_system_name,
             ltf_is_employee,
             ltf_is_employee_flag,
             ltf_is_employee_name,
             ltf_is_life_time_close_to,
             ltf_is_life_time_close_to_name,
             ltf_join_date,
             ltf_join_dim_date_key,
             ltf_join_dim_time_key,
             ltf_last_contacted_by,
             ltf_last_contacted_by_name,
             ltf_lead_source,
             ltf_lead_source_name,
             ltf_lead_type,
             ltf_lead_type_name,
             ltf_legacy,
             ltf_lt_bucks,
             ltf_measurable_goal,
             ltf_measurable_goal_name,
             ltf_member_type_list,
             ltf_member_type_list_name,
             ltf_most_recent_member_dim_mms_member_key,
             ltf_nugget,
             ltf_past_trainer_or_coach,
             ltf_past_trainer_or_coach_flag,
             ltf_past_trainer_or_coach_name,
             ltf_primary_objective,
             ltf_primary_objective_name,
             ltf_referring_contact_dim_contact_key,
             ltf_referring_contact_id_name,
             ltf_referring_contact_id_yomi_name,
             ltf_risk_score,
             ltf_specific_goal,
             ltf_specific_goal_name,
             ltf_star_value,
             ltf_time_goal,
             ltf_time_goal_dim_date_key,
             ltf_time_goal_dim_time_key,
             ltf_todays_action,
             ltf_todays_action_name,
             ltf_trainer_or_coach_preference,
             ltf_trainer_or_coach_preference_name,
             ltf_udw_id,
             ltf_volatile_contact,
             ltf_volatile_contact_flag,
             ltf_volatile_contact_name,
             ltf_workout_preference,
             ltf_workout_preference_name,
             ltf_years_of_membership,
             middle_name,
             mobile_phone,
             modified_by_dim_crm_system_user_key,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             modified_on_behalf_by_dim_crm_system_user_key,
             originating_lead_id,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_user_dim_crm_system_user_key,
             salutation,
             state_code,
             state_code_name,
             telephone_1,
             telephone_2,
             update_user,
             deleted_flag,
             p_crmcloudsync_contact_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_contact_key,
         contact_id,
         address_1_city,
         address_1_composite,
         address_1_country,
         address_1_line_1,
         address_1_line_2,
         address_1_line_3,
         address_1_postal_code,
         address_1_state_or_province,
         address_1_telephone_1,
         birth_date,
         birth_dim_date_key,
         created_by_dim_crm_system_user_key,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         created_on_behalf_by_dim_crm_system_user_key,
         dim_crm_address_1_address_key,
         dim_crm_ltf_club_key,
         dim_crm_ltf_employer_key,
         dim_crm_ltf_ltf_party_key,
         dim_crm_owner_key,
         dim_crm_team_key,
         dim_mms_member_key,
         do_not_email,
         do_not_email_flag,
         do_not_email_name,
         do_not_phone,
         do_not_phone_flag,
         do_not_phone_name,
         do_not_postal_mail_name,
         do_not_send_marketing_material_name,
         email_address_1,
         email_address_2,
         email_address_3,
         employee_id,
         first_name,
         full_name,
         gender_code,
         gender_code_name,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         last_name,
         ltf_age,
         ltf_alternate_full_name,
         ltf_anniversary_call,
         ltf_anniversary_call_dim_date_key,
         ltf_anniversary_call_dim_time_key,
         ltf_bday_years_difference,
         ltf_bday_years_difference_dim_date_key,
         ltf_bday_years_difference_dim_time_key,
         ltf_birth_year,
         ltf_calculated_age,
         ltf_club_id_name,
         ltf_club_proximity,
         ltf_club_proximity_name,
         ltf_commitment_level,
         ltf_commitment_level_name,
         ltf_commitment_reason,
         ltf_connect_member_dim_mms_member_key,
         ltf_connect_member_id_name,
         ltf_days_since_join_date,
         ltf_dn_cover_ride_flag,
         ltf_dnc_dne_update_triggered_by,
         ltf_dnc_dne_update_triggered_by_name,
         ltf_dnc_over_ride,
         ltf_dnc_over_ride_name,
         ltf_dnc_temporary_release_expiration,
         ltf_dnc_temporary_release_expiration_dim_date_key,
         ltf_dnc_temporary_release_expiration_dim_time_key,
         ltf_do_not_email_address_1,
         ltf_do_not_email_address_1_flag,
         ltf_do_not_email_address_1_name,
         ltf_do_not_email_address_2,
         ltf_do_not_email_address_2_flag,
         ltf_do_not_email_address_2_name,
         ltf_do_not_phone_mobile_phone,
         ltf_do_not_phone_mobile_phone_flag,
         ltf_do_not_phone_mobile_phone_name,
         ltf_do_not_phone_telephone_1,
         ltf_do_not_phone_telephone_1_flag,
         ltf_do_not_phone_telephone_1_name,
         ltf_do_not_phone_telephone_2,
         ltf_do_not_phone_telephone_2_flag,
         ltf_do_not_phone_telephone_2_name,
         ltf_duplicate_over_ride,
         ltf_duplicate_over_ride_flag,
         ltf_duplicate_over_ride_name,
         ltf_employer_id_name,
         ltf_employer_wellness_program,
         ltf_employer_wellness_program_name,
         ltf_exercise_history,
         ltf_exercise_history_name,
         ltf_injuries_or_limitations,
         ltf_injuries_or_limitations_description,
         ltf_injuries_or_limitations_flag,
         ltf_injuries_or_limitations_name,
         ltf_inserted_by_system,
         ltf_inserted_by_system_flag,
         ltf_inserted_by_system_name,
         ltf_is_employee,
         ltf_is_employee_flag,
         ltf_is_employee_name,
         ltf_is_life_time_close_to,
         ltf_is_life_time_close_to_name,
         ltf_join_date,
         ltf_join_dim_date_key,
         ltf_join_dim_time_key,
         ltf_last_contacted_by,
         ltf_last_contacted_by_name,
         ltf_lead_source,
         ltf_lead_source_name,
         ltf_lead_type,
         ltf_lead_type_name,
         ltf_legacy,
         ltf_lt_bucks,
         ltf_measurable_goal,
         ltf_measurable_goal_name,
         ltf_member_type_list,
         ltf_member_type_list_name,
         ltf_most_recent_member_dim_mms_member_key,
         ltf_nugget,
         ltf_past_trainer_or_coach,
         ltf_past_trainer_or_coach_flag,
         ltf_past_trainer_or_coach_name,
         ltf_primary_objective,
         ltf_primary_objective_name,
         ltf_referring_contact_dim_contact_key,
         ltf_referring_contact_id_name,
         ltf_referring_contact_id_yomi_name,
         ltf_risk_score,
         ltf_specific_goal,
         ltf_specific_goal_name,
         ltf_star_value,
         ltf_time_goal,
         ltf_time_goal_dim_date_key,
         ltf_time_goal_dim_time_key,
         ltf_todays_action,
         ltf_todays_action_name,
         ltf_trainer_or_coach_preference,
         ltf_trainer_or_coach_preference_name,
         ltf_udw_id,
         ltf_volatile_contact,
         ltf_volatile_contact_flag,
         ltf_volatile_contact_name,
         ltf_workout_preference,
         ltf_workout_preference_name,
         ltf_years_of_membership,
         middle_name,
         mobile_phone,
         modified_by_dim_crm_system_user_key,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         modified_on_behalf_by_dim_crm_system_user_key,
         originating_lead_id,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_user_dim_crm_system_user_key,
         salutation,
         state_code,
         state_code_name,
         telephone_1,
         telephone_2,
         update_user,
         dv_deleted,
         p_crmcloudsync_contact_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_contact)
--Done!
end
