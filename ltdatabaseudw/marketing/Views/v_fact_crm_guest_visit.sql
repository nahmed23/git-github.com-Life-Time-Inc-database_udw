﻿CREATE VIEW [marketing].[v_fact_crm_guest_visit]
AS select d_crmcloudsync_ltf_guest_visit.dim_crm_ltf_guest_visit_key dim_crm_ltf_guest_visit_key,
       d_crmcloudsync_ltf_guest_visit.activity_id activity_id,
       d_crmcloudsync_ltf_guest_visit.activity_type_code activity_type_code,
       d_crmcloudsync_ltf_guest_visit.activity_type_code_name activity_type_code_name,
       d_crmcloudsync_ltf_guest_visit.created_by_dim_crm_system_user_key created_by_dim_crm_system_user_key,
       d_crmcloudsync_ltf_guest_visit.created_dim_date_key created_dim_date_key,
       d_crmcloudsync_ltf_guest_visit.created_dim_time_key created_dim_time_key,
       d_crmcloudsync_ltf_guest_visit.created_on created_on,
       d_crmcloudsync_ltf_guest_visit.created_on_behalf_by_dim_crm_system_user_key created_on_behalf_by_dim_crm_system_user_key,
       d_crmcloudsync_ltf_guest_visit.description description,
       d_crmcloudsync_ltf_guest_visit.dim_crm_lead_key dim_crm_lead_key,
       d_crmcloudsync_ltf_guest_visit.dim_crm_ltf_campaign_instance_key dim_crm_ltf_campaign_instance_key,
       d_crmcloudsync_ltf_guest_visit.dim_crm_ltf_club_key dim_crm_ltf_club_key,
       d_crmcloudsync_ltf_guest_visit.dim_crm_owner_key dim_crm_owner_key,
       d_crmcloudsync_ltf_guest_visit.insert_user insert_user,
       d_crmcloudsync_ltf_guest_visit.inserted_date_time inserted_date_time,
       d_crmcloudsync_ltf_guest_visit.inserted_dim_date_key inserted_dim_date_key,
       d_crmcloudsync_ltf_guest_visit.inserted_dim_time_key inserted_dim_time_key,
       d_crmcloudsync_ltf_guest_visit.instance_type_code instance_type_code,
       d_crmcloudsync_ltf_guest_visit.instance_type_code_name instance_type_code_name,
       d_crmcloudsync_ltf_guest_visit.ltf_address_1_city ltf_address_1_city,
       d_crmcloudsync_ltf_guest_visit.ltf_address_1_county ltf_address_1_county,
       d_crmcloudsync_ltf_guest_visit.ltf_address_1_line_1 ltf_address_1_line_1,
       d_crmcloudsync_ltf_guest_visit.ltf_address_1_postal_code ltf_address_1_postal_code,
       d_crmcloudsync_ltf_guest_visit.ltf_address_1_state_or_province ltf_address_1_state_or_province,
       d_crmcloudsync_ltf_guest_visit.ltf_agreement_signature ltf_agreement_signature,
       d_crmcloudsync_ltf_guest_visit.ltf_appointment_dim_crm_activity_key ltf_appointment_dim_crm_activity_key,
       d_crmcloudsync_ltf_guest_visit.ltf_appointment_id_name ltf_appointment_id_name,
       d_crmcloudsync_ltf_guest_visit.ltf_assigned_mea_dim_crm_account_manager_key ltf_assigned_mea_dim_crm_account_manager_key,
       d_crmcloudsync_ltf_guest_visit.ltf_assigned_mea_name ltf_assigned_mea_name,
       d_crmcloudsync_ltf_guest_visit.ltf_campaign_instance_name ltf_campaign_instance_name,
       d_crmcloudsync_ltf_guest_visit.ltf_club_close_to ltf_club_close_to,
       d_crmcloudsync_ltf_guest_visit.ltf_club_close_to_name ltf_club_close_to_name,
       d_crmcloudsync_ltf_guest_visit.ltf_date_of_birth ltf_date_of_birth,
       d_crmcloudsync_ltf_guest_visit.ltf_date_of_birth_dim_date_key ltf_date_of_birth_dim_date_key,
       d_crmcloudsync_ltf_guest_visit.ltf_date_of_birth_dim_time_key ltf_date_of_birth_dim_time_key,
       d_crmcloudsync_ltf_guest_visit.ltf_deduct_guest_priv ltf_deduct_guest_priv,
       d_crmcloudsync_ltf_guest_visit.ltf_email_address_1 ltf_email_address_1,
       d_crmcloudsync_ltf_guest_visit.ltf_employer ltf_employer,
       d_crmcloudsync_ltf_guest_visit.ltf_first_name ltf_first_name,
       d_crmcloudsync_ltf_guest_visit.ltf_gender ltf_gender,
       d_crmcloudsync_ltf_guest_visit.ltf_gender_name ltf_gender_name,
       d_crmcloudsync_ltf_guest_visit.ltf_guest_type ltf_guest_type,
       d_crmcloudsync_ltf_guest_visit.ltf_guest_type_name ltf_guest_type_name,
       d_crmcloudsync_ltf_guest_visit.ltf_interests ltf_interests,
       d_crmcloudsync_ltf_guest_visit.ltf_last_name ltf_last_name,
       d_crmcloudsync_ltf_guest_visit.ltf_line_of_business ltf_line_of_business,
       d_crmcloudsync_ltf_guest_visit.ltf_line_of_business_name ltf_line_of_business_name,
       d_crmcloudsync_ltf_guest_visit.ltf_matching_contact_count ltf_matching_contact_count,
       d_crmcloudsync_ltf_guest_visit.ltf_matching_lead_count ltf_matching_lead_count,
       d_crmcloudsync_ltf_guest_visit.ltf_membership_interest ltf_membership_interest,
       d_crmcloudsync_ltf_guest_visit.ltf_membership_interest_name ltf_membership_interest_name,
       d_crmcloudsync_ltf_guest_visit.ltf_middle_name ltf_middle_name,
       d_crmcloudsync_ltf_guest_visit.ltf_mobile_phone ltf_mobile_phone,
       d_crmcloudsync_ltf_guest_visit.ltf_online ltf_online,
       d_crmcloudsync_ltf_guest_visit.ltf_online_name ltf_online_name,
       d_crmcloudsync_ltf_guest_visit.ltf_out_of_area ltf_out_of_area,
       d_crmcloudsync_ltf_guest_visit.ltf_out_of_area_name ltf_out_of_area_name,
       d_crmcloudsync_ltf_guest_visit.ltf_party_dim_crm_ltf_party_key ltf_party_dim_crm_ltf_party_key,
       d_crmcloudsync_ltf_guest_visit.ltf_qr_code_used ltf_qr_code_used,
       d_crmcloudsync_ltf_guest_visit.ltf_referral_source ltf_referral_source,
       d_crmcloudsync_ltf_guest_visit.ltf_referral_source_name ltf_referral_source_name,
       d_crmcloudsync_ltf_guest_visit.ltf_referred_by_dim_crm_system_user_key ltf_referred_by_dim_crm_system_user_key,
       d_crmcloudsync_ltf_guest_visit.ltf_referred_by_name ltf_referred_by_name,
       d_crmcloudsync_ltf_guest_visit.ltf_request_id ltf_request_id,
       d_crmcloudsync_ltf_guest_visit.ltf_same_day ltf_same_day,
       d_crmcloudsync_ltf_guest_visit.ltf_same_day_name ltf_same_day_name,
       d_crmcloudsync_ltf_guest_visit.ltf_source ltf_source,
       d_crmcloudsync_ltf_guest_visit.ltf_telephone1 ltf_telephone1,
       d_crmcloudsync_ltf_guest_visit.ltf_telephone2 ltf_telephone2,
       d_crmcloudsync_ltf_guest_visit.modified_by_dim_crm_system_user_key modified_by_dim_crm_system_user_key,
       d_crmcloudsync_ltf_guest_visit.modified_dim_date_key modified_dim_date_key,
       d_crmcloudsync_ltf_guest_visit.modified_dim_time_key modified_dim_time_key,
       d_crmcloudsync_ltf_guest_visit.modified_on modified_on,
       d_crmcloudsync_ltf_guest_visit.modified_on_behalf_by_dim_crm_system_user_key modified_on_behalf_by_dim_crm_system_user_key,
       d_crmcloudsync_ltf_guest_visit.new_club_name_dim_crm_ltf_club_key new_club_name_dim_crm_ltf_club_key,
       d_crmcloudsync_ltf_guest_visit.overridden_created_dim_date_key overridden_created_dim_date_key,
       d_crmcloudsync_ltf_guest_visit.overridden_created_dim_time_key overridden_created_dim_time_key,
       d_crmcloudsync_ltf_guest_visit.overridden_created_on overridden_created_on,
       d_crmcloudsync_ltf_guest_visit.owner_id_name owner_id_name,
       d_crmcloudsync_ltf_guest_visit.owner_id_type owner_id_type,
       d_crmcloudsync_ltf_guest_visit.owning_business_unit owning_business_unit,
       d_crmcloudsync_ltf_guest_visit.owning_team owning_team,
       d_crmcloudsync_ltf_guest_visit.owning_user owning_user,
       d_crmcloudsync_ltf_guest_visit.referring_dim_mms_member_key referring_dim_mms_member_key,
       d_crmcloudsync_ltf_guest_visit.regarding_object_dim_crm_system_user_key regarding_object_dim_crm_system_user_key,
       d_crmcloudsync_ltf_guest_visit.regarding_object_id regarding_object_id,
       d_crmcloudsync_ltf_guest_visit.regarding_object_id_name regarding_object_id_name,
       d_crmcloudsync_ltf_guest_visit.regarding_object_type_code regarding_object_type_code,
       d_crmcloudsync_ltf_guest_visit.state_code state_code,
       d_crmcloudsync_ltf_guest_visit.state_code_name state_code_name,
       d_crmcloudsync_ltf_guest_visit.status_code status_code,
       d_crmcloudsync_ltf_guest_visit.status_code_name status_code_name,
       d_crmcloudsync_ltf_guest_visit.subject subject,
       d_crmcloudsync_ltf_guest_visit.update_user update_user,
       d_crmcloudsync_ltf_guest_visit.updated_date_time updated_date_time,
       d_crmcloudsync_ltf_guest_visit.updated_dim_date_key updated_dim_date_key,
       d_crmcloudsync_ltf_guest_visit.updated_dim_time_key updated_dim_time_key
 from d_crmcloudsync_ltf_guest_visit    join d_crmcloudsync_ltf_club 
on d_crmcloudsync_ltf_guest_visit.dim_crm_ltf_club_key = d_crmcloudsync_ltf_club.dim_crm_ltf_club_key;