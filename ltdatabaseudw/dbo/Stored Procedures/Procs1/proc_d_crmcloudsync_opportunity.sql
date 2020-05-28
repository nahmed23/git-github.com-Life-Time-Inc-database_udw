CREATE PROC [dbo].[proc_d_crmcloudsync_opportunity] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_opportunity)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_crmcloudsync_opportunity_insert') is not null drop table #p_crmcloudsync_opportunity_insert
create table dbo.#p_crmcloudsync_opportunity_insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_opportunity.p_crmcloudsync_opportunity_id,
       p_crmcloudsync_opportunity.bk_hash
  from dbo.p_crmcloudsync_opportunity
 where p_crmcloudsync_opportunity.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_crmcloudsync_opportunity.dv_batch_id > @max_dv_batch_id
        or p_crmcloudsync_opportunity.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_crmcloudsync_opportunity.bk_hash,
       p_crmcloudsync_opportunity.bk_hash dim_crm_opportunity_key,
       p_crmcloudsync_opportunity.opportunity_id opportunity_id,
       s_crmcloudsync_opportunity.actual_close_date actual_close_date,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.actual_close_date is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.actual_close_date, 112)    end actual_close_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.actual_close_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.actual_close_date,114), 1, 5),':','') end actual_close_dim_time_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.created_by is null then '-998'
 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.created_by as varchar(36)),'z#@$k%&P'))),2) end created_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_opportunity.created_by_name,'') created_by_name,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.created_on, 112)    end created_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.created_on,114), 1, 5),':','') end created_dim_time_key,
       s_crmcloudsync_opportunity.created_on created_on,
       isnull(s_crmcloudsync_opportunity.description,'') description,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.ltf_club_id is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.ltf_club_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_ltf_club_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
            when l_crmcloudsync_opportunity.owner_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.owner_id as varchar(36)),'z#@$k%&P'))),2) end dim_crm_owner_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.owning_team is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.owning_team as varchar(36)),'z#@$k%&P'))),2) end dim_crm_team_key,
       isnull(s_crmcloudsync_opportunity.insert_user,'') insert_user,
       s_crmcloudsync_opportunity.inserted_date_time inserted_date_time,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.inserted_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_crmcloudsync_opportunity.ltf_assigned_by_app ltf_assigned_by_app,
       isnull(s_crmcloudsync_opportunity.ltf_assigned_by_app_name,'') ltf_assigned_by_app_name,
       s_crmcloudsync_opportunity.ltf_assignment_request_date ltf_assignment_request_date,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
        when s_crmcloudsync_opportunity.ltf_assignment_request_date is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.ltf_assignment_request_date, 112)    end ltf_assignment_request_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.ltf_assignment_request_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.ltf_assignment_request_date,114), 1, 5),':','') end ltf_assignment_request_dim_time_key,
       s_crmcloudsync_opportunity.ltf_assignment_request_id ltf_assignment_request_id,
       s_crmcloudsync_opportunity.ltf_channel ltf_channel,
       s_crmcloudsync_opportunity.ltf_channel_name ltf_channel_name,
       isnull(s_crmcloudsync_opportunity.ltf_club_id_name,'') ltf_club_id_name,
       s_crmcloudsync_opportunity.ltf_club_proximity ltf_club_proximity,
       isnull(s_crmcloudsync_opportunity.ltf_club_proximity_name,'') ltf_club_proximity_name,
       s_crmcloudsync_opportunity.ltf_commitment_level ltf_commitment_level,
       isnull(s_crmcloudsync_opportunity.ltf_commitment_level_name,'') ltf_commitment_level_name,
       isnull(s_crmcloudsync_opportunity.ltf_commitment_reason,'') ltf_commitment_reason,
       s_crmcloudsync_opportunity.ltf_exercise_history ltf_exercise_history,
       isnull(s_crmcloudsync_opportunity.ltf_exercise_history_name,'') ltf_exercise_history_name,
       s_crmcloudsync_opportunity.ltf_guest_pass_expiration_date ltf_guest_pass_expiration_date,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.ltf_guest_pass_expiration_date is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.ltf_guest_pass_expiration_date, 112)    end ltf_guest_pass_expiration_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.ltf_guest_pass_expiration_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.ltf_guest_pass_expiration_date,114), 1, 5),':','') end ltf_guest_pass_expiration_dim_time_key,
       isnull(s_crmcloudsync_opportunity.ltf_ims_join_link,'') ltf_ims_join_link,
       s_crmcloudsync_opportunity.ltf_ims_join_send_date ltf_ims_join_send_date,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.ltf_ims_join_send_date is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.ltf_ims_join_send_date, 112)    end ltf_ims_join_send_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.ltf_ims_join_send_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.ltf_ims_join_send_date,114), 1, 5),':','') end ltf_ims_join_send_dim_time_key,
       s_crmcloudsync_opportunity.ltf_injuries_or_limitations ltf_injuries_or_limitations,
       isnull(s_crmcloudsync_opportunity.ltf_injuries_or_limitations_description,'') ltf_injuries_or_limitations_description,
       case when s_crmcloudsync_opportunity.ltf_injuries_or_limitations = 1 then 'Y'        else 'N'  end ltf_injuries_or_limitations_flag,
       isnull(s_crmcloudsync_opportunity.ltf_injuries_or_limitations_name,'') ltf_injuries_or_limitations_name,
       s_crmcloudsync_opportunity.ltf_is_ims_join ltf_is_ims_join,
       case when s_crmcloudsync_opportunity.ltf_is_ims_join = 1 then 'Y'        else 'N'  end ltf_is_ims_join_flag,
       isnull(s_crmcloudsync_opportunity.ltf_is_ims_join_name,'') ltf_is_ims_join_name,
       s_crmcloudsync_opportunity.ltf_last_activity ltf_last_activity,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.ltf_last_activity is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.ltf_last_activity, 112)    end ltf_last_activity_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.ltf_last_activity is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.ltf_last_activity,114), 1, 5),':','') end ltf_last_activity_dim_time_key,
       s_crmcloudsync_opportunity.ltf_lead_source ltf_lead_source,
       isnull(s_crmcloudsync_opportunity.ltf_lead_source_name,'') ltf_lead_source_name,
       s_crmcloudsync_opportunity.ltf_lead_type ltf_lead_type,
       isnull(s_crmcloudsync_opportunity.ltf_lead_type_name,'') ltf_lead_type_name,
       s_crmcloudsync_opportunity.ltf_line_of_business ltf_line_of_business,
       s_crmcloudsync_opportunity.ltf_line_of_business_name ltf_line_of_business_name,
       s_crmcloudsync_opportunity.ltf_managed_until ltf_managed_until,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.ltf_managed_until is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.ltf_managed_until, 112)    end ltf_managed_until_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.ltf_managed_until is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.ltf_managed_until,114), 1, 5),':','') end ltf_managed_until_dim_time_key,
       s_crmcloudsync_opportunity.ltf_measurable_goal ltf_measurable_goal,
       isnull(s_crmcloudsync_opportunity.ltf_measurable_goal_name,'') ltf_measurable_goal_name,
       s_crmcloudsync_opportunity.ltf_membership_level ltf_membership_level,
       isnull(s_crmcloudsync_opportunity.ltf_membership_level_name,'') ltf_membership_level_name,
       s_crmcloudsync_opportunity.ltf_membership_type ltf_membership_type,
       isnull(s_crmcloudsync_opportunity.ltf_membership_type_name,'') ltf_membership_type_name,
       s_crmcloudsync_opportunity.ltf_next_follow_up ltf_next_follow_up,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.ltf_next_follow_up is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.ltf_next_follow_up, 112)    end ltf_next_follow_up_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.ltf_next_follow_up is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.ltf_next_follow_up,114), 1, 5),':','') end ltf_next_follow_up_dim_time_key,
       s_crmcloudsync_opportunity.ltf_number_over_14_list ltf_number_over_14_list,
       isnull(s_crmcloudsync_opportunity.ltf_number_over_14_list_name,'') ltf_number_over_14_list_name,
       s_crmcloudsync_opportunity.ltf_number_under_14_list ltf_number_under_14_list,
       isnull(s_crmcloudsync_opportunity.ltf_number_under_14_list_name,'') ltf_number_under_14_list_name,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.ltf_originating_guest_visit is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.ltf_originating_guest_visit as varchar(36)),'z#@$k%&P'))),2) end ltf_originating_guest_visit_fact_crm_guest_visit_key,
       isnull(s_crmcloudsync_opportunity.ltf_originating_guest_visit_name,'') ltf_originating_guest_visit_name,
       s_crmcloudsync_opportunity.ltf_park ltf_park,
       isnull(s_crmcloudsync_opportunity.ltf_park_comments,'') ltf_park_comments,
       case when s_crmcloudsync_opportunity.ltf_park = 1 then 'Y'        else 'N'  end ltf_park_flag,
       isnull(s_crmcloudsync_opportunity.ltf_park_name,'') ltf_park_name,
       s_crmcloudsync_opportunity.ltf_park_reason ltf_park_reason,
       isnull(s_crmcloudsync_opportunity.ltf_park_reason_name,'') ltf_park_reason_name,
       s_crmcloudsync_opportunity.ltf_park_until ltf_park_until,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.ltf_park_until is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.ltf_park_until, 112)    end ltf_park_until_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.ltf_park_until is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.ltf_park_until,114), 1, 5),':','') end ltf_park_until_dim_time_key,
       s_crmcloudsync_opportunity.ltf_past_trainer_or_coach ltf_past_trainer_or_coach,
       case when s_crmcloudsync_opportunity.ltf_past_trainer_or_coach = 1 then 'Y'        else 'N'  end ltf_past_trainer_or_coach_flag,
       isnull(s_crmcloudsync_opportunity.ltf_past_trainer_or_coach_name,'') ltf_past_trainer_or_coach_name,
       s_crmcloudsync_opportunity.ltf_primary_objective ltf_primary_objective,
       isnull(s_crmcloudsync_opportunity.ltf_primary_objective_name,'') ltf_primary_objective_name,
       isnull(s_crmcloudsync_opportunity.ltf_profile_notes,'') ltf_profile_notes,
       s_crmcloudsync_opportunity.ltf_programs_of_interest ltf_programs_of_interest,
       isnull(s_crmcloudsync_opportunity.ltf_programs_of_interest_name,'') ltf_programs_of_interest_name,
       isnull(s_crmcloudsync_opportunity.ltf_promo_code,'') ltf_promo_code,
       isnull(s_crmcloudsync_opportunity.ltf_promo_quoted,'') ltf_promo_quoted,
       s_crmcloudsync_opportunity.ltf_ready_to_join ltf_ready_to_join,
       case when s_crmcloudsync_opportunity.ltf_ready_to_join = 1 then 'Y'        else 'N'  end ltf_ready_to_join_flag,
       isnull(s_crmcloudsync_opportunity.ltf_ready_to_join_name,'') ltf_ready_to_join_name,
       s_crmcloudsync_opportunity.ltf_recommended_membership ltf_recommended_membership,
       isnull(s_crmcloudsync_opportunity.ltf_recommended_membership_name,'') ltf_recommended_membership_name,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.ltf_referring_contact_id is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.ltf_referring_contact_id as varchar(36)),'z#@$k%&P'))),2) end ltf_referring_contact_dim_crm_contact_key,
       isnull(s_crmcloudsync_opportunity.ltf_referring_contact_id_name,'') ltf_referring_contact_id_name,
       l_crmcloudsync_opportunity.ltf_referring_member_id ltf_referring_member_id,
       s_crmcloudsync_opportunity.ltf_resistance ltf_resistance,
       isnull(s_crmcloudsync_opportunity.ltf_resistance_name,'') ltf_resistance_name,
       s_crmcloudsync_opportunity.ltf_specific_goal ltf_specific_goal,
       isnull(s_crmcloudsync_opportunity.ltf_specific_goal_name,'') ltf_specific_goal_name,
       s_crmcloudsync_opportunity.ltf_time_goal ltf_time_goal,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.ltf_time_goal is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.ltf_time_goal, 112)    end ltf_time_goal_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.ltf_time_goal is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.ltf_time_goal,114), 1, 5),':','') end ltf_time_goal_dim_time_key,
       s_crmcloudsync_opportunity.ltf_todays_action ltf_todays_action,
       isnull(s_crmcloudsync_opportunity.ltf_todays_action_name,'') ltf_todays_action_name,
       s_crmcloudsync_opportunity.ltf_trainer_or_coach_preference ltf_trainer_or_coach_preference,
       isnull(s_crmcloudsync_opportunity.ltf_trainer_or_coach_preference_name,'') ltf_trainer_or_coach_preference_name,
       isnull(l_crmcloudsync_opportunity.ltf_visitor_id,'') ltf_visitor_id,
       s_crmcloudsync_opportunity.ltf_want_to_do ltf_want_to_do,
       isnull(s_crmcloudsync_opportunity.ltf_want_to_do_name,'') ltf_want_to_do_name,
       l_crmcloudsync_opportunity.ltf_web_team_id ltf_web_team_id,
       isnull(s_crmcloudsync_opportunity.ltf_web_team_id_name,'') ltf_web_team_id_name,
       s_crmcloudsync_opportunity.ltf_web_transfer_method ltf_web_transfer_method,
       isnull(s_crmcloudsync_opportunity.ltf_web_transfer_method_name,'') ltf_web_transfer_method_name,
       isnull(s_crmcloudsync_opportunity.ltf_who_met_with,'') ltf_who_met_with,
       s_crmcloudsync_opportunity.ltf_why_want_to_do ltf_why_want_to_do,
       isnull(s_crmcloudsync_opportunity.ltf_why_want_to_do_name,'') ltf_why_want_to_do_name,
       s_crmcloudsync_opportunity.ltf_workout_preference ltf_workout_preference,
       isnull(s_crmcloudsync_opportunity.ltf_workout_preference_name,'') ltf_workout_preference_name,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.modified_by is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.modified_by as varchar(36)),'z#@$k%&P'))),2) end modified_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_opportunity.modified_by_name,'') modified_by_name,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.modified_on is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.modified_on, 112)    end modified_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.modified_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.modified_on,114), 1, 5),':','') end modified_dim_time_key,
       s_crmcloudsync_opportunity.modified_on modified_on,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.modified_on_behalf_by is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.modified_on_behalf_by as varchar(36)),'z#@$k%&P'))),2) end modified_on_behalf_by_dim_crm_system_user_key,
       isnull(s_crmcloudsync_opportunity.name,'') name,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.originating_lead_id is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.originating_lead_id as varchar(36)),'z#@$k%&P'))),2) end originating_lead_dim_crm_lead_key,
       isnull(s_crmcloudsync_opportunity.originating_lead_id_name,'') originating_lead_id_name,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
           when s_crmcloudsync_opportunity.overridden_created_on is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.overridden_created_on, 112)    end overridden_created_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.overridden_created_on is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.overridden_created_on,114), 1, 5),':','') end overridden_created_dim_time_key,
       s_crmcloudsync_opportunity.overridden_created_on overridden_created_on,
       l_crmcloudsync_opportunity.owner_id owner_id,
       isnull(s_crmcloudsync_opportunity.owner_id_name,'') owner_id_name,
       isnull(s_crmcloudsync_opportunity.owner_id_type,'') owner_id_type,
       l_crmcloudsync_opportunity.owning_business_unit owning_business_unit,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.owning_user is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.owning_user as varchar(36)),'z#@$k%&P'))),2) end owning_user_dim_crm_system_user_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.parent_account_id is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.parent_account_id as varchar(36)),'z#@$k%&P'))),2) end parent_account_dim_crm_account_key,
       isnull(s_crmcloudsync_opportunity.parent_account_id_name,'') parent_account_id_name,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
    when l_crmcloudsync_opportunity.parent_contact_id is null then '-998'
    else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_crmcloudsync_opportunity.parent_contact_id as varchar(36)),'z#@$k%&P'))),2) end parent_contact_dim_crm_contact_key,
       isnull(s_crmcloudsync_opportunity.parent_contact_id_name,'') parent_contact_id_name,
       s_crmcloudsync_opportunity.state_code state_code,
       isnull(s_crmcloudsync_opportunity.state_code_name,'') state_code_name,
       s_crmcloudsync_opportunity.status_code status_code,
       isnull(s_crmcloudsync_opportunity.status_code_name,'') status_code_name,
       s_crmcloudsync_opportunity.total_amount total_amount,
       isnull(s_crmcloudsync_opportunity.update_user,'') update_user,
       s_crmcloudsync_opportunity.updated_date_time updated_date_time,
       case when p_crmcloudsync_opportunity.bk_hash in('-997', '-998', '-999') then p_crmcloudsync_opportunity.bk_hash
        when s_crmcloudsync_opportunity.updated_date_time is null then '-998'
        else convert(varchar, s_crmcloudsync_opportunity.updated_date_time, 112)    end updated_dim_date_key,
       case when p_crmcloudsync_opportunity.bk_hash in ('-997','-998','-999') then p_crmcloudsync_opportunity.bk_hash
       when s_crmcloudsync_opportunity.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_crmcloudsync_opportunity.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_crmcloudsync_opportunity.dv_deleted,0) dv_deleted,
       p_crmcloudsync_opportunity.p_crmcloudsync_opportunity_id,
       p_crmcloudsync_opportunity.dv_batch_id,
       p_crmcloudsync_opportunity.dv_load_date_time,
       p_crmcloudsync_opportunity.dv_load_end_date_time
  from dbo.h_crmcloudsync_opportunity
  join dbo.p_crmcloudsync_opportunity
    on h_crmcloudsync_opportunity.bk_hash = p_crmcloudsync_opportunity.bk_hash
  join #p_crmcloudsync_opportunity_insert
    on p_crmcloudsync_opportunity.bk_hash = #p_crmcloudsync_opportunity_insert.bk_hash
   and p_crmcloudsync_opportunity.p_crmcloudsync_opportunity_id = #p_crmcloudsync_opportunity_insert.p_crmcloudsync_opportunity_id
  join dbo.l_crmcloudsync_opportunity
    on p_crmcloudsync_opportunity.bk_hash = l_crmcloudsync_opportunity.bk_hash
   and p_crmcloudsync_opportunity.l_crmcloudsync_opportunity_id = l_crmcloudsync_opportunity.l_crmcloudsync_opportunity_id
  join dbo.s_crmcloudsync_opportunity
    on p_crmcloudsync_opportunity.bk_hash = s_crmcloudsync_opportunity.bk_hash
   and p_crmcloudsync_opportunity.s_crmcloudsync_opportunity_id = s_crmcloudsync_opportunity.s_crmcloudsync_opportunity_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_crmcloudsync_opportunity
   where d_crmcloudsync_opportunity.bk_hash in (select bk_hash from #p_crmcloudsync_opportunity_insert)

  insert dbo.d_crmcloudsync_opportunity(
             bk_hash,
             dim_crm_opportunity_key,
             opportunity_id,
             actual_close_date,
             actual_close_dim_date_key,
             actual_close_dim_time_key,
             created_by_dim_crm_system_user_key,
             created_by_name,
             created_dim_date_key,
             created_dim_time_key,
             created_on,
             description,
             dim_crm_ltf_club_key,
             dim_crm_owner_key,
             dim_crm_team_key,
             insert_user,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_assigned_by_app,
             ltf_assigned_by_app_name,
             ltf_assignment_request_date,
             ltf_assignment_request_dim_date_key,
             ltf_assignment_request_dim_time_key,
             ltf_assignment_request_id,
             ltf_channel,
             ltf_channel_name,
             ltf_club_id_name,
             ltf_club_proximity,
             ltf_club_proximity_name,
             ltf_commitment_level,
             ltf_commitment_level_name,
             ltf_commitment_reason,
             ltf_exercise_history,
             ltf_exercise_history_name,
             ltf_guest_pass_expiration_date,
             ltf_guest_pass_expiration_dim_date_key,
             ltf_guest_pass_expiration_dim_time_key,
             ltf_ims_join_link,
             ltf_ims_join_send_date,
             ltf_ims_join_send_dim_date_key,
             ltf_ims_join_send_dim_time_key,
             ltf_injuries_or_limitations,
             ltf_injuries_or_limitations_description,
             ltf_injuries_or_limitations_flag,
             ltf_injuries_or_limitations_name,
             ltf_is_ims_join,
             ltf_is_ims_join_flag,
             ltf_is_ims_join_name,
             ltf_last_activity,
             ltf_last_activity_dim_date_key,
             ltf_last_activity_dim_time_key,
             ltf_lead_source,
             ltf_lead_source_name,
             ltf_lead_type,
             ltf_lead_type_name,
             ltf_line_of_business,
             ltf_line_of_business_name,
             ltf_managed_until,
             ltf_managed_until_dim_date_key,
             ltf_managed_until_dim_time_key,
             ltf_measurable_goal,
             ltf_measurable_goal_name,
             ltf_membership_level,
             ltf_membership_level_name,
             ltf_membership_type,
             ltf_membership_type_name,
             ltf_next_follow_up,
             ltf_next_follow_up_dim_date_key,
             ltf_next_follow_up_dim_time_key,
             ltf_number_over_14_list,
             ltf_number_over_14_list_name,
             ltf_number_under_14_list,
             ltf_number_under_14_list_name,
             ltf_originating_guest_visit_fact_crm_guest_visit_key,
             ltf_originating_guest_visit_name,
             ltf_park,
             ltf_park_comments,
             ltf_park_flag,
             ltf_park_name,
             ltf_park_reason,
             ltf_park_reason_name,
             ltf_park_until,
             ltf_park_until_dim_date_key,
             ltf_park_until_dim_time_key,
             ltf_past_trainer_or_coach,
             ltf_past_trainer_or_coach_flag,
             ltf_past_trainer_or_coach_name,
             ltf_primary_objective,
             ltf_primary_objective_name,
             ltf_profile_notes,
             ltf_programs_of_interest,
             ltf_programs_of_interest_name,
             ltf_promo_code,
             ltf_promo_quoted,
             ltf_ready_to_join,
             ltf_ready_to_join_flag,
             ltf_ready_to_join_name,
             ltf_recommended_membership,
             ltf_recommended_membership_name,
             ltf_referring_contact_dim_crm_contact_key,
             ltf_referring_contact_id_name,
             ltf_referring_member_id,
             ltf_resistance,
             ltf_resistance_name,
             ltf_specific_goal,
             ltf_specific_goal_name,
             ltf_time_goal,
             ltf_time_goal_dim_date_key,
             ltf_time_goal_dim_time_key,
             ltf_todays_action,
             ltf_todays_action_name,
             ltf_trainer_or_coach_preference,
             ltf_trainer_or_coach_preference_name,
             ltf_visitor_id,
             ltf_want_to_do,
             ltf_want_to_do_name,
             ltf_web_team_id,
             ltf_web_team_id_name,
             ltf_web_transfer_method,
             ltf_web_transfer_method_name,
             ltf_who_met_with,
             ltf_why_want_to_do,
             ltf_why_want_to_do_name,
             ltf_workout_preference,
             ltf_workout_preference_name,
             modified_by_dim_crm_system_user_key,
             modified_by_name,
             modified_dim_date_key,
             modified_dim_time_key,
             modified_on,
             modified_on_behalf_by_dim_crm_system_user_key,
             name,
             originating_lead_dim_crm_lead_key,
             originating_lead_id_name,
             overridden_created_dim_date_key,
             overridden_created_dim_time_key,
             overridden_created_on,
             owner_id,
             owner_id_name,
             owner_id_type,
             owning_business_unit,
             owning_user_dim_crm_system_user_key,
             parent_account_dim_crm_account_key,
             parent_account_id_name,
             parent_contact_dim_crm_contact_key,
             parent_contact_id_name,
             state_code,
             state_code_name,
             status_code,
             status_code_name,
             total_amount,
             update_user,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_crmcloudsync_opportunity_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_crm_opportunity_key,
         opportunity_id,
         actual_close_date,
         actual_close_dim_date_key,
         actual_close_dim_time_key,
         created_by_dim_crm_system_user_key,
         created_by_name,
         created_dim_date_key,
         created_dim_time_key,
         created_on,
         description,
         dim_crm_ltf_club_key,
         dim_crm_owner_key,
         dim_crm_team_key,
         insert_user,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_assigned_by_app,
         ltf_assigned_by_app_name,
         ltf_assignment_request_date,
         ltf_assignment_request_dim_date_key,
         ltf_assignment_request_dim_time_key,
         ltf_assignment_request_id,
         ltf_channel,
         ltf_channel_name,
         ltf_club_id_name,
         ltf_club_proximity,
         ltf_club_proximity_name,
         ltf_commitment_level,
         ltf_commitment_level_name,
         ltf_commitment_reason,
         ltf_exercise_history,
         ltf_exercise_history_name,
         ltf_guest_pass_expiration_date,
         ltf_guest_pass_expiration_dim_date_key,
         ltf_guest_pass_expiration_dim_time_key,
         ltf_ims_join_link,
         ltf_ims_join_send_date,
         ltf_ims_join_send_dim_date_key,
         ltf_ims_join_send_dim_time_key,
         ltf_injuries_or_limitations,
         ltf_injuries_or_limitations_description,
         ltf_injuries_or_limitations_flag,
         ltf_injuries_or_limitations_name,
         ltf_is_ims_join,
         ltf_is_ims_join_flag,
         ltf_is_ims_join_name,
         ltf_last_activity,
         ltf_last_activity_dim_date_key,
         ltf_last_activity_dim_time_key,
         ltf_lead_source,
         ltf_lead_source_name,
         ltf_lead_type,
         ltf_lead_type_name,
         ltf_line_of_business,
         ltf_line_of_business_name,
         ltf_managed_until,
         ltf_managed_until_dim_date_key,
         ltf_managed_until_dim_time_key,
         ltf_measurable_goal,
         ltf_measurable_goal_name,
         ltf_membership_level,
         ltf_membership_level_name,
         ltf_membership_type,
         ltf_membership_type_name,
         ltf_next_follow_up,
         ltf_next_follow_up_dim_date_key,
         ltf_next_follow_up_dim_time_key,
         ltf_number_over_14_list,
         ltf_number_over_14_list_name,
         ltf_number_under_14_list,
         ltf_number_under_14_list_name,
         ltf_originating_guest_visit_fact_crm_guest_visit_key,
         ltf_originating_guest_visit_name,
         ltf_park,
         ltf_park_comments,
         ltf_park_flag,
         ltf_park_name,
         ltf_park_reason,
         ltf_park_reason_name,
         ltf_park_until,
         ltf_park_until_dim_date_key,
         ltf_park_until_dim_time_key,
         ltf_past_trainer_or_coach,
         ltf_past_trainer_or_coach_flag,
         ltf_past_trainer_or_coach_name,
         ltf_primary_objective,
         ltf_primary_objective_name,
         ltf_profile_notes,
         ltf_programs_of_interest,
         ltf_programs_of_interest_name,
         ltf_promo_code,
         ltf_promo_quoted,
         ltf_ready_to_join,
         ltf_ready_to_join_flag,
         ltf_ready_to_join_name,
         ltf_recommended_membership,
         ltf_recommended_membership_name,
         ltf_referring_contact_dim_crm_contact_key,
         ltf_referring_contact_id_name,
         ltf_referring_member_id,
         ltf_resistance,
         ltf_resistance_name,
         ltf_specific_goal,
         ltf_specific_goal_name,
         ltf_time_goal,
         ltf_time_goal_dim_date_key,
         ltf_time_goal_dim_time_key,
         ltf_todays_action,
         ltf_todays_action_name,
         ltf_trainer_or_coach_preference,
         ltf_trainer_or_coach_preference_name,
         ltf_visitor_id,
         ltf_want_to_do,
         ltf_want_to_do_name,
         ltf_web_team_id,
         ltf_web_team_id_name,
         ltf_web_transfer_method,
         ltf_web_transfer_method_name,
         ltf_who_met_with,
         ltf_why_want_to_do,
         ltf_why_want_to_do_name,
         ltf_workout_preference,
         ltf_workout_preference_name,
         modified_by_dim_crm_system_user_key,
         modified_by_name,
         modified_dim_date_key,
         modified_dim_time_key,
         modified_on,
         modified_on_behalf_by_dim_crm_system_user_key,
         name,
         originating_lead_dim_crm_lead_key,
         originating_lead_id_name,
         overridden_created_dim_date_key,
         overridden_created_dim_time_key,
         overridden_created_on,
         owner_id,
         owner_id_name,
         owner_id_type,
         owning_business_unit,
         owning_user_dim_crm_system_user_key,
         parent_account_dim_crm_account_key,
         parent_account_id_name,
         parent_contact_dim_crm_contact_key,
         parent_contact_id_name,
         state_code,
         state_code_name,
         status_code,
         status_code_name,
         total_amount,
         update_user,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_crmcloudsync_opportunity_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_crmcloudsync_opportunity)
--Done!
end
