CREATE PROC [dbo].[proc_dim_club] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

truncate table dim_club

if object_id('tempdb..#dim_club') is not null drop table #dim_club

create table #dim_club with(distribution = hash(dim_club_key)) 
as
with mms_club (dim_club_key, club_id, allow_junior_check_in_flag, assess_junior_member_dues_flag, check_in_group_level, child_center_weekly_limit, club_close_dim_date_key,
               club_code, club_name, club_open_dim_date_key, club_status, club_type, info_genesis_store_id,local_currency_code, domain_name_prefix, formal_club_name, gl_club_id,marketing_map_region,max_junior_age,marketing_club_level, member_activities_region_dim_description_key,
               region_dim_description_key, pt_rcl_area_dim_description_key, sales_area_dim_description_key, sell_junior_member_dues_flag, workday_region, address_line_1, address_line_2, city, state,
               postal_code, country, latitude, longitude, phone_number, fax_number, val_member_activity_region_id, val_pt_rcl_area_id, val_region_id,
               val_sales_area_id,val_time_zone_id,dst_offset,st_offset,dv_load_date_time,dv_load_end_date_time,dv_batch_id) as
(
    select d_mms_club.dim_club_key,
           d_mms_club.club_id,
           d_mms_club.allow_junior_check_in_flag,
           d_mms_club.assess_junior_member_dues_flag,
           d_mms_club.check_in_group_level,
           d_mms_club.child_center_weekly_limit,
           d_mms_club.club_close_dim_date_key,
           d_mms_club.club_code,
           d_mms_club.club_name,
           d_mms_club.club_open_dim_date_key,
           d_mms_club.club_status,
           d_mms_club.club_type,
           d_mms_club.info_genesis_store_id,
           currency_code_dim_description.abbreviated_description local_currency_code,
           d_mms_club.domain_name_prefix,
           d_mms_club.formal_club_name,
           d_mms_club.gl_club_id,
		   d_mms_club.marketing_map_region,
		   d_mms_club.max_junior_age,	
		   d_mms_club.marketing_club_level,		   
           d_mms_club.member_activities_region_dim_description_key,
           d_mms_club.region_dim_description_key,
           d_mms_club.pt_rcl_area_dim_description_key,
           d_mms_club.sales_area_dim_description_key,
           d_mms_club.sell_junior_member_dues_flag,
           d_mms_club.workday_region,
           d_mms_club_address.address_line_1,
           d_mms_club_address.address_line_2,
           d_mms_club_address.city,
           state_dim_description.abbreviated_description state,
           d_mms_club_address.postal_code,
           country_dim_description.abbreviated_description country,
           d_mms_club_address.latitude,
           d_mms_club_address.longitude,
           club_phone.phone_number,
           club_fax.phone_number fax_number,
           d_mms_club.val_member_activity_region_id,
           d_mms_club.val_pt_rcl_area_id,
           d_mms_club.val_region_id,
           d_mms_club.val_sales_area_id,
           d_mms_club.val_time_zone_id,
           d_mms_club.dst_offset,
           d_mms_club.st_offset,
           case when d_mms_club.dv_load_date_time >= isnull(d_mms_club_address.dv_load_date_time,'Jan 1, 1753')
                 and d_mms_club.dv_load_date_time >= isnull(club_phone.dv_load_date_time,'Jan 1, 1753')
                 and d_mms_club.dv_load_date_time >= isnull(club_fax.dv_load_date_time,'Jan 1, 1753')
                then d_mms_club.dv_load_date_time
                when d_mms_club_address.dv_load_date_time >= isnull(club_phone.dv_load_date_time,'Jan 1, 1753')
                 and d_mms_club_address.dv_load_date_time >= isnull(club_fax.dv_load_date_time,'Jan 1, 1753')
                then d_mms_club_address.dv_load_date_time
                when club_phone.dv_load_date_time >= isnull(club_fax.dv_load_date_time,'Jan 1, 1753')
                then club_phone.dv_load_date_time
                else isnull(club_fax.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
           case when d_mms_club.dv_load_end_date_time >= isnull(d_mms_club_address.dv_load_end_date_time,'Jan 1, 1753')
                and d_mms_club.dv_load_end_date_time >= isnull(club_phone.dv_load_end_date_time,'Jan 1, 1753')
                and d_mms_club.dv_load_end_date_time >= isnull(club_fax.dv_load_end_date_time,'Jan 1, 1753')
               then d_mms_club.dv_load_end_date_time
               when d_mms_club_address.dv_load_end_date_time >= isnull(club_phone.dv_load_end_date_time,'Jan 1, 1753')
                and d_mms_club_address.dv_load_end_date_time >= isnull(club_fax.dv_load_end_date_time,'Jan 1, 1753')
               then d_mms_club_address.dv_load_end_date_time
               when club_phone.dv_load_end_date_time >= isnull(club_fax.dv_load_end_date_time,'Jan 1, 1753')
               then club_phone.dv_load_end_date_time
               else isnull(club_fax.dv_load_end_date_time,'Jan 1, 1753') end dv_load_end_date_time,
          case when d_mms_club.dv_batch_id >= isnull(d_mms_club_address.dv_batch_id,-1)
                and d_mms_club.dv_batch_id >= isnull(club_phone.dv_batch_id,-1)
                and d_mms_club.dv_batch_id >= isnull(club_fax.dv_batch_id,-1)
               then d_mms_club.dv_batch_id
               when d_mms_club_address.dv_batch_id >= isnull(club_phone.dv_batch_id,-1)
                and d_mms_club_address.dv_batch_id >= isnull(club_fax.dv_batch_id,-1)
               then d_mms_club_address.dv_batch_id
               when club_phone.dv_batch_id >= isnull(club_fax.dv_batch_id,-1)
               then club_phone.dv_batch_id
               else isnull(club_fax.dv_batch_id,-1) end dv_batch_id
    from d_mms_club
    left join d_mms_club_address on d_mms_club.dim_club_key = d_mms_club_address.dim_club_key and d_mms_club_address.val_address_type_id = 5 --Club Address
    left join dim_description state_dim_description on d_mms_club_address.state_dim_description_key = state_dim_description.dim_description_key
    left join dim_description country_dim_description on d_mms_club_address.country_dim_description_key = country_dim_description.dim_description_key
    left join d_mms_club_phone club_phone on d_mms_club.dim_club_key = club_phone.dim_club_key and club_phone.val_phone_type_id = 5 --Club Phone
    left join d_mms_club_phone club_fax on d_mms_club.dim_club_key = club_fax.dim_club_key and club_fax.val_phone_type_id = 11 --Club Fax
    join dim_description currency_code_dim_description on d_mms_club.currency_code_dim_description_key = currency_code_dim_description.dim_description_key
),
crm_club (dim_club_key, club_id, area_director_dim_crm_system_user_key, club_regional_manager_dim_crm_system_user_key,
          dim_crm_team_key, five_letter_club_code, four_letter_club_code, general_manager_dim_crm_system_user_key, marketing_name,
          regional_sales_lead_dim_crm_system_user_key, regional_vice_president_dim_crm_system_user_key, state_or_province, telephone, web_specialist_dim_crm_team_key,ltf_lt_work_email,ltf_web_specialist_team,
          dv_load_date_time,dv_load_end_date_time,dv_batch_id) as
(
    select dim_club_key,
           club_id,
           area_director_dim_crm_system_user_key,
           club_regional_manager_dim_crm_system_user_key,
           dim_crm_team_key,
           five_letter_club_code,
           four_letter_club_code,
           general_manager_dim_crm_system_user_key,
           marketing_name,
           regional_sales_lead_dim_crm_system_user_key,
           regional_vice_president_dim_crm_system_user_key,
           state_or_province,
           telephone,
           web_specialist_dim_crm_team_key,
		   ltf_lt_work_email,
		   ltf_web_specialist_team,
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id
    from d_crmcloudsync_ltf_club
	where status_code <> 2 
),
cmr_club (dim_club_key, club_id,   
          access_type,center_type,
          growth_type,
          square_footage,yoga_studios,club_membership_level,
          dv_load_date_time,dv_load_end_date_time,dv_batch_id)  as
(
    select dim_club_key,
           mms_club_id,           
           access_type,
           center_type,
           growth_type,
           square_footage,
           yoga_studios,
		   membership_level,
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id
      from d_udwcloudsync_club_master_roster
	 where mms_club_Id is not null
),
ecd_club (dim_club_key, club_id, feature_basketball_courts_flag, feature_indoor_cycle_studio_flag, feature_fitness_studios_flag,
		   feature_indoor_tennis_courts_flag,feature_mixed_combats_arts_studio_flag, feature_outdoor_tennis_courts_flag,
		   feature_pilates_studio_flag,  feature_racquetball_courts_flag, feature_squash_courts_flag, 
           amenity_conditioner_flag,amenity_cotton_balls_flag,amenity_deodorant_flag,amenity_free_lockers_and_towels_flag,
           amenity_hairspray_flag,amenity_mouthwash_flag,amenity_plasma_big_screen_tvs_flag,amenity_razors_flag,
           amenity_shampoo_flag,amenity_wireless_internet_access_flag,app_created_by,app_modified_by,club_hours,compliance_asset_id,
           content_type,created_by,created_dim_date_key,feature_advanced_training_studio_flag,feature_alpha_field_flag,feature_bar_flag,
           feature_boxing_gym_flag,feature_business_center_flag,feature_cardio_equipment_flag,feature_child_center_flag,feature_connected_technology_flag,feature_deluge_flag,feature_free_weights_and_equipment_flag,
           feature_functional_training_area_flag,feature_hamam_flag,feature_indoor_golf_center_flag,feature_indoor_lap_pool_flag,
           feature_indoor_leisure_pool_flag,feature_indoor_turf_field_flag,feature_indoor_water_slides_flag,feature_indoor_whirlpool_flag,
           feature_infant_room_flag,feature_kids_academy_flag,feature_kids_gym_flag,feature_kids_language_arts_studio_flag,
           feature_kids_learning_lab_flag,feature_kids_media_room_flag,feature_kids_movement_studio_flag,
           feature_kids_outdoor_play_area_flag,feature_kids_tumbling_studio_flag,feature_lazy_river_flag,feature_lifecafe_flag,
           feature_lifecafe_poolside_bistro_flag,feature_lifespa_flag,feature_outdoor_fitness_trail_flag,feature_outdoor_lap_pool_flag,
           feature_outdoor_swimming_pool_flag,feature_outdoor_turf_field_flag,feature_outdoor_water_slides_flag,
           feature_outdoor_whirlpool_flag,feature_pickleball_court_flag,feature_play_maze_flag,feature_pre_school_flag,
           feature_proactive_care_clinic_flag,feature_rare_flag,feature_rehabilitation_and_chiropractic_clinic_flag,
           feature_resistance_training_area_flag,feature_retail_store_flag,feature_rock_wall_flag,feature_rooftop_patio_flag,
           feature_sand_volleyball_court_flag,feature_saunas_flag,feature_splash_pad_flag,feature_steam_rooms_flag,
           feature_volleyball_courts_flag,feature_walking_running_track_flag,feature_weight_machines_flag,feature_yoga_studio_flag,
           feature_zero_depth_entry_pool_flag,folder_child_count,general_manager,general_manager_email,general_manager_phone,
           item_child_count,item_type,label_applied,label_applied_by,label_setting,labels,member_services_manager,
           member_services_manager_email,member_services_manager_phone,modified_by,modified_dim_date_key,oms_flag,open_dim_date_key,
           [path],program_badminton_flag,program_barre_flag,program_basketball_flag,program_birthday_parties_flag,
           program_boxing_flag,program_golf_flag,program_handball_flag,program_indoor_cycle_flag,program_kids_academy_flag,
           program_kids_activities_flag,program_kids_camps_flag,program_kids_sports_and_fitness_flag,program_kids_swim_flag,
           program_mixed_combat_arts_flag,program_outdoor_cycle_flag,program_personal_training_flag,program_pickleball_flag,
           program_pilates_flag,program_racquetball_flag,program_rock_climbing_flag,program_run_club_flag,
           program_soccer_flag,program_squash_flag,program_studio_flag,program_swimming_flag,program_table_tennis_flag,
           program_team_training_flag,program_tennis_flag,program_volleyball_flag,program_weight_loss_flag,program_yoga_flag,
           service_bar_flag,service_basketball_training_flag,service_child_center_flag,service_golf_instruction_flag,
           service_hair_flag,service_health_assessments_flag,service_kids_academy_flag,service_kids_birthday_parties_flag,
           service_kids_camps_flag,service_kids_swim_lessons_flag,service_lifecafe_flag,service_marathon_training_flag,
           service_massage_flag,service_medi_spa_flag,service_nails_flag,service_nutritional_coaching_flag,
           service_proactive_care_flag,service_racquetball_lessons_flag,service_rare_flag,service_rehabilitation_chiropractic_flag,
           service_run_training_flag,service_skin_flag,service_soccer_training_flag,service_squash_lessons_flag,
           service_swim_assessments_flag,service_team_training_flag,service_tennis_lessons_flag,service_tri_training_flag,
           service_weight_loss_flag,title,web_free_pass,web_inquiry_flag,web_presale_waitlist_flag,web_price_request_flag,		   
		   dv_load_date_time,dv_load_end_date_time, dv_batch_id)  as
(
    select dim_club_key,mms_club_id,
	      feature_basketball_courts_flag,
		  feature_indoor_cycle_studio_flag,
		  feature_fitness_studios_flag,
		  feature_indoor_tennis_courts_flag,
		  feature_mixed_combats_arts_studio_flag,
		  feature_outdoor_tennis_courts_flag,
		  feature_pilates_studio_flag,
		  feature_racquetball_courts_flag,
		  feature_squash_courts_flag, 
		  amenity_conditioner_flag,
	      amenity_cotton_balls_flag,
	      amenity_deodorant_flag,
	      amenity_free_lockers_and_towels_flag,
	      amenity_hairspray_flag,
	      amenity_mouthwash_flag,
	      amenity_plasma_big_screen_tvs_flag,
	      amenity_razors_flag,
	      amenity_shampoo_flag,
	      amenity_wireless_internet_access_flag,
	      app_created_by,
	      app_modified_by,
	      club_hours,
	      compliance_asset_id,
	      content_type,
	      created_by,
	      created_dim_date_key,
	      feature_advanced_training_studio_flag,
	      feature_alpha_field_flag,
	      feature_bar_flag,
	      feature_boxing_gym_flag,
	      feature_business_center_flag,
	      feature_cardio_equipment_flag,
	      feature_child_center_flag,
	      feature_connected_technology_flag,
	      feature_deluge_flag,
	      feature_free_weights_and_equipment_flag,
	      feature_functional_training_area_flag,
	      feature_hamam_flag,
	      feature_indoor_golf_center_flag,
	      feature_indoor_lap_pool_flag,
	      feature_indoor_leisure_pool_flag,
	      feature_indoor_turf_field_flag,
	      feature_indoor_water_slides_flag,
	      feature_indoor_whirlpool_flag,
	      feature_infant_room_flag,
	      feature_kids_academy_flag,
	      feature_kids_gym_flag,
	      feature_kids_language_arts_studio_flag,
	      feature_kids_learning_lab_flag,
	      feature_kids_media_room_flag,
	      feature_kids_movement_studio_flag,
	      feature_kids_outdoor_play_area_flag,
	      feature_kids_tumbling_studio_flag,
	      feature_lazy_river_flag,
	      feature_lifecafe_flag,
	      feature_lifecafe_poolside_bistro_flag,
	      feature_lifespa_flag,
	      feature_outdoor_fitness_trail_flag,
	      feature_outdoor_lap_pool_flag,
	      feature_outdoor_swimming_pool_flag,
	      feature_outdoor_turf_field_flag,
	      feature_outdoor_water_slides_flag,
	      feature_outdoor_whirlpool_flag,
	      feature_pickleball_court_flag,
	      feature_play_maze_flag,
	      feature_pre_school_flag,
	      feature_proactive_care_clinic_flag,
	      feature_rare_flag,
	      feature_rehabilitation_and_chiropractic_clinic_flag,
	      feature_resistance_training_area_flag,
	      feature_retail_store_flag,
	      feature_rock_wall_flag,
	      feature_rooftop_patio_flag,
	      feature_sand_volleyball_court_flag,
	      feature_saunas_flag,
	      feature_splash_pad_flag,
	      feature_steam_rooms_flag,
	      feature_volleyball_courts_flag,
	      feature_walking_running_track_flag,
	      feature_weight_machines_flag,
	      feature_yoga_studio_flag,
	      feature_zero_depth_entry_pool_flag,
	      folder_child_count,
	      general_manager,
	      general_manager_email,
	      general_manager_phone,
	      item_child_count,
	      item_type,
	      label_applied,
	      label_applied_by,
	      label_setting,
	      labels,
	      member_services_manager,
	      member_services_manager_email,
	      member_services_manager_phone,
	      modified_by,
	      modified_dim_date_key,
	      oms_flag,
	      open_dim_date_key,
	      path,
	      program_badminton_flag,
	      program_barre_flag,
	      program_basketball_flag,
	      program_birthday_parties_flag,
	      program_boxing_flag,
	      program_golf_flag,
	      program_handball_flag,
	      program_indoor_cycle_flag,
	      program_kids_academy_flag,
	      program_kids_activities_flag,
	      program_kids_camps_flag,
	      program_kids_sports_and_fitness_flag,
	      program_kids_swim_flag,
	      program_mixed_combat_arts_flag,
	      program_outdoor_cycle_flag,
	      program_personal_training_flag,
	      program_pickleball_flag,
	      program_pilates_flag,
	      program_racquetball_flag,
	      program_rock_climbing_flag,
	      program_run_club_flag,
	      program_soccer_flag,
	      program_squash_flag,
	      program_studio_flag,
	      program_swimming_flag,
	      program_table_tennis_flag,
	      program_team_training_flag,
	      program_tennis_flag,
	      program_volleyball_flag,
	      program_weight_loss_flag,
	      program_yoga_flag,
	      service_bar_flag,
	      service_basketball_training_flag,
	      service_child_center_flag,
	      service_golf_instruction_flag,
	      service_hair_flag,
	      service_health_assessments_flag,
	      service_kids_academy_flag,
	      service_kids_birthday_parties_flag,
	      service_kids_camps_flag,
	      service_kids_swim_lessons_flag,
	      service_lifecafe_flag,
	      service_marathon_training_flag,
	      service_massage_flag,
	      service_medi_spa_flag,
	      service_nails_flag,
	      service_nutritional_coaching_flag,
	      service_proactive_care_flag,
	      service_racquetball_lessons_flag,
	      service_rare_flag,
	      service_rehabilitation_chiropractic_flag,
	      service_run_training_flag,
	      service_skin_flag,
	      service_soccer_training_flag,
	      service_squash_lessons_flag,
	      service_swim_assessments_flag,
	      service_team_training_flag,
	      service_tennis_lessons_flag,
	      service_tri_training_flag,
	      service_weight_loss_flag,
	      title,
	      web_free_pass,
	      web_inquiry_flag,
	      web_presale_waitlist_flag,
	      web_price_request_flag,		  
          dv_load_date_time,
          dv_load_end_date_time,
          dv_batch_id
      from d_udwcloudsync_enterprise_club_data
),
all_clubs (dim_club_key,club_id) as
(
    select dim_club_key,club_id from mms_club
    union
    select dim_club_key,club_id from crm_club
    union
    select dim_club_key,club_id from cmr_club
    union
    select dim_club_key,club_id from ecd_club
)
select all_clubs.dim_club_key,
       all_clubs.club_id,
       isnull(mms_club.allow_junior_check_in_flag,'') allow_junior_check_in_flag,
       isnull(mms_club.assess_junior_member_dues_flag,'') assess_junior_member_dues_flag,
       mms_club.check_in_group_level,
       mms_club.child_center_weekly_limit,
       isnull(mms_club.club_close_dim_date_key,-998) club_close_dim_date_key,
       isnull(mms_club.club_code,'') club_code,
       isnull(mms_club.club_name,'') club_name,
       isnull(mms_club.club_open_dim_date_key,-998) club_open_dim_date_key,
       isnull(mms_club.club_status,'') club_status,
       isnull(mms_club.club_type,'') club_type,
       mms_club.info_genesis_store_id,
       isnull(mms_club.local_currency_code,'USD') local_currency_code,
       isnull(mms_club.domain_name_prefix,'') domain_name_prefix,
       isnull(mms_club.formal_club_name,'') formal_club_name,
       mms_club.gl_club_id,
       isnull(mms_club.member_activities_region_dim_description_key,-998) member_activities_region_dim_description_key,
       isnull(mms_club.region_dim_description_key,-998) region_dim_description_key,
       isnull(mms_club.pt_rcl_area_dim_description_key,-998) pt_rcl_area_dim_description_key,
       isnull(mms_club.sales_area_dim_description_key,-998) sales_area_dim_description_key,
       isnull(mms_club.sell_junior_member_dues_flag,'') sell_junior_member_dues_flag,
       isnull(mms_club.workday_region,'') workday_region,
       isnull(mms_club.address_line_1,'') address_line_1,
       isnull(mms_club.address_line_2,'') address_line_2,
       isnull(mms_club.city,'') city,
       isnull(mms_club.state,'') state,
       isnull(mms_club.postal_code,'') postal_code,
       isnull(mms_club.country,'') country,
       mms_club.latitude,
       mms_club.longitude,
       isnull(mms_club.phone_number,'') phone_number,
       isnull(mms_club.fax_number,'') fax_number,
       mms_club.val_member_activity_region_id,
       mms_club.val_pt_rcl_area_id,
       mms_club.val_region_id,
       mms_club.val_sales_area_id,
       mms_club.val_time_zone_id,
       mms_club.dst_offset,
       mms_club.st_offset,
	   mms_club.marketing_map_region,
	   mms_club.max_junior_age,
       isnull(crm_club.area_director_dim_crm_system_user_key,-998) area_director_dim_crm_system_user_key,
       isnull(crm_club.club_regional_manager_dim_crm_system_user_key,-998) club_regional_manager_dim_crm_system_user_key,
       isnull(crm_club.dim_crm_team_key,-998) dim_crm_team_key,
       isnull(crm_club.five_letter_club_code,'') five_letter_club_code,
       isnull(crm_club.four_letter_club_code,'') four_letter_club_code,
       isnull(crm_club.general_manager_dim_crm_system_user_key,-998) general_manager_dim_crm_system_user_key,
       isnull(crm_club.marketing_name,'') marketing_name,
       isnull(crm_club.regional_sales_lead_dim_crm_system_user_key,-998) regional_sales_lead_dim_crm_system_user_key,
       isnull(crm_club.regional_vice_president_dim_crm_system_user_key,-998) regional_vice_president_dim_crm_system_user_key,
       isnull(crm_club.state_or_province,'') state_or_province,
       isnull(crm_club.telephone,'') telephone,
       isnull(crm_club.web_specialist_dim_crm_team_key,-998) web_specialist_dim_crm_team_key,
	   isnull(crm_club.ltf_lt_work_email,'') ltf_lt_work_email,
	   isnull(crm_club.ltf_web_specialist_team,'') ltf_web_specialist_team,     
       isnull(access_type,'') access_type,
       isnull(center_type,'') center_type,
       isnull(growth_type,'') growth_type,
       isnull(marketing_club_level,'') marketing_club_level,
       square_footage,       
       yoga_studios,
	   club_membership_level,
	   isnull(feature_basketball_courts_flag,'N') feature_basketball_courts_flag,
	   isnull(feature_indoor_cycle_studio_flag,'N') feature_indoor_cycle_studio_flag ,
	   isnull(feature_fitness_studios_flag,'N') feature_fitness_studios_flag,
	   isnull(feature_indoor_tennis_courts_flag,'N') feature_indoor_tennis_courts_flag,
	   isnull(feature_mixed_combats_arts_studio_flag,'N') feature_mixed_combats_arts_studio_flag,
	   isnull(feature_outdoor_tennis_courts_flag,'N') feature_outdoor_tennis_courts_flag,
	   isnull(feature_pilates_studio_flag,'N') feature_pilates_studio_flag,
	   isnull(feature_racquetball_courts_flag,'N') feature_racquetball_courts_flag,
	   isnull(feature_squash_courts_flag, 'N') feature_squash_courts_flag,
	   isnull(ecd_club.amenity_conditioner_flag,'N') amenity_conditioner_flag,
	   isnull(ecd_club.amenity_cotton_balls_flag,'N') amenity_cotton_balls_flag,
	   isnull(ecd_club.amenity_deodorant_flag,'N') amenity_deodorant_flag,
	   isnull(ecd_club.amenity_free_lockers_and_towels_flag,'N') amenity_free_lockers_and_towels_flag,
	   isnull(ecd_club.amenity_hairspray_flag,'N') amenity_hairspray_flag,
	   isnull(ecd_club.amenity_mouthwash_flag,'N') amenity_mouthwash_flag,
	   isnull(ecd_club.amenity_plasma_big_screen_tvs_flag,'N') amenity_plasma_big_screen_tvs_flag,
	   isnull(ecd_club.amenity_razors_flag,'N') amenity_razors_flag,
	   isnull(ecd_club.amenity_shampoo_flag,'N') amenity_shampoo_flag,
	   isnull(ecd_club.amenity_wireless_internet_access_flag,'N') amenity_wireless_internet_access_flag,
	   isnull(ecd_club.app_created_by,'') app_created_by,
	   isnull(ecd_club.app_modified_by,'') app_modified_by,
	   isnull(ecd_club.club_hours,'') club_hours,
	   isnull(ecd_club.compliance_asset_id,'') compliance_asset_id,
	   isnull(ecd_club.content_type,'') content_type,
	   isnull(ecd_club.created_by,'') created_by,      
	   isnull(ecd_club.created_dim_date_key,-998) created_dim_date_key,	   
	   isnull(ecd_club.feature_advanced_training_studio_flag,'N') feature_advanced_training_studio_flag,
	   isnull(ecd_club.feature_alpha_field_flag,'N') feature_alpha_field_flag,
	   isnull(ecd_club.feature_bar_flag,'N') feature_bar_flag,
	   isnull(ecd_club.feature_boxing_gym_flag,'N') feature_boxing_gym_flag,
	   isnull(ecd_club.feature_business_center_flag,'N') feature_business_center_flag,
	   isnull(ecd_club.feature_cardio_equipment_flag,'N') feature_cardio_equipment_flag,
	   isnull(ecd_club.feature_child_center_flag,'N') feature_child_center_flag,
	   isnull(ecd_club.feature_connected_technology_flag,'N') feature_connected_technology_flag,
	   isnull(ecd_club.feature_deluge_flag,'N') feature_deluge_flag,
	   isnull(ecd_club.feature_free_weights_and_equipment_flag,'N') feature_free_weights_and_equipment_flag,
	   isnull(ecd_club.feature_functional_training_area_flag,'N') feature_functional_training_area_flag,
	   isnull(ecd_club.feature_hamam_flag,'N') feature_hamam_flag,
	   isnull(ecd_club.feature_indoor_golf_center_flag,'N') feature_indoor_golf_center_flag,
	   isnull(ecd_club.feature_indoor_lap_pool_flag,'N') feature_indoor_lap_pool_flag,
	   isnull(ecd_club.feature_indoor_leisure_pool_flag,'N') feature_indoor_leisure_pool_flag,
	   isnull(ecd_club.feature_indoor_turf_field_flag,'N') feature_indoor_turf_field_flag,
	   isnull(ecd_club.feature_indoor_water_slides_flag,'N') feature_indoor_water_slides_flag,
	   isnull(ecd_club.feature_indoor_whirlpool_flag,'N') feature_indoor_whirlpool_flag,
	   isnull(ecd_club.feature_infant_room_flag,'N') feature_infant_room_flag,
	   isnull(ecd_club.feature_kids_academy_flag,'N') feature_kids_academy_flag,
	   isnull(ecd_club.feature_kids_gym_flag,'N') feature_kids_gym_flag,
	   isnull(ecd_club.feature_kids_language_arts_studio_flag,'N') feature_kids_language_arts_studio_flag,
	   isnull(ecd_club.feature_kids_learning_lab_flag,'N') feature_kids_learning_lab_flag,
	   isnull(ecd_club.feature_kids_media_room_flag,'N') feature_kids_media_room_flag,
	   isnull(ecd_club.feature_kids_movement_studio_flag,'N') feature_kids_movement_studio_flag,
	   isnull(ecd_club.feature_kids_outdoor_play_area_flag,'N') feature_kids_outdoor_play_area_flag,
	   isnull(ecd_club.feature_kids_tumbling_studio_flag,'N') feature_kids_tumbling_studio_flag,
	   isnull(ecd_club.feature_lazy_river_flag,'N') feature_lazy_river_flag,
	   isnull(ecd_club.feature_lifecafe_flag,'N') feature_lifecafe_flag,
	   isnull(ecd_club.feature_lifecafe_poolside_bistro_flag,'N') feature_lifecafe_poolside_bistro_flag,
	   isnull(ecd_club.feature_lifespa_flag,'N') feature_lifespa_flag,
	   isnull(ecd_club.feature_outdoor_fitness_trail_flag,'N') feature_outdoor_fitness_trail_flag,
	   isnull(ecd_club.feature_outdoor_lap_pool_flag,'N') feature_outdoor_lap_pool_flag,
	   isnull(ecd_club.feature_outdoor_swimming_pool_flag,'N') feature_outdoor_swimming_pool_flag,
	   isnull(ecd_club.feature_outdoor_turf_field_flag,'N') feature_outdoor_turf_field_flag,
	   isnull(ecd_club.feature_outdoor_water_slides_flag,'N') feature_outdoor_water_slides_flag,
	   isnull(ecd_club.feature_outdoor_whirlpool_flag,'N') feature_outdoor_whirlpool_flag,
	   isnull(ecd_club.feature_pickleball_court_flag,'N') feature_pickleball_court_flag,
	   isnull(ecd_club.feature_play_maze_flag,'N') feature_play_maze_flag,
	   isnull(ecd_club.feature_pre_school_flag,'N') feature_pre_school_flag,
	   isnull(ecd_club.feature_proactive_care_clinic_flag,'N') feature_proactive_care_clinic_flag,
	   isnull(ecd_club.feature_rare_flag,'N') feature_rare_flag,
	   isnull(ecd_club.feature_rehabilitation_and_chiropractic_clinic_flag,'N') feature_rehabilitation_and_chiropractic_clinic_flag,
	   isnull(ecd_club.feature_resistance_training_area_flag,'N') feature_resistance_training_area_flag,
	   isnull(ecd_club.feature_retail_store_flag,'N') feature_retail_store_flag,
	   isnull(ecd_club.feature_rock_wall_flag,'N') feature_rock_wall_flag,
	   isnull(ecd_club.feature_rooftop_patio_flag,'N') feature_rooftop_patio_flag,
	   isnull(ecd_club.feature_sand_volleyball_court_flag,'N') feature_sand_volleyball_court_flag,
	   isnull(ecd_club.feature_saunas_flag,'N') feature_saunas_flag,
	   isnull(ecd_club.feature_splash_pad_flag,'N') feature_splash_pad_flag,
	   isnull(ecd_club.feature_steam_rooms_flag,'N') feature_steam_rooms_flag,
	   isnull(ecd_club.feature_volleyball_courts_flag,'N') feature_volleyball_courts_flag,
	   isnull(ecd_club.feature_walking_running_track_flag,'N') feature_walking_running_track_flag,
	   isnull(ecd_club.feature_weight_machines_flag,'N') feature_weight_machines_flag,
	   isnull(ecd_club.feature_yoga_studio_flag,'N') feature_yoga_studio_flag,
	   isnull(ecd_club.feature_zero_depth_entry_pool_flag,'N') feature_zero_depth_entry_pool_flag,
	   isnull(ecd_club.folder_child_count,'') folder_child_count,
	   isnull(ecd_club.general_manager,'') general_manager,
	   isnull(ecd_club.general_manager_email,'') general_manager_email,
	   isnull(ecd_club.general_manager_phone,'') general_manager_phone,
	   isnull(ecd_club.item_child_count,'') item_child_count,
	   isnull(ecd_club.item_type,'') item_type,
	   isnull(ecd_club.label_applied,'') label_applied,
	   isnull(ecd_club.label_applied_by,'') label_applied_by,
	   isnull(ecd_club.label_setting,'') label_setting,
	   isnull(ecd_club.labels,'') labels,
	   isnull(ecd_club.member_services_manager,'') member_services_manager,
	   isnull(ecd_club.member_services_manager_email,'') member_services_manager_email,
	   isnull(ecd_club.member_services_manager_phone,'') member_services_manager_phone,
	   isnull(ecd_club.modified_by,'') modified_by,
	   isnull(ecd_club.modified_dim_date_key,-998) modified_dim_date_key,
	   isnull(ecd_club.oms_flag,'N') oms_flag,
	   isnull(ecd_club.open_dim_date_key,-998) open_dim_date_key,
	   isnull(ecd_club.path,'') path,
	   isnull(ecd_club.program_badminton_flag,'N') program_badminton_flag,
	   isnull(ecd_club.program_barre_flag,'N') program_barre_flag,
	   isnull(ecd_club.program_basketball_flag,'N') program_basketball_flag,
	   isnull(ecd_club.program_birthday_parties_flag,'N') program_birthday_parties_flag,
	   isnull(ecd_club.program_boxing_flag,'N') program_boxing_flag,
	   isnull(ecd_club.program_golf_flag,'N') program_golf_flag,
	   isnull(ecd_club.program_handball_flag,'N') program_handball_flag,
	   isnull(ecd_club.program_indoor_cycle_flag,'N') program_indoor_cycle_flag,
	   isnull(ecd_club.program_kids_academy_flag,'N') program_kids_academy_flag,
	   isnull(ecd_club.program_kids_activities_flag,'N') program_kids_activities_flag,
	   isnull(ecd_club.program_kids_camps_flag,'N') program_kids_camps_flag,
	   isnull(ecd_club.program_kids_sports_and_fitness_flag,'N') program_kids_sports_and_fitness_flag,
	   isnull(ecd_club.program_kids_swim_flag,'N') program_kids_swim_flag,
	   isnull(ecd_club.program_mixed_combat_arts_flag,'N') program_mixed_combat_arts_flag,
	   isnull(ecd_club.program_outdoor_cycle_flag,'N') program_outdoor_cycle_flag,
	   isnull(ecd_club.program_personal_training_flag,'N') program_personal_training_flag,
	   isnull(ecd_club.program_pickleball_flag,'N') program_pickleball_flag,
	   isnull(ecd_club.program_pilates_flag,'N') program_pilates_flag,
	   isnull(ecd_club.program_racquetball_flag,'N') program_racquetball_flag,
	   isnull(ecd_club.program_rock_climbing_flag,'N') program_rock_climbing_flag,
	   isnull(ecd_club.program_run_club_flag,'N') program_run_club_flag,
	   isnull(ecd_club.program_soccer_flag,'N') program_soccer_flag,
	   isnull(ecd_club.program_squash_flag,'N') program_squash_flag,
	   isnull(ecd_club.program_studio_flag,'N') program_studio_flag,
	   isnull(ecd_club.program_swimming_flag,'N') program_swimming_flag,
	   isnull(ecd_club.program_table_tennis_flag,'N') program_table_tennis_flag,
	   isnull(ecd_club.program_team_training_flag,'N') program_team_training_flag,
	   isnull(ecd_club.program_tennis_flag,'N') program_tennis_flag,
	   isnull(ecd_club.program_volleyball_flag,'N') program_volleyball_flag,
	   isnull(ecd_club.program_weight_loss_flag,'N') program_weight_loss_flag,
	   isnull(ecd_club.program_yoga_flag,'N') program_yoga_flag,
	   isnull(ecd_club.service_bar_flag,'N') service_bar_flag,
	   isnull(ecd_club.service_basketball_training_flag,'N') service_basketball_training_flag,
	   isnull(ecd_club.service_child_center_flag,'N') service_child_center_flag,
	   isnull(ecd_club.service_golf_instruction_flag,'N') service_golf_instruction_flag,
	   isnull(ecd_club.service_hair_flag,'N') service_hair_flag,
	   isnull(ecd_club.service_health_assessments_flag,'N') service_health_assessments_flag,
	   isnull(ecd_club.service_kids_academy_flag,'N') service_kids_academy_flag,
	   isnull(ecd_club.service_kids_birthday_parties_flag,'N') service_kids_birthday_parties_flag,
	   isnull(ecd_club.service_kids_camps_flag,'N') service_kids_camps_flag,
	   isnull(ecd_club.service_kids_swim_lessons_flag,'N') service_kids_swim_lessons_flag,
	   isnull(ecd_club.service_lifecafe_flag,'N') service_lifecafe_flag,
	   isnull(ecd_club.service_marathon_training_flag,'N') service_marathon_training_flag,
	   isnull(ecd_club.service_massage_flag,'N') service_massage_flag,
	   isnull(ecd_club.service_medi_spa_flag,'N') service_medi_spa_flag,
	   isnull(ecd_club.service_nails_flag,'N') service_nails_flag,
	   isnull(ecd_club.service_nutritional_coaching_flag,'N') service_nutritional_coaching_flag,
	   isnull(ecd_club.service_proactive_care_flag,'N') service_proactive_care_flag,
	   isnull(ecd_club.service_racquetball_lessons_flag,'N') service_racquetball_lessons_flag,
	   isnull(ecd_club.service_rare_flag,'N') service_rare_flag,
	   isnull(ecd_club.service_rehabilitation_chiropractic_flag,'N') service_rehabilitation_chiropractic_flag,
	   isnull(ecd_club.service_run_training_flag,'N') service_run_training_flag,
	   isnull(ecd_club.service_skin_flag,'N') service_skin_flag,
	   isnull(ecd_club.service_soccer_training_flag,'N') service_soccer_training_flag,
	   isnull(ecd_club.service_squash_lessons_flag,'N') service_squash_lessons_flag,
	   isnull(ecd_club.service_swim_assessments_flag,'N') service_swim_assessments_flag,
	   isnull(ecd_club.service_team_training_flag,'N') service_team_training_flag,
	   isnull(ecd_club.service_tennis_lessons_flag,'N') service_tennis_lessons_flag,
	   isnull(ecd_club.service_tri_training_flag,'N') service_tri_training_flag,
	   isnull(ecd_club.service_weight_loss_flag,'N') service_weight_loss_flag,
	   isnull(ecd_club.title,'') title,
	   isnull(ecd_club.web_free_pass,'') web_free_pass,
	   isnull(ecd_club.web_inquiry_flag,'N') web_inquiry_flag,
	   isnull(ecd_club.web_presale_waitlist_flag,'N') web_presale_waitlist_flag,
	   isnull(ecd_club.web_price_request_flag,'N') web_price_request_flag,
       case when mms_club.dv_load_date_time >= isnull(crm_club.dv_load_date_time,'Jan 1, 1753')
             and mms_club.dv_load_date_time >= isnull(cmr_club.dv_load_date_time,'Jan 1, 1753')
			 and mms_club.dv_load_date_time >= isnull(ecd_club.dv_load_date_time,'Jan 1, 1753')
            then mms_club.dv_load_date_time
            when crm_club.dv_load_date_time >= isnull(cmr_club.dv_load_date_time,'Jan 1, 1753')
			 and crm_club.dv_load_date_time >= isnull(ecd_club.dv_load_date_time,'Jan 1, 1753')
            then crm_club.dv_load_date_time
			when cmr_club.dv_load_date_time >= isnull(ecd_club.dv_load_date_time,'Jan 1, 1753')
			then cmr_club.dv_load_date_time
            else isnull(ecd_club.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
       case when mms_club.dv_load_end_date_time >= isnull(crm_club.dv_load_end_date_time,'Jan 1, 1753')
             and mms_club.dv_load_end_date_time >= isnull(cmr_club.dv_load_end_date_time,'Jan 1, 1753')
			 and mms_club.dv_load_end_date_time >= isnull(ecd_club.dv_load_end_date_time,'Jan 1, 1753')
            then mms_club.dv_load_end_date_time
            when crm_club.dv_load_end_date_time >= isnull(cmr_club.dv_load_end_date_time,'Jan 1, 1753')
			 and crm_club.dv_load_end_date_time >= isnull(ecd_club.dv_load_end_date_time,'Jan 1, 1753')
            then crm_club.dv_load_end_date_time
			when cmr_club.dv_load_end_date_time >= isnull(ecd_club.dv_load_end_date_time,'Jan 1, 1753')
			then cmr_club.dv_load_end_date_time
            else isnull(ecd_club.dv_load_end_date_time,'Jan 1, 1753') end dv_load_end_date_time,
       case when mms_club.dv_batch_id >= isnull(crm_club.dv_batch_id,-1)
             and mms_club.dv_batch_id >= isnull(cmr_club.dv_batch_id,-1)
			 and mms_club.dv_batch_id >= isnull(ecd_club.dv_batch_id,-1)
            then mms_club.dv_batch_id
            when crm_club.dv_batch_id >= isnull(cmr_club.dv_batch_id,-1)
			 and crm_club.dv_batch_id >= isnull(ecd_club.dv_batch_id,-1)
            then crm_club.dv_batch_id
			when cmr_club.dv_batch_id >= isnull(ecd_club.dv_batch_id,-1)
			then cmr_club.dv_batch_id
            else isnull(ecd_club.dv_batch_id,-1) end dv_batch_id
from all_clubs
left join mms_club on all_clubs.dim_club_key = mms_club.dim_club_key
left join crm_club on all_clubs.dim_club_key = crm_club.dim_club_key
left join cmr_club on all_clubs.dim_club_key = cmr_club.dim_club_key
left join ecd_club on all_clubs.dim_club_key = ecd_club.dim_club_key

declare @dt datetime = getdate()
declare @u varchar(50) = suser_sname()

INSERT INTO dim_club (
	dim_club_key
	,access_type
	,address_line_1
	,address_line_2
	,allow_junior_check_in_flag
	,area_director_dim_crm_system_user_key
	,assess_junior_member_dues_flag
	,center_type
	,check_in_group_level
	,child_center_weekly_limit
	,city
	,club_close_dim_date_key
	,club_code
	,club_id
	,club_name
	,club_open_dim_date_key
	,club_regional_manager_dim_crm_system_user_key
	,club_status
	,club_type
	,country
	,dim_crm_team_key
	,domain_name_prefix
	,fax_number
	,five_letter_club_code
	,formal_club_name
	,four_letter_club_code
	,general_manager_dim_crm_system_user_key
	,gl_club_id
	,growth_type
	,info_genesis_store_id
	,latitude
	,local_currency_code
	,longitude
	,marketing_name
	,marketing_club_level
	,marketing_map_region
	,max_junior_age
	,member_activities_region_dim_description_key
	,phone_number
	,postal_code
	,pt_rcl_area_dim_description_key
	,region_dim_description_key
	,regional_sales_lead_dim_crm_system_user_key
	,regional_vice_president_dim_crm_system_user_key
	,sales_area_dim_description_key
	,sell_junior_member_dues_flag
	,square_footage
	,[state]
	,state_or_province
	,telephone
	,val_member_activity_region_id
	,val_pt_rcl_area_id
	,val_region_id
	,val_sales_area_id
	,val_time_zone_id
	,dst_offset
	,st_offset
	,web_specialist_dim_crm_team_key
	,ltf_lt_work_email
	,ltf_web_specialist_team
	,workday_region
	,yoga_studios
	,club_membership_level
	,feature_basketball_courts_flag
	,feature_indoor_cycle_studio_flag
	,feature_fitness_studios_flag
	,feature_indoor_tennis_courts_flag
	,feature_mixed_combats_arts_studio_flag
	,feature_outdoor_tennis_courts_flag
	,feature_pilates_studio_flag
	,feature_racquetball_courts_flag
	,feature_squash_courts_flag
	,amenity_conditioner_flag
	,amenity_cotton_balls_flag
	,amenity_deodorant_flag
	,amenity_free_lockers_and_towels_flag
	,amenity_hairspray_flag
	,amenity_mouthwash_flag
	,amenity_plasma_big_screen_tvs_flag
	,amenity_razors_flag
	,amenity_shampoo_flag
	,amenity_wireless_internet_access_flag
	,app_created_by
	,app_modified_by
	,club_hours
	,compliance_asset_id
	,content_type
	,created_by
	,created_dim_date_key
	,feature_advanced_training_studio_flag
	,feature_alpha_field_flag
	,feature_bar_flag
	,feature_boxing_gym_flag
	,feature_business_center_flag
	,feature_cardio_equipment_flag
	,feature_child_center_flag
	,feature_connected_technology_flag
	,feature_deluge_flag
	,feature_free_weights_and_equipment_flag
	,feature_functional_training_area_flag
	,feature_hamam_flag
	,feature_indoor_golf_center_flag
	,feature_indoor_lap_pool_flag
	,feature_indoor_leisure_pool_flag
	,feature_indoor_turf_field_flag
	,feature_indoor_water_slides_flag
	,feature_indoor_whirlpool_flag
	,feature_infant_room_flag
	,feature_kids_academy_flag
	,feature_kids_gym_flag
	,feature_kids_language_arts_studio_flag
	,feature_kids_learning_lab_flag
	,feature_kids_media_room_flag
	,feature_kids_movement_studio_flag
	,feature_kids_outdoor_play_area_flag
	,feature_kids_tumbling_studio_flag
	,feature_lazy_river_flag
	,feature_lifecafe_flag
	,feature_lifecafe_poolside_bistro_flag
	,feature_lifespa_flag
	,feature_outdoor_fitness_trail_flag
	,feature_outdoor_lap_pool_flag
	,feature_outdoor_swimming_pool_flag
	,feature_outdoor_turf_field_flag
	,feature_outdoor_water_slides_flag
	,feature_outdoor_whirlpool_flag
	,feature_pickleball_court_flag
	,feature_play_maze_flag
	,feature_pre_school_flag
	,feature_proactive_care_clinic_flag
	,feature_rare_flag
	,feature_rehabilitation_and_chiropractic_clinic_flag
	,feature_resistance_training_area_flag
	,feature_retail_store_flag
	,feature_rock_wall_flag
	,feature_rooftop_patio_flag
	,feature_sand_volleyball_court_flag
	,feature_saunas_flag
	,feature_splash_pad_flag
	,feature_steam_rooms_flag
	,feature_volleyball_courts_flag
	,feature_walking_running_track_flag
	,feature_weight_machines_flag
	,feature_yoga_studio_flag
	,feature_zero_depth_entry_pool_flag
	,folder_child_count
	,general_manager
	,general_manager_email
	,general_manager_phone
	,item_child_count
	,item_type
	,label_applied
	,label_applied_by
	,label_setting
	,labels
	,member_services_manager
	,member_services_manager_email
	,member_services_manager_phone
	,modified_by
	,modified_dim_date_key
	,oms_flag
	,open_dim_date_key
	,path
	,program_badminton_flag
	,program_barre_flag
	,program_basketball_flag
	,program_birthday_parties_flag
	,program_boxing_flag
	,program_golf_flag
	,program_handball_flag
	,program_indoor_cycle_flag
	,program_kids_academy_flag
	,program_kids_activities_flag
	,program_kids_camps_flag
	,program_kids_sports_and_fitness_flag
	,program_kids_swim_flag
	,program_mixed_combat_arts_flag
	,program_outdoor_cycle_flag
	,program_personal_training_flag
	,program_pickleball_flag
	,program_pilates_flag
	,program_racquetball_flag
	,program_rock_climbing_flag
	,program_run_club_flag
	,program_soccer_flag
	,program_squash_flag
	,program_studio_flag
	,program_swimming_flag
	,program_table_tennis_flag
	,program_team_training_flag
	,program_tennis_flag
	,program_volleyball_flag
	,program_weight_loss_flag
	,program_yoga_flag
	,service_bar_flag
	,service_basketball_training_flag
	,service_child_center_flag
	,service_golf_instruction_flag
	,service_hair_flag
	,service_health_assessments_flag
	,service_kids_academy_flag
	,service_kids_birthday_parties_flag
	,service_kids_camps_flag
	,service_kids_swim_lessons_flag
	,service_lifecafe_flag
	,service_marathon_training_flag
	,service_massage_flag
	,service_medi_spa_flag
	,service_nails_flag
	,service_nutritional_coaching_flag
	,service_proactive_care_flag
	,service_racquetball_lessons_flag
	,service_rare_flag
	,service_rehabilitation_chiropractic_flag
	,service_run_training_flag
	,service_skin_flag
	,service_soccer_training_flag
	,service_squash_lessons_flag
	,service_swim_assessments_flag
	,service_team_training_flag
	,service_tennis_lessons_flag
	,service_tri_training_flag
	,service_weight_loss_flag
	,title
	,web_free_pass
	,web_inquiry_flag
	,web_presale_waitlist_flag
	,web_price_request_flag
	,dv_inserted_date_time
	,dv_insert_user
	,dv_load_date_time
	,dv_load_end_date_time
	,dv_batch_id
	)
SELECT dim_club_key
	,access_type
	,address_line_1
	,address_line_2
	,allow_junior_check_in_flag
	,area_director_dim_crm_system_user_key
	,assess_junior_member_dues_flag
	,center_type
	,check_in_group_level
	,child_center_weekly_limit
	,city
	,club_close_dim_date_key
	,club_code
	,club_id
	,club_name
	,club_open_dim_date_key
	,club_regional_manager_dim_crm_system_user_key
	,club_status
	,club_type
	,country
	,dim_crm_team_key
	,domain_name_prefix
	,fax_number
	,five_letter_club_code
	,formal_club_name
	,four_letter_club_code
	,general_manager_dim_crm_system_user_key
	,gl_club_id
	,growth_type
	,info_genesis_store_id
	,latitude
	,local_currency_code
	,longitude
	,marketing_name
	,marketing_club_level
	,marketing_map_region
	,max_junior_age
	,member_activities_region_dim_description_key
	,phone_number
	,postal_code
	,pt_rcl_area_dim_description_key
	,region_dim_description_key
	,regional_sales_lead_dim_crm_system_user_key
	,regional_vice_president_dim_crm_system_user_key
	,sales_area_dim_description_key
	,sell_junior_member_dues_flag
	,square_footage
	,[state]
	,state_or_province
	,telephone
	,val_member_activity_region_id
	,val_pt_rcl_area_id
	,val_region_id
	,val_sales_area_id
	,val_time_zone_id
	,dst_offset
	,st_offset
	,web_specialist_dim_crm_team_key
	,ltf_lt_work_email
	,ltf_web_specialist_team
	,workday_region
	,yoga_studios
	,club_membership_level
	,feature_basketball_courts_flag
	,feature_indoor_cycle_studio_flag
	,feature_fitness_studios_flag
	,feature_indoor_tennis_courts_flag
	,feature_mixed_combats_arts_studio_flag
	,feature_outdoor_tennis_courts_flag
	,feature_pilates_studio_flag
	,feature_racquetball_courts_flag
	,feature_squash_courts_flag
	,amenity_conditioner_flag
	,amenity_cotton_balls_flag
	,amenity_deodorant_flag
	,amenity_free_lockers_and_towels_flag
	,amenity_hairspray_flag
	,amenity_mouthwash_flag
	,amenity_plasma_big_screen_tvs_flag
	,amenity_razors_flag
	,amenity_shampoo_flag
	,amenity_wireless_internet_access_flag
	,app_created_by
	,app_modified_by
	,club_hours
	,compliance_asset_id
	,content_type
	,created_by
	,created_dim_date_key
	,feature_advanced_training_studio_flag
	,feature_alpha_field_flag
	,feature_bar_flag
	,feature_boxing_gym_flag
	,feature_business_center_flag
	,feature_cardio_equipment_flag
	,feature_child_center_flag
	,feature_connected_technology_flag
	,feature_deluge_flag
	,feature_free_weights_and_equipment_flag
	,feature_functional_training_area_flag
	,feature_hamam_flag
	,feature_indoor_golf_center_flag
	,feature_indoor_lap_pool_flag
	,feature_indoor_leisure_pool_flag
	,feature_indoor_turf_field_flag
	,feature_indoor_water_slides_flag
	,feature_indoor_whirlpool_flag
	,feature_infant_room_flag
	,feature_kids_academy_flag
	,feature_kids_gym_flag
	,feature_kids_language_arts_studio_flag
	,feature_kids_learning_lab_flag
	,feature_kids_media_room_flag
	,feature_kids_movement_studio_flag
	,feature_kids_outdoor_play_area_flag
	,feature_kids_tumbling_studio_flag
	,feature_lazy_river_flag
	,feature_lifecafe_flag
	,feature_lifecafe_poolside_bistro_flag
	,feature_lifespa_flag
	,feature_outdoor_fitness_trail_flag
	,feature_outdoor_lap_pool_flag
	,feature_outdoor_swimming_pool_flag
	,feature_outdoor_turf_field_flag
	,feature_outdoor_water_slides_flag
	,feature_outdoor_whirlpool_flag
	,feature_pickleball_court_flag
	,feature_play_maze_flag
	,feature_pre_school_flag
	,feature_proactive_care_clinic_flag
	,feature_rare_flag
	,feature_rehabilitation_and_chiropractic_clinic_flag
	,feature_resistance_training_area_flag
	,feature_retail_store_flag
	,feature_rock_wall_flag
	,feature_rooftop_patio_flag
	,feature_sand_volleyball_court_flag
	,feature_saunas_flag
	,feature_splash_pad_flag
	,feature_steam_rooms_flag
	,feature_volleyball_courts_flag
	,feature_walking_running_track_flag
	,feature_weight_machines_flag
	,feature_yoga_studio_flag
	,feature_zero_depth_entry_pool_flag
	,folder_child_count
	,general_manager
	,general_manager_email
	,general_manager_phone
	,item_child_count
	,item_type
	,label_applied
	,label_applied_by
	,label_setting
	,labels
	,member_services_manager
	,member_services_manager_email
	,member_services_manager_phone
	,modified_by
	,modified_dim_date_key
	,oms_flag
	,open_dim_date_key
	,path
	,program_badminton_flag
	,program_barre_flag
	,program_basketball_flag
	,program_birthday_parties_flag
	,program_boxing_flag
	,program_golf_flag
	,program_handball_flag
	,program_indoor_cycle_flag
	,program_kids_academy_flag
	,program_kids_activities_flag
	,program_kids_camps_flag
	,program_kids_sports_and_fitness_flag
	,program_kids_swim_flag
	,program_mixed_combat_arts_flag
	,program_outdoor_cycle_flag
	,program_personal_training_flag
	,program_pickleball_flag
	,program_pilates_flag
	,program_racquetball_flag
	,program_rock_climbing_flag
	,program_run_club_flag
	,program_soccer_flag
	,program_squash_flag
	,program_studio_flag
	,program_swimming_flag
	,program_table_tennis_flag
	,program_team_training_flag
	,program_tennis_flag
	,program_volleyball_flag
	,program_weight_loss_flag
	,program_yoga_flag
	,service_bar_flag
	,service_basketball_training_flag
	,service_child_center_flag
	,service_golf_instruction_flag
	,service_hair_flag
	,service_health_assessments_flag
	,service_kids_academy_flag
	,service_kids_birthday_parties_flag
	,service_kids_camps_flag
	,service_kids_swim_lessons_flag
	,service_lifecafe_flag
	,service_marathon_training_flag
	,service_massage_flag
	,service_medi_spa_flag
	,service_nails_flag
	,service_nutritional_coaching_flag
	,service_proactive_care_flag
	,service_racquetball_lessons_flag
	,service_rare_flag
	,service_rehabilitation_chiropractic_flag
	,service_run_training_flag
	,service_skin_flag
	,service_soccer_training_flag
	,service_squash_lessons_flag
	,service_swim_assessments_flag
	,service_team_training_flag
	,service_tennis_lessons_flag
	,service_tri_training_flag
	,service_weight_loss_flag
	,title
	,web_free_pass
	,web_inquiry_flag
	,web_presale_waitlist_flag
	,web_price_request_flag
	,@dt
	,@u
	,dv_load_date_time
	,dv_load_end_date_time
	,dv_batch_id
FROM #dim_club

drop table #dim_club

end

