CREATE PROC [dbo].[proc_etl_udwcloudsync_enterprise_club_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_udwcloudsync_enterpriseclubdata

set @insert_date_time = getdate()
insert into dbo.stage_hash_udwcloudsync_enterpriseclubdata (
       bk_hash,
       Abbr,
       Address_Line_1,
       Address_Line_2,
       Amenity_Conditioner,
       Amenity_Cotton_Balls,
       Amenity_Deodorant,
       Amenity_Free_Lockers_and_Towels,
       Amenity_Hairspray,
       Amenity_Mouthwash,
       Amenity_Plasma_Big_Screen_TVs,
       Amenity_Razors,
       Amenity_Shampoo,
       Amenity_Wireless_Internet_Access,
       App_Created_By,
       App_Modified_By,
       City,
       Club_Hours,
       Club_Level,
       Club_Status,
       Compliance_Asset_Id,
       Content_Type,
       Country,
       Created,
       Created_By,
       Feature_Advanced_Training_Studio,
       Feature_Alpha_Field,
       Feature_Bar,
       Feature_Basketball_Courts,
       Feature_Boxing_Gym,
       Feature_Business_Center,
       Feature_Cardio_Equipment,
       Feature_Child_Center,
       Feature_Connected_Technology,
       Feature_Deluge,
       Feature_Fitness_Studios,
       Feature_Free_Weights_and_Equipment,
       Feature_Functional_Training_Area,
       Feature_Hamam,
       Feature_Indoor_Cycle_Studio,
       Feature_Indoor_Golf_Center,
       Feature_Indoor_Lap_Pool,
       Feature_Indoor_Leisure_Pool,
       Feature_Indoor_Tennis_Courts,
       Feature_Indoor_Turf_Field,
       Feature_Indoor_Water_Slides,
       Feature_Indoor_Whirlpool,
       Feature_Infant_Room,
       Feature_Kids_Academy,
       Feature_Kids_Gym,
       Feature_Kids_Language_Arts_Studio,
       Feature_Kids_Learning_Lab,
       Feature_Kids_Media_Room,
       Feature_Kids_Movement_Studio,
       Feature_Kids_Outdoor_Play_Area,
       Feature_Kids_Tumbling_Studio,
       Feature_Lazy_River,
       Feature_LifeCafe,
       Feature_LifeCafe_Poolside_Bistro,
       Feature_LifeSpa,
       Feature_Mixed_Combats_Arts_Studio,
       Feature_Outdoor_Fitness_Trail,
       Feature_Outdoor_Lap_Pool,
       Feature_Outdoor_Swimming_Pool,
       Feature_Outdoor_Tennis_Courts,
       Feature_Outdoor_Turf_Field,
       Feature_Outdoor_Water_Slides,
       Feature_Outdoor_Whirlpool,
       Feature_Pickleball_Court,
       Feature_Pilates_Studio,
       Feature_Play_Maze,
       Feature_Pre_School,
       Feature_Proactive_Care_Clinic,
       Feature_Racquetball_Courts,
       Feature_Rare,
       Feature_Rehabilitation_and_Chiropractic_Clinic,
       Feature_Resistance_Training_Area,
       Feature_Retail_Store,
       Feature_Rock_Wall,
       Feature_Rooftop_Patio,
       Feature_Sand_Volleyball_Court,
       Feature_Saunas,
       Feature_Splash_Pad,
       Feature_Squash_Courts,
       Feature_Steam_Rooms,
       Feature_Volleyball_Courts,
       Feature_Walking_Running_Track,
       Feature_Weight_Machines,
       Feature_Yoga_Studio,
       Feature_Zero_Depth_Entry_Pool,
       Folder_Child_Count,
       General_Manager,
       General_Manager_Email,
       General_Manager_Phone,
       [ID],
       Item_Child_Count,
       Item_Type,
       Label_Applied,
       Label_applied_by,
       Label_setting,
       Labels,
       Latitude,
       Longitude,
       Member_Services_Manager,
       Member_Services_Manager_Email,
       Member_Services_Manager_Phone,
       MMS_Club_ID,
       Modified,
       Modified_By,
       OMS,
       Open_Date,
       Path,
       Phone,
       Program_Badminton,
       Program_Barre,
       Program_Basketball,
       Program_Birthday_Parties,
       Program_Boxing,
       Program_Golf,
       Program_Handball,
       Program_Indoor_Cycle,
       Program_Kids_Academy,
       Program_Kids_Activities,
       Program_Kids_Camps,
       Program_Kids_Sports_and_Fitness,
       Program_Kids_Swim,
       Program_Mixed_Combat_Arts,
       Program_Outdoor_Cycle,
       Program_Personal_Training,
       Program_Pickleball,
       Program_Pilates,
       Program_Racquetball,
       Program_Rock_Climbing,
       Program_Run_Club,
       Program_Soccer,
       Program_Squash,
       Program_Studio,
       Program_Swimming,
       Program_Table_Tennis,
       Program_TEAM_Training,
       Program_Tennis,
       Program_Volleyball,
       Program_Weight_Loss,
       Program_Yoga,
       Region,
       Service_Bar,
       Service_Basketball_Training,
       Service_Child_Center,
       Service_Golf_Instruction,
       Service_Hair,
       Service_Health_Assessments,
       Service_Kids_Academy,
       Service_Kids_Birthday_Parties,
       Service_Kids_Camps,
       Service_Kids_Swim_Lessons,
       Service_LifeCafe,
       Service_Marathon_Training,
       Service_Massage,
       Service_Medi_Spa,
       Service_Nails,
       Service_Nutritional_Coaching,
       Service_Proactive_Care,
       Service_Racquetball_Lessons,
       Service_Rare,
       Service_Rehabilitation_Chiropractic,
       Service_Run_Training,
       Service_Skin,
       Service_Soccer_Training,
       Service_Squash_Lessons,
       Service_Swim_Assessments,
       Service_TEAM_Training,
       Service_Tennis_Lessons,
       Service_Tri_Training,
       Service_Weight_Loss,
       Square_Footage,
       StateProvince,
       Title,
       Web_Free_Pass,
       Web_Inquiry,
       Web_PreSale_Waitlist,
       Web_Price_Request,
       Zip,
       inserteddatetime,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([ID] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       Abbr,
       Address_Line_1,
       Address_Line_2,
       Amenity_Conditioner,
       Amenity_Cotton_Balls,
       Amenity_Deodorant,
       Amenity_Free_Lockers_and_Towels,
       Amenity_Hairspray,
       Amenity_Mouthwash,
       Amenity_Plasma_Big_Screen_TVs,
       Amenity_Razors,
       Amenity_Shampoo,
       Amenity_Wireless_Internet_Access,
       App_Created_By,
       App_Modified_By,
       City,
       Club_Hours,
       Club_Level,
       Club_Status,
       Compliance_Asset_Id,
       Content_Type,
       Country,
       Created,
       Created_By,
       Feature_Advanced_Training_Studio,
       Feature_Alpha_Field,
       Feature_Bar,
       Feature_Basketball_Courts,
       Feature_Boxing_Gym,
       Feature_Business_Center,
       Feature_Cardio_Equipment,
       Feature_Child_Center,
       Feature_Connected_Technology,
       Feature_Deluge,
       Feature_Fitness_Studios,
       Feature_Free_Weights_and_Equipment,
       Feature_Functional_Training_Area,
       Feature_Hamam,
       Feature_Indoor_Cycle_Studio,
       Feature_Indoor_Golf_Center,
       Feature_Indoor_Lap_Pool,
       Feature_Indoor_Leisure_Pool,
       Feature_Indoor_Tennis_Courts,
       Feature_Indoor_Turf_Field,
       Feature_Indoor_Water_Slides,
       Feature_Indoor_Whirlpool,
       Feature_Infant_Room,
       Feature_Kids_Academy,
       Feature_Kids_Gym,
       Feature_Kids_Language_Arts_Studio,
       Feature_Kids_Learning_Lab,
       Feature_Kids_Media_Room,
       Feature_Kids_Movement_Studio,
       Feature_Kids_Outdoor_Play_Area,
       Feature_Kids_Tumbling_Studio,
       Feature_Lazy_River,
       Feature_LifeCafe,
       Feature_LifeCafe_Poolside_Bistro,
       Feature_LifeSpa,
       Feature_Mixed_Combats_Arts_Studio,
       Feature_Outdoor_Fitness_Trail,
       Feature_Outdoor_Lap_Pool,
       Feature_Outdoor_Swimming_Pool,
       Feature_Outdoor_Tennis_Courts,
       Feature_Outdoor_Turf_Field,
       Feature_Outdoor_Water_Slides,
       Feature_Outdoor_Whirlpool,
       Feature_Pickleball_Court,
       Feature_Pilates_Studio,
       Feature_Play_Maze,
       Feature_Pre_School,
       Feature_Proactive_Care_Clinic,
       Feature_Racquetball_Courts,
       Feature_Rare,
       Feature_Rehabilitation_and_Chiropractic_Clinic,
       Feature_Resistance_Training_Area,
       Feature_Retail_Store,
       Feature_Rock_Wall,
       Feature_Rooftop_Patio,
       Feature_Sand_Volleyball_Court,
       Feature_Saunas,
       Feature_Splash_Pad,
       Feature_Squash_Courts,
       Feature_Steam_Rooms,
       Feature_Volleyball_Courts,
       Feature_Walking_Running_Track,
       Feature_Weight_Machines,
       Feature_Yoga_Studio,
       Feature_Zero_Depth_Entry_Pool,
       Folder_Child_Count,
       General_Manager,
       General_Manager_Email,
       General_Manager_Phone,
       [ID],
       Item_Child_Count,
       Item_Type,
       Label_Applied,
       Label_applied_by,
       Label_setting,
       Labels,
       Latitude,
       Longitude,
       Member_Services_Manager,
       Member_Services_Manager_Email,
       Member_Services_Manager_Phone,
       MMS_Club_ID,
       Modified,
       Modified_By,
       OMS,
       Open_Date,
       Path,
       Phone,
       Program_Badminton,
       Program_Barre,
       Program_Basketball,
       Program_Birthday_Parties,
       Program_Boxing,
       Program_Golf,
       Program_Handball,
       Program_Indoor_Cycle,
       Program_Kids_Academy,
       Program_Kids_Activities,
       Program_Kids_Camps,
       Program_Kids_Sports_and_Fitness,
       Program_Kids_Swim,
       Program_Mixed_Combat_Arts,
       Program_Outdoor_Cycle,
       Program_Personal_Training,
       Program_Pickleball,
       Program_Pilates,
       Program_Racquetball,
       Program_Rock_Climbing,
       Program_Run_Club,
       Program_Soccer,
       Program_Squash,
       Program_Studio,
       Program_Swimming,
       Program_Table_Tennis,
       Program_TEAM_Training,
       Program_Tennis,
       Program_Volleyball,
       Program_Weight_Loss,
       Program_Yoga,
       Region,
       Service_Bar,
       Service_Basketball_Training,
       Service_Child_Center,
       Service_Golf_Instruction,
       Service_Hair,
       Service_Health_Assessments,
       Service_Kids_Academy,
       Service_Kids_Birthday_Parties,
       Service_Kids_Camps,
       Service_Kids_Swim_Lessons,
       Service_LifeCafe,
       Service_Marathon_Training,
       Service_Massage,
       Service_Medi_Spa,
       Service_Nails,
       Service_Nutritional_Coaching,
       Service_Proactive_Care,
       Service_Racquetball_Lessons,
       Service_Rare,
       Service_Rehabilitation_Chiropractic,
       Service_Run_Training,
       Service_Skin,
       Service_Soccer_Training,
       Service_Squash_Lessons,
       Service_Swim_Assessments,
       Service_TEAM_Training,
       Service_Tennis_Lessons,
       Service_Tri_Training,
       Service_Weight_Loss,
       Square_Footage,
       StateProvince,
       Title,
       Web_Free_Pass,
       Web_Inquiry,
       Web_PreSale_Waitlist,
       Web_Price_Request,
       Zip,
       inserteddatetime,
       isnull(cast(stage_udwcloudsync_enterpriseclubdata.inserteddatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_udwcloudsync_enterpriseclubdata
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_udwcloudsync_enterprise_club_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_udwcloudsync_enterprise_club_data (
       bk_hash,
       enterprise_club_data_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_udwcloudsync_enterpriseclubdata.bk_hash,
       stage_hash_udwcloudsync_enterpriseclubdata.[ID] enterprise_club_data_id,
       isnull(cast(stage_hash_udwcloudsync_enterpriseclubdata.inserteddatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       7,
       @insert_date_time,
       @user
  from stage_hash_udwcloudsync_enterpriseclubdata
  left join h_udwcloudsync_enterprise_club_data
    on stage_hash_udwcloudsync_enterpriseclubdata.bk_hash = h_udwcloudsync_enterprise_club_data.bk_hash
 where h_udwcloudsync_enterprise_club_data_id is null
   and stage_hash_udwcloudsync_enterpriseclubdata.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_udwcloudsync_enterprise_club_data
if object_id('tempdb..#l_udwcloudsync_enterprise_club_data_inserts') is not null drop table #l_udwcloudsync_enterprise_club_data_inserts
create table #l_udwcloudsync_enterprise_club_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_udwcloudsync_enterpriseclubdata.bk_hash,
       stage_hash_udwcloudsync_enterpriseclubdata.[ID] enterprise_club_data_id,
       stage_hash_udwcloudsync_enterpriseclubdata.MMS_Club_ID mms_club_id,
       isnull(cast(stage_hash_udwcloudsync_enterpriseclubdata.inserteddatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_enterpriseclubdata.[ID] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.MMS_Club_ID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_udwcloudsync_enterpriseclubdata
 where stage_hash_udwcloudsync_enterpriseclubdata.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_udwcloudsync_enterprise_club_data records
set @insert_date_time = getdate()
insert into l_udwcloudsync_enterprise_club_data (
       bk_hash,
       enterprise_club_data_id,
       mms_club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_udwcloudsync_enterprise_club_data_inserts.bk_hash,
       #l_udwcloudsync_enterprise_club_data_inserts.enterprise_club_data_id,
       #l_udwcloudsync_enterprise_club_data_inserts.mms_club_id,
       case when l_udwcloudsync_enterprise_club_data.l_udwcloudsync_enterprise_club_data_id is null then isnull(#l_udwcloudsync_enterprise_club_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       7,
       #l_udwcloudsync_enterprise_club_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_udwcloudsync_enterprise_club_data_inserts
  left join p_udwcloudsync_enterprise_club_data
    on #l_udwcloudsync_enterprise_club_data_inserts.bk_hash = p_udwcloudsync_enterprise_club_data.bk_hash
   and p_udwcloudsync_enterprise_club_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_udwcloudsync_enterprise_club_data
    on p_udwcloudsync_enterprise_club_data.bk_hash = l_udwcloudsync_enterprise_club_data.bk_hash
   and p_udwcloudsync_enterprise_club_data.l_udwcloudsync_enterprise_club_data_id = l_udwcloudsync_enterprise_club_data.l_udwcloudsync_enterprise_club_data_id
 where l_udwcloudsync_enterprise_club_data.l_udwcloudsync_enterprise_club_data_id is null
    or (l_udwcloudsync_enterprise_club_data.l_udwcloudsync_enterprise_club_data_id is not null
        and l_udwcloudsync_enterprise_club_data.dv_hash <> #l_udwcloudsync_enterprise_club_data_inserts.source_hash)

--calculate hash and lookup to current s_udwcloudsync_enterprise_club_data
if object_id('tempdb..#s_udwcloudsync_enterprise_club_data_inserts') is not null drop table #s_udwcloudsync_enterprise_club_data_inserts
create table #s_udwcloudsync_enterprise_club_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_udwcloudsync_enterpriseclubdata.bk_hash,
       stage_hash_udwcloudsync_enterpriseclubdata.Abbr abbr,
       stage_hash_udwcloudsync_enterpriseclubdata.Address_Line_1 address_line_1,
       stage_hash_udwcloudsync_enterpriseclubdata.Address_Line_2 address_line_2,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Conditioner amenity_conditioner,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Cotton_Balls amenity_cotton_balls,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Deodorant amenity_deodorant,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Free_Lockers_and_Towels amenity_free_lockers_and_towels,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Hairspray amenity_hairspray,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Mouthwash amenity_mouthwash,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Plasma_Big_Screen_TVs amenity_plasma_big_screen_tvs,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Razors amenity_razors,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Shampoo amenity_shampoo,
       stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Wireless_Internet_Access amenity_wireless_internet_access,
       stage_hash_udwcloudsync_enterpriseclubdata.App_Created_By app_created_by,
       stage_hash_udwcloudsync_enterpriseclubdata.App_Modified_By app_modified_by,
       stage_hash_udwcloudsync_enterpriseclubdata.City city,
       stage_hash_udwcloudsync_enterpriseclubdata.Club_Hours club_hours,
       stage_hash_udwcloudsync_enterpriseclubdata.Club_Level club_level,
       stage_hash_udwcloudsync_enterpriseclubdata.Club_Status club_status,
       stage_hash_udwcloudsync_enterpriseclubdata.Compliance_Asset_Id compliance_asset_id,
       stage_hash_udwcloudsync_enterpriseclubdata.Content_Type content_type,
       stage_hash_udwcloudsync_enterpriseclubdata.Country country,
       stage_hash_udwcloudsync_enterpriseclubdata.Created created,
       stage_hash_udwcloudsync_enterpriseclubdata.Created_By created_by,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Advanced_Training_Studio feature_advanced_training_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Alpha_Field feature_alpha_field,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Bar feature_bar,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Basketball_Courts feature_basketball_courts,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Boxing_Gym feature_boxing_gym,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Business_Center feature_business_center,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Cardio_Equipment feature_cardio_equipment,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Child_Center feature_child_center,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Connected_Technology feature_connected_technology,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Deluge feature_deluge,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Fitness_Studios feature_fitness_studios,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Free_Weights_and_Equipment feature_free_weights_and_equipment,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Functional_Training_Area feature_functional_training_area,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Hamam feature_hamam,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Cycle_Studio feature_indoor_cycle_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Golf_Center feature_indoor_golf_center,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Lap_Pool feature_indoor_lap_pool,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Leisure_Pool feature_indoor_leisure_pool,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Tennis_Courts feature_indoor_tennis_courts,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Turf_Field feature_indoor_turf_field,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Water_Slides feature_indoor_water_slides,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Whirlpool feature_indoor_whirlpool,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Infant_Room feature_infant_room,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Academy feature_kids_academy,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Gym feature_kids_gym,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Language_Arts_Studio feature_kids_language_arts_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Learning_Lab feature_kids_learning_lab,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Media_Room feature_kids_media_room,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Movement_Studio feature_kids_movement_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Outdoor_Play_Area feature_kids_outdoor_play_area,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Tumbling_Studio feature_kids_tumbling_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Lazy_River feature_lazy_river,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_LifeCafe feature_lifecafe,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_LifeCafe_Poolside_Bistro feature_lifecafe_poolside_bistro,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_LifeSpa feature_lifespa,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Mixed_Combats_Arts_Studio feature_mixed_combats_arts_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Fitness_Trail feature_outdoor_fitness_trail,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Lap_Pool feature_outdoor_lap_pool,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Swimming_Pool feature_outdoor_swimming_pool,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Tennis_Courts feature_outdoor_tennis_courts,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Turf_Field feature_outdoor_turf_field,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Water_Slides feature_outdoor_water_slides,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Whirlpool feature_outdoor_whirlpool,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Pickleball_Court feature_pickleball_court,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Pilates_Studio feature_pilates_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Play_Maze feature_play_maze,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Pre_School feature_pre_school,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Proactive_Care_Clinic feature_proactive_care_clinic,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Racquetball_Courts feature_racquetball_courts,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Rare feature_rare,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Rehabilitation_and_Chiropractic_Clinic feature_rehabilitation_and_chiropractic_clinic,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Resistance_Training_Area feature_resistance_training_area,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Retail_Store feature_retail_store,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Rock_Wall feature_rock_wall,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Rooftop_Patio feature_rooftop_patio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Sand_Volleyball_Court feature_sand_volleyball_court,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Saunas feature_saunas,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Splash_Pad feature_splash_pad,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Squash_Courts feature_squash_courts,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Steam_Rooms feature_steam_rooms,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Volleyball_Courts feature_volleyball_courts,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Walking_Running_Track feature_walking_running_track,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Weight_Machines feature_weight_machines,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Yoga_Studio feature_yoga_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Feature_Zero_Depth_Entry_Pool feature_zero_depth_entry_pool,
       stage_hash_udwcloudsync_enterpriseclubdata.Folder_Child_Count folder_child_count,
       stage_hash_udwcloudsync_enterpriseclubdata.General_Manager general_manager,
       stage_hash_udwcloudsync_enterpriseclubdata.General_Manager_Email general_manager_email,
       stage_hash_udwcloudsync_enterpriseclubdata.General_Manager_Phone general_manager_phone,
       stage_hash_udwcloudsync_enterpriseclubdata.[ID] enterprise_club_data_id,
       stage_hash_udwcloudsync_enterpriseclubdata.Item_Child_Count item_child_count,
       stage_hash_udwcloudsync_enterpriseclubdata.Item_Type item_type,
       stage_hash_udwcloudsync_enterpriseclubdata.Label_Applied label_applied,
       stage_hash_udwcloudsync_enterpriseclubdata.Label_applied_by label_applied_by,
       stage_hash_udwcloudsync_enterpriseclubdata.Label_setting label_setting,
       stage_hash_udwcloudsync_enterpriseclubdata.Labels labels,
       stage_hash_udwcloudsync_enterpriseclubdata.Latitude latitude,
       stage_hash_udwcloudsync_enterpriseclubdata.Longitude longitude,
       stage_hash_udwcloudsync_enterpriseclubdata.Member_Services_Manager member_services_manager,
       stage_hash_udwcloudsync_enterpriseclubdata.Member_Services_Manager_Email member_services_manager_email,
       stage_hash_udwcloudsync_enterpriseclubdata.Member_Services_Manager_Phone member_services_manager_phone,
       stage_hash_udwcloudsync_enterpriseclubdata.Modified modified,
       stage_hash_udwcloudsync_enterpriseclubdata.Modified_By modified_by,
       stage_hash_udwcloudsync_enterpriseclubdata.OMS oms,
       stage_hash_udwcloudsync_enterpriseclubdata.Open_Date open_date,
       stage_hash_udwcloudsync_enterpriseclubdata.Path path,
       stage_hash_udwcloudsync_enterpriseclubdata.Phone phone,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Badminton program_badminton,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Barre program_barre,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Basketball program_basketball,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Birthday_Parties program_birthday_parties,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Boxing program_boxing,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Golf program_golf,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Handball program_handball,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Indoor_Cycle program_indoor_cycle,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Academy program_kids_academy,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Activities program_kids_activities,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Camps program_kids_camps,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Sports_and_Fitness program_kids_sports_and_fitness,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Swim program_kids_swim,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Mixed_Combat_Arts program_mixed_combat_arts,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Outdoor_Cycle program_outdoor_cycle,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Personal_Training program_personal_training,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Pickleball program_pickleball,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Pilates program_pilates,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Racquetball program_racquetball,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Rock_Climbing program_rock_climbing,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Run_Club program_run_club,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Soccer program_soccer,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Squash program_squash,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Studio program_studio,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Swimming program_swimming,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Table_Tennis program_table_tennis,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_TEAM_Training program_team_training,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Tennis program_tennis,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Volleyball program_volleyball,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Weight_Loss program_weight_loss,
       stage_hash_udwcloudsync_enterpriseclubdata.Program_Yoga program_yoga,
       stage_hash_udwcloudsync_enterpriseclubdata.Region region,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Bar service_bar,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Basketball_Training service_basketball_training,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Child_Center service_child_center,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Golf_Instruction service_golf_instruction,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Hair service_hair,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Health_Assessments service_health_assessments,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Kids_Academy service_kids_academy,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Kids_Birthday_Parties service_kids_birthday_parties,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Kids_Camps service_kids_camps,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Kids_Swim_Lessons service_kids_swim_lessons,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_LifeCafe service_lifecafe,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Marathon_Training service_marathon_training,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Massage service_massage,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Medi_Spa service_medi_spa,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Nails service_nails,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Nutritional_Coaching service_nutritional_coaching,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Proactive_Care service_proactive_care,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Racquetball_Lessons service_racquetball_lessons,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Rare service_rare,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Rehabilitation_Chiropractic service_rehabilitation_chiropractic,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Run_Training service_run_training,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Skin service_skin,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Soccer_Training service_soccer_training,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Squash_Lessons service_squash_lessons,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Swim_Assessments service_swim_assessments,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_TEAM_Training service_team_training,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Tennis_Lessons service_tennis_lessons,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Tri_Training service_tri_training,
       stage_hash_udwcloudsync_enterpriseclubdata.Service_Weight_Loss service_weight_loss,
       stage_hash_udwcloudsync_enterpriseclubdata.Square_Footage square_footage,
       stage_hash_udwcloudsync_enterpriseclubdata.StateProvince stateprovince,
       stage_hash_udwcloudsync_enterpriseclubdata.Title title,
       stage_hash_udwcloudsync_enterpriseclubdata.Web_Free_Pass web_free_pass,
       stage_hash_udwcloudsync_enterpriseclubdata.Web_Inquiry web_inquiry,
       stage_hash_udwcloudsync_enterpriseclubdata.Web_PreSale_Waitlist web_presale_waitlist,
       stage_hash_udwcloudsync_enterpriseclubdata.Web_Price_Request web_price_request,
       stage_hash_udwcloudsync_enterpriseclubdata.Zip zip,
       stage_hash_udwcloudsync_enterpriseclubdata.inserteddatetime inserted_date_time,
       isnull(cast(stage_hash_udwcloudsync_enterpriseclubdata.inserteddatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Abbr,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Address_Line_1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Address_Line_2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Conditioner,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Cotton_Balls,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Deodorant,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Free_Lockers_and_Towels,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Hairspray,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Mouthwash,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Plasma_Big_Screen_TVs,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Razors,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Shampoo,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Amenity_Wireless_Internet_Access,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.App_Created_By,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.App_Modified_By,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.City,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Club_Hours,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Club_Level,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Club_Status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Compliance_Asset_Id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Content_Type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_enterpriseclubdata.Created,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Created_By,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Advanced_Training_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Alpha_Field,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Bar,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Basketball_Courts,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Boxing_Gym,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Business_Center,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Cardio_Equipment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Child_Center,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Connected_Technology,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Deluge,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Fitness_Studios,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Free_Weights_and_Equipment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Functional_Training_Area,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Hamam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Cycle_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Golf_Center,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Lap_Pool,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Leisure_Pool,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Tennis_Courts,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Turf_Field,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Water_Slides,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Indoor_Whirlpool,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Infant_Room,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Academy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Gym,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Language_Arts_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Learning_Lab,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Media_Room,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Movement_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Outdoor_Play_Area,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Kids_Tumbling_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Lazy_River,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_LifeCafe,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_LifeCafe_Poolside_Bistro,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_LifeSpa,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Mixed_Combats_Arts_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Fitness_Trail,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Lap_Pool,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Swimming_Pool,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Tennis_Courts,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Turf_Field,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Water_Slides,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Outdoor_Whirlpool,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Pickleball_Court,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Pilates_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Play_Maze,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Pre_School,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Proactive_Care_Clinic,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Racquetball_Courts,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Rare,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Rehabilitation_and_Chiropractic_Clinic,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Resistance_Training_Area,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Retail_Store,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Rock_Wall,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Rooftop_Patio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Sand_Volleyball_Court,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Saunas,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Splash_Pad,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Squash_Courts,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Steam_Rooms,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Volleyball_Courts,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Walking_Running_Track,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Weight_Machines,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Yoga_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Feature_Zero_Depth_Entry_Pool,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Folder_Child_Count,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.General_Manager,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.General_Manager_Email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.General_Manager_Phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_enterpriseclubdata.[ID] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Item_Child_Count,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Item_Type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Label_Applied,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Label_applied_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Label_setting,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Labels,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Latitude,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Longitude,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Member_Services_Manager,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Member_Services_Manager_Email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Member_Services_Manager_Phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_enterpriseclubdata.Modified,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Modified_By,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.OMS,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_enterpriseclubdata.Open_Date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Path,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Phone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Badminton,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Barre,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Basketball,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Birthday_Parties,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Boxing,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Golf,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Handball,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Indoor_Cycle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Academy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Activities,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Camps,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Sports_and_Fitness,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Kids_Swim,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Mixed_Combat_Arts,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Outdoor_Cycle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Personal_Training,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Pickleball,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Pilates,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Racquetball,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Rock_Climbing,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Run_Club,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Soccer,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Squash,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Studio,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Swimming,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Table_Tennis,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_TEAM_Training,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Tennis,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Volleyball,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Weight_Loss,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Program_Yoga,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Region,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Bar,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Basketball_Training,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Child_Center,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Golf_Instruction,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Hair,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Health_Assessments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Kids_Academy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Kids_Birthday_Parties,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Kids_Camps,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Kids_Swim_Lessons,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_LifeCafe,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Marathon_Training,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Massage,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Medi_Spa,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Nails,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Nutritional_Coaching,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Proactive_Care,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Racquetball_Lessons,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Rare,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Rehabilitation_Chiropractic,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Run_Training,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Skin,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Soccer_Training,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Squash_Lessons,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Swim_Assessments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_TEAM_Training,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Tennis_Lessons,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Tri_Training,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Service_Weight_Loss,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Square_Footage,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.StateProvince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Web_Free_Pass,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_enterpriseclubdata.Web_Inquiry as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_enterpriseclubdata.Web_PreSale_Waitlist as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_enterpriseclubdata.Web_Price_Request as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_enterpriseclubdata.Zip,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_enterpriseclubdata.inserteddatetime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_udwcloudsync_enterpriseclubdata
 where stage_hash_udwcloudsync_enterpriseclubdata.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_udwcloudsync_enterprise_club_data records
set @insert_date_time = getdate()
insert into s_udwcloudsync_enterprise_club_data (
       bk_hash,
       abbr,
       address_line_1,
       address_line_2,
       amenity_conditioner,
       amenity_cotton_balls,
       amenity_deodorant,
       amenity_free_lockers_and_towels,
       amenity_hairspray,
       amenity_mouthwash,
       amenity_plasma_big_screen_tvs,
       amenity_razors,
       amenity_shampoo,
       amenity_wireless_internet_access,
       app_created_by,
       app_modified_by,
       city,
       club_hours,
       club_level,
       club_status,
       compliance_asset_id,
       content_type,
       country,
       created,
       created_by,
       feature_advanced_training_studio,
       feature_alpha_field,
       feature_bar,
       feature_basketball_courts,
       feature_boxing_gym,
       feature_business_center,
       feature_cardio_equipment,
       feature_child_center,
       feature_connected_technology,
       feature_deluge,
       feature_fitness_studios,
       feature_free_weights_and_equipment,
       feature_functional_training_area,
       feature_hamam,
       feature_indoor_cycle_studio,
       feature_indoor_golf_center,
       feature_indoor_lap_pool,
       feature_indoor_leisure_pool,
       feature_indoor_tennis_courts,
       feature_indoor_turf_field,
       feature_indoor_water_slides,
       feature_indoor_whirlpool,
       feature_infant_room,
       feature_kids_academy,
       feature_kids_gym,
       feature_kids_language_arts_studio,
       feature_kids_learning_lab,
       feature_kids_media_room,
       feature_kids_movement_studio,
       feature_kids_outdoor_play_area,
       feature_kids_tumbling_studio,
       feature_lazy_river,
       feature_lifecafe,
       feature_lifecafe_poolside_bistro,
       feature_lifespa,
       feature_mixed_combats_arts_studio,
       feature_outdoor_fitness_trail,
       feature_outdoor_lap_pool,
       feature_outdoor_swimming_pool,
       feature_outdoor_tennis_courts,
       feature_outdoor_turf_field,
       feature_outdoor_water_slides,
       feature_outdoor_whirlpool,
       feature_pickleball_court,
       feature_pilates_studio,
       feature_play_maze,
       feature_pre_school,
       feature_proactive_care_clinic,
       feature_racquetball_courts,
       feature_rare,
       feature_rehabilitation_and_chiropractic_clinic,
       feature_resistance_training_area,
       feature_retail_store,
       feature_rock_wall,
       feature_rooftop_patio,
       feature_sand_volleyball_court,
       feature_saunas,
       feature_splash_pad,
       feature_squash_courts,
       feature_steam_rooms,
       feature_volleyball_courts,
       feature_walking_running_track,
       feature_weight_machines,
       feature_yoga_studio,
       feature_zero_depth_entry_pool,
       folder_child_count,
       general_manager,
       general_manager_email,
       general_manager_phone,
       enterprise_club_data_id,
       item_child_count,
       item_type,
       label_applied,
       label_applied_by,
       label_setting,
       labels,
       latitude,
       longitude,
       member_services_manager,
       member_services_manager_email,
       member_services_manager_phone,
       modified,
       modified_by,
       oms,
       open_date,
       path,
       phone,
       program_badminton,
       program_barre,
       program_basketball,
       program_birthday_parties,
       program_boxing,
       program_golf,
       program_handball,
       program_indoor_cycle,
       program_kids_academy,
       program_kids_activities,
       program_kids_camps,
       program_kids_sports_and_fitness,
       program_kids_swim,
       program_mixed_combat_arts,
       program_outdoor_cycle,
       program_personal_training,
       program_pickleball,
       program_pilates,
       program_racquetball,
       program_rock_climbing,
       program_run_club,
       program_soccer,
       program_squash,
       program_studio,
       program_swimming,
       program_table_tennis,
       program_team_training,
       program_tennis,
       program_volleyball,
       program_weight_loss,
       program_yoga,
       region,
       service_bar,
       service_basketball_training,
       service_child_center,
       service_golf_instruction,
       service_hair,
       service_health_assessments,
       service_kids_academy,
       service_kids_birthday_parties,
       service_kids_camps,
       service_kids_swim_lessons,
       service_lifecafe,
       service_marathon_training,
       service_massage,
       service_medi_spa,
       service_nails,
       service_nutritional_coaching,
       service_proactive_care,
       service_racquetball_lessons,
       service_rare,
       service_rehabilitation_chiropractic,
       service_run_training,
       service_skin,
       service_soccer_training,
       service_squash_lessons,
       service_swim_assessments,
       service_team_training,
       service_tennis_lessons,
       service_tri_training,
       service_weight_loss,
       square_footage,
       stateprovince,
       title,
       web_free_pass,
       web_inquiry,
       web_presale_waitlist,
       web_price_request,
       zip,
       inserted_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_udwcloudsync_enterprise_club_data_inserts.bk_hash,
       #s_udwcloudsync_enterprise_club_data_inserts.abbr,
       #s_udwcloudsync_enterprise_club_data_inserts.address_line_1,
       #s_udwcloudsync_enterprise_club_data_inserts.address_line_2,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_conditioner,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_cotton_balls,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_deodorant,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_free_lockers_and_towels,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_hairspray,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_mouthwash,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_plasma_big_screen_tvs,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_razors,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_shampoo,
       #s_udwcloudsync_enterprise_club_data_inserts.amenity_wireless_internet_access,
       #s_udwcloudsync_enterprise_club_data_inserts.app_created_by,
       #s_udwcloudsync_enterprise_club_data_inserts.app_modified_by,
       #s_udwcloudsync_enterprise_club_data_inserts.city,
       #s_udwcloudsync_enterprise_club_data_inserts.club_hours,
       #s_udwcloudsync_enterprise_club_data_inserts.club_level,
       #s_udwcloudsync_enterprise_club_data_inserts.club_status,
       #s_udwcloudsync_enterprise_club_data_inserts.compliance_asset_id,
       #s_udwcloudsync_enterprise_club_data_inserts.content_type,
       #s_udwcloudsync_enterprise_club_data_inserts.country,
       #s_udwcloudsync_enterprise_club_data_inserts.created,
       #s_udwcloudsync_enterprise_club_data_inserts.created_by,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_advanced_training_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_alpha_field,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_bar,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_basketball_courts,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_boxing_gym,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_business_center,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_cardio_equipment,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_child_center,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_connected_technology,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_deluge,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_fitness_studios,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_free_weights_and_equipment,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_functional_training_area,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_hamam,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_indoor_cycle_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_indoor_golf_center,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_indoor_lap_pool,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_indoor_leisure_pool,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_indoor_tennis_courts,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_indoor_turf_field,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_indoor_water_slides,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_indoor_whirlpool,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_infant_room,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_kids_academy,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_kids_gym,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_kids_language_arts_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_kids_learning_lab,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_kids_media_room,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_kids_movement_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_kids_outdoor_play_area,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_kids_tumbling_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_lazy_river,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_lifecafe,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_lifecafe_poolside_bistro,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_lifespa,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_mixed_combats_arts_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_outdoor_fitness_trail,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_outdoor_lap_pool,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_outdoor_swimming_pool,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_outdoor_tennis_courts,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_outdoor_turf_field,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_outdoor_water_slides,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_outdoor_whirlpool,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_pickleball_court,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_pilates_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_play_maze,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_pre_school,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_proactive_care_clinic,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_racquetball_courts,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_rare,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_rehabilitation_and_chiropractic_clinic,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_resistance_training_area,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_retail_store,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_rock_wall,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_rooftop_patio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_sand_volleyball_court,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_saunas,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_splash_pad,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_squash_courts,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_steam_rooms,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_volleyball_courts,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_walking_running_track,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_weight_machines,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_yoga_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.feature_zero_depth_entry_pool,
       #s_udwcloudsync_enterprise_club_data_inserts.folder_child_count,
       #s_udwcloudsync_enterprise_club_data_inserts.general_manager,
       #s_udwcloudsync_enterprise_club_data_inserts.general_manager_email,
       #s_udwcloudsync_enterprise_club_data_inserts.general_manager_phone,
       #s_udwcloudsync_enterprise_club_data_inserts.enterprise_club_data_id,
       #s_udwcloudsync_enterprise_club_data_inserts.item_child_count,
       #s_udwcloudsync_enterprise_club_data_inserts.item_type,
       #s_udwcloudsync_enterprise_club_data_inserts.label_applied,
       #s_udwcloudsync_enterprise_club_data_inserts.label_applied_by,
       #s_udwcloudsync_enterprise_club_data_inserts.label_setting,
       #s_udwcloudsync_enterprise_club_data_inserts.labels,
       #s_udwcloudsync_enterprise_club_data_inserts.latitude,
       #s_udwcloudsync_enterprise_club_data_inserts.longitude,
       #s_udwcloudsync_enterprise_club_data_inserts.member_services_manager,
       #s_udwcloudsync_enterprise_club_data_inserts.member_services_manager_email,
       #s_udwcloudsync_enterprise_club_data_inserts.member_services_manager_phone,
       #s_udwcloudsync_enterprise_club_data_inserts.modified,
       #s_udwcloudsync_enterprise_club_data_inserts.modified_by,
       #s_udwcloudsync_enterprise_club_data_inserts.oms,
       #s_udwcloudsync_enterprise_club_data_inserts.open_date,
       #s_udwcloudsync_enterprise_club_data_inserts.path,
       #s_udwcloudsync_enterprise_club_data_inserts.phone,
       #s_udwcloudsync_enterprise_club_data_inserts.program_badminton,
       #s_udwcloudsync_enterprise_club_data_inserts.program_barre,
       #s_udwcloudsync_enterprise_club_data_inserts.program_basketball,
       #s_udwcloudsync_enterprise_club_data_inserts.program_birthday_parties,
       #s_udwcloudsync_enterprise_club_data_inserts.program_boxing,
       #s_udwcloudsync_enterprise_club_data_inserts.program_golf,
       #s_udwcloudsync_enterprise_club_data_inserts.program_handball,
       #s_udwcloudsync_enterprise_club_data_inserts.program_indoor_cycle,
       #s_udwcloudsync_enterprise_club_data_inserts.program_kids_academy,
       #s_udwcloudsync_enterprise_club_data_inserts.program_kids_activities,
       #s_udwcloudsync_enterprise_club_data_inserts.program_kids_camps,
       #s_udwcloudsync_enterprise_club_data_inserts.program_kids_sports_and_fitness,
       #s_udwcloudsync_enterprise_club_data_inserts.program_kids_swim,
       #s_udwcloudsync_enterprise_club_data_inserts.program_mixed_combat_arts,
       #s_udwcloudsync_enterprise_club_data_inserts.program_outdoor_cycle,
       #s_udwcloudsync_enterprise_club_data_inserts.program_personal_training,
       #s_udwcloudsync_enterprise_club_data_inserts.program_pickleball,
       #s_udwcloudsync_enterprise_club_data_inserts.program_pilates,
       #s_udwcloudsync_enterprise_club_data_inserts.program_racquetball,
       #s_udwcloudsync_enterprise_club_data_inserts.program_rock_climbing,
       #s_udwcloudsync_enterprise_club_data_inserts.program_run_club,
       #s_udwcloudsync_enterprise_club_data_inserts.program_soccer,
       #s_udwcloudsync_enterprise_club_data_inserts.program_squash,
       #s_udwcloudsync_enterprise_club_data_inserts.program_studio,
       #s_udwcloudsync_enterprise_club_data_inserts.program_swimming,
       #s_udwcloudsync_enterprise_club_data_inserts.program_table_tennis,
       #s_udwcloudsync_enterprise_club_data_inserts.program_team_training,
       #s_udwcloudsync_enterprise_club_data_inserts.program_tennis,
       #s_udwcloudsync_enterprise_club_data_inserts.program_volleyball,
       #s_udwcloudsync_enterprise_club_data_inserts.program_weight_loss,
       #s_udwcloudsync_enterprise_club_data_inserts.program_yoga,
       #s_udwcloudsync_enterprise_club_data_inserts.region,
       #s_udwcloudsync_enterprise_club_data_inserts.service_bar,
       #s_udwcloudsync_enterprise_club_data_inserts.service_basketball_training,
       #s_udwcloudsync_enterprise_club_data_inserts.service_child_center,
       #s_udwcloudsync_enterprise_club_data_inserts.service_golf_instruction,
       #s_udwcloudsync_enterprise_club_data_inserts.service_hair,
       #s_udwcloudsync_enterprise_club_data_inserts.service_health_assessments,
       #s_udwcloudsync_enterprise_club_data_inserts.service_kids_academy,
       #s_udwcloudsync_enterprise_club_data_inserts.service_kids_birthday_parties,
       #s_udwcloudsync_enterprise_club_data_inserts.service_kids_camps,
       #s_udwcloudsync_enterprise_club_data_inserts.service_kids_swim_lessons,
       #s_udwcloudsync_enterprise_club_data_inserts.service_lifecafe,
       #s_udwcloudsync_enterprise_club_data_inserts.service_marathon_training,
       #s_udwcloudsync_enterprise_club_data_inserts.service_massage,
       #s_udwcloudsync_enterprise_club_data_inserts.service_medi_spa,
       #s_udwcloudsync_enterprise_club_data_inserts.service_nails,
       #s_udwcloudsync_enterprise_club_data_inserts.service_nutritional_coaching,
       #s_udwcloudsync_enterprise_club_data_inserts.service_proactive_care,
       #s_udwcloudsync_enterprise_club_data_inserts.service_racquetball_lessons,
       #s_udwcloudsync_enterprise_club_data_inserts.service_rare,
       #s_udwcloudsync_enterprise_club_data_inserts.service_rehabilitation_chiropractic,
       #s_udwcloudsync_enterprise_club_data_inserts.service_run_training,
       #s_udwcloudsync_enterprise_club_data_inserts.service_skin,
       #s_udwcloudsync_enterprise_club_data_inserts.service_soccer_training,
       #s_udwcloudsync_enterprise_club_data_inserts.service_squash_lessons,
       #s_udwcloudsync_enterprise_club_data_inserts.service_swim_assessments,
       #s_udwcloudsync_enterprise_club_data_inserts.service_team_training,
       #s_udwcloudsync_enterprise_club_data_inserts.service_tennis_lessons,
       #s_udwcloudsync_enterprise_club_data_inserts.service_tri_training,
       #s_udwcloudsync_enterprise_club_data_inserts.service_weight_loss,
       #s_udwcloudsync_enterprise_club_data_inserts.square_footage,
       #s_udwcloudsync_enterprise_club_data_inserts.stateprovince,
       #s_udwcloudsync_enterprise_club_data_inserts.title,
       #s_udwcloudsync_enterprise_club_data_inserts.web_free_pass,
       #s_udwcloudsync_enterprise_club_data_inserts.web_inquiry,
       #s_udwcloudsync_enterprise_club_data_inserts.web_presale_waitlist,
       #s_udwcloudsync_enterprise_club_data_inserts.web_price_request,
       #s_udwcloudsync_enterprise_club_data_inserts.zip,
       #s_udwcloudsync_enterprise_club_data_inserts.inserted_date_time,
       case when s_udwcloudsync_enterprise_club_data.s_udwcloudsync_enterprise_club_data_id is null then isnull(#s_udwcloudsync_enterprise_club_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       7,
       #s_udwcloudsync_enterprise_club_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_udwcloudsync_enterprise_club_data_inserts
  left join p_udwcloudsync_enterprise_club_data
    on #s_udwcloudsync_enterprise_club_data_inserts.bk_hash = p_udwcloudsync_enterprise_club_data.bk_hash
   and p_udwcloudsync_enterprise_club_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_udwcloudsync_enterprise_club_data
    on p_udwcloudsync_enterprise_club_data.bk_hash = s_udwcloudsync_enterprise_club_data.bk_hash
   and p_udwcloudsync_enterprise_club_data.s_udwcloudsync_enterprise_club_data_id = s_udwcloudsync_enterprise_club_data.s_udwcloudsync_enterprise_club_data_id
 where s_udwcloudsync_enterprise_club_data.s_udwcloudsync_enterprise_club_data_id is null
    or (s_udwcloudsync_enterprise_club_data.s_udwcloudsync_enterprise_club_data_id is not null
        and s_udwcloudsync_enterprise_club_data.dv_hash <> #s_udwcloudsync_enterprise_club_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_udwcloudsync_enterprise_club_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_udwcloudsync_enterprise_club_data @current_dv_batch_id

end
