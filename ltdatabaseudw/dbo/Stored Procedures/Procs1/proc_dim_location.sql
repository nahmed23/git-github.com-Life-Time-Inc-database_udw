CREATE PROC [dbo].[proc_dim_location] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_location)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


/*****************************************UDW data for dim_location*****************************************/

IF object_id('tempdb..#temp_dim_club') IS NOT NULL DROP TABLE #temp_dim_club

CREATE TABLE dbo.#temp_dim_club WITH (distribution = HASH (udw_business_key),location = user_db) 
AS (
	SELECT dim_club_key AS udw_business_key
		  ,formal_club_name AS description
		  ,club_name 
		  ,club_id AS external_id
		  ,REPLACE(REPLACE(REPLACE(display_name, 'feature_', ''), '_flag', ''), '_', ' ') display_name
		  ,club_type
		  ,ColValue
	FROM marketing.v_dim_club
	UNPIVOT(ColValue FOR display_name IN (
	/****************************************************Include all the Club Features Here****************************************************/
	 feature_advanced_training_studio_flag,feature_alpha_field_flag,feature_bar_flag,feature_basketball_courts_flag	,feature_boxing_gym_flag
	,feature_business_center_flag,feature_cardio_equipment_flag,feature_child_center_flag,feature_connected_technology_flag
	,feature_deluge_flag,feature_fitness_studios_flag,feature_free_weights_and_equipment_flag,feature_functional_training_area_flag
	,feature_hamam_flag,feature_indoor_cycle_studio_flag,feature_indoor_GOlf_center_flag,feature_indoor_lap_pool_flag,feature_indoor_leisure_pool_flag
	,feature_indoor_tennis_courts_flag,feature_indoor_turf_field_flag,feature_indoor_water_slides_flag,feature_indoor_whirlpool_flag
	,feature_infant_room_flag,feature_kids_academy_flag,feature_kids_gym_flag,feature_kids_language_arts_studio_flag,feature_kids_learning_lab_flag
	,feature_kids_media_room_flag,feature_kids_movement_studio_flag,feature_kids_outdoor_play_area_flag,feature_kids_tumbling_studio_flag
	,feature_lazy_river_flag,feature_lifecafe_flag,feature_lifecafe_poolside_bistro_flag,feature_lifespa_flag,feature_mixed_combats_arts_studio_flag
	,feature_outdoor_fitness_trail_flag,feature_outdoor_lap_pool_flag,feature_outdoor_swimming_pool_flag,feature_outdoor_tennis_courts_flag
	,feature_outdoor_turf_field_flag,feature_outdoor_water_slides_flag,feature_outdoor_whirlpool_flag,feature_pickleball_court_flag
	,feature_pilates_studio_flag,feature_play_maze_flag,feature_pre_school_flag,feature_proactive_care_clinic_flag,feature_racquetball_courts_flag
	,feature_rare_flag,feature_rehabilitation_and_chiropractic_clinic_flag,feature_resistance_training_area_flag,feature_retail_store_flag
	,feature_rock_wall_flag,feature_rooftop_patio_flag,feature_sand_volleyball_court_flag,feature_saunas_flag,feature_splash_pad_flag
	,feature_squash_courts_flag,feature_steam_rooms_flag,feature_volleyball_courts_flag,feature_walking_running_track_flag
	,feature_weight_machines_flag,feature_yoga_studio_flag,feature_zero_depth_entry_pool_flag
						)) unpvt
			WHERE len(club_name) > 0
	UNION
	SELECT dim_club_key AS udw_business_key
		  ,formal_club_name AS description
		  ,club_name 
		  ,club_id AS external_id
		  ,display_name
		  ,club_type
		  ,'Y' AS ColValue
	FROM [dbo].[stg_club_feature_manual_load]
	
			)

IF object_id('tempdb..#etl_step_1') IS NOT NULL DROP TABLE #etl_step_1

CREATE TABLE dbo.#etl_step_1 WITH (distribution = HASH (udw_business_key),location = user_db) 
AS
SELECT udw_business_key
	,description
	,club_name
	,external_id
	,display_name
	,club_type
	,ColValue
	,CASE WHEN lower(display_name) IN ('lifecafe','lifecafé','lifespa','lifetime work','fitness')
			THEN 2
		ELSE 3 	END AS hierarchy_level
FROM dbo.#temp_dim_club
WHERE ColValue = 'Y'

IF object_id('tempdb..#etl_step_2') IS NOT NULL DROP TABLE #etl_step_2

CREATE TABLE dbo.#etl_step_2 WITH (distribution = HASH (val_location_type_id),location = user_db) 
AS
SELECT d_loc_val_location_type.val_location_type_id AS val_location_type_id
	,d_loc_val_location_type.val_location_type_name AS val_location_type_name
	,d_loc_val_location_type.display_name AS val_location_type_display_name
	,d_loc_val_location_type_group.val_location_type_group_name AS val_location_type_group_name
	,d_loc_val_location_type_group.display_name AS val_location_type_group_display_name
	,CASE WHEN val_location_type_name ='Non-Club Location' THEN 'MMS Non-Club Location'
	ELSE 'Club' END AS club_type
FROM dbo.d_loc_val_location_type
JOIN dbo.d_loc_val_location_type_group 
ON d_loc_val_location_type_group.val_location_type_group_id = d_loc_val_location_type.val_location_type_group_id

IF object_id('tempdb..#etl_step_3') IS NOT NULL DROP TABLE #etl_step_3

CREATE TABLE dbo.#etl_step_3 WITH (distribution = HASH (dim_location_key),location = user_db) 
AS (
		SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(club_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+cast(1 as varchar(500))
                                  +'P%#&z$@k'+isnull(cast(val_location_type_id as varchar(500)),'z#@$k%&P')
										  )
					),2) dim_location_key
			,'-997' AS parent_dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(club_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+cast(1 as varchar(500))
                                  +'P%#&z$@k'+isnull(cast(val_location_type_id as varchar(500)),'z#@$k%&P')
					                    )
					),2) top_level_dim_location_key
			,dim_club_key AS business_key
			,'MMS.Club' AS business_source_name
			,'1' AS hierarchy_level
			,#etl_step_2.val_location_type_name AS location_type_name
			,#etl_step_2.val_location_type_display_name AS location_type_display_name
			,formal_club_name AS display_name
			,club_name AS description
			,#etl_step_2.val_location_type_group_name AS location_type_group_name
			,#etl_step_2.val_location_type_group_display_name AS location_type_group_display_name
			,'Y' AS managed_by_udw_flag
			,club_id AS external_id
			,convert(VARCHAR, getdate(), 112) AS created_dim_date_key
			,suser_sname() AS created_by
		FROM marketing.v_dim_club
		CROSS JOIN #etl_step_2 WHERE #etl_step_2.val_location_type_name = 'LT Site'
		AND len(club_name) > 0

		UNION
		
/***********************hierarchy level "2" for club_type club data data*****************************/

		SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(club_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+cast(2 as varchar(500))								  
                                  +'P%#&z$@k'+isnull(cast(child.val_location_type_id as varchar(500)),'z#@$k%&P')
										)
					   ),2) dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(club_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+cast(1 as varchar(500))
                                  +'P%#&z$@k'+isnull(cast(parent.val_location_type_id as varchar(500)),'z#@$k%&P')
									    )
						),2) AS parent_dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(club_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+cast(1 as varchar(500))
                                  +'P%#&z$@k'+isnull(cast(parent.val_location_type_id as varchar(500)),'z#@$k%&P')
										)
					  ),2) top_level_dim_location_key
			,dim_club_key AS business_key
			,'MMS.Club' AS business_source_name
			,'2' AS hierarchy_level
			,child.val_location_type_name AS location_type_name
			,child.val_location_type_display_name AS location_type_display_name
			,formal_club_name AS display_name
			,formal_club_name AS description
			,child.val_location_type_group_name AS location_type_group_name
			,child.val_location_type_group_display_name AS location_type_group_display_name
			,'Y' AS managed_by_udw_flag
			,club_id AS external_id
			,convert(VARCHAR, getdate(), 112) AS created_dim_date_key
			,suser_sname() AS created_by
		FROM marketing.v_dim_club
		LEFT JOIN #etl_step_2 child ON v_dim_club.club_type=child.club_type
		AND child.val_location_type_name IN ('Life Time Club','Non-Club Location')
		JOIN #etl_step_2 parent ON  parent.val_location_type_name ='LT Site' 
		WHERE len(club_name) > 0
		
		UNION


/***********************hierarchy level "2" & "3" data*****************************/
		SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+isnull(cast(hierarchy_level as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(child.val_location_type_id as varchar(500)),'z#@$k%&P')
										  )
					 ),2) dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+isnull(cast(hierarchy_level-1 as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(parent.val_location_type_id as varchar(500)),'z#@$k%&P')
										)
					 ),2) AS parent_dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+cast(1 as varchar(500))
                                  +'P%#&z$@k'+isnull(cast(parent.val_location_type_id as varchar(500)),'z#@$k%&P')
										)
					 ),2) top_level_dim_location_key
			,#etl_step_1.udw_business_key AS business_key
			,'MMS.Club' AS business_source_name
			,hierarchy_level
			,child.val_location_type_name AS location_type_name
			,child.val_location_type_display_name AS location_type_display_name
			,#etl_step_1.display_name AS display_name
			,#etl_step_1.description AS description
			,child.val_location_type_group_name AS location_type_group_name
			,child.val_location_type_group_display_name AS location_type_group_display_name
			,'Y' AS managed_by_udw_flag
			,#etl_step_1.external_id AS external_id
			,convert(VARCHAR, getdate(), 112) AS created_dim_date_key
			,suser_sname() AS created_by
		FROM #etl_step_1
		LEFT JOIN #etl_step_2 child ON child.val_location_type_name = #etl_step_1.display_name and #etl_step_1.club_type=child.club_type
		JOIN #etl_step_2 parent ON parent.val_location_type_name ='LT Site'  
		and #etl_step_1.club_type=parent.club_type and #etl_step_1.club_type=parent.club_type
		WHERE hierarchy_level=2
		
		UNION
		
		SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+isnull(cast(hierarchy_level as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(child.val_location_type_id as varchar(500)),'z#@$k%&P')
										  )
					 ),2) dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+isnull(cast(hierarchy_level-1 as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(parent.val_location_type_id as varchar(500)),'z#@$k%&P')
										)
					 ),2) AS parent_dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+cast(1 as varchar(500))
                                  +'P%#&z$@k'+isnull(cast(top_parent.val_location_type_id as varchar(500)),'z#@$k%&P')
										)
					 ),2) top_level_dim_location_key
			,#etl_step_1.udw_business_key AS business_key
			,'MMS.Club' AS business_source_name
			,hierarchy_level
			,child.val_location_type_name AS location_type_name
			,child.val_location_type_display_name AS location_type_display_name
			,#etl_step_1.display_name AS display_name
			,#etl_step_1.description AS description
			,child.val_location_type_group_name AS location_type_group_name
			,child.val_location_type_group_display_name AS location_type_group_display_name
			,'Y' AS managed_by_udw_flag
			,#etl_step_1.external_id AS external_id
			,convert(VARCHAR, getdate(), 112) AS created_dim_date_key
			,suser_sname() AS created_by
		FROM #etl_step_1
		LEFT JOIN #etl_step_2 child ON child.val_location_type_name = #etl_step_1.display_name and #etl_step_1.club_type=child.club_type
		JOIN #etl_step_2 parent ON parent.val_location_type_name IN ('Life Time Club','Non-Club Location') and #etl_step_1.club_type=parent.club_type
		JOIN #etl_step_2 top_parent ON top_parent.val_location_type_name ='LT Site'  
		and #etl_step_1.club_type=top_parent.club_type
		WHERE child.val_location_type_name NOT IN ('Bar','Lifecafe Poolside Bistro','Lifecafe Poolside Bistro','Rooftop Patio','Rare')
		and hierarchy_level=3
		
		UNION

/***********************hierarchy level "2" & "3" data*****************************/
		SELECT CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+isnull(cast(hierarchy_level as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(child.val_location_type_id as varchar(500)),'z#@$k%&P')
										  )
					 ),2) dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+isnull(cast(hierarchy_level-1 as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+isnull(cast(parent.val_location_type_id as varchar(500)),'z#@$k%&P')
										)
					 ),2) AS parent_dim_location_key
			,CONVERT(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(external_id as varchar(500)),'z#@$k%&P')
                                  +'P%#&z$@k'+'MMS.Club'
								  +'P%#&z$@k'+cast(1 as varchar(500))
                                  +'P%#&z$@k'+isnull(cast(top_parent.val_location_type_id as varchar(500)),'z#@$k%&P')
										)
					 ),2) top_level_dim_location_key
			,#etl_step_1.udw_business_key AS business_key
			,'MMS.Club' AS business_source_name
			,hierarchy_level
			,child.val_location_type_name AS location_type_name
			,child.val_location_type_display_name AS location_type_display_name
			,#etl_step_1.display_name AS display_name
			,#etl_step_1.description AS description
			,child.val_location_type_group_name AS location_type_group_name
			,child.val_location_type_group_display_name AS location_type_group_display_name
			,'Y' AS managed_by_udw_flag
			,#etl_step_1.external_id AS external_id
			,convert(VARCHAR, getdate(), 112) AS created_dim_date_key
			,suser_sname() AS created_by
		FROM #etl_step_1
		LEFT JOIN #etl_step_2 child ON child.val_location_type_name = #etl_step_1.display_name and #etl_step_1.club_type=child.club_type
		JOIN #etl_step_2 parent ON parent.val_location_type_name ='Lifecafe' and #etl_step_1.club_type=parent.club_type
		JOIN #etl_step_2 top_parent ON top_parent.val_location_type_name ='LT Site'  
		and #etl_step_1.club_type=top_parent.club_type
		WHERE child.val_location_type_name IN ('Bar','Lifecafe Poolside Bistro','Lifecafe Poolside Bistro','Rooftop Patio','Rare')
	
   )

/******************************************Load data into dim_location table******************************************/


truncate table dbo.dim_location

BEGIN TRAN

INSERT INTO dbo.dim_location (
	business_key
	,business_source_name
	,created_by
	,created_dim_date_key
	,deleted_by
	,deleted_dim_date_key
	,description
	,dim_location_key
	,display_name
	,hierarchy_level
	,location_type_display_name
	,location_type_group_display_name
	,location_type_group_name
	,location_type_name
	,managed_by_udw_flag
	,parent_dim_location_key
	,top_level_dim_location_key
	,updated_by
	,updated_dim_date_key
	,dv_load_date_time
	,dv_load_end_date_time
	,dv_batch_id
	,dv_inserted_date_time
	,dv_insert_user
	)
SELECT business_key
	,business_source_name
	,created_by
	,created_dim_date_key
	,'' AS deleted_by
	,'-997' AS deleted_dim_date_key
	,description
	,dim_location_key
	,display_name
	,hierarchy_level
	,location_type_display_name
	,location_type_group_display_name
	,location_type_group_name
	,location_type_name
	,managed_by_udw_flag
	,parent_dim_location_key
	,top_level_dim_location_key
	,'' AS updated_by
	,'-997' AS updated_dim_date_key
	,'' AS dv_load_date_time
	,'dec 31, 9999'
	,@load_dv_batch_id
	,getdate()
	,suser_sname()
FROM #etl_step_3

COMMIT TRAN 

END