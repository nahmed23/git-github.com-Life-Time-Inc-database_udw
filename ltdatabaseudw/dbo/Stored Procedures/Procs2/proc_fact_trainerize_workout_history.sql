CREATE PROC [dbo].[proc_fact_trainerize_workout_history] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM fact_trainerize_workout_history
			)
	DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
	DECLARE @load_dv_batch_id BIGINT = CASE 
			WHEN @max_dv_batch_id < @current_dv_batch_id
				THEN @max_dv_batch_id
			ELSE @current_dv_batch_id
			END

	IF object_id('tempdb..#etl_step1') IS NOT NULL
		DROP TABLE #etl_step1

	CREATE TABLE dbo.#etl_step1
		WITH (
				distribution = HASH (fact_trainerize_workout_history_key)
				,location = user_db
				) AS
select   
		 d_ec_Workout_Histories.fact_trainerize_workout_history_key 	fact_trainerize_workout_history_key
		,d_ec_Workout_Histories.workout_history_id 						workout_history_id
		,d_ec_Workout_Histories.activity_type 							activity_type
		,d_ec_Workout_Histories.average_heart_rate 						average_heart_rate
		,d_ec_Workout_Histories.average_miles_per_hour 					average_miles_per_hour
		,d_ec_Workout_Histories.average_watts 							average_watts
		,d_ec_Workout_Histories.comments	  							comments
		,d_ec_Workout_Histories.completed_flag	  						completed_flag
		,d_ec_Workout_Histories.created_dim_date_key					created_dim_date_key
		,case 
			when map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key is null 
				then '-998'
			else 
				map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key end dim_mms_member_key		
		,d_ec_Workout_Histories.d_workout_key	  						dim_trainerize_workout_key
		,d_ec_Workout_Histories.workout_description	  					workout_description
		,d_ec_Workout_Histories.distance_in_miles	  					distance_in_miles
		,d_ec_Workout_Histories.ended_dim_date_key	  					ended_dim_date_key
		,d_ec_Workout_Histories.ended_dim_time_key	  					ended_dim_time_key
		,d_ec_Workout_Histories.fat_calories	  						fat_calories
		,d_ec_Workout_Histories.heart_rate_zone_five_seconds	  		heart_rate_zone_five_seconds
		,d_ec_Workout_Histories.heart_rate_zone_four_seconds	  		heart_rate_zone_four_seconds
		,d_ec_Workout_Histories.heart_rate_zone_one_seconds	  			heart_rate_zone_one_seconds
		,d_ec_Workout_Histories.heart_rate_zone_three_seconds	  		heart_rate_zone_three_seconds
		,d_ec_Workout_Histories.heart_rate_zone_two_seconds	  			heart_rate_zone_two_seconds
		,d_ec_Workout_Histories.active_flag	  							active_flag
		,d_ec_Workout_Histories.custom_flag	  							custom_flag
		,d_ec_Workout_Histories.started_flag	  						started_flag
		,d_ec_Workout_Histories.key_value	  							key_value
		,d_ec_Workout_Histories.rating	  								rating
		,d_ec_Workout_Histories.scheduled_flag  						scheduled_flag
		,d_ec_Workout_Histories.source_name	  							source_name
		,d_ec_Workout_Histories.source_workout_id	  					source_workout_id
		,d_ec_Workout_Histories.started_dim_date_key	  				started_dim_date_key
		,d_ec_Workout_Histories.started_dim_time_key	  				started_dim_time_key
		,d_ec_Workout_Histories.total_calories	  						total_calories
		,d_ec_Workout_Histories.tracked_flag							tracked_flag
		,d_ec_Workout_Histories.workout_type							workout_type
	    ,case 
	    	when isnull(d_ec_Workout_Histories.dv_load_date_time,'Jan 1, 1753') >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
					then d_ec_Workout_Histories.dv_load_date_time
			else isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753') 
	      end dv_load_date_time
	    ,case 
	    	when isnull(d_ec_Workout_Histories.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
					then d_ec_Workout_Histories.dv_batch_id
			else isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1) 
	      end dv_batch_id
	    ,convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
/*select * */
from d_ec_Workout_Histories
    left join map_ltfeb_party_id_dim_mms_member_key
		on d_ec_Workout_Histories.party_id = map_ltfeb_party_id_dim_mms_member_key.party_id
    where (( d_ec_Workout_Histories.dv_batch_id >= @load_dv_batch_id
		or map_ltfeb_party_id_dim_mms_member_key.dv_batch_id >= @load_dv_batch_id))

	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.fact_trainerize_workout_history
	WHERE fact_trainerize_workout_history_key IN (
			SELECT fact_trainerize_workout_history_key
			FROM dbo.#etl_step1
			)

	INSERT INTO fact_trainerize_workout_history(
		  fact_trainerize_workout_history_key
		, workout_history_id
		, activity_type
		, average_heart_rate
		, average_miles_per_hour
		, average_watts
		, comments
		, completed_flag
		, created_dim_date_key
		, dim_mms_member_key
		, dim_trainerize_workout_key
		, workout_description
		, distance_in_miles
		, ended_dim_date_key
		, ended_dim_time_key
		, fat_calories
		, heart_rate_zone_five_seconds
		, heart_rate_zone_four_seconds
		, heart_rate_zone_one_seconds
		, heart_rate_zone_three_seconds
		, heart_rate_zone_two_seconds
		, active_flag
		, custom_flag
		, started_flag
		, key_value
		, rating
		, scheduled_flag
		, source_name
		, source_workout_id
		, started_dim_date_key
		, started_dim_time_key
		, total_calories
		, tracked_flag
		, workout_type
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, dv_inserted_date_time
		, dv_insert_user
		)
	SELECT
		  fact_trainerize_workout_history_key
		, workout_history_id
		, activity_type
		, average_heart_rate
		, average_miles_per_hour
		, average_watts
		, comments
		, completed_flag
		, created_dim_date_key
		, dim_mms_member_key
		, dim_trainerize_workout_key
		, workout_description
		, distance_in_miles
		, ended_dim_date_key
		, ended_dim_time_key
		, fat_calories
		, heart_rate_zone_five_seconds
		, heart_rate_zone_four_seconds
		, heart_rate_zone_one_seconds
		, heart_rate_zone_three_seconds
		, heart_rate_zone_two_seconds
		, active_flag
		, custom_flag
		, started_flag
		, key_value
		, rating
		, scheduled_flag
		, source_name
		, source_workout_id
		, started_dim_date_key
		, started_dim_time_key
		, total_calories
		, tracked_flag
		, workout_type
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, getdate()
		, suser_sname()
	FROM #etl_step1

	COMMIT TRAN

			
END
