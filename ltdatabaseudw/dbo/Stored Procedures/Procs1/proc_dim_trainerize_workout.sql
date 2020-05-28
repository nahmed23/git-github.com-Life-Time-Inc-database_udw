CREATE PROC [dbo].[proc_dim_trainerize_workout] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM dim_trainerize_workout
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
				distribution = HASH (dim_trainerize_workout_key)
				,location = user_db
				) AS
select   d_ec_workouts.dim_trainerize_workout_key	dim_trainerize_workout_key
        ,d_ec_workouts.workouts_id					workouts_id
        ,case 
			when d_ec_workouts.created_dim_date_key is null 
				then '-998'
			else 
				d_ec_workouts.created_dim_date_key end created_dim_date_key		
        ,case 
			when map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key is null 
				then '-998'
			else 
				map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key end dim_mms_member_key
        ,d_ec_workouts.discriminator    			discriminator
        ,case 
			when d_ec_workouts.inactive_dim_date_key is null 
				then '-998'
			else 
				d_ec_workouts.inactive_dim_date_key end inactive_dim_date_key		
        ,case 
			when d_ec_workouts.modified_dim_date_key is null 
				then '-998'
			else 
				d_ec_workouts.modified_dim_date_key end modified_dim_date_key		
        ,d_ec_workouts.tags							tags
        ,d_ec_workouts.description					workout_description
        ,d_ec_workouts.name							workout_name
        ,d_ec_workouts.type							workout_type
	    ,case 
	    	when isnull(d_ec_workouts.dv_load_date_time,'Jan 1, 1753') >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
					then d_ec_workouts.dv_load_date_time
			else isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753') 
	      end dv_load_date_time
	    ,case 
	    	when isnull(d_ec_workouts.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
					then d_ec_workouts.dv_batch_id
			else isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1) 
	      end dv_batch_id
	    ,convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
/*select * */
from d_ec_workouts
    left join map_ltfeb_party_id_dim_mms_member_key
		on d_ec_workouts.party_id = map_ltfeb_party_id_dim_mms_member_key.party_id
    where (( d_ec_workouts.dv_batch_id >= @load_dv_batch_id
		or map_ltfeb_party_id_dim_mms_member_key.dv_batch_id >= @load_dv_batch_id))

	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_trainerize_workout
	WHERE dim_trainerize_workout_key IN (
			SELECT dim_trainerize_workout_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_trainerize_workout(
 	      dim_trainerize_workout_key
		, workouts_id
		, created_dim_date_key
		, dim_mms_member_key
		, discriminator
		, inactive_dim_date_key
		, modified_dim_date_key
		, tags
		, workout_description
		, workout_name
		, workout_type
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, dv_inserted_date_time
		, dv_insert_user
		)
	SELECT
 	      dim_trainerize_workout_key
		, workouts_id
		, created_dim_date_key
		, dim_mms_member_key
		, discriminator
		, inactive_dim_date_key
		, modified_dim_date_key
		, tags
		, workout_description
		, workout_name
		, workout_type
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, getdate()
		, suser_sname()
	FROM #etl_step1

	COMMIT TRAN

			
END
