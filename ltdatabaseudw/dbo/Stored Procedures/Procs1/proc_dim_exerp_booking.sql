CREATE PROC [dbo].[proc_dim_exerp_booking] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (SELECT max(isnull(dv_batch_id, - 1))	FROM dim_exerp_booking )
	DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
	DECLARE @load_dv_batch_id BIGINT = CASE	WHEN @max_dv_batch_id < @current_dv_batch_id THEN @max_dv_batch_id ELSE @current_dv_batch_id END

	IF object_id('tempdb..#etl_step1') IS NOT NULL
		DROP TABLE #etl_step1

	CREATE TABLE dbo.#etl_step1
		WITH (distribution = HASH (dim_exerp_booking_key),location = user_db) AS
	select d_exerp_booking.bk_hash dim_exerp_booking_key,
       d_exerp_booking.booking_id,
	   d_exerp_booking.booking_name,
	   d_exerp_booking.d_exerp_center_bk_hash dim_club_key,
	   d_exerp_booking.d_exerp_activity_bk_hash dim_exerp_activity_key,
	   d_exerp_booking.color,
	   d_exerp_booking.start_dim_date_key,
	   d_exerp_booking.start_dim_time_key,
	   d_exerp_booking.stop_dim_date_key,
	   d_exerp_booking.stop_dim_time_key,
	   d_exerp_booking.creation_dim_date_key,
	   d_exerp_booking.creation_dim_time_key,
	   d_exerp_booking.booking_state,
	   d_exerp_booking.class_capacity,
	   d_exerp_booking.waiting_list_capacity,
	   d_exerp_booking.cancel_dim_date_key,
	   d_exerp_booking.cancel_dim_time_key,
	   d_exerp_booking.cancel_reason,
	   d_exerp_booking.max_capacity_override,
	   d_exerp_booking.description,
	   d_exerp_booking.main_d_exerp_booking_bk_hash dim_exerp_booking_recurrence_key,
	   d_exerp_booking.comment,
	   d_exerp_booking.single_cancellation_flag,
	   d_exerp_booking.strict_age_limit,
	   d_exerp_booking.minimum_age,
	   d_exerp_booking.maximum_age,
	   d_exerp_booking.minimum_age_unit,
	   d_exerp_booking.maximum_age_unit,
	   d_exerp_booking.age_text,
	   d_exerp_booking.dv_load_date_time,
       convert(DATETIME, '99991231', 112) dv_load_end_date_time,
       d_exerp_booking.dv_batch_id
	from d_exerp_booking
	WHERE d_exerp_booking.dv_batch_id >= @load_dv_batch_id


	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_exerp_booking
	WHERE dim_exerp_booking_key IN (
			SELECT dim_exerp_booking_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_exerp_booking (
         dim_exerp_booking_key
        ,booking_id
	    ,booking_name
	    ,dim_club_key
	    ,dim_exerp_activity_key
	    ,color
		,comment
	    ,start_dim_date_key
	    ,start_dim_time_key
	    ,stop_dim_date_key
	    ,stop_dim_time_key
	    ,creation_dim_date_key
	    ,creation_dim_time_key
	    ,booking_state
	    ,class_capacity
	    ,waiting_list_capacity
	    ,cancel_dim_date_key
	    ,cancel_dim_time_key
	    ,cancel_reason
		,max_capacity_override
		,description
		,dim_exerp_booking_recurrence_key
		,single_cancellation_flag
		,strict_age_limit
		,minimum_age
		,maximum_age
		,minimum_age_unit
		,maximum_age_unit
		,age_text
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT           
		dim_exerp_booking_key
        ,booking_id
	    ,booking_name
	    ,dim_club_key
	    ,dim_exerp_activity_key
	    ,color
		,comment
	    ,start_dim_date_key
	    ,start_dim_time_key
	    ,stop_dim_date_key
	    ,stop_dim_time_key
	    ,creation_dim_date_key
	    ,creation_dim_time_key
	    ,booking_state
	    ,class_capacity
	    ,waiting_list_capacity
	    ,cancel_dim_date_key
	    ,cancel_dim_time_key
	    ,cancel_reason
		,max_capacity_override
		,description
		,dim_exerp_booking_recurrence_key
		,single_cancellation_flag
		,strict_age_limit
		,minimum_age
		,maximum_age
		,minimum_age_unit
		,maximum_age_unit
		,age_text
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN


END

