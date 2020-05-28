CREATE PROC [dbo].[proc_dim_exerp_booking_recurrence] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM dim_exerp_booking_recurrence
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
				distribution = HASH (dim_exerp_booking_recurrence_key)
				,location = user_db
				) AS
	select d_exerp_booking.bk_hash as dim_exerp_booking_recurrence_key,
		d_exerp_booking.main_booking_id as main_booking_id,
		isnull(d_exerp_booking_recurrence.recurrence_type, 'N/A') as recurrence_type,
		isnull(d_exerp_booking_recurrence.recurrence, 'N/A') as recurrence,
		isnull(d_exerp_booking_recurrence.recurrence_start_dim_date_key, d_exerp_booking.start_dim_date_key) as recurrence_start_dim_date_key,
		isnull(d_exerp_booking_recurrence.recurrence_start_dim_time_key, d_exerp_booking.start_dim_time_key) as recurrence_start_dim_time_key,
		isnull(d_exerp_booking_recurrence.recurrence_end_dim_date_key, -999) as recurrence_end_dim_date_key,
		case 
			when d_exerp_booking_recurrence.recurrence_type is null
				then -999
			else d_exerp_booking.stop_dim_time_key
		end as recurrence_end_dim_time_key,
		d_exerp_booking.d_exerp_center_bk_hash as dim_club_key,
		d_exerp_booking.d_exerp_activity_bk_hash as dim_exerp_activity_key,
		d_exerp_booking.booking_name as booking_name,
        d_exerp_booking.color as color,
		d_exerp_booking.description as [description],
		d_exerp_booking.comment as comment,
		d_exerp_booking.class_capacity as class_capacity,
		case 
			when isnull(d_exerp_booking_recurrence.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_exerp_booking.dv_load_date_time, 'Jan 1, 1753')
				then isnull(d_exerp_booking_recurrence.dv_load_date_time, 'Jan 1, 1753')
			else isnull(d_exerp_booking.dv_load_date_time, 'Jan 1, 1753')
		end as dv_load_date_time,
		convert(datetime, '99991231', 112) as dv_load_end_date_time,
		case 
			when isnull(d_exerp_booking_recurrence.dv_batch_id, - 1) >= isnull(d_exerp_booking.dv_batch_id, - 1)
				then isnull(d_exerp_booking_recurrence.dv_batch_id, - 1)
			else isnull(d_exerp_booking.dv_batch_id, - 1)
		end as dv_batch_id
	FROM 
		d_exerp_booking
	LEFT JOIN 
		d_exerp_booking_recurrence on d_exerp_booking.main_d_exerp_booking_bk_hash = d_exerp_booking_recurrence.bk_hash
	WHERE 
		d_exerp_booking.main_d_exerp_booking_bk_hash = d_exerp_booking.bk_hash
		AND (d_exerp_booking_recurrence.dv_batch_id >= @load_dv_batch_id
			OR d_exerp_booking.dv_batch_id >= @load_dv_batch_id ) 


	/*   Delete records from the table that exist*/
	/*   Insert records from current and missing batches*/

	BEGIN TRAN

	DELETE dbo.dim_exerp_booking_recurrence
	WHERE dim_exerp_booking_recurrence_key IN (
			SELECT dim_exerp_booking_recurrence_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_exerp_booking_recurrence (
         dim_exerp_booking_recurrence_key
        ,main_booking_id
        ,recurrence_type
        ,recurrence
        ,recurrence_start_dim_date_key
        ,recurrence_start_dim_time_key
        ,recurrence_end_dim_date_key
		,recurrence_end_dim_time_key
        ,dim_club_key
		,dim_exerp_activity_key
		,booking_name
		,color
		,[description]
		,comment
		,class_capacity
        ,dv_load_date_time
        ,dv_load_end_date_time
        ,dv_batch_id
        ,dv_inserted_date_time
        ,dv_insert_user
		)
	SELECT 
         dim_exerp_booking_recurrence_key
        ,main_booking_id
        ,recurrence_type
        ,recurrence
        ,recurrence_start_dim_date_key
        ,recurrence_start_dim_time_key
        ,recurrence_end_dim_date_key
		,recurrence_end_dim_time_key
        ,dim_club_key
		,dim_exerp_activity_key
		,booking_name
		,color
		,[description]
		,comment
		,class_capacity
        ,dv_load_date_time
        ,dv_load_end_date_time
        ,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN
		
END
