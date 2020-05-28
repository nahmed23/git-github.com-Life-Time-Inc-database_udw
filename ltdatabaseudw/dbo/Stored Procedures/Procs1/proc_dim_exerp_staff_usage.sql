CREATE PROC [dbo].[proc_dim_exerp_staff_usage] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM dim_exerp_staff_usage
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
				distribution = HASH (dim_exerp_staff_usage_key)
				,location = user_db
				) AS
select d_exerp_staff_usage.bk_hash dim_exerp_staff_usage_key,
       d_exerp_staff_usage.d_exerp_booking_bk_hash dim_exerp_booking_key,
	   d_exerp_staff_usage.dim_employee_key,
	   d_exerp_staff_usage.d_exerp_center_bk_hash dim_club_key,
	   d_exerp_staff_usage.booking_id,
	   d_exerp_staff_usage.start_dim_date_key,
	   d_exerp_staff_usage.start_dim_time_key,
	   d_exerp_staff_usage.stop_dim_date_key,
	   d_exerp_staff_usage.stop_dim_time_key,
	   d_exerp_staff_usage.staff_usage_state,
	   d_exerp_staff_usage.staff_usage_salary,
	   d_exerp_staff_usage.substitute_of_dim_employee_key,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id
	   from d_exerp_staff_usage
	   Where d_exerp_staff_usage.dv_batch_id >= @load_dv_batch_id


	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_exerp_staff_usage
	WHERE dim_exerp_staff_usage_key IN (
			SELECT dim_exerp_staff_usage_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_exerp_staff_usage (
         dim_exerp_staff_usage_key
		,dim_exerp_booking_key
		,dim_employee_key
		,dim_club_key
		,booking_id
		,start_dim_date_key
		,start_dim_time_key
		,stop_dim_date_key
		,stop_dim_time_key
		,staff_usage_state
		,staff_usage_salary
		,substitute_of_dim_employee_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         dim_exerp_staff_usage_key
		,dim_exerp_booking_key
		,dim_employee_key
		,dim_club_key
		,booking_id
		,start_dim_date_key
		,start_dim_time_key
		,stop_dim_date_key
		,stop_dim_time_key
		,staff_usage_state
		,staff_usage_salary
		,substitute_of_dim_employee_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN


END
