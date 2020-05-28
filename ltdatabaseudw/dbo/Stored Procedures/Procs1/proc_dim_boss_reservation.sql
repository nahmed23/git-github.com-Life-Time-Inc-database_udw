CREATE PROC [dbo].[proc_dim_boss_reservation] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (SELECT max(isnull(dv_batch_id, - 1)) FROM dim_boss_reservation)
	DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
	DECLARE @load_dv_batch_id BIGINT = CASE 
			WHEN @max_dv_batch_id < @current_dv_batch_id THEN @max_dv_batch_id
			ELSE @current_dv_batch_id END

	IF object_id('tempdb..#etl_step1') IS NOT NULL DROP TABLE #etl_step1

	CREATE TABLE dbo.#etl_step1 WITH (distribution = HASH (dim_boss_reservation_key),location = user_db) 
     AS
	SELECT dim_boss_reservation_key
		,reservation_id
		,age_high
		,age_low                                             
		,allow_wait_list
		,billing_count
		,capacity
		,comment
		,continuous
		,d_boss_asi_reserv.created_dim_date_key
		,day_of_week_sunday_flag
		,day_of_week_monday_flag
		,day_of_week_tuesday_flag
		,day_of_week_wednesday_flag
		,day_of_week_thursday_flag
		,day_of_week_friday_flag
		,day_of_week_saturday_flag
		,dim_boss_product_key
		,dim_club_key
		,dim_employee_key
		,dim_mms_product_key
		,d_boss_asi_reserv.dv_deleted_flag
		,end_dim_date_key
		,end_dim_time_key
		,free_dim_date_key
		,format_id
		,grace_days
		,instructor_expense
		,inactive_start_dim_date_key
		,inactive_end_dim_date_key
		,length_in_minutes
		,link_to_dim_boss_reservation_key
		,limit
		,limit_minimum
		,modified_dim_date_key
		,non_member_price
		,print_description
		,program_id
		,publish_flag
		,reservation_status
		,reservation_type
		,resource
		,d_boss_asi_resource.resource_type
		,start_dim_date_key
		,start_dim_time_key
		,use_for_lt_bucks_flag
		,waiver_required_flag
		,web_register_flag
		,web_enable
          ,web_start_dim_date_key
          ,web_active
		,CASE 
			WHEN isnull(d_boss_asi_reserv.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_asi_club_res.dv_load_date_time, 'Jan 1, 1753')
				AND isnull(d_boss_asi_reserv.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_asi_resource.dv_load_date_time, 'Jan 1, 1753')
				THEN isnull(d_boss_asi_reserv.dv_load_date_time, 'Jan 1, 1753')
			WHEN isnull(d_boss_asi_club_res.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_boss_asi_resource.dv_load_date_time, 'Jan 1, 1753')
				THEN isnull(d_boss_asi_club_res.dv_load_date_time, 'Jan 1, 1753')
			ELSE isnull(d_boss_asi_resource.dv_load_date_time, 'Jan 1, 1753')
			END dv_load_date_time
		,convert(DATETIME, '99991231', 112) dv_load_end_date_time
		,CASE 
			WHEN isnull(d_boss_asi_reserv.dv_batch_id, - 1) >= isnull(d_boss_asi_club_res.dv_batch_id, - 1)
				AND isnull(d_boss_asi_reserv.dv_batch_id, - 1) >= isnull(d_boss_asi_resource.dv_batch_id, - 1)
				THEN isnull(d_boss_asi_reserv.dv_batch_id, - 1)
			WHEN isnull(d_boss_asi_club_res.dv_batch_id, - 1) >= isnull(d_boss_asi_resource.dv_batch_id, - 1)
				THEN isnull(d_boss_asi_club_res.dv_batch_id, - 1)
			ELSE isnull(d_boss_asi_resource.dv_batch_id, - 1)
			END dv_batch_id
	FROM d_boss_asi_reserv
	JOIN d_boss_asi_club_res ON d_boss_asi_reserv.d_boss_asi_club_res_bk_hash = d_boss_asi_club_res.bk_hash
	JOIN d_boss_asi_resource ON d_boss_asi_club_res.d_boss_asi_resource_bk_hash = d_boss_asi_resource.bk_hash
	WHERE d_boss_asi_reserv.dv_batch_id >= @load_dv_batch_id
		OR d_boss_asi_club_res.dv_batch_id >= @load_dv_batch_id
		OR d_boss_asi_resource.dv_batch_id >= @load_dv_batch_id

	--   and d_boss_asi_reserv.dv_deleted = 0
	-- Delete and re-insert as a single transaction
	--   Delete records from the table that exist
	--   Insert records from records from current and missing batches
	BEGIN TRAN

	DELETE dbo.dim_boss_reservation
	WHERE dim_boss_reservation_key IN (
			SELECT dim_boss_reservation_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_boss_reservation (
		dim_boss_reservation_key
		,reservation_id
		,age_high
		,age_low
		,allow_wait_list
		,billing_count
		,capacity
		,comment
		,continuous
		,created_dim_date_key
		,day_of_week_sunday_flag
		,day_of_week_monday_flag
		,day_of_week_tuesday_flag
		,day_of_week_wednesday_flag
		,day_of_week_thursday_flag
		,day_of_week_friday_flag
		,day_of_week_saturday_flag
		,dim_boss_product_key
		,dim_club_key
		,dim_employee_key
		,dim_mms_product_key
		,dv_deleted_flag
		,end_dim_date_key
		,end_dim_time_key
		,free_dim_date_key
		,format_id
		,grace_days
		,instructor_expense
		,inactive_start_dim_date_key
		,inactive_end_dim_date_key
		,length_in_minutes
		,link_to_dim_boss_reservation_key
		,limit
		,limit_minimum
		,modified_dim_date_key
		,non_member_price
		,print_description
		,program_id
		,publish_flag
		,reservation_status
		,reservation_type
		,resource
		,resource_type
		,start_dim_date_key
		,start_dim_time_key
		,use_for_lt_bucks_flag
		,waiver_required_flag
		,web_register_flag
		,web_enable
          ,web_start_dim_date_key
          ,web_active
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT dim_boss_reservation_key
		,reservation_id
		,age_high
		,age_low
		,allow_wait_list
		,billing_count
		,capacity
		,comment
		,continuous
		,created_dim_date_key
		,day_of_week_sunday_flag
		,day_of_week_monday_flag
		,day_of_week_tuesday_flag
		,day_of_week_wednesday_flag
		,day_of_week_thursday_flag
		,day_of_week_friday_flag
		,day_of_week_saturday_flag
		,dim_boss_product_key
		,dim_club_key
		,dim_employee_key
		,dim_mms_product_key
		,dv_deleted_flag
		,end_dim_date_key
		,end_dim_time_key
		,free_dim_date_key
		,format_id
		,grace_days
		,instructor_expense
		,inactive_start_dim_date_key
		,inactive_end_dim_date_key
		,length_in_minutes
		,link_to_dim_boss_reservation_key
		,limit
		,limit_minimum
		,modified_dim_date_key
		,non_member_price
          ,print_description
		,program_id
		,publish_flag
		,reservation_status
		,reservation_type
		,resource
		,resource_type
		,start_dim_date_key
		,start_dim_time_key
		,use_for_lt_bucks_flag
		,waiver_required_flag
		,web_register_flag
		,web_enable
          ,web_start_dim_date_key
          ,web_active
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN

	---- delete any records in dim_boss_reservation where the reservation is in the current or later batch of boss_audit_reserve and is flagged as a "DELETE"
	--DELETE dim_boss_reservation
	--WHERE dim_boss_reservation_key IN (
	--		SELECT d_boss_audit_reserve.dim_boss_reservation_key
	--		FROM d_boss_audit_reserve
	--		WHERE d_boss_audit_reserve.dv_batch_id >= @load_dv_batch_id
	--			AND d_boss_audit_reserve.audit_type = 'DELETE'
	--		)
END

