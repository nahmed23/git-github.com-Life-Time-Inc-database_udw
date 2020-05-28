CREATE PROC [dbo].[proc_fact_trainerize_notification] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM fact_trainerize_notification
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
				distribution = HASH (fact_trainerize_notification_key)
				,location = user_db
				) AS
select   d_ec_notifications.fact_trainerize_notification_key	fact_trainerize_notification_key
        ,d_ec_notifications.notification_id				notification_id
        ,d_ec_notifications.created_dim_date_key		created_dim_date_key
        ,d_ec_notifications.created_dim_time_key		created_dim_time_key
        ,case
			when map_ltfeb_party_id_dim_employee_key.dim_employee_key is null
				then '-998'
			else
				map_ltfeb_party_id_dim_employee_key.dim_employee_key end  from_dim_employee_key
        ,d_ec_notifications.message    					message
        ,d_ec_notifications.message_type_flag			message_type
        ,d_ec_notifications.received_dim_date_key		received_dim_date_key
        ,d_ec_notifications.received_dim_time_key		received_dim_time_key
        ,d_ec_notifications.source_id					source_id
        ,d_ec_notifications.source_thread_id			source_thread_id
        ,d_ec_notifications.source_type					source_type
        ,d_ec_notifications.status_flag					status
        ,d_ec_notifications.subject						subject
        ,case
			when map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key is null
				then '-998'
			else
				map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key end to_dim_mms_member_key
        ,d_ec_notifications.updated_dim_date_key		updated_dim_date_key
	    ,d_ec_notifications.updated_dim_time_key		updated_dim_time_key
	    ,case
	    	when isnull(d_ec_notifications.dv_load_date_time,'Jan 1, 1753') >= isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_ec_notifications.dv_load_date_time,'Jan 1, 1753') >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
					then d_ec_notifications.dv_load_date_time
	    	when isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
					then map_ltfeb_party_id_dim_employee_key.dv_load_date_time
			else isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
	      end dv_load_date_time
	    ,case
	    	when isnull(d_ec_notifications.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
				and isnull(d_ec_notifications.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
					then d_ec_notifications.dv_batch_id
	    	when isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
					then map_ltfeb_party_id_dim_employee_key.dv_batch_id
			else isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
	      end dv_batch_id
	    ,convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	from d_ec_notifications
    left join map_ltfeb_party_id_dim_mms_member_key
		on d_ec_notifications.notifications_to = map_ltfeb_party_id_dim_mms_member_key.party_id
    left join map_ltfeb_party_id_dim_employee_key
		on d_ec_notifications.notifications_from = map_ltfeb_party_id_dim_employee_key.party_id
    where (( d_ec_notifications.dv_batch_id >= @load_dv_batch_id
        or map_ltfeb_party_id_dim_employee_key.dv_batch_id >= @load_dv_batch_id
		or map_ltfeb_party_id_dim_mms_member_key.dv_batch_id >= @load_dv_batch_id))

	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.fact_trainerize_notification
	WHERE fact_trainerize_notification_key IN (
			SELECT fact_trainerize_notification_key
			FROM dbo.#etl_step1
			)

	INSERT INTO fact_trainerize_notification(
	      fact_trainerize_notification_key
		, notification_id
		, created_dim_date_key
		, created_dim_time_key
		, from_dim_employee_key
		, message
		, message_type
		, received_dim_date_key
		, received_dim_time_key
		, source_id
		, source_thread_id
		, source_type
		, status
		, subject
		, to_dim_mms_member_key
		, updated_dim_date_key
		, updated_dim_time_key
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, dv_inserted_date_time
		, dv_insert_user
		)
	SELECT
	      fact_trainerize_notification_key
		, notification_id
		, created_dim_date_key
		, created_dim_time_key
		, from_dim_employee_key
		, message
		, message_type
		, received_dim_date_key
		, received_dim_time_key
		, source_id
		, source_thread_id
		, source_type
		, status
		, subject
		, to_dim_mms_member_key
		, updated_dim_date_key
		, updated_dim_time_key
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, getdate()
		, suser_sname()
	FROM #etl_step1

	COMMIT TRAN


END
