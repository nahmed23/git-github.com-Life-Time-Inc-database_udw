CREATE PROC [dbo].[proc_dim_exerp_activity] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON


IF object_id('tempdb..#etl_step1') IS NOT NULL
		DROP TABLE #etl_step1

	CREATE TABLE dbo.#etl_step1
		WITH (
				distribution = HASH (dim_exerp_activity_key)
				,location = user_db
				) AS
	select  d_exerp_activity.bk_hash dim_exerp_activity_key,
			d_exerp_activity.activity_id activity_id,
			d_exerp_activity.activity_name activity_name,
			d_exerp_activity.activity_state activity_state,
			d_exerp_activity.activity_type activity_type,
			d_exerp_activity.color color,
			d_exerp_activity.description as [description],
			d_exerp_activity.dim_boss_product_key dim_boss_product_key,
			d_exerp_activity.external_id external_id,
			d_exerp_activity.max_participants max_participants,
			d_exerp_activity.max_waiting_list_participants max_waiting_list_participants,
			d_exerp_activity.access_group_id access_group_id,
			d_exerp_activity.time_configuration_id time_configuration_id,
			d_exerp_activity_group.activity_group_name activity_group_name,
			d_exerp_activity_group.activity_group_state activity_group_state,
			d_exerp_activity_group.book_api_flag book_api_flag,
			d_exerp_activity_group.book_client_flag book_client_flag,
			d_exerp_activity_group.book_kiosk_flag book_kiosk_flag,
			d_exerp_activity_group.book_mobile_api_flag book_mobile_api_flag,
			d_exerp_activity_group.book_web_flag book_web_flag,
			d_exerp_time_configuration.bk_hash dim_exerp_time_configuration_key,
			parent_activity_group.activity_group_name as department,
			d_exerp_activity_group.external_id as sku,
			d_exerp_activity.course_schedule_type,
			d_exerp_activity.d_exerp_age_group_bk_hash as dim_exerp_age_group_key,
			d_exerp_activity_group.dim_exerp_activity_group_key dim_exerp_activity_group_key,
			dim_boss_product.dim_mms_product_key dim_mms_product_key,
		   CASE
				WHEN isnull(d_exerp_activity.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_exerp_activity_group.dv_load_date_time, 'Jan 1, 1753')
				and isnull(d_exerp_activity.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_exerp_time_configuration.dv_load_date_time, 'Jan 1, 1753')
				and isnull(d_exerp_activity.dv_load_date_time, 'Jan 1, 1753') >= isnull(dim_boss_product.dv_load_date_time, 'Jan 1, 1753')
					THEN isnull(d_exerp_activity.dv_load_date_time, 'Jan 1, 1753')
				WHEN isnull(d_exerp_activity_group.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_exerp_time_configuration.dv_load_date_time, 'Jan 1, 1753')
				and isnull(d_exerp_activity_group.dv_load_date_time, 'Jan 1, 1753') >= isnull(dim_boss_product.dv_load_date_time, 'Jan 1, 1753')
					THEN isnull(d_exerp_activity_group.dv_load_date_time, 'Jan 1, 1753')
				WHEN isnull(d_exerp_time_configuration.dv_load_date_time, 'Jan 1, 1753') >= isnull(dim_boss_product.dv_load_date_time, 'Jan 1, 1753')
					THEN isnull(d_exerp_time_configuration.dv_load_date_time, 'Jan 1, 1753')
				ELSE isnull(dim_boss_product.dv_load_date_time, 'Jan 1, 1753')
			END dv_load_date_time
			,convert(DATETIME, '99991231', 112) dv_load_end_date_time
			,CASE
				WHEN isnull(d_exerp_activity.dv_batch_id, - 1) >= isnull(d_exerp_activity_group.dv_batch_id, - 1)
				and isnull(d_exerp_activity.dv_batch_id, - 1) >= isnull(d_exerp_time_configuration.dv_batch_id, - 1)
				and isnull(d_exerp_activity.dv_batch_id, - 1) >= isnull(dim_boss_product.dv_batch_id, - 1)
					THEN isnull(d_exerp_activity.dv_batch_id, - 1)
				WHEN isnull(d_exerp_activity_group.dv_batch_id, - 1) >= isnull(d_exerp_time_configuration.dv_batch_id, - 1)
				and isnull(d_exerp_activity_group.dv_batch_id, - 1) >= isnull(dim_boss_product.dv_batch_id, - 1)
					THEN isnull(d_exerp_activity_group.dv_batch_id, - 1)
				WHEN isnull(d_exerp_time_configuration.dv_batch_id, - 1) >= isnull(dim_boss_product.dv_batch_id, - 1)
					THEN isnull(d_exerp_time_configuration.dv_batch_id, - 1)
				ELSE isnull(dim_boss_product.dv_batch_id, - 1)
			END dv_batch_id
	from
		[dbo].[d_exerp_activity]
	left join [dbo].[d_exerp_activity_group] d_exerp_activity_group
		on [d_exerp_activity].d_exerp_activity_group_bk_hash = [d_exerp_activity_group].bk_hash
	left join [dbo].[d_exerp_activity_group] parent_activity_group
		on parent_activity_group.bk_hash = [d_exerp_activity_group].parent_d_exerp_activity_group_bk_hash
	left join [dbo].[d_exerp_time_configuration]
		on [d_exerp_activity].dim_exerp_time_configuration_key = [d_exerp_time_configuration].bk_hash
	left join [dbo].[dim_boss_product]
	on [d_exerp_activity].dim_boss_product_key =[dim_boss_product].dim_boss_product_key
		/*where d_exerp_activity.activity_state ='ACTIVE'*/

	/*Delete records from the table that exist*/
	/*Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_exerp_activity
	WHERE dim_exerp_activity_key IN (
			SELECT dim_exerp_activity_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_exerp_activity (
         dim_exerp_activity_key
	    ,activity_id
        ,activity_name
        ,activity_state
        ,activity_type
        ,color
		,description
		,dim_boss_product_key
		,external_id
		,max_participants
		,max_waiting_list_participants
		,access_group_id
		,time_configuration_id
		,activity_group_name
		,activity_group_state
		,book_api_flag
		,book_client_flag
		,book_kiosk_flag
		,book_mobile_api_flag
		,book_web_flag
		,dim_exerp_time_configuration_key
		,department
		,sku
		,course_schedule_type
		,dim_exerp_age_group_key
		,dim_exerp_activity_group_key
		,dim_mms_product_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT  dim_exerp_activity_key
        ,activity_id
		,activity_name
	    ,activity_state
	    ,activity_type
	    ,color
		,description
	    ,dim_boss_product_key
		,external_id
		,max_participants
		,max_waiting_list_participants
		,access_group_id
		,time_configuration_id
		,activity_group_name
		,activity_group_state
		,book_api_flag
		,book_client_flag
		,book_kiosk_flag
		,book_mobile_api_flag
		,book_web_flag
		,dim_exerp_time_configuration_key
		,department
		,sku
		,course_schedule_type
		,dim_exerp_age_group_key
		,dim_exerp_activity_group_key
		,dim_mms_product_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN

END

