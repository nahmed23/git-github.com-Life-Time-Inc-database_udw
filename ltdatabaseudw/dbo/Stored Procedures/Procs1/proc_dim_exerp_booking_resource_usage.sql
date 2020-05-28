CREATE PROC [dbo].[proc_dim_exerp_booking_resource_usage] @dv_batch_id [varchar](500) AS
 BEGIN
 	SET XACT_ABORT ON
 	SET NOCOUNT ON
 
 	DECLARE @max_dv_batch_id BIGINT = (
 			SELECT max(isnull(dv_batch_id, - 1))
 			FROM dim_exerp_booking_resource_usage
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
 				distribution = HASH (dim_exerp_booking_resource_usage_key)
 				,location = user_db
 				) AS
 select d_exerp_booking_resource_usage.bk_hash dim_exerp_booking_resource_usage_key,
        d_exerp_booking_resource_usage.d_exerp_booking_bk_hash dim_exerp_booking_key,
 	   d_exerp_booking_resource_usage.booking_id booking_id,
 	   d_exerp_booking_resource_usage.dim_club_key dim_club_key,
 	   d_exerp_resource.resource_id resource_id,
 	   d_exerp_resource.resource_name resource_name,
 	   d_exerp_resource.resource_type resource_type,
 	   d_exerp_resource.access_group_id resource_access_group_id,
 	   d_exerp_resource.resource_access_group_name resource_access_group_name,
 	   d_exerp_resource.external_id resource_external_id,
 	   d_exerp_resource.comment resource_comment,
 	   d_exerp_resource.show_calendar show_calendar_flag,
	   d_exerp_booking_resource_usage.booking_resource_usage_state,
	   d_exerp_booking_resource_usage.booking_stop_dim_date_key,
	   d_exerp_booking_resource_usage.booking_stop_dim_time_key,
	   d_exerp_booking_resource_usage.booking_start_dim_date_key,
	   d_exerp_booking_resource_usage.booking_start_dim_time_key,
 	   CASE 
 			WHEN isnull(d_exerp_booking_resource_usage.dv_load_date_time, 'Jan 1, 1753') >= isnull(d_exerp_resource.dv_load_date_time, 'Jan 1, 1753')
 			    THEN isnull(d_exerp_booking_resource_usage.dv_load_date_time, 'Jan 1, 1753')
 			 ELSE isnull(d_exerp_resource.dv_load_date_time, 'Jan 1, 1753')
 			END dv_load_date_time
 		,convert(DATETIME, '99991231', 112) dv_load_end_date_time
 		,CASE 
 			WHEN isnull(d_exerp_booking_resource_usage.dv_batch_id, - 1) >= isnull(d_exerp_resource.dv_batch_id, - 1)
 			    THEN isnull(d_exerp_booking_resource_usage.dv_batch_id, - 1)
                ELSE isnull(d_exerp_resource.dv_batch_id, - 1)
 			END dv_batch_id
   from d_exerp_booking_resource_usage
   join d_exerp_resource
     on d_exerp_booking_resource_usage.d_exerp_resource_bk_hash = d_exerp_resource.bk_hash
  where ( d_exerp_booking_resource_usage.dv_batch_id >= @load_dv_batch_id
     OR d_exerp_resource.dv_batch_id >= @load_dv_batch_id
     )
 
 
 	
 	/*   Delete records from the table that exist*/
 	/*   Insert records from records from current and missing batches*/
 	BEGIN TRAN
 
 	DELETE dbo.dim_exerp_booking_resource_usage
 	WHERE dim_exerp_booking_resource_usage_key IN (
 			SELECT dim_exerp_booking_resource_usage_key
 			FROM dbo.#etl_step1
 			)
 
 	INSERT INTO dim_exerp_booking_resource_usage (
          dim_exerp_booking_resource_usage_key
         ,dim_exerp_booking_key
         ,booking_id
         ,dim_club_key
         ,resource_id
         ,resource_name
         ,resource_type
         ,resource_access_group_id
         ,resource_access_group_name
         ,resource_external_id
         ,resource_comment
         ,show_calendar_flag
		 ,booking_resource_usage_state
		 ,booking_stop_dim_date_key
	     ,booking_stop_dim_time_key
	     ,booking_start_dim_date_key
		 ,booking_start_dim_time_key
         ,dv_load_date_time
         ,dv_load_end_date_time
         ,dv_batch_id
         ,dv_inserted_date_time
         ,dv_insert_user
 		)
 	SELECT dim_exerp_booking_resource_usage_key
         ,dim_exerp_booking_key
         ,booking_id
         ,dim_club_key
         ,resource_id
         ,resource_name
         ,resource_type
         ,resource_access_group_id
         ,resource_access_group_name
         ,resource_external_id
         ,resource_comment
         ,show_calendar_flag
		 ,booking_resource_usage_state
		 ,booking_stop_dim_date_key
	     ,booking_stop_dim_time_key
	     ,booking_start_dim_date_key
		 ,booking_start_dim_time_key
         ,dv_load_date_time
         ,dv_load_end_date_time
         ,dv_batch_id
 		,getdate()
 		,suser_sname()
 	FROM #etl_step1
 
 	COMMIT TRAN		
 END 
