CREATE PROC [dbo].[proc_dim_location_hour] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_location_hour)
    declare @current_dv_batch_id bigint = @dv_batch_id
    declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

	TRUNCATE TABLE dim_location_hour

	IF object_id('tempdb..#etl_step1') IS NOT NULL
		DROP TABLE #etl_step1

	CREATE TABLE dbo.#etl_step1
		WITH (distribution = HASH (dim_location_hour_key),location = user_db) 
	AS
    SELECT d_loc_hour.bk_hash dim_location_hour_key,
       d_loc_hour.hour_id hour_id,
	   d_loc_hour.by_appointment_only_flag by_appointment_only_flag,
	   d_loc_hour.closed_flag closed_flag,
	   d_loc_hour.created_by created_by,
	   d_loc_hour.created_date_time created_date_time,
	   d_loc_hour.day_of_week day_of_week,
	   d_loc_hour.deleted_by deleted_by,
	   d_loc_hour.deleted_date_time deleted_date_time,
	   d_loc_hour.end_dim_time_key end_dim_time_key,   
       d_loc_hour.end_time end_time,
	   d_loc_hour.hour_24_flag hour_24_flag,
	   d_loc_hour.last_updated_by last_updated_by,
       d_loc_hour.last_updated_date_time last_updated_date_time,
       d_loc_hour.updated_dim_date_key updated_dim_date_key,
       d_loc_hour.updated_dim_time_key updated_dim_time_key,
       d_loc_hour.location_id location_id,
	   d_loc_hour.start_dim_time_key start_dim_time_key,
	   d_loc_hour.start_time start_time,
       d_loc_hour.sunrise_flag sunrise_flag,
       d_loc_hour.sunset_flag sunset_flag,
       d_loc_hour.deleted_flag deleted_flag,
	   d_loc_hour.dim_location_key,
	   d_loc_val_hour_type.val_hour_type_name val_hour_type_name,
	   d_loc_val_hour_type.display_name val_hour_type_display_name,
	   d_loc_val_hour_type_group.val_hour_type_group_name val_hour_type_group_name,
	   d_loc_val_hour_type_group.display_name val_hour_type_group_display_name, 
	   case when isnull(d_loc_hour.dv_load_date_time,'Jan 1, 1753') > = isnull(d_loc_val_hour_type.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_loc_hour.dv_load_date_time,'Jan 1, 1753') > = isnull(d_loc_val_hour_type_group.dv_load_date_time,'Jan 1, 1753') 
				then isnull(d_loc_hour.dv_load_date_time,'Jan 1, 1753')
				when isnull(d_loc_val_hour_type.dv_load_date_time,'Jan 1, 1753') >= isnull(d_loc_val_hour_type_group.dv_load_date_time,'Jan 1, 1753') 
				then isnull(d_loc_val_hour_type.dv_load_date_time,'Jan 1, 1753')
           else isnull(d_loc_val_hour_type_group.dv_load_date_time,'Jan 1, 1753')  end dv_load_date_time,
       convert(datetime, '99991231', 112) dv_load_end_date_time,
       case when isnull(d_loc_hour.dv_batch_id,'-1') > = isnull(d_loc_val_hour_type.dv_batch_id,'-1')
				and isnull(d_loc_hour.dv_batch_id,'-1') > = isnull(d_loc_val_hour_type_group.dv_batch_id,'-1') 
				then isnull(d_loc_hour.dv_batch_id,'-1')
				when isnull(d_loc_val_hour_type.dv_batch_id,'-1') >= isnull(d_loc_val_hour_type_group.dv_batch_id,'-1') 
				then isnull(d_loc_val_hour_type.dv_batch_id,'-1')
           else isnull(d_loc_val_hour_type_group.dv_batch_id,'-1')  end dv_batch_id
	   from d_loc_hour d_loc_hour
	   join d_loc_val_hour_type d_loc_val_hour_type on d_loc_hour.val_hour_type_id_bk_hash=d_loc_val_hour_type.bk_hash
	   join d_loc_val_hour_type_group d_loc_val_hour_type_group on d_loc_val_hour_type.d_loc_val_hour_type_group_bk_hash=d_loc_val_hour_type_group.bk_hash	   
      where (d_loc_hour.dv_batch_id >= @load_dv_batch_id
        or d_loc_val_hour_type.dv_batch_id >= @load_dv_batch_id
		or d_loc_val_hour_type_group.dv_batch_id >= @load_dv_batch_id)


	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_location_hour
	WHERE dim_location_hour_key IN (
			SELECT dim_location_hour_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_location_hour(
       dim_location_hour_key,
       hour_id,
	   by_appointment_only_flag,
	   closed_flag,
	   created_by,
	   created_date_time,
	   day_of_week,
	   deleted_by,
	   deleted_date_time,
	   end_dim_time_key,   
       end_time,
	   hour_24_flag,
	   last_updated_by,
       last_updated_date_time,
       updated_dim_date_key,
       updated_dim_time_key,
       start_dim_time_key,
	   start_time,
       sunrise_flag,
       sunset_flag,
       deleted_flag,
	   dim_location_key,
	   val_hour_type_name,
	   val_hour_type_display_name,
	   val_hour_type_group_name,
	   val_hour_type_group_display_name,
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id,
	   dv_inserted_date_time,
	   dv_insert_user
		)
	SELECT
       dim_location_hour_key,
       hour_id,
	   by_appointment_only_flag,
	   closed_flag,
	   created_by,
	   created_date_time,
	   day_of_week,
	   deleted_by,
	   deleted_date_time,
	   end_dim_time_key,   
       end_time,
	   hour_24_flag,
	   last_updated_by,
       last_updated_date_time,
       updated_dim_date_key,
       updated_dim_time_key,
       start_dim_time_key,
	   start_time,
       sunrise_flag,
       sunset_flag,
       deleted_flag,
	   dim_location_key,
	   val_hour_type_name,
	   val_hour_type_display_name,
	   val_hour_type_group_name,
	   val_hour_type_group_display_name, 
	   dv_load_date_time,
	   dv_load_end_date_time,
	   dv_batch_id,
	   getdate(),
	   suser_sname()
	FROM #etl_step1

	COMMIT TRAN

			
END
