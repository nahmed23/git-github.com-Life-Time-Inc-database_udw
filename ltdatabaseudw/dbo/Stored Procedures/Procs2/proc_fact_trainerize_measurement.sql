CREATE PROC [dbo].[proc_fact_trainerize_measurement] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM fact_trainerize_measurement
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
				distribution = HASH (fact_trainerize_measurement_key)
				,location = user_db
				) AS
select   d_ec_measurements.fact_trainerize_measurement_key fact_trainerize_measurement_key
        ,d_ec_measurements.measurement_id measurement_id
        ,d_ec_measurements.dim_trainerize_measure_key dim_trainerize_measure_key
        ,d_ec_measurements.measure_value measure_value
        ,d_ec_measurements.unit unit
        ,d_ec_measurement_recordings.measurement_recording_id measurement_recording_id
        ,d_ec_measurement_recordings.active_flag active_flag
        ,d_ec_measurement_recordings.certified_flag certified_flag
        ,d_ec_measurement_recordings.dim_club_key dim_club_key
        ,case when map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key is null then '-998'
		 else map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key end dim_mms_member_key
        ,case when d_ec_measurement_recordings.bk_hash in ('-997','-998','-999') then d_ec_measurement_recordings.bk_hash
         when (d_ec_measurement_recordings.created_by is null or d_ec_measurement_recordings.created_by = -1) then '-998'
         when created_by_map_ltfeb_party_id_dim_employee_key.dim_employee_key is null then '-999'
         else created_by_map_ltfeb_party_id_dim_employee_key.dim_employee_key end  created_by_dim_employee_key
        ,d_ec_measurement_recordings.created_dim_date_key created_dim_date_key
        ,d_ec_measurement_recordings.measurement_dim_date_key  measurement_dim_date_key
        ,d_ec_measurement_recordings.measurement_dim_time_key  measurement_dim_time_key
        ,d_ec_measurement_recordings.metadata  metadata
        ,case when d_ec_measurement_recordings.bk_hash in ('-997','-998','-999') then d_ec_measurement_recordings.bk_hash
         when (d_ec_measurement_recordings.modified_by is null or d_ec_measurement_recordings.modified_by = -1) then '-998'
         when modified_by_map_ltfeb_party_id_dim_employee_key.dim_employee_key is null then '-999'
		 else modified_by_map_ltfeb_party_id_dim_employee_key.dim_employee_key end  modified_by_dim_employee_key
        ,d_ec_measurement_recordings.modified_dim_date_key  modified_dim_date_key
        ,d_ec_measurement_recordings.notes  notes
        ,d_ec_measurement_recordings.source  source
        ,d_ec_measurement_recordings.user_program_status_id user_program_status_id
	    , case 
	    	when isnull(d_ec_measurement_recordings.dv_load_date_time,'Jan 1, 1753')  >= isnull(d_ec_measurements.dv_load_date_time,'Jan 1, 1753') 
				and isnull(d_ec_measurement_recordings.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_ec_measurement_recordings.dv_load_date_time,'Jan 1, 1753')  >= isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_ec_measurement_recordings.dv_load_date_time,'Jan 1, 1753')  >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then d_ec_measurement_recordings.dv_load_date_time
	    	when isnull(d_ec_measurements.dv_load_date_time,'Jan 1, 1753')  >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
			and isnull(d_ec_measurements.dv_load_date_time,'Jan 1, 1753')  >= isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
			and isnull(d_ec_measurements.dv_load_date_time,'Jan 1, 1753')  >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then d_ec_measurements.dv_load_date_time
		    when isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')  >= isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
			and isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')  >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time
			when isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')  >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then created_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time
			else isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') 
	      end dv_load_date_time
		  
	    , case 
	    	when isnull(d_ec_measurement_recordings.dv_batch_id,-1) >= isnull(d_ec_measurements.dv_batch_id,-1) 
				and isnull(d_ec_measurement_recordings.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
				and isnull(d_ec_measurement_recordings.dv_batch_id,-1) >= isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
				and isnull(d_ec_measurement_recordings.dv_batch_id,-1) >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
					then d_ec_measurement_recordings.dv_batch_id
	    	when isnull(d_ec_measurements.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
			    and isnull(d_ec_measurements.dv_batch_id,-1) >= isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
			    and isnull(d_ec_measurements.dv_batch_id,-1) >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
					then d_ec_measurements.dv_batch_id
			when isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1) >= isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
			and isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1) >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
				then map_ltfeb_party_id_dim_mms_member_key.dv_batch_id
				
			when isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
				then created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id
			    else isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) 
	      end dv_batch_id
	    , convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	from d_ec_measurements 
    join d_ec_measurement_recordings
	on d_ec_measurement_recordings.bk_hash= d_ec_measurements.d_ec_measurement_recordings_bk_hash
	left join map_ltfeb_party_id_dim_mms_member_key
	on d_ec_measurement_recordings.party_id = map_ltfeb_party_id_dim_mms_member_key.party_id
	left join map_ltfeb_party_id_dim_employee_key  as created_by_map_ltfeb_party_id_dim_employee_key
	on d_ec_measurement_recordings.created_by= created_by_map_ltfeb_party_id_dim_employee_key.party_id
	left join map_ltfeb_party_id_dim_employee_key  as modified_by_map_ltfeb_party_id_dim_employee_key
	on d_ec_measurement_recordings.modified_by= modified_by_map_ltfeb_party_id_dim_employee_key.party_id
    where ( d_ec_measurement_recordings.dv_batch_id >= @load_dv_batch_id
        or d_ec_measurements.dv_batch_id >= @load_dv_batch_id
		or map_ltfeb_party_id_dim_mms_member_key.dv_batch_id >= @load_dv_batch_id
		or modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id >= @load_dv_batch_id
		or created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id >= @load_dv_batch_id) 


	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.fact_trainerize_measurement
	WHERE fact_trainerize_measurement_key IN (
			SELECT fact_trainerize_measurement_key
			FROM dbo.#etl_step1
			)

	INSERT INTO fact_trainerize_measurement(
         fact_trainerize_measurement_key
        ,measurement_id
        ,dim_trainerize_measure_key
        ,measure_value
        ,unit
        ,measurement_recording_id
        ,active_flag
        ,certified_flag
        ,dim_club_key
        ,dim_mms_member_key
        ,created_by_dim_employee_key
        ,created_dim_date_key
        ,measurement_dim_date_key
        ,measurement_dim_time_key
        ,metadata
        ,modified_by_dim_employee_key
        ,modified_dim_date_key
        ,notes
        ,source
        ,user_program_status_id
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         fact_trainerize_measurement_key
        ,measurement_id
        ,dim_trainerize_measure_key
        ,measure_value
        ,unit
        ,measurement_recording_id
        ,active_flag
        ,certified_flag
        ,dim_club_key
        ,dim_mms_member_key
        ,created_by_dim_employee_key
        ,created_dim_date_key
        ,measurement_dim_date_key
        ,measurement_dim_time_key
        ,metadata
        ,modified_by_dim_employee_key
        ,modified_dim_date_key
        ,notes
        ,source
        ,user_program_status_id
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN

			
END
