CREATE PROC [dbo].[proc_dim_trainerize_measure] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM dim_trainerize_measure
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
				distribution = HASH (dim_trainerize_measure_key)
				,location = user_db
				) AS
select   d_ec_measures.dim_trainerize_measure_key dim_trainerize_measure_key
        ,d_ec_measures.measures_id measures_id
        ,case when d_ec_measures.bk_hash in ('-997','-998','-999') then d_ec_measures.bk_hash
              when (d_ec_measures.created_by is null or d_ec_measures.created_by = -1) then '-998'
              when created_by_map_ltfeb_party_id_dim_employee_key.dim_employee_key is null then '-999'
                   else created_by_map_ltfeb_party_id_dim_employee_key.dim_employee_key
         end  created_by_dim_employee_key
        ,d_ec_measures.created_dim_date_key created_dim_date_key
        ,d_ec_measures.description description
        ,d_ec_measures.diagonostic_range_female diagonostic_range_female
        ,d_ec_measures.diagonostic_range_male diagonostic_range_male
        ,d_ec_measures.extended_metadata extended_metadata
        ,d_ec_measures.gender gender
        ,d_ec_measures.measure_value_type measure_value_type
        ,d_ec_measures.measurement_instructions_location measurement_instructions_location
        ,d_ec_measures.measurement_type  measurement_type
        ,case when d_ec_measures.bk_hash in ('-997','-998','-999') then d_ec_measures.bk_hash
              when (d_ec_measures.modified_by is null or d_ec_measures.modified_by = -1) then '-998'
              when modified_by_map_ltfeb_party_id_dim_employee_key.dim_employee_key is null then '-999'
		 else modified_by_map_ltfeb_party_id_dim_employee_key.dim_employee_key end  modified_by_dim_employee_key
        ,d_ec_measures.modified_dim_date_key  modified_dim_date_key
        ,d_ec_measures.optimum_range_female  optimum_range_female
        ,d_ec_measures.optimum_range_male optimum_range_male
        ,d_ec_measures.slug  slug
        ,d_ec_measures.tags  tags
        ,d_ec_measures.title title
		,d_ec_measures.unit unit
	    , case 
	    	when isnull(d_ec_measures.dv_load_date_time,'Jan 1, 1753') >= isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') 
			and isnull(d_ec_measures.dv_load_date_time,'Jan 1, 1753') >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753')
					then d_ec_measures.dv_load_date_time 
	    	when isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') >= 
			isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') 
					then created_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time
			else isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') 
	      end dv_load_date_time
	    , case 
	    	when isnull(d_ec_measures.dv_batch_id,-1) >= isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) 
			and  isnull(d_ec_measures.dv_batch_id,-1) >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1)
					then d_ec_measures.dv_batch_id
		    when isnull(created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) >= isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) 
					then created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id
			else isnull(modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) 
	      end dv_batch_id
	    , convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	   from d_ec_measures
	   left join map_ltfeb_party_id_dim_employee_key  as created_by_map_ltfeb_party_id_dim_employee_key
	   on d_ec_measures.created_by= created_by_map_ltfeb_party_id_dim_employee_key.party_id
	   left join map_ltfeb_party_id_dim_employee_key  as modified_by_map_ltfeb_party_id_dim_employee_key
	   on d_ec_measures.modified_by= modified_by_map_ltfeb_party_id_dim_employee_key.party_id
    where ( d_ec_measures.dv_batch_id >= @load_dv_batch_id
        or created_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id >= @load_dv_batch_id
		or modified_by_map_ltfeb_party_id_dim_employee_key.dv_batch_id >= @load_dv_batch_id) 


	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_trainerize_measure
	WHERE dim_trainerize_measure_key IN (
			SELECT dim_trainerize_measure_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_trainerize_measure(
         dim_trainerize_measure_key
        ,measures_id
        ,created_by_dim_employee_key
        ,created_dim_date_key
        ,description
        ,diagonostic_range_female
        ,diagonostic_range_male
        ,extended_metadata
        ,gender
        ,measure_value_type
        ,measurement_instructions_location
        ,measurement_type
        ,modified_by_dim_employee_key
        ,modified_dim_date_key
        ,optimum_range_female
        ,optimum_range_male
        ,slug
        ,tags
        ,title
        ,unit
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         dim_trainerize_measure_key
        ,measures_id
        ,created_by_dim_employee_key
        ,created_dim_date_key
        ,description
        ,diagonostic_range_female
        ,diagonostic_range_male
        ,extended_metadata
        ,gender
        ,measure_value_type
        ,measurement_instructions_location
        ,measurement_type
        ,modified_by_dim_employee_key
        ,modified_dim_date_key
        ,optimum_range_female
        ,optimum_range_male
        ,slug
        ,tags
        ,title
        ,unit
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN


			
END
