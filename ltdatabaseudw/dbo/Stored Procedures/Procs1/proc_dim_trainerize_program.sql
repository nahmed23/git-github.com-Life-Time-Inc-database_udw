CREATE PROC [dbo].[proc_dim_trainerize_program] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM dim_trainerize_program
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
				distribution = HASH (dim_trainerize_program_key)
				,location = user_db
				) AS
select   d_ec_programs.dim_trainerize_program_key dim_trainerize_program_key
        ,d_ec_programs.program_id program_id
        ,d_ec_programs.created_dim_date_key created_dim_date_key
        ,case when map_ltfeb_party_id_dim_employee_key.dim_employee_key is null then '-998'
		 else map_ltfeb_party_id_dim_employee_key.dim_employee_key end  dim_employee_key
        ,case when map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key is null then '-998'
		 else map_ltfeb_party_id_dim_mms_member_key.dim_mms_member_key end dim_mms_member_key
        ,d_ec_programs.end_dim_date_key end_dim_date_key
        ,d_ec_programs.end_dim_time_key end_dim_time_key
        ,d_ec_programs.program_name program_name
        ,d_ec_programs.source_id source_id
        ,d_ec_programs.source_type source_type
        ,d_ec_programs.start_dim_date_key start_dim_date_key
        ,d_ec_programs.start_dim_time_key start_dim_time_key
        ,d_ec_programs.status status
        ,d_ec_programs.updated_dim_date_key updated_dim_date_key
	    , case 
	    	when isnull(d_ec_programs.dv_load_date_time,'Jan 1, 1753') >= isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') 
				and isnull(d_ec_programs.dv_load_date_time,'Jan 1, 1753') >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
					then d_ec_programs.dv_load_date_time
	    	when isnull(map_ltfeb_party_id_dim_employee_key.dv_load_date_time,'Jan 1, 1753') >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753')
					then map_ltfeb_party_id_dim_employee_key.dv_load_date_time
			else isnull(map_ltfeb_party_id_dim_mms_member_key.dv_load_date_time,'Jan 1, 1753') 
	      end dv_load_date_time
	    , case 
	    	when isnull(d_ec_programs.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) 
				and isnull(d_ec_programs.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
					then d_ec_programs.dv_batch_id
	    	when isnull(map_ltfeb_party_id_dim_employee_key.dv_batch_id,-1) >= isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1)
					then map_ltfeb_party_id_dim_employee_key.dv_batch_id
			else isnull(map_ltfeb_party_id_dim_mms_member_key.dv_batch_id,-1) 
	      end dv_batch_id
	    , convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	from d_ec_programs
    left join map_ltfeb_party_id_dim_employee_key 
	on d_ec_programs.coach_party_id= map_ltfeb_party_id_dim_employee_key.party_id
	left join map_ltfeb_party_id_dim_mms_member_key
	on d_ec_programs.party_id = map_ltfeb_party_id_dim_mms_member_key.party_id
    where (( d_ec_programs.dv_batch_id >= @load_dv_batch_id
        or map_ltfeb_party_id_dim_employee_key.dv_batch_id >= @load_dv_batch_id
		or map_ltfeb_party_id_dim_mms_member_key.dv_batch_id >= @load_dv_batch_id))


	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_trainerize_program
	WHERE dim_trainerize_program_key IN (
			SELECT dim_trainerize_program_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_trainerize_program(
         dim_trainerize_program_key
        ,program_id
        ,created_dim_date_key
        ,dim_employee_key
        ,dim_mms_member_key
        ,end_dim_date_key
        ,end_dim_time_key
        ,program_name
        ,source_id
        ,source_type
        ,start_dim_date_key
        ,start_dim_time_key
        ,status
        ,updated_dim_date_key
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         dim_trainerize_program_key
        ,program_id
        ,created_dim_date_key
        ,dim_employee_key
        ,dim_mms_member_key
        ,end_dim_date_key
        ,end_dim_time_key
        ,program_name
        ,source_id
        ,source_type
        ,start_dim_date_key
        ,start_dim_time_key
        ,status
        ,updated_dim_date_key
	    ,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN

			
END
