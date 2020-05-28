CREATE PROC [dbo].[proc_dim_exerp_clipcard] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM dim_exerp_clipcard
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
				distribution = HASH (dim_exerp_clipcard_key)
				,location = user_db
				) AS
	select d_exerp_clipcard.bk_hash dim_exerp_clipcard_key,
       d_exerp_clipcard.clipcard_id clipcard_id,
	   d_exerp_clipcard.assigned_dim_employee_key assigned_dim_employee_key,
	   d_exerp_clipcard.blocked_flag blocked_flag,
	   d_exerp_clipcard.cancel_dim_date_key cancel_dim_date_key,
	   d_exerp_clipcard.cancel_dim_time_key cancel_dim_time_key,
       d_exerp_clipcard.cancelled_flag cancelled_flag,
	   d_exerp_clipcard.clipcard_clips_left clips_left,
	   d_exerp_clipcard.clipcard_clips_initial clips_initial,
	   d_exerp_clipcard.dim_club_key dim_club_key,
	   d_exerp_clipcard.dim_mms_member_key dim_mms_member_key,
	   d_exerp_clipcard.fact_exerp_transaction_log_key fact_exerp_transaction_log_key,
	   d_exerp_clipcard.valid_from_dim_date_key valid_from_dim_date_key,
	   d_exerp_clipcard.valid_from_dim_time_key valid_from_dim_time_key,   
       d_exerp_clipcard.valid_until_dim_date_key valid_until_dim_date_key,
	   d_exerp_clipcard.valid_until_dim_time_key valid_until_dim_time_key,
	   d_exerp_sale_log.sale_entered_dim_employee_key sale_entered_dim_employee_key,
       d_exerp_sale_log.dim_exerp_product_key dim_exerp_product_key,
       d_exerp_sale_log.entry_dim_date_key sale_entry_dim_date_key,
       d_exerp_sale_log.entry_dim_time_key sale_entry_dim_time_key,
       d_exerp_sale_log.source_type sale_source_type, 
	   d_exerp_clipcard.comment,
	   case 
	    	when d_exerp_clipcard.dv_load_date_time >= isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753')
	    		then d_exerp_clipcard.dv_load_date_time
	    	else isnull(d_exerp_sale_log.dv_load_date_time,'Jan 1, 1753') 
	    end dv_load_date_time,
	    case 
	    	when d_exerp_clipcard.dv_batch_id >= isnull(d_exerp_sale_log.dv_batch_id,-1)
	    		then d_exerp_clipcard.dv_batch_id
            else isnull(d_exerp_sale_log.dv_batch_id,-1) 
	    end dv_batch_id,
	    convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	   from d_exerp_clipcard
    join d_exerp_sale_log
    on d_exerp_clipcard.fact_exerp_transaction_log_key = d_exerp_sale_log.bk_hash
    where (d_exerp_clipcard.dv_batch_id >= @load_dv_batch_id
        or d_exerp_sale_log.dv_batch_id >= @load_dv_batch_id)


	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_exerp_clipcard
	WHERE dim_exerp_clipcard_key IN (
			SELECT dim_exerp_clipcard_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_exerp_clipcard(
         dim_exerp_clipcard_key
		,clipcard_id
        , assigned_dim_employee_key
        , blocked_flag
        , cancel_dim_date_key
        , cancel_dim_time_key
        , cancelled_flag
        , clips_initial
        , clips_left
        , dim_club_key
        , dim_mms_member_key
        , fact_exerp_transaction_log_key
        , valid_from_dim_date_key
        , valid_from_dim_time_key
        , valid_until_dim_date_key
        , valid_until_dim_time_key
        , sale_entered_dim_employee_key
        , dim_exerp_product_key
		, sale_entry_dim_date_key
		, sale_entry_dim_time_key
		, sale_source_type
		, comment
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         dim_exerp_clipcard_key
		,clipcard_id
        , assigned_dim_employee_key
        , blocked_flag
        , cancel_dim_date_key
        , cancel_dim_time_key
        , cancelled_flag
        , clips_initial
        , clips_left
        , dim_club_key
        , dim_mms_member_key
        , fact_exerp_transaction_log_key
        , valid_from_dim_date_key
        , valid_from_dim_time_key
        , valid_until_dim_date_key
        , valid_until_dim_time_key
        , sale_entered_dim_employee_key
        , dim_exerp_product_key
		, sale_entry_dim_date_key
		, sale_entry_dim_time_key
		, sale_source_type
		, comment
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN

			
END
