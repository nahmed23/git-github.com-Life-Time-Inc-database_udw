CREATE PROC [dbo].[proc_fact_exerp_sale_employee_log] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM fact_exerp_sale_employee_log
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
				distribution = HASH (fact_exerp_sale_employee_log_key)
				,location = user_db
				) AS
    select a.* from
      (select d_exerp_sale_employee_log.fact_exerp_sale_employee_log_key,
       d_exerp_sale_employee_log.sale_employee_log_id,
	   d_exerp_sale_employee_log.center_id,
	   d_exerp_sale_employee_log.sale_id,
	   d_exerp_sale_employee_log.change_dim_employee_key,
	   d_exerp_sale_employee_log.change_person_id,
	   d_exerp_sale_employee_log.dim_club_key,
       d_exerp_sale_employee_log.from_dim_date_key,
	   d_exerp_sale_employee_log.from_dim_time_key,
	   d_exerp_sale_employee_log.sale_dim_employee_key,
	   d_exerp_sale_employee_log.sale_person_id,
	   d_exerp_sale_employee_log.sale_fact_exerp_transaction_log_key,
	   d_exerp_sale_employee_log.dv_load_date_time,
       d_exerp_sale_employee_log.dv_batch_id,
       row_number() over (partition by d_exerp_sale_employee_log.sale_fact_exerp_transaction_log_key,
	   d_exerp_sale_employee_log.sale_dim_employee_key,
	   d_exerp_sale_employee_log.change_dim_employee_key,
	   d_exerp_sale_employee_log.dim_club_key
	   order by d_exerp_sale_employee_log.from_dim_date_key desc ) r
	   from d_exerp_sale_employee_log) a
    where r = 1

	if object_id('tempdb.dbo.#etl_step2') is not null drop table #etl_step2
    create table dbo.#etl_step2 with(distribution = hash(sale_fact_exerp_transaction_log_key), location=user_db) as
	select b.* from
      (select d_exerp_sale_log.sale_fact_exerp_transaction_log_key sale_fact_exerp_transaction_log_key,
       d_exerp_sale_log.sale_id sale_id,
	   d_exerp_sale_log.dv_load_date_time dv_load_date_time,
	   d_exerp_sale_log.dv_batch_id dv_batch_id,
	   row_number() over (partition by d_exerp_sale_log.sale_fact_exerp_transaction_log_key
	   order by d_exerp_sale_log.dv_load_date_time desc ) r
	   from d_exerp_sale_log)b
	   where r = 1

	if object_id('tempdb.dbo.#etl_step3') is not null drop table #etl_step3
    create table dbo.#etl_step3 with(distribution = hash(fact_exerp_sale_employee_log_key), location=user_db) as
    select #etl_step1.fact_exerp_sale_employee_log_key fact_exerp_sale_employee_log_key,
       #etl_step1.sale_employee_log_id sale_employee_log_id,
	   #etl_step1.center_id center_id,
	   #etl_step1.change_dim_employee_key change_dim_employee_key,
	   #etl_step1.change_person_id change_person_id,
	   #etl_step1.dim_club_key dim_club_key,
       #etl_step1.from_dim_date_key from_dim_date_key,
	   #etl_step1.from_dim_time_key from_dim_time_key,
	   #etl_step1.sale_dim_employee_key sale_dim_employee_key,
	   #etl_step1.sale_person_id sale_person_id,
	   #etl_step2.sale_fact_exerp_transaction_log_key sale_fact_exerp_transaction_log_key,
       #etl_step2.sale_id sale_id,
       case
	    	when #etl_step1.dv_load_date_time >= isnull(#etl_step2.dv_load_date_time,'Jan 1, 1753')
	    		then #etl_step1.dv_load_date_time
	    	else isnull(#etl_step2.dv_load_date_time,'Jan 1, 1753')
	    end dv_load_date_time,
	    case
	    	when #etl_step1.dv_batch_id >= isnull(#etl_step2.dv_batch_id,-1)
	    		then #etl_step1.dv_batch_id
            else isnull(#etl_step2.dv_batch_id,-1)
	    end dv_batch_id,
	    convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
    from #etl_step1
    left join #etl_step2
    on #etl_step1.sale_fact_exerp_transaction_log_key = #etl_step2.sale_fact_exerp_transaction_log_key

	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.fact_exerp_sale_employee_log
	WHERE fact_exerp_sale_employee_log_key IN (
			SELECT fact_exerp_sale_employee_log_key
			FROM dbo.#etl_step3
			)

	INSERT INTO fact_exerp_sale_employee_log(
         fact_exerp_sale_employee_log_key
		, sale_employee_log_id
        , center_id
        , change_dim_employee_key
        , change_person_id
        , dim_club_key
        , from_dim_date_key
        , from_dim_time_key
        , sale_dim_employee_key
        , sale_person_id
        , sale_fact_exerp_transaction_log_key
		, sale_id
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         fact_exerp_sale_employee_log_key
		, sale_employee_log_id
        , center_id
        , change_dim_employee_key
        , change_person_id
        , dim_club_key
        , from_dim_date_key
        , from_dim_time_key
        , sale_dim_employee_key
        , sale_person_id
        , sale_fact_exerp_transaction_log_key
		, sale_id
		, dv_load_date_time
		, dv_batch_id
		, dv_load_end_date_time
		,getdate()
		,suser_sname()
	FROM #etl_step3

	COMMIT TRAN


END
