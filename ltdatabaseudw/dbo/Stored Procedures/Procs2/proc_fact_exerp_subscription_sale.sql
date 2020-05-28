CREATE PROC [dbo].[proc_fact_exerp_subscription_sale] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM fact_exerp_subscription_sale
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
				distribution = HASH (fact_exerp_subscription_sale_key)
				,location = user_db
				) AS
select d_exerp_subscription_sale.bk_hash fact_exerp_subscription_sale_key,
       d_exerp_subscription_sale.subscription_sale_id subscription_sale_id
      ,d_exerp_subscription_sale.dim_exerp_subscription_key dim_exerp_subscription_key
      ,d_exerp_subscription_sale.subscription_dim_club_key  subscription_dim_club_key
      ,d_exerp_subscription_sale.dim_exerp_product_key dim_exerp_product_key
      ,d_exerp_subscription_sale.sale_dim_employee_key sale_dim_employee_key
      ,d_exerp_subscription_sale.dim_club_key dim_club_key
      ,d_exerp_subscription_sale.sale_id sale_id
      ,d_exerp_subscription_sale.jf_fact_exerp_transaction_log_key jf_fact_exerp_transaction_log_key
      ,d_exerp_subscription_sale.previous_dim_exerp_subscription_key previous_dim_exerp_subscription_key
      ,d_exerp_subscription_sale.subscription_sale_type subscription_sale_type
      ,d_exerp_subscription_sale.sale_dim_date_key sale_dim_date_key
      ,d_exerp_subscription_sale.sale_dim_time_key sale_dim_time_key
      ,d_exerp_subscription_sale.start_dim_date_key start_dim_date_key
      ,d_exerp_subscription_sale.end_dim_date_key end_dim_date_key
      ,d_exerp_subscription_sale.jf_normal_price jf_normal_price
      ,d_exerp_subscription_sale.jf_discount jf_discount
      ,d_exerp_subscription_sale.jf_price jf_price
      ,d_exerp_subscription_sale.jf_sponsored jf_sponsored
      ,d_exerp_subscription_sale.jf_member jf_member
      ,d_exerp_subscription_sale.prorata_period_normal_price prorata_period_normal_price
      ,d_exerp_subscription_sale.prorata_period_discount prorata_period_discount
      ,d_exerp_subscription_sale.prorata_period_price prorata_period_price
      ,d_exerp_subscription_sale.prorata_period_sponsored prorata_period_sponsored
      ,d_exerp_subscription_sale.prorata_period_member prorata_period_member
      ,d_exerp_subscription_sale.init_period_normal_price init_period_normal_price
      ,d_exerp_subscription_sale.init_period_discount init_period_discount
      ,d_exerp_subscription_sale.init_period_price init_period_price
      ,d_exerp_subscription_sale.init_period_sponsored init_period_sponsored
      ,d_exerp_subscription_sale.init_period_member init_period_member
      ,d_exerp_subscription_sale.admin_fee_normal_price admin_fee_normal_price
      ,d_exerp_subscription_sale.admin_fee_discount admin_fee_discount
      ,d_exerp_subscription_sale.admin_fee_price admin_fee_price
      ,d_exerp_subscription_sale.admin_fee_sponsored admin_fee_sponsored
      ,d_exerp_subscription_sale.admin_fee_member admin_fee_member
      ,d_exerp_subscription_sale.binding_days binding_days
      ,d_exerp_subscription_sale.init_contract_value init_contract_value
      ,d_exerp_subscription_sale.subscription_sale_state subscription_sale_state
	  , d_exerp_subscription.dim_mms_member_key  dim_mms_member_key,
	   case
	    	when d_exerp_subscription_sale.dv_load_date_time >= isnull(d_exerp_subscription.dv_load_date_time,'Jan 1, 1753')
	    		then d_exerp_subscription_sale.dv_load_date_time
	    	else isnull(d_exerp_subscription.dv_load_date_time,'Jan 1, 1753')
	    end dv_load_date_time,
	    case
	    	when d_exerp_subscription_sale.dv_batch_id >= isnull(d_exerp_subscription.dv_batch_id,-1)
	    		then d_exerp_subscription_sale.dv_batch_id
            else isnull(d_exerp_subscription.dv_batch_id,-1)
	    end dv_batch_id,
	    convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	     from d_exerp_subscription_sale
    join d_exerp_subscription
        on d_exerp_subscription_sale.dim_exerp_subscription_key = d_exerp_subscription.bk_hash
    where (d_exerp_subscription_sale.dv_batch_id >= @load_dv_batch_id
        or d_exerp_subscription.dv_batch_id >= @load_dv_batch_id)




	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.fact_exerp_subscription_sale
	WHERE fact_exerp_subscription_sale_key IN (
			SELECT fact_exerp_subscription_sale_key
			FROM dbo.#etl_step1
			)

	INSERT INTO fact_exerp_subscription_sale(
         fact_exerp_subscription_sale_key
        ,subscription_sale_id
        ,dim_exerp_subscription_key
        ,subscription_dim_club_key
        ,dim_exerp_product_key
        ,sale_dim_employee_key
        ,dim_club_key
        ,sale_id
        ,jf_fact_exerp_transaction_log_key
        ,previous_dim_exerp_subscription_key
        ,subscription_sale_type
        ,sale_dim_date_key
        ,sale_dim_time_key
        ,start_dim_date_key
        ,end_dim_date_key
        ,jf_normal_price
        ,jf_discount
        ,jf_price
        ,jf_sponsored
        ,jf_member
        ,prorata_period_normal_price
        ,prorata_period_discount
        ,prorata_period_price
        ,prorata_period_sponsored
        ,prorata_period_member
        ,init_period_normal_price
        ,init_period_discount
        ,init_period_price
        ,init_period_sponsored
        ,init_period_member
        ,admin_fee_normal_price
        ,admin_fee_discount
        ,admin_fee_price
        ,admin_fee_sponsored
        ,admin_fee_member
        ,binding_days
        ,init_contract_value
        ,subscription_sale_state
        ,dim_mms_member_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         fact_exerp_subscription_sale_key
        ,subscription_sale_id
        ,dim_exerp_subscription_key
        ,subscription_dim_club_key
        ,dim_exerp_product_key
        ,sale_dim_employee_key
        ,dim_club_key
        ,sale_id
        ,jf_fact_exerp_transaction_log_key
        ,previous_dim_exerp_subscription_key
        ,subscription_sale_type
        ,sale_dim_date_key
        ,sale_dim_time_key
        ,start_dim_date_key
        ,end_dim_date_key
        ,jf_normal_price
        ,jf_discount
        ,jf_price
        ,jf_sponsored
        ,jf_member
        ,prorata_period_normal_price
        ,prorata_period_discount
        ,prorata_period_price
        ,prorata_period_sponsored
        ,prorata_period_member
        ,init_period_normal_price
        ,init_period_discount
        ,init_period_price
        ,init_period_sponsored
        ,init_period_member
        ,admin_fee_normal_price
        ,admin_fee_discount
        ,admin_fee_price
        ,admin_fee_sponsored
        ,admin_fee_member
        ,binding_days
        ,init_contract_value
        ,subscription_sale_state
        ,dim_mms_member_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN


END
