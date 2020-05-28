CREATE PROC [dbo].[proc_dim_exerp_product] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM dim_exerp_product
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
				distribution = HASH (dim_exerp_product_key)
				,location = user_db
				) AS
select d_exerp_product.bk_hash dim_exerp_product_key,
       d_exerp_master_product.master_product_name master_product_name,
	   d_exerp_master_product.master_product_state  master_product_state,
	   CASE
	   WHEN d_exerp_master_product.master_product_global_id IS NULL
		THEN '-998'
	   ELSE d_exerp_master_product.master_product_global_id
	   END as master_product_global_id,
	   CASE
	   WHEN d_exerp_product.product_id IS NULL
		THEN '-998'
	   ELSE d_exerp_product.product_id
	   END as product_id,
	   CASE
	   	WHEN d_exerp_master_product.master_product_id IS NULL
	   		THEN '-998'
	   	ELSE d_exerp_master_product.master_product_id
	   END AS master_product_id,
	   CASE
	   	WHEN d_exerp_product_group.product_group_id IS NULL
	   		THEN '-998'
	   	ELSE d_exerp_product_group.product_group_id
	   END  as primary_product_group_id,
	   CASE
	   	WHEN d_exerp_product.external_id IS NULL
	   		THEN '-998'
	   	ELSE d_exerp_product.external_id
	   END AS external_id,
	   d_exerp_product.dim_club_key dim_club_key,
	   d_exerp_product.master_dim_exerp_product_key master_dim_exerp_product_key,
	   d_exerp_product.product_name product_name,
	   d_exerp_product.product_type product_type,
	   d_exerp_product.product_sales_price sales_price,
	   d_exerp_product.product_minimum_price minimum_price,
	   d_exerp_product.product_cost_price cost_price,
	   d_exerp_product.product_blocked_flag blocked,
	   d_exerp_product.product_sales_commission sales_commission,
	   d_exerp_product.product_sales_units sales_units,
	   d_exerp_product.product_period_commission period_commission,
	   d_exerp_product.product_included_member_count included_member_count,
	   d_exerp_product.product_flat_rate_commission flat_rate_commission,
	   d_exerp_product_group.product_group_name	 primary_product_group_name,
	   dim_mms_product.dim_mms_product_key,
	    CASE
	 	WHEN d_exerp_product_group.external_id IS NULL
	 		THEN '-998'
	 	ELSE d_exerp_product_group.external_id
	    END as primary_product_group_external_id,
	    CASE
	    	WHEN d_exerp_parent_product_group.product_group_id IS NULL
	    		THEN '-998'
	    	ELSE d_exerp_parent_product_group.product_group_id
	    END as primary_parent_product_group_id,
	    d_exerp_parent_product_group.product_group_name as primary_parent_product_group_name,
	    CASE
	    	WHEN d_exerp_dimension_product_group.product_group_id IS NULL
	    		THEN '-998'
	    	ELSE d_exerp_dimension_product_group.product_group_id
	    END as primary_dimension_product_group_id,
	    d_exerp_dimension_product_group.product_group_name as primary_dimension_product_group_name,
	    case
	    	when d_exerp_product.dv_load_date_time >= isnull(d_exerp_master_product.dv_load_date_time,'Jan 1, 1753')
	    		and d_exerp_product.dv_load_date_time >= isnull(d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
				and d_exerp_product.dv_load_date_time >= isnull(dim_mms_product.dv_load_date_time,'Jan 1, 1753')
	    		then d_exerp_product.dv_load_date_time
            when d_exerp_master_product.dv_load_date_time >= isnull(d_exerp_product_group.dv_load_date_time,'Jan 1, 1753')
			    and d_exerp_master_product.dv_load_date_time >= isnull(dim_mms_product.dv_load_date_time,'Jan 1, 1753')
	    		then d_exerp_master_product.dv_load_date_time
			when d_exerp_product_group.dv_load_date_time >= isnull(dim_mms_product.dv_load_date_time,'Jan 1, 1753')
	    		then d_exerp_product_group.dv_load_date_time
	    	else isnull(dim_mms_product.dv_load_date_time,'Jan 1, 1753')
	    end dv_load_date_time,
	    case
	    	when d_exerp_product.dv_batch_id >= isnull(d_exerp_master_product.dv_batch_id,-1)
	    		and d_exerp_product.dv_batch_id >= isnull(d_exerp_product_group.dv_batch_id,-1)
				and d_exerp_product.dv_batch_id >= isnull(dim_mms_product.dv_batch_id,-1)
	    		then d_exerp_product.dv_batch_id
            when d_exerp_master_product.dv_batch_id >= isnull(d_exerp_product_group.dv_batch_id,-1)
			    and d_exerp_master_product.dv_batch_id >= isnull(dim_mms_product.dv_batch_id,-1)
	    		then d_exerp_master_product.dv_batch_id
			when d_exerp_product_group.dv_batch_id >= isnull(dim_mms_product.dv_batch_id,-1)
	    		then d_exerp_product_group.dv_batch_id
            else isnull(dim_mms_product.dv_batch_id,-1)
	    end dv_batch_id,
	    convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
    FROM
    	d_exerp_product d_exerp_product
    JOIN
    	d_exerp_master_product d_exerp_master_product
    	ON d_exerp_product.master_dim_exerp_product_key = d_exerp_master_product.bk_hash
    LEFT JOIN
    	d_exerp_product_group d_exerp_product_group
    	ON d_exerp_product.d_exerp_product_group_bk_hash = d_exerp_product_group.bk_hash
    LEFT JOIN
    	d_exerp_product_group d_exerp_parent_product_group
    	ON d_exerp_parent_product_group.bk_hash  = d_exerp_product_group.parent_d_exerp_product_group_bk_hash
    LEFT JOIN
	    d_exerp_product_group d_exerp_dimension_product_group
	    ON  d_exerp_dimension_product_group.bk_hash = d_exerp_product_group.dimension_d_exerp_product_group_bk_hash
    LEFT JOIN
	    dim_mms_product dim_mms_product
		ON d_exerp_product.dim_mms_product_key = dim_mms_product.dim_mms_product_key

	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.dim_exerp_product
	WHERE dim_exerp_product_key IN (
			SELECT dim_exerp_product_key
			FROM dbo.#etl_step1
			)

	INSERT INTO dim_exerp_product(
         dim_exerp_product_key
		,master_product_name
		,master_product_state
		,master_product_global_id
		,product_id
		,master_product_id
		,primary_product_group_id
		,external_id
		,dim_club_key
		,master_dim_exerp_product_key
		,product_name
		,product_type
		,sales_price
		,minimum_price
		,cost_price
		,blocked
		,sales_commission
		,sales_units
		,period_commission
		,included_member_count
		,flat_rate_commission
		,primary_product_group_name
		,primary_product_group_external_id
		,primary_parent_product_group_id
		,primary_parent_product_group_name
		,primary_dimension_product_group_id
		,primary_dimension_product_group_name
		,dim_mms_product_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         dim_exerp_product_key
		,master_product_name
		,master_product_state
		,master_product_global_id
		,product_id
		,master_product_id
		,primary_product_group_id
		,external_id
		,dim_club_key
		,master_dim_exerp_product_key
		,product_name
		,product_type
		,sales_price
		,minimum_price
		,cost_price
		,blocked
		,sales_commission
		,sales_units
		,period_commission
		,included_member_count
		,flat_rate_commission
		,primary_product_group_name
		,primary_product_group_external_id
		,primary_parent_product_group_id
		,primary_parent_product_group_name
		,primary_dimension_product_group_id
		,primary_dimension_product_group_name
		,dim_mms_product_key
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN


END
