CREATE PROC [dbo].[proc_fact_trainerize_program_plan_item] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (
			SELECT max(isnull(dv_batch_id, - 1))
			FROM fact_trainerize_program_plan_item
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
				distribution = HASH (fact_trainerize_program_plan_item_key)
				,location = user_db
				) AS
select   d_ec_plan_items.fact_trainerize_program_plan_item_key fact_trainerize_program_plan_item_key
        ,d_ec_plan_items.plan_item_id plan_item_id
        ,d_ec_plan_items.completed_flag completed_flag
        ,d_ec_plan_items.created_dim_date_key created_dim_date_key
        ,dim_trainerize_plan.dim_mms_member_key
		,dim_trainerize_plan.dim_trainerize_plan_key dim_trainerize_plan_key
		,dim_trainerize_program.dim_trainerize_program_key dim_trainerize_program_key
        ,d_ec_plan_items.item_description  item_description
        ,d_ec_plan_items.item_dim_date_key  item_dim_date_key
        ,d_ec_plan_items.item_dim_time_key  item_dim_time_key
        ,d_ec_plan_items.item_name  item_name
        ,d_ec_plan_items.item_type  item_type
		,case when dim_trainerize_plan.dim_trainerize_plan_key in ('-997','-998','-999') then dim_trainerize_plan.dim_trainerize_plan_key
         when dim_trainerize_plan.plan_id is null then '-998'
         when dim_trainerize_plan.dim_employee_key is null then '-999'
         else dim_trainerize_plan.dim_employee_key end  plan_dim_employee_key
		,case when dim_trainerize_program.dim_trainerize_program_key in ('-997','-998','-999') then dim_trainerize_program.dim_trainerize_program_key
         when dim_trainerize_program.program_id is null  then '-998'
         when dim_trainerize_program.dim_employee_key is null then '-999'
         else dim_trainerize_program.dim_employee_key end  program_dim_employee_key
        ,d_ec_plan_items.source_id source_id
        ,d_ec_plan_items.source_type  source_type
        ,d_ec_plan_items.updated_dim_date_key updated_dim_date_key
	    ,case 
		    when isnull(d_ec_plan_items.dv_load_date_time,'Jan 1, 1753')  >= isnull(dim_trainerize_plan.dv_load_date_time,'Jan 1, 1753')
			    and isnull(d_ec_plan_items.dv_load_date_time,'Jan 1, 1753')  >= isnull(dim_trainerize_program.dv_load_date_time,'Jan 1, 1753')
					then d_ec_plan_items.dv_load_date_time		
		    when isnull(dim_trainerize_plan.dv_load_date_time,'Jan 1, 1753')  >= isnull(dim_trainerize_program.dv_load_date_time,'Jan 1, 1753')
					then dim_trainerize_plan.dv_load_date_time		
		        else isnull(dim_trainerize_program.dv_load_date_time,'Jan 1, 1753') 
	    end dv_load_date_time	
        , case 
            when isnull(d_ec_plan_items.dv_batch_id,-1) >= isnull(dim_trainerize_plan.dv_batch_id,-1)				
			    and isnull(d_ec_plan_items.dv_batch_id,-1) >= isnull(dim_trainerize_program.dv_batch_id,-1)
					then d_ec_plan_items.dv_batch_id
			when isnull(dim_trainerize_plan.dv_batch_id,-1) >= isnull(dim_trainerize_program.dv_batch_id,-1)
				    then dim_trainerize_plan.dv_batch_id
	        else isnull(dim_trainerize_program.dv_batch_id,-1) 
	    end dv_batch_id
	    , convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	from d_ec_plan_items 
    left join dim_trainerize_plan
	on d_ec_plan_items.dim_trainerize_plan_key = dim_trainerize_plan.dim_trainerize_plan_key
	left join dim_trainerize_program
	on dim_trainerize_plan.dim_trainerize_program_key = dim_trainerize_program.dim_trainerize_program_key
    where (d_ec_plan_items.dv_batch_id >= @load_dv_batch_id
		or dim_trainerize_plan.dv_batch_id >= @load_dv_batch_id
		or dim_trainerize_program.dv_batch_id >= @load_dv_batch_id)  


	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.fact_trainerize_program_plan_item
	WHERE fact_trainerize_program_plan_item_key IN (
			SELECT fact_trainerize_program_plan_item_key
			FROM dbo.#etl_step1
			)

	INSERT INTO fact_trainerize_program_plan_item(
         fact_trainerize_program_plan_item_key
        ,plan_item_id
        ,completed_flag
        ,created_dim_date_key
        ,dim_mms_member_key
        ,dim_trainerize_plan_key
        ,dim_trainerize_program_key
        ,item_description
        ,item_dim_date_key
        ,item_dim_time_key
        ,item_name
        ,item_type
        ,plan_dim_employee_key
        ,program_dim_employee_key
        ,source_id
        ,source_type
        ,updated_dim_date_key
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT
         fact_trainerize_program_plan_item_key
        ,plan_item_id
        ,completed_flag
        ,created_dim_date_key
        ,dim_mms_member_key
        ,dim_trainerize_plan_key
        ,dim_trainerize_program_key
        ,item_description
        ,item_dim_date_key
        ,item_dim_time_key
        ,item_name
        ,item_type
        ,plan_dim_employee_key
        ,program_dim_employee_key
        ,source_id
        ,source_type
        ,updated_dim_date_key
		,dv_load_date_time
		,dv_batch_id
		,dv_load_end_date_time
		,getdate()
		,suser_sname()
	FROM #etl_step1

	COMMIT TRAN

			
END
