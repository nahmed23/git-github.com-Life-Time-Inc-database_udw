CREATE PROC [dbo].[proc_wrk_pega_customer_attribute_interest] @dv_batch_id [varchar](500) AS
begin

	set nocount on
	set xact_abort on

    DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
    DECLARE @load_dv_batch_id BIGINT = ( select isnull(max(dv_batch_id), -1) from wrk_pega_customer_attribute_interest)
	
    -- get interest details of those member_keys only which were processed in wrk_pega_customer_attribute table for the current batch_id
	
	if object_id('tempdb..#member_keys') is not null drop table #member_keys
	create table dbo.#member_keys with (distribution = hash (dim_mms_member_key),location = user_db) as
	select distinct dim_mms_member_key from wrk_pega_customer_attribute where dv_batch_id >= @load_dv_batch_id
	

	if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
	create table dbo.#etl_step1 with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		d_mart_fact_member_interests.dim_mms_member_key,
		d_mart_fact_member_interests.member_id,
		d_mart_fact_member_interests.interest_id,
		ltrim(rtrim(d_mart_dim_interest_segment_details.interest_display_name)) as interest_name,
		d_mart_fact_member_interests.interest_confidence,
		row_number() over (partition by d_mart_fact_member_interests.dim_mms_member_key, d_mart_fact_member_interests.interest_id order by d_mart_fact_member_interests.dv_load_date_time desc) as rnk
	from 
		#member_keys
	left join
		d_mart_fact_member_interests
			on #member_keys.dim_mms_member_key = d_mart_fact_member_interests.dim_mms_member_key
	join
		d_mart_dim_interest_segment_details
			on d_mart_fact_member_interests.interest_id = d_mart_dim_interest_segment_details.interest_id 
	where 
		d_mart_fact_member_interests.active_flag ='Y' 


	if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
	create table dbo.#etl_step2 with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		#etl_step1.dim_mms_member_key,
		#etl_step1.member_id,
		#etl_step1.interest_id,
		#etl_step1.interest_name,
		#etl_step1.interest_confidence,
		@current_dv_batch_id as dv_batch_id
	from
		#etl_step1
	where 
		#etl_step1.rnk = 1

	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.wrk_pega_customer_attribute_interest WHERE dv_batch_id = @current_dv_batch_id 

	INSERT INTO wrk_pega_customer_attribute_interest (
         dim_mms_member_key
		,member_id
		,interest_id
        ,interest_name
	    ,interest_confidence
		,sequence_number
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT           
		dim_mms_member_key
		,member_id
		,interest_id
        ,interest_name
	    ,interest_confidence
		,row_number() over(partition by dv_batch_id order by member_id ) 
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #etl_step2

	COMMIT TRAN

END
