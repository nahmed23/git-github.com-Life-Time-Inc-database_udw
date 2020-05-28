CREATE PROC [dbo].[proc_wrk_pega_member_usage] @dv_batch_id [varchar](500) AS
begin

	set nocount on
	set xact_abort on

	DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
    DECLARE @load_dv_batch_id BIGINT = ( select isnull(max(dv_batch_id), -1) from wrk_pega_member_usage)

	if object_id('tempdb..#member_usage') is not null drop table #member_usage
	create table dbo.#member_usage with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 	
		fact_mms_member_usage.dim_mms_checkin_member_key as dim_mms_member_key,
		fact_mms_member_usage.member_usage_id as member_usage_id,
		d_mms_member.member_id as member_id,
		fact_mms_member_usage.check_in_dim_date_time as check_in_date_time,
		dim_club.club_id as club_id,
		@current_dv_batch_id as dv_batch_id
	from 
		fact_mms_member_usage 
	left join
		d_mms_member
			on d_mms_member.dim_mms_member_key = fact_mms_member_usage.dim_mms_checkin_member_key
	left join 
		dim_club 
			on dim_club.dim_club_key = fact_mms_member_usage.dim_club_key
	where 
		fact_mms_member_usage.dv_batch_id >= @load_dv_batch_id 
		and fact_mms_member_usage.check_in_dim_date_key >= '20190101'


	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.wrk_pega_member_usage WHERE dv_batch_id = @current_dv_batch_id 

	INSERT INTO wrk_pega_member_usage (
         dim_mms_member_key
		,member_usage_id
		,member_id
		,check_in_date_time
		,club_id
		,sequence_number
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
		)
	SELECT           
		dim_mms_member_key
		,member_usage_id
		,member_id
		,check_in_date_time
		,club_id
		,row_number() over(partition by dv_batch_id order by member_id ) 
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #member_usage

	COMMIT TRAN

END
