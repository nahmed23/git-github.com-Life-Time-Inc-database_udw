CREATE PROC [dbo].[proc_wrk_pega_child_center_usage] @dv_batch_id [varchar](500) AS
begin

	set nocount on
	set xact_abort on

/*	DECLARE @dv_batch_id BIGINT=-1*/
	DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
    DECLARE @load_dv_batch_id BIGINT =  ( select isnull(max(dv_batch_id), -1) from wrk_pega_child_center_usage)
	
if object_id('tempdb..#member_details_parent') is not null drop table #member_details_parent
create table dbo.#member_details_parent with (distribution = hash (child_center_usage_id),location = user_db) as
	select
	CCU.fact_mms_child_center_usage_key
	,CCU.child_center_usage_id
	,CCU.child_age_months
	,CCU.dv_batch_id
	,CCU.check_in_dim_date_key
	,CCU.check_in_dim_time_key
	,CCU.check_out_dim_date_key
	,CCU.check_out_dim_time_key
	,CCU.check_in_dim_mms_member_key
	,CCU.dim_club_key
	from 
	fact_mms_child_center_usage CCU 
	where 
	CCU.check_in_dim_date_key >= '20190101'
	and  CCU.dv_batch_id >= @load_dv_batch_id
	
if object_id('tempdb..#member_details_activity') is not null drop table #member_details_activity
create table dbo.#member_details_activity with (distribution = hash (fact_mms_child_center_usage_key),location = user_db) as
	select
	CCUAA.fact_mms_child_center_usage_key
	from 
	fact_mms_child_center_usage_activity_area CCUAA
	where 
	CCUAA.check_in_dim_date_key >= '20190101'
	and  CCUAA.dv_batch_id >= @load_dv_batch_id

if object_id('tempdb..#member_details_child') is not null drop table #member_details_child
create table dbo.#member_details_child with (distribution = hash (child_center_usage_id),location = user_db) as	
select 
	CCU.fact_mms_child_center_usage_key
	,CCU.child_center_usage_id
	,CCU.child_age_months
	,CCU.dv_batch_id
	,CCU.check_in_dim_date_key
	,CCU.check_in_dim_time_key
	,CCU.check_out_dim_date_key
	,CCU.check_out_dim_time_key
	,CCU.check_in_dim_mms_member_key
	,CCU.dim_club_key
	from 
	fact_mms_child_center_usage CCU
where 
	CCU.check_in_dim_date_key >= '20190101' and  CCU.dv_batch_id < @load_dv_batch_id
	and
	EXISTS (SELECT fact_mms_child_center_usage_key FROM #member_details_activity
	 WHERE #member_details_activity.fact_mms_child_center_usage_key=CCU.fact_mms_child_center_usage_key 
	 )
	 
if object_id('tempdb..#member_details_all') is not null drop table #member_details_all
create table dbo.#member_details_all with (distribution = hash (child_center_usage_id),location = user_db) as
select 
	fact_mms_child_center_usage_key,
	child_center_usage_id,
	child_age_months,
	dv_batch_id,
	check_in_dim_date_key,
	check_in_dim_time_key,
	check_out_dim_date_key,
	check_out_dim_time_key,
	check_in_dim_mms_member_key,
	dim_club_key
	from 
	#member_details_parent 
	UNION ALL
select 
	fact_mms_child_center_usage_key,
	child_center_usage_id,
	child_age_months,
	dv_batch_id,
	check_in_dim_date_key,
	check_in_dim_time_key,
	check_out_dim_date_key,
	check_out_dim_time_key,
	check_in_dim_mms_member_key,
	dim_club_key
	from 
	#member_details_child 	


if object_id('tempdb..#member_details') is not null drop table #member_details
create table dbo.#member_details with (distribution = hash (child_center_usage_id),location = user_db) as
	select 
		fact_mms_child_center_usage_key
		,child_center_usage_id
		,check_in_member_id
		,check_in_date
		,check_in_time
		,check_out_member_id
		,check_out_date
		,check_out_time
		,child_age_months
		,child_member_id
		,club_id
		,membership_id
		,dv_batch_id
		from(
		select
		CCU.fact_mms_child_center_usage_key as fact_mms_child_center_usage_key,
		CCU.child_center_usage_id as child_center_usage_id,
		DMMU_1.member_id  as check_in_member_id,
		DD_CIN.calendar_date as check_in_date,
		DT_CIN.display_24_hour_time as check_in_time,
		DMMU_2.member_id  as check_out_member_id,
		DD_COUT.calendar_date as check_out_date,
		DT_COUT.display_24_hour_time as check_out_time,
		CCU.child_age_months  as child_age_months,
		DMMU_3.member_id as child_member_id,
		DC.club_id as club_id,
		DMMU_3.membership_id  as membership_id,
		CCU.dv_batch_id
		from 
		#member_details_all CCU
		JOIN dim_date DD_CIN ON DD_CIN.dim_date_key=CCU.check_in_dim_date_key
		JOIN dim_time DT_CIN ON DT_CIN.dim_time_key=CCU.check_in_dim_time_key
		JOIN dim_date DD_COUT ON DD_COUT.dim_date_key=CCU.check_out_dim_date_key
		JOIN dim_time DT_COUT ON DT_COUT.dim_time_key=CCU.check_out_dim_time_key
		LEFT JOIN  d_mms_member DMMU_1 ON DMMU_1.dim_mms_member_key=CCU.check_in_dim_mms_member_key
		LEFT JOIN  d_mms_member DMMU_2 ON DMMU_2.dim_mms_member_key=CCU.check_in_dim_mms_member_key
		LEFT JOIN  d_mms_member DMMU_3 ON DMMU_3.dim_mms_member_key=CCU.check_in_dim_mms_member_key
		LEFT JOIN dim_club DC ON DC.dim_club_key=CCU.dim_club_key
		where 
		CCU.check_in_dim_date_key >= '20190101'/*convert(varchar,DATEADD(year, -1, getdate()),112)*/
		and  CCU.dv_batch_id >= @load_dv_batch_id
		)A

	BEGIN TRAN

	DELETE dbo.wrk_pega_child_center_usage 	WHERE dv_batch_id = @load_dv_batch_id

	INSERT INTO wrk_pega_child_center_usage (
		fact_mms_child_center_usage_key
		,child_center_usage_id
		,check_in_member_id
		,check_in_date
		,check_in_time
		,check_out_member_id
		,check_out_date
		,check_out_time
		,child_age_months
		,child_member_id
		,club_id
		,membership_id
		,sequence_number
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
	)
	SELECT 
		fact_mms_child_center_usage_key
		,child_center_usage_id
		,check_in_member_id
		,check_in_date
		,check_in_time
		,check_out_member_id
		,check_out_date
		,check_out_time
		,child_age_months
		,child_member_id
		,club_id
		,membership_id
		,row_number() over(order by child_center_usage_id) as sequence_number
		,@current_dv_batch_id
		,getdate()
		,suser_sname()
	FROM #member_details

	COMMIT TRAN
	
	if object_id('tempdb..#activity_details') is not null drop table #activity_details
	create table #activity_details with (distribution = hash (fact_mms_child_center_usage_key),location = user_db) as
		select 
		ltrim(rtrim(A.activity_area_dim_description_key)) as activity_area_dim_description_key,
		A.fact_mms_child_center_usage_key,
		B.val_activity_area_id,
		B.description,
		A.dv_batch_id
		from fact_mms_child_center_usage_activity_area A JOIN
		(
		select 
		'r_mms_val_activity_area_'+bk_hash as bk_hash,
		val_activity_area_id,
		description  
		from r_mms_val_activity_area)B
		on B.bk_hash=A.activity_area_dim_description_key
		where  
		A.check_in_dim_date_key >= '20190101'/*convert(varchar,DATEADD(year, -1, getdate()),112)*/
		and A.dv_batch_id>=  @load_dv_batch_id

BEGIN TRAN

	DELETE dbo.wrk_pega_child_center_usage_activity_area WHERE dv_batch_id = @load_dv_batch_id

	INSERT INTO wrk_pega_child_center_usage_activity_area (
		activity_area_dim_description_key
		,fact_mms_child_center_usage_key
		,val_activity_area_id
		,description
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
	)
	SELECT           
		activity_area_dim_description_key
		,fact_mms_child_center_usage_key
		,val_activity_area_id
		,description
		,@current_dv_batch_id
		,getdate()
		,suser_sname()
	FROM #activity_details

	COMMIT TRAN
	
END
