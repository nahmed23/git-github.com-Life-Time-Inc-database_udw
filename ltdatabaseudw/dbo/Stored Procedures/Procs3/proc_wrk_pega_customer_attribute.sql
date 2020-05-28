CREATE PROC [dbo].[proc_wrk_pega_customer_attribute] @dv_batch_id [varchar](500) AS
begin

	set nocount on
	set xact_abort on

    DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
    DECLARE @load_dv_batch_id BIGINT =  ( select isnull(max(dv_batch_id), -1) from wrk_pega_customer_attribute)


	if object_id('tempdb..#member_keys') is not null drop table #member_keys
	create table dbo.#member_keys with (distribution = hash (dim_mms_member_key),location = user_db) as
	select dim_mms_member_key
	from
	(
		select distinct dim_mms_member_key from d_mms_member where dv_batch_id >= @load_dv_batch_id
		union
		select distinct dim_mms_member_key from d_mms_member where dim_mms_membership_key in (
		select dim_mms_membership_key from dim_mms_membership where dv_batch_id >= @load_dv_batch_id)
		union
		select distinct dim_mms_member_key from d_mms_member where dim_mms_membership_key in (
		select dim_mms_membership_key from d_mms_membership_phone where dv_batch_id >= @load_dv_batch_id)
		union
		select distinct dim_mms_member_key from d_mms_member where dim_mms_membership_key in (
		select dim_mms_membership_key from dim_mms_membership where dim_mms_membership_type_key in (
		select dim_mms_membership_type_key from d_mms_membership_type_attribute where dv_batch_id >= @load_dv_batch_id))
		union
		select distinct dim_mms_member_key from d_mart_fact_seg_member_expected_value where dv_batch_id >= @load_dv_batch_id
		union
		select distinct dim_mms_member_key from d_mms_member where member_id in (
		select id from dim_mdm_golden_record_customer_id_list where id_type =1 and dv_batch_id >= @load_dv_batch_id)
		union
		select distinct dim_mms_member_key from d_mart_fact_seg_member_primary_activity where dv_batch_id >= @load_dv_batch_id
		union
		select distinct dim_mms_member_key from d_mms_member where dim_mms_membership_key in (
		select dim_mms_membership_key from d_mart_fact_seg_membership_term_risk where dv_batch_id >= @load_dv_batch_id)
		union
		select distinct dim_mms_member_key from d_mms_member where email_address in (
		select email_address from fact_commprefs_user_preferences where dv_batch_id >= @load_dv_batch_id)
		union
		select distinct dim_mms_member_key from d_mart_fact_member_interests where dv_batch_id >= @load_dv_batch_id
	)x

	if object_id('tempdb..#member_details') is not null drop table #member_details
	create table dbo.#member_details with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		#member_keys.dim_mms_member_key,
		d_mms_member.member_id,
		d_mms_member.dim_mms_membership_key,
		ltrim(rtrim(d_mms_member.first_name)) as first_name,
		ltrim(rtrim(d_mms_member.last_name)) as last_name,
		convert(varchar,d_mms_member.date_of_birth,112) as date_of_birth,
		ltrim(rtrim(d_mms_member.member_active_flag)) as member_active_flag,
		ltrim(rtrim(d_mms_member.gender_abbreviation)) as member_gender,
		ltrim(rtrim(d_mms_member.assess_junior_member_dues_flag)) as assess_junior_member_dues_flag,
		ltrim(rtrim(d_mms_member.description_member)) as member_type,
		d_mms_member.join_date_key as member_join_date,
		ltrim(rtrim(d_mms_member.party_id)) as party_id,
		ltrim(rtrim(d_mms_member.email_address)) as email_address
	from 
		#member_keys
	left join
		d_mms_member 
			on #member_keys.dim_mms_member_key = 	d_mms_member.dim_mms_member_key

	if object_id('tempdb..#membership_details') is not null drop table #membership_details
	create table dbo.#membership_details with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		#member_details.dim_mms_member_key,
		#member_details.member_id,
		#member_details.dim_mms_membership_key,
		dim_mms_membership.membership_id,
		#member_details.first_name,
		#member_details.last_name,
		#member_details.date_of_birth,
		#member_details.member_active_flag,
		#member_details.member_gender,
		#member_details.assess_junior_member_dues_flag,
		#member_details.member_type,
		#member_details.member_join_date,
		#member_details.party_id,
		#member_details.email_address,
		dim_mms_membership.created_date_time as membership_created_date_time,
		ltrim(rtrim(dim_mms_membership.membership_address_line_1)) as membership_address_line_1,
		ltrim(rtrim(dim_mms_membership.membership_address_line_2)) as membership_address_line_2,
		ltrim(rtrim(dim_mms_membership.membership_address_city)) as membership_address_city,
		ltrim(rtrim(dim_mms_membership.membership_address_country)) as membership_address_country,	
		ltrim(rtrim(dim_mms_membership.membership_address_postal_code)) as membership_address_postal_code,	
		ltrim(rtrim(dim_mms_membership.membership_address_state_abbreviation)) as membership_address_state_abbreviation,
		convert(varchar,dim_mms_membership.membership_cancellation_request_date,112) as membership_cancellation_request_date,
		convert(varchar,dim_mms_membership.membership_expiration_date,112) as membership_expiration_date,
		ltrim(rtrim(dim_mms_membership.membership_source)) as membership_source,	
		ltrim(rtrim(dim_mms_membership.membership_status)) as membership_status,	
		ltrim(rtrim(dim_mms_membership.membership_type)) as membership_type,
		ltrim(rtrim(dim_mms_membership.current_price)) as membership_current_price,
		ltrim(rtrim(dim_mms_membership.termination_reason)) as membership_termination_reason,
		ltrim(rtrim(dim_mms_membership_type.product_id)) as membership_product_id,
		ltrim(rtrim(dim_mms_product.product_description)) as membership_product_description,
		ltrim(rtrim(dim_club.club_id)) as member_mms_home_club_id,
		dim_mms_membership.home_dim_club_key as membership_home_dim_club_key,
		isnull(CONCAT(membership_phone_home.area_code,membership_phone_home.number),CONCAT(membership_phone_business.area_code,membership_phone_business.number)) as membership_phone,
		case when d_mms_membership_type_attribute.val_membership_type_attribute_id = 16 then 'No Club Access' else 'Club Access' end as membership_club_access
	from 
		#member_details
	left join
		dim_mms_membership 
			on #member_details.dim_mms_membership_key = dim_mms_membership.dim_mms_membership_key
	left join
		dim_mms_membership_type 
			on dim_mms_membership.dim_mms_membership_type_key = dim_mms_membership_type.dim_mms_membership_type_key 
	left join
		d_mms_membership_type_attribute
			on dim_mms_membership.dim_mms_membership_type_key = d_mms_membership_type_attribute.dim_mms_membership_type_key 
			and d_mms_membership_type_attribute.val_membership_type_attribute_id= 16
	left join
		dim_mms_product
			on dim_mms_membership_type.dim_mms_product_key = dim_mms_product.dim_mms_product_key
	left join
		dim_club
			on dim_mms_membership.home_dim_club_key = dim_club.dim_club_key
	left join 
		d_mms_membership_phone membership_phone_home 
			on dim_mms_membership.dim_mms_membership_key = membership_phone_home.dim_mms_membership_key 
			and membership_phone_home.phone_type_dim_description_key in 
				(
					select 
						dim_description_key 
					from 
						marketing.v_dim_description 
					where 
						source_object = 'r_mms_val_phone_type' 
						and description = 'home'
				)
	left join 
		d_mms_membership_phone membership_phone_business
			on dim_mms_membership.dim_mms_membership_key = membership_phone_business.dim_mms_membership_key 
			and membership_phone_business.phone_type_dim_description_key in 
				(
					select 
						dim_description_key 
					from 
						marketing.v_dim_description 
					where 
						source_object = 'r_mms_val_phone_type' 
						and description = 'business'
				)
				
	
	if object_id('tempdb..#seg_member_expected_value') is not null drop table #seg_member_expected_value
	create table dbo.#seg_member_expected_value with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		d_mart_fact_seg_member_expected_value.dim_mms_member_key,
		sum(d_mart_fact_seg_member_expected_value.expected_value_60_months) as sum_expected_value_60_months
	from 
		#member_details
	left join
		d_mart_fact_seg_member_expected_value
			on #member_details.dim_mms_member_key = d_mart_fact_seg_member_expected_value.dim_mms_member_key
	where 
		d_mart_fact_seg_member_expected_value.active_flag='Y'
	group by
		d_mart_fact_seg_member_expected_value.dim_mms_member_key
		
		
	if object_id('tempdb..#dim_mdm_golden_record_customer_id_list') is not null drop table #dim_mdm_golden_record_customer_id_list
	create table dbo.#dim_mdm_golden_record_customer_id_list with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		#member_details.dim_mms_member_key,
		dim_mdm_golden_record_customer_id_list.entity_id,
		row_number() over (partition by dim_mdm_golden_record_customer_id_list.id order by dim_mdm_golden_record_customer_id_list.dv_load_date_time desc) as rnk
	from 
		#member_details 
	left join
		dim_mdm_golden_record_customer_id_list 
			on  convert(varchar(50), #member_details.member_id) = convert(varchar(50),dim_mdm_golden_record_customer_id_list.id)
	where 
		dim_mdm_golden_record_customer_id_list.id_type = 1


	if object_id('tempdb..#fact_commprefs_user_preferences') is not null drop table #fact_commprefs_user_preferences
	create table dbo.#fact_commprefs_user_preferences with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		#member_details.dim_mms_member_key,
		fact_commprefs_user_preferences.promotional_opt_in,
		fact_commprefs_user_preferences.global_opt_in
	from 
		#member_details 
	left join
		fact_commprefs_user_preferences 
			on #member_details.email_address = fact_commprefs_user_preferences.email_address


	if object_id('tempdb..#mart_seg_member_primary_activity') is not null drop table #mart_seg_member_primary_activity
	create table dbo.#mart_seg_member_primary_activity with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		d_mart_fact_seg_member_primary_activity.dim_mms_member_key,
		ltrim(rtrim(v_dim_mart_seg_member_primary_activity.primary_activity)) as primary_activity_segment_name,
		row_number() over (partition by d_mart_fact_seg_member_primary_activity.dim_mms_member_key order by d_mart_fact_seg_member_primary_activity.dv_load_date_time desc) as rnk
	from 
		#member_details 
	left join
		d_mart_fact_seg_member_primary_activity 
			on #member_details.dim_mms_member_key = d_mart_fact_seg_member_primary_activity.dim_mms_member_key
	left join
		marketing.v_dim_mart_seg_member_primary_activity 
			on cast(d_mart_fact_seg_member_primary_activity.primary_activity_segment as int) = cast(v_dim_mart_seg_member_primary_activity.primary_activity_segment as int) 
	where 
		d_mart_fact_seg_member_primary_activity.active_flag='Y'
		
		
	if object_id('tempdb..#term_risk_segment_details') is not null drop table #term_risk_segment_details
	create table dbo.#term_risk_segment_details with (distribution = hash (dim_mms_membership_key),location = user_db) as
	select 
		d_mart_fact_seg_membership_term_risk.dim_mms_membership_key,
		ltrim(rtrim(v_dim_mart_seg_membership_term_risk.term_risk)) as term_risk_segment_name,
		row_number() over (partition by d_mart_fact_seg_membership_term_risk.dim_mms_membership_key order by d_mart_fact_seg_membership_term_risk.dv_load_date_time desc) as rnk
	from 
		#member_details 
	left join
		d_mart_fact_seg_membership_term_risk
			on #member_details.dim_mms_membership_key = d_mart_fact_seg_membership_term_risk.dim_mms_membership_key
	left join
		marketing.v_dim_mart_seg_membership_term_risk 
			on cast(d_mart_fact_seg_membership_term_risk.term_risk_segment as int) = cast(v_dim_mart_seg_membership_term_risk.term_risk_segment as int)
	where 
		d_mart_fact_seg_membership_term_risk.active_flag='Y'


	if object_id('tempdb..#main_result') is not null drop table #main_result
	create table dbo.#main_result with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		#membership_details.dim_mms_member_key,
		#membership_details.member_id,
		#membership_details.dim_mms_membership_key,
		#membership_details.membership_id,
		#dim_mdm_golden_record_customer_id_list.entity_id,
		#membership_details.first_name,
		#membership_details.last_name,
		#membership_details.date_of_birth,
		#membership_details.member_active_flag,
		#membership_details.member_gender,
		#membership_details.assess_junior_member_dues_flag,
		#membership_details.member_type,
		#membership_details.member_join_date,
		#membership_details.party_id,
		#membership_details.email_address,
		#membership_details.membership_created_date_time,
		#membership_details.membership_address_line_1,
		#membership_details.membership_address_line_2,
		#membership_details.membership_address_city,
		#membership_details.membership_address_country,	
		#membership_details.membership_address_postal_code,	
		#membership_details.membership_address_state_abbreviation,
		#membership_details.membership_cancellation_request_date,
		#membership_details.membership_expiration_date,
		#membership_details.membership_source,	
		#membership_details.membership_status,	
		#membership_details.membership_type,
		#membership_details.membership_current_price,
		#membership_details.membership_termination_reason,
		#membership_details.membership_product_id,
		#membership_details.membership_product_description,
		#membership_details.member_mms_home_club_id,
		#membership_details.membership_home_dim_club_key,
		#membership_details.membership_phone,
		#membership_details.membership_club_access,
		#seg_member_expected_value.sum_expected_value_60_months,
		#fact_commprefs_user_preferences.promotional_opt_in,
		#fact_commprefs_user_preferences.global_opt_in,
		#mart_seg_member_primary_activity.primary_activity_segment_name,
		#term_risk_segment_details.term_risk_segment_name,
		v_medallia_nps_member_summary_scores.nps_score,		
		v_medallia_nps_member_summary_scores.survey_date as nps_survey_date,
		@current_dv_batch_id as dv_batch_id
	from 
		#membership_details
	left join
		#seg_member_expected_value
			on #membership_details.dim_mms_member_key = #seg_member_expected_value.dim_mms_member_key 
	left join
		#dim_mdm_golden_record_customer_id_list 
			on #membership_details.dim_mms_member_key = #dim_mdm_golden_record_customer_id_list.dim_mms_member_key 
			and #dim_mdm_golden_record_customer_id_list.rnk = 1
	left join
		#fact_commprefs_user_preferences
			on #membership_details.dim_mms_member_key = #fact_commprefs_user_preferences.dim_mms_member_key 
	left join
		#mart_seg_member_primary_activity
			on #membership_details.dim_mms_member_key = #mart_seg_member_primary_activity.dim_mms_member_key 
			and #mart_seg_member_primary_activity.rnk = 1
	left join
		#term_risk_segment_details
			on #membership_details.dim_mms_membership_key = #term_risk_segment_details.dim_mms_membership_key 
			and #term_risk_segment_details.rnk = 1
	left join
		marketing.v_medallia_nps_member_summary_scores 
			on #membership_details.dim_mms_member_key = v_medallia_nps_member_summary_scores.member_key

	
	/*   Delete records from the table that exist*/
	/*   Insert records from records from current and missing batches*/
	BEGIN TRAN

	DELETE dbo.wrk_pega_customer_attribute 	WHERE dv_batch_id = @current_dv_batch_id

	INSERT INTO wrk_pega_customer_attribute (
		dim_mms_membership_key
		,membership_id
		,dim_mms_member_key
		,member_id
		,member_mms_home_club_id
		,membership_home_dim_club_key
		,sequence_number
		,entity_id
		,first_name
		,last_name
		,date_of_birth
		,member_active_flag
		,member_gender
		,assess_junior_member_dues_flag
		,member_type
		,member_join_date
		,party_id
		,membership_created_date_time
		,membership_phone
		,membership_club_access
		,membership_address_line_1
		,membership_address_line_2
		,membership_address_city
		,membership_address_country
		,membership_address_postal_code
		,membership_address_state_abbreviation
		,membership_cancellation_request_date
		,membership_expiration_date
		,membership_source
		,membership_status
		,membership_type
		,membership_current_price
		,membership_termination_reason
		,membership_product_id
		,membership_product_description
		,sum_expected_value_60_months
		,primary_activity_segment_name
		,term_risk_segment_name
		,nps_score
		,nps_survey_date
		,promotion_opt_in
		,global_opt_in
		,dv_batch_id
		,dv_inserted_date_time
		,dv_insert_user
	)
	SELECT           
		dim_mms_membership_key
		,membership_id
		,dim_mms_member_key
		,member_id
		,member_mms_home_club_id
		,membership_home_dim_club_key
		,row_number() over(partition by dv_batch_id order by member_id ) 
		,entity_id
		,first_name
		,last_name
		,date_of_birth
		,member_active_flag
		,member_gender
		,assess_junior_member_dues_flag
		,member_type
		,member_join_date
		,party_id
		,membership_created_date_time
		,membership_phone
		,membership_club_access
		,membership_address_line_1
		,membership_address_line_2
		,membership_address_city
		,membership_address_country
		,membership_address_postal_code
		,membership_address_state_abbreviation
		,membership_cancellation_request_date
		,membership_expiration_date
		,membership_source
		,membership_status
		,membership_type
		,membership_current_price
		,membership_termination_reason
		,membership_product_id
		,membership_product_description
		,sum_expected_value_60_months
		,primary_activity_segment_name
		,term_risk_segment_name
		,nps_score
		,nps_survey_date
		,promotional_opt_in
		,global_opt_in
		,dv_batch_id
		,getdate()
		,suser_sname()
	FROM #main_result

	COMMIT TRAN
END
