CREATE PROC [dbo].[proc_fact_medallia_survey_data] @dv_batch_id [varchar](500) AS
BEGIN
	SET XACT_ABORT ON
	SET NOCOUNT ON

	DECLARE @max_dv_batch_id BIGINT = (	SELECT max(isnull(dv_batch_id, - 1)) FROM fact_medallia_survey_data	)
	DECLARE @current_dv_batch_id BIGINT = @dv_batch_id
	DECLARE @load_dv_batch_id BIGINT = CASE	WHEN @max_dv_batch_id < @current_dv_batch_id THEN @max_dv_batch_id ELSE @current_dv_batch_id END

	if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
	create table dbo.#etl_step1 with (distribution = hash (d_medallia_survey_data_key),location = user_db) as
	select 
		d_medallia_survey_data.d_medallia_survey_data_key,
		d_medallia_survey_data.survey_id,
		d_medallia_survey_data.field_name,
		d_medallia_survey_data.field_value 
	from
		d_medallia_survey_data
	where
		d_medallia_survey_data.field_name in ('survey_type_alt','member_id_text','e_creationdate','last_check_in_dim_date_time_datetime','club_id_text','survey_status')  
		and d_medallia_survey_data.dv_batch_id >= @load_dv_batch_id

	if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
	create table dbo.#etl_step2 with (distribution = hash (d_medallia_survey_data_key),location = user_db) as
	select 
		d_medallia_survey_data.d_medallia_survey_data_key,
		d_medallia_survey_data.survey_id,
		d_medallia_survey_data.field_name,
		case when d_medallia_survey_data.field_name is null then '-998'	else d_medallia_survey_data.field_name end dim_medallia_field_key,
		d_medallia_survey_data.field_value as survey_data, 
		d_medallia_survey_data.file_name,
		max(case when #etl_step1.field_name = 'survey_status' then #etl_step1.field_value else null end) as survey_status,
		max(case when #etl_step1.field_name = 'survey_type_alt' then #etl_step1.field_value else null end) as survey_type,
		max(case 
				when #etl_step1.field_name = 'member_id_text' 
					then 
						case when (isnumeric(#etl_step1.field_value) = 1 and (#etl_step1.field_value like '%[0-9]%') and (len(#etl_step1.field_value) <=9))
							then #etl_step1.field_value 
						else NULL
						end
				else null 
			end) as member_id,
		max(case when #etl_step1.field_name = 'club_id_text' then #etl_step1.field_value else null end) as club_id,
		max(case 
				when #etl_step1.field_name = 'e_creationdate' 
					then 
						case
							when isnumeric(#etl_step1.field_value) = 1 
								then DATEADD(SS, cast(substring(#etl_step1.field_value,1,10) as int), '19700101') 
							else #etl_step1.field_value
						end
				else null 
			end) as e_creationdate,
		max(case 
				when #etl_step1.field_name = 'last_check_in_dim_date_time_datetime' 
					then 
						case
							when isnumeric(#etl_step1.field_value) = 1 
								then DATEADD(SS, cast(substring(#etl_step1.field_value,1,10) as int), '19700101') 
							else #etl_step1.field_value
						end
				else null 
			end) as last_check_in_dim_date_time_datetime,
		d_medallia_survey_data.dv_batch_id,
		d_medallia_survey_data.dv_load_date_time
	from
		d_medallia_survey_data
	left join
		#etl_step1
			on d_medallia_survey_data.survey_id = #etl_step1.survey_id
	where		
		d_medallia_survey_data.dv_batch_id >= @load_dv_batch_id
	group by
		d_medallia_survey_data.d_medallia_survey_data_key,
		d_medallia_survey_data.survey_id,
		d_medallia_survey_data.field_name,
		d_medallia_survey_data.field_value, 
		d_medallia_survey_data.file_name,
		d_medallia_survey_data.dv_batch_id,
		d_medallia_survey_data.dv_load_date_time

	if object_id('tempdb..#etl_step3') is not null drop table #etl_step3
		create table dbo.#etl_step3 with (distribution = hash (d_medallia_survey_data_key),location = user_db) as
	select 
		#etl_step2.d_medallia_survey_data_key,
		#etl_step2.survey_id,
		#etl_step2.field_name,
		#etl_step2.dim_medallia_field_key,
		#etl_step2.survey_data, 
		#etl_step2.file_name,
		#etl_step2.survey_status,
		#etl_step2.survey_type,
		case when d_mms_member.dim_mms_member_key is null then '-998' else d_mms_member.dim_mms_member_key end dim_mms_member_key,
		case when d_mms_member.dim_mms_member_key is null then '-998' else d_mms_member.dim_mms_membership_key end dim_mms_membership_key,
		case when dim_club.dim_club_key is null then '-998'	else dim_club.dim_club_key end dim_club_key,
		case 
			when (#etl_step2.e_creationdate is null or isdate(#etl_step2.e_creationdate) = 0)
				then  '-998' 
			else convert(varchar, #etl_step2.e_creationdate, 112) 
		end as dim_survey_created_dim_date_key,
		case 
			when (#etl_step2.e_creationdate is null or isdate(#etl_step2.e_creationdate) = 0)
				then  '-998' 
			else '1' + replace(substring(convert(varchar,#etl_step2.e_creationdate,114), 1, 5),':','') 
		end as dim_survey_created_dim_time_key,
		case 
			when (#etl_step2.last_check_in_dim_date_time_datetime is null or isdate(#etl_step2.last_check_in_dim_date_time_datetime) = 0)
				then  '-998' 
			else convert(varchar, #etl_step2.last_check_in_dim_date_time_datetime, 112)
		end  as survey_data_converted_to_dim_date_key,
		case 
			when (#etl_step2.last_check_in_dim_date_time_datetime is null or isdate(#etl_step2.last_check_in_dim_date_time_datetime) = 0)
				then  '-998' 
			else '1' + replace(substring(convert(varchar,#etl_step2.last_check_in_dim_date_time_datetime,114), 1, 5),':','')
		end as survey_data_converted_to_dim_time_key,
		#etl_step2.dv_batch_id,
		#etl_step2.dv_load_date_time,
		convert(DATETIME, '99991231', 112) AS dv_load_end_date_time
	from
		#etl_step2
	left join 
		d_mms_member  
			on d_mms_member.member_id= #etl_step2.member_id 
	left join 
		dim_club  
			on dim_club.club_id=#etl_step2.club_id

	BEGIN TRAN

	DELETE dbo.fact_medallia_survey_data WHERE fact_medallia_survey_data_key IN (SELECT d_medallia_survey_data_key FROM dbo.#etl_step3)

	INSERT INTO fact_medallia_survey_data(
	      fact_medallia_survey_data_key
		, survey_id
		, field_name
		, survey_data
		, file_name
		, survey_status
		, survey_type
		, dim_medallia_field_key
		, dim_mms_member_key
		, dim_mms_membership_key
		, dim_club_key
		, dim_survey_created_dim_date_key
		, dim_survey_created_dim_time_key
		, survey_data_converted_to_dim_date_key
		, survey_data_converted_to_dim_time_key
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, dv_inserted_date_time
		, dv_insert_user
		)
	SELECT
	      d_medallia_survey_data_key
		, survey_id
		, field_name
		, survey_data
		, file_name
		, survey_status
		, survey_type
		, dim_medallia_field_key
		, dim_mms_member_key
		, dim_mms_membership_key
		, dim_club_key
		, dim_survey_created_dim_date_key
		, dim_survey_created_dim_time_key
		, survey_data_converted_to_dim_date_key
		, survey_data_converted_to_dim_time_key
		, dv_load_date_time
		, dv_load_end_date_time
		, dv_batch_id
		, getdate()
		, suser_sname()
	FROM #etl_step3

	COMMIT TRAN


END
