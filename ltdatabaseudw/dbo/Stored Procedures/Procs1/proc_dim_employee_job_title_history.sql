CREATE PROC [dbo].[proc_dim_employee_job_title_history] AS
begin

set nocount on
set xact_abort on

	declare @delimiter varchar(2) = ','

	if object_id('tempdb..#process') is not null drop table #process
	create table #process with (distribution = hash(dim_employee_key)) as
	select 
		dim_employee_key,
		employee_id,
		cf_employment_status,
		job_codes,
		business_titles,
		marketing_titles,
		job_profiles,
		is_primary,
		dv_load_date_time,
		dv_load_end_date_time,
		dv_batch_id ,
		len(job_codes) - len(replace(job_codes,@delimiter,'')) as job_code_delim_count,
		len(business_titles) - len(replace(business_titles,@delimiter,'')) as business_title_delim_count,
		len(marketing_titles) - len(replace(marketing_titles,@delimiter,'')) as marketing_title_delim_count,
		len(job_profiles) - len(replace(job_profiles,@delimiter,'')) as job_profile_delim_count,
		len(is_primary) - len(replace(is_primary,@delimiter,'')) as is_primary_delim_count,
		row_number() over (partition by dim_employee_key order by dv_load_date_time) row_count
	from 
		d_workday_employee_history
	

	/*delete invalid data*/
	delete #process 
	where 
		(
			job_code_delim_count <> job_profile_delim_count
			or job_code_delim_count <> business_title_delim_count
			or job_code_delim_count <> marketing_title_delim_count
			or job_code_delim_count <> is_primary_delim_count
		)

	if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
	create table #etl_step1 
	(
		dim_employee_key varchar(32),
		employee_id int,
		cf_employment_status nvarchar(255),
		job_codes nvarchar(4000),
		business_titles nvarchar(4000),
		marketing_titles nvarchar(4000),
		job_profiles nvarchar(4000),
		is_primary nvarchar(255),
		dv_load_date_time datetime,
		dv_load_end_date_time datetime,
		dv_batch_id bigint,
		row_count int
	) with (distribution = hash(dim_employee_key)) 

	declare @s int = 1
	declare @e int = (Select max(job_code_delim_count)+1 from #process)
	
	while @s <= @e
	begin

		/*insert records with only a single job where no more parsing is required*/
		insert into #etl_step1 (
			dim_employee_key,
			employee_id,
			cf_employment_status,
			job_codes,
			business_titles,
			marketing_titles,
			job_profiles,
			is_primary,
			dv_load_date_time,
			dv_load_end_date_time,
			dv_batch_id ,
			row_count
			)
		select 
			dim_employee_key,
			ltrim(rtrim(employee_id)),
			ltrim(rtrim(cf_employment_status)),
			ltrim(rtrim(job_codes)),
			ltrim(rtrim(business_titles)),
			ltrim(rtrim(marketing_titles)),
			ltrim(rtrim(job_profiles)),
			case when ltrim(rtrim(is_primary)) = '' then 'N' else ltrim(rtrim(is_primary)) end,
			dv_load_date_time,
			dv_load_end_date_time,
			dv_batch_id,
			row_count
		from 
			#process
		where	
			CHARINDEX(@delimiter,job_codes) = 0
    
		/*stop processing records that have been parsed*/
		delete from #process where CHARINDEX(@delimiter,job_codes) = 0

		/*insert records with more than 1 job where additional parsing is required*/
		insert into #etl_step1 (
			dim_employee_key,
			employee_id,
			cf_employment_status,
			job_codes,
			business_titles,
			marketing_titles,
			job_profiles,
			is_primary,
			dv_load_date_time,
			dv_load_end_date_time,
			dv_batch_id,
			row_count
		)
		select 
			dim_employee_key,
			ltrim(rtrim(employee_id)),
			ltrim(rtrim(cf_employment_status)),
			ltrim(rtrim(substring(job_codes,1,charindex(@delimiter,job_codes)-1))),
			ltrim(rtrim(substring(business_titles,1,charindex(@delimiter,business_titles)-1))),
			ltrim(rtrim(substring(marketing_titles,1,charindex(@delimiter,marketing_titles)-1))),
			ltrim(rtrim(substring(job_profiles,1,charindex(@delimiter,job_profiles)-1))),
			case when ltrim(rtrim(substring(is_primary,1,charindex(@delimiter,is_primary)-1))) = '' then 'N' else ltrim(rtrim(substring(is_primary,1,charindex(@delimiter,is_primary)-1))) end,
			dv_load_date_time,
			dv_load_end_date_time,
			dv_batch_id,
			row_count
		from 
			#process

		/*remove the inserted job element from the string*/
		update #process
		set 
			job_codes = ltrim(rtrim(right(job_codes,len(job_codes)-charindex(@delimiter,job_codes)))),
			job_profiles = ltrim(rtrim(right(job_profiles,len(job_profiles)-charindex(@delimiter,job_profiles)))),
			business_titles = ltrim(rtrim(right(business_titles,len(business_titles)-charindex(@delimiter,business_titles)))),
			marketing_titles = ltrim(rtrim(right(marketing_titles,len(marketing_titles)-charindex(@delimiter,marketing_titles)))),
			is_primary = ltrim(rtrim(right(is_primary,len(is_primary)-charindex(@delimiter,is_primary))))

		delete from #process where job_codes like ',%,'

	/*repeat til end*/
	set @s = @s + 1

	end

	if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
	create table dbo.#etl_step2 with (distribution = hash (dim_employee_key),location = user_db) as 
	select 
		dim_employee_key,
		employee_id,
		cf_employment_status,
		job_codes,
		business_titles,
		marketing_titles,
		job_profiles,
		is_primary as is_primary_flag,
		dv_load_date_time,
		dv_load_end_date_time,
		dv_batch_id ,
		row_count as group_no
	from 
		#etl_step1 
	where 
		1 = 1
		and job_codes != '' 
		and business_titles != '' 
		and marketing_titles != ''
		

	/*get effective date and expiration date using window functions based on the group no got from the #etl_step1.*/
	if object_id('tempdb..#etl_step3') is not null drop table #etl_step3
	create table dbo.#etl_step3 with (distribution = hash (dim_employee_key),location = user_db) as 
	select 
		#etl_step2.dim_employee_key,
		#etl_step2.employee_id,
		#etl_step2.cf_employment_status,
		#etl_step2.job_codes,
		#etl_step2.business_titles,
		#etl_step2.marketing_titles,
		#etl_step2.job_profiles,
		#etl_step2.is_primary_flag,
		case 
			when 
				(
					((lag(#etl_step2.group_no) over(partition by #etl_step2.dim_employee_key,#etl_step2.job_codes,#etl_step2.business_titles,#etl_step2.cf_employment_status 
					order by #etl_step2.group_no)) = (#etl_step2.group_no - 1))
					or  
					((lag(#etl_step2.group_no) over(partition by #etl_step2.dim_employee_key,#etl_step2.job_codes,#etl_step2.business_titles,#etl_step2.cf_employment_status 
					order by #etl_step2.group_no)) = (#etl_step2.group_no))
				)
				then null 
			else #etl_step2.dv_load_date_time 
		end as effective_date_time,
		case 
			when 
				(
					((lead(#etl_step2.group_no) over(partition by #etl_step2.dim_employee_key,#etl_step2.job_codes,#etl_step2.business_titles,#etl_step2.cf_employment_status 
					order by #etl_step2.group_no)) = (#etl_step2.group_no + 1))
					or 
					((lead(#etl_step2.group_no) over(partition by #etl_step2.dim_employee_key,#etl_step2.job_codes,#etl_step2.business_titles,#etl_step2.cf_employment_status 
					order by #etl_step2.group_no)) = (#etl_step2.group_no))
				)
				then null 
			else #etl_step2.dv_load_end_date_time
		end as expiration_date_time,
		#etl_step2.dv_load_date_time,
		#etl_step2.dv_batch_id,
		#etl_step2.group_no
	from 
		#etl_step2 
	

	/*get effective date and expiration date using window functions from the #etl_step1.*/
	if object_id('tempdb..#etl_step4') is not null drop table #etl_step4
	create table dbo.#etl_step4 with (distribution = hash (dim_employee_key),location = user_db) as 	
	select 
		#etl_step3.dim_employee_key,
		#etl_step3.employee_id,
		#etl_step3.cf_employment_status,
		#etl_step3.job_codes,
		#etl_step3.business_titles,
		#etl_step3.marketing_titles,
		#etl_step3.job_profiles,
		#etl_step3.is_primary_flag,
		max(#etl_step3.effective_date_time) over(partition by #etl_step3.dim_employee_key,#etl_step3.job_codes,#etl_step3.business_titles,#etl_step3.cf_employment_status 
		order by #etl_step3.group_no rows between unbounded preceding and current row) as effective_date_time,
		min(#etl_step3.expiration_date_time) over(partition by #etl_step3.dim_employee_key,#etl_step3.job_codes,#etl_step3.business_titles,#etl_step3.cf_employment_status 
		order by #etl_step3.group_no rows between current row  and unbounded following) as expiration_date_time,
		#etl_step3.dv_load_date_time,
		#etl_step3.dv_batch_id
	from 
		#etl_step3 


	if object_id('tempdb..#etl_step5') is not null drop table #etl_step5
	create table dbo.#etl_step5 with (distribution = hash (dim_employee_key),location = user_db) as 
	select 
		#etl_step4.dim_employee_key,
		#etl_step4.employee_id,
		#etl_step4.cf_employment_status,
		#etl_step4.job_codes,
		#etl_step4.business_titles,
		#etl_step4.marketing_titles,
		#etl_step4.job_profiles,
		#etl_step4.is_primary_flag,
		#etl_step4.effective_date_time,
		isnull(#etl_step4.expiration_date_time,'9999-12-31 00:00:00.000') as expiration_date_time,
		max(#etl_step4.dv_load_date_time) as dv_load_date_time,
		convert(DATETIME, '99991231', 112) as dv_load_end_date_time,
		max(#etl_step4.dv_batch_id) as dv_batch_id
	from 
		#etl_step4 
	group by
		#etl_step4.dim_employee_key,
		#etl_step4.employee_id,
		#etl_step4.cf_employment_status,
		#etl_step4.job_codes,
		#etl_step4.business_titles,
		#etl_step4.marketing_titles,
		#etl_step4.job_profiles,
		#etl_step4.is_primary_flag,
		#etl_step4.effective_date_time,
		isnull(#etl_step4.expiration_date_time,'9999-12-31 00:00:00.000')

	/*   truncate and  Insert records into dim_employee_job_title_history table*/
	truncate table dim_employee_job_title_history
		
	BEGIN TRAN


		insert into dim_employee_job_title_history (
				dim_employee_key,
				employee_id,
				cf_employment_status,
				job_codes,
				business_titles,
				marketing_titles,
				job_profiles,
				is_primary_flag,
				effective_date_time,
				expiration_date_time,
				dv_load_date_time,
				dv_load_end_date_time,
				dv_batch_id,
				dv_inserted_date_time,
				dv_insert_user
			)
			SELECT
				dim_employee_key,
				employee_id,
				cf_employment_status,
				job_codes,
				business_titles,
				marketing_titles,
				job_profiles,
				is_primary_flag,
				effective_date_time,
				expiration_date_time,
				dv_load_date_time,
				dv_load_end_date_time,
				dv_batch_id,
				getdate(),
				suser_sname()
			FROM #etl_step5

	COMMIT TRAN

			
END
