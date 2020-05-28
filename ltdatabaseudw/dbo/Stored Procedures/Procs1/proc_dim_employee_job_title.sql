CREATE PROC [dbo].[proc_dim_employee_job_title] AS
begin

set nocount on
set xact_abort on

declare @delimiter varchar(2) = ','

if object_id('tempdb..#process') is not null drop table #process
create table #process with (distribution = hash(dim_employee_key)) as
select dim_employee_key,
       mms_club_id,
       job_levels,
       job_families,
       job_sub_families,
       job_profiles,
       business_titles,
       marketing_titles,
       job_codes,
       is_primary,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       len(job_levels) - len(replace(job_levels,@delimiter,'')) level_delim_count,
       len(mms_club_id) - len(replace(mms_club_id,@delimiter,'')) club_delim_count,
       len(job_families) - len(replace(job_families,@delimiter,'')) family_delim_count,
       len(job_sub_families) - len(replace(job_sub_families,@delimiter,'')) sub_family_delim_count,
       len(job_profiles) - len(replace(job_profiles,@delimiter,'')) profile_delim_count,
       len(business_titles) - len(replace(business_titles,@delimiter,'')) business_title_delim_count,
       len(marketing_titles) - len(replace(marketing_titles,@delimiter,'')) marketing_title_delim_count,
       len(job_codes) - len(replace(job_codes,@delimiter,'')) job_code_delim_count,
	   len(is_primary) - len(replace(is_primary,@delimiter,'')) is_primary_delim_count
from d_workday_employee

truncate table dim_employee_job_title

declare @s int = 1
declare @e int = (Select max(job_code_delim_count)+1 from #process)

delete #process 
where job_code_delim_count <> profile_delim_count
or job_code_delim_count <> business_title_delim_count
or job_code_delim_count <> marketing_title_delim_count
or job_code_delim_count <> is_primary_delim_count

while @s <= @e
begin

    /*insert records with only a single job where no more parsing is required*/
    insert into dim_employee_job_title (
        dim_employee_key,
        workday_region_id,
        level,
        family,
        sub_family,
        profile,
        business_title,
        marketing_title,
        job_code,
        is_primary_flag,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id,
        dv_inserted_date_time,
        dv_insert_user
        )
    select dim_employee_key,
           case when ltrim(rtrim(mms_club_id)) = '' then null else ltrim(rtrim(mms_club_id)) end,
           ltrim(rtrim(job_levels)),
           ltrim(rtrim(job_families)),
           ltrim(rtrim(job_sub_families)),
           ltrim(rtrim(job_profiles)),
           ltrim(rtrim(business_titles)),
           ltrim(rtrim(marketing_titles)),
           ltrim(rtrim(job_codes)),
           case when ltrim(rtrim(is_primary)) = '' then 'N' else ltrim(rtrim(is_primary)) end,
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id,
           getdate(),
           suser_sname()
      from #process
     where CHARINDEX(@delimiter,job_levels) = 0

     /*stop processing records that have been parsed*/
     delete from #process where CHARINDEX(@delimiter,job_levels) = 0

     /*insert records with more than 1 job where additional parsing is required*/
    insert into dim_employee_job_title (
        dim_employee_key,
        workday_region_id,
        level,
        family,
        sub_family,
        profile,
        business_title,
        marketing_title,
        job_code,
	is_primary_flag,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id,
        dv_inserted_date_time,
        dv_insert_user
        )
    select dim_employee_key,
           case when ltrim(rtrim(substring(mms_club_id,1,charindex(@delimiter,mms_club_id)-1))) = '' then null else ltrim(rtrim(substring(mms_club_id,1,charindex(@delimiter,mms_club_id)-1))) end,
           ltrim(rtrim(substring(job_levels,1,charindex(@delimiter,job_levels)-1))),
           ltrim(rtrim(substring(job_families,1,charindex(@delimiter,job_families)-1))),
           ltrim(rtrim(substring(job_sub_families,1,charindex(@delimiter,job_sub_families)-1))),
           ltrim(rtrim(substring(job_profiles,1,charindex(@delimiter,job_profiles)-1))),
           ltrim(rtrim(substring(business_titles,1,charindex(@delimiter,business_titles)-1))),
           ltrim(rtrim(substring(marketing_titles,1,charindex(@delimiter,marketing_titles)-1))),
           ltrim(rtrim(substring(job_codes,1,charindex(@delimiter,job_codes)-1))),
	   case when ltrim(rtrim(substring(is_primary,1,charindex(@delimiter,is_primary)-1))) = '' then 'N' else ltrim(rtrim(substring(is_primary,1,charindex(@delimiter,is_primary)-1))) end,
           dv_load_date_time,
           dv_load_end_date_time,
           dv_batch_id,
           getdate(),
           suser_sname()
      from #process

    /*remove the inserted job element from the string*/
    update #process
       set mms_club_id = ltrim(rtrim(right(mms_club_id,len(mms_club_id)-charindex(@delimiter,mms_club_id)))),
           job_levels = ltrim(rtrim(right(job_levels,len(job_levels)-charindex(@delimiter,job_levels)))),
           job_families = ltrim(rtrim(right(job_families,len(job_families)-charindex(@delimiter,job_families)))),
           job_sub_families = ltrim(rtrim(right(job_sub_families,len(job_sub_families)-charindex(@delimiter,job_sub_families)))),
           job_profiles = ltrim(rtrim(right(job_profiles,len(job_profiles)-charindex(@delimiter,job_profiles)))),
           business_titles = ltrim(rtrim(right(business_titles,len(business_titles)-charindex(@delimiter,business_titles)))),
           marketing_titles = ltrim(rtrim(right(marketing_titles,len(marketing_titles)-charindex(@delimiter,marketing_titles)))),
           job_codes = ltrim(rtrim(right(job_codes,len(job_codes)-charindex(@delimiter,job_codes)))),
		   is_primary = ltrim(rtrim(right(is_primary,len(is_primary)-charindex(@delimiter,is_primary))))

     delete from #process where job_codes like ',%,'
    /*repeat til end*/
    set @s = @s + 1
end

/*delete out blank records due to the "random" commas*/
delete from dim_employee_job_title where level = ''

/*jobs can be duplicated within the strings, find duplicates*/
if object_id('tempdb..#dedupe') is not null drop table #dedupe
create table #dedupe with (distribution = round_robin) as
select dim_employee_job_title_id
from dim_employee_job_title
join (select dim_employee_key,job_code,workday_region_id,is_primary_flag, min(dim_employee_job_title_id) min_id from dim_employee_job_title group by dim_employee_key, job_code,workday_region_id,is_primary_flag) dedupe
  on dim_employee_job_title.dim_employee_key = dedupe.dim_employee_key 
 and dim_employee_job_title.job_code = dedupe.job_code 
 and dim_employee_job_title.workday_region_id = dedupe.workday_region_id
 and dim_employee_job_title.is_primary_flag = dedupe.is_primary_flag
 and dim_employee_job_title.dim_employee_job_title_id > dedupe.min_id

/*remove duplicates*/
delete dim_employee_job_title
where dim_employee_job_title_id in (select dim_employee_job_title_id from #dedupe)


  
drop table #process
drop table #dedupe
end
