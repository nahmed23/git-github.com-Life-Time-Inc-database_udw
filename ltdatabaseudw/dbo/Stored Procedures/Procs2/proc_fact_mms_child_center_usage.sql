CREATE PROC [dbo].[proc_fact_mms_child_center_usage] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_child_center_usage)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#inc') is not null drop table #inc
create table #inc with(distribution = hash(fact_mms_child_center_usage_key)) as
select fact_mms_child_center_usage_key
from d_mms_child_center_usage
where dv_batch_id >= @load_dv_batch_id
union
select fact_mms_child_center_usage_key
from d_mms_child_center_usage_exception
where dv_batch_id >= @load_dv_batch_id

if object_id('tempdb..#child_center_usage') is not null drop table #child_center_usage
create table #child_center_usage with(distribution = hash(fact_mms_child_center_usage_key)) as
select distinct d_mms_child_center_usage.bk_hash fact_mms_child_center_usage_key,
       d_mms_child_center_usage.child_center_usage_id,
       d_mms_child_center_usage.check_in_dim_date_key,
       d_mms_child_center_usage.check_in_dim_mms_member_key,
       d_mms_child_center_usage.check_in_dim_time_key,
       d_mms_child_center_usage.check_out_dim_date_key,
       d_mms_child_center_usage.check_out_dim_mms_member_key,
       d_mms_child_center_usage.check_out_dim_time_key,
       DATEDIFF(mm, d_mms_member.date_of_birth, check_in_dd.calendar_date)
          - CASE WHEN (MONTH(d_mms_member.date_of_birth) > MONTH(check_in_dd.calendar_date)) OR (MONTH(d_mms_member.date_of_birth) = MONTH(check_in_dd.calendar_date) AND DAY(d_mms_member.date_of_birth) > DAY(check_in_dd.calendar_date)) THEN 1 
                 ELSE 0 END 
            as child_age_months,
       DATEDIFF(yy, d_mms_member.date_of_birth, check_in_dd.calendar_date)
          - CASE WHEN (MONTH(d_mms_member.date_of_birth) > MONTH(check_in_dd.calendar_date)) OR (MONTH(d_mms_member.date_of_birth) = MONTH(check_in_dd.calendar_date) AND DAY(d_mms_member.date_of_birth) > DAY(check_in_dd.calendar_date)) THEN 1 
                 ELSE 0 END 
            as child_age_years,
       case when d_mms_child_center_usage_exception.d_mms_child_center_usage_exception_id is not null then 'Y' else 'N' end exception_flag,
       d_mms_child_center_usage.child_dim_mms_member_key,
       d_mms_member.gender_abbreviation child_gender_abbreviation,
       d_mms_child_center_usage.dim_club_key,
	   d_mms_member.membership_id,
       d_mms_member.dim_mms_membership_key,
	   d_mms_member.description_member description_member,
       d_mms_child_center_usage.length_of_stay_minutes,
       cast(d_mms_child_center_usage.length_of_stay_minutes/60 as varchar) + ' hrs. '+cast(d_mms_child_center_usage.length_of_stay_minutes%60 as varchar)+' mins' length_of_stay_display,
       case when d_mms_child_center_usage.dv_load_date_time >= isnull(d_mms_child_center_usage_exception.dv_load_date_time,'Jan 1, 1753')
                 then d_mms_child_center_usage.dv_load_date_time
            else isnull(d_mms_child_center_usage_exception.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
       case when d_mms_child_center_usage.dv_batch_id >= isnull(d_mms_child_center_usage_exception.dv_batch_id,-1)
                 then d_mms_child_center_usage.dv_batch_id
            else isnull(d_mms_child_center_usage_exception.dv_batch_id,-1) end dv_batch_id
  from d_mms_child_center_usage
  join d_mms_member
    on d_mms_child_center_usage.child_dim_mms_member_key = d_mms_member.dim_mms_member_key
  join dim_date check_in_dd
    on d_mms_child_center_usage.check_in_dim_date_key = check_in_dd.dim_date_key
   left join d_mms_child_center_usage_exception
    on d_mms_child_center_usage.bk_hash = d_mms_child_center_usage_exception.fact_mms_child_center_usage_key
		and d_mms_child_center_usage_exception.dv_load_date_time = (select max(dv_load_date_time) from d_mms_child_center_usage_exception where d_mms_child_center_usage.bk_hash = fact_mms_child_center_usage_key) /* Added this logic after analysis as part of the defect UDW-10626*/
 where d_mms_child_center_usage.bk_hash in (select fact_mms_child_center_usage_key from #inc)
 /*where d_mms_child_center_usage.bk_hash in ('D132441B9BC129EA90FD04BE84E9521E') --(select fact_mms_child_center_usage_key from #inc)*/
 
if object_id('tempdb..#child_center_usage_1') is not null drop table #child_center_usage_1
create table #child_center_usage_1 with(distribution = hash(fact_mms_child_center_usage_key)) as
select #child_center_usage.fact_mms_child_center_usage_key fact_mms_child_center_usage_key,
       case when #child_center_usage.membership_id is null then '-998'
       when d_mms_member.dim_mms_member_key is null then '-997'
       else d_mms_member.dim_mms_member_key end   primary_dim_mms_member_key,
	   #child_center_usage.dv_load_date_time dv_load_date_time,
	   #child_center_usage.dv_batch_id dv_batch_id
from  #child_center_usage
left join d_mms_member
on d_mms_member.membership_id = #child_center_usage.membership_id
where d_mms_member.description_member = 'Primary'


if object_id('tempdb..#child_center_usage_2') is not null drop table #child_center_usage_2
create table #child_center_usage_2 with(distribution = hash(fact_mms_child_center_usage_key)) as
select #child_center_usage.fact_mms_child_center_usage_key fact_mms_child_center_usage_key,
       #child_center_usage.child_center_usage_id child_center_usage_id,
	   #child_center_usage.check_in_dim_date_key check_in_dim_date_key,
	   #child_center_usage.check_in_dim_mms_member_key check_in_dim_mms_member_key,
	   #child_center_usage.check_in_dim_time_key check_in_dim_time_key,
	   #child_center_usage.check_out_dim_date_key check_out_dim_date_key,
	   #child_center_usage.check_out_dim_mms_member_key check_out_dim_mms_member_key,
	   #child_center_usage.check_out_dim_time_key check_out_dim_time_key,
	   #child_center_usage.child_age_months child_age_months,
	   #child_center_usage.child_age_years child_age_years,
	   #child_center_usage.exception_flag exception_flag,
	   #child_center_usage.child_dim_mms_member_key child_dim_mms_member_key,
	   #child_center_usage.child_gender_abbreviation child_gender_abbreviation,
	   #child_center_usage.dim_club_key dim_club_key,
	   #child_center_usage.dim_mms_membership_key dim_mms_membership_key,
	   #child_center_usage.length_of_stay_minutes length_of_stay_minutes,
	   #child_center_usage.length_of_stay_display length_of_stay_display,
	   #child_center_usage_1.primary_dim_mms_member_key,
	   #child_center_usage.dv_load_date_time dv_load_date_time,
	   #child_center_usage.dv_batch_id dv_batch_id
from  #child_center_usage
left join #child_center_usage_1
on #child_center_usage.fact_mms_child_center_usage_key = #child_center_usage_1.fact_mms_child_center_usage_key

/*select * from #child_center_usage_2*/

delete from fact_mms_child_center_usage where fact_mms_child_center_usage_key in (select fact_mms_child_center_usage_key from #inc)

declare @insert_date_time datetime = getdate()

insert into fact_mms_child_center_usage (
fact_mms_child_center_usage_key,
child_center_usage_id,
check_in_dim_date_key,
check_in_dim_mms_member_key,
check_in_dim_time_key,
check_out_dim_date_key,
check_out_dim_mms_member_key,
check_out_dim_time_key,
child_age_months,
child_age_years,
exception_flag,
child_dim_mms_member_key,
child_gender_abbreviation,
dim_club_key,
dim_mms_membership_key,
kids_play_check_in_count,
length_of_stay_display,
length_of_stay_minutes,
primary_dim_mms_member_key,
dv_load_date_time,
dv_load_end_date_time,
dv_batch_id,
dv_inserted_date_time,
dv_insert_user)
select fact_mms_child_center_usage_key,
       child_center_usage_id,
       check_in_dim_date_key,
       check_in_dim_mms_member_key,
       check_in_dim_time_key,
       check_out_dim_date_key,
       check_out_dim_mms_member_key,
       check_out_dim_time_key,
       child_age_months,
       child_age_years,
       exception_flag,
       child_dim_mms_member_key,
       child_gender_abbreviation,
       dim_club_key,
       dim_mms_membership_key,
       0,
       length_of_stay_display,
       length_of_stay_minutes,
       primary_dim_mms_member_key,       
       dv_load_date_time,
       'dec 31, 9999',/*dv_load_end_date_time*/
       dv_batch_id,
       @insert_date_time,
       suser_sname()
  from #child_center_usage_2
    
    
    
end

