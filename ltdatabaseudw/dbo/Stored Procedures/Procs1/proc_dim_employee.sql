CREATE PROC [dbo].[proc_dim_employee] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

truncate table dim_employee

if object_id('tempdb..#dim_mms_employee') is not null drop table #dim_mms_employee
create table dbo.#dim_mms_employee with(distribution=hash(dim_employee_key), location=user_db, heap) as
select distinct d_mms_employee.dim_employee_key dim_employee_key,
	   d_mms_employee.dim_club_key dim_club_key,
	   d_mms_employee.employee_id employee_id,
	   d_mms_employee.member_id member_id,
	   d_mms_employee.first_name first_name,
	   d_mms_employee.last_name last_name,
	   case when d_mms_employee.employee_active_flag = 'Y' and d_mms_employee_role.sales_manager_flag = 'Y' then 'Y'
	        else 'N'  end membership_sales_manager_ind,
	   case when d_mms_employee.employee_active_flag = 'Y' and d_mms_employee_role.sales_group_flag = 'Y' then 'Y'
	        else 'N'  end membership_sales_group_ind,
	   case when d_mms_employee.employee_active_flag = 'Y' and d_mms_employee_role.department_head_sales_for_net_units_flag = 'Y' then 'Y'
	        else 'N'  end department_head_sales_for_net_units_flag,
	   case when d_mms_employee.employee_active_flag = 'Y' and d_mms_employee_role.assistant_department_head_sales_for_net_units_flag = 'Y'
	     and d_mms_employee_role.department_head_sales_for_net_units_flag = 'N'  then 'Y'
	        else 'N'  end assistant_department_head_sales_for_net_units_flag,
	   d_mms_employee.employee_active_flag employee_active_flag,
	   d_mms_employee.employee_name employee_name,
	   d_mms_employee.employee_name_last_first employee_name_last_first,
	   d_mms_employee.inserted_date_time inserted_date_time_employee,
	   case when d_mms_employee.dv_load_date_time > isnull(d_mms_employee_role.dv_load_date_time,'1753-01-01 00:00:00.000')
			      then d_mms_employee.dv_load_date_time
				  else isnull(d_mms_employee_role.dv_load_date_time ,'1753-01-01 00:00:00.000')
			   end dv_load_date_time,
		     case when d_mms_employee.dv_batch_id > isnull(d_mms_employee_role.dv_batch_id ,'-1')
			      then d_mms_employee.dv_batch_id
				  else isnull(d_mms_employee_role.dv_batch_id,'-1')
			   end dv_batch_id,
	   case when d_mms_employee.dv_load_end_date_time > isnull(d_mms_employee_role.dv_load_end_date_time,'Jan 1, 1753')
			      then d_mms_employee.dv_load_end_date_time
				  else isnull(d_mms_employee_role.dv_load_end_date_time ,'Jan 1, 1753')
			   end dv_load_end_date_time

from d_mms_employee
left join d_mms_employee_role
on d_mms_employee.employee_id = d_mms_employee_role.employee_id


union

select distinct d_mms_employee.dim_employee_key dim_employee_key,
	   d_mms_employee.dim_club_key dim_club_key,
	   d_mms_employee.employee_id employee_id,
	   d_mms_employee.member_id member_id,
	   d_mms_employee.first_name first_name,
	   d_mms_employee.last_name last_name,
	   case when d_mms_employee.employee_active_flag = 'Y' and d_mms_employee_role.sales_manager_flag = 'Y' then 'Y'
	        else 'N' end membership_sales_manager_ind,
	   case when d_mms_employee.employee_active_flag = 'Y' and d_mms_employee_role.sales_group_flag = 'Y' then 'Y'
	        else 'N' end membership_sales_group_ind,
	   case when d_mms_employee.employee_active_flag = 'Y' and d_mms_employee_role.department_head_sales_for_net_units_flag = 'Y' then 'Y'
	        else 'N' end department_head_sales_for_net_units_flag,
	   case when d_mms_employee.employee_active_flag = 'Y' and d_mms_employee_role.assistant_department_head_sales_for_net_units_flag = 'Y'
	         and d_mms_employee_role.department_head_sales_for_net_units_flag = 'N' then 'Y'
	        else 'N' end assistant_department_head_sales_for_net_units_flag,
	   d_mms_employee.employee_active_flag employee_active_flag,
	   d_mms_employee.employee_name employee_name,
	   d_mms_employee.employee_name_last_first employee_name_last_first,
	   d_mms_employee.inserted_date_time inserted_date_time_employee,
	   case when d_mms_employee.dv_load_date_time > isnull(d_mms_employee_role.dv_load_date_time,'1753-01-01 00:00:00.000')
			      then d_mms_employee.dv_load_date_time
				  else isnull(d_mms_employee_role.dv_load_date_time ,'1753-01-01 00:00:00.000')
			   end dv_load_date_time,
		 	case when d_mms_employee.dv_batch_id > isnull(d_mms_employee_role.dv_batch_id ,'-1')
			      then d_mms_employee.dv_batch_id
				  else isnull(d_mms_employee_role.dv_batch_id ,'-1')
			   end dv_batch_id  ,
      case when d_mms_employee.dv_load_end_date_time > isnull(d_mms_employee_role.dv_load_end_date_time,'Jan 1, 1753')
			      then d_mms_employee.dv_load_end_date_time
				  else isnull(d_mms_employee_role.dv_load_end_date_time ,'Jan 1, 1753')
			   end dv_load_end_date_time


from
(select distinct d_mms_employee_role.employee_id
 from d_mms_employee_role
 ) EmployeeList
 join d_mms_employee
 on EmployeeList.employee_id = EmployeeList.employee_id
 join d_mms_employee_role
 on d_mms_employee.employee_id = d_mms_employee_role.employee_id



  /*This table is created for aggregation. I tried not to use the audit columns in max, but they were giving duplicates in my final join if
  I do not. Nevertheless I think we should be using max of them always in UDW, hence should be harmless*/
if object_id('tempdb..#dim_employee_afterAggregation') is not null drop table #dim_employee_afterAggregation
create table dbo.#dim_employee_afterAggregation with(distribution=hash(dim_employee_key), location=user_db, heap) as
select dim_employee_key,
     employee_id,
     dim_club_key,
     member_id,
     first_name,
     last_name,
	 employee_name,
	 employee_name_last_first,
	 employee_active_flag,
	 inserted_date_time_employee,
     max(membership_sales_manager_ind) membership_sales_manager_ind,
     max(membership_sales_group_ind) membership_sales_group_ind,
     max(department_head_sales_for_net_units_flag) department_head_sales_for_net_units_flag,
     max(assistant_department_head_sales_for_net_units_flag) assistant_department_head_sales_for_net_units_flag,
	 max(dv_load_date_time) dv_load_date_time,
     max(dv_batch_id) dv_batch_id,
	 max(dv_load_end_date_time) dv_load_end_date_time

	 from #dim_mms_employee
group by
     dim_employee_key,
     employee_id,
     dim_club_key,
     member_id,
     first_name,
     last_name,
	 employee_name,
	 employee_name_last_first,
	 employee_active_flag,
	 inserted_date_time_employee



if object_id('tempdb..#dim_employee') is not null drop table #dim_employee
create table #dim_employee with(distribution = hash(dim_employee_key)) as
with mms_employee
(dim_employee_key, dim_club_key, employee_id,member_id,
 first_name, last_name, membership_sales_manager_ind,
               membership_sales_group_ind, department_head_sales_for_net_units_flag, assistant_department_head_sales_for_net_units_flag,
		   employee_active_flag,employee_name, employee_name_last_first,inserted_date_time_employee,
			   dv_load_date_time,dv_batch_id,dv_load_end_date_time) as

(select #dim_employee_afterAggregation.dim_employee_key dim_employee_key,
        #dim_employee_afterAggregation.dim_club_key dim_club_key,
	    #dim_employee_afterAggregation.employee_id employee_id,
	    #dim_employee_afterAggregation.member_id member_id,
	    #dim_employee_afterAggregation.first_name first_name,
	    #dim_employee_afterAggregation.last_name last_name,
	    #dim_employee_afterAggregation.membership_sales_manager_ind membership_sales_manager_ind,
		#dim_employee_afterAggregation.membership_sales_group_ind membership_sales_group_ind,
		#dim_employee_afterAggregation.department_head_sales_for_net_units_flag department_head_sales_for_net_units_flag,
		#dim_employee_afterAggregation.assistant_department_head_sales_for_net_units_flag assistant_department_head_sales_for_net_units_flag,
        #dim_employee_afterAggregation.employee_active_flag employee_active_flag,
        #dim_employee_afterAggregation.employee_name employee_name,
		#dim_employee_afterAggregation.employee_name_last_first employee_name_last_first,
		#dim_employee_afterAggregation.inserted_date_time_employee inserted_date_time_employee,
        #dim_employee_afterAggregation.dv_load_date_time dv_load_date_time,
		#dim_employee_afterAggregation.dv_batch_id dv_batch_id,
		#dim_employee_afterAggregation.dv_load_end_date_time dv_load_end_date_time
from #dim_employee_afterAggregation

),


 crm_employee (dim_crm_system_user_key,dim_employee_key,employee_id, full_name,
          internal_email_address, is_disabled, is_disabled_name, job_title, ltf_club_id,
          ltf_club_id_name,modified_by_name, modified_on, overridden_created_on,
		  queue_id,queue_id_name,salutation,system_user_id,title,updated_date_time,utc_conversion_time_zone_code,
          dv_load_date_time,dv_batch_id,dv_load_end_date_time) as
(


select crm.dim_crm_system_user_key dim_crm_system_user_key,
       crm.dim_mms_employee_key dim_employee_key,
	   crm.employee_id employee_id,
	   crm.full_name full_name,
	   crm.internal_email_address internal_email_address,
	   crm.is_disabled is_disabled,
	   crm.is_disabled_name is_disabled_name,
	   crm.job_title job_title,
	   crm.ltf_club_id ltf_club_id,
	   crm.ltf_club_id_name ltf_club_id_name,
	   crm.modified_by_name modified_by_name,
	   crm.modified_on modified_on,
	   crm.overridden_created_on overridden_created_on,
	   crm.queue_id queue_id,
	   crm.queue_id_name queue_id_name,
	   crm.salutation salutation,
	   crm.system_user_id system_user_id,
	   crm.title title,
	   crm.updated_date_time updated_date_time,
	   crm.utc_conversion_time_zone_code utc_conversion_time_zone_code,
	   crm.dv_load_date_time dv_load_date_time,
	   crm.dv_batch_id dv_batch_id,
	   crm.dv_load_end_date_time dv_load_end_date_time
from d_crmcloudsync_system_user crm
where isnumeric(crm.employee_id)=1
and isnull(crm.is_disabled,0)=0
),


workday_employee (dim_employee_key,employee_id, active_status, category, mms_club_id,manager_id,
          manager_dim_employee_key, hire_date,first_name, last_name,preferred_first_name, preferred_last_name, preferred_middle_name,
          primary_work_email, subordinates, termination_date,phone_number,cf_nickname,cf_employment_status,is_primary,pay_rate_for_all_positions,
		  workday_club_id,dv_load_date_time,dv_batch_id,dv_load_end_date_time) as

(select d_workday_employee.dim_employee_key dim_employee_key,
       d_workday_employee.employee_id employee_id,
	   d_workday_employee.active_status active_status,
	   d_workday_employee.category category,
	   d_workday_employee.mms_club_id mms_club_id,
	   d_workday_employee.manager_id manager_id,
	   d_workday_employee.manager_dim_employee_key manager_dim_employee_key,
	   d_workday_employee.hire_date hire_date,
       d_workday_employee.first_name first_name,
	   d_workday_employee.last_name last_name,
	   d_workday_employee.preferred_first_name preferred_first_name,
	   d_workday_employee.preferred_last_name preferred_last_name,
	   d_workday_employee.preferred_middle_name preferred_middle_name,
	   d_workday_employee.primary_work_email primary_work_email,
	   d_workday_employee.subordinates subordinates,
	   d_workday_employee.termination_date termination_date,
	   d_workday_employee.phone_number phone_number,
	   d_workday_employee.cf_nickname cf_nickname,
	   d_workday_employee.cf_employment_status cf_employment_status,
	   d_workday_employee.is_primary is_primary,
	   d_workday_employee.pay_rate_for_all_positions pay_rate_for_all_positions,
	   d_workday_employee.workday_club_id workday_club_id,
   	   d_workday_employee.dv_load_date_time dv_load_date_time,
	   d_workday_employee.dv_batch_id  dv_batch_id,
	   d_workday_employee.dv_load_end_date_time dv_load_end_date_time
	   from d_workday_employee d_workday_employee
),

all_employee (dim_employee_key,employee_id) as
(
    select dim_employee_key,employee_id from mms_employee
    union
    select dim_employee_key,employee_id from crm_employee
    union
    select dim_employee_key,employee_id from workday_employee
)

select all_employee.dim_employee_key dim_employee_key,
       all_employee.employee_id employee_id,
	   mms_employee.dim_club_key dim_club_key,
	   mms_employee.member_id member_id,
	   coalesce(mms_employee.first_name,workday_employee.first_name) first_name,
	   coalesce(mms_employee.last_name,workday_employee.last_name) last_name,
	   mms_employee.membership_sales_manager_ind membership_sales_manager_ind,
	   mms_employee.membership_sales_group_ind membership_sales_group_ind,
	   mms_employee.department_head_sales_for_net_units_flag department_head_sales_for_net_units_flag,
	   mms_employee.assistant_department_head_sales_for_net_units_flag assistant_department_head_sales_for_net_units_flag,
	   mms_employee.employee_active_flag employee_active_flag,
	   mms_employee.employee_name employee_name,

/*Commented below statement and added the case statement as part of UDW-10104*/
/*	   mms_employee.employee_name_last_first employee_name_last_first,*/
	   case when mms_employee.employee_name_last_first is not null and (coalesce(mms_employee.last_name,workday_employee.last_name) is not null or coalesce(mms_employee.first_name,workday_employee.first_name) is not null)
				then mms_employee.employee_name_last_first
			when coalesce(mms_employee.last_name,workday_employee.last_name) is null or coalesce(mms_employee.first_name,workday_employee.first_name) is null 
				then replace(full_name, ' ', ', ')
			else coalesce(mms_employee.last_name,workday_employee.last_name) +', '+coalesce(mms_employee.first_name,workday_employee.first_name)
	   end employee_name_last_first,

	   mms_employee.inserted_date_time_employee inserted_date_time_employee,
	   crm_employee.dim_crm_system_user_key dim_crm_system_user_key,
	   crm_employee.full_name full_name,
	   crm_employee.internal_email_address internal_email_address,
	   crm_employee.is_disabled is_disabled,
	   crm_employee.is_disabled_name is_disabled_name,
	   crm_employee.job_title job_title,
	   crm_employee.ltf_club_id ltf_club_id,
       crm_employee.ltf_club_id_name,
	   crm_employee.modified_by_name modified_by_name,
	   crm_employee.modified_on modified_on,
	   crm_employee.overridden_created_on overridden_created_on,
	   crm_employee.queue_id queue_id,
	   crm_employee.queue_id_name queue_id_name,
	   crm_employee.salutation salutation,
	   crm_employee.system_user_id system_user_id,
	   crm_employee.title title,
	   crm_employee.updated_date_time updated_date_time,
	   crm_employee.utc_conversion_time_zone_code utc_conversion_time_zone_code,
	   workday_employee.active_status active_status,
	   workday_employee.category category,
	   workday_employee.manager_dim_employee_key manager_dim_employee_key,
	   workday_employee.manager_id manager_id,
	   workday_employee.hire_date hire_date,
	   workday_employee.preferred_first_name preferred_first_name,
	   workday_employee.preferred_last_name preferred_last_name,
	   workday_employee.preferred_middle_name preferred_middle_name,
	   workday_employee.primary_work_email primary_work_email,
	   workday_employee.subordinates subordinates,
	   workday_employee.termination_date termination_date,
	   workday_employee.phone_number phone_number,
	   workday_employee.cf_nickname cf_nickname,
	   workday_employee.cf_employment_status cf_employment_status,
	   workday_employee.is_primary is_primary,
	   workday_employee.pay_rate_for_all_positions pay_rate_for_all_positions,
	   workday_employee.workday_club_id workday_club_id,
       case when mms_employee.dv_load_date_time >= isnull(crm_employee.dv_load_date_time,'Jan 1, 1753')
             and mms_employee.dv_load_date_time >= isnull(workday_employee.dv_load_date_time,'Jan 1, 1753')
            then mms_employee.dv_load_date_time
            when crm_employee.dv_load_date_time >= isnull(workday_employee.dv_load_date_time,'Jan 1, 1753')
            then crm_employee.dv_load_date_time
            else isnull(workday_employee.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
       case when mms_employee.dv_batch_id >= isnull(crm_employee.dv_batch_id,-1)
             and mms_employee.dv_batch_id >= isnull(workday_employee.dv_batch_id,-1)
            then mms_employee.dv_batch_id
            when crm_employee.dv_batch_id >= isnull(workday_employee.dv_batch_id,-1)
            then crm_employee.dv_batch_id
            else isnull(workday_employee.dv_batch_id,-1) end dv_batch_id
from all_employee
left join mms_employee on all_employee.dim_employee_key = mms_employee.dim_employee_key
left join crm_employee on all_employee.dim_employee_key = crm_employee.dim_employee_key
left join workday_employee on all_employee.dim_employee_key = workday_employee.dim_employee_key







declare @dv_inserted_date_time datetime = getdate()
declare @dv_insert_user varchar(50) = suser_sname()

begin tran


  insert into dim_employee
    (
	 dim_employee_key,
	 employee_id,
	 dim_club_key,
     member_id,
     first_name,
     last_name,
     membership_sales_manager_ind,
     membership_sales_group_ind,
     employee_active_flag,
     department_head_sales_for_net_units_flag,
     assistant_department_head_sales_for_net_units_flag,
	 employee_name,
	 employee_name_last_first,
	 inserted_date_time,
	 dim_crm_system_user_key,
	 full_name,
     system_user_id,
     internal_email_address,
     is_disabled,
     is_disabled_name,
     job_title,
     ltf_club_id,
     ltf_club_id_name,
     modified_by_name,
     modified_on,
     overridden_created_on,
     queue_id,
     queue_id_name,
     salutation,
     title,
     updated_date_time,
     utc_conversion_time_zone_code,
	 active_status,
	 category,
	 manager_id,
	 manager_dim_employee_key,
	 hire_date,
	 preferred_first_name,
	 preferred_last_name,
	 preferred_middle_name,
	 primary_work_email,
	 subordinates,
	 termination_date,
	 phone_number,
	 cf_nickname,
	 is_primary,
	 pay_rate_for_all_positions,
	 workday_club_id,
	 cf_employment_status,
	 dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
     )
  select

     dim_employee_key,
	 employee_id,
	 dim_club_key,
     member_id,
     first_name,
     last_name,
     membership_sales_manager_ind,
     membership_sales_group_ind,
     employee_active_flag,
     department_head_sales_for_net_units_flag,
     assistant_department_head_sales_for_net_units_flag,
	 employee_name,
	 employee_name_last_first,
	 inserted_date_time_employee,
	 dim_crm_system_user_key,
	 full_name,
     system_user_id,
     internal_email_address,
     is_disabled,
     is_disabled_name,
     job_title,
     ltf_club_id,
     ltf_club_id_name,
     modified_by_name,
     modified_on,
     overridden_created_on,
     queue_id,
     queue_id_name,
     salutation,
     title,
     updated_date_time,
     utc_conversion_time_zone_code,
	 active_status,
	 category,
	 manager_id,
	 manager_dim_employee_key,
	 hire_date,
	 preferred_first_name,
	 preferred_last_name,
	 preferred_middle_name,
	 primary_work_email,
	 subordinates,
	 termination_date,
	 phone_number,
	 cf_nickname,
	 is_primary,
	 pay_rate_for_all_positions,
	 workday_club_id,
	 cf_employment_status,
	 dv_load_date_time,
	 'dec 31, 9999' as dv_load_end_date_time,
     dv_batch_id,
     @dv_inserted_date_time,
     @dv_insert_user

 from #dim_employee

commit tran

exec proc_dim_employee_certification
exec proc_dim_employee_job_title
end
