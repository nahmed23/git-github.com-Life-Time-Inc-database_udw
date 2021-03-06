﻿CREATE VIEW [marketing].[v_dim_employee]
AS select dim_employee.dim_employee_key dim_employee_key,
       dim_employee.employee_id employee_id,
       dim_employee.active_status active_status,
       dim_employee.assistant_department_head_sales_for_net_units_flag assistant_department_head_sales_for_net_units_flag,
       dim_employee.business_titles business_titles,
       dim_employee.category category,
       dim_employee.cf_employment_status cf_employment_status,
       dim_employee.cf_nickname cf_nickname,
       dim_employee.department_head_sales_for_net_units_flag department_head_sales_for_net_units_flag,
       dim_employee.dim_club_key dim_club_key,
       dim_employee.dim_crm_system_user_key dim_crm_system_user_key,
       dim_employee.employee_active_flag employee_active_flag,
       dim_employee.employee_name employee_name,
       dim_employee.employee_name_last_first employee_name_last_first,
       dim_employee.first_name first_name,
       dim_employee.full_name full_name,
       dim_employee.hire_date hire_date,
       dim_employee.inserted_date_time inserted_date_time,
       dim_employee.internal_email_address internal_email_address,
       dim_employee.is_disabled is_disabled,
       dim_employee.is_disabled_name is_disabled_name,
       dim_employee.is_primary is_primary,
       dim_employee.job_codes job_codes,
       dim_employee.job_families job_families,
       dim_employee.job_levels job_levels,
       dim_employee.job_profiles job_profiles,
       dim_employee.job_sub_families job_sub_families,
       dim_employee.job_title job_title,
       dim_employee.last_name last_name,
       dim_employee.ltf_club_id ltf_club_id,
       dim_employee.ltf_club_id_name ltf_club_id_name,
       dim_employee.manager_dim_employee_key manager_dim_employee_key,
       dim_employee.manager_id manager_id,
       dim_employee.marketing_titles marketing_titles,
       dim_employee.member_id member_id,
       dim_employee.membership_sales_group_ind membership_sales_group_ind,
       dim_employee.membership_sales_manager_ind membership_sales_manager_ind,
       dim_employee.middle_name middle_name,
       dim_employee.mms_club_id mms_club_id,
       dim_employee.modified_by_name modified_by_name,
       dim_employee.modified_on modified_on,
       dim_employee.overridden_created_on overridden_created_on,
       dim_employee.pay_rate_for_all_positions pay_rate_for_all_positions,
       dim_employee.phone_number phone_number,
       dim_employee.preferred_first_name preferred_first_name,
       dim_employee.preferred_last_name preferred_last_name,
       dim_employee.preferred_middle_name preferred_middle_name,
       dim_employee.primary_work_email primary_work_email,
       dim_employee.queue_id queue_id,
       dim_employee.queue_id_name queue_id_name,
       dim_employee.salutation salutation,
       dim_employee.subordinates subordinates,
       dim_employee.system_user_id system_user_id,
       dim_employee.termination_date termination_date,
       dim_employee.title title,
       dim_employee.updated_date_time updated_date_time,
       dim_employee.utc_conversion_time_zone_code utc_conversion_time_zone_code,
       dim_employee.workday_club_id workday_club_id
  from dbo.dim_employee;