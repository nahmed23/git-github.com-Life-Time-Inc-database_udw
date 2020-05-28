CREATE PROC [dbo].[proc_dim_mms_member_30_day_rejoin] AS
begin

set xact_abort on
set nocount on

 
if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(member_id), location=user_db) as
SELECT mms_member.dim_mms_member_key
    ,mms_member.member_id
	,mms_member.first_name
	,mms_member.last_name
	,mms_member.date_of_birth
	,mms_member.join_date
	,mdm_customer.email_1 email_address
	,mdm_customer.phone_1 phone_number
	,mms_member.member_active_flag
	,mms_member.description_member member_type 
	,mms_member.gender_abbreviation AS sex
    ,mdm_customer_id_list.entity_id
	FROM d_mms_member mms_member
JOIN dim_mdm_golden_record_customer_id_list mdm_customer_id_list 
     ON mms_member.member_id = mdm_customer_id_list.id
JOIN dim_mdm_golden_record_customer mdm_customer 
     ON mdm_customer.dim_mdm_golden_record_customer_key = mdm_customer_id_list.dim_mdm_golden_record_customer_id_list_key
WHERE join_date >= getdate() - 30
	AND mdm_customer_id_list.id_type = 1
	AND former_member_flag='Y';

if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution=hash(entity_id), location=user_db) as
SELECT mdm_customer_id_list.entity_id,mdm_customer_id_list.id member_id,d_mms_member.membership_id
,LAG(d_mms_member.membership_id,1) OVER (PARTITION BY mdm_customer_id_list.entity_id ORDER BY d_mms_member.join_date) AS previous_membership_id
,d_mms_member.join_date
,LAG(d_mms_member.join_date,1) OVER (PARTITION BY mdm_customer_id_list.entity_id ORDER BY d_mms_member.join_date) AS previous_join_date
	FROM dbo.#etl_step1 etl_step1
JOIN dim_mdm_golden_record_customer_id_list mdm_customer_id_list 
     ON etl_step1.entity_id = mdm_customer_id_list.entity_id
JOIN d_mms_member d_mms_member ON d_mms_member.member_id=mdm_customer_id_list.id
WHERE mdm_customer_id_list.id_type = 1 

if object_id('tempdb..#etl_step3') is not null drop table #etl_step3
create table dbo.#etl_step3 with(distribution=hash(member_id), location=user_db) as
select etl_step1.dim_mms_member_key
    ,etl_step1.member_id
    ,etl_step1.entity_id
	,etl_step1.first_name
	,etl_step1.last_name
	,etl_step1.date_of_birth
	,etl_step1.email_address
	,etl_step1.phone_number
	,etl_step1.member_active_flag
	,etl_step1.member_type 
	,etl_step1.sex
	,etl_step1.join_date
	,etl_step2.previous_join_date
    ,dim_mms_membership.termination_reason
	,dim_mms_membership.dv_load_date_time
	,dim_mms_membership.dv_load_end_date_time
	,dim_mms_membership.dv_batch_id
from dbo.#etl_step2  etl_step2
join dim_mms_membership dim_mms_membership
on etl_step2.previous_membership_id=dim_mms_membership.membership_id
join dbo.#etl_step1  etl_step1 on etl_step1.member_id=etl_step2.member_id
where etl_step2.join_date >= getdate()-30 and etl_step2.join_date <> etl_step2.previous_join_date

/* The above condition is because we have many duplicates in dim_mdm_golden_record_customer_id_list*/

truncate table dbo.dim_mms_member_30_day_rejoin;

begin tran

   insert into dim_mms_member_30_day_rejoin
        (dim_mms_member_key,
         member_id,
		 entity_id,
		 first_name,
		 last_name,
		 date_of_birth,
		 email_address,
	     phone_number,
		 member_active_flag,
	     member_type,
		 sex,
		 join_date,
		 previous_join_date,
		 termination_reason,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user)
  select dim_mms_member_key,
         member_id,
		 entity_id,
		 first_name,
		 last_name,
		 date_of_birth,
		 email_address,
	     phone_number,
		 member_active_flag,
	     member_type,
		 sex,
		 join_date,
		 previous_join_date,
		 termination_reason,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate() ,
         suser_sname()
    from #etl_step3

commit tran

end

