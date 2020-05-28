CREATE PROC [dbo].[proc_fact_combined_member_spend] AS
begin

set xact_abort on
set nocount on


	
if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(dim_mms_member_key), location=user_db) as
	select dim_mms_member_key
		,dim_mms_membership_key
		,last_12_month_spend_amount
		,total_spend_amount
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
	from fact_magento_member_spend	
	union all	
	select dim_mms_member_key
		,dim_mms_membership_key
		,last_12_month_spend_amount
		,total_spend_amount
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
	from fact_mms_member_spend	
	union all	
	select dim_mms_member_key
		,dim_mms_membership_key
		,last_12_month_spend_amount
		,total_spend_amount
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
	from fact_spa_member_spend
   union all	
	select dim_mms_member_key
		,dim_mms_membership_key
		,last_12_month_spend_amount
		,total_spend_amount
		,dv_load_date_time
		,dv_load_end_date_time
		,dv_batch_id
	from fact_cafe_member_spend

	
if object_id('tempdb..#etl_step2') is not null drop table #etl_step2
create table dbo.#etl_step2 with(distribution=hash(dim_mms_member_key), location=user_db) as
select etl_step1.dim_mms_member_key dim_mms_member_key
       ,d_mms_member.dim_mms_membership_key dim_mms_membership_key
	   ,d_mms_member.first_name first_name
	   ,d_mms_member.last_name last_name
	   ,d_mms_membership.home_dim_club_key home_dim_club_key
	   ,dim_club.club_code home_club
       ,sum(last_12_month_spend_amount) last_12_month_spend_amount
       ,sum(total_spend_amount) total_spend_amount
       ,max(etl_step1.dv_load_date_time) dv_load_date_time
       ,max(etl_step1.dv_load_end_date_time) dv_load_end_date_time
       ,max(etl_step1.dv_batch_id) dv_batch_id
from  dbo.#etl_step1 etl_step1
join d_mms_member d_mms_member on etl_step1.dim_mms_member_key=d_mms_member.dim_mms_member_key
join d_mms_membership d_mms_membership on d_mms_membership.dim_mms_membership_key=d_mms_member.dim_mms_membership_key
join dim_club dim_club on dim_club.dim_club_key=d_mms_membership.home_dim_club_key
group by etl_step1.dim_mms_member_key
       ,d_mms_member.dim_mms_membership_key
	   ,d_mms_member.first_name
	   ,d_mms_member.last_name
	   ,d_mms_membership.home_dim_club_key
	   ,dim_club.club_code  
	
truncate table dbo.fact_combined_member_spend;

begin tran

   insert into fact_combined_member_spend
        (dim_mms_member_key,
         dim_mms_membership_key,
		 first_name,
		 last_name,
		 home_dim_club_key,
		 home_club,
	     last_12_month_spend_amount,
	     total_spend_amount,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user)
  select dim_mms_member_key,
         dim_mms_membership_key,
		 first_name,
		 last_name,
		 home_dim_club_key,
		 home_club,
	     last_12_month_spend_amount,
	     total_spend_amount,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate() ,
         suser_sname()
    from #etl_step2
 
commit tran

end

