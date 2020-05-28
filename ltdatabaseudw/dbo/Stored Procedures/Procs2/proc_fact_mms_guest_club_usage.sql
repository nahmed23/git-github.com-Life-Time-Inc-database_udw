CREATE PROC [dbo].[proc_fact_mms_guest_club_usage] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_guest_club_usage)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_guest_club_usage_key), location=user_db) as
select d_mms_guest_visit.bk_hash fact_mms_guest_club_usage_key,
	   d_dim_mms_club_guest.dim_club_guest_key dim_club_guest_key,
       d_mms_guest_visit.guest_visit_id,
       d_mms_guest_visit.guest_id,
       d_mms_guest_visit.dim_club_key,
	   d_mms_guest_visit.dim_mms_member_key guest_of_dim_mms_member_key,
       d_mms_guest_visit.visit_date_time,
       d_mms_guest_visit.check_in_dim_date_key,
       d_mms_guest_visit.check_in_dim_time_key,
	   dim_mms_membership.membership_id,
       dim_mms_membership.created_date_time,
	   isnull(convert(varchar, dim_mms_membership.created_date_time, 112),'-998') created_date_time_key,
	   dim_mms_membership_type.val_check_in_group_id membership_type_check_in_group_level ,
       d_mms_guest_visit.dv_batch_id,
       d_mms_guest_visit.dv_load_date_time,
	   'Dec 31, 9999' dv_load_end_date_time,
	   getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
  from d_mms_guest_visit d_mms_guest_visit
  LEFT JOIN d_mms_member d_mms_member
    ON d_mms_guest_visit.member_id = d_mms_member.member_id
	LEFT JOIN d_mms_guest d_dim_mms_club_guest
	ON d_mms_guest_visit.guest_id = d_dim_mms_club_guest.guest_id
  LEFT JOIN d_mms_membership dim_mms_membership
    ON dim_mms_membership.membership_id = d_mms_member.membership_id
 LEFT JOIN dim_mms_membership_type dim_mms_membership_type 
 ON dim_mms_membership.membership_type_id = dim_mms_membership_type.membership_type_id
 where d_mms_guest_visit.dv_batch_id >= @load_dv_batch_id


  
  
--#etl_step_2
if object_id('tempdb..#fact_mms_guest_club_usage') is not null drop table #fact_mms_guest_club_usage
create table dbo.#fact_mms_guest_club_usage with(distribution=hash(fact_mms_guest_club_usage_key), location=user_db) as
SELECT  #etl_step_1.fact_mms_guest_club_usage_key fact_mms_guest_club_usage_key,
        #etl_step_1.dim_club_guest_key dim_club_guest_key,
        #etl_step_1.dim_club_key  dim_club_key,
    	#etl_step_1.guest_of_dim_mms_member_key  guest_of_dim_mms_member_key,
        #etl_step_1.check_in_dim_date_key check_in_dim_date_key,
        #etl_step_1.check_in_dim_time_key check_in_dim_time_key,
        #etl_step_1.guest_visit_id guest_visit_id,
        #etl_step_1.membership_id membership_id,
	    d_mms_guest_privilege_rule.dim_mms_membership_guest_privilege_rule_key dim_mms_membership_guest_privilege_rule_key,
		#etl_step_1.dv_batch_id,
		#etl_step_1.dv_load_date_time,
		#etl_step_1.dv_load_end_date_time,
		#etl_step_1.dv_inserted_date_time,
		#etl_step_1.dv_insert_user
    FROM #etl_step_1
	LEFT JOIN d_mms_guest_privilege_rule
	ON #etl_step_1.created_date_time_key >= d_mms_guest_privilege_rule.earliest_membership_created_dim_date_key
	AND #etl_step_1.created_date_time_key <= d_mms_guest_privilege_rule.latest_membership_created_dim_date_key
	AND #etl_step_1.membership_type_check_in_group_level >= d_mms_guest_privilege_rule.low_membership_type_check_in_group_level
	AND #etl_step_1.membership_type_check_in_group_level <= d_mms_guest_privilege_rule.high_membership_type_check_in_group_level


		-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.fact_mms_guest_club_usage
   where fact_mms_guest_club_usage_key in (select fact_mms_guest_club_usage_key from dbo.#fact_mms_guest_club_usage) 

  insert into fact_mms_guest_club_usage
    (fact_mms_guest_club_usage_key,
     dim_club_guest_key,
	 dim_club_key,
	 guest_of_dim_mms_member_key,
	 check_in_dim_date_key,
	 check_in_dim_time_key,
	 guest_visit_id,
	 membership_id,
	 dim_mms_membership_guest_privilege_rule_key,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
     )
  select fact_mms_guest_club_usage_key,
     dim_club_guest_key,
	 dim_club_key,
	 guest_of_dim_mms_member_key,
	 check_in_dim_date_key,
	 check_in_dim_time_key,
	 guest_visit_id,
	 membership_id,
	 dim_mms_membership_guest_privilege_rule_key,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
    from #fact_mms_guest_club_usage

commit tran

end
