CREATE PROC [dbo].[proc_fact_mms_child_center_usage_activity_area] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_child_center_usage_activity_area)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_mms_child_center_usage_activity_area_key), location=user_db) as 
select d_mms_child_center_usage_activity_area.fact_mms_child_center_usage_activity_area_key fact_mms_child_center_usage_activity_area_key,
       d_mms_child_center_usage_activity_area.fact_mms_child_center_usage_key fact_mms_child_center_usage_key,
	   d_mms_child_center_usage_activity_area.child_center_usage_activity_area_id child_center_usage_activity_area_id,
	   d_mms_child_center_usage_activity_area.activity_area_dim_description_key activity_area_dim_description_key,
	   fact_mms_child_center_usage.check_in_dim_mms_member_key check_in_dim_mms_member_key,
	   fact_mms_child_center_usage.check_in_dim_date_key check_in_dim_date_key,
	   fact_mms_child_center_usage.check_in_dim_time_key check_in_dim_time_key,
	   fact_mms_child_center_usage.check_out_dim_mms_member_key check_out_dim_mms_member_key,
	   fact_mms_child_center_usage.check_out_dim_date_key check_out_dim_date_key,
	   fact_mms_child_center_usage.check_out_dim_time_key check_out_dim_time_key,
	   fact_mms_child_center_usage.dim_club_key dim_club_key,
	   fact_mms_child_center_usage.dim_mms_membership_key dim_mms_membership_key,
	   fact_mms_child_center_usage.length_of_stay_minutes length_of_stay_minutes,
	   case when d_mms_child_center_usage_activity_area.dv_load_date_time > fact_mms_child_center_usage.dv_load_date_time
	   then d_mms_child_center_usage_activity_area.dv_load_date_time
	   else fact_mms_child_center_usage.dv_load_date_time
	   end dv_load_date_time,
	   case when d_mms_child_center_usage_activity_area.dv_batch_id > fact_mms_child_center_usage.dv_batch_id
	   then d_mms_child_center_usage_activity_area.dv_batch_id
	   else fact_mms_child_center_usage.dv_batch_id
	   end dv_batch_id
	   from d_mms_child_center_usage_activity_area
       join fact_mms_child_center_usage
       on d_mms_child_center_usage_activity_area.fact_mms_child_center_usage_key = fact_mms_child_center_usage.fact_mms_child_center_usage_key
       where d_mms_child_center_usage_activity_area.dv_batch_id >= @load_dv_batch_id
       or fact_mms_child_center_usage.dv_batch_id >= @load_dv_batch_id



 -- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

  begin tran

  delete dbo.fact_mms_child_center_usage_activity_area
   where fact_mms_child_center_usage_activity_area_key in (select fact_mms_child_center_usage_activity_area_key from dbo.#etl_step_1) 

insert into   fact_mms_child_center_usage_activity_area
      (
	   fact_mms_child_center_usage_activity_area_key,
	   fact_mms_child_center_usage_key,
	   child_center_usage_activity_area_id,
	   activity_area_dim_description_key,
	   check_in_dim_mms_member_key,
	   check_in_dim_date_key,
	   check_in_dim_time_key,
	   check_out_dim_mms_member_key,
	   check_out_dim_date_key,
	   check_out_dim_time_key,
	   dim_club_key,
	   dim_mms_membership_key,
	   length_of_stay_minutes,
	   dv_batch_id,
	   dv_load_date_time,
       dv_load_end_date_time,
	   dv_inserted_date_time,
       dv_insert_user
	   
	  )
	  
 select fact_mms_child_center_usage_activity_area_key,
	   fact_mms_child_center_usage_key,
	   child_center_usage_activity_area_id,
	   activity_area_dim_description_key,
	   check_in_dim_mms_member_key,
	   check_in_dim_date_key,
	   check_in_dim_time_key,
	   check_out_dim_mms_member_key,
	   check_out_dim_date_key,
	   check_out_dim_time_key,
	   dim_club_key,
	   dim_mms_membership_key,
	   length_of_stay_minutes,
	   dv_batch_id,
	   dv_load_date_time,
	   'dec 31, 9999',
		getdate() ,
        suser_sname()
 
	   
from #etl_step_1
 
	   
 commit tran
	end

