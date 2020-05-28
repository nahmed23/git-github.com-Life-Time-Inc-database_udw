CREATE PROC [dbo].[proc_fact_mms_kids_play_usage] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_mms_kids_play_usage)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#fact_mms_kids_play_usage') is not null drop table #fact_mms_kids_play_usage
create table dbo.#fact_mms_kids_play_usage with(distribution=hash(fact_mms_kids_play_usage_key), location=user_db, heap) as


select d_mms_kids_play_check_in.fact_mms_kids_play_check_in_key fact_mms_kids_play_usage_key,  --generate the key in d_mms_pricing_discount
     d_mms_kids_play_check_in.kids_play_check_in_id,
     case when d_mms_kids_play_check_in.fact_mms_kids_play_check_in_key in ('-997','-998','-999') 
          then d_mms_kids_play_check_in.fact_mms_kids_play_check_in_key
          else isnull(fact_mms_child_center_usage.dim_club_key,'-998')
     end dim_club_key, 
     isnull(d_mms_kids_play_check_in.check_in_dim_date_key,'-998') check_in_dim_date_key,
     isnull(d_mms_kids_play_check_in.check_in_dim_time_key,'-998') check_in_dim_time_key,   
	 isnull(fact_mms_child_center_usage.child_dim_mms_member_key,'-998') child_dim_mms_member_key,
     isnull(fact_mms_child_center_usage.child_gender_abbreviation,'U') child_gender_abbreviation,
     isnull(fact_mms_child_center_usage.child_age_months,0) child_age_months,
     isnull(fact_mms_child_center_usage.child_age_years,0) child_age_years,      
     case when d_mms_kids_play_check_in.dv_load_date_time >= isnull(fact_mms_child_center_usage.dv_load_date_time,'jan 1, 1753')
          then d_mms_kids_play_check_in.dv_load_date_time
          else fact_mms_child_center_usage.dv_load_date_time
     end dv_load_date_time,
     'Dec 31, 9999' dv_load_end_date_time,
     case when d_mms_kids_play_check_in.dv_batch_id >= isnull(fact_mms_child_center_usage.dv_batch_id,-1)
          then d_mms_kids_play_check_in.dv_batch_id
          else fact_mms_child_center_usage.dv_batch_id
     end dv_batch_id,
     getdate() dv_inserted_date_time,
     suser_sname() dv_insert_user
from d_mms_kids_play_check_in d_mms_kids_play_check_in
left join fact_mms_child_center_usage fact_mms_child_center_usage
     on d_mms_kids_play_check_in.child_center_usage_id = fact_mms_child_center_usage.child_center_usage_id
where d_mms_kids_play_check_in.dv_batch_id >= @load_dv_batch_id
     or fact_mms_child_center_usage.dv_batch_id >= @load_dv_batch_id

	   
-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

delete dbo.fact_mms_kids_play_usage
     where fact_mms_kids_play_usage_key in (select fact_mms_kids_play_usage_key from dbo.#fact_mms_kids_play_usage) 

insert into fact_mms_kids_play_usage
   (	
     fact_mms_kids_play_usage_key,
     kids_play_check_in_id,
     dim_club_key,
     check_in_dim_date_key,
     check_in_dim_time_key,
     child_dim_mms_member_key,
     child_gender_abbreviation,
     child_age_months,
     child_age_years,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user   						
   )
 select fact_mms_kids_play_usage_key,
     kids_play_check_in_id,
     dim_club_key,
     check_in_dim_date_key,
     check_in_dim_time_key,
	 child_dim_mms_member_key,
     child_gender_abbreviation,
     child_age_months,
     child_age_years,
     dv_load_date_time,
     dv_load_end_date_time,
     dv_batch_id,
     dv_inserted_date_time,
     dv_insert_user
    from #fact_mms_kids_play_usage

commit tran

end
