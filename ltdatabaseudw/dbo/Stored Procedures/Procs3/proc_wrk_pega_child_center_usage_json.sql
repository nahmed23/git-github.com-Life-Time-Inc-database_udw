CREATE PROC [proc_wrk_pega_child_center_usage_json] @dv_batch_id [bigint],@start [int],@end [int] AS
begin

set nocount on
  set xact_abort on

  declare @row_start int = @start
  declare @row_end int = @end
  
if object_id('tempdb..#wrk_pega_child_center_usage') is not null drop table #wrk_pega_child_center_usage
  create table dbo.#wrk_pega_child_center_usage with (distribution = hash (fact_mms_child_center_usage_key),location = user_db) as
  select 
		fact_mms_child_center_usage_key
		,cast(child_center_usage_id as varchar) as child_center_usage_id
		,cast(check_in_member_id as varchar) as check_in_member_id
		,check_in_date
		,check_in_time
		,cast(check_out_member_id as varchar) as check_out_member_id
		,check_out_date
		,check_out_time
		,cast(child_age_months as varchar) as child_age_months
		,cast(child_member_id as varchar) as child_member_id
		,cast(club_id as varchar) as club_id
		,cast(membership_id as varchar) as membership_id
		,Row_number() over(order by fact_mms_child_center_usage_key) as rnk_child_center_usage_id
		from 
		wrk_pega_child_center_usage
		where dv_batch_id = @dv_batch_id
			and sequence_number >= @start
			and sequence_number < @end

if object_id('tempdb..#wrk_pega_child_center_usage_json') is not null drop table #wrk_pega_child_center_usage_json
  create table dbo.#wrk_pega_child_center_usage_json with (distribution = hash (fact_mms_child_center_usage_key),location = user_db) as
  select 
		fact_mms_child_center_usage_key
		,child_center_usage_id
		,check_in_member_id
		,check_in_date
		,check_in_time
		,check_out_member_id
		,check_out_date
		,check_out_time
		,child_age_months
		,child_member_id
		,club_id
		,membership_id
		,rnk_child_center_usage_id
		,Row_number() over(order by fact_mms_child_center_usage_key) as rnk_fact_mms_child_center_usage_key
		,case when rnk_child_center_usage_id=1 then'{' else ',{' end
		+'"child_center_usage_id":'+child_center_usage_id
		+',"check_in_member_id":'+check_in_member_id
		+',"check_in_date_time":"'+CONVERT(varchar, check_in_date, 23)+' '+check_in_time
		+'","check_out_member_id":'+check_out_member_id
		+',"check_out_date_time":"'+CONVERT(varchar, check_out_date, 23)+' '+check_out_time
		+'","child_age_months":'+child_age_months
		+',"child_member_id":'+child_member_id
		+',"club_id":'+club_id
		+',"membership_id":'+membership_id as json_child_center_usage
		from 
		#wrk_pega_child_center_usage

		
   if object_id('tempdb..#wrk_pega_child_center_usage_activity_area') is not null drop table #wrk_pega_child_center_usage_activity_area
  create table dbo.#wrk_pega_child_center_usage_activity_area with (distribution = hash (fact_mms_child_center_usage_key),location = user_db) as
  select distinct fact_mms_child_center_usage_key,
         val_activity_area_id,
         ltrim(rtrim(description)) as description
    from wrk_pega_child_center_usage_activity_area usage_activity_area
	where  dv_batch_id = @dv_batch_id
   
  if object_id('tempdb..#wrk_pega_child_center_usage_activity_area_json') is not null drop table #wrk_pega_child_center_usage_activity_area_json
  create table dbo.#wrk_pega_child_center_usage_activity_area_json with (distribution = hash (fact_mms_child_center_usage_key),location = user_db) as
  select fact_mms_child_center_usage_key,
/*         ',"activity_area_details":['*/
 /*      + */
	   string_agg('{'
                  + '"val_activity_area_id":' + isnull(convert(varchar(4000),usage_activity_area.val_activity_area_id),'null') + ','
                  + '"description":"' + isnull(replace(replace(replace(replace(replace(replace(usage_activity_area.description,'\','\\'),'/','\/'),'"','\"'),char(13),'\r'),char(10),'\n'),char(9),'\t'),'null')
                  + '"}'
                    , ',') within group (order by usage_activity_area.val_activity_area_id asc)
--       + '] ' 
	   as usage_activity_area_list
    from #wrk_pega_child_center_usage_activity_area usage_activity_area
   group by fact_mms_child_center_usage_key

        

select json_child_center_usage+',"activity_area_details":['+
isnull(CCAAJ.usage_activity_area_list,'null')+'] }' as JSON_OUTPUT
from 
#wrk_pega_child_center_usage_json CCUJ 
LEFT JOIN
#wrk_pega_child_center_usage_activity_area_json CCAAJ
on CCUJ.fact_mms_child_center_usage_key=CCAAJ.fact_mms_child_center_usage_key
 order by CCUJ.rnk_child_center_usage_id


end
