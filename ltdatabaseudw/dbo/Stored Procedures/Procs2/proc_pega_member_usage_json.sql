CREATE PROC [dbo].[proc_pega_member_usage_json] @dv_batch_id [varchar](500),@start [int],@end [int] AS
begin

	set nocount on
	set xact_abort on

	declare @current_dv_batch_id bigint = @dv_batch_id
	declare @row_start int = @start
	declare @row_end int = @end

	if object_id('tempdb..#wrk_pega_member_usage') is not null drop table #wrk_pega_member_usage
	create table dbo.#wrk_pega_member_usage with (distribution = hash (dim_mms_member_key),location = user_db) as
	select 
		dim_mms_member_key,
		member_usage_id,
		member_id,
		check_in_date_time,
		club_id,
		sequence_number
	from 
		wrk_pega_member_usage
	where 
		dv_batch_id = @current_dv_batch_id     
		and sequence_number >= @row_start     
		and sequence_number < @row_end


	/* Generate the json*/
	select 
		case when ca_member_usage.sequence_number != @row_start then ',' else '' end
        + '{'
        + '"member_usage_id":' + isnull(convert(varchar(4000),ca_member_usage.member_usage_id),'null') + ','
        + '"member_id":' + isnull(convert(varchar(4000),ca_member_usage.member_id),'null') + ','
		+ '"check_in_date_time":' + isnull('"' + substring(convert(varchar(4000),ca_member_usage.check_in_date_time,120),1,16) + '"','null') + ','
        + '"club_id":' + isnull(convert(varchar(4000),ca_member_usage.club_id),'null') 
        + '}' json_output
	from 
		#wrk_pega_member_usage ca_member_usage
	order by 
		ca_member_usage.sequence_number

end
