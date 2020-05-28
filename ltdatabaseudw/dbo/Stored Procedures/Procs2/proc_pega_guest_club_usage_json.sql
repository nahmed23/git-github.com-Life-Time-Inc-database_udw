CREATE PROC [dbo].[proc_pega_guest_club_usage_json] @dv_batch_id [varchar](500),@start [int],@end [int] AS
begin

	set nocount on
	set xact_abort on

	declare @current_dv_batch_id bigint = @dv_batch_id
	declare @row_start int = @start
	declare @row_end int = @end

	/* Get the customer attributes*/
	/* replace any special characters in JSON with escape sequences*/
	if object_id('tempdb..#wrk_pega_guest_club_usage') is not null drop table #wrk_pega_guest_club_usage
	create table dbo.#wrk_pega_guest_club_usage with (distribution = hash (guest_of_member_id),location = user_db) as
	select 
		guest_visit_id,
		check_in_date_time,
		club_id,
        guest_privilege_rule_id,
        max_number_of_guests,
        guest_of_member_id ,
		membership_id,
		guest_id,
		sequence_number
	from 
		wrk_pega_guest_club_usage
	where 
		dv_batch_id = @current_dv_batch_id     
		and sequence_number >= @row_start     
		and sequence_number < @row_end


	/* Generate the json*/
	select 
		case when gcu_member.sequence_number != @row_start then ',' else '' end
        + '{'
        + '"guest_visit_id":'+ +isnull(convert(varchar(4000),guest_visit_id),'null')
	    +','+'"guest_id":'+ isnull(convert(varchar(4000),guest_id),'null')
        +','+'"club_id":'+isnull(convert(varchar(4000),club_id),'null')
        +','+'"membership_id":'+ isnull(convert(varchar(4000),membership_id),'null')
	    +','+'"guest_of_member_id":'+ isnull(convert(varchar(4000),guest_of_member_id),'null')
        +','+'"check_in_date_time":'+ isnull('"' + convert(varchar(4000),check_in_date_time) + '"','null')
	    +','+'"guest_privilege_rule_id":'+ isnull(convert(varchar(4000),guest_privilege_rule_id),'null')
        +','+'"max_number_of_guests":'+ isnull(convert(varchar(4000),max_number_of_guests),'null')
        + '}' json_output
	from 
		#wrk_pega_guest_club_usage gcu_member
	order by 
		gcu_member.sequence_number

end
