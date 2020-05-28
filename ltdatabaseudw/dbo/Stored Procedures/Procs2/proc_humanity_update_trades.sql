CREATE PROC [dbo].[proc_humanity_update_trades] AS
begin

--exec proc_humanity_update_trades 
--select * from dbo.fact_humanity_trades
--select top 1 * from stage_humanity_trades

truncate table dbo.fact_humanity_trades
insert into dbo.fact_humanity_trades
(
    [fact_humanity_trades_key],
    [shift_id] ,
	[trade_reason], 
	[trade_requested_datetime_utc] ,
	[trade_status] ,
	[swap] ,
	[shift_start_datetime_utc] ,
	[shift_end_datetime_utc] ,
	[hours] ,
	[shift_type] ,
	[position_id] ,
	[workday_position_id] ,
	[company_id] ,
	[position_name] ,
	[location_id] ,
	[location_name] ,
	[company_name] ,
	[trade_requested_employee_id] ,
	[trade_requested_employee_eid] ,
	[trade_requested_employee_name] ,
	[traded_to_employee_id] ,
	[traded_to_employee_eid] ,
	[traded_to_employee_name] ,
	[file_arrive_date] ,	
	[deleted_flag] ,
	dv_load_date_time,
	dv_load_end_date_time,
	dv_batch_id,
	dv_inserted_date_time,
	dv_insert_user,
	dv_updated_date_time,
	dv_update_user
	 
)
select 
    [bk_hash],
    [shift_id] ,
	[trade_reason], 
	[trade_requested_datetime_utc] ,
	[trade_status] ,
	[swap] ,
	[shift_start_datetime_utc] ,
	[shift_end_datetime_utc] ,
	[hours] ,
	[shift_type] ,
	[position_id] ,
	[workday_position_id] ,
	[company_id] ,
	[position_name] ,
	[location_id] ,
	[location_name] ,
	[company_name] ,
	[trade_requested_employee_id] ,
	[trade_requested_employee_eid] ,
	[trade_requested_employee_name] ,
	[traded_to_employee_id] ,
	[traded_to_employee_eid] ,
	[traded_to_employee_name] ,
	[file_arrive_date] ,	
	[deleted_flag] ,
	dv_load_date_time,
	dv_load_end_date_time,
	dv_batch_id,
	dv_inserted_date_time,
	dv_insert_user,
	dv_updated_date_time,
	dv_update_user
from dbo.d_humanity_trades
Where 
file_arrive_date=(select max(file_arrive_date) 
from dbo.d_humanity_trades where bk_hash not in ('-997', '-998','-999'))
and bk_hash not in ('-997', '-998','-999')


end


