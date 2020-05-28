CREATE PROC [dbo].[proc_etl_humanity_trades] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_humanity_trades

set @insert_date_time = getdate()
insert into dbo.stage_hash_humanity_trades (
       bk_hash,
       shift_id,
       company_id,
       company_name,
       trade_requested_employee_id,
       trade_requested_employee_eid,
       trade_requested_employee_name,
       traded_to_employee_id,
       traded_to_employee_eid,
       traded_to_employee_name,
       trade_reason,
       trade_requested_datetime_utc,
       trade_status,
       swap,
       shift_start_datetime_utc,
       shift_end_datetime_utc,
       hours,
       shift_type,
       position_id,
       workday_position_id,
       position_name,
       location_id,
       location_name,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(shift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(company_id,'z#@$k%&P')+'P%#&z$@k'+isnull(company_name,'z#@$k%&P')+'P%#&z$@k'+isnull(trade_requested_employee_id,'z#@$k%&P')+'P%#&z$@k'+isnull(trade_requested_employee_eid,'z#@$k%&P')+'P%#&z$@k'+isnull(trade_requested_employee_name,'z#@$k%&P')+'P%#&z$@k'+isnull(traded_to_employee_id,'z#@$k%&P')+'P%#&z$@k'+isnull(traded_to_employee_eid,'z#@$k%&P')+'P%#&z$@k'+isnull(traded_to_employee_name	,'z#@$k%&P')+'P%#&z$@k'+isnull(trade_reason,'z#@$k%&P')+'P%#&z$@k'+isnull(trade_requested_datetime_utc,'z#@$k%&P')+'P%#&z$@k'+isnull(trade_status,'z#@$k%&P')+'P%#&z$@k'+isnull(swap,'z#@$k%&P')+'P%#&z$@k'+isnull(shift_start_datetime_utc,'z#@$k%&P')+'P%#&z$@k'+isnull(shift_end_datetime_utc,'z#@$k%&P')+'P%#&z$@k'+isnull(hours,'z#@$k%&P')+'P%#&z$@k'+isnull(shift_type,'z#@$k%&P')+'P%#&z$@k'+isnull(position_id,'z#@$k%&P')+'P%#&z$@k'+isnull(workday_position_id,'z#@$k%&P')+'P%#&z$@k'+isnull(position_name,'z#@$k%&P')+'P%#&z$@k'+isnull(location_id,'z#@$k%&P')+'P%#&z$@k'+isnull(location_name,'z#@$k%&P')+'P%#&z$@k'+isnull(ltf_file_name,'z#@$k%&P'))),2) bk_hash,
       shift_id,
       company_id,
       company_name,
       trade_requested_employee_id,
       trade_requested_employee_eid,
       trade_requested_employee_name,
       traded_to_employee_id,
       traded_to_employee_eid,
       traded_to_employee_name,
       trade_reason,
       trade_requested_datetime_utc,
       trade_status,
       swap,
       shift_start_datetime_utc,
       shift_end_datetime_utc,
       hours,
       shift_type,
       position_id,
       workday_position_id,
       position_name,
       location_id,
       location_name,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       isnull(cast(stage_humanity_trades.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_humanity_trades
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_humanity_trades @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_humanity_trades (
       bk_hash,
       shift_id,
       company_id,
       company_name,
       trade_requested_employee_id,
       trade_requested_employee_eid,
       trade_requested_employee_name,
       traded_to_employee_id,
       traded_to_employee_eid,
       traded_to_employee_name,
       trade_reason,
       trade_requested_datetime_utc,
       trade_status,
       swap,
       shift_start_datetime_utc,
       shift_end_datetime_utc,
       hours,
       shift_type,
       position_id,
       workday_position_id,
       position_name,
       location_id,
       location_name,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_humanity_trades.bk_hash,
       stage_hash_humanity_trades.shift_id shift_id,
       stage_hash_humanity_trades.company_id company_id,
       stage_hash_humanity_trades.company_name company_name,
       stage_hash_humanity_trades.trade_requested_employee_id trade_requested_employee_id,
       stage_hash_humanity_trades.trade_requested_employee_eid trade_requested_employee_eid,
       stage_hash_humanity_trades.trade_requested_employee_name trade_requested_employee_name,
       stage_hash_humanity_trades.traded_to_employee_id traded_to_employee_id,
       stage_hash_humanity_trades.traded_to_employee_eid traded_to_employee_eid,
       stage_hash_humanity_trades.traded_to_employee_name	 traded_to_employee_name,
       stage_hash_humanity_trades.trade_reason trade_reason,
       stage_hash_humanity_trades.trade_requested_datetime_utc trade_requested_datetime_utc,
       stage_hash_humanity_trades.trade_status trade_status,
       stage_hash_humanity_trades.swap swap,
       stage_hash_humanity_trades.shift_start_datetime_utc shift_start_datetime_utc,
       stage_hash_humanity_trades.shift_end_datetime_utc shift_end_datetime_utc,
       stage_hash_humanity_trades.hours hours,
       stage_hash_humanity_trades.shift_type shift_type,
       stage_hash_humanity_trades.position_id position_id,
       stage_hash_humanity_trades.workday_position_id workday_position_id,
       stage_hash_humanity_trades.position_name position_name,
       stage_hash_humanity_trades.location_id location_id,
       stage_hash_humanity_trades.location_name location_name,
       stage_hash_humanity_trades.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_humanity_trades.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       47,
       @insert_date_time,
       @user
  from stage_hash_humanity_trades
  left join h_humanity_trades
    on stage_hash_humanity_trades.bk_hash = h_humanity_trades.bk_hash
 where h_humanity_trades_id is null
   and stage_hash_humanity_trades.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_humanity_trades
if object_id('tempdb..#l_humanity_trades_inserts') is not null drop table #l_humanity_trades_inserts
create table #l_humanity_trades_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_trades.bk_hash,
       stage_hash_humanity_trades.shift_id shift_id,
       stage_hash_humanity_trades.company_id company_id,
       stage_hash_humanity_trades.company_name company_name,
       stage_hash_humanity_trades.trade_requested_employee_id trade_requested_employee_id,
       stage_hash_humanity_trades.trade_requested_employee_eid trade_requested_employee_eid,
       stage_hash_humanity_trades.trade_requested_employee_name trade_requested_employee_name,
       stage_hash_humanity_trades.traded_to_employee_id traded_to_employee_id,
       stage_hash_humanity_trades.traded_to_employee_eid traded_to_employee_eid,
       stage_hash_humanity_trades.traded_to_employee_name	 traded_to_employee_name,
       stage_hash_humanity_trades.trade_reason trade_reason,
       stage_hash_humanity_trades.trade_requested_datetime_utc trade_requested_datetime_utc,
       stage_hash_humanity_trades.trade_status trade_status,
       stage_hash_humanity_trades.swap swap,
       stage_hash_humanity_trades.shift_start_datetime_utc shift_start_datetime_utc,
       stage_hash_humanity_trades.shift_end_datetime_utc shift_end_datetime_utc,
       stage_hash_humanity_trades.hours hours,
       stage_hash_humanity_trades.shift_type shift_type,
       stage_hash_humanity_trades.position_id position_id,
       stage_hash_humanity_trades.workday_position_id workday_position_id,
       stage_hash_humanity_trades.position_name position_name,
       stage_hash_humanity_trades.location_id location_id,
       stage_hash_humanity_trades.location_name location_name,
       stage_hash_humanity_trades.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_humanity_trades.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_trades.shift_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.company_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_requested_employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_requested_employee_eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_requested_employee_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.traded_to_employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.traded_to_employee_eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.traded_to_employee_name	,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_reason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_requested_datetime_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.swap,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.shift_start_datetime_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.shift_end_datetime_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.hours,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.shift_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.workday_position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.position_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.location_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.location_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.ltf_file_name,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_trades
 where stage_hash_humanity_trades.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_humanity_trades records
set @insert_date_time = getdate()
insert into l_humanity_trades (
       bk_hash,
       shift_id,
       company_id,
       company_name,
       trade_requested_employee_id,
       trade_requested_employee_eid,
       trade_requested_employee_name,
       traded_to_employee_id,
       traded_to_employee_eid,
       traded_to_employee_name,
       trade_reason,
       trade_requested_datetime_utc,
       trade_status,
       swap,
       shift_start_datetime_utc,
       shift_end_datetime_utc,
       hours,
       shift_type,
       position_id,
       workday_position_id,
       position_name,
       location_id,
       location_name,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_humanity_trades_inserts.bk_hash,
       #l_humanity_trades_inserts.shift_id,
       #l_humanity_trades_inserts.company_id,
       #l_humanity_trades_inserts.company_name,
       #l_humanity_trades_inserts.trade_requested_employee_id,
       #l_humanity_trades_inserts.trade_requested_employee_eid,
       #l_humanity_trades_inserts.trade_requested_employee_name,
       #l_humanity_trades_inserts.traded_to_employee_id,
       #l_humanity_trades_inserts.traded_to_employee_eid,
       #l_humanity_trades_inserts.traded_to_employee_name,
       #l_humanity_trades_inserts.trade_reason,
       #l_humanity_trades_inserts.trade_requested_datetime_utc,
       #l_humanity_trades_inserts.trade_status,
       #l_humanity_trades_inserts.swap,
       #l_humanity_trades_inserts.shift_start_datetime_utc,
       #l_humanity_trades_inserts.shift_end_datetime_utc,
       #l_humanity_trades_inserts.hours,
       #l_humanity_trades_inserts.shift_type,
       #l_humanity_trades_inserts.position_id,
       #l_humanity_trades_inserts.workday_position_id,
       #l_humanity_trades_inserts.position_name,
       #l_humanity_trades_inserts.location_id,
       #l_humanity_trades_inserts.location_name,
       #l_humanity_trades_inserts.ltf_file_name,
       case when l_humanity_trades.l_humanity_trades_id is null then isnull(#l_humanity_trades_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #l_humanity_trades_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_humanity_trades_inserts
  left join p_humanity_trades
    on #l_humanity_trades_inserts.bk_hash = p_humanity_trades.bk_hash
   and p_humanity_trades.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_humanity_trades
    on p_humanity_trades.bk_hash = l_humanity_trades.bk_hash
   and p_humanity_trades.l_humanity_trades_id = l_humanity_trades.l_humanity_trades_id
 where l_humanity_trades.l_humanity_trades_id is null
    or (l_humanity_trades.l_humanity_trades_id is not null
        and l_humanity_trades.dv_hash <> #l_humanity_trades_inserts.source_hash)

--calculate hash and lookup to current s_humanity_trades
if object_id('tempdb..#s_humanity_trades_inserts') is not null drop table #s_humanity_trades_inserts
create table #s_humanity_trades_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_trades.bk_hash,
       stage_hash_humanity_trades.shift_id shift_id,
       stage_hash_humanity_trades.company_id company_id,
       stage_hash_humanity_trades.company_name company_name,
       stage_hash_humanity_trades.trade_requested_employee_id trade_requested_employee_id,
       stage_hash_humanity_trades.trade_requested_employee_eid trade_requested_employee_eid,
       stage_hash_humanity_trades.trade_requested_employee_name trade_requested_employee_name,
       stage_hash_humanity_trades.traded_to_employee_id traded_to_employee_id,
       stage_hash_humanity_trades.traded_to_employee_eid traded_to_employee_eid,
       stage_hash_humanity_trades.traded_to_employee_name	 traded_to_employee_name,
       stage_hash_humanity_trades.trade_reason trade_reason,
       stage_hash_humanity_trades.trade_requested_datetime_utc trade_requested_datetime_utc,
       stage_hash_humanity_trades.trade_status trade_status,
       stage_hash_humanity_trades.swap swap,
       stage_hash_humanity_trades.shift_start_datetime_utc shift_start_datetime_utc,
       stage_hash_humanity_trades.shift_end_datetime_utc shift_end_datetime_utc,
       stage_hash_humanity_trades.hours hours,
       stage_hash_humanity_trades.shift_type shift_type,
       stage_hash_humanity_trades.position_id position_id,
       stage_hash_humanity_trades.workday_position_id workday_position_id,
       stage_hash_humanity_trades.position_name position_name,
       stage_hash_humanity_trades.location_id location_id,
       stage_hash_humanity_trades.location_name location_name,
       stage_hash_humanity_trades.ltf_file_name ltf_file_name,
       stage_hash_humanity_trades.file_arrive_date file_arrive_date,
       stage_hash_humanity_trades.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_humanity_trades.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_trades.shift_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.company_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_requested_employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_requested_employee_eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_requested_employee_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.traded_to_employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.traded_to_employee_eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.traded_to_employee_name	,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_reason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_requested_datetime_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.trade_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.swap,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.shift_start_datetime_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.shift_end_datetime_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.hours,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.shift_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.workday_position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.position_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.location_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.location_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.ltf_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_trades.file_arrive_date,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_trades
 where stage_hash_humanity_trades.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_humanity_trades records
set @insert_date_time = getdate()
insert into s_humanity_trades (
       bk_hash,
       shift_id,
       company_id,
       company_name,
       trade_requested_employee_id,
       trade_requested_employee_eid,
       trade_requested_employee_name,
       traded_to_employee_id,
       traded_to_employee_eid,
       traded_to_employee_name,
       trade_reason,
       trade_requested_datetime_utc,
       trade_status,
       swap,
       shift_start_datetime_utc,
       shift_end_datetime_utc,
       hours,
       shift_type,
       position_id,
       workday_position_id,
       position_name,
       location_id,
       location_name,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_humanity_trades_inserts.bk_hash,
       #s_humanity_trades_inserts.shift_id,
       #s_humanity_trades_inserts.company_id,
       #s_humanity_trades_inserts.company_name,
       #s_humanity_trades_inserts.trade_requested_employee_id,
       #s_humanity_trades_inserts.trade_requested_employee_eid,
       #s_humanity_trades_inserts.trade_requested_employee_name,
       #s_humanity_trades_inserts.traded_to_employee_id,
       #s_humanity_trades_inserts.traded_to_employee_eid,
       #s_humanity_trades_inserts.traded_to_employee_name,
       #s_humanity_trades_inserts.trade_reason,
       #s_humanity_trades_inserts.trade_requested_datetime_utc,
       #s_humanity_trades_inserts.trade_status,
       #s_humanity_trades_inserts.swap,
       #s_humanity_trades_inserts.shift_start_datetime_utc,
       #s_humanity_trades_inserts.shift_end_datetime_utc,
       #s_humanity_trades_inserts.hours,
       #s_humanity_trades_inserts.shift_type,
       #s_humanity_trades_inserts.position_id,
       #s_humanity_trades_inserts.workday_position_id,
       #s_humanity_trades_inserts.position_name,
       #s_humanity_trades_inserts.location_id,
       #s_humanity_trades_inserts.location_name,
       #s_humanity_trades_inserts.ltf_file_name,
       #s_humanity_trades_inserts.file_arrive_date,
       #s_humanity_trades_inserts.dummy_modified_date_time,
       case when s_humanity_trades.s_humanity_trades_id is null then isnull(#s_humanity_trades_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #s_humanity_trades_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_humanity_trades_inserts
  left join p_humanity_trades
    on #s_humanity_trades_inserts.bk_hash = p_humanity_trades.bk_hash
   and p_humanity_trades.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_humanity_trades
    on p_humanity_trades.bk_hash = s_humanity_trades.bk_hash
   and p_humanity_trades.s_humanity_trades_id = s_humanity_trades.s_humanity_trades_id
 where s_humanity_trades.s_humanity_trades_id is null
    or (s_humanity_trades.s_humanity_trades_id is not null
        and s_humanity_trades.dv_hash <> #s_humanity_trades_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_humanity_trades @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_humanity_trades @current_dv_batch_id

end
