CREATE PROC [dbo].[proc_etl_exerp_booking_Resource_usage] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_booking_resource_usage

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_booking_resource_usage (
       bk_hash,
       resource_id,
       booking_id,
       state,
       booking_start_datetime,
       booking_stop_datetime,
       center_id,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(resource_id,'z#@$k%&P')+'P%#&z$@k'+isnull(booking_id,'z#@$k%&P')+'P%#&z$@k'+isnull(state,'z#@$k%&P'))),2) bk_hash,
       resource_id,
       booking_id,
       state,
       booking_start_datetime,
       booking_stop_datetime,
       center_id,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_booking_resource_usage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_booking_resource_usage
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_booking_Resource_usage @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_booking_Resource_usage (
       bk_hash,
       resource_id,
       booking_id,
       state,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_booking_resource_usage.bk_hash,
       stage_hash_exerp_booking_resource_usage.resource_id resource_id,
       stage_hash_exerp_booking_resource_usage.booking_id booking_id,
       stage_hash_exerp_booking_resource_usage.state state,
       isnull(cast(stage_hash_exerp_booking_resource_usage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_booking_resource_usage
  left join h_exerp_booking_Resource_usage
    on stage_hash_exerp_booking_resource_usage.bk_hash = h_exerp_booking_Resource_usage.bk_hash
 where h_exerp_booking_Resource_usage_id is null
   and stage_hash_exerp_booking_resource_usage.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_booking_resource_usage
if object_id('tempdb..#l_exerp_booking_resource_usage_inserts') is not null drop table #l_exerp_booking_resource_usage_inserts
create table #l_exerp_booking_resource_usage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_booking_resource_usage.bk_hash,
       stage_hash_exerp_booking_resource_usage.resource_id resource_id,
       stage_hash_exerp_booking_resource_usage.booking_id booking_id,
       stage_hash_exerp_booking_resource_usage.state state,
       stage_hash_exerp_booking_resource_usage.center_id center_id,
       isnull(cast(stage_hash_exerp_booking_resource_usage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_booking_resource_usage.resource_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking_resource_usage.booking_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking_resource_usage.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking_resource_usage.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_booking_resource_usage
 where stage_hash_exerp_booking_resource_usage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_booking_resource_usage records
set @insert_date_time = getdate()
insert into l_exerp_booking_resource_usage (
       bk_hash,
       resource_id,
       booking_id,
       state,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_booking_resource_usage_inserts.bk_hash,
       #l_exerp_booking_resource_usage_inserts.resource_id,
       #l_exerp_booking_resource_usage_inserts.booking_id,
       #l_exerp_booking_resource_usage_inserts.state,
       #l_exerp_booking_resource_usage_inserts.center_id,
       case when l_exerp_booking_resource_usage.l_exerp_booking_resource_usage_id is null then isnull(#l_exerp_booking_resource_usage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_booking_resource_usage_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_booking_resource_usage_inserts
  left join p_exerp_booking_Resource_usage
    on #l_exerp_booking_resource_usage_inserts.bk_hash = p_exerp_booking_Resource_usage.bk_hash
   and p_exerp_booking_Resource_usage.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_booking_resource_usage
    on p_exerp_booking_Resource_usage.bk_hash = l_exerp_booking_resource_usage.bk_hash
   and p_exerp_booking_Resource_usage.l_exerp_booking_resource_usage_id = l_exerp_booking_resource_usage.l_exerp_booking_resource_usage_id
 where l_exerp_booking_resource_usage.l_exerp_booking_resource_usage_id is null
    or (l_exerp_booking_resource_usage.l_exerp_booking_resource_usage_id is not null
        and l_exerp_booking_resource_usage.dv_hash <> #l_exerp_booking_resource_usage_inserts.source_hash)

--calculate hash and lookup to current s_exerp_booking_resource_usage
if object_id('tempdb..#s_exerp_booking_resource_usage_inserts') is not null drop table #s_exerp_booking_resource_usage_inserts
create table #s_exerp_booking_resource_usage_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_booking_resource_usage.bk_hash,
       stage_hash_exerp_booking_resource_usage.resource_id resource_id,
       stage_hash_exerp_booking_resource_usage.booking_id booking_id,
       stage_hash_exerp_booking_resource_usage.state state,
       stage_hash_exerp_booking_resource_usage.booking_start_datetime booking_start_datetime,
       stage_hash_exerp_booking_resource_usage.booking_stop_datetime booking_stop_datetime,
       stage_hash_exerp_booking_resource_usage.ets ets,
       stage_hash_exerp_booking_resource_usage.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_booking_resource_usage.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_booking_resource_usage.resource_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking_resource_usage.booking_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking_resource_usage.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_booking_resource_usage.booking_start_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_booking_resource_usage.booking_stop_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking_resource_usage.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_booking_resource_usage
 where stage_hash_exerp_booking_resource_usage.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_booking_resource_usage records
set @insert_date_time = getdate()
insert into s_exerp_booking_resource_usage (
       bk_hash,
       resource_id,
       booking_id,
       state,
       booking_start_datetime,
       booking_stop_datetime,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_booking_resource_usage_inserts.bk_hash,
       #s_exerp_booking_resource_usage_inserts.resource_id,
       #s_exerp_booking_resource_usage_inserts.booking_id,
       #s_exerp_booking_resource_usage_inserts.state,
       #s_exerp_booking_resource_usage_inserts.booking_start_datetime,
       #s_exerp_booking_resource_usage_inserts.booking_stop_datetime,
       #s_exerp_booking_resource_usage_inserts.ets,
       #s_exerp_booking_resource_usage_inserts.dummy_modified_date_time,
       case when s_exerp_booking_resource_usage.s_exerp_booking_resource_usage_id is null then isnull(#s_exerp_booking_resource_usage_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_booking_resource_usage_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_booking_resource_usage_inserts
  left join p_exerp_booking_Resource_usage
    on #s_exerp_booking_resource_usage_inserts.bk_hash = p_exerp_booking_Resource_usage.bk_hash
   and p_exerp_booking_Resource_usage.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_booking_resource_usage
    on p_exerp_booking_Resource_usage.bk_hash = s_exerp_booking_resource_usage.bk_hash
   and p_exerp_booking_Resource_usage.s_exerp_booking_resource_usage_id = s_exerp_booking_resource_usage.s_exerp_booking_resource_usage_id
 where s_exerp_booking_resource_usage.s_exerp_booking_resource_usage_id is null
    or (s_exerp_booking_resource_usage.s_exerp_booking_resource_usage_id is not null
        and s_exerp_booking_resource_usage.dv_hash <> #s_exerp_booking_resource_usage_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_booking_Resource_usage @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_booking_resource_usage @current_dv_batch_id

end
