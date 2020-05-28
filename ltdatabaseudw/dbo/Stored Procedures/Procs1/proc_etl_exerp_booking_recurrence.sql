CREATE PROC [dbo].[proc_etl_exerp_booking_recurrence] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_booking_recurrence

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_booking_recurrence (
       bk_hash,
       main_booking_id,
       recurrence_type,
       recurrence,
       recurrence_start_datetime,
       recurrence_end,
       center_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(main_booking_id,'z#@$k%&P'))),2) bk_hash,
       main_booking_id,
       recurrence_type,
       recurrence,
       recurrence_start_datetime,
       recurrence_end,
       center_id,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_booking_recurrence.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_exerp_booking_recurrence
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_booking_recurrence @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_booking_recurrence (
       bk_hash,
       main_booking_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_booking_recurrence.bk_hash,
       stage_hash_exerp_booking_recurrence.main_booking_id main_booking_id,
       isnull(cast(stage_hash_exerp_booking_recurrence.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_booking_recurrence
  left join h_exerp_booking_recurrence
    on stage_hash_exerp_booking_recurrence.bk_hash = h_exerp_booking_recurrence.bk_hash
 where h_exerp_booking_recurrence_id is null
   and stage_hash_exerp_booking_recurrence.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_booking_recurrence
if object_id('tempdb..#l_exerp_booking_recurrence_inserts') is not null drop table #l_exerp_booking_recurrence_inserts
create table #l_exerp_booking_recurrence_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_booking_recurrence.bk_hash,
       stage_hash_exerp_booking_recurrence.main_booking_id main_booking_id,
       stage_hash_exerp_booking_recurrence.center_id center_id,
       isnull(cast(stage_hash_exerp_booking_recurrence.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_booking_recurrence.main_booking_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking_recurrence.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_booking_recurrence
 where stage_hash_exerp_booking_recurrence.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_booking_recurrence records
set @insert_date_time = getdate()
insert into l_exerp_booking_recurrence (
       bk_hash,
       main_booking_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_booking_recurrence_inserts.bk_hash,
       #l_exerp_booking_recurrence_inserts.main_booking_id,
       #l_exerp_booking_recurrence_inserts.center_id,
       case when l_exerp_booking_recurrence.l_exerp_booking_recurrence_id is null then isnull(#l_exerp_booking_recurrence_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_booking_recurrence_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_booking_recurrence_inserts
  left join p_exerp_booking_recurrence
    on #l_exerp_booking_recurrence_inserts.bk_hash = p_exerp_booking_recurrence.bk_hash
   and p_exerp_booking_recurrence.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_booking_recurrence
    on p_exerp_booking_recurrence.bk_hash = l_exerp_booking_recurrence.bk_hash
   and p_exerp_booking_recurrence.l_exerp_booking_recurrence_id = l_exerp_booking_recurrence.l_exerp_booking_recurrence_id
 where l_exerp_booking_recurrence.l_exerp_booking_recurrence_id is null
    or (l_exerp_booking_recurrence.l_exerp_booking_recurrence_id is not null
        and l_exerp_booking_recurrence.dv_hash <> #l_exerp_booking_recurrence_inserts.source_hash)

--calculate hash and lookup to current s_exerp_booking_recurrence
if object_id('tempdb..#s_exerp_booking_recurrence_inserts') is not null drop table #s_exerp_booking_recurrence_inserts
create table #s_exerp_booking_recurrence_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_booking_recurrence.bk_hash,
       stage_hash_exerp_booking_recurrence.main_booking_id main_booking_id,
       stage_hash_exerp_booking_recurrence.recurrence_type recurrence_type,
       stage_hash_exerp_booking_recurrence.recurrence recurrence,
       stage_hash_exerp_booking_recurrence.recurrence_start_datetime recurrence_start_datetime,
       stage_hash_exerp_booking_recurrence.recurrence_end recurrence_end,
       stage_hash_exerp_booking_recurrence.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_booking_recurrence.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_booking_recurrence.main_booking_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking_recurrence.recurrence_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking_recurrence.recurrence,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_booking_recurrence.recurrence_start_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_booking_recurrence.recurrence_end,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_booking_recurrence
 where stage_hash_exerp_booking_recurrence.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_booking_recurrence records
set @insert_date_time = getdate()
insert into s_exerp_booking_recurrence (
       bk_hash,
       main_booking_id,
       recurrence_type,
       recurrence,
       recurrence_start_datetime,
       recurrence_end,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_booking_recurrence_inserts.bk_hash,
       #s_exerp_booking_recurrence_inserts.main_booking_id,
       #s_exerp_booking_recurrence_inserts.recurrence_type,
       #s_exerp_booking_recurrence_inserts.recurrence,
       #s_exerp_booking_recurrence_inserts.recurrence_start_datetime,
       #s_exerp_booking_recurrence_inserts.recurrence_end,
       #s_exerp_booking_recurrence_inserts.dummy_modified_date_time,
       case when s_exerp_booking_recurrence.s_exerp_booking_recurrence_id is null then isnull(#s_exerp_booking_recurrence_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_booking_recurrence_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_booking_recurrence_inserts
  left join p_exerp_booking_recurrence
    on #s_exerp_booking_recurrence_inserts.bk_hash = p_exerp_booking_recurrence.bk_hash
   and p_exerp_booking_recurrence.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_booking_recurrence
    on p_exerp_booking_recurrence.bk_hash = s_exerp_booking_recurrence.bk_hash
   and p_exerp_booking_recurrence.s_exerp_booking_recurrence_id = s_exerp_booking_recurrence.s_exerp_booking_recurrence_id
 where s_exerp_booking_recurrence.s_exerp_booking_recurrence_id is null
    or (s_exerp_booking_recurrence.s_exerp_booking_recurrence_id is not null
        and s_exerp_booking_recurrence.dv_hash <> #s_exerp_booking_recurrence_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_booking_recurrence @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_booking_recurrence @current_dv_batch_id

end
