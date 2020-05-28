CREATE PROC [dbo].[proc_etl_spabiz_shift] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_SHIFT

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_SHIFT (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       OPENSTAFFID,
       CLOSESTAFFID,
       Date,
       DAYID,
       PERIODID,
       TIMEOPEN,
       TIMECLOSE,
       TIMEREC,
       STATUS,
       ERRORNOTE,
       DRAWERID,
       VOIDERID,
       STORE_NUMBER,
       AMOUNTINDRAWER,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       OPENSTAFFID,
       CLOSESTAFFID,
       Date,
       DAYID,
       PERIODID,
       TIMEOPEN,
       TIMECLOSE,
       TIMEREC,
       STATUS,
       ERRORNOTE,
       DRAWERID,
       VOIDERID,
       STORE_NUMBER,
       AMOUNTINDRAWER,
       isnull(cast(stage_spabiz_SHIFT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_SHIFT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_shift @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_shift (
       bk_hash,
       shift_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_SHIFT.bk_hash,
       stage_hash_spabiz_SHIFT.ID shift_id,
       stage_hash_spabiz_SHIFT.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_SHIFT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_SHIFT
  left join h_spabiz_shift
    on stage_hash_spabiz_SHIFT.bk_hash = h_spabiz_shift.bk_hash
 where h_spabiz_shift_id is null
   and stage_hash_spabiz_SHIFT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_shift
if object_id('tempdb..#l_spabiz_shift_inserts') is not null drop table #l_spabiz_shift_inserts
create table #l_spabiz_shift_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SHIFT.bk_hash,
       stage_hash_spabiz_SHIFT.ID shift_id,
       stage_hash_spabiz_SHIFT.STOREID store_id,
       stage_hash_spabiz_SHIFT.OPENSTAFFID open_staff_id,
       stage_hash_spabiz_SHIFT.CLOSESTAFFID close_staff_id,
       stage_hash_spabiz_SHIFT.PERIODID period_id,
       stage_hash_spabiz_SHIFT.DRAWERID drawer_id,
       stage_hash_spabiz_SHIFT.VOIDERID voider_id,
       stage_hash_spabiz_SHIFT.STORE_NUMBER store_number,
       stage_hash_spabiz_SHIFT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.OPENSTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.CLOSESTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.PERIODID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.DRAWERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.VOIDERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SHIFT
 where stage_hash_spabiz_SHIFT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_shift records
set @insert_date_time = getdate()
insert into l_spabiz_shift (
       bk_hash,
       shift_id,
       store_id,
       open_staff_id,
       close_staff_id,
       period_id,
       drawer_id,
       voider_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_shift_inserts.bk_hash,
       #l_spabiz_shift_inserts.shift_id,
       #l_spabiz_shift_inserts.store_id,
       #l_spabiz_shift_inserts.open_staff_id,
       #l_spabiz_shift_inserts.close_staff_id,
       #l_spabiz_shift_inserts.period_id,
       #l_spabiz_shift_inserts.drawer_id,
       #l_spabiz_shift_inserts.voider_id,
       #l_spabiz_shift_inserts.store_number,
       case when l_spabiz_shift.l_spabiz_shift_id is null then isnull(#l_spabiz_shift_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_shift_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_shift_inserts
  left join p_spabiz_shift
    on #l_spabiz_shift_inserts.bk_hash = p_spabiz_shift.bk_hash
   and p_spabiz_shift.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_shift
    on p_spabiz_shift.bk_hash = l_spabiz_shift.bk_hash
   and p_spabiz_shift.l_spabiz_shift_id = l_spabiz_shift.l_spabiz_shift_id
 where l_spabiz_shift.l_spabiz_shift_id is null
    or (l_spabiz_shift.l_spabiz_shift_id is not null
        and l_spabiz_shift.dv_hash <> #l_spabiz_shift_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_shift
if object_id('tempdb..#s_spabiz_shift_inserts') is not null drop table #s_spabiz_shift_inserts
create table #s_spabiz_shift_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SHIFT.bk_hash,
       stage_hash_spabiz_SHIFT.ID shift_id,
       stage_hash_spabiz_SHIFT.COUNTERID counter_id,
       stage_hash_spabiz_SHIFT.EDITTIME edit_time,
       stage_hash_spabiz_SHIFT.Date date,
       stage_hash_spabiz_SHIFT.DAYID day_id,
       stage_hash_spabiz_SHIFT.TIMEOPEN time_open,
       stage_hash_spabiz_SHIFT.TIMECLOSE time_close,
       stage_hash_spabiz_SHIFT.TIMEREC time_rec,
       stage_hash_spabiz_SHIFT.STATUS status,
       stage_hash_spabiz_SHIFT.ERRORNOTE error_note,
       stage_hash_spabiz_SHIFT.STORE_NUMBER store_number,
       stage_hash_spabiz_SHIFT.AMOUNTINDRAWER amount_in_drawer,
       stage_hash_spabiz_SHIFT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SHIFT.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SHIFT.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.DAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SHIFT.TIMEOPEN,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SHIFT.TIMECLOSE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SHIFT.TIMEREC,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SHIFT.ERRORNOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SHIFT.AMOUNTINDRAWER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SHIFT
 where stage_hash_spabiz_SHIFT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_shift records
set @insert_date_time = getdate()
insert into s_spabiz_shift (
       bk_hash,
       shift_id,
       counter_id,
       edit_time,
       date,
       day_id,
       time_open,
       time_close,
       time_rec,
       status,
       error_note,
       store_number,
       amount_in_drawer,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_shift_inserts.bk_hash,
       #s_spabiz_shift_inserts.shift_id,
       #s_spabiz_shift_inserts.counter_id,
       #s_spabiz_shift_inserts.edit_time,
       #s_spabiz_shift_inserts.date,
       #s_spabiz_shift_inserts.day_id,
       #s_spabiz_shift_inserts.time_open,
       #s_spabiz_shift_inserts.time_close,
       #s_spabiz_shift_inserts.time_rec,
       #s_spabiz_shift_inserts.status,
       #s_spabiz_shift_inserts.error_note,
       #s_spabiz_shift_inserts.store_number,
       #s_spabiz_shift_inserts.amount_in_drawer,
       case when s_spabiz_shift.s_spabiz_shift_id is null then isnull(#s_spabiz_shift_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_shift_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_shift_inserts
  left join p_spabiz_shift
    on #s_spabiz_shift_inserts.bk_hash = p_spabiz_shift.bk_hash
   and p_spabiz_shift.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_shift
    on p_spabiz_shift.bk_hash = s_spabiz_shift.bk_hash
   and p_spabiz_shift.s_spabiz_shift_id = s_spabiz_shift.s_spabiz_shift_id
 where s_spabiz_shift.s_spabiz_shift_id is null
    or (s_spabiz_shift.s_spabiz_shift_id is not null
        and s_spabiz_shift.dv_hash <> #s_spabiz_shift_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_shift @current_dv_batch_id

end
