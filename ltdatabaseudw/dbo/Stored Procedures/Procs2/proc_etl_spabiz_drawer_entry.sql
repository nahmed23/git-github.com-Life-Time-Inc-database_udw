CREATE PROC [dbo].[proc_etl_spabiz_drawer_entry] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_DRAWERENTRY

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_DRAWERENTRY (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       STATUS,
       SHIFTID,
       NUM,
       INAMOUNT,
       INTYPE,
       INOK,
       OUTAMOUNT,
       OUTTYPE,
       OUTOK,
       STAFFID,
       PERIODID,
       DAYID,
       Date,
       TIME,
       PAYEEID,
       PAYEETYPE,
       PAYEEINDEX,
       REASONID,
       NOTE,
       OK,
       CHECKNUM,
       DRAWERNUM,
       STORE_NUMBER,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       STATUS,
       SHIFTID,
       NUM,
       INAMOUNT,
       INTYPE,
       INOK,
       OUTAMOUNT,
       OUTTYPE,
       OUTOK,
       STAFFID,
       PERIODID,
       DAYID,
       Date,
       TIME,
       PAYEEID,
       PAYEETYPE,
       PAYEEINDEX,
       REASONID,
       NOTE,
       OK,
       CHECKNUM,
       DRAWERNUM,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_DRAWERENTRY.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_DRAWERENTRY
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_drawer_entry @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_drawer_entry (
       bk_hash,
       drawer_entry_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_DRAWERENTRY.bk_hash,
       stage_hash_spabiz_DRAWERENTRY.ID drawer_entry_id,
       stage_hash_spabiz_DRAWERENTRY.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_DRAWERENTRY.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_DRAWERENTRY
  left join h_spabiz_drawer_entry
    on stage_hash_spabiz_DRAWERENTRY.bk_hash = h_spabiz_drawer_entry.bk_hash
 where h_spabiz_drawer_entry_id is null
   and stage_hash_spabiz_DRAWERENTRY.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_drawer_entry
if object_id('tempdb..#l_spabiz_drawer_entry_inserts') is not null drop table #l_spabiz_drawer_entry_inserts
create table #l_spabiz_drawer_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_DRAWERENTRY.bk_hash,
       stage_hash_spabiz_DRAWERENTRY.ID drawer_entry_id,
       stage_hash_spabiz_DRAWERENTRY.STOREID store_id,
       stage_hash_spabiz_DRAWERENTRY.SHIFTID shift_id,
       stage_hash_spabiz_DRAWERENTRY.INTYPE in_type,
       stage_hash_spabiz_DRAWERENTRY.OUTTYPE out_type,
       stage_hash_spabiz_DRAWERENTRY.STAFFID staff_id,
       stage_hash_spabiz_DRAWERENTRY.PERIODID period_id,
       stage_hash_spabiz_DRAWERENTRY.DAYID day_id,
       stage_hash_spabiz_DRAWERENTRY.PAYEEID payee_id,
       stage_hash_spabiz_DRAWERENTRY.REASONID reason_id,
       stage_hash_spabiz_DRAWERENTRY.STORE_NUMBER store_number,
       stage_hash_spabiz_DRAWERENTRY.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.SHIFTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.INTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.OUTTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.PERIODID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.DAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.PAYEEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.REASONID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_DRAWERENTRY
 where stage_hash_spabiz_DRAWERENTRY.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_drawer_entry records
set @insert_date_time = getdate()
insert into l_spabiz_drawer_entry (
       bk_hash,
       drawer_entry_id,
       store_id,
       shift_id,
       in_type,
       out_type,
       staff_id,
       period_id,
       day_id,
       payee_id,
       reason_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_drawer_entry_inserts.bk_hash,
       #l_spabiz_drawer_entry_inserts.drawer_entry_id,
       #l_spabiz_drawer_entry_inserts.store_id,
       #l_spabiz_drawer_entry_inserts.shift_id,
       #l_spabiz_drawer_entry_inserts.in_type,
       #l_spabiz_drawer_entry_inserts.out_type,
       #l_spabiz_drawer_entry_inserts.staff_id,
       #l_spabiz_drawer_entry_inserts.period_id,
       #l_spabiz_drawer_entry_inserts.day_id,
       #l_spabiz_drawer_entry_inserts.payee_id,
       #l_spabiz_drawer_entry_inserts.reason_id,
       #l_spabiz_drawer_entry_inserts.store_number,
       case when l_spabiz_drawer_entry.l_spabiz_drawer_entry_id is null then isnull(#l_spabiz_drawer_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_drawer_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_drawer_entry_inserts
  left join p_spabiz_drawer_entry
    on #l_spabiz_drawer_entry_inserts.bk_hash = p_spabiz_drawer_entry.bk_hash
   and p_spabiz_drawer_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_drawer_entry
    on p_spabiz_drawer_entry.bk_hash = l_spabiz_drawer_entry.bk_hash
   and p_spabiz_drawer_entry.l_spabiz_drawer_entry_id = l_spabiz_drawer_entry.l_spabiz_drawer_entry_id
 where l_spabiz_drawer_entry.l_spabiz_drawer_entry_id is null
    or (l_spabiz_drawer_entry.l_spabiz_drawer_entry_id is not null
        and l_spabiz_drawer_entry.dv_hash <> #l_spabiz_drawer_entry_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_drawer_entry
if object_id('tempdb..#s_spabiz_drawer_entry_inserts') is not null drop table #s_spabiz_drawer_entry_inserts
create table #s_spabiz_drawer_entry_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_DRAWERENTRY.bk_hash,
       stage_hash_spabiz_DRAWERENTRY.ID drawer_entry_id,
       stage_hash_spabiz_DRAWERENTRY.COUNTERID counter_id,
       stage_hash_spabiz_DRAWERENTRY.EDITTIME edit_time,
       stage_hash_spabiz_DRAWERENTRY.STATUS status,
       stage_hash_spabiz_DRAWERENTRY.NUM num,
       stage_hash_spabiz_DRAWERENTRY.INAMOUNT in_amount,
       stage_hash_spabiz_DRAWERENTRY.INOK in_ok,
       stage_hash_spabiz_DRAWERENTRY.OUTAMOUNT out_amount,
       stage_hash_spabiz_DRAWERENTRY.OUTOK out_ok,
       stage_hash_spabiz_DRAWERENTRY.Date date,
       stage_hash_spabiz_DRAWERENTRY.TIME time,
       stage_hash_spabiz_DRAWERENTRY.PAYEETYPE payee_type,
       stage_hash_spabiz_DRAWERENTRY.PAYEEINDEX payee_index,
       stage_hash_spabiz_DRAWERENTRY.NOTE note,
       stage_hash_spabiz_DRAWERENTRY.OK ok,
       stage_hash_spabiz_DRAWERENTRY.CHECKNUM check_num,
       stage_hash_spabiz_DRAWERENTRY.DRAWERNUM drawer_num,
       stage_hash_spabiz_DRAWERENTRY.STORE_NUMBER store_number,
       stage_hash_spabiz_DRAWERENTRY.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DRAWERENTRY.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DRAWERENTRY.NUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.INAMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.INOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.OUTAMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.OUTOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DRAWERENTRY.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DRAWERENTRY.TIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.PAYEETYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DRAWERENTRY.PAYEEINDEX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DRAWERENTRY.NOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.OK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DRAWERENTRY.CHECKNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DRAWERENTRY.DRAWERNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DRAWERENTRY.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_DRAWERENTRY
 where stage_hash_spabiz_DRAWERENTRY.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_drawer_entry records
set @insert_date_time = getdate()
insert into s_spabiz_drawer_entry (
       bk_hash,
       drawer_entry_id,
       counter_id,
       edit_time,
       status,
       num,
       in_amount,
       in_ok,
       out_amount,
       out_ok,
       date,
       time,
       payee_type,
       payee_index,
       note,
       ok,
       check_num,
       drawer_num,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_drawer_entry_inserts.bk_hash,
       #s_spabiz_drawer_entry_inserts.drawer_entry_id,
       #s_spabiz_drawer_entry_inserts.counter_id,
       #s_spabiz_drawer_entry_inserts.edit_time,
       #s_spabiz_drawer_entry_inserts.status,
       #s_spabiz_drawer_entry_inserts.num,
       #s_spabiz_drawer_entry_inserts.in_amount,
       #s_spabiz_drawer_entry_inserts.in_ok,
       #s_spabiz_drawer_entry_inserts.out_amount,
       #s_spabiz_drawer_entry_inserts.out_ok,
       #s_spabiz_drawer_entry_inserts.date,
       #s_spabiz_drawer_entry_inserts.time,
       #s_spabiz_drawer_entry_inserts.payee_type,
       #s_spabiz_drawer_entry_inserts.payee_index,
       #s_spabiz_drawer_entry_inserts.note,
       #s_spabiz_drawer_entry_inserts.ok,
       #s_spabiz_drawer_entry_inserts.check_num,
       #s_spabiz_drawer_entry_inserts.drawer_num,
       #s_spabiz_drawer_entry_inserts.store_number,
       case when s_spabiz_drawer_entry.s_spabiz_drawer_entry_id is null then isnull(#s_spabiz_drawer_entry_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_drawer_entry_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_drawer_entry_inserts
  left join p_spabiz_drawer_entry
    on #s_spabiz_drawer_entry_inserts.bk_hash = p_spabiz_drawer_entry.bk_hash
   and p_spabiz_drawer_entry.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_drawer_entry
    on p_spabiz_drawer_entry.bk_hash = s_spabiz_drawer_entry.bk_hash
   and p_spabiz_drawer_entry.s_spabiz_drawer_entry_id = s_spabiz_drawer_entry.s_spabiz_drawer_entry_id
 where s_spabiz_drawer_entry.s_spabiz_drawer_entry_id is null
    or (s_spabiz_drawer_entry.s_spabiz_drawer_entry_id is not null
        and s_spabiz_drawer_entry.dv_hash <> #s_spabiz_drawer_entry_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_drawer_entry @current_dv_batch_id

end
