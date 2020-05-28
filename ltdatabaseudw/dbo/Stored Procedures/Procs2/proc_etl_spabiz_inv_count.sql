CREATE PROC [dbo].[proc_etl_spabiz_inv_count] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_INV_COUNT

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_INV_COUNT (
       bk_hash,
       [ID],
       COUNTERID,
       NUM,
       CYCLEID,
       COUNTID,
       STATUS,
       NOCYCLE,
       DATEEXPECTED,
       DATESTARTED,
       Date,
       STAFFID,
       COUNTCHECKEDBY,
       ENTEREDBY,
       ENTERCHECKEDBY,
       STARTRANGE,
       ENDRANGE,
       SORTCOUNTBY,
       NAME,
       ITEMTYPE,
       TOTALSKUS,
       NUMADJUSTED,
       INVEFFECT,
       EXTRA,
       EDITTIME,
       ADJNUM,
       STORE_NUMBER,
       STOREID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([ID] as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [ID],
       COUNTERID,
       NUM,
       CYCLEID,
       COUNTID,
       STATUS,
       NOCYCLE,
       DATEEXPECTED,
       DATESTARTED,
       Date,
       STAFFID,
       COUNTCHECKEDBY,
       ENTEREDBY,
       ENTERCHECKEDBY,
       STARTRANGE,
       ENDRANGE,
       SORTCOUNTBY,
       NAME,
       ITEMTYPE,
       TOTALSKUS,
       NUMADJUSTED,
       INVEFFECT,
       EXTRA,
       EDITTIME,
       ADJNUM,
       STORE_NUMBER,
       STOREID,
       isnull(cast(stage_spabiz_INV_COUNT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_INV_COUNT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_inv_count @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_inv_count (
       bk_hash,
       inv_count_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_INV_COUNT.bk_hash,
       stage_hash_spabiz_INV_COUNT.[ID] inv_count_id,
       stage_hash_spabiz_INV_COUNT.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_INV_COUNT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_INV_COUNT
  left join h_spabiz_inv_count
    on stage_hash_spabiz_INV_COUNT.bk_hash = h_spabiz_inv_count.bk_hash
 where h_spabiz_inv_count_id is null
   and stage_hash_spabiz_INV_COUNT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_inv_count
if object_id('tempdb..#l_spabiz_inv_count_inserts') is not null drop table #l_spabiz_inv_count_inserts
create table #l_spabiz_inv_count_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_INV_COUNT.bk_hash,
       stage_hash_spabiz_INV_COUNT.[ID] inv_count_id,
       stage_hash_spabiz_INV_COUNT.CYCLEID cycle_id,
       stage_hash_spabiz_INV_COUNT.COUNTID count_id,
       stage_hash_spabiz_INV_COUNT.STAFFID staff_id,
       stage_hash_spabiz_INV_COUNT.COUNTCHECKEDBY count_checked_by,
       stage_hash_spabiz_INV_COUNT.ENTEREDBY entered_by,
       stage_hash_spabiz_INV_COUNT.ENTERCHECKEDBY enter_checked_by,
       stage_hash_spabiz_INV_COUNT.STORE_NUMBER store_number,
       stage_hash_spabiz_INV_COUNT.STOREID store_id,
       stage_hash_spabiz_INV_COUNT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.CYCLEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.COUNTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.COUNTCHECKEDBY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.ENTEREDBY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.ENTERCHECKEDBY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.STOREID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_INV_COUNT
 where stage_hash_spabiz_INV_COUNT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_inv_count records
set @insert_date_time = getdate()
insert into l_spabiz_inv_count (
       bk_hash,
       inv_count_id,
       cycle_id,
       count_id,
       staff_id,
       count_checked_by,
       entered_by,
       enter_checked_by,
       store_number,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_inv_count_inserts.bk_hash,
       #l_spabiz_inv_count_inserts.inv_count_id,
       #l_spabiz_inv_count_inserts.cycle_id,
       #l_spabiz_inv_count_inserts.count_id,
       #l_spabiz_inv_count_inserts.staff_id,
       #l_spabiz_inv_count_inserts.count_checked_by,
       #l_spabiz_inv_count_inserts.entered_by,
       #l_spabiz_inv_count_inserts.enter_checked_by,
       #l_spabiz_inv_count_inserts.store_number,
       #l_spabiz_inv_count_inserts.store_id,
       case when l_spabiz_inv_count.l_spabiz_inv_count_id is null then isnull(#l_spabiz_inv_count_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_inv_count_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_inv_count_inserts
  left join p_spabiz_inv_count
    on #l_spabiz_inv_count_inserts.bk_hash = p_spabiz_inv_count.bk_hash
   and p_spabiz_inv_count.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_inv_count
    on p_spabiz_inv_count.bk_hash = l_spabiz_inv_count.bk_hash
   and p_spabiz_inv_count.l_spabiz_inv_count_id = l_spabiz_inv_count.l_spabiz_inv_count_id
 where l_spabiz_inv_count.l_spabiz_inv_count_id is null
    or (l_spabiz_inv_count.l_spabiz_inv_count_id is not null
        and l_spabiz_inv_count.dv_hash <> #l_spabiz_inv_count_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_inv_count
if object_id('tempdb..#s_spabiz_inv_count_inserts') is not null drop table #s_spabiz_inv_count_inserts
create table #s_spabiz_inv_count_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_INV_COUNT.bk_hash,
       stage_hash_spabiz_INV_COUNT.[ID] inv_count_id,
       stage_hash_spabiz_INV_COUNT.COUNTERID counter_id,
       stage_hash_spabiz_INV_COUNT.NUM num,
       stage_hash_spabiz_INV_COUNT.STATUS status,
       stage_hash_spabiz_INV_COUNT.NOCYCLE no_cycle,
       stage_hash_spabiz_INV_COUNT.DATEEXPECTED date_expected,
       stage_hash_spabiz_INV_COUNT.DATESTARTED date_started,
       stage_hash_spabiz_INV_COUNT.Date date,
       stage_hash_spabiz_INV_COUNT.STARTRANGE start_range,
       stage_hash_spabiz_INV_COUNT.ENDRANGE end_range,
       stage_hash_spabiz_INV_COUNT.SORTCOUNTBY sort_count_by,
       stage_hash_spabiz_INV_COUNT.NAME name,
       stage_hash_spabiz_INV_COUNT.ITEMTYPE item_type,
       stage_hash_spabiz_INV_COUNT.TOTALSKUS total_skus,
       stage_hash_spabiz_INV_COUNT.NUMADJUSTED num_adjusted,
       stage_hash_spabiz_INV_COUNT.INVEFFECT inv_effect,
       stage_hash_spabiz_INV_COUNT.EXTRA extra,
       stage_hash_spabiz_INV_COUNT.EDITTIME edit_time,
       stage_hash_spabiz_INV_COUNT.ADJNUM adj_num,
       stage_hash_spabiz_INV_COUNT.STORE_NUMBER store_number,
       stage_hash_spabiz_INV_COUNT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INV_COUNT.NUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.NOCYCLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INV_COUNT.DATEEXPECTED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INV_COUNT.DATESTARTED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INV_COUNT.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INV_COUNT.STARTRANGE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INV_COUNT.ENDRANGE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.SORTCOUNTBY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INV_COUNT.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.ITEMTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.TOTALSKUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.NUMADJUSTED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.INVEFFECT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INV_COUNT.EXTRA,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INV_COUNT.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INV_COUNT.ADJNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INV_COUNT.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_INV_COUNT
 where stage_hash_spabiz_INV_COUNT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_inv_count records
set @insert_date_time = getdate()
insert into s_spabiz_inv_count (
       bk_hash,
       inv_count_id,
       counter_id,
       num,
       status,
       no_cycle,
       date_expected,
       date_started,
       date,
       start_range,
       end_range,
       sort_count_by,
       name,
       item_type,
       total_skus,
       num_adjusted,
       inv_effect,
       extra,
       edit_time,
       adj_num,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_inv_count_inserts.bk_hash,
       #s_spabiz_inv_count_inserts.inv_count_id,
       #s_spabiz_inv_count_inserts.counter_id,
       #s_spabiz_inv_count_inserts.num,
       #s_spabiz_inv_count_inserts.status,
       #s_spabiz_inv_count_inserts.no_cycle,
       #s_spabiz_inv_count_inserts.date_expected,
       #s_spabiz_inv_count_inserts.date_started,
       #s_spabiz_inv_count_inserts.date,
       #s_spabiz_inv_count_inserts.start_range,
       #s_spabiz_inv_count_inserts.end_range,
       #s_spabiz_inv_count_inserts.sort_count_by,
       #s_spabiz_inv_count_inserts.name,
       #s_spabiz_inv_count_inserts.item_type,
       #s_spabiz_inv_count_inserts.total_skus,
       #s_spabiz_inv_count_inserts.num_adjusted,
       #s_spabiz_inv_count_inserts.inv_effect,
       #s_spabiz_inv_count_inserts.extra,
       #s_spabiz_inv_count_inserts.edit_time,
       #s_spabiz_inv_count_inserts.adj_num,
       #s_spabiz_inv_count_inserts.store_number,
       case when s_spabiz_inv_count.s_spabiz_inv_count_id is null then isnull(#s_spabiz_inv_count_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_inv_count_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_inv_count_inserts
  left join p_spabiz_inv_count
    on #s_spabiz_inv_count_inserts.bk_hash = p_spabiz_inv_count.bk_hash
   and p_spabiz_inv_count.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_inv_count
    on p_spabiz_inv_count.bk_hash = s_spabiz_inv_count.bk_hash
   and p_spabiz_inv_count.s_spabiz_inv_count_id = s_spabiz_inv_count.s_spabiz_inv_count_id
 where s_spabiz_inv_count.s_spabiz_inv_count_id is null
    or (s_spabiz_inv_count.s_spabiz_inv_count_id is not null
        and s_spabiz_inv_count.dv_hash <> #s_spabiz_inv_count_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_inv_count @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_inv_count @current_dv_batch_id

end
