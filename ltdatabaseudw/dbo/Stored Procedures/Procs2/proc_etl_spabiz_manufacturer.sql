CREATE PROC [dbo].[proc_etl_spabiz_manufacturer] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_MANUFACTURER

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_MANUFACTURER (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       REFRESH,
       STORE_NUMBER,
       NEWID,
       MANUFACTURERBACKUPID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       REFRESH,
       STORE_NUMBER,
       NEWID,
       MANUFACTURERBACKUPID,
       isnull(cast(stage_spabiz_MANUFACTURER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_MANUFACTURER
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_manufacturer @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_manufacturer (
       bk_hash,
       manufacturer_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_MANUFACTURER.bk_hash,
       stage_hash_spabiz_MANUFACTURER.ID manufacturer_id,
       stage_hash_spabiz_MANUFACTURER.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_MANUFACTURER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_MANUFACTURER
  left join h_spabiz_manufacturer
    on stage_hash_spabiz_MANUFACTURER.bk_hash = h_spabiz_manufacturer.bk_hash
 where h_spabiz_manufacturer_id is null
   and stage_hash_spabiz_MANUFACTURER.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_manufacturer
if object_id('tempdb..#l_spabiz_manufacturer_inserts') is not null drop table #l_spabiz_manufacturer_inserts
create table #l_spabiz_manufacturer_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_MANUFACTURER.bk_hash,
       stage_hash_spabiz_MANUFACTURER.ID manufacturer_id,
       stage_hash_spabiz_MANUFACTURER.STOREID store_id,
       stage_hash_spabiz_MANUFACTURER.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_MANUFACTURER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_MANUFACTURER
 where stage_hash_spabiz_MANUFACTURER.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_manufacturer records
set @insert_date_time = getdate()
insert into l_spabiz_manufacturer (
       bk_hash,
       manufacturer_id,
       store_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_manufacturer_inserts.bk_hash,
       #l_spabiz_manufacturer_inserts.manufacturer_id,
       #l_spabiz_manufacturer_inserts.store_id,
       #l_spabiz_manufacturer_inserts.store_number,
       case when l_spabiz_manufacturer.l_spabiz_manufacturer_id is null then isnull(#l_spabiz_manufacturer_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_manufacturer_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_manufacturer_inserts
  left join p_spabiz_manufacturer
    on #l_spabiz_manufacturer_inserts.bk_hash = p_spabiz_manufacturer.bk_hash
   and p_spabiz_manufacturer.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_manufacturer
    on p_spabiz_manufacturer.bk_hash = l_spabiz_manufacturer.bk_hash
   and p_spabiz_manufacturer.l_spabiz_manufacturer_id = l_spabiz_manufacturer.l_spabiz_manufacturer_id
 where l_spabiz_manufacturer.l_spabiz_manufacturer_id is null
    or (l_spabiz_manufacturer.l_spabiz_manufacturer_id is not null
        and l_spabiz_manufacturer.dv_hash <> #l_spabiz_manufacturer_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_manufacturer
if object_id('tempdb..#s_spabiz_manufacturer_inserts') is not null drop table #s_spabiz_manufacturer_inserts
create table #s_spabiz_manufacturer_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_MANUFACTURER.bk_hash,
       stage_hash_spabiz_MANUFACTURER.ID manufacturer_id,
       stage_hash_spabiz_MANUFACTURER.COUNTERID counter_id,
       stage_hash_spabiz_MANUFACTURER.EDITTIME edit_time,
       stage_hash_spabiz_MANUFACTURER.[Delete] manufacturer_delete,
       stage_hash_spabiz_MANUFACTURER.DELETEDATE delete_date,
       stage_hash_spabiz_MANUFACTURER.NAME name,
       stage_hash_spabiz_MANUFACTURER.QUICKID quick_id,
       stage_hash_spabiz_MANUFACTURER.REFRESH refresh,
       stage_hash_spabiz_MANUFACTURER.STORE_NUMBER store_number,
       stage_hash_spabiz_MANUFACTURER.NEWID new_id,
       stage_hash_spabiz_MANUFACTURER.MANUFACTURERBACKUPID manufacturer_backup_id,
       isnull(cast(stage_hash_spabiz_MANUFACTURER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_MANUFACTURER.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_MANUFACTURER.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_MANUFACTURER.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_MANUFACTURER.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.REFRESH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.NEWID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_MANUFACTURER.MANUFACTURERBACKUPID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_MANUFACTURER
 where stage_hash_spabiz_MANUFACTURER.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_manufacturer records
set @insert_date_time = getdate()
insert into s_spabiz_manufacturer (
       bk_hash,
       manufacturer_id,
       counter_id,
       edit_time,
       manufacturer_delete,
       delete_date,
       name,
       quick_id,
       refresh,
       store_number,
       new_id,
       manufacturer_backup_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_manufacturer_inserts.bk_hash,
       #s_spabiz_manufacturer_inserts.manufacturer_id,
       #s_spabiz_manufacturer_inserts.counter_id,
       #s_spabiz_manufacturer_inserts.edit_time,
       #s_spabiz_manufacturer_inserts.manufacturer_delete,
       #s_spabiz_manufacturer_inserts.delete_date,
       #s_spabiz_manufacturer_inserts.name,
       #s_spabiz_manufacturer_inserts.quick_id,
       #s_spabiz_manufacturer_inserts.refresh,
       #s_spabiz_manufacturer_inserts.store_number,
       #s_spabiz_manufacturer_inserts.new_id,
       #s_spabiz_manufacturer_inserts.manufacturer_backup_id,
       case when s_spabiz_manufacturer.s_spabiz_manufacturer_id is null then isnull(#s_spabiz_manufacturer_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_manufacturer_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_manufacturer_inserts
  left join p_spabiz_manufacturer
    on #s_spabiz_manufacturer_inserts.bk_hash = p_spabiz_manufacturer.bk_hash
   and p_spabiz_manufacturer.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_manufacturer
    on p_spabiz_manufacturer.bk_hash = s_spabiz_manufacturer.bk_hash
   and p_spabiz_manufacturer.s_spabiz_manufacturer_id = s_spabiz_manufacturer.s_spabiz_manufacturer_id
 where s_spabiz_manufacturer.s_spabiz_manufacturer_id is null
    or (s_spabiz_manufacturer.s_spabiz_manufacturer_id is not null
        and s_spabiz_manufacturer.dv_hash <> #s_spabiz_manufacturer_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_manufacturer @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_manufacturer @current_dv_batch_id

end
