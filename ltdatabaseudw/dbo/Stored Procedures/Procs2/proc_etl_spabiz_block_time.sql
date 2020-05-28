CREATE PROC [dbo].[proc_etl_spabiz_block_time] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_BLOCKTIME

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_BLOCKTIME (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       QUICKID,
       [Delete],
       DELETEDATE,
       NAME,
       STORE_NUMBER,
       REDUCESPRODUCTIVITY,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       QUICKID,
       [Delete],
       DELETEDATE,
       NAME,
       STORE_NUMBER,
       REDUCESPRODUCTIVITY,
       isnull(cast(stage_spabiz_BLOCKTIME.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_BLOCKTIME
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_block_time @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_block_time (
       bk_hash,
       block_time_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_BLOCKTIME.bk_hash,
       stage_hash_spabiz_BLOCKTIME.ID block_time_id,
       stage_hash_spabiz_BLOCKTIME.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_BLOCKTIME.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_BLOCKTIME
  left join h_spabiz_block_time
    on stage_hash_spabiz_BLOCKTIME.bk_hash = h_spabiz_block_time.bk_hash
 where h_spabiz_block_time_id is null
   and stage_hash_spabiz_BLOCKTIME.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_block_time
if object_id('tempdb..#l_spabiz_block_time_inserts') is not null drop table #l_spabiz_block_time_inserts
create table #l_spabiz_block_time_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_BLOCKTIME.bk_hash,
       stage_hash_spabiz_BLOCKTIME.ID block_time_id,
       stage_hash_spabiz_BLOCKTIME.STOREID store_id,
       stage_hash_spabiz_BLOCKTIME.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_BLOCKTIME.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLOCKTIME.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLOCKTIME.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLOCKTIME.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_BLOCKTIME
 where stage_hash_spabiz_BLOCKTIME.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_block_time records
set @insert_date_time = getdate()
insert into l_spabiz_block_time (
       bk_hash,
       block_time_id,
       store_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_block_time_inserts.bk_hash,
       #l_spabiz_block_time_inserts.block_time_id,
       #l_spabiz_block_time_inserts.store_id,
       #l_spabiz_block_time_inserts.store_number,
       case when l_spabiz_block_time.l_spabiz_block_time_id is null then isnull(#l_spabiz_block_time_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_block_time_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_block_time_inserts
  left join p_spabiz_block_time
    on #l_spabiz_block_time_inserts.bk_hash = p_spabiz_block_time.bk_hash
   and p_spabiz_block_time.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_block_time
    on p_spabiz_block_time.bk_hash = l_spabiz_block_time.bk_hash
   and p_spabiz_block_time.l_spabiz_block_time_id = l_spabiz_block_time.l_spabiz_block_time_id
 where l_spabiz_block_time.l_spabiz_block_time_id is null
    or (l_spabiz_block_time.l_spabiz_block_time_id is not null
        and l_spabiz_block_time.dv_hash <> #l_spabiz_block_time_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_block_time
if object_id('tempdb..#s_spabiz_block_time_inserts') is not null drop table #s_spabiz_block_time_inserts
create table #s_spabiz_block_time_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_BLOCKTIME.bk_hash,
       stage_hash_spabiz_BLOCKTIME.ID block_time_id,
       stage_hash_spabiz_BLOCKTIME.COUNTERID counter_id,
       stage_hash_spabiz_BLOCKTIME.EDITTIME edit_time,
       stage_hash_spabiz_BLOCKTIME.QUICKID quick_id,
       stage_hash_spabiz_BLOCKTIME.[Delete] block_time_delete,
       stage_hash_spabiz_BLOCKTIME.DELETEDATE delete_date,
       stage_hash_spabiz_BLOCKTIME.NAME name,
       stage_hash_spabiz_BLOCKTIME.STORE_NUMBER store_number,
       stage_hash_spabiz_BLOCKTIME.REDUCESPRODUCTIVITY reduces_productivity,
       isnull(cast(stage_hash_spabiz_BLOCKTIME.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLOCKTIME.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLOCKTIME.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_BLOCKTIME.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_BLOCKTIME.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLOCKTIME.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_BLOCKTIME.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_BLOCKTIME.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLOCKTIME.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_BLOCKTIME.REDUCESPRODUCTIVITY as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_BLOCKTIME
 where stage_hash_spabiz_BLOCKTIME.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_block_time records
set @insert_date_time = getdate()
insert into s_spabiz_block_time (
       bk_hash,
       block_time_id,
       counter_id,
       edit_time,
       quick_id,
       block_time_delete,
       delete_date,
       name,
       store_number,
       reduces_productivity,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_block_time_inserts.bk_hash,
       #s_spabiz_block_time_inserts.block_time_id,
       #s_spabiz_block_time_inserts.counter_id,
       #s_spabiz_block_time_inserts.edit_time,
       #s_spabiz_block_time_inserts.quick_id,
       #s_spabiz_block_time_inserts.block_time_delete,
       #s_spabiz_block_time_inserts.delete_date,
       #s_spabiz_block_time_inserts.name,
       #s_spabiz_block_time_inserts.store_number,
       #s_spabiz_block_time_inserts.reduces_productivity,
       case when s_spabiz_block_time.s_spabiz_block_time_id is null then isnull(#s_spabiz_block_time_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_block_time_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_block_time_inserts
  left join p_spabiz_block_time
    on #s_spabiz_block_time_inserts.bk_hash = p_spabiz_block_time.bk_hash
   and p_spabiz_block_time.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_block_time
    on p_spabiz_block_time.bk_hash = s_spabiz_block_time.bk_hash
   and p_spabiz_block_time.s_spabiz_block_time_id = s_spabiz_block_time.s_spabiz_block_time_id
 where s_spabiz_block_time.s_spabiz_block_time_id is null
    or (s_spabiz_block_time.s_spabiz_block_time_id is not null
        and s_spabiz_block_time.dv_hash <> #s_spabiz_block_time_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_block_time @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_block_time @current_dv_batch_id

end
