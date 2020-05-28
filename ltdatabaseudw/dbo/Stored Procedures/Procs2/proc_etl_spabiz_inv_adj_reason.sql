CREATE PROC [dbo].[proc_etl_spabiz_inv_adj_reason] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_INVADJREASON

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_INVADJREASON (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       RECEIPTPRINTER,
       STORE_NUMBER,
       GLACCOUNT,
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
       RECEIPTPRINTER,
       STORE_NUMBER,
       GLACCOUNT,
       isnull(cast(stage_spabiz_INVADJREASON.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_INVADJREASON
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_inv_adj_reason @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_inv_adj_reason (
       bk_hash,
       inv_adj_reason_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_INVADJREASON.bk_hash,
       stage_hash_spabiz_INVADJREASON.ID inv_adj_reason_id,
       stage_hash_spabiz_INVADJREASON.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_INVADJREASON.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_INVADJREASON
  left join h_spabiz_inv_adj_reason
    on stage_hash_spabiz_INVADJREASON.bk_hash = h_spabiz_inv_adj_reason.bk_hash
 where h_spabiz_inv_adj_reason_id is null
   and stage_hash_spabiz_INVADJREASON.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_inv_adj_reason
if object_id('tempdb..#l_spabiz_inv_adj_reason_inserts') is not null drop table #l_spabiz_inv_adj_reason_inserts
create table #l_spabiz_inv_adj_reason_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_INVADJREASON.bk_hash,
       stage_hash_spabiz_INVADJREASON.ID inv_adj_reason_id,
       stage_hash_spabiz_INVADJREASON.STOREID store_id,
       stage_hash_spabiz_INVADJREASON.STORE_NUMBER store_number,
       stage_hash_spabiz_INVADJREASON.GLACCOUNT gl_account,
       stage_hash_spabiz_INVADJREASON.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJREASON.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJREASON.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJREASON.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INVADJREASON.GLACCOUNT,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_INVADJREASON
 where stage_hash_spabiz_INVADJREASON.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_inv_adj_reason records
set @insert_date_time = getdate()
insert into l_spabiz_inv_adj_reason (
       bk_hash,
       inv_adj_reason_id,
       store_id,
       store_number,
       gl_account,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_inv_adj_reason_inserts.bk_hash,
       #l_spabiz_inv_adj_reason_inserts.inv_adj_reason_id,
       #l_spabiz_inv_adj_reason_inserts.store_id,
       #l_spabiz_inv_adj_reason_inserts.store_number,
       #l_spabiz_inv_adj_reason_inserts.gl_account,
       case when l_spabiz_inv_adj_reason.l_spabiz_inv_adj_reason_id is null then isnull(#l_spabiz_inv_adj_reason_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_inv_adj_reason_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_inv_adj_reason_inserts
  left join p_spabiz_inv_adj_reason
    on #l_spabiz_inv_adj_reason_inserts.bk_hash = p_spabiz_inv_adj_reason.bk_hash
   and p_spabiz_inv_adj_reason.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_inv_adj_reason
    on p_spabiz_inv_adj_reason.bk_hash = l_spabiz_inv_adj_reason.bk_hash
   and p_spabiz_inv_adj_reason.l_spabiz_inv_adj_reason_id = l_spabiz_inv_adj_reason.l_spabiz_inv_adj_reason_id
 where l_spabiz_inv_adj_reason.l_spabiz_inv_adj_reason_id is null
    or (l_spabiz_inv_adj_reason.l_spabiz_inv_adj_reason_id is not null
        and l_spabiz_inv_adj_reason.dv_hash <> #l_spabiz_inv_adj_reason_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_inv_adj_reason
if object_id('tempdb..#s_spabiz_inv_adj_reason_inserts') is not null drop table #s_spabiz_inv_adj_reason_inserts
create table #s_spabiz_inv_adj_reason_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_INVADJREASON.bk_hash,
       stage_hash_spabiz_INVADJREASON.ID inv_adj_reason_id,
       stage_hash_spabiz_INVADJREASON.COUNTERID counter_id,
       stage_hash_spabiz_INVADJREASON.EDITTIME edit_time,
       stage_hash_spabiz_INVADJREASON.[Delete] inv_adj_reason_delete,
       stage_hash_spabiz_INVADJREASON.DELETEDATE delete_date,
       stage_hash_spabiz_INVADJREASON.NAME name,
       stage_hash_spabiz_INVADJREASON.RECEIPTPRINTER receipt_printer,
       stage_hash_spabiz_INVADJREASON.STORE_NUMBER store_number,
       stage_hash_spabiz_INVADJREASON.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJREASON.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJREASON.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INVADJREASON.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJREASON.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INVADJREASON.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INVADJREASON.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INVADJREASON.RECEIPTPRINTER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJREASON.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_INVADJREASON
 where stage_hash_spabiz_INVADJREASON.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_inv_adj_reason records
set @insert_date_time = getdate()
insert into s_spabiz_inv_adj_reason (
       bk_hash,
       inv_adj_reason_id,
       counter_id,
       edit_time,
       inv_adj_reason_delete,
       delete_date,
       name,
       receipt_printer,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_inv_adj_reason_inserts.bk_hash,
       #s_spabiz_inv_adj_reason_inserts.inv_adj_reason_id,
       #s_spabiz_inv_adj_reason_inserts.counter_id,
       #s_spabiz_inv_adj_reason_inserts.edit_time,
       #s_spabiz_inv_adj_reason_inserts.inv_adj_reason_delete,
       #s_spabiz_inv_adj_reason_inserts.delete_date,
       #s_spabiz_inv_adj_reason_inserts.name,
       #s_spabiz_inv_adj_reason_inserts.receipt_printer,
       #s_spabiz_inv_adj_reason_inserts.store_number,
       case when s_spabiz_inv_adj_reason.s_spabiz_inv_adj_reason_id is null then isnull(#s_spabiz_inv_adj_reason_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_inv_adj_reason_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_inv_adj_reason_inserts
  left join p_spabiz_inv_adj_reason
    on #s_spabiz_inv_adj_reason_inserts.bk_hash = p_spabiz_inv_adj_reason.bk_hash
   and p_spabiz_inv_adj_reason.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_inv_adj_reason
    on p_spabiz_inv_adj_reason.bk_hash = s_spabiz_inv_adj_reason.bk_hash
   and p_spabiz_inv_adj_reason.s_spabiz_inv_adj_reason_id = s_spabiz_inv_adj_reason.s_spabiz_inv_adj_reason_id
 where s_spabiz_inv_adj_reason.s_spabiz_inv_adj_reason_id is null
    or (s_spabiz_inv_adj_reason.s_spabiz_inv_adj_reason_id is not null
        and s_spabiz_inv_adj_reason.dv_hash <> #s_spabiz_inv_adj_reason_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_inv_adj_reason @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_inv_adj_reason @current_dv_batch_id

end
