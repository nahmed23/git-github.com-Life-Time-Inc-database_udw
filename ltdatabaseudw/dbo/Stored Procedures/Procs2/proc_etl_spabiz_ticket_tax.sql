CREATE PROC [dbo].[proc_etl_spabiz_ticket_tax] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_TICKETTAX

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_TICKETTAX (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       TAXID,
       AMOUNT,
       Date,
       STATUS,
       SHIFTID,
       DAYID,
       PERIODID,
       COST,
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
       TICKETID,
       TAXID,
       AMOUNT,
       Date,
       STATUS,
       SHIFTID,
       DAYID,
       PERIODID,
       COST,
       STORE_NUMBER,
       GLACCOUNT,
       isnull(cast(stage_spabiz_TICKETTAX.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_TICKETTAX
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ticket_tax @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ticket_tax (
       bk_hash,
       ticket_tax_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_TICKETTAX.bk_hash,
       stage_hash_spabiz_TICKETTAX.ID ticket_tax_id,
       stage_hash_spabiz_TICKETTAX.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_TICKETTAX.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_TICKETTAX
  left join h_spabiz_ticket_tax
    on stage_hash_spabiz_TICKETTAX.bk_hash = h_spabiz_ticket_tax.bk_hash
 where h_spabiz_ticket_tax_id is null
   and stage_hash_spabiz_TICKETTAX.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ticket_tax
if object_id('tempdb..#l_spabiz_ticket_tax_inserts') is not null drop table #l_spabiz_ticket_tax_inserts
create table #l_spabiz_ticket_tax_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETTAX.bk_hash,
       stage_hash_spabiz_TICKETTAX.ID ticket_tax_id,
       stage_hash_spabiz_TICKETTAX.COUNTERID counter_id,
       stage_hash_spabiz_TICKETTAX.STOREID store_id,
       stage_hash_spabiz_TICKETTAX.TICKETID ticket_id,
       stage_hash_spabiz_TICKETTAX.TAXID tax_id,
       stage_hash_spabiz_TICKETTAX.SHIFTID shift_id,
       stage_hash_spabiz_TICKETTAX.DAYID day_id,
       stage_hash_spabiz_TICKETTAX.PERIODID period_id,
       stage_hash_spabiz_TICKETTAX.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETTAX.GLACCOUNT gl_account,
       stage_hash_spabiz_TICKETTAX.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.TICKETID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.TAXID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.SHIFTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.DAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.PERIODID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETTAX.GLACCOUNT,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETTAX
 where stage_hash_spabiz_TICKETTAX.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ticket_tax records
set @insert_date_time = getdate()
insert into l_spabiz_ticket_tax (
       bk_hash,
       ticket_tax_id,
       counter_id,
       store_id,
       ticket_id,
       tax_id,
       shift_id,
       day_id,
       period_id,
       store_number,
       gl_account,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ticket_tax_inserts.bk_hash,
       #l_spabiz_ticket_tax_inserts.ticket_tax_id,
       #l_spabiz_ticket_tax_inserts.counter_id,
       #l_spabiz_ticket_tax_inserts.store_id,
       #l_spabiz_ticket_tax_inserts.ticket_id,
       #l_spabiz_ticket_tax_inserts.tax_id,
       #l_spabiz_ticket_tax_inserts.shift_id,
       #l_spabiz_ticket_tax_inserts.day_id,
       #l_spabiz_ticket_tax_inserts.period_id,
       #l_spabiz_ticket_tax_inserts.store_number,
       #l_spabiz_ticket_tax_inserts.gl_account,
       case when l_spabiz_ticket_tax.l_spabiz_ticket_tax_id is null then isnull(#l_spabiz_ticket_tax_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ticket_tax_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ticket_tax_inserts
  left join p_spabiz_ticket_tax
    on #l_spabiz_ticket_tax_inserts.bk_hash = p_spabiz_ticket_tax.bk_hash
   and p_spabiz_ticket_tax.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ticket_tax
    on p_spabiz_ticket_tax.bk_hash = l_spabiz_ticket_tax.bk_hash
   and p_spabiz_ticket_tax.l_spabiz_ticket_tax_id = l_spabiz_ticket_tax.l_spabiz_ticket_tax_id
 where l_spabiz_ticket_tax.l_spabiz_ticket_tax_id is null
    or (l_spabiz_ticket_tax.l_spabiz_ticket_tax_id is not null
        and l_spabiz_ticket_tax.dv_hash <> #l_spabiz_ticket_tax_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ticket_tax
if object_id('tempdb..#s_spabiz_ticket_tax_inserts') is not null drop table #s_spabiz_ticket_tax_inserts
create table #s_spabiz_ticket_tax_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETTAX.bk_hash,
       stage_hash_spabiz_TICKETTAX.ID ticket_tax_id,
       stage_hash_spabiz_TICKETTAX.EDITTIME edit_time,
       stage_hash_spabiz_TICKETTAX.AMOUNT amount,
       stage_hash_spabiz_TICKETTAX.Date date,
       stage_hash_spabiz_TICKETTAX.STATUS status,
       stage_hash_spabiz_TICKETTAX.COST cost,
       stage_hash_spabiz_TICKETTAX.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETTAX.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETTAX.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETTAX.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.COST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTAX.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETTAX
 where stage_hash_spabiz_TICKETTAX.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ticket_tax records
set @insert_date_time = getdate()
insert into s_spabiz_ticket_tax (
       bk_hash,
       ticket_tax_id,
       edit_time,
       amount,
       date,
       status,
       cost,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ticket_tax_inserts.bk_hash,
       #s_spabiz_ticket_tax_inserts.ticket_tax_id,
       #s_spabiz_ticket_tax_inserts.edit_time,
       #s_spabiz_ticket_tax_inserts.amount,
       #s_spabiz_ticket_tax_inserts.date,
       #s_spabiz_ticket_tax_inserts.status,
       #s_spabiz_ticket_tax_inserts.cost,
       #s_spabiz_ticket_tax_inserts.store_number,
       case when s_spabiz_ticket_tax.s_spabiz_ticket_tax_id is null then isnull(#s_spabiz_ticket_tax_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ticket_tax_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ticket_tax_inserts
  left join p_spabiz_ticket_tax
    on #s_spabiz_ticket_tax_inserts.bk_hash = p_spabiz_ticket_tax.bk_hash
   and p_spabiz_ticket_tax.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ticket_tax
    on p_spabiz_ticket_tax.bk_hash = s_spabiz_ticket_tax.bk_hash
   and p_spabiz_ticket_tax.s_spabiz_ticket_tax_id = s_spabiz_ticket_tax.s_spabiz_ticket_tax_id
 where s_spabiz_ticket_tax.s_spabiz_ticket_tax_id is null
    or (s_spabiz_ticket_tax.s_spabiz_ticket_tax_id is not null
        and s_spabiz_ticket_tax.dv_hash <> #s_spabiz_ticket_tax_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ticket_tax @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ticket_tax @current_dv_batch_id

end
