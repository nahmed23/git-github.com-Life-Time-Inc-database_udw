CREATE PROC [dbo].[proc_etl_spabiz_INV_ADJ] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_INVADJ

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_INVADJ (
       bk_hash,
       [ID],
       COUNTERID,
       STOREID,
       EDITTIME,
       Date,
       STAFFID,
       STATUS,
       NUM,
       TOTAL,
       STORE_NUMBER,
       GLACCOUNT,
       TRANSFERLOC,
       TRANSFERROID,
       TRANSFERVENDORID,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([ID] as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [ID],
       COUNTERID,
       STOREID,
       EDITTIME,
       Date,
       STAFFID,
       STATUS,
       NUM,
       TOTAL,
       STORE_NUMBER,
       GLACCOUNT,
       TRANSFERLOC,
       TRANSFERROID,
       TRANSFERVENDORID,
       isnull(cast(stage_spabiz_INVADJ.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_INVADJ
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_inv_adj @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_inv_adj (
       bk_hash,
       inv_adj_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_INVADJ.bk_hash,
       stage_hash_spabiz_INVADJ.[ID] inv_adj_id,
       stage_hash_spabiz_INVADJ.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_INVADJ.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_INVADJ
  left join h_spabiz_inv_adj
    on stage_hash_spabiz_INVADJ.bk_hash = h_spabiz_inv_adj.bk_hash
 where h_spabiz_inv_adj_id is null
   and stage_hash_spabiz_INVADJ.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_inv_adj
if object_id('tempdb..#l_spabiz_inv_adj_inserts') is not null drop table #l_spabiz_inv_adj_inserts
create table #l_spabiz_inv_adj_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_INVADJ.bk_hash,
       stage_hash_spabiz_INVADJ.[ID] inv_adj_id,
       stage_hash_spabiz_INVADJ.COUNTERID counter_id,
       stage_hash_spabiz_INVADJ.STOREID store_id,
       stage_hash_spabiz_INVADJ.STAFFID staff_id,
       stage_hash_spabiz_INVADJ.STORE_NUMBER store_number,
       stage_hash_spabiz_INVADJ.GLACCOUNT gl_account,
       stage_hash_spabiz_INVADJ.TRANSFERLOC transfer_loc,
       stage_hash_spabiz_INVADJ.TRANSFERROID transfer_ro_id,
       stage_hash_spabiz_INVADJ.TRANSFERVENDORID transfer_vendor_id,
       stage_hash_spabiz_INVADJ.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INVADJ.GLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.TRANSFERLOC as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.TRANSFERROID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.TRANSFERVENDORID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_INVADJ
 where stage_hash_spabiz_INVADJ.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_inv_adj records
set @insert_date_time = getdate()
insert into l_spabiz_inv_adj (
       bk_hash,
       inv_adj_id,
       counter_id,
       store_id,
       staff_id,
       store_number,
       gl_account,
       transfer_loc,
       transfer_ro_id,
       transfer_vendor_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_inv_adj_inserts.bk_hash,
       #l_spabiz_inv_adj_inserts.inv_adj_id,
       #l_spabiz_inv_adj_inserts.counter_id,
       #l_spabiz_inv_adj_inserts.store_id,
       #l_spabiz_inv_adj_inserts.staff_id,
       #l_spabiz_inv_adj_inserts.store_number,
       #l_spabiz_inv_adj_inserts.gl_account,
       #l_spabiz_inv_adj_inserts.transfer_loc,
       #l_spabiz_inv_adj_inserts.transfer_ro_id,
       #l_spabiz_inv_adj_inserts.transfer_vendor_id,
       case when l_spabiz_inv_adj.l_spabiz_inv_adj_id is null then isnull(#l_spabiz_inv_adj_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_inv_adj_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_inv_adj_inserts
  left join p_spabiz_inv_adj
    on #l_spabiz_inv_adj_inserts.bk_hash = p_spabiz_inv_adj.bk_hash
   and p_spabiz_inv_adj.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_inv_adj
    on p_spabiz_inv_adj.bk_hash = l_spabiz_inv_adj.bk_hash
   and p_spabiz_inv_adj.l_spabiz_inv_adj_id = l_spabiz_inv_adj.l_spabiz_inv_adj_id
 where l_spabiz_inv_adj.l_spabiz_inv_adj_id is null
    or (l_spabiz_inv_adj.l_spabiz_inv_adj_id is not null
        and l_spabiz_inv_adj.dv_hash <> #l_spabiz_inv_adj_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_inv_adj
if object_id('tempdb..#s_spabiz_inv_adj_inserts') is not null drop table #s_spabiz_inv_adj_inserts
create table #s_spabiz_inv_adj_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_INVADJ.bk_hash,
       stage_hash_spabiz_INVADJ.[ID] inv_adj_id,
       stage_hash_spabiz_INVADJ.EDITTIME edit_time,
       stage_hash_spabiz_INVADJ.Date date,
       stage_hash_spabiz_INVADJ.STATUS status,
       stage_hash_spabiz_INVADJ.NUM num,
       stage_hash_spabiz_INVADJ.TOTAL total,
       stage_hash_spabiz_INVADJ.STORE_NUMBER store_number,
       stage_hash_spabiz_INVADJ.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.[ID] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INVADJ.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INVADJ.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_INVADJ.NUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.TOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJ.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_INVADJ
 where stage_hash_spabiz_INVADJ.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_inv_adj records
set @insert_date_time = getdate()
insert into s_spabiz_inv_adj (
       bk_hash,
       inv_adj_id,
       edit_time,
       date,
       status,
       num,
       total,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_inv_adj_inserts.bk_hash,
       #s_spabiz_inv_adj_inserts.inv_adj_id,
       #s_spabiz_inv_adj_inserts.edit_time,
       #s_spabiz_inv_adj_inserts.date,
       #s_spabiz_inv_adj_inserts.status,
       #s_spabiz_inv_adj_inserts.num,
       #s_spabiz_inv_adj_inserts.total,
       #s_spabiz_inv_adj_inserts.store_number,
       case when s_spabiz_inv_adj.s_spabiz_inv_adj_id is null then isnull(#s_spabiz_inv_adj_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_inv_adj_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_inv_adj_inserts
  left join p_spabiz_inv_adj
    on #s_spabiz_inv_adj_inserts.bk_hash = p_spabiz_inv_adj.bk_hash
   and p_spabiz_inv_adj.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_inv_adj
    on p_spabiz_inv_adj.bk_hash = s_spabiz_inv_adj.bk_hash
   and p_spabiz_inv_adj.s_spabiz_inv_adj_id = s_spabiz_inv_adj.s_spabiz_inv_adj_id
 where s_spabiz_inv_adj.s_spabiz_inv_adj_id is null
    or (s_spabiz_inv_adj.s_spabiz_inv_adj_id is not null
        and s_spabiz_inv_adj.dv_hash <> #s_spabiz_inv_adj_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_inv_adj @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_inv_adj @current_dv_batch_id

end
