CREATE PROC [dbo].[proc_etl_spabiz_ticket_tip] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_TICKETTIP

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_TICKETTIP (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       TICKETNUM,
       Date,
       CUSTID,
       STATUS,
       SHIFTID,
       AMOUNT,
       SPLIT,
       STAFFID,
       PAID,
       LAYERID,
       STORE_NUMBER,
       GLACCOUNT,
       CREDITCARD,
       PAIDDRAWERID,
       ISAUTOGRATUITY,
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
       TICKETNUM,
       Date,
       CUSTID,
       STATUS,
       SHIFTID,
       AMOUNT,
       SPLIT,
       STAFFID,
       PAID,
       LAYERID,
       STORE_NUMBER,
       GLACCOUNT,
       CREDITCARD,
       PAIDDRAWERID,
       ISAUTOGRATUITY,
       isnull(cast(stage_spabiz_TICKETTIP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_TICKETTIP
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ticket_tip @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ticket_tip (
       bk_hash,
       ticket_tip_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_TICKETTIP.bk_hash,
       stage_hash_spabiz_TICKETTIP.ID ticket_tip_id,
       stage_hash_spabiz_TICKETTIP.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_TICKETTIP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_TICKETTIP
  left join h_spabiz_ticket_tip
    on stage_hash_spabiz_TICKETTIP.bk_hash = h_spabiz_ticket_tip.bk_hash
 where h_spabiz_ticket_tip_id is null
   and stage_hash_spabiz_TICKETTIP.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ticket_tip
if object_id('tempdb..#l_spabiz_ticket_tip_inserts') is not null drop table #l_spabiz_ticket_tip_inserts
create table #l_spabiz_ticket_tip_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETTIP.bk_hash,
       stage_hash_spabiz_TICKETTIP.ID ticket_tip_id,
       stage_hash_spabiz_TICKETTIP.STOREID store_id,
       stage_hash_spabiz_TICKETTIP.TICKETID ticket_id,
       stage_hash_spabiz_TICKETTIP.TICKETNUM ticket_num,
       stage_hash_spabiz_TICKETTIP.CUSTID cust_id,
       stage_hash_spabiz_TICKETTIP.SHIFTID shift_id,
       stage_hash_spabiz_TICKETTIP.STAFFID staff_id,
       stage_hash_spabiz_TICKETTIP.LAYERID layer_id,
       stage_hash_spabiz_TICKETTIP.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETTIP.GLACCOUNT gl_account,
       stage_hash_spabiz_TICKETTIP.PAIDDRAWERID paid_drawer_id,
       stage_hash_spabiz_TICKETTIP.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.TICKETID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETTIP.TICKETNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.SHIFTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.LAYERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETTIP.GLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.PAIDDRAWERID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETTIP
 where stage_hash_spabiz_TICKETTIP.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ticket_tip records
set @insert_date_time = getdate()
insert into l_spabiz_ticket_tip (
       bk_hash,
       ticket_tip_id,
       store_id,
       ticket_id,
       ticket_num,
       cust_id,
       shift_id,
       staff_id,
       layer_id,
       store_number,
       gl_account,
       paid_drawer_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ticket_tip_inserts.bk_hash,
       #l_spabiz_ticket_tip_inserts.ticket_tip_id,
       #l_spabiz_ticket_tip_inserts.store_id,
       #l_spabiz_ticket_tip_inserts.ticket_id,
       #l_spabiz_ticket_tip_inserts.ticket_num,
       #l_spabiz_ticket_tip_inserts.cust_id,
       #l_spabiz_ticket_tip_inserts.shift_id,
       #l_spabiz_ticket_tip_inserts.staff_id,
       #l_spabiz_ticket_tip_inserts.layer_id,
       #l_spabiz_ticket_tip_inserts.store_number,
       #l_spabiz_ticket_tip_inserts.gl_account,
       #l_spabiz_ticket_tip_inserts.paid_drawer_id,
       case when l_spabiz_ticket_tip.l_spabiz_ticket_tip_id is null then isnull(#l_spabiz_ticket_tip_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ticket_tip_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ticket_tip_inserts
  left join p_spabiz_ticket_tip
    on #l_spabiz_ticket_tip_inserts.bk_hash = p_spabiz_ticket_tip.bk_hash
   and p_spabiz_ticket_tip.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ticket_tip
    on p_spabiz_ticket_tip.bk_hash = l_spabiz_ticket_tip.bk_hash
   and p_spabiz_ticket_tip.l_spabiz_ticket_tip_id = l_spabiz_ticket_tip.l_spabiz_ticket_tip_id
 where l_spabiz_ticket_tip.l_spabiz_ticket_tip_id is null
    or (l_spabiz_ticket_tip.l_spabiz_ticket_tip_id is not null
        and l_spabiz_ticket_tip.dv_hash <> #l_spabiz_ticket_tip_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ticket_tip
if object_id('tempdb..#s_spabiz_ticket_tip_inserts') is not null drop table #s_spabiz_ticket_tip_inserts
create table #s_spabiz_ticket_tip_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETTIP.bk_hash,
       stage_hash_spabiz_TICKETTIP.ID ticket_tip_id,
       stage_hash_spabiz_TICKETTIP.COUNTERID counter_id,
       stage_hash_spabiz_TICKETTIP.EDITTIME edit_time,
       stage_hash_spabiz_TICKETTIP.Date date,
       stage_hash_spabiz_TICKETTIP.STATUS status,
       stage_hash_spabiz_TICKETTIP.AMOUNT amount,
       stage_hash_spabiz_TICKETTIP.SPLIT split,
       stage_hash_spabiz_TICKETTIP.PAID paid,
       stage_hash_spabiz_TICKETTIP.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETTIP.CREDITCARD credit_card,
       stage_hash_spabiz_TICKETTIP.ISAUTOGRATUITY is_auto_gratuity,
       stage_hash_spabiz_TICKETTIP.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETTIP.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETTIP.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.SPLIT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.PAID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.CREDITCARD as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETTIP.ISAUTOGRATUITY as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETTIP
 where stage_hash_spabiz_TICKETTIP.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ticket_tip records
set @insert_date_time = getdate()
insert into s_spabiz_ticket_tip (
       bk_hash,
       ticket_tip_id,
       counter_id,
       edit_time,
       date,
       status,
       amount,
       split,
       paid,
       store_number,
       credit_card,
       is_auto_gratuity,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ticket_tip_inserts.bk_hash,
       #s_spabiz_ticket_tip_inserts.ticket_tip_id,
       #s_spabiz_ticket_tip_inserts.counter_id,
       #s_spabiz_ticket_tip_inserts.edit_time,
       #s_spabiz_ticket_tip_inserts.date,
       #s_spabiz_ticket_tip_inserts.status,
       #s_spabiz_ticket_tip_inserts.amount,
       #s_spabiz_ticket_tip_inserts.split,
       #s_spabiz_ticket_tip_inserts.paid,
       #s_spabiz_ticket_tip_inserts.store_number,
       #s_spabiz_ticket_tip_inserts.credit_card,
       #s_spabiz_ticket_tip_inserts.is_auto_gratuity,
       case when s_spabiz_ticket_tip.s_spabiz_ticket_tip_id is null then isnull(#s_spabiz_ticket_tip_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ticket_tip_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ticket_tip_inserts
  left join p_spabiz_ticket_tip
    on #s_spabiz_ticket_tip_inserts.bk_hash = p_spabiz_ticket_tip.bk_hash
   and p_spabiz_ticket_tip.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ticket_tip
    on p_spabiz_ticket_tip.bk_hash = s_spabiz_ticket_tip.bk_hash
   and p_spabiz_ticket_tip.s_spabiz_ticket_tip_id = s_spabiz_ticket_tip.s_spabiz_ticket_tip_id
 where s_spabiz_ticket_tip.s_spabiz_ticket_tip_id is null
    or (s_spabiz_ticket_tip.s_spabiz_ticket_tip_id is not null
        and s_spabiz_ticket_tip.dv_hash <> #s_spabiz_ticket_tip_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ticket_tip @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ticket_tip @current_dv_batch_id

end
