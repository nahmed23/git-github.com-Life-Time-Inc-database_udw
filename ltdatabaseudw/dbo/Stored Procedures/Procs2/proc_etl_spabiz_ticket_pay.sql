CREATE PROC [dbo].[proc_etl_spabiz_ticket_pay] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_TICKETPAY

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_TICKETPAY (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       Date,
       PAYNUM,
       PAYID,
       PAYTYPE,
       CUSTID,
       STATUS,
       SHIFTID,
       DAYID,
       PERIODID,
       REFID,
       OK,
       AMOUNT,
       APPROVAL,
       STAFFID,
       STORE_NUMBER,
       GLACCOUNT,
       ACQ,
       ADJUSTED,
       CARDIS,
       PAYCOUNTER,
       PROCESSDATA,
       REFNO,
       TOKEN1,
       TOKEN2,
       TOKEN3,
       SBCC,
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
       Date,
       PAYNUM,
       PAYID,
       PAYTYPE,
       CUSTID,
       STATUS,
       SHIFTID,
       DAYID,
       PERIODID,
       REFID,
       OK,
       AMOUNT,
       APPROVAL,
       STAFFID,
       STORE_NUMBER,
       GLACCOUNT,
       ACQ,
       ADJUSTED,
       CARDIS,
       PAYCOUNTER,
       PROCESSDATA,
       REFNO,
       TOKEN1,
       TOKEN2,
       TOKEN3,
       SBCC,
       isnull(cast(stage_spabiz_TICKETPAY.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_TICKETPAY
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ticket_pay @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ticket_pay (
       bk_hash,
       ticket_pay_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_TICKETPAY.bk_hash,
       stage_hash_spabiz_TICKETPAY.ID ticket_pay_id,
       stage_hash_spabiz_TICKETPAY.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_TICKETPAY.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_TICKETPAY
  left join h_spabiz_ticket_pay
    on stage_hash_spabiz_TICKETPAY.bk_hash = h_spabiz_ticket_pay.bk_hash
 where h_spabiz_ticket_pay_id is null
   and stage_hash_spabiz_TICKETPAY.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ticket_pay
if object_id('tempdb..#l_spabiz_ticket_pay_inserts') is not null drop table #l_spabiz_ticket_pay_inserts
create table #l_spabiz_ticket_pay_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETPAY.bk_hash,
       stage_hash_spabiz_TICKETPAY.ID ticket_pay_id,
       stage_hash_spabiz_TICKETPAY.STOREID store_id,
       stage_hash_spabiz_TICKETPAY.TICKETID ticket_id,
       stage_hash_spabiz_TICKETPAY.PAYID pay_id,
       stage_hash_spabiz_TICKETPAY.PAYTYPE pay_type,
       stage_hash_spabiz_TICKETPAY.CUSTID cust_id,
       stage_hash_spabiz_TICKETPAY.SHIFTID shift_id,
       stage_hash_spabiz_TICKETPAY.DAYID day_id,
       stage_hash_spabiz_TICKETPAY.PERIODID period_id,
       stage_hash_spabiz_TICKETPAY.REFID ref_id,
       stage_hash_spabiz_TICKETPAY.STAFFID staff_id,
       stage_hash_spabiz_TICKETPAY.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETPAY.GLACCOUNT gl_account,
       stage_hash_spabiz_TICKETPAY.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.TICKETID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.PAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.PAYTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.SHIFTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.DAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.PERIODID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.REFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.GLACCOUNT,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETPAY
 where stage_hash_spabiz_TICKETPAY.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ticket_pay records
set @insert_date_time = getdate()
insert into l_spabiz_ticket_pay (
       bk_hash,
       ticket_pay_id,
       store_id,
       ticket_id,
       pay_id,
       pay_type,
       cust_id,
       shift_id,
       day_id,
       period_id,
       ref_id,
       staff_id,
       store_number,
       gl_account,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ticket_pay_inserts.bk_hash,
       #l_spabiz_ticket_pay_inserts.ticket_pay_id,
       #l_spabiz_ticket_pay_inserts.store_id,
       #l_spabiz_ticket_pay_inserts.ticket_id,
       #l_spabiz_ticket_pay_inserts.pay_id,
       #l_spabiz_ticket_pay_inserts.pay_type,
       #l_spabiz_ticket_pay_inserts.cust_id,
       #l_spabiz_ticket_pay_inserts.shift_id,
       #l_spabiz_ticket_pay_inserts.day_id,
       #l_spabiz_ticket_pay_inserts.period_id,
       #l_spabiz_ticket_pay_inserts.ref_id,
       #l_spabiz_ticket_pay_inserts.staff_id,
       #l_spabiz_ticket_pay_inserts.store_number,
       #l_spabiz_ticket_pay_inserts.gl_account,
       case when l_spabiz_ticket_pay.l_spabiz_ticket_pay_id is null then isnull(#l_spabiz_ticket_pay_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ticket_pay_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ticket_pay_inserts
  left join p_spabiz_ticket_pay
    on #l_spabiz_ticket_pay_inserts.bk_hash = p_spabiz_ticket_pay.bk_hash
   and p_spabiz_ticket_pay.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ticket_pay
    on p_spabiz_ticket_pay.bk_hash = l_spabiz_ticket_pay.bk_hash
   and p_spabiz_ticket_pay.l_spabiz_ticket_pay_id = l_spabiz_ticket_pay.l_spabiz_ticket_pay_id
 where l_spabiz_ticket_pay.l_spabiz_ticket_pay_id is null
    or (l_spabiz_ticket_pay.l_spabiz_ticket_pay_id is not null
        and l_spabiz_ticket_pay.dv_hash <> #l_spabiz_ticket_pay_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ticket_pay
if object_id('tempdb..#s_spabiz_ticket_pay_inserts') is not null drop table #s_spabiz_ticket_pay_inserts
create table #s_spabiz_ticket_pay_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETPAY.bk_hash,
       stage_hash_spabiz_TICKETPAY.ID ticket_pay_id,
       stage_hash_spabiz_TICKETPAY.COUNTERID counter_id,
       stage_hash_spabiz_TICKETPAY.EDITTIME edit_time,
       stage_hash_spabiz_TICKETPAY.Date date,
       stage_hash_spabiz_TICKETPAY.PAYNUM pay_num,
       stage_hash_spabiz_TICKETPAY.STATUS status,
       stage_hash_spabiz_TICKETPAY.OK ok,
       stage_hash_spabiz_TICKETPAY.AMOUNT amount,
       stage_hash_spabiz_TICKETPAY.APPROVAL approval,
       stage_hash_spabiz_TICKETPAY.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETPAY.ACQ acq,
       stage_hash_spabiz_TICKETPAY.ADJUSTED adjusted,
       stage_hash_spabiz_TICKETPAY.CARDIS card_is,
       stage_hash_spabiz_TICKETPAY.PAYCOUNTER pay_counter,
       stage_hash_spabiz_TICKETPAY.PROCESSDATA process_data,
       stage_hash_spabiz_TICKETPAY.REFNO ref_no,
       stage_hash_spabiz_TICKETPAY.TOKEN1 token_1,
       stage_hash_spabiz_TICKETPAY.TOKEN2 token_2,
       stage_hash_spabiz_TICKETPAY.TOKEN3 token_3,
       stage_hash_spabiz_TICKETPAY.SBCC sbcc,
       stage_hash_spabiz_TICKETPAY.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETPAY.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETPAY.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.PAYNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.OK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.APPROVAL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.ACQ,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.ADJUSTED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.CARDIS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.PAYCOUNTER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.PROCESSDATA,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.REFNO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.TOKEN1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.TOKEN2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETPAY.TOKEN3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETPAY.SBCC as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETPAY
 where stage_hash_spabiz_TICKETPAY.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ticket_pay records
set @insert_date_time = getdate()
insert into s_spabiz_ticket_pay (
       bk_hash,
       ticket_pay_id,
       counter_id,
       edit_time,
       date,
       pay_num,
       status,
       ok,
       amount,
       approval,
       store_number,
       acq,
       adjusted,
       card_is,
       pay_counter,
       process_data,
       ref_no,
       token_1,
       token_2,
       token_3,
       sbcc,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ticket_pay_inserts.bk_hash,
       #s_spabiz_ticket_pay_inserts.ticket_pay_id,
       #s_spabiz_ticket_pay_inserts.counter_id,
       #s_spabiz_ticket_pay_inserts.edit_time,
       #s_spabiz_ticket_pay_inserts.date,
       #s_spabiz_ticket_pay_inserts.pay_num,
       #s_spabiz_ticket_pay_inserts.status,
       #s_spabiz_ticket_pay_inserts.ok,
       #s_spabiz_ticket_pay_inserts.amount,
       #s_spabiz_ticket_pay_inserts.approval,
       #s_spabiz_ticket_pay_inserts.store_number,
       #s_spabiz_ticket_pay_inserts.acq,
       #s_spabiz_ticket_pay_inserts.adjusted,
       #s_spabiz_ticket_pay_inserts.card_is,
       #s_spabiz_ticket_pay_inserts.pay_counter,
       #s_spabiz_ticket_pay_inserts.process_data,
       #s_spabiz_ticket_pay_inserts.ref_no,
       #s_spabiz_ticket_pay_inserts.token_1,
       #s_spabiz_ticket_pay_inserts.token_2,
       #s_spabiz_ticket_pay_inserts.token_3,
       #s_spabiz_ticket_pay_inserts.sbcc,
       case when s_spabiz_ticket_pay.s_spabiz_ticket_pay_id is null then isnull(#s_spabiz_ticket_pay_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ticket_pay_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ticket_pay_inserts
  left join p_spabiz_ticket_pay
    on #s_spabiz_ticket_pay_inserts.bk_hash = p_spabiz_ticket_pay.bk_hash
   and p_spabiz_ticket_pay.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ticket_pay
    on p_spabiz_ticket_pay.bk_hash = s_spabiz_ticket_pay.bk_hash
   and p_spabiz_ticket_pay.s_spabiz_ticket_pay_id = s_spabiz_ticket_pay.s_spabiz_ticket_pay_id
 where s_spabiz_ticket_pay.s_spabiz_ticket_pay_id is null
    or (s_spabiz_ticket_pay.s_spabiz_ticket_pay_id is not null
        and s_spabiz_ticket_pay.dv_hash <> #s_spabiz_ticket_pay_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ticket_pay @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ticket_pay @current_dv_batch_id

end
