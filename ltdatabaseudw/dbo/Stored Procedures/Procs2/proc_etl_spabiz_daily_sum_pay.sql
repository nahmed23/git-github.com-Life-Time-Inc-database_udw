﻿CREATE PROC [dbo].[proc_etl_spabiz_daily_sum_pay] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_DAILYSUMPAY

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_DAILYSUMPAY (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       DAYID,
       PAYID,
       Date,
       STARTAMOUNT,
       TICKETNUM,
       TICKETAMT,
       CHANGEOUT,
       DRAWERENTRIES,
       YOUHAVE,
       ERROR,
       DEPOSIT,
       TOTAL,
       DAY_PAYINDEX,
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
       DAYID,
       PAYID,
       Date,
       STARTAMOUNT,
       TICKETNUM,
       TICKETAMT,
       CHANGEOUT,
       DRAWERENTRIES,
       YOUHAVE,
       ERROR,
       DEPOSIT,
       TOTAL,
       DAY_PAYINDEX,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_DAILYSUMPAY.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_DAILYSUMPAY
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_daily_sum_pay @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_daily_sum_pay (
       bk_hash,
       daily_sum_pay_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_DAILYSUMPAY.bk_hash,
       stage_hash_spabiz_DAILYSUMPAY.ID daily_sum_pay_id,
       stage_hash_spabiz_DAILYSUMPAY.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_DAILYSUMPAY.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_DAILYSUMPAY
  left join h_spabiz_daily_sum_pay
    on stage_hash_spabiz_DAILYSUMPAY.bk_hash = h_spabiz_daily_sum_pay.bk_hash
 where h_spabiz_daily_sum_pay_id is null
   and stage_hash_spabiz_DAILYSUMPAY.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_daily_sum_pay
if object_id('tempdb..#l_spabiz_daily_sum_pay_inserts') is not null drop table #l_spabiz_daily_sum_pay_inserts
create table #l_spabiz_daily_sum_pay_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_DAILYSUMPAY.bk_hash,
       stage_hash_spabiz_DAILYSUMPAY.ID daily_sum_pay_id,
       stage_hash_spabiz_DAILYSUMPAY.STOREID store_id,
       stage_hash_spabiz_DAILYSUMPAY.DAYID day_id,
       stage_hash_spabiz_DAILYSUMPAY.PAYID pay_id,
       stage_hash_spabiz_DAILYSUMPAY.TICKETNUM ticket_num,
       stage_hash_spabiz_DAILYSUMPAY.DAY_PAYINDEX day_pay_index,
       stage_hash_spabiz_DAILYSUMPAY.STORE_NUMBER store_number,
       stage_hash_spabiz_DAILYSUMPAY.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.DAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.PAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.TICKETNUM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_DAILYSUMPAY.DAY_PAYINDEX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_DAILYSUMPAY
 where stage_hash_spabiz_DAILYSUMPAY.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_daily_sum_pay records
set @insert_date_time = getdate()
insert into l_spabiz_daily_sum_pay (
       bk_hash,
       daily_sum_pay_id,
       store_id,
       day_id,
       pay_id,
       ticket_num,
       day_pay_index,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_daily_sum_pay_inserts.bk_hash,
       #l_spabiz_daily_sum_pay_inserts.daily_sum_pay_id,
       #l_spabiz_daily_sum_pay_inserts.store_id,
       #l_spabiz_daily_sum_pay_inserts.day_id,
       #l_spabiz_daily_sum_pay_inserts.pay_id,
       #l_spabiz_daily_sum_pay_inserts.ticket_num,
       #l_spabiz_daily_sum_pay_inserts.day_pay_index,
       #l_spabiz_daily_sum_pay_inserts.store_number,
       case when l_spabiz_daily_sum_pay.l_spabiz_daily_sum_pay_id is null then isnull(#l_spabiz_daily_sum_pay_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_daily_sum_pay_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_daily_sum_pay_inserts
  left join p_spabiz_daily_sum_pay
    on #l_spabiz_daily_sum_pay_inserts.bk_hash = p_spabiz_daily_sum_pay.bk_hash
   and p_spabiz_daily_sum_pay.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_daily_sum_pay
    on p_spabiz_daily_sum_pay.bk_hash = l_spabiz_daily_sum_pay.bk_hash
   and p_spabiz_daily_sum_pay.l_spabiz_daily_sum_pay_id = l_spabiz_daily_sum_pay.l_spabiz_daily_sum_pay_id
 where l_spabiz_daily_sum_pay.l_spabiz_daily_sum_pay_id is null
    or (l_spabiz_daily_sum_pay.l_spabiz_daily_sum_pay_id is not null
        and l_spabiz_daily_sum_pay.dv_hash <> #l_spabiz_daily_sum_pay_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_daily_sum_pay
if object_id('tempdb..#s_spabiz_daily_sum_pay_inserts') is not null drop table #s_spabiz_daily_sum_pay_inserts
create table #s_spabiz_daily_sum_pay_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_DAILYSUMPAY.bk_hash,
       stage_hash_spabiz_DAILYSUMPAY.ID daily_sum_pay_id,
       stage_hash_spabiz_DAILYSUMPAY.COUNTERID counter_id,
       stage_hash_spabiz_DAILYSUMPAY.EDITTIME edit_time,
       stage_hash_spabiz_DAILYSUMPAY.Date date,
       stage_hash_spabiz_DAILYSUMPAY.STARTAMOUNT start_amount,
       stage_hash_spabiz_DAILYSUMPAY.TICKETAMT ticket_amt,
       stage_hash_spabiz_DAILYSUMPAY.CHANGEOUT change_out,
       stage_hash_spabiz_DAILYSUMPAY.DRAWERENTRIES drawer_entries,
       stage_hash_spabiz_DAILYSUMPAY.YOUHAVE you_have,
       stage_hash_spabiz_DAILYSUMPAY.ERROR error,
       stage_hash_spabiz_DAILYSUMPAY.DEPOSIT deposit,
       stage_hash_spabiz_DAILYSUMPAY.TOTAL total,
       stage_hash_spabiz_DAILYSUMPAY.STORE_NUMBER store_number,
       stage_hash_spabiz_DAILYSUMPAY.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DAILYSUMPAY.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_DAILYSUMPAY.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.STARTAMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.TICKETAMT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.CHANGEOUT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.DRAWERENTRIES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.YOUHAVE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.ERROR as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.DEPOSIT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.TOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_DAILYSUMPAY.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_DAILYSUMPAY
 where stage_hash_spabiz_DAILYSUMPAY.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_daily_sum_pay records
set @insert_date_time = getdate()
insert into s_spabiz_daily_sum_pay (
       bk_hash,
       daily_sum_pay_id,
       counter_id,
       edit_time,
       date,
       start_amount,
       ticket_amt,
       change_out,
       drawer_entries,
       you_have,
       error,
       deposit,
       total,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_daily_sum_pay_inserts.bk_hash,
       #s_spabiz_daily_sum_pay_inserts.daily_sum_pay_id,
       #s_spabiz_daily_sum_pay_inserts.counter_id,
       #s_spabiz_daily_sum_pay_inserts.edit_time,
       #s_spabiz_daily_sum_pay_inserts.date,
       #s_spabiz_daily_sum_pay_inserts.start_amount,
       #s_spabiz_daily_sum_pay_inserts.ticket_amt,
       #s_spabiz_daily_sum_pay_inserts.change_out,
       #s_spabiz_daily_sum_pay_inserts.drawer_entries,
       #s_spabiz_daily_sum_pay_inserts.you_have,
       #s_spabiz_daily_sum_pay_inserts.error,
       #s_spabiz_daily_sum_pay_inserts.deposit,
       #s_spabiz_daily_sum_pay_inserts.total,
       #s_spabiz_daily_sum_pay_inserts.store_number,
       case when s_spabiz_daily_sum_pay.s_spabiz_daily_sum_pay_id is null then isnull(#s_spabiz_daily_sum_pay_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_daily_sum_pay_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_daily_sum_pay_inserts
  left join p_spabiz_daily_sum_pay
    on #s_spabiz_daily_sum_pay_inserts.bk_hash = p_spabiz_daily_sum_pay.bk_hash
   and p_spabiz_daily_sum_pay.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_daily_sum_pay
    on p_spabiz_daily_sum_pay.bk_hash = s_spabiz_daily_sum_pay.bk_hash
   and p_spabiz_daily_sum_pay.s_spabiz_daily_sum_pay_id = s_spabiz_daily_sum_pay.s_spabiz_daily_sum_pay_id
 where s_spabiz_daily_sum_pay.s_spabiz_daily_sum_pay_id is null
    or (s_spabiz_daily_sum_pay.s_spabiz_daily_sum_pay_id is not null
        and s_spabiz_daily_sum_pay.dv_hash <> #s_spabiz_daily_sum_pay_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_daily_sum_pay @current_dv_batch_id

end
