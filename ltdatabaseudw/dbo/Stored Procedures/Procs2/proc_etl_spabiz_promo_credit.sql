CREATE PROC [dbo].[proc_etl_spabiz_promo_credit] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_PROMOCREDIT

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_PROMOCREDIT (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       STAFFID,
       STATUS,
       CUSTID,
       PROMOID,
       SERIALNUM,
       AMOUNT,
       BALANCE,
       Date,
       DAYSGOOD,
       LASTUSED,
       ADDRESS1,
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
       STAFFID,
       STATUS,
       CUSTID,
       PROMOID,
       SERIALNUM,
       AMOUNT,
       BALANCE,
       Date,
       DAYSGOOD,
       LASTUSED,
       ADDRESS1,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_PROMOCREDIT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_PROMOCREDIT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_promo_credit @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_promo_credit (
       bk_hash,
       promo_credit_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_PROMOCREDIT.bk_hash,
       stage_hash_spabiz_PROMOCREDIT.ID promo_credit_id,
       stage_hash_spabiz_PROMOCREDIT.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_PROMOCREDIT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_PROMOCREDIT
  left join h_spabiz_promo_credit
    on stage_hash_spabiz_PROMOCREDIT.bk_hash = h_spabiz_promo_credit.bk_hash
 where h_spabiz_promo_credit_id is null
   and stage_hash_spabiz_PROMOCREDIT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_promo_credit
if object_id('tempdb..#l_spabiz_promo_credit_inserts') is not null drop table #l_spabiz_promo_credit_inserts
create table #l_spabiz_promo_credit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PROMOCREDIT.bk_hash,
       stage_hash_spabiz_PROMOCREDIT.ID promo_credit_id,
       stage_hash_spabiz_PROMOCREDIT.STOREID store_id,
       stage_hash_spabiz_PROMOCREDIT.STAFFID staff_id,
       stage_hash_spabiz_PROMOCREDIT.CUSTID cust_id,
       stage_hash_spabiz_PROMOCREDIT.PROMOID promo_id,
       stage_hash_spabiz_PROMOCREDIT.STORE_NUMBER store_number,
       stage_hash_spabiz_PROMOCREDIT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.PROMOID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PROMOCREDIT
 where stage_hash_spabiz_PROMOCREDIT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_promo_credit records
set @insert_date_time = getdate()
insert into l_spabiz_promo_credit (
       bk_hash,
       promo_credit_id,
       store_id,
       staff_id,
       cust_id,
       promo_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_promo_credit_inserts.bk_hash,
       #l_spabiz_promo_credit_inserts.promo_credit_id,
       #l_spabiz_promo_credit_inserts.store_id,
       #l_spabiz_promo_credit_inserts.staff_id,
       #l_spabiz_promo_credit_inserts.cust_id,
       #l_spabiz_promo_credit_inserts.promo_id,
       #l_spabiz_promo_credit_inserts.store_number,
       case when l_spabiz_promo_credit.l_spabiz_promo_credit_id is null then isnull(#l_spabiz_promo_credit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_promo_credit_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_promo_credit_inserts
  left join p_spabiz_promo_credit
    on #l_spabiz_promo_credit_inserts.bk_hash = p_spabiz_promo_credit.bk_hash
   and p_spabiz_promo_credit.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_promo_credit
    on p_spabiz_promo_credit.bk_hash = l_spabiz_promo_credit.bk_hash
   and p_spabiz_promo_credit.l_spabiz_promo_credit_id = l_spabiz_promo_credit.l_spabiz_promo_credit_id
 where l_spabiz_promo_credit.l_spabiz_promo_credit_id is null
    or (l_spabiz_promo_credit.l_spabiz_promo_credit_id is not null
        and l_spabiz_promo_credit.dv_hash <> #l_spabiz_promo_credit_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_promo_credit
if object_id('tempdb..#s_spabiz_promo_credit_inserts') is not null drop table #s_spabiz_promo_credit_inserts
create table #s_spabiz_promo_credit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PROMOCREDIT.bk_hash,
       stage_hash_spabiz_PROMOCREDIT.ID promo_credit_id,
       stage_hash_spabiz_PROMOCREDIT.COUNTERID counter_id,
       stage_hash_spabiz_PROMOCREDIT.EDITTIME edit_time,
       stage_hash_spabiz_PROMOCREDIT.STATUS status,
       stage_hash_spabiz_PROMOCREDIT.SERIALNUM serial_num,
       stage_hash_spabiz_PROMOCREDIT.AMOUNT amount,
       stage_hash_spabiz_PROMOCREDIT.BALANCE balance,
       stage_hash_spabiz_PROMOCREDIT.Date date,
       stage_hash_spabiz_PROMOCREDIT.DAYSGOOD days_good,
       stage_hash_spabiz_PROMOCREDIT.LASTUSED last_used,
       stage_hash_spabiz_PROMOCREDIT.ADDRESS1 address_1,
       stage_hash_spabiz_PROMOCREDIT.STORE_NUMBER store_number,
       stage_hash_spabiz_PROMOCREDIT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PROMOCREDIT.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PROMOCREDIT.SERIALNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.BALANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PROMOCREDIT.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.DAYSGOOD as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PROMOCREDIT.LASTUSED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PROMOCREDIT.ADDRESS1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PROMOCREDIT.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PROMOCREDIT
 where stage_hash_spabiz_PROMOCREDIT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_promo_credit records
set @insert_date_time = getdate()
insert into s_spabiz_promo_credit (
       bk_hash,
       promo_credit_id,
       counter_id,
       edit_time,
       status,
       serial_num,
       amount,
       balance,
       date,
       days_good,
       last_used,
       address_1,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_promo_credit_inserts.bk_hash,
       #s_spabiz_promo_credit_inserts.promo_credit_id,
       #s_spabiz_promo_credit_inserts.counter_id,
       #s_spabiz_promo_credit_inserts.edit_time,
       #s_spabiz_promo_credit_inserts.status,
       #s_spabiz_promo_credit_inserts.serial_num,
       #s_spabiz_promo_credit_inserts.amount,
       #s_spabiz_promo_credit_inserts.balance,
       #s_spabiz_promo_credit_inserts.date,
       #s_spabiz_promo_credit_inserts.days_good,
       #s_spabiz_promo_credit_inserts.last_used,
       #s_spabiz_promo_credit_inserts.address_1,
       #s_spabiz_promo_credit_inserts.store_number,
       case when s_spabiz_promo_credit.s_spabiz_promo_credit_id is null then isnull(#s_spabiz_promo_credit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_promo_credit_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_promo_credit_inserts
  left join p_spabiz_promo_credit
    on #s_spabiz_promo_credit_inserts.bk_hash = p_spabiz_promo_credit.bk_hash
   and p_spabiz_promo_credit.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_promo_credit
    on p_spabiz_promo_credit.bk_hash = s_spabiz_promo_credit.bk_hash
   and p_spabiz_promo_credit.s_spabiz_promo_credit_id = s_spabiz_promo_credit.s_spabiz_promo_credit_id
 where s_spabiz_promo_credit.s_spabiz_promo_credit_id is null
    or (s_spabiz_promo_credit.s_spabiz_promo_credit_id is not null
        and s_spabiz_promo_credit.dv_hash <> #s_spabiz_promo_credit_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_promo_credit @current_dv_batch_id

end
