CREATE PROC [dbo].[proc_etl_spabiz_gift_certificate] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_GIFTCERTIFICATE

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_GIFTCERTIFICATE (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       SERIALNUM,
       Date,
       GIFTID,
       STAFFIDCREATE,
       STAFFID1,
       STAFFID2,
       DAYSGOOD,
       EXPDATE,
       BUY_CUSTID,
       CUSTID,
       STATUS,
       MESSAGE,
       LASTUSED,
       NOTE,
       AMOUNT,
       BALANCE,
       SELLAMOUNT,
       STORE_NUMBER,
       WEBMETADATA,
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
       SERIALNUM,
       Date,
       GIFTID,
       STAFFIDCREATE,
       STAFFID1,
       STAFFID2,
       DAYSGOOD,
       EXPDATE,
       BUY_CUSTID,
       CUSTID,
       STATUS,
       MESSAGE,
       LASTUSED,
       NOTE,
       AMOUNT,
       BALANCE,
       SELLAMOUNT,
       STORE_NUMBER,
       WEBMETADATA,
       isnull(cast(stage_spabiz_GIFTCERTIFICATE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_GIFTCERTIFICATE
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_gift_certificate @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_gift_certificate (
       bk_hash,
       gift_certificate_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_GIFTCERTIFICATE.bk_hash,
       stage_hash_spabiz_GIFTCERTIFICATE.ID gift_certificate_id,
       stage_hash_spabiz_GIFTCERTIFICATE.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_GIFTCERTIFICATE
  left join h_spabiz_gift_certificate
    on stage_hash_spabiz_GIFTCERTIFICATE.bk_hash = h_spabiz_gift_certificate.bk_hash
 where h_spabiz_gift_certificate_id is null
   and stage_hash_spabiz_GIFTCERTIFICATE.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_gift_certificate
if object_id('tempdb..#l_spabiz_gift_certificate_inserts') is not null drop table #l_spabiz_gift_certificate_inserts
create table #l_spabiz_gift_certificate_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_GIFTCERTIFICATE.bk_hash,
       stage_hash_spabiz_GIFTCERTIFICATE.ID gift_certificate_id,
       stage_hash_spabiz_GIFTCERTIFICATE.STOREID store_id,
       stage_hash_spabiz_GIFTCERTIFICATE.TICKETID ticket_id,
       stage_hash_spabiz_GIFTCERTIFICATE.GIFTID gift_id,
       stage_hash_spabiz_GIFTCERTIFICATE.STAFFIDCREATE staff_id_create,
       stage_hash_spabiz_GIFTCERTIFICATE.STAFFID1 staff_id_1,
       stage_hash_spabiz_GIFTCERTIFICATE.STAFFID2 staff_id_2,
       stage_hash_spabiz_GIFTCERTIFICATE.BUY_CUSTID buy_cust_id,
       stage_hash_spabiz_GIFTCERTIFICATE.CUSTID cust_id,
       stage_hash_spabiz_GIFTCERTIFICATE.STORE_NUMBER store_number,
       stage_hash_spabiz_GIFTCERTIFICATE.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.TICKETID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.GIFTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.STAFFIDCREATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.STAFFID1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.STAFFID2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.BUY_CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_GIFTCERTIFICATE
 where stage_hash_spabiz_GIFTCERTIFICATE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_gift_certificate records
set @insert_date_time = getdate()
insert into l_spabiz_gift_certificate (
       bk_hash,
       gift_certificate_id,
       store_id,
       ticket_id,
       gift_id,
       staff_id_create,
       staff_id_1,
       staff_id_2,
       buy_cust_id,
       cust_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_gift_certificate_inserts.bk_hash,
       #l_spabiz_gift_certificate_inserts.gift_certificate_id,
       #l_spabiz_gift_certificate_inserts.store_id,
       #l_spabiz_gift_certificate_inserts.ticket_id,
       #l_spabiz_gift_certificate_inserts.gift_id,
       #l_spabiz_gift_certificate_inserts.staff_id_create,
       #l_spabiz_gift_certificate_inserts.staff_id_1,
       #l_spabiz_gift_certificate_inserts.staff_id_2,
       #l_spabiz_gift_certificate_inserts.buy_cust_id,
       #l_spabiz_gift_certificate_inserts.cust_id,
       #l_spabiz_gift_certificate_inserts.store_number,
       case when l_spabiz_gift_certificate.l_spabiz_gift_certificate_id is null then isnull(#l_spabiz_gift_certificate_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_gift_certificate_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_gift_certificate_inserts
  left join p_spabiz_gift_certificate
    on #l_spabiz_gift_certificate_inserts.bk_hash = p_spabiz_gift_certificate.bk_hash
   and p_spabiz_gift_certificate.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_gift_certificate
    on p_spabiz_gift_certificate.bk_hash = l_spabiz_gift_certificate.bk_hash
   and p_spabiz_gift_certificate.l_spabiz_gift_certificate_id = l_spabiz_gift_certificate.l_spabiz_gift_certificate_id
 where l_spabiz_gift_certificate.l_spabiz_gift_certificate_id is null
    or (l_spabiz_gift_certificate.l_spabiz_gift_certificate_id is not null
        and l_spabiz_gift_certificate.dv_hash <> #l_spabiz_gift_certificate_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_gift_certificate
if object_id('tempdb..#s_spabiz_gift_certificate_inserts') is not null drop table #s_spabiz_gift_certificate_inserts
create table #s_spabiz_gift_certificate_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_GIFTCERTIFICATE.bk_hash,
       stage_hash_spabiz_GIFTCERTIFICATE.ID gift_certificate_id,
       stage_hash_spabiz_GIFTCERTIFICATE.COUNTERID counter_id,
       stage_hash_spabiz_GIFTCERTIFICATE.EDITTIME edit_time,
       stage_hash_spabiz_GIFTCERTIFICATE.SERIALNUM serial_num,
       stage_hash_spabiz_GIFTCERTIFICATE.Date date,
       stage_hash_spabiz_GIFTCERTIFICATE.DAYSGOOD days_good,
       stage_hash_spabiz_GIFTCERTIFICATE.EXPDATE exp_date,
       stage_hash_spabiz_GIFTCERTIFICATE.STATUS status,
       stage_hash_spabiz_GIFTCERTIFICATE.MESSAGE message,
       stage_hash_spabiz_GIFTCERTIFICATE.LASTUSED last_used,
       stage_hash_spabiz_GIFTCERTIFICATE.NOTE note,
       stage_hash_spabiz_GIFTCERTIFICATE.AMOUNT amount,
       stage_hash_spabiz_GIFTCERTIFICATE.BALANCE balance,
       stage_hash_spabiz_GIFTCERTIFICATE.SELLAMOUNT sell_amount,
       stage_hash_spabiz_GIFTCERTIFICATE.STORE_NUMBER store_number,
       stage_hash_spabiz_GIFTCERTIFICATE.WEBMETADATA web_meta_data,
       stage_hash_spabiz_GIFTCERTIFICATE.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_GIFTCERTIFICATE.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_GIFTCERTIFICATE.SERIALNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_GIFTCERTIFICATE.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.DAYSGOOD as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_GIFTCERTIFICATE.EXPDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_GIFTCERTIFICATE.MESSAGE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_GIFTCERTIFICATE.LASTUSED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_GIFTCERTIFICATE.NOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.BALANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.SELLAMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_GIFTCERTIFICATE.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_GIFTCERTIFICATE.WEBMETADATA,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_GIFTCERTIFICATE
 where stage_hash_spabiz_GIFTCERTIFICATE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_gift_certificate records
set @insert_date_time = getdate()
insert into s_spabiz_gift_certificate (
       bk_hash,
       gift_certificate_id,
       counter_id,
       edit_time,
       serial_num,
       date,
       days_good,
       exp_date,
       status,
       message,
       last_used,
       note,
       amount,
       balance,
       sell_amount,
       store_number,
       web_meta_data,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_gift_certificate_inserts.bk_hash,
       #s_spabiz_gift_certificate_inserts.gift_certificate_id,
       #s_spabiz_gift_certificate_inserts.counter_id,
       #s_spabiz_gift_certificate_inserts.edit_time,
       #s_spabiz_gift_certificate_inserts.serial_num,
       #s_spabiz_gift_certificate_inserts.date,
       #s_spabiz_gift_certificate_inserts.days_good,
       #s_spabiz_gift_certificate_inserts.exp_date,
       #s_spabiz_gift_certificate_inserts.status,
       #s_spabiz_gift_certificate_inserts.message,
       #s_spabiz_gift_certificate_inserts.last_used,
       #s_spabiz_gift_certificate_inserts.note,
       #s_spabiz_gift_certificate_inserts.amount,
       #s_spabiz_gift_certificate_inserts.balance,
       #s_spabiz_gift_certificate_inserts.sell_amount,
       #s_spabiz_gift_certificate_inserts.store_number,
       #s_spabiz_gift_certificate_inserts.web_meta_data,
       case when s_spabiz_gift_certificate.s_spabiz_gift_certificate_id is null then isnull(#s_spabiz_gift_certificate_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_gift_certificate_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_gift_certificate_inserts
  left join p_spabiz_gift_certificate
    on #s_spabiz_gift_certificate_inserts.bk_hash = p_spabiz_gift_certificate.bk_hash
   and p_spabiz_gift_certificate.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_gift_certificate
    on p_spabiz_gift_certificate.bk_hash = s_spabiz_gift_certificate.bk_hash
   and p_spabiz_gift_certificate.s_spabiz_gift_certificate_id = s_spabiz_gift_certificate.s_spabiz_gift_certificate_id
 where s_spabiz_gift_certificate.s_spabiz_gift_certificate_id is null
    or (s_spabiz_gift_certificate.s_spabiz_gift_certificate_id is not null
        and s_spabiz_gift_certificate.dv_hash <> #s_spabiz_gift_certificate_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_gift_certificate @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_gift_certificate @current_dv_batch_id

end
