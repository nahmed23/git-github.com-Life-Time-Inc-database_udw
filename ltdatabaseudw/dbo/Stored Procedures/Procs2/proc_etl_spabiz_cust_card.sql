CREATE PROC [dbo].[proc_etl_spabiz_cust_card] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_CUSTCARD

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_CUSTCARD (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       SERIALNUM,
       Date,
       CUSTCARDID,
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
       PRICE,
       TOTALSALES,
       YTDSALES,
       PRODUCTSALES,
       SERVICESALES,
       STORE_NUMBER,
       DELETED,
       MEMTYPE,
       NEXTBILLINGDATE,
       RECURRING,
       RECURRING_DECLINED,
       RECURRING_DECLINED_REASON,
       CURRENT_INSTALLMENT,
       RECURRING_AFTER_EXPIRE,
       PRORATED_AMOUNT,
       INITIAL_AMOUNT,
       CANCELLED,
       CANCELID,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       SERIALNUM,
       Date,
       CUSTCARDID,
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
       PRICE,
       TOTALSALES,
       YTDSALES,
       PRODUCTSALES,
       SERVICESALES,
       STORE_NUMBER,
       DELETED,
       MEMTYPE,
       NEXTBILLINGDATE,
       RECURRING,
       RECURRING_DECLINED,
       RECURRING_DECLINED_REASON,
       CURRENT_INSTALLMENT,
       RECURRING_AFTER_EXPIRE,
       PRORATED_AMOUNT,
       INITIAL_AMOUNT,
       CANCELLED,
       CANCELID,
       isnull(cast(stage_spabiz_CUSTCARD.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_spabiz_CUSTCARD
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_cust_card @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_cust_card (
       bk_hash,
       cust_card_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_CUSTCARD.bk_hash,
       stage_hash_spabiz_CUSTCARD.ID cust_card_id,
       stage_hash_spabiz_CUSTCARD.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_CUSTCARD.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_CUSTCARD
  left join h_spabiz_cust_card
    on stage_hash_spabiz_CUSTCARD.bk_hash = h_spabiz_cust_card.bk_hash
 where h_spabiz_cust_card_id is null
   and stage_hash_spabiz_CUSTCARD.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_cust_card
if object_id('tempdb..#l_spabiz_cust_card_inserts') is not null drop table #l_spabiz_cust_card_inserts
create table #l_spabiz_cust_card_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CUSTCARD.bk_hash,
       stage_hash_spabiz_CUSTCARD.ID cust_card_id,
       stage_hash_spabiz_CUSTCARD.STOREID store_id,
       stage_hash_spabiz_CUSTCARD.TICKETID ticket_id,
       stage_hash_spabiz_CUSTCARD.CUSTCARDID cust_card_type_id,
       stage_hash_spabiz_CUSTCARD.STAFFIDCREATE staff_id_create,
       stage_hash_spabiz_CUSTCARD.STAFFID1 staff_id_1,
       stage_hash_spabiz_CUSTCARD.STAFFID2 staff_id_2,
       stage_hash_spabiz_CUSTCARD.BUY_CUSTID buy_cust_id,
       stage_hash_spabiz_CUSTCARD.CUSTID cust_id,
       stage_hash_spabiz_CUSTCARD.STORE_NUMBER store_number,
       stage_hash_spabiz_CUSTCARD.CANCELID cancel_id,
       isnull(cast(stage_hash_spabiz_CUSTCARD.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.ID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.STOREID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.TICKETID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.CUSTCARDID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.STAFFIDCREATE as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.STAFFID1 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.STAFFID2 as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.BUY_CUSTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.CUSTID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.STORE_NUMBER as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.CANCELID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CUSTCARD
 where stage_hash_spabiz_CUSTCARD.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_cust_card records
set @insert_date_time = getdate()
insert into l_spabiz_cust_card (
       bk_hash,
       cust_card_id,
       store_id,
       ticket_id,
       cust_card_type_id,
       staff_id_create,
       staff_id_1,
       staff_id_2,
       buy_cust_id,
       cust_id,
       store_number,
       cancel_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_cust_card_inserts.bk_hash,
       #l_spabiz_cust_card_inserts.cust_card_id,
       #l_spabiz_cust_card_inserts.store_id,
       #l_spabiz_cust_card_inserts.ticket_id,
       #l_spabiz_cust_card_inserts.cust_card_type_id,
       #l_spabiz_cust_card_inserts.staff_id_create,
       #l_spabiz_cust_card_inserts.staff_id_1,
       #l_spabiz_cust_card_inserts.staff_id_2,
       #l_spabiz_cust_card_inserts.buy_cust_id,
       #l_spabiz_cust_card_inserts.cust_id,
       #l_spabiz_cust_card_inserts.store_number,
       #l_spabiz_cust_card_inserts.cancel_id,
       case when l_spabiz_cust_card.l_spabiz_cust_card_id is null then isnull(#l_spabiz_cust_card_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_cust_card_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_cust_card_inserts
  left join p_spabiz_cust_card
    on #l_spabiz_cust_card_inserts.bk_hash = p_spabiz_cust_card.bk_hash
   and p_spabiz_cust_card.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_cust_card
    on p_spabiz_cust_card.bk_hash = l_spabiz_cust_card.bk_hash
   and p_spabiz_cust_card.l_spabiz_cust_card_id = l_spabiz_cust_card.l_spabiz_cust_card_id
 where l_spabiz_cust_card.l_spabiz_cust_card_id is null
    or (l_spabiz_cust_card.l_spabiz_cust_card_id is not null
        and l_spabiz_cust_card.dv_hash <> #l_spabiz_cust_card_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_cust_card
if object_id('tempdb..#s_spabiz_cust_card_inserts') is not null drop table #s_spabiz_cust_card_inserts
create table #s_spabiz_cust_card_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CUSTCARD.bk_hash,
       stage_hash_spabiz_CUSTCARD.ID cust_card_id,
       stage_hash_spabiz_CUSTCARD.COUNTERID counter_id,
       stage_hash_spabiz_CUSTCARD.EDITTIME edit_time,
       stage_hash_spabiz_CUSTCARD.SERIALNUM serial_num,
       stage_hash_spabiz_CUSTCARD.Date date,
       stage_hash_spabiz_CUSTCARD.DAYSGOOD days_good,
       stage_hash_spabiz_CUSTCARD.EXPDATE exp_date,
       stage_hash_spabiz_CUSTCARD.STATUS status,
       stage_hash_spabiz_CUSTCARD.MESSAGE message,
       stage_hash_spabiz_CUSTCARD.LASTUSED last_used,
       stage_hash_spabiz_CUSTCARD.NOTE note,
       stage_hash_spabiz_CUSTCARD.PRICE price,
       stage_hash_spabiz_CUSTCARD.TOTALSALES total_sales,
       stage_hash_spabiz_CUSTCARD.YTDSALES ytd_sales,
       stage_hash_spabiz_CUSTCARD.PRODUCTSALES product_sales,
       stage_hash_spabiz_CUSTCARD.SERVICESALES service_sales,
       stage_hash_spabiz_CUSTCARD.STORE_NUMBER store_number,
       stage_hash_spabiz_CUSTCARD.DELETED deleted,
       stage_hash_spabiz_CUSTCARD.MEMTYPE mem_type,
       stage_hash_spabiz_CUSTCARD.NEXTBILLINGDATE next_billing_date,
       stage_hash_spabiz_CUSTCARD.RECURRING recurring,
       stage_hash_spabiz_CUSTCARD.RECURRING_DECLINED recurring_declined,
       stage_hash_spabiz_CUSTCARD.RECURRING_DECLINED_REASON recurring_declined_reason,
       stage_hash_spabiz_CUSTCARD.CURRENT_INSTALLMENT current_installment,
       stage_hash_spabiz_CUSTCARD.RECURRING_AFTER_EXPIRE recurring_after_expire,
       stage_hash_spabiz_CUSTCARD.PRORATED_AMOUNT prorated_amount,
       stage_hash_spabiz_CUSTCARD.INITIAL_AMOUNT initial_amount,
       stage_hash_spabiz_CUSTCARD.CANCELLED cancelled,
       isnull(cast(stage_hash_spabiz_CUSTCARD.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.ID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.COUNTERID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTCARD.EDITTIME,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTCARD.SERIALNUM,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTCARD.Date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.DAYSGOOD as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTCARD.EXPDATE,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.STATUS as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTCARD.MESSAGE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTCARD.LASTUSED,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTCARD.NOTE,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.PRICE as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.TOTALSALES as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.YTDSALES as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.PRODUCTSALES as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.SERVICESALES as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.STORE_NUMBER as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.DELETED as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.MEMTYPE as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTCARD.NEXTBILLINGDATE,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.RECURRING as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.RECURRING_DECLINED as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTCARD.RECURRING_DECLINED_REASON,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.CURRENT_INSTALLMENT as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.RECURRING_AFTER_EXPIRE as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.PRORATED_AMOUNT as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.INITIAL_AMOUNT as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARD.CANCELLED as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CUSTCARD
 where stage_hash_spabiz_CUSTCARD.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_cust_card records
set @insert_date_time = getdate()
insert into s_spabiz_cust_card (
       bk_hash,
       cust_card_id,
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
       price,
       total_sales,
       ytd_sales,
       product_sales,
       service_sales,
       store_number,
       deleted,
       mem_type,
       next_billing_date,
       recurring,
       recurring_declined,
       recurring_declined_reason,
       current_installment,
       recurring_after_expire,
       prorated_amount,
       initial_amount,
       cancelled,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_cust_card_inserts.bk_hash,
       #s_spabiz_cust_card_inserts.cust_card_id,
       #s_spabiz_cust_card_inserts.counter_id,
       #s_spabiz_cust_card_inserts.edit_time,
       #s_spabiz_cust_card_inserts.serial_num,
       #s_spabiz_cust_card_inserts.date,
       #s_spabiz_cust_card_inserts.days_good,
       #s_spabiz_cust_card_inserts.exp_date,
       #s_spabiz_cust_card_inserts.status,
       #s_spabiz_cust_card_inserts.message,
       #s_spabiz_cust_card_inserts.last_used,
       #s_spabiz_cust_card_inserts.note,
       #s_spabiz_cust_card_inserts.price,
       #s_spabiz_cust_card_inserts.total_sales,
       #s_spabiz_cust_card_inserts.ytd_sales,
       #s_spabiz_cust_card_inserts.product_sales,
       #s_spabiz_cust_card_inserts.service_sales,
       #s_spabiz_cust_card_inserts.store_number,
       #s_spabiz_cust_card_inserts.deleted,
       #s_spabiz_cust_card_inserts.mem_type,
       #s_spabiz_cust_card_inserts.next_billing_date,
       #s_spabiz_cust_card_inserts.recurring,
       #s_spabiz_cust_card_inserts.recurring_declined,
       #s_spabiz_cust_card_inserts.recurring_declined_reason,
       #s_spabiz_cust_card_inserts.current_installment,
       #s_spabiz_cust_card_inserts.recurring_after_expire,
       #s_spabiz_cust_card_inserts.prorated_amount,
       #s_spabiz_cust_card_inserts.initial_amount,
       #s_spabiz_cust_card_inserts.cancelled,
       case when s_spabiz_cust_card.s_spabiz_cust_card_id is null then isnull(#s_spabiz_cust_card_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_cust_card_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_cust_card_inserts
  left join p_spabiz_cust_card
    on #s_spabiz_cust_card_inserts.bk_hash = p_spabiz_cust_card.bk_hash
   and p_spabiz_cust_card.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_cust_card
    on p_spabiz_cust_card.bk_hash = s_spabiz_cust_card.bk_hash
   and p_spabiz_cust_card.s_spabiz_cust_card_id = s_spabiz_cust_card.s_spabiz_cust_card_id
 where s_spabiz_cust_card.s_spabiz_cust_card_id is null
    or (s_spabiz_cust_card.s_spabiz_cust_card_id is not null
        and s_spabiz_cust_card.dv_hash <> #s_spabiz_cust_card_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_cust_card @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_cust_card @current_dv_batch_id

end
