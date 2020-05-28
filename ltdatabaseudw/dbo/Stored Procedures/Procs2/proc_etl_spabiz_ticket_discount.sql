CREATE PROC [dbo].[proc_etl_spabiz_ticket_discount] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_TICKETDISCOUNT

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_TICKETDISCOUNT (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       Date,
       CUSTID,
       DISCOUNTID,
       AMOUNT,
       [PERCENT],
       STATUS,
       SHIFTID,
       DAYID,
       PERIODID,
       DOUBLEIT,
       PRODUCTID,
       STORE_NUMBER,
       GLACCOUNT,
       CREATEDBYSTAFF,
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
       CUSTID,
       DISCOUNTID,
       AMOUNT,
       [PERCENT],
       STATUS,
       SHIFTID,
       DAYID,
       PERIODID,
       DOUBLEIT,
       PRODUCTID,
       STORE_NUMBER,
       GLACCOUNT,
       CREATEDBYSTAFF,
       isnull(cast(stage_spabiz_TICKETDISCOUNT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_TICKETDISCOUNT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ticket_discount @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ticket_discount (
       bk_hash,
       ticket_discount_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_TICKETDISCOUNT.bk_hash,
       stage_hash_spabiz_TICKETDISCOUNT.ID ticket_discount_id,
       stage_hash_spabiz_TICKETDISCOUNT.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_TICKETDISCOUNT
  left join h_spabiz_ticket_discount
    on stage_hash_spabiz_TICKETDISCOUNT.bk_hash = h_spabiz_ticket_discount.bk_hash
 where h_spabiz_ticket_discount_id is null
   and stage_hash_spabiz_TICKETDISCOUNT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ticket_discount
if object_id('tempdb..#l_spabiz_ticket_discount_inserts') is not null drop table #l_spabiz_ticket_discount_inserts
create table #l_spabiz_ticket_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETDISCOUNT.bk_hash,
       stage_hash_spabiz_TICKETDISCOUNT.ID ticket_discount_id,
       stage_hash_spabiz_TICKETDISCOUNT.STOREID store_id,
       stage_hash_spabiz_TICKETDISCOUNT.TICKETID ticket_id,
       stage_hash_spabiz_TICKETDISCOUNT.CUSTID cust_id,
       stage_hash_spabiz_TICKETDISCOUNT.DISCOUNTID discount_id,
       stage_hash_spabiz_TICKETDISCOUNT.SHIFTID shift_id,
       stage_hash_spabiz_TICKETDISCOUNT.DAYID day_id,
       stage_hash_spabiz_TICKETDISCOUNT.PERIODID period_id,
       stage_hash_spabiz_TICKETDISCOUNT.PRODUCTID product_id,
       stage_hash_spabiz_TICKETDISCOUNT.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETDISCOUNT.GLACCOUNT gl_account,
       stage_hash_spabiz_TICKETDISCOUNT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.TICKETID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.DISCOUNTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.SHIFTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.DAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.PERIODID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.PRODUCTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETDISCOUNT.GLACCOUNT,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETDISCOUNT
 where stage_hash_spabiz_TICKETDISCOUNT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ticket_discount records
set @insert_date_time = getdate()
insert into l_spabiz_ticket_discount (
       bk_hash,
       ticket_discount_id,
       store_id,
       ticket_id,
       cust_id,
       discount_id,
       shift_id,
       day_id,
       period_id,
       product_id,
       store_number,
       gl_account,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ticket_discount_inserts.bk_hash,
       #l_spabiz_ticket_discount_inserts.ticket_discount_id,
       #l_spabiz_ticket_discount_inserts.store_id,
       #l_spabiz_ticket_discount_inserts.ticket_id,
       #l_spabiz_ticket_discount_inserts.cust_id,
       #l_spabiz_ticket_discount_inserts.discount_id,
       #l_spabiz_ticket_discount_inserts.shift_id,
       #l_spabiz_ticket_discount_inserts.day_id,
       #l_spabiz_ticket_discount_inserts.period_id,
       #l_spabiz_ticket_discount_inserts.product_id,
       #l_spabiz_ticket_discount_inserts.store_number,
       #l_spabiz_ticket_discount_inserts.gl_account,
       case when l_spabiz_ticket_discount.l_spabiz_ticket_discount_id is null then isnull(#l_spabiz_ticket_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ticket_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ticket_discount_inserts
  left join p_spabiz_ticket_discount
    on #l_spabiz_ticket_discount_inserts.bk_hash = p_spabiz_ticket_discount.bk_hash
   and p_spabiz_ticket_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ticket_discount
    on p_spabiz_ticket_discount.bk_hash = l_spabiz_ticket_discount.bk_hash
   and p_spabiz_ticket_discount.l_spabiz_ticket_discount_id = l_spabiz_ticket_discount.l_spabiz_ticket_discount_id
 where l_spabiz_ticket_discount.l_spabiz_ticket_discount_id is null
    or (l_spabiz_ticket_discount.l_spabiz_ticket_discount_id is not null
        and l_spabiz_ticket_discount.dv_hash <> #l_spabiz_ticket_discount_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ticket_discount
if object_id('tempdb..#s_spabiz_ticket_discount_inserts') is not null drop table #s_spabiz_ticket_discount_inserts
create table #s_spabiz_ticket_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETDISCOUNT.bk_hash,
       stage_hash_spabiz_TICKETDISCOUNT.ID ticket_discount_id,
       stage_hash_spabiz_TICKETDISCOUNT.COUNTERID counter_id,
       stage_hash_spabiz_TICKETDISCOUNT.EDITTIME edit_time,
       stage_hash_spabiz_TICKETDISCOUNT.Date date,
       stage_hash_spabiz_TICKETDISCOUNT.AMOUNT amount,
       stage_hash_spabiz_TICKETDISCOUNT.[PERCENT] ticket_discount_percent,
       stage_hash_spabiz_TICKETDISCOUNT.STATUS status,
       stage_hash_spabiz_TICKETDISCOUNT.DOUBLEIT double_it,
       stage_hash_spabiz_TICKETDISCOUNT.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETDISCOUNT.CREATEDBYSTAFF created_by_staff,
       stage_hash_spabiz_TICKETDISCOUNT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETDISCOUNT.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETDISCOUNT.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.[PERCENT] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.DOUBLEIT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDISCOUNT.CREATEDBYSTAFF as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETDISCOUNT
 where stage_hash_spabiz_TICKETDISCOUNT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ticket_discount records
set @insert_date_time = getdate()
insert into s_spabiz_ticket_discount (
       bk_hash,
       ticket_discount_id,
       counter_id,
       edit_time,
       date,
       amount,
       ticket_discount_percent,
       status,
       double_it,
       store_number,
       created_by_staff,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ticket_discount_inserts.bk_hash,
       #s_spabiz_ticket_discount_inserts.ticket_discount_id,
       #s_spabiz_ticket_discount_inserts.counter_id,
       #s_spabiz_ticket_discount_inserts.edit_time,
       #s_spabiz_ticket_discount_inserts.date,
       #s_spabiz_ticket_discount_inserts.amount,
       #s_spabiz_ticket_discount_inserts.ticket_discount_percent,
       #s_spabiz_ticket_discount_inserts.status,
       #s_spabiz_ticket_discount_inserts.double_it,
       #s_spabiz_ticket_discount_inserts.store_number,
       #s_spabiz_ticket_discount_inserts.created_by_staff,
       case when s_spabiz_ticket_discount.s_spabiz_ticket_discount_id is null then isnull(#s_spabiz_ticket_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ticket_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ticket_discount_inserts
  left join p_spabiz_ticket_discount
    on #s_spabiz_ticket_discount_inserts.bk_hash = p_spabiz_ticket_discount.bk_hash
   and p_spabiz_ticket_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ticket_discount
    on p_spabiz_ticket_discount.bk_hash = s_spabiz_ticket_discount.bk_hash
   and p_spabiz_ticket_discount.s_spabiz_ticket_discount_id = s_spabiz_ticket_discount.s_spabiz_ticket_discount_id
 where s_spabiz_ticket_discount.s_spabiz_ticket_discount_id is null
    or (s_spabiz_ticket_discount.s_spabiz_ticket_discount_id is not null
        and s_spabiz_ticket_discount.dv_hash <> #s_spabiz_ticket_discount_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ticket_discount @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ticket_discount @current_dv_batch_id

end
