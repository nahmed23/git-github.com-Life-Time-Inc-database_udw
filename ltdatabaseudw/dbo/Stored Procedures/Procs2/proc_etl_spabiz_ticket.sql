CREATE PROC [dbo].[proc_etl_spabiz_ticket] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_TICKET

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_TICKET (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       CUSTID,
       TICKETNUM,
       TICKETID,
       Date,
       STAFFID,
       VOIDERID,
       VOIDREASONID,
       STATUS,
       SALES_SUBTOTAL,
       SALES_TOTAL,
       SALES_PRODUCTTOTAL,
       SALES_SERVICETOTAL,
       CASHCHANGE,
       DISCOUNTTOTAL,
       DISCOUNT_SERVICE,
       DISCOUNT_PRODUCT,
       TAXTOTAL,
       NODEID,
       SHIFTID,
       DAYID,
       PERIODID,
       SHIFTCREATEDID,
       APID,
       CHECKINTIME,
       LATE,
       TIP,
       USEDIMAGESCRIPT,
       CUSTBALANCE,
       PAYTYPEID,
       TIME,
       NEXTSTAFFID,
       CHECKINSTATUS,
       DISCOUNTDBL,
       PERFORMEDVALUEADDEDSERV,
       PRODUCTONLY,
       SERVICEONLY,
       HASPRODUCT,
       HASSERVICE,
       HASONLYPRODUCT,
       HASONLYSERVICE,
       HASBOTH,
       SALES_GIFTTOTAL,
       SALES_PACKAGETOTAL,
       SALES_SERIESTOTAL,
       STORE_NUMBER,
       NOTE,
       PROMESSAGEANSWER2,
       PROMESSAGE3,
       PROMESSAGEANSWER3,
       PROMESSAGE,
       PROMESSAGE1,
       PROMESSAGEANSWER1,
       PROMESSAGE2,
       PARTYID,
       ISMASTERTICKET,
       PAYCOUNTER,
       PROCESSEDON,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       CUSTID,
       TICKETNUM,
       TICKETID,
       Date,
       STAFFID,
       VOIDERID,
       VOIDREASONID,
       STATUS,
       SALES_SUBTOTAL,
       SALES_TOTAL,
       SALES_PRODUCTTOTAL,
       SALES_SERVICETOTAL,
       CASHCHANGE,
       DISCOUNTTOTAL,
       DISCOUNT_SERVICE,
       DISCOUNT_PRODUCT,
       TAXTOTAL,
       NODEID,
       SHIFTID,
       DAYID,
       PERIODID,
       SHIFTCREATEDID,
       APID,
       CHECKINTIME,
       LATE,
       TIP,
       USEDIMAGESCRIPT,
       CUSTBALANCE,
       PAYTYPEID,
       TIME,
       NEXTSTAFFID,
       CHECKINSTATUS,
       DISCOUNTDBL,
       PERFORMEDVALUEADDEDSERV,
       PRODUCTONLY,
       SERVICEONLY,
       HASPRODUCT,
       HASSERVICE,
       HASONLYPRODUCT,
       HASONLYSERVICE,
       HASBOTH,
       SALES_GIFTTOTAL,
       SALES_PACKAGETOTAL,
       SALES_SERIESTOTAL,
       STORE_NUMBER,
       NOTE,
       PROMESSAGEANSWER2,
       PROMESSAGE3,
       PROMESSAGEANSWER3,
       PROMESSAGE,
       PROMESSAGE1,
       PROMESSAGEANSWER1,
       PROMESSAGE2,
       PARTYID,
       ISMASTERTICKET,
       PAYCOUNTER,
       PROCESSEDON,
       isnull(cast(stage_spabiz_TICKET.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_TICKET
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ticket @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ticket (
       bk_hash,
       ticket_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_TICKET.bk_hash,
       stage_hash_spabiz_TICKET.ID ticket_id,
       stage_hash_spabiz_TICKET.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_TICKET.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_TICKET
  left join h_spabiz_ticket
    on stage_hash_spabiz_TICKET.bk_hash = h_spabiz_ticket.bk_hash
 where h_spabiz_ticket_id is null
   and stage_hash_spabiz_TICKET.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ticket
if object_id('tempdb..#l_spabiz_ticket_inserts') is not null drop table #l_spabiz_ticket_inserts
create table #l_spabiz_ticket_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKET.bk_hash,
       stage_hash_spabiz_TICKET.ID ticket_id,
       stage_hash_spabiz_TICKET.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKET.COUNTERID counter_id,
       stage_hash_spabiz_TICKET.STOREID store_id,
       stage_hash_spabiz_TICKET.CUSTID cust_id,
       stage_hash_spabiz_TICKET.STAFFID staff_id,
       stage_hash_spabiz_TICKET.VOIDERID voider_id,
       stage_hash_spabiz_TICKET.VOIDREASONID void_reason_id,
       stage_hash_spabiz_TICKET.NODEID node_id,
       stage_hash_spabiz_TICKET.SHIFTID shift_id,
       stage_hash_spabiz_TICKET.DAYID day_id,
       stage_hash_spabiz_TICKET.PERIODID period_id,
       stage_hash_spabiz_TICKET.SHIFTCREATEDID shift_created_id,
       stage_hash_spabiz_TICKET.APID ap_id,
       stage_hash_spabiz_TICKET.PAYTYPEID pay_type_id,
       stage_hash_spabiz_TICKET.NEXTSTAFFID next_staff_id,
       stage_hash_spabiz_TICKET.PARTYID party_id,
       isnull(cast(stage_hash_spabiz_TICKET.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.VOIDERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.VOIDREASONID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.NODEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SHIFTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.DAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PERIODID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SHIFTCREATEDID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.APID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PAYTYPEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.NEXTSTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PARTYID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKET
 where stage_hash_spabiz_TICKET.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ticket records
set @insert_date_time = getdate()
insert into l_spabiz_ticket (
       bk_hash,
       ticket_id,
       store_number,
       counter_id,
       store_id,
       cust_id,
       staff_id,
       voider_id,
       void_reason_id,
       node_id,
       shift_id,
       day_id,
       period_id,
       shift_created_id,
       ap_id,
       pay_type_id,
       next_staff_id,
       party_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ticket_inserts.bk_hash,
       #l_spabiz_ticket_inserts.ticket_id,
       #l_spabiz_ticket_inserts.store_number,
       #l_spabiz_ticket_inserts.counter_id,
       #l_spabiz_ticket_inserts.store_id,
       #l_spabiz_ticket_inserts.cust_id,
       #l_spabiz_ticket_inserts.staff_id,
       #l_spabiz_ticket_inserts.voider_id,
       #l_spabiz_ticket_inserts.void_reason_id,
       #l_spabiz_ticket_inserts.node_id,
       #l_spabiz_ticket_inserts.shift_id,
       #l_spabiz_ticket_inserts.day_id,
       #l_spabiz_ticket_inserts.period_id,
       #l_spabiz_ticket_inserts.shift_created_id,
       #l_spabiz_ticket_inserts.ap_id,
       #l_spabiz_ticket_inserts.pay_type_id,
       #l_spabiz_ticket_inserts.next_staff_id,
       #l_spabiz_ticket_inserts.party_id,
       case when l_spabiz_ticket.l_spabiz_ticket_id is null then isnull(#l_spabiz_ticket_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ticket_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ticket_inserts
  left join p_spabiz_ticket
    on #l_spabiz_ticket_inserts.bk_hash = p_spabiz_ticket.bk_hash
   and p_spabiz_ticket.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ticket
    on p_spabiz_ticket.bk_hash = l_spabiz_ticket.bk_hash
   and p_spabiz_ticket.l_spabiz_ticket_id = l_spabiz_ticket.l_spabiz_ticket_id
 where l_spabiz_ticket.l_spabiz_ticket_id is null
    or (l_spabiz_ticket.l_spabiz_ticket_id is not null
        and l_spabiz_ticket.dv_hash <> #l_spabiz_ticket_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ticket
if object_id('tempdb..#s_spabiz_ticket_inserts') is not null drop table #s_spabiz_ticket_inserts
create table #s_spabiz_ticket_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKET.bk_hash,
       stage_hash_spabiz_TICKET.ID ticket_id,
       stage_hash_spabiz_TICKET.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKET.EDITTIME edit_time,
       stage_hash_spabiz_TICKET.TICKETNUM ticket_num,
       stage_hash_spabiz_TICKET.TICKETID ticket_id_for_day,
       stage_hash_spabiz_TICKET.Date date,
       stage_hash_spabiz_TICKET.STATUS status,
       stage_hash_spabiz_TICKET.SALES_SUBTOTAL sales_subtotal,
       stage_hash_spabiz_TICKET.SALES_TOTAL sales_total,
       stage_hash_spabiz_TICKET.SALES_PRODUCTTOTAL sales_product_total,
       stage_hash_spabiz_TICKET.SALES_SERVICETOTAL sales_service_total,
       stage_hash_spabiz_TICKET.CASHCHANGE cash_change,
       stage_hash_spabiz_TICKET.DISCOUNTTOTAL discount_total,
       stage_hash_spabiz_TICKET.DISCOUNT_SERVICE discount_service,
       stage_hash_spabiz_TICKET.DISCOUNT_PRODUCT discount_product,
       stage_hash_spabiz_TICKET.TAXTOTAL tax_total,
       stage_hash_spabiz_TICKET.CHECKINTIME check_in_time,
       stage_hash_spabiz_TICKET.LATE late,
       stage_hash_spabiz_TICKET.TIP tip,
       stage_hash_spabiz_TICKET.USEDIMAGESCRIPT used_image_script,
       stage_hash_spabiz_TICKET.CUSTBALANCE cust_balance,
       stage_hash_spabiz_TICKET.TIME time,
       stage_hash_spabiz_TICKET.CHECKINSTATUS check_in_status,
       stage_hash_spabiz_TICKET.DISCOUNTDBL discount_dbl,
       stage_hash_spabiz_TICKET.PERFORMEDVALUEADDEDSERV performed_value_added_serv,
       stage_hash_spabiz_TICKET.PRODUCTONLY product_only,
       stage_hash_spabiz_TICKET.SERVICEONLY service_only,
       stage_hash_spabiz_TICKET.HASPRODUCT has_product,
       stage_hash_spabiz_TICKET.HASSERVICE has_service,
       stage_hash_spabiz_TICKET.HASONLYPRODUCT has_only_product,
       stage_hash_spabiz_TICKET.HASONLYSERVICE has_only_service,
       stage_hash_spabiz_TICKET.HASBOTH has_both,
       stage_hash_spabiz_TICKET.SALES_GIFTTOTAL sales_gift_total,
       stage_hash_spabiz_TICKET.SALES_PACKAGETOTAL sales_package_total,
       stage_hash_spabiz_TICKET.SALES_SERIESTOTAL sales_series_total,
       stage_hash_spabiz_TICKET.NOTE note,
       stage_hash_spabiz_TICKET.PROMESSAGEANSWER2 pro_message_answer_2,
       stage_hash_spabiz_TICKET.PROMESSAGE3 pro_message_3,
       stage_hash_spabiz_TICKET.PROMESSAGEANSWER3 pro_message_answer_3,
       stage_hash_spabiz_TICKET.PROMESSAGE pro_message,
       stage_hash_spabiz_TICKET.PROMESSAGE1 pro_message_1,
       stage_hash_spabiz_TICKET.PROMESSAGEANSWER1 pro_message_answer_1,
       stage_hash_spabiz_TICKET.PROMESSAGE2 pro_message_2,
       stage_hash_spabiz_TICKET.ISMASTERTICKET is_master_ticket,
       stage_hash_spabiz_TICKET.PAYCOUNTER pay_counter,
       stage_hash_spabiz_TICKET.PROCESSEDON processed_on,
       isnull(cast(stage_hash_spabiz_TICKET.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKET.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKET.TICKETNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKET.TICKETID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKET.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SALES_SUBTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SALES_TOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SALES_PRODUCTTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SALES_SERVICETOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.CASHCHANGE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.DISCOUNTTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.DISCOUNT_SERVICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.DISCOUNT_PRODUCT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.TAXTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKET.CHECKINTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.LATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.TIP as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.USEDIMAGESCRIPT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.CUSTBALANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKET.TIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.CHECKINSTATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.DISCOUNTDBL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PERFORMEDVALUEADDEDSERV as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PRODUCTONLY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SERVICEONLY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.HASPRODUCT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.HASSERVICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.HASONLYPRODUCT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.HASONLYSERVICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.HASBOTH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SALES_GIFTTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SALES_PACKAGETOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.SALES_SERIESTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKET.NOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PROMESSAGEANSWER2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKET.PROMESSAGE3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PROMESSAGEANSWER3 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKET.PROMESSAGE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKET.PROMESSAGE1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PROMESSAGEANSWER1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKET.PROMESSAGE2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.ISMASTERTICKET as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PAYCOUNTER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKET.PROCESSEDON as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKET
 where stage_hash_spabiz_TICKET.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ticket records
set @insert_date_time = getdate()
insert into s_spabiz_ticket (
       bk_hash,
       ticket_id,
       store_number,
       edit_time,
       ticket_num,
       ticket_id_for_day,
       date,
       status,
       sales_subtotal,
       sales_total,
       sales_product_total,
       sales_service_total,
       cash_change,
       discount_total,
       discount_service,
       discount_product,
       tax_total,
       check_in_time,
       late,
       tip,
       used_image_script,
       cust_balance,
       time,
       check_in_status,
       discount_dbl,
       performed_value_added_serv,
       product_only,
       service_only,
       has_product,
       has_service,
       has_only_product,
       has_only_service,
       has_both,
       sales_gift_total,
       sales_package_total,
       sales_series_total,
       note,
       pro_message_answer_2,
       pro_message_3,
       pro_message_answer_3,
       pro_message,
       pro_message_1,
       pro_message_answer_1,
       pro_message_2,
       is_master_ticket,
       pay_counter,
       processed_on,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ticket_inserts.bk_hash,
       #s_spabiz_ticket_inserts.ticket_id,
       #s_spabiz_ticket_inserts.store_number,
       #s_spabiz_ticket_inserts.edit_time,
       #s_spabiz_ticket_inserts.ticket_num,
       #s_spabiz_ticket_inserts.ticket_id_for_day,
       #s_spabiz_ticket_inserts.date,
       #s_spabiz_ticket_inserts.status,
       #s_spabiz_ticket_inserts.sales_subtotal,
       #s_spabiz_ticket_inserts.sales_total,
       #s_spabiz_ticket_inserts.sales_product_total,
       #s_spabiz_ticket_inserts.sales_service_total,
       #s_spabiz_ticket_inserts.cash_change,
       #s_spabiz_ticket_inserts.discount_total,
       #s_spabiz_ticket_inserts.discount_service,
       #s_spabiz_ticket_inserts.discount_product,
       #s_spabiz_ticket_inserts.tax_total,
       #s_spabiz_ticket_inserts.check_in_time,
       #s_spabiz_ticket_inserts.late,
       #s_spabiz_ticket_inserts.tip,
       #s_spabiz_ticket_inserts.used_image_script,
       #s_spabiz_ticket_inserts.cust_balance,
       #s_spabiz_ticket_inserts.time,
       #s_spabiz_ticket_inserts.check_in_status,
       #s_spabiz_ticket_inserts.discount_dbl,
       #s_spabiz_ticket_inserts.performed_value_added_serv,
       #s_spabiz_ticket_inserts.product_only,
       #s_spabiz_ticket_inserts.service_only,
       #s_spabiz_ticket_inserts.has_product,
       #s_spabiz_ticket_inserts.has_service,
       #s_spabiz_ticket_inserts.has_only_product,
       #s_spabiz_ticket_inserts.has_only_service,
       #s_spabiz_ticket_inserts.has_both,
       #s_spabiz_ticket_inserts.sales_gift_total,
       #s_spabiz_ticket_inserts.sales_package_total,
       #s_spabiz_ticket_inserts.sales_series_total,
       #s_spabiz_ticket_inserts.note,
       #s_spabiz_ticket_inserts.pro_message_answer_2,
       #s_spabiz_ticket_inserts.pro_message_3,
       #s_spabiz_ticket_inserts.pro_message_answer_3,
       #s_spabiz_ticket_inserts.pro_message,
       #s_spabiz_ticket_inserts.pro_message_1,
       #s_spabiz_ticket_inserts.pro_message_answer_1,
       #s_spabiz_ticket_inserts.pro_message_2,
       #s_spabiz_ticket_inserts.is_master_ticket,
       #s_spabiz_ticket_inserts.pay_counter,
       #s_spabiz_ticket_inserts.processed_on,
       case when s_spabiz_ticket.s_spabiz_ticket_id is null then isnull(#s_spabiz_ticket_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ticket_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ticket_inserts
  left join p_spabiz_ticket
    on #s_spabiz_ticket_inserts.bk_hash = p_spabiz_ticket.bk_hash
   and p_spabiz_ticket.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ticket
    on p_spabiz_ticket.bk_hash = s_spabiz_ticket.bk_hash
   and p_spabiz_ticket.s_spabiz_ticket_id = s_spabiz_ticket.s_spabiz_ticket_id
 where s_spabiz_ticket.s_spabiz_ticket_id is null
    or (s_spabiz_ticket.s_spabiz_ticket_id is not null
        and s_spabiz_ticket.dv_hash <> #s_spabiz_ticket_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ticket @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ticket @current_dv_batch_id

end
