CREATE PROC [dbo].[proc_etl_spabiz_ticket_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_TICKETDATA

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_TICKETDATA (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TICKETID,
       LINENUM,
       ITEMID,
       DATATYPE,
       GROUPID,
       CUSTID,
       STAFFID1,
       STAFFID2,
       QTY,
       RETAILPRICE,
       COST,
       DISCOUNTAMOUNT,
       DISCOUNTPER,
       DISCOUNTID,
       Date,
       STATUS,
       RETURNWHERE,
       EXTPRICE,
       SHIFTID,
       DAYID,
       PERIODID,
       TICKETDISAMT,
       TAXABLE,
       RETENTION,
       SUBGROUPID,
       PACKAGEID,
       SERVICEAMT,
       SERVICEQTY,
       PRODUCTAMT,
       PRODUCTQTY,
       OTHERAMT,
       OTHERQTY,
       TIMEID,
       PROMOID,
       STORE_NUMBER,
       GLACCOUNT,
       SALESGLACCOUNT,
       DISCOUNTGLACCOUNT,
       COSTGLACCOUNT,
       STARTTIME,
       ENDTIME,
       MASTERTICKET,
       MERGEDITEM,
       SHIP_TO,
       SHIP_STATUS,
       RETURNREASON,
       SERVICECHARGEPARENTID,
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
       LINENUM,
       ITEMID,
       DATATYPE,
       GROUPID,
       CUSTID,
       STAFFID1,
       STAFFID2,
       QTY,
       RETAILPRICE,
       COST,
       DISCOUNTAMOUNT,
       DISCOUNTPER,
       DISCOUNTID,
       Date,
       STATUS,
       RETURNWHERE,
       EXTPRICE,
       SHIFTID,
       DAYID,
       PERIODID,
       TICKETDISAMT,
       TAXABLE,
       RETENTION,
       SUBGROUPID,
       PACKAGEID,
       SERVICEAMT,
       SERVICEQTY,
       PRODUCTAMT,
       PRODUCTQTY,
       OTHERAMT,
       OTHERQTY,
       TIMEID,
       PROMOID,
       STORE_NUMBER,
       GLACCOUNT,
       SALESGLACCOUNT,
       DISCOUNTGLACCOUNT,
       COSTGLACCOUNT,
       STARTTIME,
       ENDTIME,
       MASTERTICKET,
       MERGEDITEM,
       SHIP_TO,
       SHIP_STATUS,
       RETURNREASON,
       SERVICECHARGEPARENTID,
       isnull(cast(stage_spabiz_TICKETDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_TICKETDATA
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ticket_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ticket_data (
       bk_hash,
       ticket_data_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_TICKETDATA.bk_hash,
       stage_hash_spabiz_TICKETDATA.ID ticket_data_id,
       stage_hash_spabiz_TICKETDATA.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_TICKETDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_TICKETDATA
  left join h_spabiz_ticket_data
    on stage_hash_spabiz_TICKETDATA.bk_hash = h_spabiz_ticket_data.bk_hash
 where h_spabiz_ticket_data_id is null
   and stage_hash_spabiz_TICKETDATA.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ticket_data
if object_id('tempdb..#l_spabiz_ticket_data_inserts') is not null drop table #l_spabiz_ticket_data_inserts
create table #l_spabiz_ticket_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETDATA.bk_hash,
       stage_hash_spabiz_TICKETDATA.ID ticket_data_id,
       stage_hash_spabiz_TICKETDATA.COUNTERID counter_id,
       stage_hash_spabiz_TICKETDATA.STOREID store_id,
       stage_hash_spabiz_TICKETDATA.TICKETID ticket_id,
       stage_hash_spabiz_TICKETDATA.ITEMID item_id,
       stage_hash_spabiz_TICKETDATA.DATATYPE data_type,
       stage_hash_spabiz_TICKETDATA.GROUPID group_id,
       stage_hash_spabiz_TICKETDATA.CUSTID cust_id,
       stage_hash_spabiz_TICKETDATA.STAFFID1 staff_id_1,
       stage_hash_spabiz_TICKETDATA.STAFFID2 staff_id_2,
       stage_hash_spabiz_TICKETDATA.DISCOUNTID discount_id,
       stage_hash_spabiz_TICKETDATA.SHIFTID shift_id,
       stage_hash_spabiz_TICKETDATA.DAYID day_id,
       stage_hash_spabiz_TICKETDATA.PERIODID period_id,
       stage_hash_spabiz_TICKETDATA.RETENTION retention,
       stage_hash_spabiz_TICKETDATA.SUBGROUPID sub_group_id,
       stage_hash_spabiz_TICKETDATA.PACKAGEID package_id,
       stage_hash_spabiz_TICKETDATA.TIMEID time_id,
       stage_hash_spabiz_TICKETDATA.PROMOID promo_id,
       stage_hash_spabiz_TICKETDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETDATA.GLACCOUNT gl_account,
       stage_hash_spabiz_TICKETDATA.SALESGLACCOUNT sales_gl_account,
       stage_hash_spabiz_TICKETDATA.DISCOUNTGLACCOUNT discount_gl_account,
       stage_hash_spabiz_TICKETDATA.COSTGLACCOUNT cost_gl_account,
       stage_hash_spabiz_TICKETDATA.MASTERTICKET master_ticket,
       stage_hash_spabiz_TICKETDATA.RETURNREASON return_reason,
       stage_hash_spabiz_TICKETDATA.SERVICECHARGEPARENTID service_charge_parent_id,
       isnull(cast(stage_hash_spabiz_TICKETDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.TICKETID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.ITEMID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.DATATYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.GROUPID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.STAFFID1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.STAFFID2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.DISCOUNTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.SHIFTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.DAYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.PERIODID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.RETENTION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.SUBGROUPID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.PACKAGEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.TIMEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.PROMOID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETDATA.GLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETDATA.SALESGLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETDATA.DISCOUNTGLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETDATA.COSTGLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.MASTERTICKET as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.RETURNREASON as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.SERVICECHARGEPARENTID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETDATA
 where stage_hash_spabiz_TICKETDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ticket_data records
set @insert_date_time = getdate()
insert into l_spabiz_ticket_data (
       bk_hash,
       ticket_data_id,
       counter_id,
       store_id,
       ticket_id,
       item_id,
       data_type,
       group_id,
       cust_id,
       staff_id_1,
       staff_id_2,
       discount_id,
       shift_id,
       day_id,
       period_id,
       retention,
       sub_group_id,
       package_id,
       time_id,
       promo_id,
       store_number,
       gl_account,
       sales_gl_account,
       discount_gl_account,
       cost_gl_account,
       master_ticket,
       return_reason,
       service_charge_parent_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ticket_data_inserts.bk_hash,
       #l_spabiz_ticket_data_inserts.ticket_data_id,
       #l_spabiz_ticket_data_inserts.counter_id,
       #l_spabiz_ticket_data_inserts.store_id,
       #l_spabiz_ticket_data_inserts.ticket_id,
       #l_spabiz_ticket_data_inserts.item_id,
       #l_spabiz_ticket_data_inserts.data_type,
       #l_spabiz_ticket_data_inserts.group_id,
       #l_spabiz_ticket_data_inserts.cust_id,
       #l_spabiz_ticket_data_inserts.staff_id_1,
       #l_spabiz_ticket_data_inserts.staff_id_2,
       #l_spabiz_ticket_data_inserts.discount_id,
       #l_spabiz_ticket_data_inserts.shift_id,
       #l_spabiz_ticket_data_inserts.day_id,
       #l_spabiz_ticket_data_inserts.period_id,
       #l_spabiz_ticket_data_inserts.retention,
       #l_spabiz_ticket_data_inserts.sub_group_id,
       #l_spabiz_ticket_data_inserts.package_id,
       #l_spabiz_ticket_data_inserts.time_id,
       #l_spabiz_ticket_data_inserts.promo_id,
       #l_spabiz_ticket_data_inserts.store_number,
       #l_spabiz_ticket_data_inserts.gl_account,
       #l_spabiz_ticket_data_inserts.sales_gl_account,
       #l_spabiz_ticket_data_inserts.discount_gl_account,
       #l_spabiz_ticket_data_inserts.cost_gl_account,
       #l_spabiz_ticket_data_inserts.master_ticket,
       #l_spabiz_ticket_data_inserts.return_reason,
       #l_spabiz_ticket_data_inserts.service_charge_parent_id,
       case when l_spabiz_ticket_data.l_spabiz_ticket_data_id is null then isnull(#l_spabiz_ticket_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ticket_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ticket_data_inserts
  left join p_spabiz_ticket_data
    on #l_spabiz_ticket_data_inserts.bk_hash = p_spabiz_ticket_data.bk_hash
   and p_spabiz_ticket_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ticket_data
    on p_spabiz_ticket_data.bk_hash = l_spabiz_ticket_data.bk_hash
   and p_spabiz_ticket_data.l_spabiz_ticket_data_id = l_spabiz_ticket_data.l_spabiz_ticket_data_id
 where l_spabiz_ticket_data.l_spabiz_ticket_data_id is null
    or (l_spabiz_ticket_data.l_spabiz_ticket_data_id is not null
        and l_spabiz_ticket_data.dv_hash <> #l_spabiz_ticket_data_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ticket_data
if object_id('tempdb..#s_spabiz_ticket_data_inserts') is not null drop table #s_spabiz_ticket_data_inserts
create table #s_spabiz_ticket_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_TICKETDATA.bk_hash,
       stage_hash_spabiz_TICKETDATA.ID ticket_data_id,
       stage_hash_spabiz_TICKETDATA.EDITTIME edit_time,
       stage_hash_spabiz_TICKETDATA.LINENUM line_num,
       stage_hash_spabiz_TICKETDATA.QTY qty,
       stage_hash_spabiz_TICKETDATA.RETAILPRICE retail_price,
       stage_hash_spabiz_TICKETDATA.COST cost,
       stage_hash_spabiz_TICKETDATA.DISCOUNTAMOUNT discount_amount,
       stage_hash_spabiz_TICKETDATA.DISCOUNTPER discount_per,
       stage_hash_spabiz_TICKETDATA.Date date,
       stage_hash_spabiz_TICKETDATA.STATUS status,
       stage_hash_spabiz_TICKETDATA.RETURNWHERE return_where,
       stage_hash_spabiz_TICKETDATA.EXTPRICE ext_price,
       stage_hash_spabiz_TICKETDATA.TICKETDISAMT ticket_dis_amt,
       stage_hash_spabiz_TICKETDATA.TAXABLE taxable,
       stage_hash_spabiz_TICKETDATA.SERVICEAMT service_amt,
       stage_hash_spabiz_TICKETDATA.SERVICEQTY service_qty,
       stage_hash_spabiz_TICKETDATA.PRODUCTAMT product_amt,
       stage_hash_spabiz_TICKETDATA.PRODUCTQTY product_qty,
       stage_hash_spabiz_TICKETDATA.OTHERAMT other_amt,
       stage_hash_spabiz_TICKETDATA.OTHERQTY other_qty,
       stage_hash_spabiz_TICKETDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_TICKETDATA.STARTTIME start_time,
       stage_hash_spabiz_TICKETDATA.ENDTIME end_time,
       stage_hash_spabiz_TICKETDATA.MERGEDITEM merged_item,
       stage_hash_spabiz_TICKETDATA.SHIP_TO ship_to,
       stage_hash_spabiz_TICKETDATA.SHIP_STATUS ship_status,
       isnull(cast(stage_hash_spabiz_TICKETDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETDATA.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.LINENUM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.QTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.COST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.DISCOUNTAMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.DISCOUNTPER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETDATA.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.RETURNWHERE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.EXTPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.TICKETDISAMT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.TAXABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.SERVICEAMT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.SERVICEQTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.PRODUCTAMT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.PRODUCTQTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.OTHERAMT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.OTHERQTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETDATA.STARTTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_TICKETDATA.ENDTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.MERGEDITEM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_TICKETDATA.SHIP_TO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_TICKETDATA.SHIP_STATUS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_TICKETDATA
 where stage_hash_spabiz_TICKETDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ticket_data records
set @insert_date_time = getdate()
insert into s_spabiz_ticket_data (
       bk_hash,
       ticket_data_id,
       edit_time,
       line_num,
       qty,
       retail_price,
       cost,
       discount_amount,
       discount_per,
       date,
       status,
       return_where,
       ext_price,
       ticket_dis_amt,
       taxable,
       service_amt,
       service_qty,
       product_amt,
       product_qty,
       other_amt,
       other_qty,
       store_number,
       start_time,
       end_time,
       merged_item,
       ship_to,
       ship_status,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ticket_data_inserts.bk_hash,
       #s_spabiz_ticket_data_inserts.ticket_data_id,
       #s_spabiz_ticket_data_inserts.edit_time,
       #s_spabiz_ticket_data_inserts.line_num,
       #s_spabiz_ticket_data_inserts.qty,
       #s_spabiz_ticket_data_inserts.retail_price,
       #s_spabiz_ticket_data_inserts.cost,
       #s_spabiz_ticket_data_inserts.discount_amount,
       #s_spabiz_ticket_data_inserts.discount_per,
       #s_spabiz_ticket_data_inserts.date,
       #s_spabiz_ticket_data_inserts.status,
       #s_spabiz_ticket_data_inserts.return_where,
       #s_spabiz_ticket_data_inserts.ext_price,
       #s_spabiz_ticket_data_inserts.ticket_dis_amt,
       #s_spabiz_ticket_data_inserts.taxable,
       #s_spabiz_ticket_data_inserts.service_amt,
       #s_spabiz_ticket_data_inserts.service_qty,
       #s_spabiz_ticket_data_inserts.product_amt,
       #s_spabiz_ticket_data_inserts.product_qty,
       #s_spabiz_ticket_data_inserts.other_amt,
       #s_spabiz_ticket_data_inserts.other_qty,
       #s_spabiz_ticket_data_inserts.store_number,
       #s_spabiz_ticket_data_inserts.start_time,
       #s_spabiz_ticket_data_inserts.end_time,
       #s_spabiz_ticket_data_inserts.merged_item,
       #s_spabiz_ticket_data_inserts.ship_to,
       #s_spabiz_ticket_data_inserts.ship_status,
       case when s_spabiz_ticket_data.s_spabiz_ticket_data_id is null then isnull(#s_spabiz_ticket_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ticket_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ticket_data_inserts
  left join p_spabiz_ticket_data
    on #s_spabiz_ticket_data_inserts.bk_hash = p_spabiz_ticket_data.bk_hash
   and p_spabiz_ticket_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ticket_data
    on p_spabiz_ticket_data.bk_hash = s_spabiz_ticket_data.bk_hash
   and p_spabiz_ticket_data.s_spabiz_ticket_data_id = s_spabiz_ticket_data.s_spabiz_ticket_data_id
 where s_spabiz_ticket_data.s_spabiz_ticket_data_id is null
    or (s_spabiz_ticket_data.s_spabiz_ticket_data_id is not null
        and s_spabiz_ticket_data.dv_hash <> #s_spabiz_ticket_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ticket_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ticket_data @current_dv_batch_id

end
