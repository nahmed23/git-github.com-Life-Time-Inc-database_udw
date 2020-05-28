CREATE PROC [dbo].[proc_etl_spabiz_cust_card_type] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_CUSTCARDTYPE

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_CUSTCARDTYPE (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       RETAILPRICE,
       DAYSGOODFOR,
       SERIALNUMCOUNTER,
       STORE_NUMBER,
       PAYMENTINTERVAL,
       SERVICEDISC,
       PRODDISC,
       DISCOUNTID,
       DISPCOLOR,
       INITIALPRICE,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       RETAILPRICE,
       DAYSGOODFOR,
       SERIALNUMCOUNTER,
       STORE_NUMBER,
       PAYMENTINTERVAL,
       SERVICEDISC,
       PRODDISC,
       DISCOUNTID,
       DISPCOLOR,
       INITIALPRICE,
       isnull(cast(stage_spabiz_CUSTCARDTYPE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_spabiz_CUSTCARDTYPE
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_cust_card_type @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_cust_card_type (
       bk_hash,
       cust_card_type_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_CUSTCARDTYPE.bk_hash,
       stage_hash_spabiz_CUSTCARDTYPE.ID cust_card_type_id,
       stage_hash_spabiz_CUSTCARDTYPE.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_CUSTCARDTYPE
  left join h_spabiz_cust_card_type
    on stage_hash_spabiz_CUSTCARDTYPE.bk_hash = h_spabiz_cust_card_type.bk_hash
 where h_spabiz_cust_card_type_id is null
   and stage_hash_spabiz_CUSTCARDTYPE.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_cust_card_type
if object_id('tempdb..#l_spabiz_cust_card_type_inserts') is not null drop table #l_spabiz_cust_card_type_inserts
create table #l_spabiz_cust_card_type_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CUSTCARDTYPE.bk_hash,
       stage_hash_spabiz_CUSTCARDTYPE.ID cust_card_type_id,
       stage_hash_spabiz_CUSTCARDTYPE.STOREID store_id,
       stage_hash_spabiz_CUSTCARDTYPE.STORE_NUMBER store_number,
       stage_hash_spabiz_CUSTCARDTYPE.DISCOUNTID discount_id,
       isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.ID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.STOREID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.STORE_NUMBER as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.DISCOUNTID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CUSTCARDTYPE
 where stage_hash_spabiz_CUSTCARDTYPE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_cust_card_type records
set @insert_date_time = getdate()
insert into l_spabiz_cust_card_type (
       bk_hash,
       cust_card_type_id,
       store_id,
       store_number,
       discount_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_cust_card_type_inserts.bk_hash,
       #l_spabiz_cust_card_type_inserts.cust_card_type_id,
       #l_spabiz_cust_card_type_inserts.store_id,
       #l_spabiz_cust_card_type_inserts.store_number,
       #l_spabiz_cust_card_type_inserts.discount_id,
       case when l_spabiz_cust_card_type.l_spabiz_cust_card_type_id is null then isnull(#l_spabiz_cust_card_type_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_cust_card_type_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_cust_card_type_inserts
  left join p_spabiz_cust_card_type
    on #l_spabiz_cust_card_type_inserts.bk_hash = p_spabiz_cust_card_type.bk_hash
   and p_spabiz_cust_card_type.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_cust_card_type
    on p_spabiz_cust_card_type.bk_hash = l_spabiz_cust_card_type.bk_hash
   and p_spabiz_cust_card_type.l_spabiz_cust_card_type_id = l_spabiz_cust_card_type.l_spabiz_cust_card_type_id
 where l_spabiz_cust_card_type.l_spabiz_cust_card_type_id is null
    or (l_spabiz_cust_card_type.l_spabiz_cust_card_type_id is not null
        and l_spabiz_cust_card_type.dv_hash <> #l_spabiz_cust_card_type_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_cust_card_type
if object_id('tempdb..#s_spabiz_cust_card_type_inserts') is not null drop table #s_spabiz_cust_card_type_inserts
create table #s_spabiz_cust_card_type_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CUSTCARDTYPE.bk_hash,
       stage_hash_spabiz_CUSTCARDTYPE.ID cust_card_type_id,
       stage_hash_spabiz_CUSTCARDTYPE.COUNTERID counter_id,
       stage_hash_spabiz_CUSTCARDTYPE.EDITTIME edit_time,
       stage_hash_spabiz_CUSTCARDTYPE.[Delete] cust_card_type_delete,
       stage_hash_spabiz_CUSTCARDTYPE.DELETEDATE delete_date,
       stage_hash_spabiz_CUSTCARDTYPE.NAME name,
       stage_hash_spabiz_CUSTCARDTYPE.RETAILPRICE retail_price,
       stage_hash_spabiz_CUSTCARDTYPE.DAYSGOODFOR days_good_for,
       stage_hash_spabiz_CUSTCARDTYPE.SERIALNUMCOUNTER serial_num_counter,
       stage_hash_spabiz_CUSTCARDTYPE.STORE_NUMBER store_number,
       stage_hash_spabiz_CUSTCARDTYPE.PAYMENTINTERVAL payment_interval,
       stage_hash_spabiz_CUSTCARDTYPE.SERVICEDISC service_disc,
       stage_hash_spabiz_CUSTCARDTYPE.PRODDISC prod_disc,
       stage_hash_spabiz_CUSTCARDTYPE.DISPCOLOR disp_color,
       stage_hash_spabiz_CUSTCARDTYPE.INITIALPRICE initial_price,
       isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.ID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.COUNTERID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTCARDTYPE.EDITTIME,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.[Delete] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTCARDTYPE.DELETEDATE,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTCARDTYPE.NAME,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.RETAILPRICE as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.DAYSGOODFOR as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.SERIALNUMCOUNTER as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.STORE_NUMBER as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.PAYMENTINTERVAL as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTCARDTYPE.SERVICEDISC,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTCARDTYPE.PRODDISC,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTCARDTYPE.DISPCOLOR,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTCARDTYPE.INITIALPRICE as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CUSTCARDTYPE
 where stage_hash_spabiz_CUSTCARDTYPE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_cust_card_type records
set @insert_date_time = getdate()
insert into s_spabiz_cust_card_type (
       bk_hash,
       cust_card_type_id,
       counter_id,
       edit_time,
       cust_card_type_delete,
       delete_date,
       name,
       retail_price,
       days_good_for,
       serial_num_counter,
       store_number,
       payment_interval,
       service_disc,
       prod_disc,
       disp_color,
       initial_price,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_cust_card_type_inserts.bk_hash,
       #s_spabiz_cust_card_type_inserts.cust_card_type_id,
       #s_spabiz_cust_card_type_inserts.counter_id,
       #s_spabiz_cust_card_type_inserts.edit_time,
       #s_spabiz_cust_card_type_inserts.cust_card_type_delete,
       #s_spabiz_cust_card_type_inserts.delete_date,
       #s_spabiz_cust_card_type_inserts.name,
       #s_spabiz_cust_card_type_inserts.retail_price,
       #s_spabiz_cust_card_type_inserts.days_good_for,
       #s_spabiz_cust_card_type_inserts.serial_num_counter,
       #s_spabiz_cust_card_type_inserts.store_number,
       #s_spabiz_cust_card_type_inserts.payment_interval,
       #s_spabiz_cust_card_type_inserts.service_disc,
       #s_spabiz_cust_card_type_inserts.prod_disc,
       #s_spabiz_cust_card_type_inserts.disp_color,
       #s_spabiz_cust_card_type_inserts.initial_price,
       case when s_spabiz_cust_card_type.s_spabiz_cust_card_type_id is null then isnull(#s_spabiz_cust_card_type_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_cust_card_type_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_cust_card_type_inserts
  left join p_spabiz_cust_card_type
    on #s_spabiz_cust_card_type_inserts.bk_hash = p_spabiz_cust_card_type.bk_hash
   and p_spabiz_cust_card_type.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_cust_card_type
    on p_spabiz_cust_card_type.bk_hash = s_spabiz_cust_card_type.bk_hash
   and p_spabiz_cust_card_type.s_spabiz_cust_card_type_id = s_spabiz_cust_card_type.s_spabiz_cust_card_type_id
 where s_spabiz_cust_card_type.s_spabiz_cust_card_type_id is null
    or (s_spabiz_cust_card_type.s_spabiz_cust_card_type_id is not null
        and s_spabiz_cust_card_type.dv_hash <> #s_spabiz_cust_card_type_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_cust_card_type @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_cust_card_type @current_dv_batch_id

end
