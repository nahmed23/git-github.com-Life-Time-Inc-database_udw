CREATE PROC [dbo].[proc_etl_hybris_orders] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_orders

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_orders (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_calculated,
       p_code,
       p_currency,
       p_deliveryaddress,
       p_deliverycost,
       p_deliverymode,
       p_deliverystatus,
       p_globaldiscountvaluesinternal,
       p_net,
       p_paymentaddress,
       p_paymentcost,
       p_paymentinfo,
       p_paymentmode,
       p_paymentstatus,
       p_status,
       p_exportstatus,
       p_statusinfo,
       p_totalprice,
       p_totaldiscounts,
       p_totaltax,
       p_totaltaxvaluesinternal,
       p_user,
       p_subtotal,
       p_discountsincludedeliverycost,
       p_discountsincludepaymentcost,
       p_europe1pricefactory_udg,
       p_europe1pricefactory_upg,
       p_europe1pricefactory_utg,
       p_previousdeliverymode,
       p_site,
       p_store,
       p_guid,
       p_billingtime,
       p_parent,
       p_versionid,
       p_originalversion,
       p_fraudulent,
       p_potentiallyfraudulent,
       p_salesapplication,
       p_language,
       p_placedby,
       aCLTS,
       propTS,
       p_fulfilmentstatus,
       p_notes,
       p_fraudpreventionsessionid,
       p_commissionemployeeid,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_calculated,
       p_code,
       p_currency,
       p_deliveryaddress,
       p_deliverycost,
       p_deliverymode,
       p_deliverystatus,
       p_globaldiscountvaluesinternal,
       p_net,
       p_paymentaddress,
       p_paymentcost,
       p_paymentinfo,
       p_paymentmode,
       p_paymentstatus,
       p_status,
       p_exportstatus,
       p_statusinfo,
       p_totalprice,
       p_totaldiscounts,
       p_totaltax,
       p_totaltaxvaluesinternal,
       p_user,
       p_subtotal,
       p_discountsincludedeliverycost,
       p_discountsincludepaymentcost,
       p_europe1pricefactory_udg,
       p_europe1pricefactory_upg,
       p_europe1pricefactory_utg,
       p_previousdeliverymode,
       p_site,
       p_store,
       p_guid,
       p_billingtime,
       p_parent,
       p_versionid,
       p_originalversion,
       p_fraudulent,
       p_potentiallyfraudulent,
       p_salesapplication,
       p_language,
       p_placedby,
       aCLTS,
       propTS,
       p_fulfilmentstatus,
       p_notes,
       p_fraudpreventionsessionid,
       p_commissionemployeeid,
       isnull(cast(stage_hybris_orders.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_orders
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_orders @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_orders (
       bk_hash,
       orders_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_orders.bk_hash,
       stage_hash_hybris_orders.[PK] orders_pk,
       isnull(cast(stage_hash_hybris_orders.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_orders
  left join h_hybris_orders
    on stage_hash_hybris_orders.bk_hash = h_hybris_orders.bk_hash
 where h_hybris_orders_id is null
   and stage_hash_hybris_orders.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_orders
if object_id('tempdb..#l_hybris_orders_inserts') is not null drop table #l_hybris_orders_inserts
create table #l_hybris_orders_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_orders.bk_hash,
       stage_hash_hybris_orders.TypePkString type_pk_string,
       stage_hash_hybris_orders.OwnerPkString owner_pk_string,
       stage_hash_hybris_orders.[PK] orders_pk,
       stage_hash_hybris_orders.p_currency p_currency,
       stage_hash_hybris_orders.p_deliveryaddress p_delivery_address,
       stage_hash_hybris_orders.p_deliverymode p_delivery_mode,
       stage_hash_hybris_orders.p_deliverystatus p_delivery_status,
       stage_hash_hybris_orders.p_paymentaddress p_payment_address,
       stage_hash_hybris_orders.p_paymentinfo p_payment_info,
       stage_hash_hybris_orders.p_paymentmode p_payment_mode,
       stage_hash_hybris_orders.p_paymentstatus p_payment_status,
       stage_hash_hybris_orders.p_status p_status,
       stage_hash_hybris_orders.p_exportstatus p_export_status,
       stage_hash_hybris_orders.p_user p_user,
       stage_hash_hybris_orders.p_europe1pricefactory_udg p_europe_1_price_factory_udg,
       stage_hash_hybris_orders.p_europe1pricefactory_upg p_europe_1_price_factory_upg,
       stage_hash_hybris_orders.p_europe1pricefactory_utg p_europe_1_price_factory_utg,
       stage_hash_hybris_orders.p_previousdeliverymode p_previous_delivery_mode,
       stage_hash_hybris_orders.p_site p_site,
       stage_hash_hybris_orders.p_store p_store,
       stage_hash_hybris_orders.p_billingtime p_billing_time,
       stage_hash_hybris_orders.p_parent p_parent,
       stage_hash_hybris_orders.p_originalversion p_original_version,
       stage_hash_hybris_orders.p_salesapplication p_sales_application,
       stage_hash_hybris_orders.p_language p_language,
       stage_hash_hybris_orders.p_placedby p_placed_by,
       stage_hash_hybris_orders.p_fulfilmentstatus p_fulfilment_status,
       stage_hash_hybris_orders.p_fraudpreventionsessionid p_fraud_prevention_session_id,
       stage_hash_hybris_orders.p_commissionemployeeid p_commission_employee_id,
       stage_hash_hybris_orders.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_currency as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_deliveryaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_deliverymode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_deliverystatus as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_paymentaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_paymentinfo as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_paymentmode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_paymentstatus as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_status as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_exportstatus as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_user as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_europe1pricefactory_udg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_europe1pricefactory_upg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_europe1pricefactory_utg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_previousdeliverymode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_site as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_store as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_billingtime as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_parent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_originalversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_salesapplication as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_language as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_placedby as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_fulfilmentstatus as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_fraudpreventionsessionid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_commissionemployeeid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_orders
 where stage_hash_hybris_orders.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_orders records
set @insert_date_time = getdate()
insert into l_hybris_orders (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       orders_pk,
       p_currency,
       p_delivery_address,
       p_delivery_mode,
       p_delivery_status,
       p_payment_address,
       p_payment_info,
       p_payment_mode,
       p_payment_status,
       p_status,
       p_export_status,
       p_user,
       p_europe_1_price_factory_udg,
       p_europe_1_price_factory_upg,
       p_europe_1_price_factory_utg,
       p_previous_delivery_mode,
       p_site,
       p_store,
       p_billing_time,
       p_parent,
       p_original_version,
       p_sales_application,
       p_language,
       p_placed_by,
       p_fulfilment_status,
       p_fraud_prevention_session_id,
       p_commission_employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_orders_inserts.bk_hash,
       #l_hybris_orders_inserts.type_pk_string,
       #l_hybris_orders_inserts.owner_pk_string,
       #l_hybris_orders_inserts.orders_pk,
       #l_hybris_orders_inserts.p_currency,
       #l_hybris_orders_inserts.p_delivery_address,
       #l_hybris_orders_inserts.p_delivery_mode,
       #l_hybris_orders_inserts.p_delivery_status,
       #l_hybris_orders_inserts.p_payment_address,
       #l_hybris_orders_inserts.p_payment_info,
       #l_hybris_orders_inserts.p_payment_mode,
       #l_hybris_orders_inserts.p_payment_status,
       #l_hybris_orders_inserts.p_status,
       #l_hybris_orders_inserts.p_export_status,
       #l_hybris_orders_inserts.p_user,
       #l_hybris_orders_inserts.p_europe_1_price_factory_udg,
       #l_hybris_orders_inserts.p_europe_1_price_factory_upg,
       #l_hybris_orders_inserts.p_europe_1_price_factory_utg,
       #l_hybris_orders_inserts.p_previous_delivery_mode,
       #l_hybris_orders_inserts.p_site,
       #l_hybris_orders_inserts.p_store,
       #l_hybris_orders_inserts.p_billing_time,
       #l_hybris_orders_inserts.p_parent,
       #l_hybris_orders_inserts.p_original_version,
       #l_hybris_orders_inserts.p_sales_application,
       #l_hybris_orders_inserts.p_language,
       #l_hybris_orders_inserts.p_placed_by,
       #l_hybris_orders_inserts.p_fulfilment_status,
       #l_hybris_orders_inserts.p_fraud_prevention_session_id,
       #l_hybris_orders_inserts.p_commission_employee_id,
       case when l_hybris_orders.l_hybris_orders_id is null then isnull(#l_hybris_orders_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_orders_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_orders_inserts
  left join p_hybris_orders
    on #l_hybris_orders_inserts.bk_hash = p_hybris_orders.bk_hash
   and p_hybris_orders.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_orders
    on p_hybris_orders.bk_hash = l_hybris_orders.bk_hash
   and p_hybris_orders.l_hybris_orders_id = l_hybris_orders.l_hybris_orders_id
 where l_hybris_orders.l_hybris_orders_id is null
    or (l_hybris_orders.l_hybris_orders_id is not null
        and l_hybris_orders.dv_hash <> #l_hybris_orders_inserts.source_hash)

--calculate hash and lookup to current s_hybris_orders
if object_id('tempdb..#s_hybris_orders_inserts') is not null drop table #s_hybris_orders_inserts
create table #s_hybris_orders_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_orders.bk_hash,
       stage_hash_hybris_orders.hjmpTS hjmpts,
       stage_hash_hybris_orders.createdTS created_ts,
       stage_hash_hybris_orders.modifiedTS modified_ts,
       stage_hash_hybris_orders.[PK] orders_pk,
       stage_hash_hybris_orders.p_calculated p_calculated,
       stage_hash_hybris_orders.p_code p_code,
       stage_hash_hybris_orders.p_deliverycost p_delivery_cost,
       stage_hash_hybris_orders.p_globaldiscountvaluesinternal p_global_discount_values_internal,
       stage_hash_hybris_orders.p_net p_net,
       stage_hash_hybris_orders.p_paymentcost p_payment_cost,
       stage_hash_hybris_orders.p_statusinfo p_status_info,
       stage_hash_hybris_orders.p_totalprice p_total_price,
       stage_hash_hybris_orders.p_totaldiscounts p_total_discounts,
       stage_hash_hybris_orders.p_totaltax p_total_tax,
       stage_hash_hybris_orders.p_totaltaxvaluesinternal p_total_tax_values_internal,
       stage_hash_hybris_orders.p_subtotal p_subtotal,
       stage_hash_hybris_orders.p_discountsincludedeliverycost p_discounts_include_delivery_cost,
       stage_hash_hybris_orders.p_discountsincludepaymentcost p_discounts_include_payment_cost,
       stage_hash_hybris_orders.p_guid p_guid,
       stage_hash_hybris_orders.p_versionid p_version_id,
       stage_hash_hybris_orders.p_fraudulent p_fraudulent,
       stage_hash_hybris_orders.p_potentiallyfraudulent p_potentially_fraudulent,
       stage_hash_hybris_orders.aCLTS acl_ts,
       stage_hash_hybris_orders.propTS prop_ts,
       stage_hash_hybris_orders.p_notes p_notes,
       stage_hash_hybris_orders.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_orders.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_orders.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_calculated as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_deliverycost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_globaldiscountvaluesinternal,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_net as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_paymentcost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_statusinfo,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_totalprice as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_totaldiscounts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_totaltax as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_totaltaxvaluesinternal,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_subtotal as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_discountsincludedeliverycost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_discountsincludepaymentcost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_guid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_versionid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_fraudulent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.p_potentiallyfraudulent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orders.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orders.p_notes,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_orders
 where stage_hash_hybris_orders.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_orders records
set @insert_date_time = getdate()
insert into s_hybris_orders (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       orders_pk,
       p_calculated,
       p_code,
       p_delivery_cost,
       p_global_discount_values_internal,
       p_net,
       p_payment_cost,
       p_status_info,
       p_total_price,
       p_total_discounts,
       p_total_tax,
       p_total_tax_values_internal,
       p_subtotal,
       p_discounts_include_delivery_cost,
       p_discounts_include_payment_cost,
       p_guid,
       p_version_id,
       p_fraudulent,
       p_potentially_fraudulent,
       acl_ts,
       prop_ts,
       p_notes,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_orders_inserts.bk_hash,
       #s_hybris_orders_inserts.hjmpts,
       #s_hybris_orders_inserts.created_ts,
       #s_hybris_orders_inserts.modified_ts,
       #s_hybris_orders_inserts.orders_pk,
       #s_hybris_orders_inserts.p_calculated,
       #s_hybris_orders_inserts.p_code,
       #s_hybris_orders_inserts.p_delivery_cost,
       #s_hybris_orders_inserts.p_global_discount_values_internal,
       #s_hybris_orders_inserts.p_net,
       #s_hybris_orders_inserts.p_payment_cost,
       #s_hybris_orders_inserts.p_status_info,
       #s_hybris_orders_inserts.p_total_price,
       #s_hybris_orders_inserts.p_total_discounts,
       #s_hybris_orders_inserts.p_total_tax,
       #s_hybris_orders_inserts.p_total_tax_values_internal,
       #s_hybris_orders_inserts.p_subtotal,
       #s_hybris_orders_inserts.p_discounts_include_delivery_cost,
       #s_hybris_orders_inserts.p_discounts_include_payment_cost,
       #s_hybris_orders_inserts.p_guid,
       #s_hybris_orders_inserts.p_version_id,
       #s_hybris_orders_inserts.p_fraudulent,
       #s_hybris_orders_inserts.p_potentially_fraudulent,
       #s_hybris_orders_inserts.acl_ts,
       #s_hybris_orders_inserts.prop_ts,
       #s_hybris_orders_inserts.p_notes,
       case when s_hybris_orders.s_hybris_orders_id is null then isnull(#s_hybris_orders_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_orders_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_orders_inserts
  left join p_hybris_orders
    on #s_hybris_orders_inserts.bk_hash = p_hybris_orders.bk_hash
   and p_hybris_orders.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_orders
    on p_hybris_orders.bk_hash = s_hybris_orders.bk_hash
   and p_hybris_orders.s_hybris_orders_id = s_hybris_orders.s_hybris_orders_id
 where s_hybris_orders.s_hybris_orders_id is null
    or (s_hybris_orders.s_hybris_orders_id is not null
        and s_hybris_orders.dv_hash <> #s_hybris_orders_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_orders @current_dv_batch_id

end
