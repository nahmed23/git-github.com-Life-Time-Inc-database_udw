CREATE PROC [dbo].[proc_etl_hybris_seven_day_orders] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_SEVEN_DAY_ORDERS

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_SEVEN_DAY_ORDERS (
       bk_hash,
       OrderCode,
       OrderEntryEntryNumber,
       OrderEntryTrackingNumber,
       OrderEntrySettlementDatetime,
       OrderDatetime,
       OrderEntryShippingCost,
       OrderEntryTotalTax,
       OrderLtfPartyId,
       OrderCommisionEmployeeId,
       OrderAutoshipFlag,
       OrderEntryProductCode,
       OrderEntryProductName,
       OrderEntryProductDescription,
       OrderEntryQuantity,
       OrderEntryBasePrice,
       OrderEntryTotalPrice,
       OrderEntryTotalDiscounts,
       OrderEntryRefundStatus,
       OrderEntryRefundDatetime,
       OrderEntryRefundAmount,
       RefundReason,
       CustomerName,
       CustomerEmail,
       CustomerGroup,
       MethodOfPay,
       FulfillmentPartner,
       AffiliateID,
       mmsTransactionID,
       mmsPackageID,
       SelectedClub,
       LTBUCKSearned,
       Capture_LTBUCKS,
       Capture_amex,
       Capture_discover,
       Capture_master,
       Capture_visa,
       Capture_PAYPAL,
       Refund_LTBUCKS,
       Refund_amex,
       Refund_discover,
       Refund_master,
       Refund_visa,
       Refund_PAYPAL,
       Ordstat,
       OEstat,
       OEsettlementFlag,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(OrderCode,'z#@$k%&P')+'P%#&z$@k'+isnull(cast(OrderEntryEntryNumber as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       OrderCode,
       OrderEntryEntryNumber,
       OrderEntryTrackingNumber,
       OrderEntrySettlementDatetime,
       OrderDatetime,
       OrderEntryShippingCost,
       OrderEntryTotalTax,
       OrderLtfPartyId,
       OrderCommisionEmployeeId,
       OrderAutoshipFlag,
       OrderEntryProductCode,
       OrderEntryProductName,
       OrderEntryProductDescription,
       OrderEntryQuantity,
       OrderEntryBasePrice,
       OrderEntryTotalPrice,
       OrderEntryTotalDiscounts,
       OrderEntryRefundStatus,
       OrderEntryRefundDatetime,
       OrderEntryRefundAmount,
       RefundReason,
       CustomerName,
       CustomerEmail,
       CustomerGroup,
       MethodOfPay,
       FulfillmentPartner,
       AffiliateID,
       mmsTransactionID,
       mmsPackageID,
       SelectedClub,
       LTBUCKSearned,
       Capture_LTBUCKS,
       Capture_amex,
       Capture_discover,
       Capture_master,
       Capture_visa,
       Capture_PAYPAL,
       Refund_LTBUCKS,
       Refund_amex,
       Refund_discover,
       Refund_master,
       Refund_visa,
       Refund_PAYPAL,
       Ordstat,
       OEstat,
       OEsettlementFlag,
       isnull(cast(stage_hybris_SEVEN_DAY_ORDERS.OrderDatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_hybris_SEVEN_DAY_ORDERS
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_seven_day_orders @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_seven_day_orders (
       bk_hash,
       order_code,
       order_entry_entry_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_hybris_SEVEN_DAY_ORDERS.bk_hash,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderCode order_code,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryEntryNumber order_entry_entry_number,
       isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderDatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_SEVEN_DAY_ORDERS
  left join h_hybris_seven_day_orders
    on stage_hash_hybris_SEVEN_DAY_ORDERS.bk_hash = h_hybris_seven_day_orders.bk_hash
 where h_hybris_seven_day_orders_id is null
   and stage_hash_hybris_SEVEN_DAY_ORDERS.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_seven_day_orders
if object_id('tempdb..#l_hybris_seven_day_orders_inserts') is not null drop table #l_hybris_seven_day_orders_inserts
create table #l_hybris_seven_day_orders_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_SEVEN_DAY_ORDERS.bk_hash,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderCode order_code,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryEntryNumber order_entry_entry_number,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderLtfPartyId order_ltf_party_id,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderCommisionEmployeeId order_commision_employee_id,
       stage_hash_hybris_SEVEN_DAY_ORDERS.AffiliateID affiliate_id,
       stage_hash_hybris_SEVEN_DAY_ORDERS.mmsTransactionID mms_transaction_id,
       stage_hash_hybris_SEVEN_DAY_ORDERS.mmsPackageID mms_package_id,
       isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderDatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryEntryNumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderLtfPartyId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderCommisionEmployeeId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.AffiliateID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.mmsTransactionID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.mmsPackageID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_SEVEN_DAY_ORDERS
 where stage_hash_hybris_SEVEN_DAY_ORDERS.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_seven_day_orders records
set @insert_date_time = getdate()
insert into l_hybris_seven_day_orders (
       bk_hash,
       order_code,
       order_entry_entry_number,
       order_ltf_party_id,
       order_commision_employee_id,
       affiliate_id,
       mms_transaction_id,
       mms_package_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_seven_day_orders_inserts.bk_hash,
       #l_hybris_seven_day_orders_inserts.order_code,
       #l_hybris_seven_day_orders_inserts.order_entry_entry_number,
       #l_hybris_seven_day_orders_inserts.order_ltf_party_id,
       #l_hybris_seven_day_orders_inserts.order_commision_employee_id,
       #l_hybris_seven_day_orders_inserts.affiliate_id,
       #l_hybris_seven_day_orders_inserts.mms_transaction_id,
       #l_hybris_seven_day_orders_inserts.mms_package_id,
       case when l_hybris_seven_day_orders.l_hybris_seven_day_orders_id is null then isnull(#l_hybris_seven_day_orders_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_seven_day_orders_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_seven_day_orders_inserts
  left join p_hybris_seven_day_orders
    on #l_hybris_seven_day_orders_inserts.bk_hash = p_hybris_seven_day_orders.bk_hash
   and p_hybris_seven_day_orders.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_seven_day_orders
    on p_hybris_seven_day_orders.bk_hash = l_hybris_seven_day_orders.bk_hash
   and p_hybris_seven_day_orders.l_hybris_seven_day_orders_id = l_hybris_seven_day_orders.l_hybris_seven_day_orders_id
 where l_hybris_seven_day_orders.l_hybris_seven_day_orders_id is null
    or (l_hybris_seven_day_orders.l_hybris_seven_day_orders_id is not null
        and l_hybris_seven_day_orders.dv_hash <> #l_hybris_seven_day_orders_inserts.source_hash)

--calculate hash and lookup to current s_hybris_seven_day_orders
if object_id('tempdb..#s_hybris_seven_day_orders_inserts') is not null drop table #s_hybris_seven_day_orders_inserts
create table #s_hybris_seven_day_orders_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_SEVEN_DAY_ORDERS.bk_hash,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderCode order_code,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryEntryNumber order_entry_entry_number,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryTrackingNumber order_entry_tracking_number,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntrySettlementDatetime order_entry_settlement_datetime,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderDatetime order_datetime,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryShippingCost order_entry_shipping_cost,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryTotalTax order_entry_total_tax,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderAutoshipFlag order_auto_ship_flag,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryProductCode order_entry_product_code,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryProductName order_entry_product_name,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryProductDescription order_entry_product_description,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryQuantity order_entry_quantity,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryBasePrice order_entry_base_price,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryTotalPrice order_entry_total_price,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryTotalDiscounts order_entry_total_discounts,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryRefundStatus order_entry_refund_status,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryRefundDatetime order_entry_refund_datetime,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryRefundAmount order_entry_refund_amount,
       stage_hash_hybris_SEVEN_DAY_ORDERS.RefundReason refund_reason,
       stage_hash_hybris_SEVEN_DAY_ORDERS.CustomerName customer_name,
       stage_hash_hybris_SEVEN_DAY_ORDERS.CustomerEmail customer_email,
       stage_hash_hybris_SEVEN_DAY_ORDERS.CustomerGroup customer_group,
       stage_hash_hybris_SEVEN_DAY_ORDERS.MethodOfPay method_of_pay,
       stage_hash_hybris_SEVEN_DAY_ORDERS.FulfillmentPartner fulfillment_partner,
       stage_hash_hybris_SEVEN_DAY_ORDERS.SelectedClub selected_club,
       stage_hash_hybris_SEVEN_DAY_ORDERS.LTBUCKSearned lt_bucks_earned,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_LTBUCKS capture_lt_bucks,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_amex capture_amex,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_discover capture_discover,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_master capture_master,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_visa capture_visa,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_PAYPAL capture_paypal,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_LTBUCKS refund_lt_bucks,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_amex refund_amex,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_discover refund_discover,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_master refund_master,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_visa refund_visa,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_PAYPAL refund_paypal,
       stage_hash_hybris_SEVEN_DAY_ORDERS.Ordstat ord_stat,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OEstat oe_stat,
       stage_hash_hybris_SEVEN_DAY_ORDERS.OEsettlementFlag oe_settlement_flag,
       isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderDatetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryEntryNumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryTrackingNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntrySettlementDatetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_SEVEN_DAY_ORDERS.OrderDatetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryShippingCost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryTotalTax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderAutoshipFlag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryProductCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryProductName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryProductDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryQuantity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryBasePrice as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryTotalPrice as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryTotalDiscounts as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryRefundStatus,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryRefundDatetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OrderEntryRefundAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.RefundReason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.CustomerName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.CustomerEmail,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.CustomerGroup,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.MethodOfPay,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.FulfillmentPartner,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.SelectedClub,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.LTBUCKSearned as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_LTBUCKS as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_amex as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_discover as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_master as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_visa as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Capture_PAYPAL as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_LTBUCKS as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_amex as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_discover as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_master as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_visa as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.Refund_PAYPAL as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.Ordstat,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_SEVEN_DAY_ORDERS.OEstat,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_SEVEN_DAY_ORDERS.OEsettlementFlag as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_SEVEN_DAY_ORDERS
 where stage_hash_hybris_SEVEN_DAY_ORDERS.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_seven_day_orders records
set @insert_date_time = getdate()
insert into s_hybris_seven_day_orders (
       bk_hash,
       order_code,
       order_entry_entry_number,
       order_entry_tracking_number,
       order_entry_settlement_datetime,
       order_datetime,
       order_entry_shipping_cost,
       order_entry_total_tax,
       order_auto_ship_flag,
       order_entry_product_code,
       order_entry_product_name,
       order_entry_product_description,
       order_entry_quantity,
       order_entry_base_price,
       order_entry_total_price,
       order_entry_total_discounts,
       order_entry_refund_status,
       order_entry_refund_datetime,
       order_entry_refund_amount,
       refund_reason,
       customer_name,
       customer_email,
       customer_group,
       method_of_pay,
       fulfillment_partner,
       selected_club,
       lt_bucks_earned,
       capture_lt_bucks,
       capture_amex,
       capture_discover,
       capture_master,
       capture_visa,
       capture_paypal,
       refund_lt_bucks,
       refund_amex,
       refund_discover,
       refund_master,
       refund_visa,
       refund_paypal,
       ord_stat,
       oe_stat,
       oe_settlement_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_seven_day_orders_inserts.bk_hash,
       #s_hybris_seven_day_orders_inserts.order_code,
       #s_hybris_seven_day_orders_inserts.order_entry_entry_number,
       #s_hybris_seven_day_orders_inserts.order_entry_tracking_number,
       #s_hybris_seven_day_orders_inserts.order_entry_settlement_datetime,
       #s_hybris_seven_day_orders_inserts.order_datetime,
       #s_hybris_seven_day_orders_inserts.order_entry_shipping_cost,
       #s_hybris_seven_day_orders_inserts.order_entry_total_tax,
       #s_hybris_seven_day_orders_inserts.order_auto_ship_flag,
       #s_hybris_seven_day_orders_inserts.order_entry_product_code,
       #s_hybris_seven_day_orders_inserts.order_entry_product_name,
       #s_hybris_seven_day_orders_inserts.order_entry_product_description,
       #s_hybris_seven_day_orders_inserts.order_entry_quantity,
       #s_hybris_seven_day_orders_inserts.order_entry_base_price,
       #s_hybris_seven_day_orders_inserts.order_entry_total_price,
       #s_hybris_seven_day_orders_inserts.order_entry_total_discounts,
       #s_hybris_seven_day_orders_inserts.order_entry_refund_status,
       #s_hybris_seven_day_orders_inserts.order_entry_refund_datetime,
       #s_hybris_seven_day_orders_inserts.order_entry_refund_amount,
       #s_hybris_seven_day_orders_inserts.refund_reason,
       #s_hybris_seven_day_orders_inserts.customer_name,
       #s_hybris_seven_day_orders_inserts.customer_email,
       #s_hybris_seven_day_orders_inserts.customer_group,
       #s_hybris_seven_day_orders_inserts.method_of_pay,
       #s_hybris_seven_day_orders_inserts.fulfillment_partner,
       #s_hybris_seven_day_orders_inserts.selected_club,
       #s_hybris_seven_day_orders_inserts.lt_bucks_earned,
       #s_hybris_seven_day_orders_inserts.capture_lt_bucks,
       #s_hybris_seven_day_orders_inserts.capture_amex,
       #s_hybris_seven_day_orders_inserts.capture_discover,
       #s_hybris_seven_day_orders_inserts.capture_master,
       #s_hybris_seven_day_orders_inserts.capture_visa,
       #s_hybris_seven_day_orders_inserts.capture_paypal,
       #s_hybris_seven_day_orders_inserts.refund_lt_bucks,
       #s_hybris_seven_day_orders_inserts.refund_amex,
       #s_hybris_seven_day_orders_inserts.refund_discover,
       #s_hybris_seven_day_orders_inserts.refund_master,
       #s_hybris_seven_day_orders_inserts.refund_visa,
       #s_hybris_seven_day_orders_inserts.refund_paypal,
       #s_hybris_seven_day_orders_inserts.ord_stat,
       #s_hybris_seven_day_orders_inserts.oe_stat,
       #s_hybris_seven_day_orders_inserts.oe_settlement_flag,
       case when s_hybris_seven_day_orders.s_hybris_seven_day_orders_id is null then isnull(#s_hybris_seven_day_orders_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_seven_day_orders_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_seven_day_orders_inserts
  left join p_hybris_seven_day_orders
    on #s_hybris_seven_day_orders_inserts.bk_hash = p_hybris_seven_day_orders.bk_hash
   and p_hybris_seven_day_orders.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_seven_day_orders
    on p_hybris_seven_day_orders.bk_hash = s_hybris_seven_day_orders.bk_hash
   and p_hybris_seven_day_orders.s_hybris_seven_day_orders_id = s_hybris_seven_day_orders.s_hybris_seven_day_orders_id
 where s_hybris_seven_day_orders.s_hybris_seven_day_orders_id is null
    or (s_hybris_seven_day_orders.s_hybris_seven_day_orders_id is not null
        and s_hybris_seven_day_orders.dv_hash <> #s_hybris_seven_day_orders_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_seven_day_orders @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_hybris_seven_day_orders @current_dv_batch_id

end
