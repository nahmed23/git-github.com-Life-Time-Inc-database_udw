CREATE PROC [dbo].[proc_etl_hybris_Ecommerce_payment_breakdown] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_ECommercePaymentBreakdown

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_ECommercePaymentBreakdown (
       bk_hash,
       AccountSet,
       LineCompany,
       LedgerAccount,
       WorkdayRegion,
       ClubID,
       CostCenter,
       OfferingID,
       SpendCategory,
       RevenueCategory,
       MerchantLocation,
       order_num,
       oe_num,
       SubCatName,
       ProductID,
       ProductName,
       MMSProductID,
       DeliveryState,
       Tax,
       Shipping,
       OrdDate,
       tran_date,
       ShipDate,
       MMSTransactionID,
       MMSPackageID,
       MemberID,
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
       ReportStartDate,
       ReportEndDate,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(order_num,'z#@$k%&P')+'P%#&z$@k'+isnull(cast(oe_num as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(tran_date,'z#@$k%&P'))),2) bk_hash,
       AccountSet,
       LineCompany,
       LedgerAccount,
       WorkdayRegion,
       ClubID,
       CostCenter,
       OfferingID,
       SpendCategory,
       RevenueCategory,
       MerchantLocation,
       order_num,
       oe_num,
       SubCatName,
       ProductID,
       ProductName,
       MMSProductID,
       DeliveryState,
       Tax,
       Shipping,
       OrdDate,
       tran_date,
       ShipDate,
       MMSTransactionID,
       MMSPackageID,
       MemberID,
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
       ReportStartDate,
       ReportEndDate,
       jan_one,
       isnull(cast(stage_hybris_ECommercePaymentBreakdown.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_ECommercePaymentBreakdown
 where dv_batch_id = @current_dv_batch_id
 
--Run PIT proc for retry logic
exec dbo.proc_p_hybris_Ecommerce_payment_breakdown @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_Ecommerce_payment_breakdown (
       bk_hash,
       order_num,
       oe_num,
       tran_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_ECommercePaymentBreakdown.bk_hash,
       stage_hash_hybris_ECommercePaymentBreakdown.order_num order_num,
       stage_hash_hybris_ECommercePaymentBreakdown.oe_num oe_num,
       stage_hash_hybris_ECommercePaymentBreakdown.tran_date tran_date,
       isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_ECommercePaymentBreakdown
  left join h_hybris_Ecommerce_payment_breakdown
    on stage_hash_hybris_ECommercePaymentBreakdown.bk_hash = h_hybris_Ecommerce_payment_breakdown.bk_hash
 where h_hybris_Ecommerce_payment_breakdown_id is null
   and stage_hash_hybris_ECommercePaymentBreakdown.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_Ecommerce_payment_breakdown
if object_id('tempdb..#l_hybris_Ecommerce_payment_breakdown_inserts') is not null drop table #l_hybris_Ecommerce_payment_breakdown_inserts
create table #l_hybris_Ecommerce_payment_breakdown_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_ECommercePaymentBreakdown.bk_hash,
       stage_hash_hybris_ECommercePaymentBreakdown.ClubID club_id,
       stage_hash_hybris_ECommercePaymentBreakdown.OfferingID offering_id,
       stage_hash_hybris_ECommercePaymentBreakdown.order_num order_num,
       stage_hash_hybris_ECommercePaymentBreakdown.oe_num oe_num,
       stage_hash_hybris_ECommercePaymentBreakdown.ProductID product_id,
       stage_hash_hybris_ECommercePaymentBreakdown.MMSProductID mms_product_id,
       stage_hash_hybris_ECommercePaymentBreakdown.tran_date tran_date,
       stage_hash_hybris_ECommercePaymentBreakdown.MMSTransactionID mms_transaction_id,
       stage_hash_hybris_ECommercePaymentBreakdown.MMSPackageID mms_package_id,
       stage_hash_hybris_ECommercePaymentBreakdown.MemberID member_id,
       isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.OfferingID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.order_num,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.oe_num as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.ProductID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.MMSProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.tran_date,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.MMSTransactionID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.MMSPackageID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.MemberID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_ECommercePaymentBreakdown
 where stage_hash_hybris_ECommercePaymentBreakdown.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_Ecommerce_payment_breakdown records
set @insert_date_time = getdate()
insert into l_hybris_Ecommerce_payment_breakdown (
       bk_hash,
       club_id,
       offering_id,
       order_num,
       oe_num,
       product_id,
       mms_product_id,
       tran_date,
       mms_transaction_id,
       mms_package_id,
       member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_Ecommerce_payment_breakdown_inserts.bk_hash,
       #l_hybris_Ecommerce_payment_breakdown_inserts.club_id,
       #l_hybris_Ecommerce_payment_breakdown_inserts.offering_id,
       #l_hybris_Ecommerce_payment_breakdown_inserts.order_num,
       #l_hybris_Ecommerce_payment_breakdown_inserts.oe_num,
       #l_hybris_Ecommerce_payment_breakdown_inserts.product_id,
       #l_hybris_Ecommerce_payment_breakdown_inserts.mms_product_id,
       #l_hybris_Ecommerce_payment_breakdown_inserts.tran_date,
       #l_hybris_Ecommerce_payment_breakdown_inserts.mms_transaction_id,
       #l_hybris_Ecommerce_payment_breakdown_inserts.mms_package_id,
       #l_hybris_Ecommerce_payment_breakdown_inserts.member_id,
       case when l_hybris_Ecommerce_payment_breakdown.l_hybris_Ecommerce_payment_breakdown_id is null then isnull(#l_hybris_Ecommerce_payment_breakdown_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_Ecommerce_payment_breakdown_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_Ecommerce_payment_breakdown_inserts
  left join p_hybris_Ecommerce_payment_breakdown
    on #l_hybris_Ecommerce_payment_breakdown_inserts.bk_hash = p_hybris_Ecommerce_payment_breakdown.bk_hash
   and p_hybris_Ecommerce_payment_breakdown.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_Ecommerce_payment_breakdown
    on p_hybris_Ecommerce_payment_breakdown.bk_hash = l_hybris_Ecommerce_payment_breakdown.bk_hash
   and p_hybris_Ecommerce_payment_breakdown.l_hybris_Ecommerce_payment_breakdown_id = l_hybris_Ecommerce_payment_breakdown.l_hybris_Ecommerce_payment_breakdown_id
 where l_hybris_Ecommerce_payment_breakdown.l_hybris_Ecommerce_payment_breakdown_id is null
    or (l_hybris_Ecommerce_payment_breakdown.l_hybris_Ecommerce_payment_breakdown_id is not null
        and l_hybris_Ecommerce_payment_breakdown.dv_hash <> #l_hybris_Ecommerce_payment_breakdown_inserts.source_hash)

--calculate hash and lookup to current s_hybris_Ecommerce_payment_breakdown
if object_id('tempdb..#s_hybris_Ecommerce_payment_breakdown_inserts') is not null drop table #s_hybris_Ecommerce_payment_breakdown_inserts
create table #s_hybris_Ecommerce_payment_breakdown_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_ECommercePaymentBreakdown.bk_hash,
       stage_hash_hybris_ECommercePaymentBreakdown.AccountSet account_set,
       stage_hash_hybris_ECommercePaymentBreakdown.LineCompany line_company,
       stage_hash_hybris_ECommercePaymentBreakdown.LedgerAccount ledger_account,
       stage_hash_hybris_ECommercePaymentBreakdown.WorkdayRegion workday_region,
       stage_hash_hybris_ECommercePaymentBreakdown.CostCenter cost_center,
       stage_hash_hybris_ECommercePaymentBreakdown.SpendCategory spend_category,
       stage_hash_hybris_ECommercePaymentBreakdown.RevenueCategory revenue_category,
       stage_hash_hybris_ECommercePaymentBreakdown.MerchantLocation merchant_location,
       stage_hash_hybris_ECommercePaymentBreakdown.order_num order_num,
       stage_hash_hybris_ECommercePaymentBreakdown.oe_num oe_num,
       stage_hash_hybris_ECommercePaymentBreakdown.SubCatName sub_cat_name,
       stage_hash_hybris_ECommercePaymentBreakdown.ProductName product_name,
       stage_hash_hybris_ECommercePaymentBreakdown.DeliveryState delivery_state,
       stage_hash_hybris_ECommercePaymentBreakdown.Tax tax,
       stage_hash_hybris_ECommercePaymentBreakdown.Shipping shipping,
       stage_hash_hybris_ECommercePaymentBreakdown.OrdDate ord_date,
       stage_hash_hybris_ECommercePaymentBreakdown.tran_date tran_date,
       stage_hash_hybris_ECommercePaymentBreakdown.ShipDate ship_date,
       stage_hash_hybris_ECommercePaymentBreakdown.Capture_LTBUCKS capture_ltbucks,
       stage_hash_hybris_ECommercePaymentBreakdown.Capture_amex capture_amex,
       stage_hash_hybris_ECommercePaymentBreakdown.Capture_discover capture_discover,
       stage_hash_hybris_ECommercePaymentBreakdown.Capture_master capture_master,
       stage_hash_hybris_ECommercePaymentBreakdown.Capture_visa capture_visa,
       stage_hash_hybris_ECommercePaymentBreakdown.Capture_PAYPAL capture_paypal,
       stage_hash_hybris_ECommercePaymentBreakdown.Refund_LTBUCKS refund_ltbucks,
       stage_hash_hybris_ECommercePaymentBreakdown.Refund_amex refund_amex,
       stage_hash_hybris_ECommercePaymentBreakdown.Refund_discover refund_discover,
       stage_hash_hybris_ECommercePaymentBreakdown.Refund_master refund_master,
       stage_hash_hybris_ECommercePaymentBreakdown.Refund_visa refund_visa,
       stage_hash_hybris_ECommercePaymentBreakdown.Refund_PAYPAL refund_paypal,
       stage_hash_hybris_ECommercePaymentBreakdown.ReportStartDate report_start_date,
       stage_hash_hybris_ECommercePaymentBreakdown.ReportEndDate report_end_date,
       stage_hash_hybris_ECommercePaymentBreakdown.jan_one jan_one,
       isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.AccountSet,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.LineCompany,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.LedgerAccount,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.WorkdayRegion,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.CostCenter,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.SpendCategory,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.RevenueCategory,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.MerchantLocation,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.order_num,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.oe_num as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.SubCatName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.ProductName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.DeliveryState,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Tax as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Shipping as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.OrdDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.tran_date,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_ECommercePaymentBreakdown.ShipDate,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Capture_LTBUCKS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Capture_amex as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Capture_discover as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Capture_master as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Capture_visa as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Capture_PAYPAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Refund_LTBUCKS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Refund_amex as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Refund_discover as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Refund_master as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Refund_visa as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_ECommercePaymentBreakdown.Refund_PAYPAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ECommercePaymentBreakdown.ReportStartDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ECommercePaymentBreakdown.ReportEndDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ECommercePaymentBreakdown.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_ECommercePaymentBreakdown
 where stage_hash_hybris_ECommercePaymentBreakdown.dv_batch_id = @current_dv_batch_id
 ----and stage_hash_hybris_ECommercePaymentBreakdown.bk_hash not in (select distinct bk_hash from dbo.s_hybris_Ecommerce_payment_breakdown)

--Insert all updated and new s_hybris_Ecommerce_payment_breakdown records
set @insert_date_time = getdate()
insert into s_hybris_Ecommerce_payment_breakdown (
       bk_hash,
       account_set,
       line_company,
       ledger_account,
       workday_region,
       cost_center,
       spend_category,
       revenue_category,
       merchant_location,
       order_num,
       oe_num,
       sub_cat_name,
       product_name,
       delivery_state,
       tax,
       shipping,
       ord_date,
       tran_date,
       ship_date,
       capture_ltbucks,
       capture_amex,
       capture_discover,
       capture_master,
       capture_visa,
       capture_paypal,
       refund_ltbucks,
       refund_amex,
       refund_discover,
       refund_master,
       refund_visa,
       refund_paypal,
       report_start_date,
       report_end_date,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_Ecommerce_payment_breakdown_inserts.bk_hash,
       #s_hybris_Ecommerce_payment_breakdown_inserts.account_set,
       #s_hybris_Ecommerce_payment_breakdown_inserts.line_company,
       #s_hybris_Ecommerce_payment_breakdown_inserts.ledger_account,
       #s_hybris_Ecommerce_payment_breakdown_inserts.workday_region,
       #s_hybris_Ecommerce_payment_breakdown_inserts.cost_center,
       #s_hybris_Ecommerce_payment_breakdown_inserts.spend_category,
       #s_hybris_Ecommerce_payment_breakdown_inserts.revenue_category,
       #s_hybris_Ecommerce_payment_breakdown_inserts.merchant_location,
       #s_hybris_Ecommerce_payment_breakdown_inserts.order_num,
       #s_hybris_Ecommerce_payment_breakdown_inserts.oe_num,
       #s_hybris_Ecommerce_payment_breakdown_inserts.sub_cat_name,
       #s_hybris_Ecommerce_payment_breakdown_inserts.product_name,
       #s_hybris_Ecommerce_payment_breakdown_inserts.delivery_state,
       #s_hybris_Ecommerce_payment_breakdown_inserts.tax,
       #s_hybris_Ecommerce_payment_breakdown_inserts.shipping,
       #s_hybris_Ecommerce_payment_breakdown_inserts.ord_date,
       #s_hybris_Ecommerce_payment_breakdown_inserts.tran_date,
       #s_hybris_Ecommerce_payment_breakdown_inserts.ship_date,
       #s_hybris_Ecommerce_payment_breakdown_inserts.capture_ltbucks,
       #s_hybris_Ecommerce_payment_breakdown_inserts.capture_amex,
       #s_hybris_Ecommerce_payment_breakdown_inserts.capture_discover,
       #s_hybris_Ecommerce_payment_breakdown_inserts.capture_master,
       #s_hybris_Ecommerce_payment_breakdown_inserts.capture_visa,
       #s_hybris_Ecommerce_payment_breakdown_inserts.capture_paypal,
       #s_hybris_Ecommerce_payment_breakdown_inserts.refund_ltbucks,
       #s_hybris_Ecommerce_payment_breakdown_inserts.refund_amex,
       #s_hybris_Ecommerce_payment_breakdown_inserts.refund_discover,
       #s_hybris_Ecommerce_payment_breakdown_inserts.refund_master,
       #s_hybris_Ecommerce_payment_breakdown_inserts.refund_visa,
       #s_hybris_Ecommerce_payment_breakdown_inserts.refund_paypal,
       #s_hybris_Ecommerce_payment_breakdown_inserts.report_start_date,
       #s_hybris_Ecommerce_payment_breakdown_inserts.report_end_date,
       #s_hybris_Ecommerce_payment_breakdown_inserts.jan_one,
       case when s_hybris_Ecommerce_payment_breakdown.s_hybris_Ecommerce_payment_breakdown_id is null then isnull(#s_hybris_Ecommerce_payment_breakdown_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
       else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_Ecommerce_payment_breakdown_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_Ecommerce_payment_breakdown_inserts
  left join p_hybris_Ecommerce_payment_breakdown
    on #s_hybris_Ecommerce_payment_breakdown_inserts.bk_hash = p_hybris_Ecommerce_payment_breakdown.bk_hash
   and p_hybris_Ecommerce_payment_breakdown.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_Ecommerce_payment_breakdown
    on p_hybris_Ecommerce_payment_breakdown.bk_hash = s_hybris_Ecommerce_payment_breakdown.bk_hash
   and p_hybris_Ecommerce_payment_breakdown.s_hybris_Ecommerce_payment_breakdown_id = s_hybris_Ecommerce_payment_breakdown.s_hybris_Ecommerce_payment_breakdown_id
 where s_hybris_Ecommerce_payment_breakdown.s_hybris_Ecommerce_payment_breakdown_id is null
    or (s_hybris_Ecommerce_payment_breakdown.s_hybris_Ecommerce_payment_breakdown_id is not null
        and s_hybris_Ecommerce_payment_breakdown.dv_hash <> #s_hybris_Ecommerce_payment_breakdown_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_Ecommerce_payment_breakdown @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_hybris_Ecommerce_payment_breakdown @current_dv_batch_id

--run fact load procs
exec dbo.proc_fact_hybris_Ecommerce_payment_breakdown @current_dv_batch_id

end
