CREATE PROC [dbo].[proc_etl_magento_payment_breakdown] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_payment_breakdown

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_payment_breakdown (
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
       OrderNum,
       OENum,
       SubCatName,
       ProductID,
       ProductName,
       MMSProductID,
       DeliveryState,
       Tax,
       OrderShippingAmount,
       OrdDate,
       TranDate,
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
       ReportRunDateTime,
       ReportStartDate,
       ReportEndDate,
       TransactionAmount,
       jan_one,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(OrderNum,'z#@$k%&P')+'P%#&z$@k'+isnull(cast(OENum as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(TranDate,'z#@$k%&P'))),2) bk_hash,
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
       OrderNum,
       OENum,
       SubCatName,
       ProductID,
       ProductName,
       MMSProductID,
       DeliveryState,
       Tax,
       OrderShippingAmount,
       OrdDate,
       TranDate,
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
       ReportRunDateTime,
       ReportStartDate,
       ReportEndDate,
       TransactionAmount,
       jan_one,
       isnull(cast(stage_magento_payment_breakdown.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_payment_breakdown
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_payment_breakdown @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_payment_breakdown (
       bk_hash,
       OrderNum,
       OENum,
       TranDate,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_magento_payment_breakdown.bk_hash,
       stage_hash_magento_payment_breakdown.OrderNum OrderNum,
       stage_hash_magento_payment_breakdown.OENum OENum,
       stage_hash_magento_payment_breakdown.TranDate TranDate,
       isnull(cast(stage_hash_magento_payment_breakdown.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       37,
       @insert_date_time,
       @user
  from stage_hash_magento_payment_breakdown
  left join h_magento_payment_breakdown
    on stage_hash_magento_payment_breakdown.bk_hash = h_magento_payment_breakdown.bk_hash
 where h_magento_payment_breakdown_id is null
   and stage_hash_magento_payment_breakdown.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_payment_breakdown
if object_id('tempdb..#l_magento_payment_breakdown_inserts') is not null drop table #l_magento_payment_breakdown_inserts
create table #l_magento_payment_breakdown_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_payment_breakdown.bk_hash,
       stage_hash_magento_payment_breakdown.OrderNum OrderNum,
       stage_hash_magento_payment_breakdown.OENum OENum,
       stage_hash_magento_payment_breakdown.TranDate TranDate,
       stage_hash_magento_payment_breakdown.ClubID club_id,
       stage_hash_magento_payment_breakdown.OfferingID offering_id,
       stage_hash_magento_payment_breakdown.ProductID product_id,
       stage_hash_magento_payment_breakdown.MMSProductID mms_product_id,
       stage_hash_magento_payment_breakdown.MMSTransactionID mms_transaction_id,
       stage_hash_magento_payment_breakdown.MMSPackageID mms_package_id,
       stage_hash_magento_payment_breakdown.MemberID member_id,
       isnull(cast(stage_hash_magento_payment_breakdown.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.OrderNum,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.OENum as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.TranDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.ClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.OfferingID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.ProductID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.MMSProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.MMSTransactionID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.MMSPackageID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.MemberID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_payment_breakdown
 where stage_hash_magento_payment_breakdown.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_payment_breakdown records
set @insert_date_time = getdate()
insert into l_magento_payment_breakdown (
       bk_hash,
       OrderNum,
       OENum,
       TranDate,
       club_id,
       offering_id,
       product_id,
       mms_product_id,
       mms_transaction_id,
       mms_package_id,
       member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_payment_breakdown_inserts.bk_hash,
       #l_magento_payment_breakdown_inserts.OrderNum,
       #l_magento_payment_breakdown_inserts.OENum,
       #l_magento_payment_breakdown_inserts.TranDate,
       #l_magento_payment_breakdown_inserts.club_id,
       #l_magento_payment_breakdown_inserts.offering_id,
       #l_magento_payment_breakdown_inserts.product_id,
       #l_magento_payment_breakdown_inserts.mms_product_id,
       #l_magento_payment_breakdown_inserts.mms_transaction_id,
       #l_magento_payment_breakdown_inserts.mms_package_id,
       #l_magento_payment_breakdown_inserts.member_id,
       case when l_magento_payment_breakdown.l_magento_payment_breakdown_id is null then isnull(#l_magento_payment_breakdown_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       37,
       #l_magento_payment_breakdown_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_payment_breakdown_inserts
  left join p_magento_payment_breakdown
    on #l_magento_payment_breakdown_inserts.bk_hash = p_magento_payment_breakdown.bk_hash
   and p_magento_payment_breakdown.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_payment_breakdown
    on p_magento_payment_breakdown.bk_hash = l_magento_payment_breakdown.bk_hash
   and p_magento_payment_breakdown.l_magento_payment_breakdown_id = l_magento_payment_breakdown.l_magento_payment_breakdown_id
 where l_magento_payment_breakdown.l_magento_payment_breakdown_id is null
    or (l_magento_payment_breakdown.l_magento_payment_breakdown_id is not null
        and l_magento_payment_breakdown.dv_hash <> #l_magento_payment_breakdown_inserts.source_hash)

--calculate hash and lookup to current s_magento_payment_breakdown
if object_id('tempdb..#s_magento_payment_breakdown_inserts') is not null drop table #s_magento_payment_breakdown_inserts
create table #s_magento_payment_breakdown_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_payment_breakdown.bk_hash,
       stage_hash_magento_payment_breakdown.AccountSet account_set,
       stage_hash_magento_payment_breakdown.LineCompany line_company,
       stage_hash_magento_payment_breakdown.LedgerAccount ledger_account,
       stage_hash_magento_payment_breakdown.WorkdayRegion workday_region,
       stage_hash_magento_payment_breakdown.CostCenter cost_center,
       stage_hash_magento_payment_breakdown.SpendCategory spend_category,
       stage_hash_magento_payment_breakdown.RevenueCategory revenue_category,
       stage_hash_magento_payment_breakdown.MerchantLocation merchant_location,
       stage_hash_magento_payment_breakdown.OrderNum OrderNum,
       stage_hash_magento_payment_breakdown.OENum OENum,
       stage_hash_magento_payment_breakdown.SubCatName sub_cat_name,
       stage_hash_magento_payment_breakdown.ProductName product_name,
       stage_hash_magento_payment_breakdown.DeliveryState delivery_state,
       stage_hash_magento_payment_breakdown.Tax tax,
       stage_hash_magento_payment_breakdown.OrderShippingAmount OrderShippingAmount,
       stage_hash_magento_payment_breakdown.OrdDate ord_date,
       stage_hash_magento_payment_breakdown.TranDate TranDate,
       stage_hash_magento_payment_breakdown.ShipDate ship_date,
       stage_hash_magento_payment_breakdown.Capture_LTBUCKS capture_ltbucks,
       stage_hash_magento_payment_breakdown.Capture_amex capture_amex,
       stage_hash_magento_payment_breakdown.Capture_discover capture_discover,
       stage_hash_magento_payment_breakdown.Capture_master capture_master,
       stage_hash_magento_payment_breakdown.Capture_visa capture_visa,
       stage_hash_magento_payment_breakdown.Capture_PAYPAL capture_paypal,
       stage_hash_magento_payment_breakdown.Refund_LTBUCKS refund_ltbucks,
       stage_hash_magento_payment_breakdown.Refund_amex refund_amex,
       stage_hash_magento_payment_breakdown.Refund_discover refund_discover,
       stage_hash_magento_payment_breakdown.Refund_master refund_master,
       stage_hash_magento_payment_breakdown.Refund_visa refund_visa,
       stage_hash_magento_payment_breakdown.Refund_PAYPAL refund_paypal,
       stage_hash_magento_payment_breakdown.ReportRunDateTime report_run_date_time,
       stage_hash_magento_payment_breakdown.ReportStartDate report_start_date,
       stage_hash_magento_payment_breakdown.ReportEndDate report_end_date,
       stage_hash_magento_payment_breakdown.TransactionAmount transaction_amount,
       stage_hash_magento_payment_breakdown.jan_one jan_one,
       isnull(cast(stage_hash_magento_payment_breakdown.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.AccountSet,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.LineCompany,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.LedgerAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.WorkdayRegion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.CostCenter,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.SpendCategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.RevenueCategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.MerchantLocation,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.OrderNum,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.OENum as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.SubCatName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.ProductName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.DeliveryState,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Tax as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.OrderShippingAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.OrdDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.TranDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.ShipDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Capture_LTBUCKS as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Capture_amex as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Capture_discover as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Capture_master as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Capture_visa as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Capture_PAYPAL as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Refund_LTBUCKS as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Refund_amex as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Refund_discover as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Refund_master as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Refund_visa as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.Refund_PAYPAL as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_payment_breakdown.ReportRunDateTime,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_payment_breakdown.ReportStartDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_payment_breakdown.ReportEndDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_payment_breakdown.TransactionAmount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_payment_breakdown.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_payment_breakdown
 where stage_hash_magento_payment_breakdown.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_payment_breakdown records
set @insert_date_time = getdate()
insert into s_magento_payment_breakdown (
       bk_hash,
       account_set,
       line_company,
       ledger_account,
       workday_region,
       cost_center,
       spend_category,
       revenue_category,
       merchant_location,
       OrderNum,
       OENum,
       sub_cat_name,
       product_name,
       delivery_state,
       tax,
       OrderShippingAmount,
       ord_date,
       TranDate,
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
       report_run_date_time,
       report_start_date,
       report_end_date,
       transaction_amount,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_payment_breakdown_inserts.bk_hash,
       #s_magento_payment_breakdown_inserts.account_set,
       #s_magento_payment_breakdown_inserts.line_company,
       #s_magento_payment_breakdown_inserts.ledger_account,
       #s_magento_payment_breakdown_inserts.workday_region,
       #s_magento_payment_breakdown_inserts.cost_center,
       #s_magento_payment_breakdown_inserts.spend_category,
       #s_magento_payment_breakdown_inserts.revenue_category,
       #s_magento_payment_breakdown_inserts.merchant_location,
       #s_magento_payment_breakdown_inserts.OrderNum,
       #s_magento_payment_breakdown_inserts.OENum,
       #s_magento_payment_breakdown_inserts.sub_cat_name,
       #s_magento_payment_breakdown_inserts.product_name,
       #s_magento_payment_breakdown_inserts.delivery_state,
       #s_magento_payment_breakdown_inserts.tax,
       #s_magento_payment_breakdown_inserts.OrderShippingAmount,
       #s_magento_payment_breakdown_inserts.ord_date,
       #s_magento_payment_breakdown_inserts.TranDate,
       #s_magento_payment_breakdown_inserts.ship_date,
       #s_magento_payment_breakdown_inserts.capture_ltbucks,
       #s_magento_payment_breakdown_inserts.capture_amex,
       #s_magento_payment_breakdown_inserts.capture_discover,
       #s_magento_payment_breakdown_inserts.capture_master,
       #s_magento_payment_breakdown_inserts.capture_visa,
       #s_magento_payment_breakdown_inserts.capture_paypal,
       #s_magento_payment_breakdown_inserts.refund_ltbucks,
       #s_magento_payment_breakdown_inserts.refund_amex,
       #s_magento_payment_breakdown_inserts.refund_discover,
       #s_magento_payment_breakdown_inserts.refund_master,
       #s_magento_payment_breakdown_inserts.refund_visa,
       #s_magento_payment_breakdown_inserts.refund_paypal,
       #s_magento_payment_breakdown_inserts.report_run_date_time,
       #s_magento_payment_breakdown_inserts.report_start_date,
       #s_magento_payment_breakdown_inserts.report_end_date,
       #s_magento_payment_breakdown_inserts.transaction_amount,
       #s_magento_payment_breakdown_inserts.jan_one,
       case when s_magento_payment_breakdown.s_magento_payment_breakdown_id is null then isnull(#s_magento_payment_breakdown_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       37,
       #s_magento_payment_breakdown_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_payment_breakdown_inserts
  left join p_magento_payment_breakdown
    on #s_magento_payment_breakdown_inserts.bk_hash = p_magento_payment_breakdown.bk_hash
   and p_magento_payment_breakdown.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_payment_breakdown
    on p_magento_payment_breakdown.bk_hash = s_magento_payment_breakdown.bk_hash
   and p_magento_payment_breakdown.s_magento_payment_breakdown_id = s_magento_payment_breakdown.s_magento_payment_breakdown_id
 where s_magento_payment_breakdown.s_magento_payment_breakdown_id is null
    or (s_magento_payment_breakdown.s_magento_payment_breakdown_id is not null
        and s_magento_payment_breakdown.dv_hash <> #s_magento_payment_breakdown_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_payment_breakdown @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_payment_breakdown @current_dv_batch_id

--run Fact procs
exec dbo.proc_fact_magento_payment_breakdown @current_dv_batch_id

end
