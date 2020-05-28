CREATE PROC [dbo].[proc_etl_mms_sales_promotion] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_SalesPromotion

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_SalesPromotion (
       bk_hash,
       SalesPromotionID,
       EffectiveFromDateTime,
       EffectiveThruDateTime,
       DisplayText,
       ReceiptText,
       ValSalesPromotionTypeID,
       AvailableForAllSalesChannelsFlag,
       AvailableForAllClubsFlag,
       AvailableForAllCustomersFlag,
       InsertedDateTime,
       UpdatedDateTime,
       PromotionOwnerEmployeeID,
       PromotionCodeUsageLimit,
       PromotionCodeRequiredFlag,
       PromotionCodeIssuerCreateLimit,
       PromotionCodeOverallCreateLimit,
       CompanyID,
       ExcludeMyHealthCheckFlag,
       ValRevenueReportingCategoryID,
       ValSalesReportingCategoryID,
       ExcludeFromAttritionReportingFlag,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(SalesPromotionID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       SalesPromotionID,
       EffectiveFromDateTime,
       EffectiveThruDateTime,
       DisplayText,
       ReceiptText,
       ValSalesPromotionTypeID,
       AvailableForAllSalesChannelsFlag,
       AvailableForAllClubsFlag,
       AvailableForAllCustomersFlag,
       InsertedDateTime,
       UpdatedDateTime,
       PromotionOwnerEmployeeID,
       PromotionCodeUsageLimit,
       PromotionCodeRequiredFlag,
       PromotionCodeIssuerCreateLimit,
       PromotionCodeOverallCreateLimit,
       CompanyID,
       ExcludeMyHealthCheckFlag,
       ValRevenueReportingCategoryID,
       ValSalesReportingCategoryID,
       ExcludeFromAttritionReportingFlag,
       isnull(cast(stage_mms_SalesPromotion.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_SalesPromotion
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_sales_promotion @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_sales_promotion (
       bk_hash,
       sales_promotion_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_SalesPromotion.bk_hash,
       stage_hash_mms_SalesPromotion.SalesPromotionID sales_promotion_id,
       isnull(cast(stage_hash_mms_SalesPromotion.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_SalesPromotion
  left join h_mms_sales_promotion
    on stage_hash_mms_SalesPromotion.bk_hash = h_mms_sales_promotion.bk_hash
 where h_mms_sales_promotion_id is null
   and stage_hash_mms_SalesPromotion.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_sales_promotion
if object_id('tempdb..#l_mms_sales_promotion_inserts') is not null drop table #l_mms_sales_promotion_inserts
create table #l_mms_sales_promotion_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_SalesPromotion.bk_hash,
       stage_hash_mms_SalesPromotion.SalesPromotionID sales_promotion_id,
       stage_hash_mms_SalesPromotion.ValSalesPromotionTypeID val_sales_promotion_type_id,
       stage_hash_mms_SalesPromotion.PromotionOwnerEmployeeID promotion_owner_employee_id,
       stage_hash_mms_SalesPromotion.CompanyID company_id,
       stage_hash_mms_SalesPromotion.ValRevenueReportingCategoryID val_revenue_reporting_category_id,
       stage_hash_mms_SalesPromotion.ValSalesReportingCategoryID val_sales_reporting_category_id,
       isnull(cast(stage_hash_mms_SalesPromotion.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.SalesPromotionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.ValSalesPromotionTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.PromotionOwnerEmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.CompanyID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.ValRevenueReportingCategoryID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.ValSalesReportingCategoryID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_SalesPromotion
 where stage_hash_mms_SalesPromotion.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_sales_promotion records
set @insert_date_time = getdate()
insert into l_mms_sales_promotion (
       bk_hash,
       sales_promotion_id,
       val_sales_promotion_type_id,
       promotion_owner_employee_id,
       company_id,
       val_revenue_reporting_category_id,
       val_sales_reporting_category_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_sales_promotion_inserts.bk_hash,
       #l_mms_sales_promotion_inserts.sales_promotion_id,
       #l_mms_sales_promotion_inserts.val_sales_promotion_type_id,
       #l_mms_sales_promotion_inserts.promotion_owner_employee_id,
       #l_mms_sales_promotion_inserts.company_id,
       #l_mms_sales_promotion_inserts.val_revenue_reporting_category_id,
       #l_mms_sales_promotion_inserts.val_sales_reporting_category_id,
       case when l_mms_sales_promotion.l_mms_sales_promotion_id is null then isnull(#l_mms_sales_promotion_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_sales_promotion_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_sales_promotion_inserts
  left join p_mms_sales_promotion
    on #l_mms_sales_promotion_inserts.bk_hash = p_mms_sales_promotion.bk_hash
   and p_mms_sales_promotion.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_sales_promotion
    on p_mms_sales_promotion.bk_hash = l_mms_sales_promotion.bk_hash
   and p_mms_sales_promotion.l_mms_sales_promotion_id = l_mms_sales_promotion.l_mms_sales_promotion_id
 where l_mms_sales_promotion.l_mms_sales_promotion_id is null
    or (l_mms_sales_promotion.l_mms_sales_promotion_id is not null
        and l_mms_sales_promotion.dv_hash <> #l_mms_sales_promotion_inserts.source_hash)

--calculate hash and lookup to current s_mms_sales_promotion
if object_id('tempdb..#s_mms_sales_promotion_inserts') is not null drop table #s_mms_sales_promotion_inserts
create table #s_mms_sales_promotion_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_SalesPromotion.bk_hash,
       stage_hash_mms_SalesPromotion.SalesPromotionID sales_promotion_id,
       stage_hash_mms_SalesPromotion.EffectiveFromDateTime effective_from_date_time,
       stage_hash_mms_SalesPromotion.EffectiveThruDateTime effective_thru_date_time,
       stage_hash_mms_SalesPromotion.DisplayText display_text,
       stage_hash_mms_SalesPromotion.ReceiptText receipt_text,
       stage_hash_mms_SalesPromotion.AvailableForAllSalesChannelsFlag available_for_all_sales_channels_flag,
       stage_hash_mms_SalesPromotion.AvailableForAllClubsFlag available_for_all_clubs_flag,
       stage_hash_mms_SalesPromotion.AvailableForAllCustomersFlag available_for_all_customers_flag,
       stage_hash_mms_SalesPromotion.InsertedDateTime inserted_date_time,
       stage_hash_mms_SalesPromotion.UpdatedDateTime updated_date_time,
       stage_hash_mms_SalesPromotion.PromotionCodeUsageLimit promotion_code_usage_limit,
       stage_hash_mms_SalesPromotion.PromotionCodeRequiredFlag promotion_code_required_flag,
       stage_hash_mms_SalesPromotion.PromotionCodeIssuerCreateLimit promotion_code_issuer_create_limit,
       stage_hash_mms_SalesPromotion.PromotionCodeOverallCreateLimit promotion_code_overall_create_limit,
       stage_hash_mms_SalesPromotion.ExcludeMyHealthCheckFlag exclude_my_health_check_flag,
       stage_hash_mms_SalesPromotion.ExcludeFromAttritionReportingFlag exclude_from_attrition_reporting_flag,
       isnull(cast(stage_hash_mms_SalesPromotion.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.SalesPromotionID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SalesPromotion.EffectiveFromDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SalesPromotion.EffectiveThruDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_SalesPromotion.DisplayText,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_SalesPromotion.ReceiptText,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.AvailableForAllSalesChannelsFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.AvailableForAllClubsFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.AvailableForAllCustomersFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SalesPromotion.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_SalesPromotion.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.PromotionCodeUsageLimit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.PromotionCodeRequiredFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.PromotionCodeIssuerCreateLimit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.PromotionCodeOverallCreateLimit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.ExcludeMyHealthCheckFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_SalesPromotion.ExcludeFromAttritionReportingFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_SalesPromotion
 where stage_hash_mms_SalesPromotion.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_sales_promotion records
set @insert_date_time = getdate()
insert into s_mms_sales_promotion (
       bk_hash,
       sales_promotion_id,
       effective_from_date_time,
       effective_thru_date_time,
       display_text,
       receipt_text,
       available_for_all_sales_channels_flag,
       available_for_all_clubs_flag,
       available_for_all_customers_flag,
       inserted_date_time,
       updated_date_time,
       promotion_code_usage_limit,
       promotion_code_required_flag,
       promotion_code_issuer_create_limit,
       promotion_code_overall_create_limit,
       exclude_my_health_check_flag,
       exclude_from_attrition_reporting_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_sales_promotion_inserts.bk_hash,
       #s_mms_sales_promotion_inserts.sales_promotion_id,
       #s_mms_sales_promotion_inserts.effective_from_date_time,
       #s_mms_sales_promotion_inserts.effective_thru_date_time,
       #s_mms_sales_promotion_inserts.display_text,
       #s_mms_sales_promotion_inserts.receipt_text,
       #s_mms_sales_promotion_inserts.available_for_all_sales_channels_flag,
       #s_mms_sales_promotion_inserts.available_for_all_clubs_flag,
       #s_mms_sales_promotion_inserts.available_for_all_customers_flag,
       #s_mms_sales_promotion_inserts.inserted_date_time,
       #s_mms_sales_promotion_inserts.updated_date_time,
       #s_mms_sales_promotion_inserts.promotion_code_usage_limit,
       #s_mms_sales_promotion_inserts.promotion_code_required_flag,
       #s_mms_sales_promotion_inserts.promotion_code_issuer_create_limit,
       #s_mms_sales_promotion_inserts.promotion_code_overall_create_limit,
       #s_mms_sales_promotion_inserts.exclude_my_health_check_flag,
       #s_mms_sales_promotion_inserts.exclude_from_attrition_reporting_flag,
       case when s_mms_sales_promotion.s_mms_sales_promotion_id is null then isnull(#s_mms_sales_promotion_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_sales_promotion_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_sales_promotion_inserts
  left join p_mms_sales_promotion
    on #s_mms_sales_promotion_inserts.bk_hash = p_mms_sales_promotion.bk_hash
   and p_mms_sales_promotion.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_sales_promotion
    on p_mms_sales_promotion.bk_hash = s_mms_sales_promotion.bk_hash
   and p_mms_sales_promotion.s_mms_sales_promotion_id = s_mms_sales_promotion.s_mms_sales_promotion_id
 where s_mms_sales_promotion.s_mms_sales_promotion_id is null
    or (s_mms_sales_promotion.s_mms_sales_promotion_id is not null
        and s_mms_sales_promotion.dv_hash <> #s_mms_sales_promotion_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_sales_promotion @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_sales_promotion @current_dv_batch_id

end
