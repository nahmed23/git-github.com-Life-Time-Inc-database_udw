CREATE PROC [dbo].[proc_etl_udwcloudsync_product_master] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_udwcloudsync_ProductMaster

set @insert_date_time = getdate()
insert into dbo.stage_hash_udwcloudsync_ProductMaster (
       bk_hash,
       AppCreatedBy,
       AppModifiedBy,
       AssessJuniorDuesFlag,
       Attachments,
       ConnectivityLeadGenerator,
       ConnectivityPrimaryLeadGeneratorFlag,
       ContentType,
       CorporateTransferFlag,
       CorporateTransferMultiplier,
       Created,
       CreatedBy,
       DeferredRevenueFlag,
       DepartmentalDSSRFlag,
       Discount1Description,
       Discount1EffectiveFromDate,
       Discount1EffectiveThroughDate,
       Discount1SalesCommissionPercent,
       Discount1ServiceCommissionPercent,
       Discount2Description,
       Discount2EffectiveFromDate,
       Discount2EffectiveThroughDate,
       Discount2SalesCommissionPercent,
       Discount2ServiceCommissionPercent,
       Discount3Description,
       Discount3EffectiveFromDate,
       Discount3EffectiveThroughDate,
       Discount3SalesCommissionPercent,
       Discount3ServiceCommissionPercent,
       Discount4Description,
       Discount4EffectiveFromDate,
       Discount4EffectiveThroughDate,
       Discount4SalesCommissionPercent,
       Discount4ServiceCommissionPercent,
       Discount5Description,
       Discount5EffectiveFromDate,
       Discount5EffectiveThroughDate,
       Discount5SalesCommissionPercent,
       Discount5ServiceCommissionPercent,
       Division,
       DSSRDowngradeOtherEnrollmentFeeFlag,
       DSSRIFAdminFeeFlag,
       ECommerceOfferFlag,
       Edit,
       ExperienceLifeMagazineFlag,
       FolderChildCount,
       [ID],
       ItemChildCount,
       MMSDepartment,
       MMSPackageProductFlag,
       MMSProductDisplayUIFlag,
       MMSProductGLOverrideClubID,
       MMSProductTipAllowedFlag,
       MMSRecurrentProductTypeDescription,
       Modified,
       ModifiedBy,
       MTDAverageDeliveredSessionPrice,
       MTDAverageSalePrice,
       NewBusinessOldBusiness,
       PackageProductCountasHalfSessionFlag,
       PackageProductSessionType,
       PayrollExtractDescription,
       PayrollExtractRegionType,
       PayrollmyLTBucksProductGroupDescription,
       PayrollmyLTBucksProductGroupFlag,
       PayrollmyLTBucksProductGroupSortOrder,
       PayrollmyLTBucksSalesAmountFlag,
       PayrollmyLTBucksServiceAmountFlag,
       PayrollmyLTBucksServiceQuantityFlag,
       PayrollProductGroupDescription,
       PayrollProductGroupSortOrder,
       PayrollSalesAmountFlag,
       PayrollServiceAmountFlag,
       PayrollServiceQuantityFlag,
       PayrollStandardProductGroupFlag,
       PayrollTrackSalesFlag,
       PayrollTrackServiceFlag,
       ProductDescription,
       ProductDiscountGLAccount,
       ProductGLAccount,
       ProductGLDepartmentCode,
       ProductGLProductCode,
       ProductID,
       ProductRefundGLAccount,
       ProductSKU,
       ProductStatus,
       ProductWorkdayAccount,
       ProductWorkdayCostCenter,
       ProductWorkdayDiscountGLAccount,
       ProductWorkdayOffering,
       ProductWorkdayOverRideRegion,
       ProductWorkdayRefundGLAccount,
       ReportingDept,
       ReportingDeptForNonCommissionedSales,
       RevenueAllocationRule,
       RevenueProductGroupDescription,
       RevenueProductGroupDiscountGLAccount,
       RevenueProductGroupGLAccount,
       RevenueProductGroupRefundGLAccount,
       RevenueProductGroupSortOrder,
       RevenueReportingRegionType,
       SalesCategoryDescription,
       SourceSystem_LinkTitle,
       SourceSystem_LinkTitleNoMenu,
       SourceSystem_Title,
       Subdivision,
       Type,
       Version,
       VirtualLocalRelativePath,
       WorkdayRevenueProductGroupAccount,
       WorkdayRevenueProductGroupDiscountGLAccount,
       WorkdayRevenueProductGroupRefundGLAccount,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ProductID,'z#@$k%&P')+'P%#&z$@k'+isnull(ProductSKU,'z#@$k%&P')+'P%#&z$@k'+isnull(SourceSystem_LinkTitle,'z#@$k%&P'))),2) bk_hash,
       AppCreatedBy,
       AppModifiedBy,
       AssessJuniorDuesFlag,
       Attachments,
       ConnectivityLeadGenerator,
       ConnectivityPrimaryLeadGeneratorFlag,
       ContentType,
       CorporateTransferFlag,
       CorporateTransferMultiplier,
       Created,
       CreatedBy,
       DeferredRevenueFlag,
       DepartmentalDSSRFlag,
       Discount1Description,
       Discount1EffectiveFromDate,
       Discount1EffectiveThroughDate,
       Discount1SalesCommissionPercent,
       Discount1ServiceCommissionPercent,
       Discount2Description,
       Discount2EffectiveFromDate,
       Discount2EffectiveThroughDate,
       Discount2SalesCommissionPercent,
       Discount2ServiceCommissionPercent,
       Discount3Description,
       Discount3EffectiveFromDate,
       Discount3EffectiveThroughDate,
       Discount3SalesCommissionPercent,
       Discount3ServiceCommissionPercent,
       Discount4Description,
       Discount4EffectiveFromDate,
       Discount4EffectiveThroughDate,
       Discount4SalesCommissionPercent,
       Discount4ServiceCommissionPercent,
       Discount5Description,
       Discount5EffectiveFromDate,
       Discount5EffectiveThroughDate,
       Discount5SalesCommissionPercent,
       Discount5ServiceCommissionPercent,
       Division,
       DSSRDowngradeOtherEnrollmentFeeFlag,
       DSSRIFAdminFeeFlag,
       ECommerceOfferFlag,
       Edit,
       ExperienceLifeMagazineFlag,
       FolderChildCount,
       [ID],
       ItemChildCount,
       MMSDepartment,
       MMSPackageProductFlag,
       MMSProductDisplayUIFlag,
       MMSProductGLOverrideClubID,
       MMSProductTipAllowedFlag,
       MMSRecurrentProductTypeDescription,
       Modified,
       ModifiedBy,
       MTDAverageDeliveredSessionPrice,
       MTDAverageSalePrice,
       NewBusinessOldBusiness,
       PackageProductCountasHalfSessionFlag,
       PackageProductSessionType,
       PayrollExtractDescription,
       PayrollExtractRegionType,
       PayrollmyLTBucksProductGroupDescription,
       PayrollmyLTBucksProductGroupFlag,
       PayrollmyLTBucksProductGroupSortOrder,
       PayrollmyLTBucksSalesAmountFlag,
       PayrollmyLTBucksServiceAmountFlag,
       PayrollmyLTBucksServiceQuantityFlag,
       PayrollProductGroupDescription,
       PayrollProductGroupSortOrder,
       PayrollSalesAmountFlag,
       PayrollServiceAmountFlag,
       PayrollServiceQuantityFlag,
       PayrollStandardProductGroupFlag,
       PayrollTrackSalesFlag,
       PayrollTrackServiceFlag,
       ProductDescription,
       ProductDiscountGLAccount,
       ProductGLAccount,
       ProductGLDepartmentCode,
       ProductGLProductCode,
       ProductID,
       ProductRefundGLAccount,
       ProductSKU,
       ProductStatus,
       ProductWorkdayAccount,
       ProductWorkdayCostCenter,
       ProductWorkdayDiscountGLAccount,
       ProductWorkdayOffering,
       ProductWorkdayOverRideRegion,
       ProductWorkdayRefundGLAccount,
       ReportingDept,
       ReportingDeptForNonCommissionedSales,
       RevenueAllocationRule,
       RevenueProductGroupDescription,
       RevenueProductGroupDiscountGLAccount,
       RevenueProductGroupGLAccount,
       RevenueProductGroupRefundGLAccount,
       RevenueProductGroupSortOrder,
       RevenueReportingRegionType,
       SalesCategoryDescription,
       SourceSystem_LinkTitle,
       SourceSystem_LinkTitleNoMenu,
       SourceSystem_Title,
       Subdivision,
       Type,
       Version,
       VirtualLocalRelativePath,
       WorkdayRevenueProductGroupAccount,
       WorkdayRevenueProductGroupDiscountGLAccount,
       WorkdayRevenueProductGroupRefundGLAccount,
       isnull(cast(stage_udwcloudsync_ProductMaster.Modified as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_udwcloudsync_ProductMaster
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_udwcloudsync_product_master @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_udwcloudsync_product_master (
       bk_hash,
       product_id,
       product_sku,
       source_system_link_title,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_udwcloudsync_ProductMaster.bk_hash,
       stage_hash_udwcloudsync_ProductMaster.ProductID product_id,
       stage_hash_udwcloudsync_ProductMaster.ProductSKU product_sku,
       stage_hash_udwcloudsync_ProductMaster.SourceSystem_LinkTitle source_system_link_title,
       isnull(cast(stage_hash_udwcloudsync_ProductMaster.Modified as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       7,
       @insert_date_time,
       @user
  from stage_hash_udwcloudsync_ProductMaster
  left join h_udwcloudsync_product_master
    on stage_hash_udwcloudsync_ProductMaster.bk_hash = h_udwcloudsync_product_master.bk_hash
 where h_udwcloudsync_product_master_id is null
   and stage_hash_udwcloudsync_ProductMaster.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_udwcloudsync_product_master
if object_id('tempdb..#l_udwcloudsync_product_master_inserts') is not null drop table #l_udwcloudsync_product_master_inserts
create table #l_udwcloudsync_product_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_udwcloudsync_ProductMaster.bk_hash,
       stage_hash_udwcloudsync_ProductMaster.ProductDiscountGLAccount product_discount_gl_account,
       stage_hash_udwcloudsync_ProductMaster.ProductGLAccount product_gl_account,
       stage_hash_udwcloudsync_ProductMaster.ProductGLDepartmentCode product_gl_department_code,
       stage_hash_udwcloudsync_ProductMaster.ProductGLProductCode product_gl_product_code,
       stage_hash_udwcloudsync_ProductMaster.ProductID product_id,
       stage_hash_udwcloudsync_ProductMaster.ProductRefundGLAccount product_refund_gl_account,
       stage_hash_udwcloudsync_ProductMaster.ProductSKU product_sku,
       stage_hash_udwcloudsync_ProductMaster.ProductWorkdayAccount product_workday_account,
       stage_hash_udwcloudsync_ProductMaster.ProductWorkdayCostCenter product_workday_cost_center,
       stage_hash_udwcloudsync_ProductMaster.ProductWorkdayDiscountGLAccount product_workday_discount_gl_account,
       stage_hash_udwcloudsync_ProductMaster.ProductWorkdayRefundGLAccount product_workday_refund_gl_account,
       stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupDiscountGLAccount revenue_product_group_discount_gl_account,
       stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupGLAccount revenue_product_group_gl_account,
       stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupRefundGLAccount revenue_product_group_refund_gl_account,
       stage_hash_udwcloudsync_ProductMaster.SourceSystem_LinkTitle source_system_link_title,
       stage_hash_udwcloudsync_ProductMaster.WorkdayRevenueProductGroupAccount workday_revenue_product_group_account,
       stage_hash_udwcloudsync_ProductMaster.WorkdayRevenueProductGroupDiscountGLAccount workday_revenue_product_group_discount_gl_account,
       stage_hash_udwcloudsync_ProductMaster.WorkdayRevenueProductGroupRefundGLAccount workday_revenue_product_group_refund_gl_account,
       isnull(cast(stage_hash_udwcloudsync_ProductMaster.Modified as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductDiscountGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductGLDepartmentCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductGLProductCode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductRefundGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductSKU,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductWorkdayAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductWorkdayCostCenter,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductWorkdayDiscountGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductWorkdayRefundGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupDiscountGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupRefundGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.SourceSystem_LinkTitle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.WorkdayRevenueProductGroupAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.WorkdayRevenueProductGroupDiscountGLAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.WorkdayRevenueProductGroupRefundGLAccount,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_udwcloudsync_ProductMaster
 where stage_hash_udwcloudsync_ProductMaster.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_udwcloudsync_product_master records
set @insert_date_time = getdate()
insert into l_udwcloudsync_product_master (
       bk_hash,
       product_discount_gl_account,
       product_gl_account,
       product_gl_department_code,
       product_gl_product_code,
       product_id,
       product_refund_gl_account,
       product_sku,
       product_workday_account,
       product_workday_cost_center,
       product_workday_discount_gl_account,
       product_workday_refund_gl_account,
       revenue_product_group_discount_gl_account,
       revenue_product_group_gl_account,
       revenue_product_group_refund_gl_account,
       source_system_link_title,
       workday_revenue_product_group_account,
       workday_revenue_product_group_discount_gl_account,
       workday_revenue_product_group_refund_gl_account,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_udwcloudsync_product_master_inserts.bk_hash,
       #l_udwcloudsync_product_master_inserts.product_discount_gl_account,
       #l_udwcloudsync_product_master_inserts.product_gl_account,
       #l_udwcloudsync_product_master_inserts.product_gl_department_code,
       #l_udwcloudsync_product_master_inserts.product_gl_product_code,
       #l_udwcloudsync_product_master_inserts.product_id,
       #l_udwcloudsync_product_master_inserts.product_refund_gl_account,
       #l_udwcloudsync_product_master_inserts.product_sku,
       #l_udwcloudsync_product_master_inserts.product_workday_account,
       #l_udwcloudsync_product_master_inserts.product_workday_cost_center,
       #l_udwcloudsync_product_master_inserts.product_workday_discount_gl_account,
       #l_udwcloudsync_product_master_inserts.product_workday_refund_gl_account,
       #l_udwcloudsync_product_master_inserts.revenue_product_group_discount_gl_account,
       #l_udwcloudsync_product_master_inserts.revenue_product_group_gl_account,
       #l_udwcloudsync_product_master_inserts.revenue_product_group_refund_gl_account,
       #l_udwcloudsync_product_master_inserts.source_system_link_title,
       #l_udwcloudsync_product_master_inserts.workday_revenue_product_group_account,
       #l_udwcloudsync_product_master_inserts.workday_revenue_product_group_discount_gl_account,
       #l_udwcloudsync_product_master_inserts.workday_revenue_product_group_refund_gl_account,
       case when l_udwcloudsync_product_master.l_udwcloudsync_product_master_id is null then isnull(#l_udwcloudsync_product_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       7,
       #l_udwcloudsync_product_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_udwcloudsync_product_master_inserts
  left join p_udwcloudsync_product_master
    on #l_udwcloudsync_product_master_inserts.bk_hash = p_udwcloudsync_product_master.bk_hash
   and p_udwcloudsync_product_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_udwcloudsync_product_master
    on p_udwcloudsync_product_master.bk_hash = l_udwcloudsync_product_master.bk_hash
   and p_udwcloudsync_product_master.l_udwcloudsync_product_master_id = l_udwcloudsync_product_master.l_udwcloudsync_product_master_id
 where l_udwcloudsync_product_master.l_udwcloudsync_product_master_id is null
    or (l_udwcloudsync_product_master.l_udwcloudsync_product_master_id is not null
        and l_udwcloudsync_product_master.dv_hash <> #l_udwcloudsync_product_master_inserts.source_hash)

--calculate hash and lookup to current s_udwcloudsync_product_master
if object_id('tempdb..#s_udwcloudsync_product_master_inserts') is not null drop table #s_udwcloudsync_product_master_inserts
create table #s_udwcloudsync_product_master_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_udwcloudsync_ProductMaster.bk_hash,
       stage_hash_udwcloudsync_ProductMaster.AppCreatedBy app_created_by,
       stage_hash_udwcloudsync_ProductMaster.AppModifiedBy app_modified_by,
       stage_hash_udwcloudsync_ProductMaster.AssessJuniorDuesFlag assess_junior_dues_flag,
       stage_hash_udwcloudsync_ProductMaster.Attachments attachments,
       stage_hash_udwcloudsync_ProductMaster.ConnectivityLeadGenerator connectivity_lead_generator,
       stage_hash_udwcloudsync_ProductMaster.ConnectivityPrimaryLeadGeneratorFlag connectivity_primary_lead_generator_flag,
       stage_hash_udwcloudsync_ProductMaster.ContentType content_type,
       stage_hash_udwcloudsync_ProductMaster.CorporateTransferFlag corporate_transfer_flag,
       stage_hash_udwcloudsync_ProductMaster.CorporateTransferMultiplier corporate_transfer_multiplier,
       stage_hash_udwcloudsync_ProductMaster.Created created,
       stage_hash_udwcloudsync_ProductMaster.CreatedBy created_by,
       stage_hash_udwcloudsync_ProductMaster.DeferredRevenueFlag deferred_revenue_flag,
       stage_hash_udwcloudsync_ProductMaster.DepartmentalDSSRFlag departmental_dssr_flag,
       stage_hash_udwcloudsync_ProductMaster.Discount1Description discount_1_description,
       stage_hash_udwcloudsync_ProductMaster.Discount1EffectiveFromDate discount_1_effective_from_date,
       stage_hash_udwcloudsync_ProductMaster.Discount1EffectiveThroughDate discount_1_effective_through_date,
       stage_hash_udwcloudsync_ProductMaster.Discount1SalesCommissionPercent discount_1_sales_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount1ServiceCommissionPercent discount_1_service_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount2Description discount_2_description,
       stage_hash_udwcloudsync_ProductMaster.Discount2EffectiveFromDate discount_2_effective_from_date,
       stage_hash_udwcloudsync_ProductMaster.Discount2EffectiveThroughDate discount_2_effective_through_date,
       stage_hash_udwcloudsync_ProductMaster.Discount2SalesCommissionPercent discount_2_sales_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount2ServiceCommissionPercent discount_2_service_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount3Description discount_3_description,
       stage_hash_udwcloudsync_ProductMaster.Discount3EffectiveFromDate discount_3_effective_from_date,
       stage_hash_udwcloudsync_ProductMaster.Discount3EffectiveThroughDate discount_3_effective_through_date,
       stage_hash_udwcloudsync_ProductMaster.Discount3SalesCommissionPercent discount_3_sales_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount3ServiceCommissionPercent discount_3_service_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount4Description discount_4_description,
       stage_hash_udwcloudsync_ProductMaster.Discount4EffectiveFromDate discount_4_effective_from_date,
       stage_hash_udwcloudsync_ProductMaster.Discount4EffectiveThroughDate discount_4_effective_through_date,
       stage_hash_udwcloudsync_ProductMaster.Discount4SalesCommissionPercent discount_4_sales_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount4ServiceCommissionPercent discount_4_service_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount5Description discount_5_description,
       stage_hash_udwcloudsync_ProductMaster.Discount5EffectiveFromDate discount_5_effective_from_date,
       stage_hash_udwcloudsync_ProductMaster.Discount5EffectiveThroughDate discount_5_effective_through_date,
       stage_hash_udwcloudsync_ProductMaster.Discount5SalesCommissionPercent discount_5_sales_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Discount5ServiceCommissionPercent discount_5_service_commission_percent,
       stage_hash_udwcloudsync_ProductMaster.Division division,
       stage_hash_udwcloudsync_ProductMaster.DSSRDowngradeOtherEnrollmentFeeFlag dssr_down_grade_other_enrollment_fee_flag,
       stage_hash_udwcloudsync_ProductMaster.DSSRIFAdminFeeFlag dssr_if_admin_fee_flag,
       stage_hash_udwcloudsync_ProductMaster.ECommerceOfferFlag ecommerce_offer_flag,
       stage_hash_udwcloudsync_ProductMaster.Edit edit,
       stage_hash_udwcloudsync_ProductMaster.ExperienceLifeMagazineFlag experience_life_magazine_flag,
       stage_hash_udwcloudsync_ProductMaster.FolderChildCount folder_child_count,
       stage_hash_udwcloudsync_ProductMaster.[ID] Product_Master_id,
       stage_hash_udwcloudsync_ProductMaster.ItemChildCount item_child_count,
       stage_hash_udwcloudsync_ProductMaster.MMSDepartment mms_department,
       stage_hash_udwcloudsync_ProductMaster.MMSPackageProductFlag mms_package_product_flag,
       stage_hash_udwcloudsync_ProductMaster.MMSProductDisplayUIFlag mms_productd_is_play_ui_flag,
       stage_hash_udwcloudsync_ProductMaster.MMSProductGLOverrideClubID mms_product_gl_override_club_id,
       stage_hash_udwcloudsync_ProductMaster.MMSProductTipAllowedFlag mms_product_tip_allowed_flag,
       stage_hash_udwcloudsync_ProductMaster.MMSRecurrentProductTypeDescription mms_recurrent_product_type_description,
       stage_hash_udwcloudsync_ProductMaster.Modified modified,
       stage_hash_udwcloudsync_ProductMaster.ModifiedBy modified_by,
       stage_hash_udwcloudsync_ProductMaster.MTDAverageDeliveredSessionPrice mtd_average_delivered_session_price,
       stage_hash_udwcloudsync_ProductMaster.MTDAverageSalePrice mtd_average_sale_price,
       stage_hash_udwcloudsync_ProductMaster.NewBusinessOldBusiness new_business_old_business,
       stage_hash_udwcloudsync_ProductMaster.PackageProductCountasHalfSessionFlag package_product_count_as_half_session_flag,
       stage_hash_udwcloudsync_ProductMaster.PackageProductSessionType package_product_session_type,
       stage_hash_udwcloudsync_ProductMaster.PayrollExtractDescription payroll_extract_description,
       stage_hash_udwcloudsync_ProductMaster.PayrollExtractRegionType payroll_extract_region_type,
       stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksProductGroupDescription payroll_my_ltbucks_product_group_description,
       stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksProductGroupFlag payroll_my_ltbucks_product_group_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksProductGroupSortOrder payroll_my_ltbucks_product_group_sort_order,
       stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksSalesAmountFlag payroll_my_ltbucks_sales_amount_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksServiceAmountFlag payroll_my_ltbucks_service_amount_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksServiceQuantityFlag payroll_my_ltbucks_service_quantity_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollProductGroupDescription payroll_product_group_description,
       stage_hash_udwcloudsync_ProductMaster.PayrollProductGroupSortOrder payroll_product_group_sort_order,
       stage_hash_udwcloudsync_ProductMaster.PayrollSalesAmountFlag payroll_sales_amount_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollServiceAmountFlag payroll_service_amount_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollServiceQuantityFlag payroll_service_quantity_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollStandardProductGroupFlag payroll_standard_product_group_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollTrackSalesFlag payroll_track_sales_flag,
       stage_hash_udwcloudsync_ProductMaster.PayrollTrackServiceFlag payroll_track_service_flag,
       stage_hash_udwcloudsync_ProductMaster.ProductDescription product_description,
       stage_hash_udwcloudsync_ProductMaster.ProductID product_id,
       stage_hash_udwcloudsync_ProductMaster.ProductSKU product_sku,
       stage_hash_udwcloudsync_ProductMaster.ProductStatus product_status,
       stage_hash_udwcloudsync_ProductMaster.ProductWorkdayOffering product_workday_offering,
       stage_hash_udwcloudsync_ProductMaster.ProductWorkdayOverRideRegion product_workday_override_region,
       stage_hash_udwcloudsync_ProductMaster.ReportingDept reporting_dept,
       stage_hash_udwcloudsync_ProductMaster.ReportingDeptForNonCommissionedSales reporting_dept_for_non_commissioned_sales,
       stage_hash_udwcloudsync_ProductMaster.RevenueAllocationRule revenue_allocation_rule,
       stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupDescription revenue_product_group_description,
       stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupSortOrder revenue_product_group_sort_order,
       stage_hash_udwcloudsync_ProductMaster.RevenueReportingRegionType revenue_reporting_region_type,
       stage_hash_udwcloudsync_ProductMaster.SalesCategoryDescription sales_category_description,
       stage_hash_udwcloudsync_ProductMaster.SourceSystem_LinkTitle source_system_link_title,
       stage_hash_udwcloudsync_ProductMaster.SourceSystem_LinkTitleNoMenu source_system_link_title_no_menu,
       stage_hash_udwcloudsync_ProductMaster.SourceSystem_Title source_system_title,
       stage_hash_udwcloudsync_ProductMaster.Subdivision sub_division,
       stage_hash_udwcloudsync_ProductMaster.Type type,
       stage_hash_udwcloudsync_ProductMaster.Version version,
       stage_hash_udwcloudsync_ProductMaster.VirtualLocalRelativePath virtual_local_relative_path,
       isnull(cast(stage_hash_udwcloudsync_ProductMaster.Modified as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.AppCreatedBy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.AppModifiedBy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.AssessJuniorDuesFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Attachments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ConnectivityLeadGenerator,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ConnectivityPrimaryLeadGeneratorFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ContentType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.CorporateTransferFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.CorporateTransferMultiplier,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ProductMaster.Created,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.CreatedBy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.DeferredRevenueFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.DepartmentalDSSRFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount1Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount1EffectiveFromDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount1EffectiveThroughDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount1SalesCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount1ServiceCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount2Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount2EffectiveFromDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount2EffectiveThroughDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount2SalesCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount2ServiceCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount3Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount3EffectiveFromDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount3EffectiveThroughDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount3SalesCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount3ServiceCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount4Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount4EffectiveFromDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount4EffectiveThroughDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount4SalesCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount4ServiceCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount5Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount5EffectiveFromDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount5EffectiveThroughDate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount5SalesCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Discount5ServiceCommissionPercent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Division,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.DSSRDowngradeOtherEnrollmentFeeFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.DSSRIFAdminFeeFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ECommerceOfferFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Edit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ExperienceLifeMagazineFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.FolderChildCount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_udwcloudsync_ProductMaster.[ID] as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ItemChildCount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.MMSDepartment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.MMSPackageProductFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.MMSProductDisplayUIFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.MMSProductGLOverrideClubID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.MMSProductTipAllowedFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.MMSRecurrentProductTypeDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_udwcloudsync_ProductMaster.Modified,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ModifiedBy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.MTDAverageDeliveredSessionPrice,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.MTDAverageSalePrice,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.NewBusinessOldBusiness,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PackageProductCountasHalfSessionFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PackageProductSessionType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollExtractDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollExtractRegionType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksProductGroupDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksProductGroupFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksProductGroupSortOrder,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksSalesAmountFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksServiceAmountFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollmyLTBucksServiceQuantityFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollProductGroupDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollProductGroupSortOrder,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollSalesAmountFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollServiceAmountFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollServiceQuantityFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollStandardProductGroupFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollTrackSalesFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.PayrollTrackServiceFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductSKU,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductStatus,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductWorkdayOffering,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ProductWorkdayOverRideRegion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ReportingDept,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.ReportingDeptForNonCommissionedSales,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.RevenueAllocationRule,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.RevenueProductGroupSortOrder,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.RevenueReportingRegionType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.SalesCategoryDescription,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.SourceSystem_LinkTitle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.SourceSystem_LinkTitleNoMenu,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.SourceSystem_Title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Subdivision,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.Version,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_udwcloudsync_ProductMaster.VirtualLocalRelativePath,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_udwcloudsync_ProductMaster
 where stage_hash_udwcloudsync_ProductMaster.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_udwcloudsync_product_master records
set @insert_date_time = getdate()
insert into s_udwcloudsync_product_master (
       bk_hash,
       app_created_by,
       app_modified_by,
       assess_junior_dues_flag,
       attachments,
       connectivity_lead_generator,
       connectivity_primary_lead_generator_flag,
       content_type,
       corporate_transfer_flag,
       corporate_transfer_multiplier,
       created,
       created_by,
       deferred_revenue_flag,
       departmental_dssr_flag,
       discount_1_description,
       discount_1_effective_from_date,
       discount_1_effective_through_date,
       discount_1_sales_commission_percent,
       discount_1_service_commission_percent,
       discount_2_description,
       discount_2_effective_from_date,
       discount_2_effective_through_date,
       discount_2_sales_commission_percent,
       discount_2_service_commission_percent,
       discount_3_description,
       discount_3_effective_from_date,
       discount_3_effective_through_date,
       discount_3_sales_commission_percent,
       discount_3_service_commission_percent,
       discount_4_description,
       discount_4_effective_from_date,
       discount_4_effective_through_date,
       discount_4_sales_commission_percent,
       discount_4_service_commission_percent,
       discount_5_description,
       discount_5_effective_from_date,
       discount_5_effective_through_date,
       discount_5_sales_commission_percent,
       discount_5_service_commission_percent,
       division,
       dssr_down_grade_other_enrollment_fee_flag,
       dssr_if_admin_fee_flag,
       ecommerce_offer_flag,
       edit,
       experience_life_magazine_flag,
       folder_child_count,
       Product_Master_id,
       item_child_count,
       mms_department,
       mms_package_product_flag,
       mms_productd_is_play_ui_flag,
       mms_product_gl_override_club_id,
       mms_product_tip_allowed_flag,
       mms_recurrent_product_type_description,
       modified,
       modified_by,
       mtd_average_delivered_session_price,
       mtd_average_sale_price,
       new_business_old_business,
       package_product_count_as_half_session_flag,
       package_product_session_type,
       payroll_extract_description,
       payroll_extract_region_type,
       payroll_my_ltbucks_product_group_description,
       payroll_my_ltbucks_product_group_flag,
       payroll_my_ltbucks_product_group_sort_order,
       payroll_my_ltbucks_sales_amount_flag,
       payroll_my_ltbucks_service_amount_flag,
       payroll_my_ltbucks_service_quantity_flag,
       payroll_product_group_description,
       payroll_product_group_sort_order,
       payroll_sales_amount_flag,
       payroll_service_amount_flag,
       payroll_service_quantity_flag,
       payroll_standard_product_group_flag,
       payroll_track_sales_flag,
       payroll_track_service_flag,
       product_description,
       product_id,
       product_sku,
       product_status,
       product_workday_offering,
       product_workday_override_region,
       reporting_dept,
       reporting_dept_for_non_commissioned_sales,
       revenue_allocation_rule,
       revenue_product_group_description,
       revenue_product_group_sort_order,
       revenue_reporting_region_type,
       sales_category_description,
       source_system_link_title,
       source_system_link_title_no_menu,
       source_system_title,
       sub_division,
       type,
       version,
       virtual_local_relative_path,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_udwcloudsync_product_master_inserts.bk_hash,
       #s_udwcloudsync_product_master_inserts.app_created_by,
       #s_udwcloudsync_product_master_inserts.app_modified_by,
       #s_udwcloudsync_product_master_inserts.assess_junior_dues_flag,
       #s_udwcloudsync_product_master_inserts.attachments,
       #s_udwcloudsync_product_master_inserts.connectivity_lead_generator,
       #s_udwcloudsync_product_master_inserts.connectivity_primary_lead_generator_flag,
       #s_udwcloudsync_product_master_inserts.content_type,
       #s_udwcloudsync_product_master_inserts.corporate_transfer_flag,
       #s_udwcloudsync_product_master_inserts.corporate_transfer_multiplier,
       #s_udwcloudsync_product_master_inserts.created,
       #s_udwcloudsync_product_master_inserts.created_by,
       #s_udwcloudsync_product_master_inserts.deferred_revenue_flag,
       #s_udwcloudsync_product_master_inserts.departmental_dssr_flag,
       #s_udwcloudsync_product_master_inserts.discount_1_description,
       #s_udwcloudsync_product_master_inserts.discount_1_effective_from_date,
       #s_udwcloudsync_product_master_inserts.discount_1_effective_through_date,
       #s_udwcloudsync_product_master_inserts.discount_1_sales_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_1_service_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_2_description,
       #s_udwcloudsync_product_master_inserts.discount_2_effective_from_date,
       #s_udwcloudsync_product_master_inserts.discount_2_effective_through_date,
       #s_udwcloudsync_product_master_inserts.discount_2_sales_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_2_service_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_3_description,
       #s_udwcloudsync_product_master_inserts.discount_3_effective_from_date,
       #s_udwcloudsync_product_master_inserts.discount_3_effective_through_date,
       #s_udwcloudsync_product_master_inserts.discount_3_sales_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_3_service_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_4_description,
       #s_udwcloudsync_product_master_inserts.discount_4_effective_from_date,
       #s_udwcloudsync_product_master_inserts.discount_4_effective_through_date,
       #s_udwcloudsync_product_master_inserts.discount_4_sales_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_4_service_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_5_description,
       #s_udwcloudsync_product_master_inserts.discount_5_effective_from_date,
       #s_udwcloudsync_product_master_inserts.discount_5_effective_through_date,
       #s_udwcloudsync_product_master_inserts.discount_5_sales_commission_percent,
       #s_udwcloudsync_product_master_inserts.discount_5_service_commission_percent,
       #s_udwcloudsync_product_master_inserts.division,
       #s_udwcloudsync_product_master_inserts.dssr_down_grade_other_enrollment_fee_flag,
       #s_udwcloudsync_product_master_inserts.dssr_if_admin_fee_flag,
       #s_udwcloudsync_product_master_inserts.ecommerce_offer_flag,
       #s_udwcloudsync_product_master_inserts.edit,
       #s_udwcloudsync_product_master_inserts.experience_life_magazine_flag,
       #s_udwcloudsync_product_master_inserts.folder_child_count,
       #s_udwcloudsync_product_master_inserts.Product_Master_id,
       #s_udwcloudsync_product_master_inserts.item_child_count,
       #s_udwcloudsync_product_master_inserts.mms_department,
       #s_udwcloudsync_product_master_inserts.mms_package_product_flag,
       #s_udwcloudsync_product_master_inserts.mms_productd_is_play_ui_flag,
       #s_udwcloudsync_product_master_inserts.mms_product_gl_override_club_id,
       #s_udwcloudsync_product_master_inserts.mms_product_tip_allowed_flag,
       #s_udwcloudsync_product_master_inserts.mms_recurrent_product_type_description,
       #s_udwcloudsync_product_master_inserts.modified,
       #s_udwcloudsync_product_master_inserts.modified_by,
       #s_udwcloudsync_product_master_inserts.mtd_average_delivered_session_price,
       #s_udwcloudsync_product_master_inserts.mtd_average_sale_price,
       #s_udwcloudsync_product_master_inserts.new_business_old_business,
       #s_udwcloudsync_product_master_inserts.package_product_count_as_half_session_flag,
       #s_udwcloudsync_product_master_inserts.package_product_session_type,
       #s_udwcloudsync_product_master_inserts.payroll_extract_description,
       #s_udwcloudsync_product_master_inserts.payroll_extract_region_type,
       #s_udwcloudsync_product_master_inserts.payroll_my_ltbucks_product_group_description,
       #s_udwcloudsync_product_master_inserts.payroll_my_ltbucks_product_group_flag,
       #s_udwcloudsync_product_master_inserts.payroll_my_ltbucks_product_group_sort_order,
       #s_udwcloudsync_product_master_inserts.payroll_my_ltbucks_sales_amount_flag,
       #s_udwcloudsync_product_master_inserts.payroll_my_ltbucks_service_amount_flag,
       #s_udwcloudsync_product_master_inserts.payroll_my_ltbucks_service_quantity_flag,
       #s_udwcloudsync_product_master_inserts.payroll_product_group_description,
       #s_udwcloudsync_product_master_inserts.payroll_product_group_sort_order,
       #s_udwcloudsync_product_master_inserts.payroll_sales_amount_flag,
       #s_udwcloudsync_product_master_inserts.payroll_service_amount_flag,
       #s_udwcloudsync_product_master_inserts.payroll_service_quantity_flag,
       #s_udwcloudsync_product_master_inserts.payroll_standard_product_group_flag,
       #s_udwcloudsync_product_master_inserts.payroll_track_sales_flag,
       #s_udwcloudsync_product_master_inserts.payroll_track_service_flag,
       #s_udwcloudsync_product_master_inserts.product_description,
       #s_udwcloudsync_product_master_inserts.product_id,
       #s_udwcloudsync_product_master_inserts.product_sku,
       #s_udwcloudsync_product_master_inserts.product_status,
       #s_udwcloudsync_product_master_inserts.product_workday_offering,
       #s_udwcloudsync_product_master_inserts.product_workday_override_region,
       #s_udwcloudsync_product_master_inserts.reporting_dept,
       #s_udwcloudsync_product_master_inserts.reporting_dept_for_non_commissioned_sales,
       #s_udwcloudsync_product_master_inserts.revenue_allocation_rule,
       #s_udwcloudsync_product_master_inserts.revenue_product_group_description,
       #s_udwcloudsync_product_master_inserts.revenue_product_group_sort_order,
       #s_udwcloudsync_product_master_inserts.revenue_reporting_region_type,
       #s_udwcloudsync_product_master_inserts.sales_category_description,
       #s_udwcloudsync_product_master_inserts.source_system_link_title,
       #s_udwcloudsync_product_master_inserts.source_system_link_title_no_menu,
       #s_udwcloudsync_product_master_inserts.source_system_title,
       #s_udwcloudsync_product_master_inserts.sub_division,
       #s_udwcloudsync_product_master_inserts.type,
       #s_udwcloudsync_product_master_inserts.version,
       #s_udwcloudsync_product_master_inserts.virtual_local_relative_path,
       case when s_udwcloudsync_product_master.s_udwcloudsync_product_master_id is null then isnull(#s_udwcloudsync_product_master_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       7,
       #s_udwcloudsync_product_master_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_udwcloudsync_product_master_inserts
  left join p_udwcloudsync_product_master
    on #s_udwcloudsync_product_master_inserts.bk_hash = p_udwcloudsync_product_master.bk_hash
   and p_udwcloudsync_product_master.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_udwcloudsync_product_master
    on p_udwcloudsync_product_master.bk_hash = s_udwcloudsync_product_master.bk_hash
   and p_udwcloudsync_product_master.s_udwcloudsync_product_master_id = s_udwcloudsync_product_master.s_udwcloudsync_product_master_id
 where s_udwcloudsync_product_master.s_udwcloudsync_product_master_id is null
    or (s_udwcloudsync_product_master.s_udwcloudsync_product_master_id is not null
        and s_udwcloudsync_product_master.dv_hash <> #s_udwcloudsync_product_master_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_udwcloudsync_product_master @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_udwcloudsync_product_master @current_dv_batch_id
exec dbo.proc_d_udwcloudsync_product_master_history @current_dv_batch_id

end
