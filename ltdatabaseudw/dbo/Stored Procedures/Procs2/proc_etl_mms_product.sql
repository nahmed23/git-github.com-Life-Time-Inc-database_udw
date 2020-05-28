CREATE PROC [dbo].[proc_etl_mms_product] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_Product

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Product (
       bk_hash,
       ProductID,
       DepartmentID,
       Name,
       Description,
       DisplayUIFlag,
       SortOrder,
       InsertedDateTime,
       TurnOffDateTime,
       StartDate,
       EndDate,
       GLAccountNumber,
       GLSubAccountNumber,
       GLOverRideClubID,
       ValGLGroupID,
       ValRecurrentProductTypeID,
       CompletePackageFlag,
       AllowZeroDollarFlag,
       PackageProductFlag,
       SoldNotServicedFlag,
       UpdatedDateTime,
       ValProductStatusID,
       TipAllowedFlag,
       JrMemberDuesFlag,
       ValAssessmentDayID,
       EligibleForHoldFlag,
       ConfirmMemberDataFlag,
       MedicalProductFlag,
       BundleProductFlag,
       WorkdayAccount,
       WorkdayCostCenter,
       WorkdayOffering,
       WorkdayOverRideRegion,
       WorkdayRevenueProductGroupAccount,
       DeferredRevenueFlag,
       PriceLockedFlag,
       RevenueCategory,
       SpendCategory,
       PayComponent,
       AssessAsDuesFlag,
       ValEmployeeLevelTypeID,
       SKU,
       LTBuckEligible,
       LTBuckCostPercent,
       ExcludeFromClubPOSFlag,
       AccessByPricePaidFlag,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ProductID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ProductID,
       DepartmentID,
       Name,
       Description,
       DisplayUIFlag,
       SortOrder,
       InsertedDateTime,
       TurnOffDateTime,
       StartDate,
       EndDate,
       GLAccountNumber,
       GLSubAccountNumber,
       GLOverRideClubID,
       ValGLGroupID,
       ValRecurrentProductTypeID,
       CompletePackageFlag,
       AllowZeroDollarFlag,
       PackageProductFlag,
       SoldNotServicedFlag,
       UpdatedDateTime,
       ValProductStatusID,
       TipAllowedFlag,
       JrMemberDuesFlag,
       ValAssessmentDayID,
       EligibleForHoldFlag,
       ConfirmMemberDataFlag,
       MedicalProductFlag,
       BundleProductFlag,
       WorkdayAccount,
       WorkdayCostCenter,
       WorkdayOffering,
       WorkdayOverRideRegion,
       WorkdayRevenueProductGroupAccount,
       DeferredRevenueFlag,
       PriceLockedFlag,
       RevenueCategory,
       SpendCategory,
       PayComponent,
       AssessAsDuesFlag,
       ValEmployeeLevelTypeID,
       SKU,
       LTBuckEligible,
       LTBuckCostPercent,
       ExcludeFromClubPOSFlag,
       AccessByPricePaidFlag,
       isnull(cast(stage_mms_Product.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mms_Product
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_product @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_product (
       bk_hash,
       product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mms_Product.bk_hash,
       stage_hash_mms_Product.ProductID product_id,
       isnull(cast(stage_hash_mms_Product.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Product
  left join h_mms_product
    on stage_hash_mms_Product.bk_hash = h_mms_product.bk_hash
 where h_mms_product_id is null
   and stage_hash_mms_Product.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_product
if object_id('tempdb..#l_mms_product_inserts') is not null drop table #l_mms_product_inserts
create table #l_mms_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Product.bk_hash,
       stage_hash_mms_Product.ProductID product_id,
       stage_hash_mms_Product.DepartmentID department_id,
       stage_hash_mms_Product.GLOverRideClubID gl_over_ride_club_id,
       stage_hash_mms_Product.ValGLGroupID val_gl_group_id,
       stage_hash_mms_Product.ValRecurrentProductTypeID val_recurrent_product_type_id,
       stage_hash_mms_Product.ValProductStatusID val_product_status_id,
       stage_hash_mms_Product.ValAssessmentDayID val_assessment_day_id,
       stage_hash_mms_Product.WorkdayAccount workday_account,
       stage_hash_mms_Product.WorkdayCostCenter workday_cost_center,
       stage_hash_mms_Product.WorkdayOffering workday_offering,
       stage_hash_mms_Product.WorkdayOverRideRegion workday_over_ride_region,
       stage_hash_mms_Product.WorkdayRevenueProductGroupAccount workday_revenue_product_group_account,
       stage_hash_mms_Product.RevenueCategory revenue_category,
       stage_hash_mms_Product.SpendCategory spend_category,
       stage_hash_mms_Product.PayComponent pay_component,
       stage_hash_mms_Product.ValEmployeeLevelTypeID val_employee_level_type_id,
       isnull(cast(stage_hash_mms_Product.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.DepartmentID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.GLOverRideClubID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ValGLGroupID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ValRecurrentProductTypeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ValProductStatusID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ValAssessmentDayID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.WorkdayAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.WorkdayCostCenter,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.WorkdayOffering,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.WorkdayOverRideRegion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.WorkdayRevenueProductGroupAccount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.RevenueCategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.SpendCategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.PayComponent,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ValEmployeeLevelTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Product
 where stage_hash_mms_Product.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_product records
set @insert_date_time = getdate()
insert into l_mms_product (
       bk_hash,
       product_id,
       department_id,
       gl_over_ride_club_id,
       val_gl_group_id,
       val_recurrent_product_type_id,
       val_product_status_id,
       val_assessment_day_id,
       workday_account,
       workday_cost_center,
       workday_offering,
       workday_over_ride_region,
       workday_revenue_product_group_account,
       revenue_category,
       spend_category,
       pay_component,
       val_employee_level_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_product_inserts.bk_hash,
       #l_mms_product_inserts.product_id,
       #l_mms_product_inserts.department_id,
       #l_mms_product_inserts.gl_over_ride_club_id,
       #l_mms_product_inserts.val_gl_group_id,
       #l_mms_product_inserts.val_recurrent_product_type_id,
       #l_mms_product_inserts.val_product_status_id,
       #l_mms_product_inserts.val_assessment_day_id,
       #l_mms_product_inserts.workday_account,
       #l_mms_product_inserts.workday_cost_center,
       #l_mms_product_inserts.workday_offering,
       #l_mms_product_inserts.workday_over_ride_region,
       #l_mms_product_inserts.workday_revenue_product_group_account,
       #l_mms_product_inserts.revenue_category,
       #l_mms_product_inserts.spend_category,
       #l_mms_product_inserts.pay_component,
       #l_mms_product_inserts.val_employee_level_type_id,
       case when l_mms_product.l_mms_product_id is null then isnull(#l_mms_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_product_inserts
  left join p_mms_product
    on #l_mms_product_inserts.bk_hash = p_mms_product.bk_hash
   and p_mms_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_product
    on p_mms_product.bk_hash = l_mms_product.bk_hash
   and p_mms_product.l_mms_product_id = l_mms_product.l_mms_product_id
 where l_mms_product.l_mms_product_id is null
    or (l_mms_product.l_mms_product_id is not null
        and l_mms_product.dv_hash <> #l_mms_product_inserts.source_hash)

--calculate hash and lookup to current s_mms_product
if object_id('tempdb..#s_mms_product_inserts') is not null drop table #s_mms_product_inserts
create table #s_mms_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Product.bk_hash,
       stage_hash_mms_Product.ProductID product_id,
       stage_hash_mms_Product.Name name,
       stage_hash_mms_Product.Description description,
       stage_hash_mms_Product.DisplayUIFlag display_ui_flag,
       stage_hash_mms_Product.SortOrder sort_order,
       stage_hash_mms_Product.InsertedDateTime inserted_date_time,
       stage_hash_mms_Product.TurnOffDateTime turn_off_date_time,
       stage_hash_mms_Product.StartDate start_date,
       stage_hash_mms_Product.EndDate end_date,
       stage_hash_mms_Product.GLAccountNumber gl_account_number,
       stage_hash_mms_Product.GLSubAccountNumber gl_sub_account_number,
       stage_hash_mms_Product.CompletePackageFlag complete_package_flag,
       stage_hash_mms_Product.AllowZeroDollarFlag allow_zero_dollar_flag,
       stage_hash_mms_Product.PackageProductFlag package_product_flag,
       stage_hash_mms_Product.SoldNotServicedFlag sold_not_serviced_flag,
       stage_hash_mms_Product.UpdatedDateTime updated_date_time,
       stage_hash_mms_Product.TipAllowedFlag tip_allowed_flag,
       stage_hash_mms_Product.JrMemberDuesFlag jr_member_dues_flag,
       stage_hash_mms_Product.EligibleForHoldFlag eligible_for_hold_flag,
       stage_hash_mms_Product.ConfirmMemberDataFlag confirm_member_data_flag,
       stage_hash_mms_Product.MedicalProductFlag medical_product_flag,
       stage_hash_mms_Product.BundleProductFlag bundle_product_flag,
       stage_hash_mms_Product.DeferredRevenueFlag deferred_revenue_flag,
       stage_hash_mms_Product.PriceLockedFlag price_locked_flag,
       stage_hash_mms_Product.AssessAsDuesFlag assess_as_dues_flag,
       stage_hash_mms_Product.SKU sku,
       isnull(cast(stage_hash_mms_Product.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.Name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.DisplayUIFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.SortOrder as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Product.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Product.TurnOffDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Product.StartDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Product.EndDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.GLAccountNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.GLSubAccountNumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.CompletePackageFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.AllowZeroDollarFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.PackageProductFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.SoldNotServicedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Product.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.TipAllowedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.JrMemberDuesFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.EligibleForHoldFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ConfirmMemberDataFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.MedicalProductFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.BundleProductFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.DeferredRevenueFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.PriceLockedFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.AssessAsDuesFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mms_Product.SKU,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Product
 where stage_hash_mms_Product.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_product records
set @insert_date_time = getdate()
insert into s_mms_product (
       bk_hash,
       product_id,
       name,
       description,
       display_ui_flag,
       sort_order,
       inserted_date_time,
       turn_off_date_time,
       start_date,
       end_date,
       gl_account_number,
       gl_sub_account_number,
       complete_package_flag,
       allow_zero_dollar_flag,
       package_product_flag,
       sold_not_serviced_flag,
       updated_date_time,
       tip_allowed_flag,
       jr_member_dues_flag,
       eligible_for_hold_flag,
       confirm_member_data_flag,
       medical_product_flag,
       bundle_product_flag,
       deferred_revenue_flag,
       price_locked_flag,
       assess_as_dues_flag,
       sku,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_product_inserts.bk_hash,
       #s_mms_product_inserts.product_id,
       #s_mms_product_inserts.name,
       #s_mms_product_inserts.description,
       #s_mms_product_inserts.display_ui_flag,
       #s_mms_product_inserts.sort_order,
       #s_mms_product_inserts.inserted_date_time,
       #s_mms_product_inserts.turn_off_date_time,
       #s_mms_product_inserts.start_date,
       #s_mms_product_inserts.end_date,
       #s_mms_product_inserts.gl_account_number,
       #s_mms_product_inserts.gl_sub_account_number,
       #s_mms_product_inserts.complete_package_flag,
       #s_mms_product_inserts.allow_zero_dollar_flag,
       #s_mms_product_inserts.package_product_flag,
       #s_mms_product_inserts.sold_not_serviced_flag,
       #s_mms_product_inserts.updated_date_time,
       #s_mms_product_inserts.tip_allowed_flag,
       #s_mms_product_inserts.jr_member_dues_flag,
       #s_mms_product_inserts.eligible_for_hold_flag,
       #s_mms_product_inserts.confirm_member_data_flag,
       #s_mms_product_inserts.medical_product_flag,
       #s_mms_product_inserts.bundle_product_flag,
       #s_mms_product_inserts.deferred_revenue_flag,
       #s_mms_product_inserts.price_locked_flag,
       #s_mms_product_inserts.assess_as_dues_flag,
       #s_mms_product_inserts.sku,
       case when s_mms_product.s_mms_product_id is null then isnull(#s_mms_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_product_inserts
  left join p_mms_product
    on #s_mms_product_inserts.bk_hash = p_mms_product.bk_hash
   and p_mms_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_product
    on p_mms_product.bk_hash = s_mms_product.bk_hash
   and p_mms_product.s_mms_product_id = s_mms_product.s_mms_product_id
 where s_mms_product.s_mms_product_id is null
    or (s_mms_product.s_mms_product_id is not null
        and s_mms_product.dv_hash <> #s_mms_product_inserts.source_hash)

--calculate hash and lookup to current s_mms_product_1
if object_id('tempdb..#s_mms_product_1_inserts') is not null drop table #s_mms_product_1_inserts
create table #s_mms_product_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Product.bk_hash,
       stage_hash_mms_Product.ProductID product_id,
       stage_hash_mms_Product.LTBuckEligible lt_buck_eligible,
       stage_hash_mms_Product.LTBuckCostPercent lt_buck_cost_percent,
       isnull(cast(stage_hash_mms_Product.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.LTBuckEligible as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.LTBuckCostPercent as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Product
 where stage_hash_mms_Product.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_product_1 records
set @insert_date_time = getdate()
insert into s_mms_product_1 (
       bk_hash,
       product_id,
       lt_buck_eligible,
       lt_buck_cost_percent,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_product_1_inserts.bk_hash,
       #s_mms_product_1_inserts.product_id,
       #s_mms_product_1_inserts.lt_buck_eligible,
       #s_mms_product_1_inserts.lt_buck_cost_percent,
       case when s_mms_product_1.s_mms_product_1_id is null then isnull(#s_mms_product_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_product_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_product_1_inserts
  left join p_mms_product
    on #s_mms_product_1_inserts.bk_hash = p_mms_product.bk_hash
   and p_mms_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_product_1
    on p_mms_product.bk_hash = s_mms_product_1.bk_hash
   and p_mms_product.s_mms_product_1_id = s_mms_product_1.s_mms_product_1_id
 where s_mms_product_1.s_mms_product_1_id is null
    or (s_mms_product_1.s_mms_product_1_id is not null
        and s_mms_product_1.dv_hash <> #s_mms_product_1_inserts.source_hash)

--calculate hash and lookup to current s_mms_product_2
if object_id('tempdb..#s_mms_product_2_inserts') is not null drop table #s_mms_product_2_inserts
create table #s_mms_product_2_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Product.bk_hash,
       stage_hash_mms_Product.ProductID product_id,
       stage_hash_mms_Product.ExcludeFromClubPOSFlag exclude_from_club_POS_flag,
       stage_hash_mms_Product.AccessByPricePaidFlag access_by_price_paid_flag,
       isnull(cast(stage_hash_mms_Product.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ProductID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.ExcludeFromClubPOSFlag as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_Product.AccessByPricePaidFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Product
 where stage_hash_mms_Product.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_product_2 records
set @insert_date_time = getdate()
insert into s_mms_product_2 (
       bk_hash,
       product_id,
       exclude_from_club_POS_flag,
       access_by_price_paid_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_product_2_inserts.bk_hash,
       #s_mms_product_2_inserts.product_id,
       #s_mms_product_2_inserts.exclude_from_club_POS_flag,
       #s_mms_product_2_inserts.access_by_price_paid_flag,
       case when s_mms_product_2.s_mms_product_2_id is null then isnull(#s_mms_product_2_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_product_2_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_product_2_inserts
  left join p_mms_product
    on #s_mms_product_2_inserts.bk_hash = p_mms_product.bk_hash
   and p_mms_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_product_2
    on p_mms_product.bk_hash = s_mms_product_2.bk_hash
   and p_mms_product.s_mms_product_2_id = s_mms_product_2.s_mms_product_2_id
 where s_mms_product_2.s_mms_product_2_id is null
    or (s_mms_product_2.s_mms_product_2_id is not null
        and s_mms_product_2.dv_hash <> #s_mms_product_2_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_product @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_product @current_dv_batch_id
exec dbo.proc_d_mms_product_history @current_dv_batch_id

end
