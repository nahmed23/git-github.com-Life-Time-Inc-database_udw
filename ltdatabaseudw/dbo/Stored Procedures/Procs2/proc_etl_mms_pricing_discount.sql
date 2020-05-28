CREATE PROC [dbo].[proc_etl_mms_pricing_discount] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PricingDiscount

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PricingDiscount (
       bk_hash,
       PricingDiscountID,
       SalesPromotionID,
       ValDiscountTypeID,
       DiscountValue,
       ValDiscountApplicationTypeID,
       SalesCommissionPercent,
       ValDiscountCombineRuleID,
       AvailableForAllProductsFlag,
       AllProductsDiscountUseLimit,
       InsertedDateTime,
       UpdatedDateTime,
       ServiceCommissionPercent,
       EffectiveFromDateTime,
       EffectiveThruDateTime,
       Description,
       MustBuyAllFlag,
       BundleDiscountFlag,
       ProductAddedFromDate,
       ProductAddedToDate,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PricingDiscountID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PricingDiscountID,
       SalesPromotionID,
       ValDiscountTypeID,
       DiscountValue,
       ValDiscountApplicationTypeID,
       SalesCommissionPercent,
       ValDiscountCombineRuleID,
       AvailableForAllProductsFlag,
       AllProductsDiscountUseLimit,
       InsertedDateTime,
       UpdatedDateTime,
       ServiceCommissionPercent,
       EffectiveFromDateTime,
       EffectiveThruDateTime,
       Description,
       MustBuyAllFlag,
       BundleDiscountFlag,
       ProductAddedFromDate,
       ProductAddedToDate,
       isnull(cast(stage_mms_PricingDiscount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PricingDiscount
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_pricing_discount @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_pricing_discount (
       bk_hash,
       pricing_discount_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PricingDiscount.bk_hash,
       stage_hash_mms_PricingDiscount.PricingDiscountID pricing_discount_id,
       isnull(cast(stage_hash_mms_PricingDiscount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PricingDiscount
  left join h_mms_pricing_discount
    on stage_hash_mms_PricingDiscount.bk_hash = h_mms_pricing_discount.bk_hash
 where h_mms_pricing_discount_id is null
   and stage_hash_mms_PricingDiscount.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_pricing_discount
if object_id('tempdb..#l_mms_pricing_discount_inserts') is not null drop table #l_mms_pricing_discount_inserts
create table #l_mms_pricing_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PricingDiscount.bk_hash,
       stage_hash_mms_PricingDiscount.PricingDiscountID pricing_discount_id,
       stage_hash_mms_PricingDiscount.SalesPromotionID sales_promotion_id,
       stage_hash_mms_PricingDiscount.ValDiscountTypeID val_discount_type_id,
       stage_hash_mms_PricingDiscount.ValDiscountApplicationTypeID val_discount_application_type_id,
       stage_hash_mms_PricingDiscount.ValDiscountCombineRuleID val_discount_combine_rule_id,
       isnull(cast(stage_hash_mms_PricingDiscount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.PricingDiscountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.SalesPromotionID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.ValDiscountTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.ValDiscountApplicationTypeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.ValDiscountCombineRuleID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PricingDiscount
 where stage_hash_mms_PricingDiscount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_pricing_discount records
set @insert_date_time = getdate()
insert into l_mms_pricing_discount (
       bk_hash,
       pricing_discount_id,
       sales_promotion_id,
       val_discount_type_id,
       val_discount_application_type_id,
       val_discount_combine_rule_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_pricing_discount_inserts.bk_hash,
       #l_mms_pricing_discount_inserts.pricing_discount_id,
       #l_mms_pricing_discount_inserts.sales_promotion_id,
       #l_mms_pricing_discount_inserts.val_discount_type_id,
       #l_mms_pricing_discount_inserts.val_discount_application_type_id,
       #l_mms_pricing_discount_inserts.val_discount_combine_rule_id,
       case when l_mms_pricing_discount.l_mms_pricing_discount_id is null then isnull(#l_mms_pricing_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_pricing_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_pricing_discount_inserts
  left join p_mms_pricing_discount
    on #l_mms_pricing_discount_inserts.bk_hash = p_mms_pricing_discount.bk_hash
   and p_mms_pricing_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_pricing_discount
    on p_mms_pricing_discount.bk_hash = l_mms_pricing_discount.bk_hash
   and p_mms_pricing_discount.l_mms_pricing_discount_id = l_mms_pricing_discount.l_mms_pricing_discount_id
 where l_mms_pricing_discount.l_mms_pricing_discount_id is null
    or (l_mms_pricing_discount.l_mms_pricing_discount_id is not null
        and l_mms_pricing_discount.dv_hash <> #l_mms_pricing_discount_inserts.source_hash)

--calculate hash and lookup to current s_mms_pricing_discount
if object_id('tempdb..#s_mms_pricing_discount_inserts') is not null drop table #s_mms_pricing_discount_inserts
create table #s_mms_pricing_discount_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PricingDiscount.bk_hash,
       stage_hash_mms_PricingDiscount.PricingDiscountID pricing_discount_id,
       stage_hash_mms_PricingDiscount.DiscountValue discount_value,
       stage_hash_mms_PricingDiscount.SalesCommissionPercent sales_commission_percent,
       stage_hash_mms_PricingDiscount.AvailableForAllProductsFlag available_for_all_products_flag,
       stage_hash_mms_PricingDiscount.AllProductsDiscountUseLimit all_products_discount_use_limit,
       stage_hash_mms_PricingDiscount.InsertedDateTime inserted_date_time,
       stage_hash_mms_PricingDiscount.UpdatedDateTime updated_date_time,
       stage_hash_mms_PricingDiscount.ServiceCommissionPercent service_commission_percent,
       stage_hash_mms_PricingDiscount.EffectiveFromDateTime effective_from_date_time,
       stage_hash_mms_PricingDiscount.EffectiveThruDateTime effective_thru_date_time,
       stage_hash_mms_PricingDiscount.Description description,
       stage_hash_mms_PricingDiscount.MustBuyAllFlag must_buy_all_flag,
       stage_hash_mms_PricingDiscount.BundleDiscountFlag bundle_discount_flag,
       stage_hash_mms_PricingDiscount.ProductAddedFromDate product_added_from_date,
       stage_hash_mms_PricingDiscount.ProductAddedToDate product_added_to_date,
       isnull(cast(stage_hash_mms_PricingDiscount.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.PricingDiscountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.DiscountValue as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.SalesCommissionPercent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.AvailableForAllProductsFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.AllProductsDiscountUseLimit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PricingDiscount.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PricingDiscount.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.ServiceCommissionPercent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PricingDiscount.EffectiveFromDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PricingDiscount.EffectiveThruDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_PricingDiscount.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.MustBuyAllFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscount.BundleDiscountFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PricingDiscount.ProductAddedFromDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PricingDiscount.ProductAddedToDate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PricingDiscount
 where stage_hash_mms_PricingDiscount.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_pricing_discount records
set @insert_date_time = getdate()
insert into s_mms_pricing_discount (
       bk_hash,
       pricing_discount_id,
       discount_value,
       sales_commission_percent,
       available_for_all_products_flag,
       all_products_discount_use_limit,
       inserted_date_time,
       updated_date_time,
       service_commission_percent,
       effective_from_date_time,
       effective_thru_date_time,
       description,
       must_buy_all_flag,
       bundle_discount_flag,
       product_added_from_date,
       product_added_to_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_pricing_discount_inserts.bk_hash,
       #s_mms_pricing_discount_inserts.pricing_discount_id,
       #s_mms_pricing_discount_inserts.discount_value,
       #s_mms_pricing_discount_inserts.sales_commission_percent,
       #s_mms_pricing_discount_inserts.available_for_all_products_flag,
       #s_mms_pricing_discount_inserts.all_products_discount_use_limit,
       #s_mms_pricing_discount_inserts.inserted_date_time,
       #s_mms_pricing_discount_inserts.updated_date_time,
       #s_mms_pricing_discount_inserts.service_commission_percent,
       #s_mms_pricing_discount_inserts.effective_from_date_time,
       #s_mms_pricing_discount_inserts.effective_thru_date_time,
       #s_mms_pricing_discount_inserts.description,
       #s_mms_pricing_discount_inserts.must_buy_all_flag,
       #s_mms_pricing_discount_inserts.bundle_discount_flag,
       #s_mms_pricing_discount_inserts.product_added_from_date,
       #s_mms_pricing_discount_inserts.product_added_to_date,
       case when s_mms_pricing_discount.s_mms_pricing_discount_id is null then isnull(#s_mms_pricing_discount_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_pricing_discount_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_pricing_discount_inserts
  left join p_mms_pricing_discount
    on #s_mms_pricing_discount_inserts.bk_hash = p_mms_pricing_discount.bk_hash
   and p_mms_pricing_discount.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_pricing_discount
    on p_mms_pricing_discount.bk_hash = s_mms_pricing_discount.bk_hash
   and p_mms_pricing_discount.s_mms_pricing_discount_id = s_mms_pricing_discount.s_mms_pricing_discount_id
 where s_mms_pricing_discount.s_mms_pricing_discount_id is null
    or (s_mms_pricing_discount.s_mms_pricing_discount_id is not null
        and s_mms_pricing_discount.dv_hash <> #s_mms_pricing_discount_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_pricing_discount @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_pricing_discount @current_dv_batch_id

end
