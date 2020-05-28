CREATE PROC [dbo].[proc_etl_mms_pricing_discount_include_product] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_PricingDiscountIncludeProduct

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_PricingDiscountIncludeProduct (
       bk_hash,
       PricingDiscountIncludeProductID,
       PricingDiscountID,
       ProductID,
       TriggerQuantity,
       DiscountedProductID,
       DiscountUseLimit,
       InsertedDateTime,
       UpdatedDateTime,
       OverrideDiscountTypeID,
       OverrideDiscountValue,
       OverrideSalesCommissionPercent,
       OverrideServiceCommissionPercent,
       BundleProductFlag,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PricingDiscountIncludeProductID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PricingDiscountIncludeProductID,
       PricingDiscountID,
       ProductID,
       TriggerQuantity,
       DiscountedProductID,
       DiscountUseLimit,
       InsertedDateTime,
       UpdatedDateTime,
       OverrideDiscountTypeID,
       OverrideDiscountValue,
       OverrideSalesCommissionPercent,
       OverrideServiceCommissionPercent,
       BundleProductFlag,
       isnull(cast(stage_mms_PricingDiscountIncludeProduct.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_PricingDiscountIncludeProduct
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_pricing_discount_include_product @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_pricing_discount_include_product (
       bk_hash,
       pricing_discount_include_product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_PricingDiscountIncludeProduct.bk_hash,
       stage_hash_mms_PricingDiscountIncludeProduct.PricingDiscountIncludeProductID pricing_discount_include_product_id,
       isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_PricingDiscountIncludeProduct
  left join h_mms_pricing_discount_include_product
    on stage_hash_mms_PricingDiscountIncludeProduct.bk_hash = h_mms_pricing_discount_include_product.bk_hash
 where h_mms_pricing_discount_include_product_id is null
   and stage_hash_mms_PricingDiscountIncludeProduct.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_pricing_discount_include_product
if object_id('tempdb..#l_mms_pricing_discount_include_product_inserts') is not null drop table #l_mms_pricing_discount_include_product_inserts
create table #l_mms_pricing_discount_include_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PricingDiscountIncludeProduct.bk_hash,
       stage_hash_mms_PricingDiscountIncludeProduct.PricingDiscountIncludeProductID pricing_discount_include_product_id,
       stage_hash_mms_PricingDiscountIncludeProduct.PricingDiscountID pricing_discount_id,
       stage_hash_mms_PricingDiscountIncludeProduct.ProductID product_id,
       stage_hash_mms_PricingDiscountIncludeProduct.DiscountedProductID discounted_product_id,
       stage_hash_mms_PricingDiscountIncludeProduct.OverrideDiscountTypeID override_discount_type_id,
       stage_hash_mms_PricingDiscountIncludeProduct.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.PricingDiscountIncludeProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.PricingDiscountID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.ProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.DiscountedProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.OverrideDiscountTypeID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PricingDiscountIncludeProduct
 where stage_hash_mms_PricingDiscountIncludeProduct.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_pricing_discount_include_product records
set @insert_date_time = getdate()
insert into l_mms_pricing_discount_include_product (
       bk_hash,
       pricing_discount_include_product_id,
       pricing_discount_id,
       product_id,
       discounted_product_id,
       override_discount_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_pricing_discount_include_product_inserts.bk_hash,
       #l_mms_pricing_discount_include_product_inserts.pricing_discount_include_product_id,
       #l_mms_pricing_discount_include_product_inserts.pricing_discount_id,
       #l_mms_pricing_discount_include_product_inserts.product_id,
       #l_mms_pricing_discount_include_product_inserts.discounted_product_id,
       #l_mms_pricing_discount_include_product_inserts.override_discount_type_id,
       case when l_mms_pricing_discount_include_product.l_mms_pricing_discount_include_product_id is null then isnull(#l_mms_pricing_discount_include_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_pricing_discount_include_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_pricing_discount_include_product_inserts
  left join p_mms_pricing_discount_include_product
    on #l_mms_pricing_discount_include_product_inserts.bk_hash = p_mms_pricing_discount_include_product.bk_hash
   and p_mms_pricing_discount_include_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_pricing_discount_include_product
    on p_mms_pricing_discount_include_product.bk_hash = l_mms_pricing_discount_include_product.bk_hash
   and p_mms_pricing_discount_include_product.l_mms_pricing_discount_include_product_id = l_mms_pricing_discount_include_product.l_mms_pricing_discount_include_product_id
 where l_mms_pricing_discount_include_product.l_mms_pricing_discount_include_product_id is null
    or (l_mms_pricing_discount_include_product.l_mms_pricing_discount_include_product_id is not null
        and l_mms_pricing_discount_include_product.dv_hash <> #l_mms_pricing_discount_include_product_inserts.source_hash)

--calculate hash and lookup to current s_mms_pricing_discount_include_product
if object_id('tempdb..#s_mms_pricing_discount_include_product_inserts') is not null drop table #s_mms_pricing_discount_include_product_inserts
create table #s_mms_pricing_discount_include_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_PricingDiscountIncludeProduct.bk_hash,
       stage_hash_mms_PricingDiscountIncludeProduct.PricingDiscountIncludeProductID pricing_discount_include_product_id,
       stage_hash_mms_PricingDiscountIncludeProduct.TriggerQuantity trigger_quantity,
       stage_hash_mms_PricingDiscountIncludeProduct.DiscountUseLimit discount_use_limit,
       stage_hash_mms_PricingDiscountIncludeProduct.InsertedDateTime inserted_date_time,
       stage_hash_mms_PricingDiscountIncludeProduct.UpdatedDateTime updated_date_time,
       stage_hash_mms_PricingDiscountIncludeProduct.OverrideDiscountValue override_discount_value,
       stage_hash_mms_PricingDiscountIncludeProduct.OverrideSalesCommissionPercent override_sales_commission_percent,
       stage_hash_mms_PricingDiscountIncludeProduct.OverrideServiceCommissionPercent override_service_commission_percent,
       stage_hash_mms_PricingDiscountIncludeProduct.BundleProductFlag bundle_product_flag,
       stage_hash_mms_PricingDiscountIncludeProduct.InsertedDateTime dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.PricingDiscountIncludeProductID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.TriggerQuantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.DiscountUseLimit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PricingDiscountIncludeProduct.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_PricingDiscountIncludeProduct.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.OverrideDiscountValue as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.OverrideSalesCommissionPercent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.OverrideServiceCommissionPercent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_PricingDiscountIncludeProduct.BundleProductFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_PricingDiscountIncludeProduct
 where stage_hash_mms_PricingDiscountIncludeProduct.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_pricing_discount_include_product records
set @insert_date_time = getdate()
insert into s_mms_pricing_discount_include_product (
       bk_hash,
       pricing_discount_include_product_id,
       trigger_quantity,
       discount_use_limit,
       inserted_date_time,
       updated_date_time,
       override_discount_value,
       override_sales_commission_percent,
       override_service_commission_percent,
       bundle_product_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_pricing_discount_include_product_inserts.bk_hash,
       #s_mms_pricing_discount_include_product_inserts.pricing_discount_include_product_id,
       #s_mms_pricing_discount_include_product_inserts.trigger_quantity,
       #s_mms_pricing_discount_include_product_inserts.discount_use_limit,
       #s_mms_pricing_discount_include_product_inserts.inserted_date_time,
       #s_mms_pricing_discount_include_product_inserts.updated_date_time,
       #s_mms_pricing_discount_include_product_inserts.override_discount_value,
       #s_mms_pricing_discount_include_product_inserts.override_sales_commission_percent,
       #s_mms_pricing_discount_include_product_inserts.override_service_commission_percent,
       #s_mms_pricing_discount_include_product_inserts.bundle_product_flag,
       case when s_mms_pricing_discount_include_product.s_mms_pricing_discount_include_product_id is null then isnull(#s_mms_pricing_discount_include_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_pricing_discount_include_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_pricing_discount_include_product_inserts
  left join p_mms_pricing_discount_include_product
    on #s_mms_pricing_discount_include_product_inserts.bk_hash = p_mms_pricing_discount_include_product.bk_hash
   and p_mms_pricing_discount_include_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_pricing_discount_include_product
    on p_mms_pricing_discount_include_product.bk_hash = s_mms_pricing_discount_include_product.bk_hash
   and p_mms_pricing_discount_include_product.s_mms_pricing_discount_include_product_id = s_mms_pricing_discount_include_product.s_mms_pricing_discount_include_product_id
 where s_mms_pricing_discount_include_product.s_mms_pricing_discount_include_product_id is null
    or (s_mms_pricing_discount_include_product.s_mms_pricing_discount_include_product_id is not null
        and s_mms_pricing_discount_include_product.dv_hash <> #s_mms_pricing_discount_include_product_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_pricing_discount_include_product @current_dv_batch_id

end
