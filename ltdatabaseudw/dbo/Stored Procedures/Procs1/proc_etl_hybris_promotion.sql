CREATE PROC [dbo].[proc_etl_hybris_promotion] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_promotion

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_promotion (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_code,
       p_title,
       p_description,
       p_startdate,
       p_enddate,
       p_detailsurl,
       p_enabled,
       p_priority,
       p_immutablekeyhash,
       p_immutablekey,
       p_promotiongroup,
       aCLTS,
       propTS,
       p_productbanner,
       p_productfixedunitprice,
       p_percentagediscount,
       p_qualifyingcount,
       p_freecount,
       p_bundleprices,
       p_qualifyingcountsandbundlepri,
       p_partnerproducts,
       p_partnerprices,
       p_partnerproduct,
       p_thresholdtotals,
       p_discountproduct,
       p_productprices,
       p_includediscountedpriceinthre,
       p_discountprices,
       p_giftproduct,
       p_freevoucher,
       p_deliverymode,
       p_rule,
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
       p_code,
       p_title,
       p_description,
       p_startdate,
       p_enddate,
       p_detailsurl,
       p_enabled,
       p_priority,
       p_immutablekeyhash,
       p_immutablekey,
       p_promotiongroup,
       aCLTS,
       propTS,
       p_productbanner,
       p_productfixedunitprice,
       p_percentagediscount,
       p_qualifyingcount,
       p_freecount,
       p_bundleprices,
       p_qualifyingcountsandbundlepri,
       p_partnerproducts,
       p_partnerprices,
       p_partnerproduct,
       p_thresholdtotals,
       p_discountproduct,
       p_productprices,
       p_includediscountedpriceinthre,
       p_discountprices,
       p_giftproduct,
       p_freevoucher,
       p_deliverymode,
       p_rule,
       isnull(cast(stage_hybris_promotion.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_promotion
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_promotion @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_promotion (
       bk_hash,
       promotion_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_promotion.bk_hash,
       stage_hash_hybris_promotion.[PK] promotion_pk,
       isnull(cast(stage_hash_hybris_promotion.createdTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_promotion
  left join h_hybris_promotion
    on stage_hash_hybris_promotion.bk_hash = h_hybris_promotion.bk_hash
 where h_hybris_promotion_id is null
   and stage_hash_hybris_promotion.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_promotion
if object_id('tempdb..#l_hybris_promotion_inserts') is not null drop table #l_hybris_promotion_inserts
create table #l_hybris_promotion_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_promotion.bk_hash,
       stage_hash_hybris_promotion.TypePkString type_pk_string,
       stage_hash_hybris_promotion.OwnerPkString owner_pk_string,
       stage_hash_hybris_promotion.[PK] promotion_pk,
       stage_hash_hybris_promotion.p_promotiongroup p_promotion_group,
       stage_hash_hybris_promotion.p_productbanner p_product_banner,
       stage_hash_hybris_promotion.p_partnerproduct p_partner_product,
       stage_hash_hybris_promotion.p_discountproduct p_discount_product,
       stage_hash_hybris_promotion.p_giftproduct p_gift_product,
       stage_hash_hybris_promotion.p_freevoucher p_free_voucher,
       stage_hash_hybris_promotion.p_deliverymode p_delivery_mode,
       stage_hash_hybris_promotion.p_rule p_rule,
       stage_hash_hybris_promotion.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_promotiongroup as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_productbanner as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_partnerproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_discountproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_giftproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_freevoucher as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_deliverymode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_rule as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_promotion
 where stage_hash_hybris_promotion.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_promotion records
set @insert_date_time = getdate()
insert into l_hybris_promotion (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       promotion_pk,
       p_promotion_group,
       p_product_banner,
       p_partner_product,
       p_discount_product,
       p_gift_product,
       p_free_voucher,
       p_delivery_mode,
       p_rule,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_promotion_inserts.bk_hash,
       #l_hybris_promotion_inserts.type_pk_string,
       #l_hybris_promotion_inserts.owner_pk_string,
       #l_hybris_promotion_inserts.promotion_pk,
       #l_hybris_promotion_inserts.p_promotion_group,
       #l_hybris_promotion_inserts.p_product_banner,
       #l_hybris_promotion_inserts.p_partner_product,
       #l_hybris_promotion_inserts.p_discount_product,
       #l_hybris_promotion_inserts.p_gift_product,
       #l_hybris_promotion_inserts.p_free_voucher,
       #l_hybris_promotion_inserts.p_delivery_mode,
       #l_hybris_promotion_inserts.p_rule,
       case when l_hybris_promotion.l_hybris_promotion_id is null then isnull(#l_hybris_promotion_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_promotion_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_promotion_inserts
  left join p_hybris_promotion
    on #l_hybris_promotion_inserts.bk_hash = p_hybris_promotion.bk_hash
   and p_hybris_promotion.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_promotion
    on p_hybris_promotion.bk_hash = l_hybris_promotion.bk_hash
   and p_hybris_promotion.l_hybris_promotion_id = l_hybris_promotion.l_hybris_promotion_id
 where l_hybris_promotion.l_hybris_promotion_id is null
    or (l_hybris_promotion.l_hybris_promotion_id is not null
        and l_hybris_promotion.dv_hash <> #l_hybris_promotion_inserts.source_hash)

--calculate hash and lookup to current s_hybris_promotion
if object_id('tempdb..#s_hybris_promotion_inserts') is not null drop table #s_hybris_promotion_inserts
create table #s_hybris_promotion_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_promotion.bk_hash,
       stage_hash_hybris_promotion.hjmpTS hjmpts,
       stage_hash_hybris_promotion.createdTS created_ts,
       stage_hash_hybris_promotion.modifiedTS modified_ts,
       stage_hash_hybris_promotion.[PK] promotion_pk,
       stage_hash_hybris_promotion.p_code p_code,
       stage_hash_hybris_promotion.p_title p_title,
       stage_hash_hybris_promotion.p_description p_description,
       stage_hash_hybris_promotion.p_startdate p_start_date,
       stage_hash_hybris_promotion.p_enddate p_end_date,
       stage_hash_hybris_promotion.p_detailsurl p_details_url,
       stage_hash_hybris_promotion.p_enabled p_enabled,
       stage_hash_hybris_promotion.p_priority p_priority,
       stage_hash_hybris_promotion.p_immutablekeyhash p_immutable_key_hash,
       stage_hash_hybris_promotion.p_immutablekey p_immutable_key,
       stage_hash_hybris_promotion.aCLTS acl_ts,
       stage_hash_hybris_promotion.propTS prop_ts,
       stage_hash_hybris_promotion.p_productfixedunitprice p_product_fixed_unit_price,
       stage_hash_hybris_promotion.p_percentagediscount p_percentage_discount,
       stage_hash_hybris_promotion.p_qualifyingcount p_qualifying_count,
       stage_hash_hybris_promotion.p_freecount p_free_count,
       stage_hash_hybris_promotion.p_bundleprices p_bundle_prices,
       stage_hash_hybris_promotion.p_qualifyingcountsandbundlepri p_qualifying_counts_and_bundle_pri,
       stage_hash_hybris_promotion.p_partnerproducts p_partner_products,
       stage_hash_hybris_promotion.p_partnerprices p_partner_prices,
       stage_hash_hybris_promotion.p_thresholdtotals p_threshold_totals,
       stage_hash_hybris_promotion.p_productprices p_product_prices,
       stage_hash_hybris_promotion.p_includediscountedpriceinthre p_include_discounted_price_in_thre,
       stage_hash_hybris_promotion.p_discountprices p_discount_prices,
       stage_hash_hybris_promotion.createdTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotion.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotion.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_title,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotion.p_startdate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_promotion.p_enddate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_detailsurl,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_enabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_priority as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_immutablekeyhash,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_immutablekey,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_productfixedunitprice,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_percentagediscount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_qualifyingcount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_freecount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_bundleprices,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_qualifyingcountsandbundlepri,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_partnerproducts,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_partnerprices,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_thresholdtotals,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_productprices,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_promotion.p_includediscountedpriceinthre as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_promotion.p_discountprices,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_promotion
 where stage_hash_hybris_promotion.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_promotion records
set @insert_date_time = getdate()
insert into s_hybris_promotion (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       promotion_pk,
       p_code,
       p_title,
       p_description,
       p_start_date,
       p_end_date,
       p_details_url,
       p_enabled,
       p_priority,
       p_immutable_key_hash,
       p_immutable_key,
       acl_ts,
       prop_ts,
       p_product_fixed_unit_price,
       p_percentage_discount,
       p_qualifying_count,
       p_free_count,
       p_bundle_prices,
       p_qualifying_counts_and_bundle_pri,
       p_partner_products,
       p_partner_prices,
       p_threshold_totals,
       p_product_prices,
       p_include_discounted_price_in_thre,
       p_discount_prices,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_promotion_inserts.bk_hash,
       #s_hybris_promotion_inserts.hjmpts,
       #s_hybris_promotion_inserts.created_ts,
       #s_hybris_promotion_inserts.modified_ts,
       #s_hybris_promotion_inserts.promotion_pk,
       #s_hybris_promotion_inserts.p_code,
       #s_hybris_promotion_inserts.p_title,
       #s_hybris_promotion_inserts.p_description,
       #s_hybris_promotion_inserts.p_start_date,
       #s_hybris_promotion_inserts.p_end_date,
       #s_hybris_promotion_inserts.p_details_url,
       #s_hybris_promotion_inserts.p_enabled,
       #s_hybris_promotion_inserts.p_priority,
       #s_hybris_promotion_inserts.p_immutable_key_hash,
       #s_hybris_promotion_inserts.p_immutable_key,
       #s_hybris_promotion_inserts.acl_ts,
       #s_hybris_promotion_inserts.prop_ts,
       #s_hybris_promotion_inserts.p_product_fixed_unit_price,
       #s_hybris_promotion_inserts.p_percentage_discount,
       #s_hybris_promotion_inserts.p_qualifying_count,
       #s_hybris_promotion_inserts.p_free_count,
       #s_hybris_promotion_inserts.p_bundle_prices,
       #s_hybris_promotion_inserts.p_qualifying_counts_and_bundle_pri,
       #s_hybris_promotion_inserts.p_partner_products,
       #s_hybris_promotion_inserts.p_partner_prices,
       #s_hybris_promotion_inserts.p_threshold_totals,
       #s_hybris_promotion_inserts.p_product_prices,
       #s_hybris_promotion_inserts.p_include_discounted_price_in_thre,
       #s_hybris_promotion_inserts.p_discount_prices,
       case when s_hybris_promotion.s_hybris_promotion_id is null then isnull(#s_hybris_promotion_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_promotion_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_promotion_inserts
  left join p_hybris_promotion
    on #s_hybris_promotion_inserts.bk_hash = p_hybris_promotion.bk_hash
   and p_hybris_promotion.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_promotion
    on p_hybris_promotion.bk_hash = s_hybris_promotion.bk_hash
   and p_hybris_promotion.s_hybris_promotion_id = s_hybris_promotion.s_hybris_promotion_id
 where s_hybris_promotion.s_hybris_promotion_id is null
    or (s_hybris_promotion.s_hybris_promotion_id is not null
        and s_hybris_promotion.dv_hash <> #s_hybris_promotion_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_promotion @current_dv_batch_id

end
