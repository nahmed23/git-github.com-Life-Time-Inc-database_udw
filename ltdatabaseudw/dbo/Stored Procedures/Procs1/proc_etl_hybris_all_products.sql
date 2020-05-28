CREATE PROC [dbo].[proc_etl_hybris_all_products] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_ALL_PRODUCTS

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_ALL_PRODUCTS (
       bk_hash,
       createdTS,
       creationTime,
       code,
       name,
       onlineDatetime,
       offlineDatetime,
       summary,
       description,
       weight,
       modifiedTime,
       caption,
       ean,
       productCost,
       productHeight,
       productWidth,
       productLength,
       autoShipFlag,
       electronicShippingFlag,
       fulfillmentPartner,
       ltfOnlyProduct,
       ltfOfferFlag,
       offerExternalLinkFlag,
       eGiftCardFlag,
       offerLink,
       catalogVersion,
       activeCatalogVersion,
       catalogName,
       catalogVersionName,
       productCategory,
       productSubCategory,
       productType,
       productStockLevel,
       productStockStatus,
       LTBUCKSearned,
       AcceptLTBUCKSflag,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(code,'z#@$k%&P'))),2) bk_hash,
       createdTS,
       creationTime,
       code,
       name,
       onlineDatetime,
       offlineDatetime,
       summary,
       description,
       weight,
       modifiedTime,
       caption,
       ean,
       productCost,
       productHeight,
       productWidth,
       productLength,
       autoShipFlag,
       electronicShippingFlag,
       fulfillmentPartner,
       ltfOnlyProduct,
       ltfOfferFlag,
       offerExternalLinkFlag,
       eGiftCardFlag,
       offerLink,
       catalogVersion,
       activeCatalogVersion,
       catalogName,
       catalogVersionName,
       productCategory,
       productSubCategory,
       productType,
       productStockLevel,
       productStockStatus,
       LTBUCKSearned,
       AcceptLTBUCKSflag,
       isnull(cast(stage_hybris_ALL_PRODUCTS.modifiedtime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_hybris_ALL_PRODUCTS
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_all_products @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_all_products (
       bk_hash,
       code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_ALL_PRODUCTS.bk_hash,
       stage_hash_hybris_ALL_PRODUCTS.code code,
       isnull(cast(stage_hash_hybris_ALL_PRODUCTS.modifiedtime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_ALL_PRODUCTS
  left join h_hybris_all_products
    on stage_hash_hybris_ALL_PRODUCTS.bk_hash = h_hybris_all_products.bk_hash
 where h_hybris_all_products_id is null
   and stage_hash_hybris_ALL_PRODUCTS.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_all_products
if object_id('tempdb..#l_hybris_all_products_inserts') is not null drop table #l_hybris_all_products_inserts
create table #l_hybris_all_products_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_ALL_PRODUCTS.bk_hash,
       stage_hash_hybris_ALL_PRODUCTS.code code,
       stage_hash_hybris_ALL_PRODUCTS.catalogVersion catalog_version,
       stage_hash_hybris_ALL_PRODUCTS.activeCatalogVersion active_catalog_version,
       isnull(cast(stage_hash_hybris_ALL_PRODUCTS.modifiedtime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.catalogVersion as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.activeCatalogVersion as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_ALL_PRODUCTS
 where stage_hash_hybris_ALL_PRODUCTS.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_all_products records
set @insert_date_time = getdate()
insert into l_hybris_all_products (
       bk_hash,
       code,
       catalog_version,
       active_catalog_version,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_all_products_inserts.bk_hash,
       #l_hybris_all_products_inserts.code,
       #l_hybris_all_products_inserts.catalog_version,
       #l_hybris_all_products_inserts.active_catalog_version,
       case when l_hybris_all_products.l_hybris_all_products_id is null then isnull(#l_hybris_all_products_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_all_products_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_all_products_inserts
  left join p_hybris_all_products
    on #l_hybris_all_products_inserts.bk_hash = p_hybris_all_products.bk_hash
   and p_hybris_all_products.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_all_products
    on p_hybris_all_products.bk_hash = l_hybris_all_products.bk_hash
   and p_hybris_all_products.l_hybris_all_products_id = l_hybris_all_products.l_hybris_all_products_id
 where l_hybris_all_products.l_hybris_all_products_id is null
    or (l_hybris_all_products.l_hybris_all_products_id is not null
        and l_hybris_all_products.dv_hash <> #l_hybris_all_products_inserts.source_hash)

--calculate hash and lookup to current s_hybris_all_products
if object_id('tempdb..#s_hybris_all_products_inserts') is not null drop table #s_hybris_all_products_inserts
create table #s_hybris_all_products_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_ALL_PRODUCTS.bk_hash,
       stage_hash_hybris_ALL_PRODUCTS.createdTS created_ts,
       stage_hash_hybris_ALL_PRODUCTS.creationTime creation_time,
       stage_hash_hybris_ALL_PRODUCTS.code code,
       stage_hash_hybris_ALL_PRODUCTS.name name,
       stage_hash_hybris_ALL_PRODUCTS.onlineDatetime online_datetime,
       stage_hash_hybris_ALL_PRODUCTS.offlineDatetime offline_datetime,
       stage_hash_hybris_ALL_PRODUCTS.summary summary,
       stage_hash_hybris_ALL_PRODUCTS.description description,
       stage_hash_hybris_ALL_PRODUCTS.weight weight,
       stage_hash_hybris_ALL_PRODUCTS.modifiedTime modified_time,
       stage_hash_hybris_ALL_PRODUCTS.caption caption,
       stage_hash_hybris_ALL_PRODUCTS.ean ean,
       stage_hash_hybris_ALL_PRODUCTS.productCost product_cost,
       stage_hash_hybris_ALL_PRODUCTS.productHeight product_height,
       stage_hash_hybris_ALL_PRODUCTS.productWidth product_width,
       stage_hash_hybris_ALL_PRODUCTS.productLength product_length,
       stage_hash_hybris_ALL_PRODUCTS.autoShipFlag auto_ship_flag,
       stage_hash_hybris_ALL_PRODUCTS.electronicShippingFlag electronic_shipping_flag,
       stage_hash_hybris_ALL_PRODUCTS.fulfillmentPartner fulfillment_partner,
       stage_hash_hybris_ALL_PRODUCTS.ltfOnlyProduct ltf_only_product,
       stage_hash_hybris_ALL_PRODUCTS.ltfOfferFlag ltf_offer_flag,
       stage_hash_hybris_ALL_PRODUCTS.offerExternalLinkFlag offer_external_link_flag,
       stage_hash_hybris_ALL_PRODUCTS.eGiftCardFlag e_gift_card_flag,
       stage_hash_hybris_ALL_PRODUCTS.offerLink offer_link,
       stage_hash_hybris_ALL_PRODUCTS.catalogName catalog_name,
       stage_hash_hybris_ALL_PRODUCTS.catalogVersionName catalog_version_name,
       stage_hash_hybris_ALL_PRODUCTS.productCategory product_category,
       stage_hash_hybris_ALL_PRODUCTS.productSubCategory product_sub_category,
       stage_hash_hybris_ALL_PRODUCTS.productType product_type,
       stage_hash_hybris_ALL_PRODUCTS.productStockLevel product_stock_level,
       stage_hash_hybris_ALL_PRODUCTS.productStockStatus product_stock_status,
       stage_hash_hybris_ALL_PRODUCTS.LTBUCKSearned lt_bucks_earned,
       stage_hash_hybris_ALL_PRODUCTS.AcceptLTBUCKSflag accept_lt_bucks_flag,
       isnull(cast(stage_hash_hybris_ALL_PRODUCTS.modifiedtime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ALL_PRODUCTS.createdTS,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ALL_PRODUCTS.creationTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ALL_PRODUCTS.onlineDatetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ALL_PRODUCTS.offlineDatetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.summary,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.weight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_ALL_PRODUCTS.modifiedTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.caption,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.ean,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.productCost as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.productHeight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.productWidth as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.productLength as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.autoShipFlag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.electronicShippingFlag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.fulfillmentPartner,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.ltfOnlyProduct as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.ltfOfferFlag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.offerExternalLinkFlag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.eGiftCardFlag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.offerLink,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.catalogName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.catalogVersionName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.productCategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.productSubCategory,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.productType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.productStockLevel as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_hybris_ALL_PRODUCTS.productStockStatus,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.LTBUCKSearned as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_hybris_ALL_PRODUCTS.AcceptLTBUCKSflag as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_ALL_PRODUCTS
 where stage_hash_hybris_ALL_PRODUCTS.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_all_products records
set @insert_date_time = getdate()
insert into s_hybris_all_products (
       bk_hash,
       created_ts,
       creation_time,
       code,
       name,
       online_datetime,
       offline_datetime,
       summary,
       description,
       weight,
       modified_time,
       caption,
       ean,
       product_cost,
       product_height,
       product_width,
       product_length,
       auto_ship_flag,
       electronic_shipping_flag,
       fulfillment_partner,
       ltf_only_product,
       ltf_offer_flag,
       offer_external_link_flag,
       e_gift_card_flag,
       offer_link,
       catalog_name,
       catalog_version_name,
       product_category,
       product_sub_category,
       product_type,
       product_stock_level,
       product_stock_status,
       lt_bucks_earned,
       accept_lt_bucks_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_all_products_inserts.bk_hash,
       #s_hybris_all_products_inserts.created_ts,
       #s_hybris_all_products_inserts.creation_time,
       #s_hybris_all_products_inserts.code,
       #s_hybris_all_products_inserts.name,
       #s_hybris_all_products_inserts.online_datetime,
       #s_hybris_all_products_inserts.offline_datetime,
       #s_hybris_all_products_inserts.summary,
       #s_hybris_all_products_inserts.description,
       #s_hybris_all_products_inserts.weight,
       #s_hybris_all_products_inserts.modified_time,
       #s_hybris_all_products_inserts.caption,
       #s_hybris_all_products_inserts.ean,
       #s_hybris_all_products_inserts.product_cost,
       #s_hybris_all_products_inserts.product_height,
       #s_hybris_all_products_inserts.product_width,
       #s_hybris_all_products_inserts.product_length,
       #s_hybris_all_products_inserts.auto_ship_flag,
       #s_hybris_all_products_inserts.electronic_shipping_flag,
       #s_hybris_all_products_inserts.fulfillment_partner,
       #s_hybris_all_products_inserts.ltf_only_product,
       #s_hybris_all_products_inserts.ltf_offer_flag,
       #s_hybris_all_products_inserts.offer_external_link_flag,
       #s_hybris_all_products_inserts.e_gift_card_flag,
       #s_hybris_all_products_inserts.offer_link,
       #s_hybris_all_products_inserts.catalog_name,
       #s_hybris_all_products_inserts.catalog_version_name,
       #s_hybris_all_products_inserts.product_category,
       #s_hybris_all_products_inserts.product_sub_category,
       #s_hybris_all_products_inserts.product_type,
       #s_hybris_all_products_inserts.product_stock_level,
       #s_hybris_all_products_inserts.product_stock_status,
       #s_hybris_all_products_inserts.lt_bucks_earned,
       #s_hybris_all_products_inserts.accept_lt_bucks_flag,
       case when s_hybris_all_products.s_hybris_all_products_id is null then isnull(#s_hybris_all_products_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_all_products_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_all_products_inserts
  left join p_hybris_all_products
    on #s_hybris_all_products_inserts.bk_hash = p_hybris_all_products.bk_hash
   and p_hybris_all_products.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_all_products
    on p_hybris_all_products.bk_hash = s_hybris_all_products.bk_hash
   and p_hybris_all_products.s_hybris_all_products_id = s_hybris_all_products.s_hybris_all_products_id
 where s_hybris_all_products.s_hybris_all_products_id is null
    or (s_hybris_all_products.s_hybris_all_products_id is not null
        and s_hybris_all_products.dv_hash <> #s_hybris_all_products_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_all_products @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_hybris_all_products @current_dv_batch_id

end
