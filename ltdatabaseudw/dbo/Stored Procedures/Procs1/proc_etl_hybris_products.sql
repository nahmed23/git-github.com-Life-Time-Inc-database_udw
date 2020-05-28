CREATE PROC [dbo].[proc_etl_hybris_products] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_products

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_products (
       bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_varianttype,
       p_manufactureraid,
       p_normal,
       p_state,
       p_number,
       p_autothumbnails,
       p_erpgroupsupplier,
       p_catalogversion,
       code,
       p_productwidth,
       p_offlinedate,
       p_detail,
       p_electronicshippingflag,
       p_order,
       p_mediacontainer,
       p_offerexternallinkflag,
       p_swatchcolour,
       p_simpleasset,
       p_giftcardflag,
       p_startlinenumber,
       p_numbercontentunits,
       p_data_sheet,
       p_europe1pricefactory_pdg,
       p_fulfillmentpartner,
       p_baseproduct,
       p_denomination,
       unitpk,
       p_deliverytime,
       p_mobileproductflag,
       p_previousbaseproduct,
       p_maxorderquantity,
       p_thumbnail,
       p_productlength,
       p_manufacturername,
       p_endlinenumber,
       p_thumbnails,
       p_minorderquantity,
       p_productheight,
       p_freeuspsshippingflag,
       p_ean,
       p_egiftcards,
       p_club,
       p_logo,
       p_autoshipflag,
       p_orderquantityinterval,
       p_others,
       p_egiftcardflag,
       p_galleryimages,
       p_onlinedate,
       p_media,
       p_contentunit,
       p_ltfofferflag,
       p_genders,
       p_rawasset,
       p_europe1pricefactory_ptg,
       p_productcost,
       p_pricequantity,
       p_weight,
       p_picture,
       p_approvalstatus,
       p_europe1pricefactory_ppg,
       p_erpgroupbuyer,
       p_ltfonlyproduct,
       p_catalog,
       p_supplieralternativeaid,
       p_productorderlimit,
       p_sequenceid,
       p_ltbucksamount,
       p_ltbucksflag,
       p_canadianproduct,
       p_externalredeemflag,
       p_mmsproductid,
       p_emailtemplateid,
       p_cardtype,
       p_personaldetails,
       p_trainingsession,
       p_displaynumber,
       p_regionid,
       p_revcategoryid,
       p_costcenterid,
       p_itemdescription,
       p_offeringid,
       p_linecompany,
       p_spendcategoryid,
       p_malocalflag,
       p_article,
       p_agreement,
       p_gender,
       p_fulfillmentproductid,
       p_typedescriptor,
       p_barcode,
       p_producttag,
       p_searchable,
       p_productmessages,
       p_occasions,
       p_isgiftable,
       p_mmsrecurrent,
       p_allowmemberselection,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast([PK] as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       TypePkString,
       [PK],
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       p_varianttype,
       p_manufactureraid,
       p_normal,
       p_state,
       p_number,
       p_autothumbnails,
       p_erpgroupsupplier,
       p_catalogversion,
       code,
       p_productwidth,
       p_offlinedate,
       p_detail,
       p_electronicshippingflag,
       p_order,
       p_mediacontainer,
       p_offerexternallinkflag,
       p_swatchcolour,
       p_simpleasset,
       p_giftcardflag,
       p_startlinenumber,
       p_numbercontentunits,
       p_data_sheet,
       p_europe1pricefactory_pdg,
       p_fulfillmentpartner,
       p_baseproduct,
       p_denomination,
       unitpk,
       p_deliverytime,
       p_mobileproductflag,
       p_previousbaseproduct,
       p_maxorderquantity,
       p_thumbnail,
       p_productlength,
       p_manufacturername,
       p_endlinenumber,
       p_thumbnails,
       p_minorderquantity,
       p_productheight,
       p_freeuspsshippingflag,
       p_ean,
       p_egiftcards,
       p_club,
       p_logo,
       p_autoshipflag,
       p_orderquantityinterval,
       p_others,
       p_egiftcardflag,
       p_galleryimages,
       p_onlinedate,
       p_media,
       p_contentunit,
       p_ltfofferflag,
       p_genders,
       p_rawasset,
       p_europe1pricefactory_ptg,
       p_productcost,
       p_pricequantity,
       p_weight,
       p_picture,
       p_approvalstatus,
       p_europe1pricefactory_ppg,
       p_erpgroupbuyer,
       p_ltfonlyproduct,
       p_catalog,
       p_supplieralternativeaid,
       p_productorderlimit,
       p_sequenceid,
       p_ltbucksamount,
       p_ltbucksflag,
       p_canadianproduct,
       p_externalredeemflag,
       p_mmsproductid,
       p_emailtemplateid,
       p_cardtype,
       p_personaldetails,
       p_trainingsession,
       p_displaynumber,
       p_regionid,
       p_revcategoryid,
       p_costcenterid,
       p_itemdescription,
       p_offeringid,
       p_linecompany,
       p_spendcategoryid,
       p_malocalflag,
       p_article,
       p_agreement,
       p_gender,
       p_fulfillmentproductid,
       p_typedescriptor,
       p_barcode,
       p_producttag,
       p_searchable,
       p_productmessages,
       p_occasions,
       p_isgiftable,
       p_mmsrecurrent,
       p_allowmemberselection,
       isnull(cast(stage_hybris_products.modifiedts as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_products
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_products @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_products (
       bk_hash,
       products_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_products.bk_hash,
       stage_hash_hybris_products.[PK] products_pk,
       isnull(cast(stage_hash_hybris_products.modifiedts as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_products
  left join h_hybris_products
    on stage_hash_hybris_products.bk_hash = h_hybris_products.bk_hash
 where h_hybris_products_id is null
   and stage_hash_hybris_products.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_products
if object_id('tempdb..#l_hybris_products_inserts') is not null drop table #l_hybris_products_inserts
create table #l_hybris_products_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_products.bk_hash,
       stage_hash_hybris_products.TypePkString type_pk_string,
       stage_hash_hybris_products.[PK] products_pk,
       stage_hash_hybris_products.OwnerPkString owner_pk_string,
       stage_hash_hybris_products.p_varianttype p_variant_type,
       stage_hash_hybris_products.p_state p_state,
       stage_hash_hybris_products.p_catalogversion p_catalog_version,
       stage_hash_hybris_products.p_mediacontainer p_media_container,
       stage_hash_hybris_products.p_simpleasset p_simple_asset,
       stage_hash_hybris_products.p_europe1pricefactory_pdg p_europe_1_price_factory_pdg,
       stage_hash_hybris_products.p_fulfillmentpartner p_fulfillment_partner,
       stage_hash_hybris_products.p_baseproduct p_base_product,
       stage_hash_hybris_products.unitpk unit_pk,
       stage_hash_hybris_products.p_previousbaseproduct p_previous_base_product,
       stage_hash_hybris_products.p_thumbnail p_thumb_nail,
       stage_hash_hybris_products.p_club p_club,
       stage_hash_hybris_products.p_media p_media,
       stage_hash_hybris_products.p_contentunit p_content_unit,
       stage_hash_hybris_products.p_rawasset p_raw_asset,
       stage_hash_hybris_products.p_europe1pricefactory_ptg p_europe_1_price_factory_ptg,
       stage_hash_hybris_products.p_picture p_picture,
       stage_hash_hybris_products.p_approvalstatus p_approval_status,
       stage_hash_hybris_products.p_europe1pricefactory_ppg p_europe_1_price_factory_ppg,
       stage_hash_hybris_products.p_catalog p_catalog,
       stage_hash_hybris_products.p_productorderlimit p_product_order_limit,
       stage_hash_hybris_products.p_sequenceid p_sequence_id,
       stage_hash_hybris_products.p_cardtype p_card_type,
       stage_hash_hybris_products.p_personaldetails p_personal_details,
       stage_hash_hybris_products.p_trainingsession p_training_session,
       stage_hash_hybris_products.p_article p_article,
       stage_hash_hybris_products.p_agreement p_agreement,
       stage_hash_hybris_products.p_gender p_gender,
       stage_hash_hybris_products.p_producttag p_product_tag,
       isnull(cast(stage_hash_hybris_products.modifiedts as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_products.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_varianttype as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_state as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_catalogversion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_mediacontainer as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_simpleasset as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_europe1pricefactory_pdg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_fulfillmentpartner as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_baseproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.unitpk as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_previousbaseproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_thumbnail as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_club as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_media as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_contentunit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_rawasset as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_europe1pricefactory_ptg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_picture as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_approvalstatus as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_europe1pricefactory_ppg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_catalog as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_productorderlimit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_sequenceid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_cardtype as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_personaldetails as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_trainingsession as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_article as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_agreement as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_gender as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_producttag as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_products
 where stage_hash_hybris_products.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_products records
set @insert_date_time = getdate()
insert into l_hybris_products (
       bk_hash,
       type_pk_string,
       products_pk,
       owner_pk_string,
       p_variant_type,
       p_state,
       p_catalog_version,
       p_media_container,
       p_simple_asset,
       p_europe_1_price_factory_pdg,
       p_fulfillment_partner,
       p_base_product,
       unit_pk,
       p_previous_base_product,
       p_thumb_nail,
       p_club,
       p_media,
       p_content_unit,
       p_raw_asset,
       p_europe_1_price_factory_ptg,
       p_picture,
       p_approval_status,
       p_europe_1_price_factory_ppg,
       p_catalog,
       p_product_order_limit,
       p_sequence_id,
       p_card_type,
       p_personal_details,
       p_training_session,
       p_article,
       p_agreement,
       p_gender,
       p_product_tag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_products_inserts.bk_hash,
       #l_hybris_products_inserts.type_pk_string,
       #l_hybris_products_inserts.products_pk,
       #l_hybris_products_inserts.owner_pk_string,
       #l_hybris_products_inserts.p_variant_type,
       #l_hybris_products_inserts.p_state,
       #l_hybris_products_inserts.p_catalog_version,
       #l_hybris_products_inserts.p_media_container,
       #l_hybris_products_inserts.p_simple_asset,
       #l_hybris_products_inserts.p_europe_1_price_factory_pdg,
       #l_hybris_products_inserts.p_fulfillment_partner,
       #l_hybris_products_inserts.p_base_product,
       #l_hybris_products_inserts.unit_pk,
       #l_hybris_products_inserts.p_previous_base_product,
       #l_hybris_products_inserts.p_thumb_nail,
       #l_hybris_products_inserts.p_club,
       #l_hybris_products_inserts.p_media,
       #l_hybris_products_inserts.p_content_unit,
       #l_hybris_products_inserts.p_raw_asset,
       #l_hybris_products_inserts.p_europe_1_price_factory_ptg,
       #l_hybris_products_inserts.p_picture,
       #l_hybris_products_inserts.p_approval_status,
       #l_hybris_products_inserts.p_europe_1_price_factory_ppg,
       #l_hybris_products_inserts.p_catalog,
       #l_hybris_products_inserts.p_product_order_limit,
       #l_hybris_products_inserts.p_sequence_id,
       #l_hybris_products_inserts.p_card_type,
       #l_hybris_products_inserts.p_personal_details,
       #l_hybris_products_inserts.p_training_session,
       #l_hybris_products_inserts.p_article,
       #l_hybris_products_inserts.p_agreement,
       #l_hybris_products_inserts.p_gender,
       #l_hybris_products_inserts.p_product_tag,
       case when l_hybris_products.l_hybris_products_id is null then isnull(#l_hybris_products_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_products_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_products_inserts
  left join p_hybris_products
    on #l_hybris_products_inserts.bk_hash = p_hybris_products.bk_hash
   and p_hybris_products.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_products
    on p_hybris_products.bk_hash = l_hybris_products.bk_hash
   and p_hybris_products.l_hybris_products_id = l_hybris_products.l_hybris_products_id
 where l_hybris_products.l_hybris_products_id is null
    or (l_hybris_products.l_hybris_products_id is not null
        and l_hybris_products.dv_hash <> #l_hybris_products_inserts.source_hash)

--calculate hash and lookup to current s_hybris_products
if object_id('tempdb..#s_hybris_products_inserts') is not null drop table #s_hybris_products_inserts
create table #s_hybris_products_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_products.bk_hash,
       stage_hash_hybris_products.hjmpTS hjmpts,
       stage_hash_hybris_products.[PK] products_pk,
       stage_hash_hybris_products.createdTS created_ts,
       stage_hash_hybris_products.modifiedTS modified_ts,
       stage_hash_hybris_products.aCLTS acl_ts,
       stage_hash_hybris_products.propTS prop_ts,
       stage_hash_hybris_products.p_manufactureraid p_manufacturer_aid,
       stage_hash_hybris_products.p_normal p_normal,
       stage_hash_hybris_products.p_number p_number,
       stage_hash_hybris_products.p_autothumbnails p_auto_thumb_nails,
       stage_hash_hybris_products.p_erpgroupsupplier p_erp_group_supplier,
       stage_hash_hybris_products.code code,
       stage_hash_hybris_products.p_productwidth p_product_width,
       stage_hash_hybris_products.p_offlinedate p_offline_date,
       stage_hash_hybris_products.p_detail p_detail,
       stage_hash_hybris_products.p_electronicshippingflag p_electronic_shipping_flag,
       stage_hash_hybris_products.p_order p_order,
       stage_hash_hybris_products.p_offerexternallinkflag p_offer_external_link_flag,
       stage_hash_hybris_products.p_swatchcolour p_swatch_colour,
       stage_hash_hybris_products.p_giftcardflag p_gift_card_flag,
       stage_hash_hybris_products.p_startlinenumber p_start_line_number,
       stage_hash_hybris_products.p_numbercontentunits p_number_content_units,
       stage_hash_hybris_products.p_data_sheet p_data_sheet,
       stage_hash_hybris_products.p_denomination p_denomination,
       stage_hash_hybris_products.p_deliverytime p_delivery_time,
       stage_hash_hybris_products.p_mobileproductflag p_mobile_product_flag,
       stage_hash_hybris_products.p_maxorderquantity p_max_order_quantity,
       stage_hash_hybris_products.p_productlength p_product_length,
       stage_hash_hybris_products.p_manufacturername p_manufacturer_name,
       stage_hash_hybris_products.p_endlinenumber p_end_line_number,
       stage_hash_hybris_products.p_thumbnails p_thumb_nails,
       stage_hash_hybris_products.p_minorderquantity p_min_order_quantity,
       stage_hash_hybris_products.p_productheight p_product_height,
       stage_hash_hybris_products.p_freeuspsshippingflag p_free_usps_shipping_flag,
       stage_hash_hybris_products.p_ean p_ean,
       stage_hash_hybris_products.p_egiftcards p_e_gift_cards,
       stage_hash_hybris_products.p_logo p_logo,
       stage_hash_hybris_products.p_autoshipflag p_auto_shipflag,
       stage_hash_hybris_products.p_orderquantityinterval p_order_quantity_interval,
       stage_hash_hybris_products.p_others p_others,
       stage_hash_hybris_products.p_egiftcardflag p_e_gift_card_flag,
       stage_hash_hybris_products.p_galleryimages p_gallery_images,
       stage_hash_hybris_products.p_onlinedate p_online_date,
       stage_hash_hybris_products.p_ltfofferflag p_ltf_offer_flag,
       stage_hash_hybris_products.p_genders p_genders,
       stage_hash_hybris_products.p_productcost p_product_cost,
       stage_hash_hybris_products.p_pricequantity p_price_quantity,
       stage_hash_hybris_products.p_weight p_weight,
       stage_hash_hybris_products.p_erpgroupbuyer p_erp_group_buyer,
       stage_hash_hybris_products.p_ltfonlyproduct p_ltf_only_product,
       stage_hash_hybris_products.p_supplieralternativeaid p_supplier_alternative_aid,
       stage_hash_hybris_products.p_ltbucksamount p_lt_bucks_amount,
       stage_hash_hybris_products.p_ltbucksflag p_lt_bucks_flag,
       stage_hash_hybris_products.p_canadianproduct p_canadian_product,
       stage_hash_hybris_products.p_externalredeemflag p_external_redeem_flag,
       stage_hash_hybris_products.p_mmsproductid p_mms_product_id,
       stage_hash_hybris_products.p_emailtemplateid p_email_template_id,
       stage_hash_hybris_products.p_displaynumber p_display_number,
       stage_hash_hybris_products.p_regionid p_region_id,
       stage_hash_hybris_products.p_revcategoryid p_rev_category_id,
       stage_hash_hybris_products.p_costcenterid p_cost_center_id,
       stage_hash_hybris_products.p_itemdescription p_item_description,
       stage_hash_hybris_products.p_offeringid p_offering_id,
       stage_hash_hybris_products.p_linecompany p_line_company,
       stage_hash_hybris_products.p_spendcategoryid p_spend_category_id,
       stage_hash_hybris_products.p_malocalflag p_ma_local_flag,
       stage_hash_hybris_products.p_fulfillmentproductid p_fulfillment_product_id,
       stage_hash_hybris_products.p_typedescriptor p_type_descriptor,
       stage_hash_hybris_products.p_barcode p_barcode,
       stage_hash_hybris_products.p_searchable p_searchable,
       stage_hash_hybris_products.p_productmessages p_product_messages,
       stage_hash_hybris_products.p_occasions p_occasions,
       stage_hash_hybris_products.p_isgiftable p_is_giftable,
       stage_hash_hybris_products.p_mmsrecurrent p_mms_recurrent,
       stage_hash_hybris_products.p_allowmemberselection p_allow_member_selection,
       isnull(cast(stage_hash_hybris_products.modifiedts as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_products.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_products.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_products.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_manufactureraid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_normal,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_number as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_autothumbnails,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_erpgroupsupplier,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.code,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_productwidth as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_products.p_offlinedate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_detail,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_electronicshippingflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_order as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_offerexternallinkflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_swatchcolour,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_giftcardflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_startlinenumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_numbercontentunits as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_data_sheet,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_denomination as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_deliverytime as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_mobileproductflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_maxorderquantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_productlength as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_manufacturername,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_endlinenumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_thumbnails,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_minorderquantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_productheight as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_freeuspsshippingflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_ean,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_egiftcards,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_logo,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_autoshipflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_orderquantityinterval as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_others,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_egiftcardflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_galleryimages,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_products.p_onlinedate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_ltfofferflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_genders,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_productcost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_pricequantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_weight as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_erpgroupbuyer,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_ltfonlyproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_supplieralternativeaid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_ltbucksamount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_ltbucksflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_canadianproduct as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_externalredeemflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_mmsproductid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_emailtemplateid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_displaynumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_regionid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_revcategoryid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_costcenterid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_itemdescription,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_offeringid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_linecompany,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_spendcategoryid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_malocalflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_fulfillmentproductid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_typedescriptor,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_barcode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_searchable as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_productmessages,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_products.p_occasions,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_isgiftable as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_mmsrecurrent as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_products.p_allowmemberselection as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_products
 where stage_hash_hybris_products.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_products records
set @insert_date_time = getdate()
insert into s_hybris_products (
       bk_hash,
       hjmpts,
       products_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       p_manufacturer_aid,
       p_normal,
       p_number,
       p_auto_thumb_nails,
       p_erp_group_supplier,
       code,
       p_product_width,
       p_offline_date,
       p_detail,
       p_electronic_shipping_flag,
       p_order,
       p_offer_external_link_flag,
       p_swatch_colour,
       p_gift_card_flag,
       p_start_line_number,
       p_number_content_units,
       p_data_sheet,
       p_denomination,
       p_delivery_time,
       p_mobile_product_flag,
       p_max_order_quantity,
       p_product_length,
       p_manufacturer_name,
       p_end_line_number,
       p_thumb_nails,
       p_min_order_quantity,
       p_product_height,
       p_free_usps_shipping_flag,
       p_ean,
       p_e_gift_cards,
       p_logo,
       p_auto_shipflag,
       p_order_quantity_interval,
       p_others,
       p_e_gift_card_flag,
       p_gallery_images,
       p_online_date,
       p_ltf_offer_flag,
       p_genders,
       p_product_cost,
       p_price_quantity,
       p_weight,
       p_erp_group_buyer,
       p_ltf_only_product,
       p_supplier_alternative_aid,
       p_lt_bucks_amount,
       p_lt_bucks_flag,
       p_canadian_product,
       p_external_redeem_flag,
       p_mms_product_id,
       p_email_template_id,
       p_display_number,
       p_region_id,
       p_rev_category_id,
       p_cost_center_id,
       p_item_description,
       p_offering_id,
       p_line_company,
       p_spend_category_id,
       p_ma_local_flag,
       p_fulfillment_product_id,
       p_type_descriptor,
       p_barcode,
       p_searchable,
       p_product_messages,
       p_occasions,
       p_is_giftable,
       p_mms_recurrent,
       p_allow_member_selection,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_products_inserts.bk_hash,
       #s_hybris_products_inserts.hjmpts,
       #s_hybris_products_inserts.products_pk,
       #s_hybris_products_inserts.created_ts,
       #s_hybris_products_inserts.modified_ts,
       #s_hybris_products_inserts.acl_ts,
       #s_hybris_products_inserts.prop_ts,
       #s_hybris_products_inserts.p_manufacturer_aid,
       #s_hybris_products_inserts.p_normal,
       #s_hybris_products_inserts.p_number,
       #s_hybris_products_inserts.p_auto_thumb_nails,
       #s_hybris_products_inserts.p_erp_group_supplier,
       #s_hybris_products_inserts.code,
       #s_hybris_products_inserts.p_product_width,
       #s_hybris_products_inserts.p_offline_date,
       #s_hybris_products_inserts.p_detail,
       #s_hybris_products_inserts.p_electronic_shipping_flag,
       #s_hybris_products_inserts.p_order,
       #s_hybris_products_inserts.p_offer_external_link_flag,
       #s_hybris_products_inserts.p_swatch_colour,
       #s_hybris_products_inserts.p_gift_card_flag,
       #s_hybris_products_inserts.p_start_line_number,
       #s_hybris_products_inserts.p_number_content_units,
       #s_hybris_products_inserts.p_data_sheet,
       #s_hybris_products_inserts.p_denomination,
       #s_hybris_products_inserts.p_delivery_time,
       #s_hybris_products_inserts.p_mobile_product_flag,
       #s_hybris_products_inserts.p_max_order_quantity,
       #s_hybris_products_inserts.p_product_length,
       #s_hybris_products_inserts.p_manufacturer_name,
       #s_hybris_products_inserts.p_end_line_number,
       #s_hybris_products_inserts.p_thumb_nails,
       #s_hybris_products_inserts.p_min_order_quantity,
       #s_hybris_products_inserts.p_product_height,
       #s_hybris_products_inserts.p_free_usps_shipping_flag,
       #s_hybris_products_inserts.p_ean,
       #s_hybris_products_inserts.p_e_gift_cards,
       #s_hybris_products_inserts.p_logo,
       #s_hybris_products_inserts.p_auto_shipflag,
       #s_hybris_products_inserts.p_order_quantity_interval,
       #s_hybris_products_inserts.p_others,
       #s_hybris_products_inserts.p_e_gift_card_flag,
       #s_hybris_products_inserts.p_gallery_images,
       #s_hybris_products_inserts.p_online_date,
       #s_hybris_products_inserts.p_ltf_offer_flag,
       #s_hybris_products_inserts.p_genders,
       #s_hybris_products_inserts.p_product_cost,
       #s_hybris_products_inserts.p_price_quantity,
       #s_hybris_products_inserts.p_weight,
       #s_hybris_products_inserts.p_erp_group_buyer,
       #s_hybris_products_inserts.p_ltf_only_product,
       #s_hybris_products_inserts.p_supplier_alternative_aid,
       #s_hybris_products_inserts.p_lt_bucks_amount,
       #s_hybris_products_inserts.p_lt_bucks_flag,
       #s_hybris_products_inserts.p_canadian_product,
       #s_hybris_products_inserts.p_external_redeem_flag,
       #s_hybris_products_inserts.p_mms_product_id,
       #s_hybris_products_inserts.p_email_template_id,
       #s_hybris_products_inserts.p_display_number,
       #s_hybris_products_inserts.p_region_id,
       #s_hybris_products_inserts.p_rev_category_id,
       #s_hybris_products_inserts.p_cost_center_id,
       #s_hybris_products_inserts.p_item_description,
       #s_hybris_products_inserts.p_offering_id,
       #s_hybris_products_inserts.p_line_company,
       #s_hybris_products_inserts.p_spend_category_id,
       #s_hybris_products_inserts.p_ma_local_flag,
       #s_hybris_products_inserts.p_fulfillment_product_id,
       #s_hybris_products_inserts.p_type_descriptor,
       #s_hybris_products_inserts.p_barcode,
       #s_hybris_products_inserts.p_searchable,
       #s_hybris_products_inserts.p_product_messages,
       #s_hybris_products_inserts.p_occasions,
       #s_hybris_products_inserts.p_is_giftable,
       #s_hybris_products_inserts.p_mms_recurrent,
       #s_hybris_products_inserts.p_allow_member_selection,
       case when s_hybris_products.s_hybris_products_id is null then isnull(#s_hybris_products_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_products_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_products_inserts
  left join p_hybris_products
    on #s_hybris_products_inserts.bk_hash = p_hybris_products.bk_hash
   and p_hybris_products.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_products
    on p_hybris_products.bk_hash = s_hybris_products.bk_hash
   and p_hybris_products.s_hybris_products_id = s_hybris_products.s_hybris_products_id
 where s_hybris_products.s_hybris_products_id is null
    or (s_hybris_products.s_hybris_products_id is not null
        and s_hybris_products.dv_hash <> #s_hybris_products_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_products @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_hybris_products @current_dv_batch_id

end
