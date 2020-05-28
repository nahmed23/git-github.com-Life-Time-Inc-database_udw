CREATE PROC [dbo].[proc_etl_hybris_order_entries] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_orderentries

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_orderentries (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_baseprice,
       p_calculated,
       p_discountvaluesinternal,
       p_entrynumber,
       p_info,
       p_product,
       p_quantity,
       p_taxvaluesinternal,
       p_totalprice,
       p_unit,
       p_giveaway,
       p_rejected,
       p_order,
       p_europe1pricefactory_ppg,
       p_europe1pricefactory_ptg,
       p_europe1pricefactory_pdg,
       p_chosenvendor,
       p_deliveryaddress,
       p_deliverymode,
       p_nameddeliverydate,
       p_quantitystatus,
       p_deliverypointofservice,
       p_xmlproduct,
       p_originalsubscriptionid,
       p_originalorderentry,
       p_masterentry,
       p_bundleno,
       p_bundletemplate,
       aCLTS,
       propTS,
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
       p_baseprice,
       p_calculated,
       p_discountvaluesinternal,
       p_entrynumber,
       p_info,
       p_product,
       p_quantity,
       p_taxvaluesinternal,
       p_totalprice,
       p_unit,
       p_giveaway,
       p_rejected,
       p_order,
       p_europe1pricefactory_ppg,
       p_europe1pricefactory_ptg,
       p_europe1pricefactory_pdg,
       p_chosenvendor,
       p_deliveryaddress,
       p_deliverymode,
       p_nameddeliverydate,
       p_quantitystatus,
       p_deliverypointofservice,
       p_xmlproduct,
       p_originalsubscriptionid,
       p_originalorderentry,
       p_masterentry,
       p_bundleno,
       p_bundletemplate,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_orderentries.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_orderentries
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_order_entries @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_order_entries (
       bk_hash,
       order_entries_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_orderentries.bk_hash,
       stage_hash_hybris_orderentries.[PK] order_entries_pk,
       isnull(cast(stage_hash_hybris_orderentries.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_orderentries
  left join h_hybris_order_entries
    on stage_hash_hybris_orderentries.bk_hash = h_hybris_order_entries.bk_hash
 where h_hybris_order_entries_id is null
   and stage_hash_hybris_orderentries.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_order_entries
if object_id('tempdb..#l_hybris_order_entries_inserts') is not null drop table #l_hybris_order_entries_inserts
create table #l_hybris_order_entries_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_orderentries.bk_hash,
       stage_hash_hybris_orderentries.TypePkString type_pk_string,
       stage_hash_hybris_orderentries.OwnerPkString owner_pk_string,
       stage_hash_hybris_orderentries.[PK] order_entries_pk,
       stage_hash_hybris_orderentries.p_product p_product,
       stage_hash_hybris_orderentries.p_unit p_unit,
       stage_hash_hybris_orderentries.p_order p_order,
       stage_hash_hybris_orderentries.p_europe1pricefactory_ppg p_europe_1_price_factory_ppg,
       stage_hash_hybris_orderentries.p_europe1pricefactory_ptg p_europe_1_price_factory_ptg,
       stage_hash_hybris_orderentries.p_europe1pricefactory_pdg p_europe_1_price_factory_pdg,
       stage_hash_hybris_orderentries.p_chosenvendor p_chosen_vendor,
       stage_hash_hybris_orderentries.p_deliveryaddress p_delivery_address,
       stage_hash_hybris_orderentries.p_deliverymode p_delivery_mode,
       stage_hash_hybris_orderentries.p_quantitystatus p_quantity_status,
       stage_hash_hybris_orderentries.p_deliverypointofservice p_delivery_point_of_service,
       stage_hash_hybris_orderentries.p_originalorderentry p_original_order_entry,
       stage_hash_hybris_orderentries.p_masterentry p_master_entry,
       stage_hash_hybris_orderentries.p_bundletemplate p_bundle_template,
       stage_hash_hybris_orderentries.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_product as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_unit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_order as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_europe1pricefactory_ppg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_europe1pricefactory_ptg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_europe1pricefactory_pdg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_chosenvendor as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_deliveryaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_deliverymode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_quantitystatus as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_deliverypointofservice as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_originalorderentry as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_masterentry as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_bundletemplate as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_orderentries
 where stage_hash_hybris_orderentries.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_order_entries records
set @insert_date_time = getdate()
insert into l_hybris_order_entries (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       order_entries_pk,
       p_product,
       p_unit,
       p_order,
       p_europe_1_price_factory_ppg,
       p_europe_1_price_factory_ptg,
       p_europe_1_price_factory_pdg,
       p_chosen_vendor,
       p_delivery_address,
       p_delivery_mode,
       p_quantity_status,
       p_delivery_point_of_service,
       p_original_order_entry,
       p_master_entry,
       p_bundle_template,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_order_entries_inserts.bk_hash,
       #l_hybris_order_entries_inserts.type_pk_string,
       #l_hybris_order_entries_inserts.owner_pk_string,
       #l_hybris_order_entries_inserts.order_entries_pk,
       #l_hybris_order_entries_inserts.p_product,
       #l_hybris_order_entries_inserts.p_unit,
       #l_hybris_order_entries_inserts.p_order,
       #l_hybris_order_entries_inserts.p_europe_1_price_factory_ppg,
       #l_hybris_order_entries_inserts.p_europe_1_price_factory_ptg,
       #l_hybris_order_entries_inserts.p_europe_1_price_factory_pdg,
       #l_hybris_order_entries_inserts.p_chosen_vendor,
       #l_hybris_order_entries_inserts.p_delivery_address,
       #l_hybris_order_entries_inserts.p_delivery_mode,
       #l_hybris_order_entries_inserts.p_quantity_status,
       #l_hybris_order_entries_inserts.p_delivery_point_of_service,
       #l_hybris_order_entries_inserts.p_original_order_entry,
       #l_hybris_order_entries_inserts.p_master_entry,
       #l_hybris_order_entries_inserts.p_bundle_template,
       case when l_hybris_order_entries.l_hybris_order_entries_id is null then isnull(#l_hybris_order_entries_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_order_entries_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_order_entries_inserts
  left join p_hybris_order_entries
    on #l_hybris_order_entries_inserts.bk_hash = p_hybris_order_entries.bk_hash
   and p_hybris_order_entries.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_order_entries
    on p_hybris_order_entries.bk_hash = l_hybris_order_entries.bk_hash
   and p_hybris_order_entries.l_hybris_order_entries_id = l_hybris_order_entries.l_hybris_order_entries_id
 where l_hybris_order_entries.l_hybris_order_entries_id is null
    or (l_hybris_order_entries.l_hybris_order_entries_id is not null
        and l_hybris_order_entries.dv_hash <> #l_hybris_order_entries_inserts.source_hash)

--calculate hash and lookup to current s_hybris_order_entries
if object_id('tempdb..#s_hybris_order_entries_inserts') is not null drop table #s_hybris_order_entries_inserts
create table #s_hybris_order_entries_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_orderentries.bk_hash,
       stage_hash_hybris_orderentries.hjmpTS hjmpts,
       stage_hash_hybris_orderentries.createdTS created_ts,
       stage_hash_hybris_orderentries.modifiedTS modified_ts,
       stage_hash_hybris_orderentries.[PK] order_entries_pk,
       stage_hash_hybris_orderentries.p_baseprice p_base_price,
       stage_hash_hybris_orderentries.p_calculated p_calculated,
       stage_hash_hybris_orderentries.p_discountvaluesinternal p_discount_values_internal,
       stage_hash_hybris_orderentries.p_entrynumber p_entry_number,
       stage_hash_hybris_orderentries.p_info p_info,
       stage_hash_hybris_orderentries.p_quantity p_quantity,
       stage_hash_hybris_orderentries.p_taxvaluesinternal p_tax_values_internal,
       stage_hash_hybris_orderentries.p_totalprice p_total_price,
       stage_hash_hybris_orderentries.p_giveaway p_give_away,
       stage_hash_hybris_orderentries.p_rejected p_rejected,
       stage_hash_hybris_orderentries.p_nameddeliverydate p_named_delivery_date,
       stage_hash_hybris_orderentries.p_xmlproduct p_xml_product,
       stage_hash_hybris_orderentries.p_originalsubscriptionid p_original_subscription_id,
       stage_hash_hybris_orderentries.p_bundleno p_bundle_no,
       stage_hash_hybris_orderentries.aCLTS acl_ts,
       stage_hash_hybris_orderentries.propTS prop_ts,
       stage_hash_hybris_orderentries.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_orderentries.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_orderentries.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_baseprice as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_calculated as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orderentries.p_discountvaluesinternal,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_entrynumber as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orderentries.p_info,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_quantity as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orderentries.p_taxvaluesinternal,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_totalprice as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_giveaway as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_rejected as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_orderentries.p_nameddeliverydate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orderentries.p_xmlproduct,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_orderentries.p_originalsubscriptionid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.p_bundleno as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_orderentries.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_orderentries
 where stage_hash_hybris_orderentries.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_order_entries records
set @insert_date_time = getdate()
insert into s_hybris_order_entries (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       order_entries_pk,
       p_base_price,
       p_calculated,
       p_discount_values_internal,
       p_entry_number,
       p_info,
       p_quantity,
       p_tax_values_internal,
       p_total_price,
       p_give_away,
       p_rejected,
       p_named_delivery_date,
       p_xml_product,
       p_original_subscription_id,
       p_bundle_no,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_order_entries_inserts.bk_hash,
       #s_hybris_order_entries_inserts.hjmpts,
       #s_hybris_order_entries_inserts.created_ts,
       #s_hybris_order_entries_inserts.modified_ts,
       #s_hybris_order_entries_inserts.order_entries_pk,
       #s_hybris_order_entries_inserts.p_base_price,
       #s_hybris_order_entries_inserts.p_calculated,
       #s_hybris_order_entries_inserts.p_discount_values_internal,
       #s_hybris_order_entries_inserts.p_entry_number,
       #s_hybris_order_entries_inserts.p_info,
       #s_hybris_order_entries_inserts.p_quantity,
       #s_hybris_order_entries_inserts.p_tax_values_internal,
       #s_hybris_order_entries_inserts.p_total_price,
       #s_hybris_order_entries_inserts.p_give_away,
       #s_hybris_order_entries_inserts.p_rejected,
       #s_hybris_order_entries_inserts.p_named_delivery_date,
       #s_hybris_order_entries_inserts.p_xml_product,
       #s_hybris_order_entries_inserts.p_original_subscription_id,
       #s_hybris_order_entries_inserts.p_bundle_no,
       #s_hybris_order_entries_inserts.acl_ts,
       #s_hybris_order_entries_inserts.prop_ts,
       case when s_hybris_order_entries.s_hybris_order_entries_id is null then isnull(#s_hybris_order_entries_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_order_entries_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_order_entries_inserts
  left join p_hybris_order_entries
    on #s_hybris_order_entries_inserts.bk_hash = p_hybris_order_entries.bk_hash
   and p_hybris_order_entries.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_order_entries
    on p_hybris_order_entries.bk_hash = s_hybris_order_entries.bk_hash
   and p_hybris_order_entries.s_hybris_order_entries_id = s_hybris_order_entries.s_hybris_order_entries_id
 where s_hybris_order_entries.s_hybris_order_entries_id is null
    or (s_hybris_order_entries.s_hybris_order_entries_id is not null
        and s_hybris_order_entries.dv_hash <> #s_hybris_order_entries_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_order_entries @current_dv_batch_id

end
