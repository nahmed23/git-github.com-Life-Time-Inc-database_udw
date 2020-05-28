CREATE PROC [dbo].[proc_etl_hybris_base_store] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_basestore

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_basestore (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_uid,
       p_storelocatordistanceunit,
       p_net,
       p_taxgroup,
       p_defaultlanguage,
       p_defaultcurrency,
       p_defaultdeliveryorigin,
       p_solrfacetsearchconfiguration,
       p_submitorderprocesscode,
       p_createreturnprocesscode,
       p_externaltaxenabled,
       p_pickupinstoremode,
       p_maxradiusforpossearch,
       p_customerallowedtoignoresugge,
       p_paymentprovider,
       p_expresscheckoutenabled,
       p_taxestimationenabled,
       p_checkoutflowgroup,
       p_defaultatpformula,
       p_sourcingconfig,
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
       p_uid,
       p_storelocatordistanceunit,
       p_net,
       p_taxgroup,
       p_defaultlanguage,
       p_defaultcurrency,
       p_defaultdeliveryorigin,
       p_solrfacetsearchconfiguration,
       p_submitorderprocesscode,
       p_createreturnprocesscode,
       p_externaltaxenabled,
       p_pickupinstoremode,
       p_maxradiusforpossearch,
       p_customerallowedtoignoresugge,
       p_paymentprovider,
       p_expresscheckoutenabled,
       p_taxestimationenabled,
       p_checkoutflowgroup,
       p_defaultatpformula,
       p_sourcingconfig,
       aCLTS,
       propTS,
       isnull(cast(stage_hybris_basestore.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_basestore
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_base_store @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_base_store (
       bk_hash,
       base_store_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_basestore.bk_hash,
       stage_hash_hybris_basestore.[PK] base_store_pk,
       isnull(cast(stage_hash_hybris_basestore.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_basestore
  left join h_hybris_base_store
    on stage_hash_hybris_basestore.bk_hash = h_hybris_base_store.bk_hash
 where h_hybris_base_store_id is null
   and stage_hash_hybris_basestore.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_base_store
if object_id('tempdb..#l_hybris_base_store_inserts') is not null drop table #l_hybris_base_store_inserts
create table #l_hybris_base_store_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_basestore.bk_hash,
       stage_hash_hybris_basestore.TypePkString type_pk_string,
       stage_hash_hybris_basestore.OwnerPkString owner_pk_string,
       stage_hash_hybris_basestore.[PK] base_store_pk,
       stage_hash_hybris_basestore.p_storelocatordistanceunit p_store_locator_distance_unit,
       stage_hash_hybris_basestore.p_taxgroup p_tax_group,
       stage_hash_hybris_basestore.p_defaultlanguage p_default_language,
       stage_hash_hybris_basestore.p_defaultcurrency p_default_currency,
       stage_hash_hybris_basestore.p_defaultdeliveryorigin p_default_delivery_origin,
       stage_hash_hybris_basestore.p_solrfacetsearchconfiguration p_solr_facet_search_configuration,
       stage_hash_hybris_basestore.p_pickupinstoremode p_pickup_in_store_mode,
       stage_hash_hybris_basestore.p_defaultatpformula p_default_atp_formula,
       stage_hash_hybris_basestore.p_sourcingconfig p_sourcing_config,
       stage_hash_hybris_basestore.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_storelocatordistanceunit as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_taxgroup as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_defaultlanguage as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_defaultcurrency as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_defaultdeliveryorigin as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_solrfacetsearchconfiguration as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_pickupinstoremode as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_defaultatpformula as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_sourcingconfig as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_basestore
 where stage_hash_hybris_basestore.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_base_store records
set @insert_date_time = getdate()
insert into l_hybris_base_store (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       base_store_pk,
       p_store_locator_distance_unit,
       p_tax_group,
       p_default_language,
       p_default_currency,
       p_default_delivery_origin,
       p_solr_facet_search_configuration,
       p_pickup_in_store_mode,
       p_default_atp_formula,
       p_sourcing_config,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_base_store_inserts.bk_hash,
       #l_hybris_base_store_inserts.type_pk_string,
       #l_hybris_base_store_inserts.owner_pk_string,
       #l_hybris_base_store_inserts.base_store_pk,
       #l_hybris_base_store_inserts.p_store_locator_distance_unit,
       #l_hybris_base_store_inserts.p_tax_group,
       #l_hybris_base_store_inserts.p_default_language,
       #l_hybris_base_store_inserts.p_default_currency,
       #l_hybris_base_store_inserts.p_default_delivery_origin,
       #l_hybris_base_store_inserts.p_solr_facet_search_configuration,
       #l_hybris_base_store_inserts.p_pickup_in_store_mode,
       #l_hybris_base_store_inserts.p_default_atp_formula,
       #l_hybris_base_store_inserts.p_sourcing_config,
       case when l_hybris_base_store.l_hybris_base_store_id is null then isnull(#l_hybris_base_store_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_base_store_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_base_store_inserts
  left join p_hybris_base_store
    on #l_hybris_base_store_inserts.bk_hash = p_hybris_base_store.bk_hash
   and p_hybris_base_store.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_base_store
    on p_hybris_base_store.bk_hash = l_hybris_base_store.bk_hash
   and p_hybris_base_store.l_hybris_base_store_id = l_hybris_base_store.l_hybris_base_store_id
 where l_hybris_base_store.l_hybris_base_store_id is null
    or (l_hybris_base_store.l_hybris_base_store_id is not null
        and l_hybris_base_store.dv_hash <> #l_hybris_base_store_inserts.source_hash)

--calculate hash and lookup to current s_hybris_base_store
if object_id('tempdb..#s_hybris_base_store_inserts') is not null drop table #s_hybris_base_store_inserts
create table #s_hybris_base_store_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_basestore.bk_hash,
       stage_hash_hybris_basestore.hjmpTS hjmpts,
       stage_hash_hybris_basestore.createdTS created_ts,
       stage_hash_hybris_basestore.modifiedTS modified_ts,
       stage_hash_hybris_basestore.[PK] base_store_pk,
       stage_hash_hybris_basestore.p_uid p_uid,
       stage_hash_hybris_basestore.p_net p_net,
       stage_hash_hybris_basestore.p_submitorderprocesscode p_submit_order_process_code,
       stage_hash_hybris_basestore.p_createreturnprocesscode p_create_return_process_code,
       stage_hash_hybris_basestore.p_externaltaxenabled p_external_tax_enabled,
       stage_hash_hybris_basestore.p_maxradiusforpossearch p_max_radius_for_pos_search,
       stage_hash_hybris_basestore.p_customerallowedtoignoresugge p_customer_allowed_to_ignore_sugge,
       stage_hash_hybris_basestore.p_paymentprovider p_payment_provider,
       stage_hash_hybris_basestore.p_expresscheckoutenabled p_express_checkout_enabled,
       stage_hash_hybris_basestore.p_taxestimationenabled p_tax_estimation_enabled,
       stage_hash_hybris_basestore.p_checkoutflowgroup p_checkout_flow_group,
       stage_hash_hybris_basestore.aCLTS acl_ts,
       stage_hash_hybris_basestore.propTS prop_ts,
       stage_hash_hybris_basestore.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_basestore.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_basestore.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_basestore.p_uid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_net as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_basestore.p_submitorderprocesscode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_basestore.p_createreturnprocesscode,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_externaltaxenabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_maxradiusforpossearch as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_customerallowedtoignoresugge as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_basestore.p_paymentprovider,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_expresscheckoutenabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.p_taxestimationenabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_basestore.p_checkoutflowgroup,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_basestore.propTS as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_basestore
 where stage_hash_hybris_basestore.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_base_store records
set @insert_date_time = getdate()
insert into s_hybris_base_store (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       base_store_pk,
       p_uid,
       p_net,
       p_submit_order_process_code,
       p_create_return_process_code,
       p_external_tax_enabled,
       p_max_radius_for_pos_search,
       p_customer_allowed_to_ignore_sugge,
       p_payment_provider,
       p_express_checkout_enabled,
       p_tax_estimation_enabled,
       p_checkout_flow_group,
       acl_ts,
       prop_ts,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_base_store_inserts.bk_hash,
       #s_hybris_base_store_inserts.hjmpts,
       #s_hybris_base_store_inserts.created_ts,
       #s_hybris_base_store_inserts.modified_ts,
       #s_hybris_base_store_inserts.base_store_pk,
       #s_hybris_base_store_inserts.p_uid,
       #s_hybris_base_store_inserts.p_net,
       #s_hybris_base_store_inserts.p_submit_order_process_code,
       #s_hybris_base_store_inserts.p_create_return_process_code,
       #s_hybris_base_store_inserts.p_external_tax_enabled,
       #s_hybris_base_store_inserts.p_max_radius_for_pos_search,
       #s_hybris_base_store_inserts.p_customer_allowed_to_ignore_sugge,
       #s_hybris_base_store_inserts.p_payment_provider,
       #s_hybris_base_store_inserts.p_express_checkout_enabled,
       #s_hybris_base_store_inserts.p_tax_estimation_enabled,
       #s_hybris_base_store_inserts.p_checkout_flow_group,
       #s_hybris_base_store_inserts.acl_ts,
       #s_hybris_base_store_inserts.prop_ts,
       case when s_hybris_base_store.s_hybris_base_store_id is null then isnull(#s_hybris_base_store_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_base_store_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_base_store_inserts
  left join p_hybris_base_store
    on #s_hybris_base_store_inserts.bk_hash = p_hybris_base_store.bk_hash
   and p_hybris_base_store.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_base_store
    on p_hybris_base_store.bk_hash = s_hybris_base_store.bk_hash
   and p_hybris_base_store.s_hybris_base_store_id = s_hybris_base_store.s_hybris_base_store_id
 where s_hybris_base_store.s_hybris_base_store_id is null
    or (s_hybris_base_store.s_hybris_base_store_id is not null
        and s_hybris_base_store.dv_hash <> #s_hybris_base_store_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_base_store @current_dv_batch_id

end
