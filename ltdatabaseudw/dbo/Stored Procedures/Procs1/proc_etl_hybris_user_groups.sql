CREATE PROC [dbo].[proc_etl_hybris_user_groups] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_usergroups

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_usergroups (
       bk_hash,
       hjmpTS,
       createdTS,
       modifiedTS,
       TypePkString,
       OwnerPkString,
       [PK],
       p_description,
       p_name,
       p_uid,
       p_profilepicture,
       p_backofficelogindisabled,
       p_ldapsearchbase,
       p_dn,
       p_cn,
       p_maxbruteforceloginattempts,
       p_writeablelanguages,
       p_readablelanguages,
       p_userdiscountgroup,
       p_userpricegroup,
       p_usertaxgroup,
       p_hmclogindisabled,
       aCLTS,
       propTS,
       p_dunsid,
       p_ilnid,
       p_buyerspecificid,
       p_id,
       p_supplierspecificid,
       p_medias,
       p_shippingaddress,
       p_unloadingaddress,
       p_billingaddress,
       p_contactaddress,
       p_contact,
       p_vatid,
       p_responsiblecompany,
       p_country,
       p_lineofbuisness,
       p_buyer,
       p_supplier,
       p_manufacturer,
       p_carrier,
       p_active,
       p_implementationtype,
       p_priority,
       p_store,
       p_emaildistributionlist,
       p_defaultassignee,
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
       p_description,
       p_name,
       p_uid,
       p_profilepicture,
       p_backofficelogindisabled,
       p_ldapsearchbase,
       p_dn,
       p_cn,
       p_maxbruteforceloginattempts,
       p_writeablelanguages,
       p_readablelanguages,
       p_userdiscountgroup,
       p_userpricegroup,
       p_usertaxgroup,
       p_hmclogindisabled,
       aCLTS,
       propTS,
       p_dunsid,
       p_ilnid,
       p_buyerspecificid,
       p_id,
       p_supplierspecificid,
       p_medias,
       p_shippingaddress,
       p_unloadingaddress,
       p_billingaddress,
       p_contactaddress,
       p_contact,
       p_vatid,
       p_responsiblecompany,
       p_country,
       p_lineofbuisness,
       p_buyer,
       p_supplier,
       p_manufacturer,
       p_carrier,
       p_active,
       p_implementationtype,
       p_priority,
       p_store,
       p_emaildistributionlist,
       p_defaultassignee,
       isnull(cast(stage_hybris_usergroups.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_usergroups
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_user_groups @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_user_groups (
       bk_hash,
       user_groups_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_usergroups.bk_hash,
       stage_hash_hybris_usergroups.[PK] user_groups_pk,
       isnull(cast(stage_hash_hybris_usergroups.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_usergroups
  left join h_hybris_user_groups
    on stage_hash_hybris_usergroups.bk_hash = h_hybris_user_groups.bk_hash
 where h_hybris_user_groups_id is null
   and stage_hash_hybris_usergroups.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_user_groups
if object_id('tempdb..#l_hybris_user_groups_inserts') is not null drop table #l_hybris_user_groups_inserts
create table #l_hybris_user_groups_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_usergroups.bk_hash,
       stage_hash_hybris_usergroups.TypePkString type_pk_string,
       stage_hash_hybris_usergroups.OwnerPkString owner_pk_string,
       stage_hash_hybris_usergroups.[PK] user_groups_pk,
       stage_hash_hybris_usergroups.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.[PK] as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_usergroups
 where stage_hash_hybris_usergroups.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_user_groups records
set @insert_date_time = getdate()
insert into l_hybris_user_groups (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       user_groups_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_user_groups_inserts.bk_hash,
       #l_hybris_user_groups_inserts.type_pk_string,
       #l_hybris_user_groups_inserts.owner_pk_string,
       #l_hybris_user_groups_inserts.user_groups_pk,
       case when l_hybris_user_groups.l_hybris_user_groups_id is null then isnull(#l_hybris_user_groups_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_user_groups_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_user_groups_inserts
  left join p_hybris_user_groups
    on #l_hybris_user_groups_inserts.bk_hash = p_hybris_user_groups.bk_hash
   and p_hybris_user_groups.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_user_groups
    on p_hybris_user_groups.bk_hash = l_hybris_user_groups.bk_hash
   and p_hybris_user_groups.l_hybris_user_groups_id = l_hybris_user_groups.l_hybris_user_groups_id
 where l_hybris_user_groups.l_hybris_user_groups_id is null
    or (l_hybris_user_groups.l_hybris_user_groups_id is not null
        and l_hybris_user_groups.dv_hash <> #l_hybris_user_groups_inserts.source_hash)

--calculate hash and lookup to current s_hybris_user_groups
if object_id('tempdb..#s_hybris_user_groups_inserts') is not null drop table #s_hybris_user_groups_inserts
create table #s_hybris_user_groups_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_usergroups.bk_hash,
       stage_hash_hybris_usergroups.hjmpTS hjmpts,
       stage_hash_hybris_usergroups.createdTS created_ts,
       stage_hash_hybris_usergroups.modifiedTS modified_ts,
       stage_hash_hybris_usergroups.[PK] user_groups_pk,
       stage_hash_hybris_usergroups.p_description p_description,
       stage_hash_hybris_usergroups.p_name p_name,
       stage_hash_hybris_usergroups.p_uid p_uid,
       stage_hash_hybris_usergroups.p_profilepicture p_profile_picture,
       stage_hash_hybris_usergroups.p_backofficelogindisabled p_back_office_login_disabled,
       stage_hash_hybris_usergroups.p_ldapsearchbase p_ldap_search_base,
       stage_hash_hybris_usergroups.p_dn p_dn,
       stage_hash_hybris_usergroups.p_cn p_cn,
       stage_hash_hybris_usergroups.p_maxbruteforceloginattempts p_max_brute_force_login_attempts,
       stage_hash_hybris_usergroups.p_writeablelanguages p_writeable_languages,
       stage_hash_hybris_usergroups.p_readablelanguages p_readable_languages,
       stage_hash_hybris_usergroups.p_userdiscountgroup p_user_discount_group,
       stage_hash_hybris_usergroups.p_userpricegroup p_user_price_group,
       stage_hash_hybris_usergroups.p_usertaxgroup p_user_tax_group,
       stage_hash_hybris_usergroups.p_hmclogindisabled p_hmc_login_disabled,
       stage_hash_hybris_usergroups.aCLTS acl_ts,
       stage_hash_hybris_usergroups.propTS prop_ts,
       stage_hash_hybris_usergroups.p_dunsid p_duns_id,
       stage_hash_hybris_usergroups.p_ilnid p_iln_id,
       stage_hash_hybris_usergroups.p_buyerspecificid p_buyer_specific_id,
       stage_hash_hybris_usergroups.p_id p_id,
       stage_hash_hybris_usergroups.p_supplierspecificid p_supplier_specific_id,
       stage_hash_hybris_usergroups.p_medias p_medias,
       stage_hash_hybris_usergroups.p_shippingaddress p_shipping_address,
       stage_hash_hybris_usergroups.p_unloadingaddress p_unloading_address,
       stage_hash_hybris_usergroups.p_billingaddress p_billing_address,
       stage_hash_hybris_usergroups.p_contactaddress p_contact_address,
       stage_hash_hybris_usergroups.p_contact p_contact,
       stage_hash_hybris_usergroups.p_vatid p_vat_id,
       stage_hash_hybris_usergroups.p_responsiblecompany p_responsible_company,
       stage_hash_hybris_usergroups.p_country p_country,
       stage_hash_hybris_usergroups.p_lineofbuisness p_line_of_buisness,
       stage_hash_hybris_usergroups.p_buyer p_buyer,
       stage_hash_hybris_usergroups.p_supplier p_supplier,
       stage_hash_hybris_usergroups.p_manufacturer p_manufacturer,
       stage_hash_hybris_usergroups.p_carrier p_carrier,
       stage_hash_hybris_usergroups.p_active p_active,
       stage_hash_hybris_usergroups.p_implementationtype p_implementation_type,
       stage_hash_hybris_usergroups.p_priority p_priority,
       stage_hash_hybris_usergroups.p_store p_store,
       stage_hash_hybris_usergroups.p_emaildistributionlist p_email_distribution_list,
       stage_hash_hybris_usergroups.p_defaultassignee p_default_assignee,
       stage_hash_hybris_usergroups.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_usergroups.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_usergroups.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_uid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_profilepicture as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_backofficelogindisabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_ldapsearchbase,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_dn,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_cn,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_maxbruteforceloginattempts as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_writeablelanguages,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_readablelanguages,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_userdiscountgroup as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_userpricegroup as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_usertaxgroup as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_hmclogindisabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_dunsid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_ilnid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_buyerspecificid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_id,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_supplierspecificid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_medias,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_shippingaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_unloadingaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_billingaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_contactaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_contact as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_vatid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_responsiblecompany as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_country as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_lineofbuisness as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_buyer as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_supplier as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_manufacturer as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_carrier as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_active as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_implementationtype,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_priority as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_store as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_usergroups.p_emaildistributionlist,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_usergroups.p_defaultassignee as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_usergroups
 where stage_hash_hybris_usergroups.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_user_groups records
set @insert_date_time = getdate()
insert into s_hybris_user_groups (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       user_groups_pk,
       p_description,
       p_name,
       p_uid,
       p_profile_picture,
       p_back_office_login_disabled,
       p_ldap_search_base,
       p_dn,
       p_cn,
       p_max_brute_force_login_attempts,
       p_writeable_languages,
       p_readable_languages,
       p_user_discount_group,
       p_user_price_group,
       p_user_tax_group,
       p_hmc_login_disabled,
       acl_ts,
       prop_ts,
       p_duns_id,
       p_iln_id,
       p_buyer_specific_id,
       p_id,
       p_supplier_specific_id,
       p_medias,
       p_shipping_address,
       p_unloading_address,
       p_billing_address,
       p_contact_address,
       p_contact,
       p_vat_id,
       p_responsible_company,
       p_country,
       p_line_of_buisness,
       p_buyer,
       p_supplier,
       p_manufacturer,
       p_carrier,
       p_active,
       p_implementation_type,
       p_priority,
       p_store,
       p_email_distribution_list,
       p_default_assignee,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_user_groups_inserts.bk_hash,
       #s_hybris_user_groups_inserts.hjmpts,
       #s_hybris_user_groups_inserts.created_ts,
       #s_hybris_user_groups_inserts.modified_ts,
       #s_hybris_user_groups_inserts.user_groups_pk,
       #s_hybris_user_groups_inserts.p_description,
       #s_hybris_user_groups_inserts.p_name,
       #s_hybris_user_groups_inserts.p_uid,
       #s_hybris_user_groups_inserts.p_profile_picture,
       #s_hybris_user_groups_inserts.p_back_office_login_disabled,
       #s_hybris_user_groups_inserts.p_ldap_search_base,
       #s_hybris_user_groups_inserts.p_dn,
       #s_hybris_user_groups_inserts.p_cn,
       #s_hybris_user_groups_inserts.p_max_brute_force_login_attempts,
       #s_hybris_user_groups_inserts.p_writeable_languages,
       #s_hybris_user_groups_inserts.p_readable_languages,
       #s_hybris_user_groups_inserts.p_user_discount_group,
       #s_hybris_user_groups_inserts.p_user_price_group,
       #s_hybris_user_groups_inserts.p_user_tax_group,
       #s_hybris_user_groups_inserts.p_hmc_login_disabled,
       #s_hybris_user_groups_inserts.acl_ts,
       #s_hybris_user_groups_inserts.prop_ts,
       #s_hybris_user_groups_inserts.p_duns_id,
       #s_hybris_user_groups_inserts.p_iln_id,
       #s_hybris_user_groups_inserts.p_buyer_specific_id,
       #s_hybris_user_groups_inserts.p_id,
       #s_hybris_user_groups_inserts.p_supplier_specific_id,
       #s_hybris_user_groups_inserts.p_medias,
       #s_hybris_user_groups_inserts.p_shipping_address,
       #s_hybris_user_groups_inserts.p_unloading_address,
       #s_hybris_user_groups_inserts.p_billing_address,
       #s_hybris_user_groups_inserts.p_contact_address,
       #s_hybris_user_groups_inserts.p_contact,
       #s_hybris_user_groups_inserts.p_vat_id,
       #s_hybris_user_groups_inserts.p_responsible_company,
       #s_hybris_user_groups_inserts.p_country,
       #s_hybris_user_groups_inserts.p_line_of_buisness,
       #s_hybris_user_groups_inserts.p_buyer,
       #s_hybris_user_groups_inserts.p_supplier,
       #s_hybris_user_groups_inserts.p_manufacturer,
       #s_hybris_user_groups_inserts.p_carrier,
       #s_hybris_user_groups_inserts.p_active,
       #s_hybris_user_groups_inserts.p_implementation_type,
       #s_hybris_user_groups_inserts.p_priority,
       #s_hybris_user_groups_inserts.p_store,
       #s_hybris_user_groups_inserts.p_email_distribution_list,
       #s_hybris_user_groups_inserts.p_default_assignee,
       case when s_hybris_user_groups.s_hybris_user_groups_id is null then isnull(#s_hybris_user_groups_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_user_groups_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_user_groups_inserts
  left join p_hybris_user_groups
    on #s_hybris_user_groups_inserts.bk_hash = p_hybris_user_groups.bk_hash
   and p_hybris_user_groups.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_user_groups
    on p_hybris_user_groups.bk_hash = s_hybris_user_groups.bk_hash
   and p_hybris_user_groups.s_hybris_user_groups_id = s_hybris_user_groups.s_hybris_user_groups_id
 where s_hybris_user_groups.s_hybris_user_groups_id is null
    or (s_hybris_user_groups.s_hybris_user_groups_id is not null
        and s_hybris_user_groups.dv_hash <> #s_hybris_user_groups_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_user_groups @current_dv_batch_id

end
