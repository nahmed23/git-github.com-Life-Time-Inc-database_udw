CREATE PROC [dbo].[proc_etl_hybris_users] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_users

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_users (
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
       p_defaultpaymentaddress,
       p_defaultshipmentaddress,
       p_sessionlanguage,
       p_sessioncurrency,
       p_logindisabled,
       p_lastlogin,
       p_hmclogindisabled,
       p_userprofile,
       p_europe1pricefactory_udg,
       p_europe1pricefactory_upg,
       p_europe1pricefactory_utg,
       p_ldapaccount,
       p_domain,
       p_ldaplogin,
       p_authorizedtounlockpages,
       p_ltfpartyid,
       p_memberid,
       p_membershipid,
       aCLTS,
       propTS,
       p_customerid,
       p_previewcatalogversions,
       p_title,
       p_defaultpaymentinfo,
       p_token,
       p_originaluid,
       p_type,
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
       p_defaultpaymentaddress,
       p_defaultshipmentaddress,
       p_sessionlanguage,
       p_sessioncurrency,
       p_logindisabled,
       p_lastlogin,
       p_hmclogindisabled,
       p_userprofile,
       p_europe1pricefactory_udg,
       p_europe1pricefactory_upg,
       p_europe1pricefactory_utg,
       p_ldapaccount,
       p_domain,
       p_ldaplogin,
       p_authorizedtounlockpages,
       p_ltfpartyid,
       p_memberid,
       p_membershipid,
       aCLTS,
       propTS,
       p_customerid,
       p_previewcatalogversions,
       p_title,
       p_defaultpaymentinfo,
       p_token,
       p_originaluid,
       p_type,
       isnull(cast(stage_hybris_users.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_users
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_users @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_users (
       bk_hash,
       users_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_users.bk_hash,
       stage_hash_hybris_users.[PK] users_pk,
       isnull(cast(stage_hash_hybris_users.modifiedTS as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_users
  left join h_hybris_users
    on stage_hash_hybris_users.bk_hash = h_hybris_users.bk_hash
 where h_hybris_users_id is null
   and stage_hash_hybris_users.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_users
if object_id('tempdb..#l_hybris_users_inserts') is not null drop table #l_hybris_users_inserts
create table #l_hybris_users_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_users.bk_hash,
       stage_hash_hybris_users.TypePkString type_pk_string,
       stage_hash_hybris_users.OwnerPkString owner_pk_string,
       stage_hash_hybris_users.[PK] users_pk,
       stage_hash_hybris_users.p_profilepicture p_profile_picture,
       stage_hash_hybris_users.p_defaultpaymentaddress p_default_payment_address,
       stage_hash_hybris_users.p_defaultshipmentaddress p_default_shipment_address,
       stage_hash_hybris_users.p_sessionlanguage p_session_language,
       stage_hash_hybris_users.p_sessioncurrency p_session_currency,
       stage_hash_hybris_users.p_userprofile p_user_profile,
       stage_hash_hybris_users.p_europe1pricefactory_udg p_europe_1_price_factory_udg,
       stage_hash_hybris_users.p_europe1pricefactory_upg p_europe_1_price_factory_upg,
       stage_hash_hybris_users.p_europe1pricefactory_utg p_europe_1_price_factory_utg,
       stage_hash_hybris_users.p_title p_title,
       stage_hash_hybris_users.p_defaultpaymentinfo p_default_payment_info,
       stage_hash_hybris_users.p_type p_type,
       stage_hash_hybris_users.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_users.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_profilepicture as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_defaultpaymentaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_defaultshipmentaddress as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_sessionlanguage as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_sessioncurrency as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_userprofile as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_europe1pricefactory_udg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_europe1pricefactory_upg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_europe1pricefactory_utg as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_title as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_defaultpaymentinfo as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_type as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_users
 where stage_hash_hybris_users.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_users records
set @insert_date_time = getdate()
insert into l_hybris_users (
       bk_hash,
       type_pk_string,
       owner_pk_string,
       users_pk,
       p_profile_picture,
       p_default_payment_address,
       p_default_shipment_address,
       p_session_language,
       p_session_currency,
       p_user_profile,
       p_europe_1_price_factory_udg,
       p_europe_1_price_factory_upg,
       p_europe_1_price_factory_utg,
       p_title,
       p_default_payment_info,
       p_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_users_inserts.bk_hash,
       #l_hybris_users_inserts.type_pk_string,
       #l_hybris_users_inserts.owner_pk_string,
       #l_hybris_users_inserts.users_pk,
       #l_hybris_users_inserts.p_profile_picture,
       #l_hybris_users_inserts.p_default_payment_address,
       #l_hybris_users_inserts.p_default_shipment_address,
       #l_hybris_users_inserts.p_session_language,
       #l_hybris_users_inserts.p_session_currency,
       #l_hybris_users_inserts.p_user_profile,
       #l_hybris_users_inserts.p_europe_1_price_factory_udg,
       #l_hybris_users_inserts.p_europe_1_price_factory_upg,
       #l_hybris_users_inserts.p_europe_1_price_factory_utg,
       #l_hybris_users_inserts.p_title,
       #l_hybris_users_inserts.p_default_payment_info,
       #l_hybris_users_inserts.p_type,
       case when l_hybris_users.l_hybris_users_id is null then isnull(#l_hybris_users_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_users_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_users_inserts
  left join p_hybris_users
    on #l_hybris_users_inserts.bk_hash = p_hybris_users.bk_hash
   and p_hybris_users.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_users
    on p_hybris_users.bk_hash = l_hybris_users.bk_hash
   and p_hybris_users.l_hybris_users_id = l_hybris_users.l_hybris_users_id
 where l_hybris_users.l_hybris_users_id is null
    or (l_hybris_users.l_hybris_users_id is not null
        and l_hybris_users.dv_hash <> #l_hybris_users_inserts.source_hash)

--calculate hash and lookup to current s_hybris_users
if object_id('tempdb..#s_hybris_users_inserts') is not null drop table #s_hybris_users_inserts
create table #s_hybris_users_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_users.bk_hash,
       stage_hash_hybris_users.hjmpTS hjmpts,
       stage_hash_hybris_users.createdTS created_ts,
       stage_hash_hybris_users.modifiedTS modified_ts,
       stage_hash_hybris_users.[PK] users_pk,
       stage_hash_hybris_users.p_description p_description,
       stage_hash_hybris_users.p_name p_name,
       stage_hash_hybris_users.p_uid p_uid,
       stage_hash_hybris_users.p_backofficelogindisabled p_back_office_log_in_disabled,
       stage_hash_hybris_users.p_ldapsearchbase p_ldap_search_base,
       stage_hash_hybris_users.p_dn p_dn,
       stage_hash_hybris_users.p_cn p_cn,
       stage_hash_hybris_users.p_logindisabled p_login_disabled,
       stage_hash_hybris_users.p_lastlogin p_last_login,
       stage_hash_hybris_users.p_hmclogindisabled p_hmc_log_in_disabled,
       stage_hash_hybris_users.p_ldapaccount p_ldap_account,
       stage_hash_hybris_users.p_domain p_domain,
       stage_hash_hybris_users.p_ldaplogin p_ldap_log_in,
       stage_hash_hybris_users.p_authorizedtounlockpages p_authorized_to_unlock_pages,
       stage_hash_hybris_users.p_ltfpartyid p_ltf_party_id,
       stage_hash_hybris_users.p_memberid p_member_id,
       stage_hash_hybris_users.p_membershipid p_membership_id,
       stage_hash_hybris_users.aCLTS acl_ts,
       stage_hash_hybris_users.propTS prop_ts,
       stage_hash_hybris_users.p_customerid p_customer_id,
       stage_hash_hybris_users.p_previewcatalogversions p_preview_catalog_versions,
       stage_hash_hybris_users.p_token p_token,
       stage_hash_hybris_users.p_originaluid p_original_uid,
       stage_hash_hybris_users.modifiedTS dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_users.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_users.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_users.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.[PK] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_uid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_backofficelogindisabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_ldapsearchbase,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_dn,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_cn,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_logindisabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_users.p_lastlogin,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_hmclogindisabled as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_ldapaccount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_domain,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_ldaplogin,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_authorizedtounlockpages as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_ltfpartyid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_memberid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.p_membershipid as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_users.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_customerid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_previewcatalogversions,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_token,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_users.p_originaluid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_users
 where stage_hash_hybris_users.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_users records
set @insert_date_time = getdate()
insert into s_hybris_users (
       bk_hash,
       hjmpts,
       created_ts,
       modified_ts,
       users_pk,
       p_description,
       p_name,
       p_uid,
       p_back_office_log_in_disabled,
       p_ldap_search_base,
       p_dn,
       p_cn,
       p_login_disabled,
       p_last_login,
       p_hmc_log_in_disabled,
       p_ldap_account,
       p_domain,
       p_ldap_log_in,
       p_authorized_to_unlock_pages,
       p_ltf_party_id,
       p_member_id,
       p_membership_id,
       acl_ts,
       prop_ts,
       p_customer_id,
       p_preview_catalog_versions,
       p_token,
       p_original_uid,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_users_inserts.bk_hash,
       #s_hybris_users_inserts.hjmpts,
       #s_hybris_users_inserts.created_ts,
       #s_hybris_users_inserts.modified_ts,
       #s_hybris_users_inserts.users_pk,
       #s_hybris_users_inserts.p_description,
       #s_hybris_users_inserts.p_name,
       #s_hybris_users_inserts.p_uid,
       #s_hybris_users_inserts.p_back_office_log_in_disabled,
       #s_hybris_users_inserts.p_ldap_search_base,
       #s_hybris_users_inserts.p_dn,
       #s_hybris_users_inserts.p_cn,
       #s_hybris_users_inserts.p_login_disabled,
       #s_hybris_users_inserts.p_last_login,
       #s_hybris_users_inserts.p_hmc_log_in_disabled,
       #s_hybris_users_inserts.p_ldap_account,
       #s_hybris_users_inserts.p_domain,
       #s_hybris_users_inserts.p_ldap_log_in,
       #s_hybris_users_inserts.p_authorized_to_unlock_pages,
       #s_hybris_users_inserts.p_ltf_party_id,
       #s_hybris_users_inserts.p_member_id,
       #s_hybris_users_inserts.p_membership_id,
       #s_hybris_users_inserts.acl_ts,
       #s_hybris_users_inserts.prop_ts,
       #s_hybris_users_inserts.p_customer_id,
       #s_hybris_users_inserts.p_preview_catalog_versions,
       #s_hybris_users_inserts.p_token,
       #s_hybris_users_inserts.p_original_uid,
       case when s_hybris_users.s_hybris_users_id is null then isnull(#s_hybris_users_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_users_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_users_inserts
  left join p_hybris_users
    on #s_hybris_users_inserts.bk_hash = p_hybris_users.bk_hash
   and p_hybris_users.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_users
    on p_hybris_users.bk_hash = s_hybris_users.bk_hash
   and p_hybris_users.s_hybris_users_id = s_hybris_users.s_hybris_users_id
 where s_hybris_users.s_hybris_users_id is null
    or (s_hybris_users.s_hybris_users_id is not null
        and s_hybris_users.dv_hash <> #s_hybris_users_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_users @current_dv_batch_id

end
