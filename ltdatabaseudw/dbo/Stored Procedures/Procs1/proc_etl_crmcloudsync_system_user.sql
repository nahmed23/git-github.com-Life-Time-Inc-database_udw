CREATE PROC [dbo].[proc_etl_crmcloudsync_system_user] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_SystemUser

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_SystemUser (
       bk_hash,
       accessmode,
       accessmodename,
       address1_addressid,
       address1_addresstypecode,
       address1_addresstypecodename,
       address1_city,
       address1_composite,
       address1_country,
       address1_county,
       address1_fax,
       address1_latitude,
       address1_line1,
       address1_line2,
       address1_line3,
       address1_longitude,
       address1_name,
       address1_postalcode,
       address1_postofficebox,
       address1_shippingmethodcode,
       address1_shippingmethodcodename,
       address1_stateorprovince,
       address1_telephone1,
       address1_telephone2,
       address1_telephone3,
       address1_upszone,
       address1_utcoffset,
       address2_addressid,
       address2_addresstypecode,
       address2_addresstypecodename,
       address2_city,
       address2_composite,
       address2_country,
       address2_county,
       address2_fax,
       address2_latitude,
       address2_line1,
       address2_line2,
       address2_line3,
       address2_longitude,
       address2_name,
       address2_postalcode,
       address2_postofficebox,
       address2_shippingmethodcode,
       address2_shippingmethodcodename,
       address2_stateorprovince,
       address2_telephone1,
       address2_telephone2,
       address2_telephone3,
       address2_upszone,
       address2_utcoffset,
       businessunitid,
       businessunitidname,
       calendarid,
       caltype,
       caltypename,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       defaultfilterspopulated,
       defaultmailbox,
       defaultmailboxname,
       disabledreason,
       displayinserviceviews,
       displayinserviceviewsname,
       domainname,
       emailrouteraccessapproval,
       emailrouteraccessapprovalname,
       employeeid,
       entityimage_timestamp,
       entityimage_url,
       entityimageid,
       exchangerate,
       firstname,
       fullname,
       governmentid,
       homephone,
       importsequencenumber,
       incomingemaildeliverymethod,
       incomingemaildeliverymethodname,
       internalemailaddress,
       invitestatuscode,
       invitestatuscodename,
       isdisabled,
       isdisabledname,
       isemailaddressapprovedbyo365admin,
       isintegrationuser,
       isintegrationusername,
       islicensed,
       issyncwithdirectory,
       jobtitle,
       lastname,
       ltf_clubid,
       ltf_clubidname,
       middlename,
       mobilealertemail,
       mobilephone,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       nickname,
       organizationid,
       organizationidname,
       outgoingemaildeliverymethod,
       outgoingemaildeliverymethodname,
       overriddencreatedon,
       parentsystemuserid,
       parentsystemuseridname,
       parentsystemuseridyominame,
       passporthi,
       passportlo,
       personalemailaddress,
       photourl,
       preferredaddresscode,
       preferredaddresscodename,
       preferredemailcode,
       preferredemailcodename,
       preferredphonecode,
       preferredphonecodename,
       processid,
       queueid,
       queueidname,
       salutation,
       setupuser,
       setupusername,
       siteid,
       siteidname,
       skills,
       stageid,
       systemuserid,
       territoryid,
       territoryidname,
       timezoneruleversionnumber,
       title,
       transactioncurrencyid,
       transactioncurrencyidname,
       userlicensetype,
       utcconversiontimezonecode,
       versionnumber,
       windowsliveid,
       yammeremailaddress,
       yammeruserid,
       yomifirstname,
       yomifullname,
       yomilastname,
       yomimiddlename,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       defaultodbfoldername,
       mobileofflineprofileid,
       mobileofflineprofileidname,
       positionid,
       positionidname,
       sharepointemailaddress,
       traversedpath,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(systemuserid,'z#@$k%&P'))),2) bk_hash,
       accessmode,
       accessmodename,
       address1_addressid,
       address1_addresstypecode,
       address1_addresstypecodename,
       address1_city,
       address1_composite,
       address1_country,
       address1_county,
       address1_fax,
       address1_latitude,
       address1_line1,
       address1_line2,
       address1_line3,
       address1_longitude,
       address1_name,
       address1_postalcode,
       address1_postofficebox,
       address1_shippingmethodcode,
       address1_shippingmethodcodename,
       address1_stateorprovince,
       address1_telephone1,
       address1_telephone2,
       address1_telephone3,
       address1_upszone,
       address1_utcoffset,
       address2_addressid,
       address2_addresstypecode,
       address2_addresstypecodename,
       address2_city,
       address2_composite,
       address2_country,
       address2_county,
       address2_fax,
       address2_latitude,
       address2_line1,
       address2_line2,
       address2_line3,
       address2_longitude,
       address2_name,
       address2_postalcode,
       address2_postofficebox,
       address2_shippingmethodcode,
       address2_shippingmethodcodename,
       address2_stateorprovince,
       address2_telephone1,
       address2_telephone2,
       address2_telephone3,
       address2_upszone,
       address2_utcoffset,
       businessunitid,
       businessunitidname,
       calendarid,
       caltype,
       caltypename,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       defaultfilterspopulated,
       defaultmailbox,
       defaultmailboxname,
       disabledreason,
       displayinserviceviews,
       displayinserviceviewsname,
       domainname,
       emailrouteraccessapproval,
       emailrouteraccessapprovalname,
       employeeid,
       entityimage_timestamp,
       entityimage_url,
       entityimageid,
       exchangerate,
       firstname,
       fullname,
       governmentid,
       homephone,
       importsequencenumber,
       incomingemaildeliverymethod,
       incomingemaildeliverymethodname,
       internalemailaddress,
       invitestatuscode,
       invitestatuscodename,
       isdisabled,
       isdisabledname,
       isemailaddressapprovedbyo365admin,
       isintegrationuser,
       isintegrationusername,
       islicensed,
       issyncwithdirectory,
       jobtitle,
       lastname,
       ltf_clubid,
       ltf_clubidname,
       middlename,
       mobilealertemail,
       mobilephone,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       nickname,
       organizationid,
       organizationidname,
       outgoingemaildeliverymethod,
       outgoingemaildeliverymethodname,
       overriddencreatedon,
       parentsystemuserid,
       parentsystemuseridname,
       parentsystemuseridyominame,
       passporthi,
       passportlo,
       personalemailaddress,
       photourl,
       preferredaddresscode,
       preferredaddresscodename,
       preferredemailcode,
       preferredemailcodename,
       preferredphonecode,
       preferredphonecodename,
       processid,
       queueid,
       queueidname,
       salutation,
       setupuser,
       setupusername,
       siteid,
       siteidname,
       skills,
       stageid,
       systemuserid,
       territoryid,
       territoryidname,
       timezoneruleversionnumber,
       title,
       transactioncurrencyid,
       transactioncurrencyidname,
       userlicensetype,
       utcconversiontimezonecode,
       versionnumber,
       windowsliveid,
       yammeremailaddress,
       yammeruserid,
       yomifirstname,
       yomifullname,
       yomilastname,
       yomimiddlename,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       defaultodbfoldername,
       mobileofflineprofileid,
       mobileofflineprofileidname,
       positionid,
       positionidname,
       sharepointemailaddress,
       traversedpath,
       isnull(cast(stage_crmcloudsync_SystemUser.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_SystemUser
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_system_user @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_system_user (
       bk_hash,
       system_user_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_SystemUser.bk_hash,
       stage_hash_crmcloudsync_SystemUser.systemuserid system_user_id,
       isnull(cast(stage_hash_crmcloudsync_SystemUser.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_SystemUser
  left join h_crmcloudsync_system_user
    on stage_hash_crmcloudsync_SystemUser.bk_hash = h_crmcloudsync_system_user.bk_hash
 where h_crmcloudsync_system_user_id is null
   and stage_hash_crmcloudsync_SystemUser.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_system_user
if object_id('tempdb..#l_crmcloudsync_system_user_inserts') is not null drop table #l_crmcloudsync_system_user_inserts
create table #l_crmcloudsync_system_user_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_SystemUser.bk_hash,
       stage_hash_crmcloudsync_SystemUser.accessmodename access_mode_name,
       stage_hash_crmcloudsync_SystemUser.address1_addressid address_1_address_id,
       stage_hash_crmcloudsync_SystemUser.address2_addressid address_2_address_id,
       stage_hash_crmcloudsync_SystemUser.businessunitid business_unit_id,
       stage_hash_crmcloudsync_SystemUser.calendarid calendar_id,
       stage_hash_crmcloudsync_SystemUser.createdby created_by,
       stage_hash_crmcloudsync_SystemUser.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_SystemUser.defaultmailbox default_mail_box,
       stage_hash_crmcloudsync_SystemUser.employeeid employee_id,
       stage_hash_crmcloudsync_SystemUser.entityimageid entity_image_id,
       stage_hash_crmcloudsync_SystemUser.governmentid government_id,
       stage_hash_crmcloudsync_SystemUser.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_SystemUser.modifiedby modified_by,
       stage_hash_crmcloudsync_SystemUser.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_SystemUser.organizationid organization_id,
       stage_hash_crmcloudsync_SystemUser.parentsystemuserid parent_system_user_id,
       stage_hash_crmcloudsync_SystemUser.processid process_id,
       stage_hash_crmcloudsync_SystemUser.queueid queue_id,
       stage_hash_crmcloudsync_SystemUser.siteid site_id,
       stage_hash_crmcloudsync_SystemUser.stageid stage_id,
       stage_hash_crmcloudsync_SystemUser.systemuserid system_user_id,
       stage_hash_crmcloudsync_SystemUser.territoryid territory_id,
       stage_hash_crmcloudsync_SystemUser.transactioncurrencyid transaction_currency_id,
       stage_hash_crmcloudsync_SystemUser.yammeruserid yammer_user_id,
       stage_hash_crmcloudsync_SystemUser.mobileofflineprofileid mobile_offline_profile_id,
       stage_hash_crmcloudsync_SystemUser.positionid position_id,
       isnull(cast(stage_hash_crmcloudsync_SystemUser.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.accessmodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_addressid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_addressid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.businessunitid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.calendarid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.defaultmailbox,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.employeeid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.entityimageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.governmentid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.organizationid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.parentsystemuserid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.processid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.queueid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.siteid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.stageid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.systemuserid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.territoryid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.transactioncurrencyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.yammeruserid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.mobileofflineprofileid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.positionid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_SystemUser
 where stage_hash_crmcloudsync_SystemUser.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_system_user records
set @insert_date_time = getdate()
insert into l_crmcloudsync_system_user (
       bk_hash,
       access_mode_name,
       address_1_address_id,
       address_2_address_id,
       business_unit_id,
       calendar_id,
       created_by,
       created_on_behalf_by,
       default_mail_box,
       employee_id,
       entity_image_id,
       government_id,
       ltf_club_id,
       modified_by,
       modified_on_behalf_by,
       organization_id,
       parent_system_user_id,
       process_id,
       queue_id,
       site_id,
       stage_id,
       system_user_id,
       territory_id,
       transaction_currency_id,
       yammer_user_id,
       mobile_offline_profile_id,
       position_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_system_user_inserts.bk_hash,
       #l_crmcloudsync_system_user_inserts.access_mode_name,
       #l_crmcloudsync_system_user_inserts.address_1_address_id,
       #l_crmcloudsync_system_user_inserts.address_2_address_id,
       #l_crmcloudsync_system_user_inserts.business_unit_id,
       #l_crmcloudsync_system_user_inserts.calendar_id,
       #l_crmcloudsync_system_user_inserts.created_by,
       #l_crmcloudsync_system_user_inserts.created_on_behalf_by,
       #l_crmcloudsync_system_user_inserts.default_mail_box,
       #l_crmcloudsync_system_user_inserts.employee_id,
       #l_crmcloudsync_system_user_inserts.entity_image_id,
       #l_crmcloudsync_system_user_inserts.government_id,
       #l_crmcloudsync_system_user_inserts.ltf_club_id,
       #l_crmcloudsync_system_user_inserts.modified_by,
       #l_crmcloudsync_system_user_inserts.modified_on_behalf_by,
       #l_crmcloudsync_system_user_inserts.organization_id,
       #l_crmcloudsync_system_user_inserts.parent_system_user_id,
       #l_crmcloudsync_system_user_inserts.process_id,
       #l_crmcloudsync_system_user_inserts.queue_id,
       #l_crmcloudsync_system_user_inserts.site_id,
       #l_crmcloudsync_system_user_inserts.stage_id,
       #l_crmcloudsync_system_user_inserts.system_user_id,
       #l_crmcloudsync_system_user_inserts.territory_id,
       #l_crmcloudsync_system_user_inserts.transaction_currency_id,
       #l_crmcloudsync_system_user_inserts.yammer_user_id,
       #l_crmcloudsync_system_user_inserts.mobile_offline_profile_id,
       #l_crmcloudsync_system_user_inserts.position_id,
       case when l_crmcloudsync_system_user.l_crmcloudsync_system_user_id is null then isnull(#l_crmcloudsync_system_user_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_system_user_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_system_user_inserts
  left join p_crmcloudsync_system_user
    on #l_crmcloudsync_system_user_inserts.bk_hash = p_crmcloudsync_system_user.bk_hash
   and p_crmcloudsync_system_user.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_system_user
    on p_crmcloudsync_system_user.bk_hash = l_crmcloudsync_system_user.bk_hash
   and p_crmcloudsync_system_user.l_crmcloudsync_system_user_id = l_crmcloudsync_system_user.l_crmcloudsync_system_user_id
 where l_crmcloudsync_system_user.l_crmcloudsync_system_user_id is null
    or (l_crmcloudsync_system_user.l_crmcloudsync_system_user_id is not null
        and l_crmcloudsync_system_user.dv_hash <> #l_crmcloudsync_system_user_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_system_user
if object_id('tempdb..#s_crmcloudsync_system_user_inserts') is not null drop table #s_crmcloudsync_system_user_inserts
create table #s_crmcloudsync_system_user_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_SystemUser.bk_hash,
       stage_hash_crmcloudsync_SystemUser.accessmode access_mode,
       stage_hash_crmcloudsync_SystemUser.address1_addresstypecode address_1_address_type_code,
       stage_hash_crmcloudsync_SystemUser.address1_addresstypecodename address_1_address_type_code_name,
       stage_hash_crmcloudsync_SystemUser.address1_city address_1_city,
       stage_hash_crmcloudsync_SystemUser.address1_composite address_1_composite,
       stage_hash_crmcloudsync_SystemUser.address1_country address_1_country,
       stage_hash_crmcloudsync_SystemUser.address1_county address_1_county,
       stage_hash_crmcloudsync_SystemUser.address1_fax address_1_fax,
       stage_hash_crmcloudsync_SystemUser.address1_latitude address_1_latitude,
       stage_hash_crmcloudsync_SystemUser.address1_line1 address_1_line_1,
       stage_hash_crmcloudsync_SystemUser.address1_line2 address_1_line_2,
       stage_hash_crmcloudsync_SystemUser.address1_line3 address_1_line_3,
       stage_hash_crmcloudsync_SystemUser.address1_longitude address_1_longitude,
       stage_hash_crmcloudsync_SystemUser.address1_name address_1_name,
       stage_hash_crmcloudsync_SystemUser.address1_postalcode address_1_postal_code,
       stage_hash_crmcloudsync_SystemUser.address1_postofficebox address_1_post_office_box,
       stage_hash_crmcloudsync_SystemUser.address1_shippingmethodcode address_1_shipping_method_code,
       stage_hash_crmcloudsync_SystemUser.address1_shippingmethodcodename address_1_shipping_method_code_name,
       stage_hash_crmcloudsync_SystemUser.address1_stateorprovince address_1_state_or_province,
       stage_hash_crmcloudsync_SystemUser.address1_telephone1 address_1_telephone_1,
       stage_hash_crmcloudsync_SystemUser.address1_telephone2 address_1_telephone_2,
       stage_hash_crmcloudsync_SystemUser.address1_telephone3 address_1_telephone_3,
       stage_hash_crmcloudsync_SystemUser.address1_upszone address_1_ups_zone,
       stage_hash_crmcloudsync_SystemUser.address1_utcoffset address_1_utc_offset,
       stage_hash_crmcloudsync_SystemUser.address2_addresstypecode address_2_address_type_code,
       stage_hash_crmcloudsync_SystemUser.address2_addresstypecodename address_2_address_type_code_name,
       stage_hash_crmcloudsync_SystemUser.address2_city address_2_city,
       stage_hash_crmcloudsync_SystemUser.address2_composite address_2_composite,
       stage_hash_crmcloudsync_SystemUser.address2_country address_2_country,
       stage_hash_crmcloudsync_SystemUser.address2_county address_2_county,
       stage_hash_crmcloudsync_SystemUser.address2_fax address_2_fax,
       stage_hash_crmcloudsync_SystemUser.address2_latitude address_2_latitude,
       stage_hash_crmcloudsync_SystemUser.address2_line1 address_2_line_1,
       stage_hash_crmcloudsync_SystemUser.address2_line2 address_2_line_2,
       stage_hash_crmcloudsync_SystemUser.address2_line3 address_2_line_3,
       stage_hash_crmcloudsync_SystemUser.address2_longitude address_2_longitude,
       stage_hash_crmcloudsync_SystemUser.address2_name address_2_name,
       stage_hash_crmcloudsync_SystemUser.address2_postalcode address_2_postal_code,
       stage_hash_crmcloudsync_SystemUser.address2_postofficebox address_2_post_office_box,
       stage_hash_crmcloudsync_SystemUser.address2_shippingmethodcode address_2_shipping_method_code,
       stage_hash_crmcloudsync_SystemUser.address2_shippingmethodcodename address_2_shipping_method_code_name,
       stage_hash_crmcloudsync_SystemUser.address2_stateorprovince address_2_state_or_province,
       stage_hash_crmcloudsync_SystemUser.address2_telephone1 address_2_telephone_1,
       stage_hash_crmcloudsync_SystemUser.address2_telephone2 address_2_telephone_2,
       stage_hash_crmcloudsync_SystemUser.address2_telephone3 address_2_telephone_3,
       stage_hash_crmcloudsync_SystemUser.address2_upszone address_2_ups_zone,
       stage_hash_crmcloudsync_SystemUser.address2_utcoffset address_2_utc_offset,
       stage_hash_crmcloudsync_SystemUser.businessunitidname business_unit_id_name,
       stage_hash_crmcloudsync_SystemUser.caltype cal_type,
       stage_hash_crmcloudsync_SystemUser.caltypename cal_type_name,
       stage_hash_crmcloudsync_SystemUser.createdbyname created_by_name,
       stage_hash_crmcloudsync_SystemUser.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_SystemUser.createdon created_on,
       stage_hash_crmcloudsync_SystemUser.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_SystemUser.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_SystemUser.defaultfilterspopulated default_filters_populated,
       stage_hash_crmcloudsync_SystemUser.defaultmailboxname default_mail_box_name,
       stage_hash_crmcloudsync_SystemUser.disabledreason disabled_reason,
       stage_hash_crmcloudsync_SystemUser.displayinserviceviews display_in_service_views,
       stage_hash_crmcloudsync_SystemUser.displayinserviceviewsname display_in_service_views_name,
       stage_hash_crmcloudsync_SystemUser.domainname domain_name,
       stage_hash_crmcloudsync_SystemUser.emailrouteraccessapproval email_router_access_approval,
       stage_hash_crmcloudsync_SystemUser.emailrouteraccessapprovalname email_router_access_approval_name,
       stage_hash_crmcloudsync_SystemUser.entityimage_timestamp entity_image_timestamp,
       stage_hash_crmcloudsync_SystemUser.entityimage_url entity_image_url,
       stage_hash_crmcloudsync_SystemUser.exchangerate exchange_rate,
       stage_hash_crmcloudsync_SystemUser.firstname first_name,
       stage_hash_crmcloudsync_SystemUser.fullname full_name,
       stage_hash_crmcloudsync_SystemUser.homephone home_phone,
       stage_hash_crmcloudsync_SystemUser.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_SystemUser.incomingemaildeliverymethod incoming_email_delivery_method,
       stage_hash_crmcloudsync_SystemUser.incomingemaildeliverymethodname incoming_email_delivery_method_name,
       stage_hash_crmcloudsync_SystemUser.internalemailaddress internal_email_address,
       stage_hash_crmcloudsync_SystemUser.invitestatuscode invite_status_code,
       stage_hash_crmcloudsync_SystemUser.invitestatuscodename invite_status_code_name,
       stage_hash_crmcloudsync_SystemUser.isdisabled is_disabled,
       stage_hash_crmcloudsync_SystemUser.isdisabledname is_disabled_name,
       stage_hash_crmcloudsync_SystemUser.isemailaddressapprovedbyo365admin is_email_address_approved_by_o365admin,
       stage_hash_crmcloudsync_SystemUser.isintegrationuser is_integration_user,
       stage_hash_crmcloudsync_SystemUser.isintegrationusername is_integration_user_name,
       stage_hash_crmcloudsync_SystemUser.islicensed is_licensed,
       stage_hash_crmcloudsync_SystemUser.issyncwithdirectory is_sync_with_directory,
       stage_hash_crmcloudsync_SystemUser.jobtitle job_title,
       stage_hash_crmcloudsync_SystemUser.lastname last_name,
       stage_hash_crmcloudsync_SystemUser.ltf_clubidname ltf_club_id_name,
       stage_hash_crmcloudsync_SystemUser.middlename middle_name,
       stage_hash_crmcloudsync_SystemUser.mobilealertemail mobile_alert_email,
       stage_hash_crmcloudsync_SystemUser.mobilephone mobile_phone,
       stage_hash_crmcloudsync_SystemUser.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_SystemUser.modifiedbyyominame modified_by_yomi_name,
       stage_hash_crmcloudsync_SystemUser.modifiedon modified_on,
       stage_hash_crmcloudsync_SystemUser.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_SystemUser.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_SystemUser.nickname nick_name,
       stage_hash_crmcloudsync_SystemUser.organizationidname organization_id_name,
       stage_hash_crmcloudsync_SystemUser.outgoingemaildeliverymethod outgoing_email_delivery_method,
       stage_hash_crmcloudsync_SystemUser.outgoingemaildeliverymethodname outgoing_email_delivery_method_name,
       stage_hash_crmcloudsync_SystemUser.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_SystemUser.parentsystemuseridname parent_system_user_id_name,
       stage_hash_crmcloudsync_SystemUser.parentsystemuseridyominame parent_system_user_id_yomi_name,
       stage_hash_crmcloudsync_SystemUser.passporthi passport_hi,
       stage_hash_crmcloudsync_SystemUser.passportlo passport_lo,
       stage_hash_crmcloudsync_SystemUser.personalemailaddress personal_email_address,
       stage_hash_crmcloudsync_SystemUser.photourl photo_url,
       stage_hash_crmcloudsync_SystemUser.preferredaddresscode preferred_address_code,
       stage_hash_crmcloudsync_SystemUser.preferredaddresscodename preferred_address_code_name,
       stage_hash_crmcloudsync_SystemUser.preferredemailcode preferred_email_code,
       stage_hash_crmcloudsync_SystemUser.preferredemailcodename preferred_email_code_name,
       stage_hash_crmcloudsync_SystemUser.preferredphonecode preferred_phone_code,
       stage_hash_crmcloudsync_SystemUser.preferredphonecodename preferred_phone_code_name,
       stage_hash_crmcloudsync_SystemUser.queueidname queue_id_name,
       stage_hash_crmcloudsync_SystemUser.salutation salutation,
       stage_hash_crmcloudsync_SystemUser.setupuser set_up_user,
       stage_hash_crmcloudsync_SystemUser.setupusername set_up_user_name,
       stage_hash_crmcloudsync_SystemUser.siteidname site_id_name,
       stage_hash_crmcloudsync_SystemUser.skills skills,
       stage_hash_crmcloudsync_SystemUser.systemuserid system_user_id,
       stage_hash_crmcloudsync_SystemUser.territoryidname territory_id_name,
       stage_hash_crmcloudsync_SystemUser.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_SystemUser.title title,
       stage_hash_crmcloudsync_SystemUser.transactioncurrencyidname transaction_currency_id_name,
       stage_hash_crmcloudsync_SystemUser.userlicensetype user_license_type,
       stage_hash_crmcloudsync_SystemUser.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_SystemUser.versionnumber version_number,
       stage_hash_crmcloudsync_SystemUser.windowsliveid windows_live_id,
       stage_hash_crmcloudsync_SystemUser.yammeremailaddress yammer_email_address,
       stage_hash_crmcloudsync_SystemUser.yomifirstname yomi_first_name,
       stage_hash_crmcloudsync_SystemUser.yomifullname yomi_full_name,
       stage_hash_crmcloudsync_SystemUser.yomilastname yomi_last_name,
       stage_hash_crmcloudsync_SystemUser.yomimiddlename yomi_middle_name,
       stage_hash_crmcloudsync_SystemUser.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_SystemUser.InsertUser insert_user,
       stage_hash_crmcloudsync_SystemUser.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_SystemUser.UpdateUser update_user,
       stage_hash_crmcloudsync_SystemUser.defaultodbfoldername default_odb_folder_name,
       stage_hash_crmcloudsync_SystemUser.mobileofflineprofileidname mobile_off_line_profile_id_name,
       stage_hash_crmcloudsync_SystemUser.positionidname position_id_name,
       stage_hash_crmcloudsync_SystemUser.sharepointemailaddress sharepoint_email_address,
       stage_hash_crmcloudsync_SystemUser.traversedpath traversed_path,
       isnull(cast(stage_hash_crmcloudsync_SystemUser.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.accessmode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address1_addresstypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_addresstypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_composite,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_fax,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address1_latitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_line3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address1_longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_postofficebox,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address1_shippingmethodcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_shippingmethodcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_telephone1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_telephone2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_telephone3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address1_upszone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address1_utcoffset as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address2_addresstypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_addresstypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_composite,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_fax,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address2_latitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_line3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address2_longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_postofficebox,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address2_shippingmethodcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_shippingmethodcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_telephone1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_telephone2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_telephone3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.address2_upszone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.address2_utcoffset as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.businessunitidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.caltype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.caltypename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_SystemUser.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.defaultfilterspopulated as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.defaultmailboxname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.disabledreason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.displayinserviceviews as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.displayinserviceviewsname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.domainname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.emailrouteraccessapproval as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.emailrouteraccessapprovalname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.entityimage_timestamp as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.entityimage_url,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.exchangerate as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.firstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.fullname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.homephone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.incomingemaildeliverymethod as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.incomingemaildeliverymethodname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.internalemailaddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.invitestatuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.invitestatuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.isdisabled as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.isdisabledname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.isemailaddressapprovedbyo365admin as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.isintegrationuser as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.isintegrationusername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.islicensed as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.issyncwithdirectory as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.jobtitle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.lastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.ltf_clubidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.middlename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.mobilealertemail,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.mobilephone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_SystemUser.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.nickname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.organizationidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.outgoingemaildeliverymethod as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.outgoingemaildeliverymethodname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_SystemUser.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.parentsystemuseridname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.parentsystemuseridyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.passporthi as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.passportlo as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.personalemailaddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.photourl,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.preferredaddresscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.preferredaddresscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.preferredemailcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.preferredemailcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.preferredphonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.preferredphonecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.queueidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.salutation,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.setupuser as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.setupusername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.siteidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.skills,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.systemuserid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.territoryidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.transactioncurrencyidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.userlicensetype as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_SystemUser.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.windowsliveid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.yammeremailaddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.yomifirstname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.yomifullname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.yomilastname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.yomimiddlename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_SystemUser.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_SystemUser.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.defaultodbfoldername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.mobileofflineprofileidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.positionidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.sharepointemailaddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_SystemUser.traversedpath,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_SystemUser
 where stage_hash_crmcloudsync_SystemUser.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_system_user records
set @insert_date_time = getdate()
insert into s_crmcloudsync_system_user (
       bk_hash,
       access_mode,
       address_1_address_type_code,
       address_1_address_type_code_name,
       address_1_city,
       address_1_composite,
       address_1_country,
       address_1_county,
       address_1_fax,
       address_1_latitude,
       address_1_line_1,
       address_1_line_2,
       address_1_line_3,
       address_1_longitude,
       address_1_name,
       address_1_postal_code,
       address_1_post_office_box,
       address_1_shipping_method_code,
       address_1_shipping_method_code_name,
       address_1_state_or_province,
       address_1_telephone_1,
       address_1_telephone_2,
       address_1_telephone_3,
       address_1_ups_zone,
       address_1_utc_offset,
       address_2_address_type_code,
       address_2_address_type_code_name,
       address_2_city,
       address_2_composite,
       address_2_country,
       address_2_county,
       address_2_fax,
       address_2_latitude,
       address_2_line_1,
       address_2_line_2,
       address_2_line_3,
       address_2_longitude,
       address_2_name,
       address_2_postal_code,
       address_2_post_office_box,
       address_2_shipping_method_code,
       address_2_shipping_method_code_name,
       address_2_state_or_province,
       address_2_telephone_1,
       address_2_telephone_2,
       address_2_telephone_3,
       address_2_ups_zone,
       address_2_utc_offset,
       business_unit_id_name,
       cal_type,
       cal_type_name,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       default_filters_populated,
       default_mail_box_name,
       disabled_reason,
       display_in_service_views,
       display_in_service_views_name,
       domain_name,
       email_router_access_approval,
       email_router_access_approval_name,
       entity_image_timestamp,
       entity_image_url,
       exchange_rate,
       first_name,
       full_name,
       home_phone,
       import_sequence_number,
       incoming_email_delivery_method,
       incoming_email_delivery_method_name,
       internal_email_address,
       invite_status_code,
       invite_status_code_name,
       is_disabled,
       is_disabled_name,
       is_email_address_approved_by_o365admin,
       is_integration_user,
       is_integration_user_name,
       is_licensed,
       is_sync_with_directory,
       job_title,
       last_name,
       ltf_club_id_name,
       middle_name,
       mobile_alert_email,
       mobile_phone,
       modified_by_name,
       modified_by_yomi_name,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       nick_name,
       organization_id_name,
       outgoing_email_delivery_method,
       outgoing_email_delivery_method_name,
       overridden_created_on,
       parent_system_user_id_name,
       parent_system_user_id_yomi_name,
       passport_hi,
       passport_lo,
       personal_email_address,
       photo_url,
       preferred_address_code,
       preferred_address_code_name,
       preferred_email_code,
       preferred_email_code_name,
       preferred_phone_code,
       preferred_phone_code_name,
       queue_id_name,
       salutation,
       set_up_user,
       set_up_user_name,
       site_id_name,
       skills,
       system_user_id,
       territory_id_name,
       time_zone_rule_version_number,
       title,
       transaction_currency_id_name,
       user_license_type,
       utc_conversion_time_zone_code,
       version_number,
       windows_live_id,
       yammer_email_address,
       yomi_first_name,
       yomi_full_name,
       yomi_last_name,
       yomi_middle_name,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       default_odb_folder_name,
       mobile_off_line_profile_id_name,
       position_id_name,
       sharepoint_email_address,
       traversed_path,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_system_user_inserts.bk_hash,
       #s_crmcloudsync_system_user_inserts.access_mode,
       #s_crmcloudsync_system_user_inserts.address_1_address_type_code,
       #s_crmcloudsync_system_user_inserts.address_1_address_type_code_name,
       #s_crmcloudsync_system_user_inserts.address_1_city,
       #s_crmcloudsync_system_user_inserts.address_1_composite,
       #s_crmcloudsync_system_user_inserts.address_1_country,
       #s_crmcloudsync_system_user_inserts.address_1_county,
       #s_crmcloudsync_system_user_inserts.address_1_fax,
       #s_crmcloudsync_system_user_inserts.address_1_latitude,
       #s_crmcloudsync_system_user_inserts.address_1_line_1,
       #s_crmcloudsync_system_user_inserts.address_1_line_2,
       #s_crmcloudsync_system_user_inserts.address_1_line_3,
       #s_crmcloudsync_system_user_inserts.address_1_longitude,
       #s_crmcloudsync_system_user_inserts.address_1_name,
       #s_crmcloudsync_system_user_inserts.address_1_postal_code,
       #s_crmcloudsync_system_user_inserts.address_1_post_office_box,
       #s_crmcloudsync_system_user_inserts.address_1_shipping_method_code,
       #s_crmcloudsync_system_user_inserts.address_1_shipping_method_code_name,
       #s_crmcloudsync_system_user_inserts.address_1_state_or_province,
       #s_crmcloudsync_system_user_inserts.address_1_telephone_1,
       #s_crmcloudsync_system_user_inserts.address_1_telephone_2,
       #s_crmcloudsync_system_user_inserts.address_1_telephone_3,
       #s_crmcloudsync_system_user_inserts.address_1_ups_zone,
       #s_crmcloudsync_system_user_inserts.address_1_utc_offset,
       #s_crmcloudsync_system_user_inserts.address_2_address_type_code,
       #s_crmcloudsync_system_user_inserts.address_2_address_type_code_name,
       #s_crmcloudsync_system_user_inserts.address_2_city,
       #s_crmcloudsync_system_user_inserts.address_2_composite,
       #s_crmcloudsync_system_user_inserts.address_2_country,
       #s_crmcloudsync_system_user_inserts.address_2_county,
       #s_crmcloudsync_system_user_inserts.address_2_fax,
       #s_crmcloudsync_system_user_inserts.address_2_latitude,
       #s_crmcloudsync_system_user_inserts.address_2_line_1,
       #s_crmcloudsync_system_user_inserts.address_2_line_2,
       #s_crmcloudsync_system_user_inserts.address_2_line_3,
       #s_crmcloudsync_system_user_inserts.address_2_longitude,
       #s_crmcloudsync_system_user_inserts.address_2_name,
       #s_crmcloudsync_system_user_inserts.address_2_postal_code,
       #s_crmcloudsync_system_user_inserts.address_2_post_office_box,
       #s_crmcloudsync_system_user_inserts.address_2_shipping_method_code,
       #s_crmcloudsync_system_user_inserts.address_2_shipping_method_code_name,
       #s_crmcloudsync_system_user_inserts.address_2_state_or_province,
       #s_crmcloudsync_system_user_inserts.address_2_telephone_1,
       #s_crmcloudsync_system_user_inserts.address_2_telephone_2,
       #s_crmcloudsync_system_user_inserts.address_2_telephone_3,
       #s_crmcloudsync_system_user_inserts.address_2_ups_zone,
       #s_crmcloudsync_system_user_inserts.address_2_utc_offset,
       #s_crmcloudsync_system_user_inserts.business_unit_id_name,
       #s_crmcloudsync_system_user_inserts.cal_type,
       #s_crmcloudsync_system_user_inserts.cal_type_name,
       #s_crmcloudsync_system_user_inserts.created_by_name,
       #s_crmcloudsync_system_user_inserts.created_by_yomi_name,
       #s_crmcloudsync_system_user_inserts.created_on,
       #s_crmcloudsync_system_user_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_system_user_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_system_user_inserts.default_filters_populated,
       #s_crmcloudsync_system_user_inserts.default_mail_box_name,
       #s_crmcloudsync_system_user_inserts.disabled_reason,
       #s_crmcloudsync_system_user_inserts.display_in_service_views,
       #s_crmcloudsync_system_user_inserts.display_in_service_views_name,
       #s_crmcloudsync_system_user_inserts.domain_name,
       #s_crmcloudsync_system_user_inserts.email_router_access_approval,
       #s_crmcloudsync_system_user_inserts.email_router_access_approval_name,
       #s_crmcloudsync_system_user_inserts.entity_image_timestamp,
       #s_crmcloudsync_system_user_inserts.entity_image_url,
       #s_crmcloudsync_system_user_inserts.exchange_rate,
       #s_crmcloudsync_system_user_inserts.first_name,
       #s_crmcloudsync_system_user_inserts.full_name,
       #s_crmcloudsync_system_user_inserts.home_phone,
       #s_crmcloudsync_system_user_inserts.import_sequence_number,
       #s_crmcloudsync_system_user_inserts.incoming_email_delivery_method,
       #s_crmcloudsync_system_user_inserts.incoming_email_delivery_method_name,
       #s_crmcloudsync_system_user_inserts.internal_email_address,
       #s_crmcloudsync_system_user_inserts.invite_status_code,
       #s_crmcloudsync_system_user_inserts.invite_status_code_name,
       #s_crmcloudsync_system_user_inserts.is_disabled,
       #s_crmcloudsync_system_user_inserts.is_disabled_name,
       #s_crmcloudsync_system_user_inserts.is_email_address_approved_by_o365admin,
       #s_crmcloudsync_system_user_inserts.is_integration_user,
       #s_crmcloudsync_system_user_inserts.is_integration_user_name,
       #s_crmcloudsync_system_user_inserts.is_licensed,
       #s_crmcloudsync_system_user_inserts.is_sync_with_directory,
       #s_crmcloudsync_system_user_inserts.job_title,
       #s_crmcloudsync_system_user_inserts.last_name,
       #s_crmcloudsync_system_user_inserts.ltf_club_id_name,
       #s_crmcloudsync_system_user_inserts.middle_name,
       #s_crmcloudsync_system_user_inserts.mobile_alert_email,
       #s_crmcloudsync_system_user_inserts.mobile_phone,
       #s_crmcloudsync_system_user_inserts.modified_by_name,
       #s_crmcloudsync_system_user_inserts.modified_by_yomi_name,
       #s_crmcloudsync_system_user_inserts.modified_on,
       #s_crmcloudsync_system_user_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_system_user_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_system_user_inserts.nick_name,
       #s_crmcloudsync_system_user_inserts.organization_id_name,
       #s_crmcloudsync_system_user_inserts.outgoing_email_delivery_method,
       #s_crmcloudsync_system_user_inserts.outgoing_email_delivery_method_name,
       #s_crmcloudsync_system_user_inserts.overridden_created_on,
       #s_crmcloudsync_system_user_inserts.parent_system_user_id_name,
       #s_crmcloudsync_system_user_inserts.parent_system_user_id_yomi_name,
       #s_crmcloudsync_system_user_inserts.passport_hi,
       #s_crmcloudsync_system_user_inserts.passport_lo,
       #s_crmcloudsync_system_user_inserts.personal_email_address,
       #s_crmcloudsync_system_user_inserts.photo_url,
       #s_crmcloudsync_system_user_inserts.preferred_address_code,
       #s_crmcloudsync_system_user_inserts.preferred_address_code_name,
       #s_crmcloudsync_system_user_inserts.preferred_email_code,
       #s_crmcloudsync_system_user_inserts.preferred_email_code_name,
       #s_crmcloudsync_system_user_inserts.preferred_phone_code,
       #s_crmcloudsync_system_user_inserts.preferred_phone_code_name,
       #s_crmcloudsync_system_user_inserts.queue_id_name,
       #s_crmcloudsync_system_user_inserts.salutation,
       #s_crmcloudsync_system_user_inserts.set_up_user,
       #s_crmcloudsync_system_user_inserts.set_up_user_name,
       #s_crmcloudsync_system_user_inserts.site_id_name,
       #s_crmcloudsync_system_user_inserts.skills,
       #s_crmcloudsync_system_user_inserts.system_user_id,
       #s_crmcloudsync_system_user_inserts.territory_id_name,
       #s_crmcloudsync_system_user_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_system_user_inserts.title,
       #s_crmcloudsync_system_user_inserts.transaction_currency_id_name,
       #s_crmcloudsync_system_user_inserts.user_license_type,
       #s_crmcloudsync_system_user_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_system_user_inserts.version_number,
       #s_crmcloudsync_system_user_inserts.windows_live_id,
       #s_crmcloudsync_system_user_inserts.yammer_email_address,
       #s_crmcloudsync_system_user_inserts.yomi_first_name,
       #s_crmcloudsync_system_user_inserts.yomi_full_name,
       #s_crmcloudsync_system_user_inserts.yomi_last_name,
       #s_crmcloudsync_system_user_inserts.yomi_middle_name,
       #s_crmcloudsync_system_user_inserts.inserted_date_time,
       #s_crmcloudsync_system_user_inserts.insert_user,
       #s_crmcloudsync_system_user_inserts.updated_date_time,
       #s_crmcloudsync_system_user_inserts.update_user,
       #s_crmcloudsync_system_user_inserts.default_odb_folder_name,
       #s_crmcloudsync_system_user_inserts.mobile_off_line_profile_id_name,
       #s_crmcloudsync_system_user_inserts.position_id_name,
       #s_crmcloudsync_system_user_inserts.sharepoint_email_address,
       #s_crmcloudsync_system_user_inserts.traversed_path,
       case when s_crmcloudsync_system_user.s_crmcloudsync_system_user_id is null then isnull(#s_crmcloudsync_system_user_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_system_user_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_system_user_inserts
  left join p_crmcloudsync_system_user
    on #s_crmcloudsync_system_user_inserts.bk_hash = p_crmcloudsync_system_user.bk_hash
   and p_crmcloudsync_system_user.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_system_user
    on p_crmcloudsync_system_user.bk_hash = s_crmcloudsync_system_user.bk_hash
   and p_crmcloudsync_system_user.s_crmcloudsync_system_user_id = s_crmcloudsync_system_user.s_crmcloudsync_system_user_id
 where s_crmcloudsync_system_user.s_crmcloudsync_system_user_id is null
    or (s_crmcloudsync_system_user.s_crmcloudsync_system_user_id is not null
        and s_crmcloudsync_system_user.dv_hash <> #s_crmcloudsync_system_user_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_system_user @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_system_user @current_dv_batch_id

end
