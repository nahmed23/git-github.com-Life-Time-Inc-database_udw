CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_club] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_Club

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_Club (
       bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_address1_addresstypecode,
       ltf_address1_addresstypecodename,
       ltf_address1_city,
       ltf_address1_country,
       ltf_address1_county,
       ltf_address1_fax,
       ltf_address1_latitude,
       ltf_address1_line1,
       ltf_address1_line2,
       ltf_address1_line3,
       ltf_address1_longitude,
       ltf_address1_name,
       ltf_address1_namename,
       ltf_address1_postalcode,
       ltf_address1_postofficebox,
       ltf_address1_shippingmethodcode,
       ltf_address1_shippingmethodcodename,
       ltf_address1_stateorprovince,
       ltf_address1_telephone1,
       ltf_address1_telephone2,
       ltf_address1_telephone3,
       ltf_address1_upszone,
       ltf_address1_utcoffset,
       ltf_address2_addresstypecode,
       ltf_address2_addresstypecodename,
       ltf_address2_city,
       ltf_address2_country,
       ltf_address2_county,
       ltf_address2_fax,
       ltf_address2_latitude,
       ltf_address2_line1,
       ltf_address2_line2,
       ltf_address2_line3,
       ltf_address2_longitude,
       ltf_address2_name,
       ltf_address2_namename,
       ltf_address2_postalcode,
       ltf_address2_postofficebox,
       ltf_address2_shippingmethodcode,
       ltf_address2_shippingmethodcodename,
       ltf_address2_stateorprovince,
       ltf_address2_telephone1,
       ltf_address2_telephone2,
       ltf_address2_telephone3,
       ltf_address2_upszone,
       ltf_address2_utcoffset,
       ltf_buddyreferral,
       ltf_buddyreferralname,
       ltf_clubdivision,
       ltf_clubdivisionname,
       ltf_clubid,
       ltf_clubregion,
       ltf_clubregionalmanager,
       ltf_clubregionalmanagername,
       ltf_clubregionalmanageryominame,
       ltf_clubregionname,
       ltf_clubsid,
       ltf_clubsidname,
       ltf_clubteamid,
       ltf_clubteamidname,
       ltf_clubteamidyominame,
       ltf_contactus,
       ltf_contactusname,
       ltf_emailaddress,
       ltf_enablemyltreferral,
       ltf_enablemyltreferralname,
       ltf_fiveletterclubcode,
       ltf_fourletterclubcode,
       ltf_generalmanager,
       ltf_generalmanagername,
       ltf_generalmanageryominame,
       ltf_guestpassduration,
       ltf_guestpassdurationname,
       ltf_lthealthclub,
       ltf_lthealthclubname,
       ltf_mem,
       ltf_membershiplevelsavailable,
       ltf_membershiplevelsavailablename,
       ltf_memname,
       ltf_memyominame,
       ltf_mmsclubid,
       ltf_mmsclubname,
       ltf_name,
       ltf_nptrep,
       ltf_nptrepname,
       ltf_nptrepyominame,
       ltf_pdth,
       ltf_pdthname,
       ltf_pdthyominame,
       ltf_pricerequest,
       ltf_pricerequestname,
       ltf_regioncode,
       ltf_timezonecode,
       ltf_udwid,
       ltf_webinquiry,
       ltf_webinquiryname,
       ltf_webreferralpass,
       ltf_webreferralpassname,
       ltf_websiteclubname,
       ltf_webspecialistteam,
       ltf_webspecialistteamname,
       ltf_webspecialistteamyominame,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       organizationid,
       organizationidname,
       overriddencreatedon,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_territoryid,
       ltf_parentterritoryid,
       ltf_acceptingpresalesleads,
       ltf_areadirector,
       ltf_areadirectorname,
       ltf_areadirectoryominame,
       ltf_callcurentsucle,
       ltf_callcurentsucle_date,
       ltf_callcurentsucle_state,
       ltf_ciguser,
       ltf_cigusername,
       ltf_ciguseryominame,
       ltf_clubbrandname,
       ltf_clubmarketingname,
       ltf_disablewebtransfer,
       ltf_disablewebtransfername,
       ltf_oldteam,
       ltf_oldteamname,
       ltf_oldteamyominame,
       ltf_parentterritoryidname,
       ltf_presale,
       ltf_regionalsaleslead,
       ltf_regionalsalesleadname,
       ltf_regionalsalesleadyominame,
       ltf_regionalvicepresident,
       ltf_regionalvicepresidentname,
       ltf_regionalvicepresidentyominame,
       ltf_srmem,
       ltf_srmemname,
       ltf_srmemyominame,
       ltf_territoryidname,
       ltf_webteamline,
       ltf_acceptingpresalesleadsname,
       ltf_presalename,
       ltf_buddyresetdays,
       ltf_excludefromexpiration,
       ltf_maxpassdays,
       ltf_passscanneractive,
       ltf_onlinejoinlink,
       ltf_kidmemberships,
       ltf_buddyresetperiod,
       ltf_restrictedguestpasspolicy,
       ltf_restrictedpassresetperiod,
       ltf_restrictedpassresetunits,
       ltf_restrictedpassgraceperiod,
       ltf_restrictedpassgraceunits,
       ltf_webguestpassprocess,
       ltf_ltworkemail,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_clubid,'z#@$k%&P'))),2) bk_hash,
       createdby,
       createdbyname,
       createdbyyominame,
       createdon,
       createdonbehalfby,
       createdonbehalfbyname,
       createdonbehalfbyyominame,
       importsequencenumber,
       ltf_address1_addresstypecode,
       ltf_address1_addresstypecodename,
       ltf_address1_city,
       ltf_address1_country,
       ltf_address1_county,
       ltf_address1_fax,
       ltf_address1_latitude,
       ltf_address1_line1,
       ltf_address1_line2,
       ltf_address1_line3,
       ltf_address1_longitude,
       ltf_address1_name,
       ltf_address1_namename,
       ltf_address1_postalcode,
       ltf_address1_postofficebox,
       ltf_address1_shippingmethodcode,
       ltf_address1_shippingmethodcodename,
       ltf_address1_stateorprovince,
       ltf_address1_telephone1,
       ltf_address1_telephone2,
       ltf_address1_telephone3,
       ltf_address1_upszone,
       ltf_address1_utcoffset,
       ltf_address2_addresstypecode,
       ltf_address2_addresstypecodename,
       ltf_address2_city,
       ltf_address2_country,
       ltf_address2_county,
       ltf_address2_fax,
       ltf_address2_latitude,
       ltf_address2_line1,
       ltf_address2_line2,
       ltf_address2_line3,
       ltf_address2_longitude,
       ltf_address2_name,
       ltf_address2_namename,
       ltf_address2_postalcode,
       ltf_address2_postofficebox,
       ltf_address2_shippingmethodcode,
       ltf_address2_shippingmethodcodename,
       ltf_address2_stateorprovince,
       ltf_address2_telephone1,
       ltf_address2_telephone2,
       ltf_address2_telephone3,
       ltf_address2_upszone,
       ltf_address2_utcoffset,
       ltf_buddyreferral,
       ltf_buddyreferralname,
       ltf_clubdivision,
       ltf_clubdivisionname,
       ltf_clubid,
       ltf_clubregion,
       ltf_clubregionalmanager,
       ltf_clubregionalmanagername,
       ltf_clubregionalmanageryominame,
       ltf_clubregionname,
       ltf_clubsid,
       ltf_clubsidname,
       ltf_clubteamid,
       ltf_clubteamidname,
       ltf_clubteamidyominame,
       ltf_contactus,
       ltf_contactusname,
       ltf_emailaddress,
       ltf_enablemyltreferral,
       ltf_enablemyltreferralname,
       ltf_fiveletterclubcode,
       ltf_fourletterclubcode,
       ltf_generalmanager,
       ltf_generalmanagername,
       ltf_generalmanageryominame,
       ltf_guestpassduration,
       ltf_guestpassdurationname,
       ltf_lthealthclub,
       ltf_lthealthclubname,
       ltf_mem,
       ltf_membershiplevelsavailable,
       ltf_membershiplevelsavailablename,
       ltf_memname,
       ltf_memyominame,
       ltf_mmsclubid,
       ltf_mmsclubname,
       ltf_name,
       ltf_nptrep,
       ltf_nptrepname,
       ltf_nptrepyominame,
       ltf_pdth,
       ltf_pdthname,
       ltf_pdthyominame,
       ltf_pricerequest,
       ltf_pricerequestname,
       ltf_regioncode,
       ltf_timezonecode,
       ltf_udwid,
       ltf_webinquiry,
       ltf_webinquiryname,
       ltf_webreferralpass,
       ltf_webreferralpassname,
       ltf_websiteclubname,
       ltf_webspecialistteam,
       ltf_webspecialistteamname,
       ltf_webspecialistteamyominame,
       modifiedby,
       modifiedbyname,
       modifiedbyyominame,
       modifiedon,
       modifiedonbehalfby,
       modifiedonbehalfbyname,
       modifiedonbehalfbyyominame,
       organizationid,
       organizationidname,
       overriddencreatedon,
       statecode,
       statecodename,
       statuscode,
       statuscodename,
       timezoneruleversionnumber,
       utcconversiontimezonecode,
       versionnumber,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_territoryid,
       ltf_parentterritoryid,
       ltf_acceptingpresalesleads,
       ltf_areadirector,
       ltf_areadirectorname,
       ltf_areadirectoryominame,
       ltf_callcurentsucle,
       ltf_callcurentsucle_date,
       ltf_callcurentsucle_state,
       ltf_ciguser,
       ltf_cigusername,
       ltf_ciguseryominame,
       ltf_clubbrandname,
       ltf_clubmarketingname,
       ltf_disablewebtransfer,
       ltf_disablewebtransfername,
       ltf_oldteam,
       ltf_oldteamname,
       ltf_oldteamyominame,
       ltf_parentterritoryidname,
       ltf_presale,
       ltf_regionalsaleslead,
       ltf_regionalsalesleadname,
       ltf_regionalsalesleadyominame,
       ltf_regionalvicepresident,
       ltf_regionalvicepresidentname,
       ltf_regionalvicepresidentyominame,
       ltf_srmem,
       ltf_srmemname,
       ltf_srmemyominame,
       ltf_territoryidname,
       ltf_webteamline,
       ltf_acceptingpresalesleadsname,
       ltf_presalename,
       ltf_buddyresetdays,
       ltf_excludefromexpiration,
       ltf_maxpassdays,
       ltf_passscanneractive,
       ltf_onlinejoinlink,
       ltf_kidmemberships,
       ltf_buddyresetperiod,
       ltf_restrictedguestpasspolicy,
       ltf_restrictedpassresetperiod,
       ltf_restrictedpassresetunits,
       ltf_restrictedpassgraceperiod,
       ltf_restrictedpassgraceunits,
       ltf_webguestpassprocess,
       ltf_ltworkemail,
       isnull(cast(stage_crmcloudsync_LTF_Club.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_Club
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_club @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_club (
       bk_hash,
       ltf_club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_Club.bk_hash,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubid ltf_club_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_Club.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_Club
  left join h_crmcloudsync_ltf_club
    on stage_hash_crmcloudsync_LTF_Club.bk_hash = h_crmcloudsync_ltf_club.bk_hash
 where h_crmcloudsync_ltf_club_id is null
   and stage_hash_crmcloudsync_LTF_Club.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_club
if object_id('tempdb..#l_crmcloudsync_ltf_club_inserts') is not null drop table #l_crmcloudsync_ltf_club_inserts
create table #l_crmcloudsync_ltf_club_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Club.bk_hash,
       stage_hash_crmcloudsync_LTF_Club.createdby created_by,
       stage_hash_crmcloudsync_LTF_Club.createdonbehalfby created_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubdivision ltf_club_division,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubregion ltf_club_region,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubregionalmanager ltf_club_regional_manager,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubsid ltf_clubs_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubteamid ltf_club_team_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_generalmanager ltf_general_manager,
       stage_hash_crmcloudsync_LTF_Club.ltf_mem ltf_mem,
       stage_hash_crmcloudsync_LTF_Club.ltf_mmsclubid ltf_mms_club_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_nptrep ltf_npt_rep,
       stage_hash_crmcloudsync_LTF_Club.ltf_pdth ltf_pdth,
       stage_hash_crmcloudsync_LTF_Club.ltf_udwid ltf_udw_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_webspecialistteam ltf_web_specialist_team,
       stage_hash_crmcloudsync_LTF_Club.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_Club.modifiedonbehalfby modified_on_behalf_by,
       stage_hash_crmcloudsync_LTF_Club.organizationid organization_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_territoryid ltf_territory_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_parentterritoryid ltf_parent_territory_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_areadirector ltf_area_director,
       stage_hash_crmcloudsync_LTF_Club.ltf_ciguser ltf_cig_user,
       stage_hash_crmcloudsync_LTF_Club.ltf_oldteam ltf_old_team,
       stage_hash_crmcloudsync_LTF_Club.ltf_regionalsaleslead ltf_regional_sales_lead,
       stage_hash_crmcloudsync_LTF_Club.ltf_regionalvicepresident ltf_regional_vice_president,
       stage_hash_crmcloudsync_LTF_Club.ltf_srmem ltf_sr_mem,
       isnull(cast(stage_hash_crmcloudsync_LTF_Club.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.createdonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubdivision,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubregion,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubregionalmanager,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubsid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubteamid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_generalmanager,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_mem,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_mmsclubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_nptrep,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_pdth,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_udwid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_webspecialistteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.modifiedonbehalfby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.organizationid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_territoryid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_parentterritoryid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_areadirector,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_ciguser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_oldteam,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_regionalsaleslead,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_regionalvicepresident,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_srmem,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Club
 where stage_hash_crmcloudsync_LTF_Club.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_club records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_club (
       bk_hash,
       created_by,
       created_on_behalf_by,
       ltf_club_division,
       ltf_club_id,
       ltf_club_region,
       ltf_club_regional_manager,
       ltf_clubs_id,
       ltf_club_team_id,
       ltf_general_manager,
       ltf_mem,
       ltf_mms_club_id,
       ltf_npt_rep,
       ltf_pdth,
       ltf_udw_id,
       ltf_web_specialist_team,
       modified_by,
       modified_on_behalf_by,
       organization_id,
       ltf_territory_id,
       ltf_parent_territory_id,
       ltf_area_director,
       ltf_cig_user,
       ltf_old_team,
       ltf_regional_sales_lead,
       ltf_regional_vice_president,
       ltf_sr_mem,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_club_inserts.bk_hash,
       #l_crmcloudsync_ltf_club_inserts.created_by,
       #l_crmcloudsync_ltf_club_inserts.created_on_behalf_by,
       #l_crmcloudsync_ltf_club_inserts.ltf_club_division,
       #l_crmcloudsync_ltf_club_inserts.ltf_club_id,
       #l_crmcloudsync_ltf_club_inserts.ltf_club_region,
       #l_crmcloudsync_ltf_club_inserts.ltf_club_regional_manager,
       #l_crmcloudsync_ltf_club_inserts.ltf_clubs_id,
       #l_crmcloudsync_ltf_club_inserts.ltf_club_team_id,
       #l_crmcloudsync_ltf_club_inserts.ltf_general_manager,
       #l_crmcloudsync_ltf_club_inserts.ltf_mem,
       #l_crmcloudsync_ltf_club_inserts.ltf_mms_club_id,
       #l_crmcloudsync_ltf_club_inserts.ltf_npt_rep,
       #l_crmcloudsync_ltf_club_inserts.ltf_pdth,
       #l_crmcloudsync_ltf_club_inserts.ltf_udw_id,
       #l_crmcloudsync_ltf_club_inserts.ltf_web_specialist_team,
       #l_crmcloudsync_ltf_club_inserts.modified_by,
       #l_crmcloudsync_ltf_club_inserts.modified_on_behalf_by,
       #l_crmcloudsync_ltf_club_inserts.organization_id,
       #l_crmcloudsync_ltf_club_inserts.ltf_territory_id,
       #l_crmcloudsync_ltf_club_inserts.ltf_parent_territory_id,
       #l_crmcloudsync_ltf_club_inserts.ltf_area_director,
       #l_crmcloudsync_ltf_club_inserts.ltf_cig_user,
       #l_crmcloudsync_ltf_club_inserts.ltf_old_team,
       #l_crmcloudsync_ltf_club_inserts.ltf_regional_sales_lead,
       #l_crmcloudsync_ltf_club_inserts.ltf_regional_vice_president,
       #l_crmcloudsync_ltf_club_inserts.ltf_sr_mem,
       case when l_crmcloudsync_ltf_club.l_crmcloudsync_ltf_club_id is null then isnull(#l_crmcloudsync_ltf_club_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_club_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_club_inserts
  left join p_crmcloudsync_ltf_club
    on #l_crmcloudsync_ltf_club_inserts.bk_hash = p_crmcloudsync_ltf_club.bk_hash
   and p_crmcloudsync_ltf_club.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_club
    on p_crmcloudsync_ltf_club.bk_hash = l_crmcloudsync_ltf_club.bk_hash
   and p_crmcloudsync_ltf_club.l_crmcloudsync_ltf_club_id = l_crmcloudsync_ltf_club.l_crmcloudsync_ltf_club_id
 where l_crmcloudsync_ltf_club.l_crmcloudsync_ltf_club_id is null
    or (l_crmcloudsync_ltf_club.l_crmcloudsync_ltf_club_id is not null
        and l_crmcloudsync_ltf_club.dv_hash <> #l_crmcloudsync_ltf_club_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_club
if object_id('tempdb..#s_crmcloudsync_ltf_club_inserts') is not null drop table #s_crmcloudsync_ltf_club_inserts
create table #s_crmcloudsync_ltf_club_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Club.bk_hash,
       stage_hash_crmcloudsync_LTF_Club.createdbyname created_by_name,
       stage_hash_crmcloudsync_LTF_Club.createdbyyominame created_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.createdon created_on,
       stage_hash_crmcloudsync_LTF_Club.createdonbehalfbyname created_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Club.createdonbehalfbyyominame created_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.importsequencenumber import_sequence_number,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_addresstypecode ltf_address_1_address_type_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_addresstypecodename ltf_address_1_address_type_code_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_city ltf_address_1_city,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_country ltf_address_1_country,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_county ltf_address_1_county,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_fax ltf_address_1_fax,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_latitude ltf_address_1_latitude,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_line1 ltf_address_1_line_1,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_line2 ltf_address_1_line_2,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_line3 ltf_address_1_line_3,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_longitude ltf_address_1_longitude,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_name ltf_address_1_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_namename ltf_address_1_name_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_postalcode ltf_address_1_postal_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_postofficebox ltf_address_1_post_office_box,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_shippingmethodcode ltf_address_1_shipping_method_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_shippingmethodcodename ltf_address_1_shipping_method_code_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_stateorprovince ltf_address_1_state_or_province,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_telephone1 ltf_address_1_telephone_1,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_telephone2 ltf_address_1_telephone_2,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_telephone3 ltf_address_1_telephone_3,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_upszone ltf_address_1_ups_zone,
       stage_hash_crmcloudsync_LTF_Club.ltf_address1_utcoffset ltf_address_1_utc_offset,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_addresstypecode ltf_address_2_address_type_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_addresstypecodename ltf_address_2_address_type_code_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_city ltf_address_2_city,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_country ltf_address_2_country,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_county ltf_address_2_county,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_fax ltf_address_2_fax,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_latitude ltf_address_2_latitude,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_line1 ltf_address_2_line_1,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_line2 ltf_address_2_line_2,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_line3 ltf_address_2_line_3,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_longitude ltf_address_2_longitude,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_name ltf_address_2_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_namename ltf_address_2_name_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_postalcode ltf_address_2_postal_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_postofficebox ltf_address_2_post_office_box,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_shippingmethodcode ltf_address_2_shipping_method_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_shippingmethodcodename ltf_address_2_shipping_method_code_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_stateorprovince ltf_address_2_state_or_province,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_telephone1 ltf_address_2_telephone_1,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_telephone2 ltf_address_2_telephone_2,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_telephone3 ltf_address_2_telephone_3,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_upszone ltf_address_2_ups_zone,
       stage_hash_crmcloudsync_LTF_Club.ltf_address2_utcoffset ltf_address_2_utc_off_set,
       stage_hash_crmcloudsync_LTF_Club.ltf_buddyreferral ltf_buddy_referral,
       stage_hash_crmcloudsync_LTF_Club.ltf_buddyreferralname ltf_buddy_referral_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubdivisionname ltf_club_division_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubid ltf_club_id,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubregionalmanagername ltf_club_regional_manager_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubregionalmanageryominame ltf_club_regional_manager_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubregionname ltf_club_regionname,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubsidname ltf_clubs_id_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubteamidname ltf_club_team_id_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubteamidyominame ltf_club_team_id_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_contactus ltf_contact_us,
       stage_hash_crmcloudsync_LTF_Club.ltf_contactusname ltf_contact_us_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_emailaddress ltf_email_address,
       stage_hash_crmcloudsync_LTF_Club.ltf_enablemyltreferral ltf_enable_my_lt_referral,
       stage_hash_crmcloudsync_LTF_Club.ltf_enablemyltreferralname ltf_enable_my_lt_referral_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_fiveletterclubcode ltf_five_letter_club_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_fourletterclubcode ltf_four_letter_club_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_generalmanagername ltf_general_manager_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_generalmanageryominame ltf_general_manager_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_guestpassduration ltf_guest_pass_duration,
       stage_hash_crmcloudsync_LTF_Club.ltf_guestpassdurationname ltf_guest_pass_duration_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_lthealthclub ltf_lt_health_club,
       stage_hash_crmcloudsync_LTF_Club.ltf_lthealthclubname ltf_lt_health_club_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_membershiplevelsavailable ltf_membership_levels_available,
       stage_hash_crmcloudsync_LTF_Club.ltf_membershiplevelsavailablename ltf_membership_levels_available_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_memname ltf_mem_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_memyominame ltf_mem_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_mmsclubname ltf_mms_club_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_name ltf_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_nptrepname ltf_npt_rep_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_nptrepyominame ltf_npt_rep_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_pdthname ltf_pdth_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_pdthyominame ltf_pdth_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_pricerequest ltf_price_request,
       stage_hash_crmcloudsync_LTF_Club.ltf_pricerequestname ltf_price_request_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_regioncode ltf_region_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_timezonecode ltf_time_zone_code,
       stage_hash_crmcloudsync_LTF_Club.ltf_webinquiry ltf_web_inquiry,
       stage_hash_crmcloudsync_LTF_Club.ltf_webinquiryname ltf_web_inquiry_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_webreferralpass ltf_web_referral_pass,
       stage_hash_crmcloudsync_LTF_Club.ltf_webreferralpassname ltf_web_referral_passname,
       stage_hash_crmcloudsync_LTF_Club.ltf_websiteclubname ltf_web_site_club_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_webspecialistteamname ltf_web_specialist_team_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_webspecialistteamyominame ltf_web_specialist_team_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.modifiedbyname modified_by_name,
       stage_hash_crmcloudsync_LTF_Club.modifiedbyyominame modified_by_yominame,
       stage_hash_crmcloudsync_LTF_Club.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_Club.modifiedonbehalfbyname modified_on_behalf_by_name,
       stage_hash_crmcloudsync_LTF_Club.modifiedonbehalfbyyominame modified_on_behalf_by_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.organizationidname organization_id_name,
       stage_hash_crmcloudsync_LTF_Club.overriddencreatedon overridden_created_on,
       stage_hash_crmcloudsync_LTF_Club.statecode state_code,
       stage_hash_crmcloudsync_LTF_Club.statecodename state_code_name,
       stage_hash_crmcloudsync_LTF_Club.statuscode status_code,
       stage_hash_crmcloudsync_LTF_Club.statuscodename status_code_name,
       stage_hash_crmcloudsync_LTF_Club.timezoneruleversionnumber time_zone_rule_version_number,
       stage_hash_crmcloudsync_LTF_Club.utcconversiontimezonecode utc_conversion_time_zone_code,
       stage_hash_crmcloudsync_LTF_Club.versionnumber version_number,
       stage_hash_crmcloudsync_LTF_Club.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_Club.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_Club.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_Club.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_Club.ltf_acceptingpresalesleads ltf_accepting_presales_leads,
       stage_hash_crmcloudsync_LTF_Club.ltf_areadirectorname ltf_area_director_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_areadirectoryominame ltf_area_director_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_callcurentsucle ltf_call_curents_ucle,
       stage_hash_crmcloudsync_LTF_Club.ltf_callcurentsucle_date ltf_call_curents_ucle_date,
       stage_hash_crmcloudsync_LTF_Club.ltf_callcurentsucle_state ltf_call_curents_ucle_state,
       stage_hash_crmcloudsync_LTF_Club.ltf_cigusername ltf_cig_user_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_ciguseryominame ltf_cig_user_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubbrandname ltf_club_brand_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_clubmarketingname ltf_club_marketing_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_disablewebtransfer ltf_disable_web_transfer,
       stage_hash_crmcloudsync_LTF_Club.ltf_disablewebtransfername ltf_disable_web_transfer_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_oldteamname ltf_old_team_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_oldteamyominame ltf_old_team_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_parentterritoryidname ltf_parent_territory_id_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_presale ltf_pre_sale,
       stage_hash_crmcloudsync_LTF_Club.ltf_regionalsalesleadname ltf_regional_sales_lead_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_regionalsalesleadyominame ltf_regional_sales_lead_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_regionalvicepresidentname ltf_regional_vice_president_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_regionalvicepresidentyominame ltf_regional_vice_president_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_srmemname ltf_sr_mem_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_srmemyominame ltf_sr_mem_yomi_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_territoryidname ltf_territory_id_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_webteamline ltf_web_team_line,
       stage_hash_crmcloudsync_LTF_Club.ltf_acceptingpresalesleadsname ltf_accepting_presales_leads_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_presalename ltf_presale_name,
       stage_hash_crmcloudsync_LTF_Club.ltf_buddyresetdays ltf_buddy_reset_days,
       stage_hash_crmcloudsync_LTF_Club.ltf_excludefromexpiration ltf_exclude_from_expiration,
       stage_hash_crmcloudsync_LTF_Club.ltf_maxpassdays ltf_max_pass_days,
       stage_hash_crmcloudsync_LTF_Club.ltf_passscanneractive ltf_passs_canner_active,
       stage_hash_crmcloudsync_LTF_Club.ltf_onlinejoinlink ltf_online_join_link,
       stage_hash_crmcloudsync_LTF_Club.ltf_kidmemberships ltf_kid_memberships,
       stage_hash_crmcloudsync_LTF_Club.ltf_buddyresetperiod ltf_buddy_reset_period,
       stage_hash_crmcloudsync_LTF_Club.ltf_restrictedguestpasspolicy ltf_restricted_guest_pass_policy,
       stage_hash_crmcloudsync_LTF_Club.ltf_restrictedpassresetperiod ltf_restricted_pass_reset_period,
       stage_hash_crmcloudsync_LTF_Club.ltf_restrictedpassresetunits ltf_restricted_pass_reset_units,
       stage_hash_crmcloudsync_LTF_Club.ltf_restrictedpassgraceperiod ltf_restricted_pass_grace_period,
       stage_hash_crmcloudsync_LTF_Club.ltf_restrictedpassgraceunits ltf_restricted_pass_grace_units,
       stage_hash_crmcloudsync_LTF_Club.ltf_webguestpassprocess ltf_web_guest_pass_process,
       stage_hash_crmcloudsync_LTF_Club.ltf_ltworkemail ltf_lt_work_email,
       isnull(cast(stage_hash_crmcloudsync_LTF_Club.createdon as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.createdbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.createdbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Club.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.createdonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.createdonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.importsequencenumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address1_addresstypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_addresstypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_fax,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address1_latitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_line3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address1_longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address1_name as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_namename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_postofficebox,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address1_shippingmethodcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_shippingmethodcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_telephone1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_telephone2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_telephone3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address1_upszone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address1_utcoffset as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address2_addresstypecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_addresstypecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_city,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_country,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_county,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_fax,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address2_latitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_line1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_line2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_line3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address2_longitude as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address2_name as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_namename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_postalcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_postofficebox,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address2_shippingmethodcode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_shippingmethodcodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_stateorprovince,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_telephone1,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_telephone2,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_telephone3,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_address2_upszone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_address2_utcoffset as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_buddyreferral as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_buddyreferralname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubdivisionname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubregionalmanagername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubregionalmanageryominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubregionname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubsidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubteamidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubteamidyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_contactus as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_contactusname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_emailaddress,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_enablemyltreferral as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_enablemyltreferralname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_fiveletterclubcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_fourletterclubcode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_generalmanagername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_generalmanageryominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_guestpassduration as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_guestpassdurationname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_lthealthclub as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_lthealthclubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_membershiplevelsavailable as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_membershiplevelsavailablename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_memname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_memyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_mmsclubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_nptrepname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_nptrepyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_pdthname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_pdthyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_pricerequest as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_pricerequestname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_regioncode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_timezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_webinquiry as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_webinquiryname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_webreferralpass as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_webreferralpassname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_websiteclubname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_webspecialistteamname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_webspecialistteamyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.modifiedbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.modifiedbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Club.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.modifiedonbehalfbyname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.modifiedonbehalfbyyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.organizationidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Club.overriddencreatedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.statecodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.statuscodename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.timezoneruleversionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.utcconversiontimezonecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.versionnumber as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Club.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Club.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_acceptingpresalesleads as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_areadirectorname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_areadirectoryominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_callcurentsucle as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Club.ltf_callcurentsucle_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_callcurentsucle_state as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_cigusername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_ciguseryominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubbrandname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_clubmarketingname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_disablewebtransfer as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_disablewebtransfername,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_oldteamname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_oldteamyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_parentterritoryidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_presale as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_regionalsalesleadname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_regionalsalesleadyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_regionalvicepresidentname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_regionalvicepresidentyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_srmemname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_srmemyominame,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_territoryidname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_webteamline,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_acceptingpresalesleadsname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Club.ltf_presalename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_buddyresetdays as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_excludefromexpiration as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_maxpassdays as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_passscanneractive as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_onlinejoinlink as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_kidmemberships as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_buddyresetperiod as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_restrictedguestpasspolicy as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_restrictedpassresetperiod as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_restrictedpassresetunits as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_restrictedpassgraceperiod as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_restrictedpassgraceunits as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_webguestpassprocess as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Club.ltf_ltworkemail as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Club
 where stage_hash_crmcloudsync_LTF_Club.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_club records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_club (
       bk_hash,
       created_by_name,
       created_by_yomi_name,
       created_on,
       created_on_behalf_by_name,
       created_on_behalf_by_yomi_name,
       import_sequence_number,
       ltf_address_1_address_type_code,
       ltf_address_1_address_type_code_name,
       ltf_address_1_city,
       ltf_address_1_country,
       ltf_address_1_county,
       ltf_address_1_fax,
       ltf_address_1_latitude,
       ltf_address_1_line_1,
       ltf_address_1_line_2,
       ltf_address_1_line_3,
       ltf_address_1_longitude,
       ltf_address_1_name,
       ltf_address_1_name_name,
       ltf_address_1_postal_code,
       ltf_address_1_post_office_box,
       ltf_address_1_shipping_method_code,
       ltf_address_1_shipping_method_code_name,
       ltf_address_1_state_or_province,
       ltf_address_1_telephone_1,
       ltf_address_1_telephone_2,
       ltf_address_1_telephone_3,
       ltf_address_1_ups_zone,
       ltf_address_1_utc_offset,
       ltf_address_2_address_type_code,
       ltf_address_2_address_type_code_name,
       ltf_address_2_city,
       ltf_address_2_country,
       ltf_address_2_county,
       ltf_address_2_fax,
       ltf_address_2_latitude,
       ltf_address_2_line_1,
       ltf_address_2_line_2,
       ltf_address_2_line_3,
       ltf_address_2_longitude,
       ltf_address_2_name,
       ltf_address_2_name_name,
       ltf_address_2_postal_code,
       ltf_address_2_post_office_box,
       ltf_address_2_shipping_method_code,
       ltf_address_2_shipping_method_code_name,
       ltf_address_2_state_or_province,
       ltf_address_2_telephone_1,
       ltf_address_2_telephone_2,
       ltf_address_2_telephone_3,
       ltf_address_2_ups_zone,
       ltf_address_2_utc_off_set,
       ltf_buddy_referral,
       ltf_buddy_referral_name,
       ltf_club_division_name,
       ltf_club_id,
       ltf_club_regional_manager_name,
       ltf_club_regional_manager_yomi_name,
       ltf_club_regionname,
       ltf_clubs_id_name,
       ltf_club_team_id_name,
       ltf_club_team_id_yomi_name,
       ltf_contact_us,
       ltf_contact_us_name,
       ltf_email_address,
       ltf_enable_my_lt_referral,
       ltf_enable_my_lt_referral_name,
       ltf_five_letter_club_code,
       ltf_four_letter_club_code,
       ltf_general_manager_name,
       ltf_general_manager_yomi_name,
       ltf_guest_pass_duration,
       ltf_guest_pass_duration_name,
       ltf_lt_health_club,
       ltf_lt_health_club_name,
       ltf_membership_levels_available,
       ltf_membership_levels_available_name,
       ltf_mem_name,
       ltf_mem_yomi_name,
       ltf_mms_club_name,
       ltf_name,
       ltf_npt_rep_name,
       ltf_npt_rep_yomi_name,
       ltf_pdth_name,
       ltf_pdth_yomi_name,
       ltf_price_request,
       ltf_price_request_name,
       ltf_region_code,
       ltf_time_zone_code,
       ltf_web_inquiry,
       ltf_web_inquiry_name,
       ltf_web_referral_pass,
       ltf_web_referral_passname,
       ltf_web_site_club_name,
       ltf_web_specialist_team_name,
       ltf_web_specialist_team_yomi_name,
       modified_by_name,
       modified_by_yominame,
       modified_on,
       modified_on_behalf_by_name,
       modified_on_behalf_by_yomi_name,
       organization_id_name,
       overridden_created_on,
       state_code,
       state_code_name,
       status_code,
       status_code_name,
       time_zone_rule_version_number,
       utc_conversion_time_zone_code,
       version_number,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       ltf_accepting_presales_leads,
       ltf_area_director_name,
       ltf_area_director_yomi_name,
       ltf_call_curents_ucle,
       ltf_call_curents_ucle_date,
       ltf_call_curents_ucle_state,
       ltf_cig_user_name,
       ltf_cig_user_yomi_name,
       ltf_club_brand_name,
       ltf_club_marketing_name,
       ltf_disable_web_transfer,
       ltf_disable_web_transfer_name,
       ltf_old_team_name,
       ltf_old_team_yomi_name,
       ltf_parent_territory_id_name,
       ltf_pre_sale,
       ltf_regional_sales_lead_name,
       ltf_regional_sales_lead_yomi_name,
       ltf_regional_vice_president_name,
       ltf_regional_vice_president_yomi_name,
       ltf_sr_mem_name,
       ltf_sr_mem_yomi_name,
       ltf_territory_id_name,
       ltf_web_team_line,
       ltf_accepting_presales_leads_name,
       ltf_presale_name,
       ltf_buddy_reset_days,
       ltf_exclude_from_expiration,
       ltf_max_pass_days,
       ltf_passs_canner_active,
       ltf_online_join_link,
       ltf_kid_memberships,
       ltf_buddy_reset_period,
       ltf_restricted_guest_pass_policy,
       ltf_restricted_pass_reset_period,
       ltf_restricted_pass_reset_units,
       ltf_restricted_pass_grace_period,
       ltf_restricted_pass_grace_units,
       ltf_web_guest_pass_process,
       ltf_lt_work_email,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_club_inserts.bk_hash,
       #s_crmcloudsync_ltf_club_inserts.created_by_name,
       #s_crmcloudsync_ltf_club_inserts.created_by_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.created_on,
       #s_crmcloudsync_ltf_club_inserts.created_on_behalf_by_name,
       #s_crmcloudsync_ltf_club_inserts.created_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.import_sequence_number,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_address_type_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_address_type_code_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_city,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_country,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_county,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_fax,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_latitude,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_line_1,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_line_2,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_line_3,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_longitude,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_name_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_postal_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_post_office_box,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_shipping_method_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_shipping_method_code_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_state_or_province,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_telephone_1,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_telephone_2,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_telephone_3,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_ups_zone,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_1_utc_offset,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_address_type_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_address_type_code_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_city,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_country,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_county,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_fax,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_latitude,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_line_1,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_line_2,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_line_3,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_longitude,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_name_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_postal_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_post_office_box,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_shipping_method_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_shipping_method_code_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_state_or_province,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_telephone_1,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_telephone_2,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_telephone_3,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_ups_zone,
       #s_crmcloudsync_ltf_club_inserts.ltf_address_2_utc_off_set,
       #s_crmcloudsync_ltf_club_inserts.ltf_buddy_referral,
       #s_crmcloudsync_ltf_club_inserts.ltf_buddy_referral_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_division_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_id,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_regional_manager_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_regional_manager_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_regionname,
       #s_crmcloudsync_ltf_club_inserts.ltf_clubs_id_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_team_id_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_team_id_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_contact_us,
       #s_crmcloudsync_ltf_club_inserts.ltf_contact_us_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_email_address,
       #s_crmcloudsync_ltf_club_inserts.ltf_enable_my_lt_referral,
       #s_crmcloudsync_ltf_club_inserts.ltf_enable_my_lt_referral_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_five_letter_club_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_four_letter_club_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_general_manager_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_general_manager_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_guest_pass_duration,
       #s_crmcloudsync_ltf_club_inserts.ltf_guest_pass_duration_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_lt_health_club,
       #s_crmcloudsync_ltf_club_inserts.ltf_lt_health_club_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_membership_levels_available,
       #s_crmcloudsync_ltf_club_inserts.ltf_membership_levels_available_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_mem_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_mem_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_mms_club_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_npt_rep_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_npt_rep_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_pdth_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_pdth_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_price_request,
       #s_crmcloudsync_ltf_club_inserts.ltf_price_request_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_region_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_time_zone_code,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_inquiry,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_inquiry_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_referral_pass,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_referral_passname,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_site_club_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_specialist_team_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_specialist_team_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.modified_by_name,
       #s_crmcloudsync_ltf_club_inserts.modified_by_yominame,
       #s_crmcloudsync_ltf_club_inserts.modified_on,
       #s_crmcloudsync_ltf_club_inserts.modified_on_behalf_by_name,
       #s_crmcloudsync_ltf_club_inserts.modified_on_behalf_by_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.organization_id_name,
       #s_crmcloudsync_ltf_club_inserts.overridden_created_on,
       #s_crmcloudsync_ltf_club_inserts.state_code,
       #s_crmcloudsync_ltf_club_inserts.state_code_name,
       #s_crmcloudsync_ltf_club_inserts.status_code,
       #s_crmcloudsync_ltf_club_inserts.status_code_name,
       #s_crmcloudsync_ltf_club_inserts.time_zone_rule_version_number,
       #s_crmcloudsync_ltf_club_inserts.utc_conversion_time_zone_code,
       #s_crmcloudsync_ltf_club_inserts.version_number,
       #s_crmcloudsync_ltf_club_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_club_inserts.insert_user,
       #s_crmcloudsync_ltf_club_inserts.updated_date_time,
       #s_crmcloudsync_ltf_club_inserts.update_user,
       #s_crmcloudsync_ltf_club_inserts.ltf_accepting_presales_leads,
       #s_crmcloudsync_ltf_club_inserts.ltf_area_director_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_area_director_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_call_curents_ucle,
       #s_crmcloudsync_ltf_club_inserts.ltf_call_curents_ucle_date,
       #s_crmcloudsync_ltf_club_inserts.ltf_call_curents_ucle_state,
       #s_crmcloudsync_ltf_club_inserts.ltf_cig_user_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_cig_user_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_brand_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_club_marketing_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_disable_web_transfer,
       #s_crmcloudsync_ltf_club_inserts.ltf_disable_web_transfer_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_old_team_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_old_team_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_parent_territory_id_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_pre_sale,
       #s_crmcloudsync_ltf_club_inserts.ltf_regional_sales_lead_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_regional_sales_lead_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_regional_vice_president_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_regional_vice_president_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_sr_mem_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_sr_mem_yomi_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_territory_id_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_team_line,
       #s_crmcloudsync_ltf_club_inserts.ltf_accepting_presales_leads_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_presale_name,
       #s_crmcloudsync_ltf_club_inserts.ltf_buddy_reset_days,
       #s_crmcloudsync_ltf_club_inserts.ltf_exclude_from_expiration,
       #s_crmcloudsync_ltf_club_inserts.ltf_max_pass_days,
       #s_crmcloudsync_ltf_club_inserts.ltf_passs_canner_active,
       #s_crmcloudsync_ltf_club_inserts.ltf_online_join_link,
       #s_crmcloudsync_ltf_club_inserts.ltf_kid_memberships,
       #s_crmcloudsync_ltf_club_inserts.ltf_buddy_reset_period,
       #s_crmcloudsync_ltf_club_inserts.ltf_restricted_guest_pass_policy,
       #s_crmcloudsync_ltf_club_inserts.ltf_restricted_pass_reset_period,
       #s_crmcloudsync_ltf_club_inserts.ltf_restricted_pass_reset_units,
       #s_crmcloudsync_ltf_club_inserts.ltf_restricted_pass_grace_period,
       #s_crmcloudsync_ltf_club_inserts.ltf_restricted_pass_grace_units,
       #s_crmcloudsync_ltf_club_inserts.ltf_web_guest_pass_process,
       #s_crmcloudsync_ltf_club_inserts.ltf_lt_work_email,
       case when s_crmcloudsync_ltf_club.s_crmcloudsync_ltf_club_id is null then isnull(#s_crmcloudsync_ltf_club_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_club_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_club_inserts
  left join p_crmcloudsync_ltf_club
    on #s_crmcloudsync_ltf_club_inserts.bk_hash = p_crmcloudsync_ltf_club.bk_hash
   and p_crmcloudsync_ltf_club.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_club
    on p_crmcloudsync_ltf_club.bk_hash = s_crmcloudsync_ltf_club.bk_hash
   and p_crmcloudsync_ltf_club.s_crmcloudsync_ltf_club_id = s_crmcloudsync_ltf_club.s_crmcloudsync_ltf_club_id
 where s_crmcloudsync_ltf_club.s_crmcloudsync_ltf_club_id is null
    or (s_crmcloudsync_ltf_club.s_crmcloudsync_ltf_club_id is not null
        and s_crmcloudsync_ltf_club.dv_hash <> #s_crmcloudsync_ltf_club_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_club @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_club @current_dv_batch_id

end
