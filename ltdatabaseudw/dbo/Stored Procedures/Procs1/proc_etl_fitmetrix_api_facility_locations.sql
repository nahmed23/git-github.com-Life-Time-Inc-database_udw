CREATE PROC [dbo].[proc_etl_fitmetrix_api_facility_locations] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_fitmetrix_api_facility_locations

set @insert_date_time = getdate()
insert into dbo.stage_hash_fitmetrix_api_facility_locations (
       bk_hash,
       FACILITYLOCATIONID,
       FACILITYID,
       STREET1,
       CITY,
       STATE,
       ZIP,
       COUNTRY,
       PHONE,
       STREET2,
       HOURS,
       EMAIL,
       MANAGER,
       EXTERNALID,
       DESCRIPTION,
       LATITUDE,
       LONGITUDE,
       SERVERTIMEOFFSET,
       NAME,
       PHONEEXT,
       BOOKINGURL,
       CHECKOUTURL,
       RATINGURL,
       SOCIALURL,
       CLASSDETAILURL,
       LOCATIONURL,
       EMAILFROMNAME,
       HIDEINPORTAL,
       DATEFORMAT,
       LOCATIONBOOKINGWINDOW,
       DISPLAYORDER,
       ICSENABLED,
       BOOKINGCONVERSION,
       PURCHASECONVERSION,
       MAILCHIMPAPIKEY,
       MAILCHIMPLISTID,
       MAILCHIMPENABLED,
       PACKAGEHEADER,
       PACKAGEFOOTER,
       SUNDAYHOURS,
       MONDAYHOURS,
       TUESDAYHOURS,
       WEDNESDAYHOURS,
       THURSDAYHOURS,
       FRIDAYHOURS,
       SATURDAYHOURS,
       TWITTERURL,
       FACEBOOKURL,
       CHECKOUTHEADER,
       CHECKOUTFOOTER,
       CHECKOUTCALLOUT,
       PICKASPOTHEADER,
       PICKASPOTFOOTER,
       PICKASPOTCALLOUT,
       ANNOUNCEMENTTITLE,
       ANNOUNCEMENTBODY,
       ANNOUNCEMENTLINK,
       ANNOUNCEMENTLINKTEXT,
       ANNOUNCEMENT,
       BOOKINGNOTES,
       STRIPEAPIKEY,
       INSTAGRAMURL,
       ISAPPROVAL,
       SENDSUBTEXTMESSAGES,
       NOTIFYPARTICIPANTS,
       EXTERNALID2,
       CRMCLUBID,
       NOTIFYMEMBERSOFFAVINSTRUCTORSUB,
       FacilityLocationActivities,
       APPSCHEDULEURL,
       REDIRECTAPPID,
       GUESTPASSLIMIT,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(FACILITYLOCATIONID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       FACILITYLOCATIONID,
       FACILITYID,
       STREET1,
       CITY,
       STATE,
       ZIP,
       COUNTRY,
       PHONE,
       STREET2,
       HOURS,
       EMAIL,
       MANAGER,
       EXTERNALID,
       DESCRIPTION,
       LATITUDE,
       LONGITUDE,
       SERVERTIMEOFFSET,
       NAME,
       PHONEEXT,
       BOOKINGURL,
       CHECKOUTURL,
       RATINGURL,
       SOCIALURL,
       CLASSDETAILURL,
       LOCATIONURL,
       EMAILFROMNAME,
       HIDEINPORTAL,
       DATEFORMAT,
       LOCATIONBOOKINGWINDOW,
       DISPLAYORDER,
       ICSENABLED,
       BOOKINGCONVERSION,
       PURCHASECONVERSION,
       MAILCHIMPAPIKEY,
       MAILCHIMPLISTID,
       MAILCHIMPENABLED,
       PACKAGEHEADER,
       PACKAGEFOOTER,
       SUNDAYHOURS,
       MONDAYHOURS,
       TUESDAYHOURS,
       WEDNESDAYHOURS,
       THURSDAYHOURS,
       FRIDAYHOURS,
       SATURDAYHOURS,
       TWITTERURL,
       FACEBOOKURL,
       CHECKOUTHEADER,
       CHECKOUTFOOTER,
       CHECKOUTCALLOUT,
       PICKASPOTHEADER,
       PICKASPOTFOOTER,
       PICKASPOTCALLOUT,
       ANNOUNCEMENTTITLE,
       ANNOUNCEMENTBODY,
       ANNOUNCEMENTLINK,
       ANNOUNCEMENTLINKTEXT,
       ANNOUNCEMENT,
       BOOKINGNOTES,
       STRIPEAPIKEY,
       INSTAGRAMURL,
       ISAPPROVAL,
       SENDSUBTEXTMESSAGES,
       NOTIFYPARTICIPANTS,
       EXTERNALID2,
       CRMCLUBID,
       NOTIFYMEMBERSOFFAVINSTRUCTORSUB,
       FacilityLocationActivities,
       APPSCHEDULEURL,
       REDIRECTAPPID,
       GUESTPASSLIMIT,
       dummy_modified_date_time,
       isnull(cast(stage_fitmetrix_api_facility_locations.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_fitmetrix_api_facility_locations
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_fitmetrix_api_facility_locations @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_fitmetrix_api_facility_locations (
       bk_hash,
       facility_location_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_fitmetrix_api_facility_locations.bk_hash,
       stage_hash_fitmetrix_api_facility_locations.FACILITYLOCATIONID facility_location_id,
       isnull(cast(stage_hash_fitmetrix_api_facility_locations.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       29,
       @insert_date_time,
       @user
  from stage_hash_fitmetrix_api_facility_locations
  left join h_fitmetrix_api_facility_locations
    on stage_hash_fitmetrix_api_facility_locations.bk_hash = h_fitmetrix_api_facility_locations.bk_hash
 where h_fitmetrix_api_facility_locations_id is null
   and stage_hash_fitmetrix_api_facility_locations.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_fitmetrix_api_facility_locations
if object_id('tempdb..#l_fitmetrix_api_facility_locations_inserts') is not null drop table #l_fitmetrix_api_facility_locations_inserts
create table #l_fitmetrix_api_facility_locations_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_facility_locations.bk_hash,
       stage_hash_fitmetrix_api_facility_locations.FACILITYLOCATIONID facility_location_id,
       stage_hash_fitmetrix_api_facility_locations.FACILITYID facility_id,
       stage_hash_fitmetrix_api_facility_locations.EXTERNALID external_id,
       stage_hash_fitmetrix_api_facility_locations.MAILCHIMPLISTID mail_chimp_list_id,
       stage_hash_fitmetrix_api_facility_locations.EXTERNALID2 external_id_2,
       stage_hash_fitmetrix_api_facility_locations.CRMCLUBID crm_club_id,
       stage_hash_fitmetrix_api_facility_locations.REDIRECTAPPID redirect_app_id,
       isnull(cast(stage_hash_fitmetrix_api_facility_locations.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_locations.FACILITYLOCATIONID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_locations.FACILITYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.EXTERNALID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.MAILCHIMPLISTID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.EXTERNALID2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.CRMCLUBID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.REDIRECTAPPID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_facility_locations
 where stage_hash_fitmetrix_api_facility_locations.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_fitmetrix_api_facility_locations records
set @insert_date_time = getdate()
insert into l_fitmetrix_api_facility_locations (
       bk_hash,
       facility_location_id,
       facility_id,
       external_id,
       mail_chimp_list_id,
       external_id_2,
       crm_club_id,
       redirect_app_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_fitmetrix_api_facility_locations_inserts.bk_hash,
       #l_fitmetrix_api_facility_locations_inserts.facility_location_id,
       #l_fitmetrix_api_facility_locations_inserts.facility_id,
       #l_fitmetrix_api_facility_locations_inserts.external_id,
       #l_fitmetrix_api_facility_locations_inserts.mail_chimp_list_id,
       #l_fitmetrix_api_facility_locations_inserts.external_id_2,
       #l_fitmetrix_api_facility_locations_inserts.crm_club_id,
       #l_fitmetrix_api_facility_locations_inserts.redirect_app_id,
       case when l_fitmetrix_api_facility_locations.l_fitmetrix_api_facility_locations_id is null then isnull(#l_fitmetrix_api_facility_locations_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #l_fitmetrix_api_facility_locations_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_fitmetrix_api_facility_locations_inserts
  left join p_fitmetrix_api_facility_locations
    on #l_fitmetrix_api_facility_locations_inserts.bk_hash = p_fitmetrix_api_facility_locations.bk_hash
   and p_fitmetrix_api_facility_locations.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_fitmetrix_api_facility_locations
    on p_fitmetrix_api_facility_locations.bk_hash = l_fitmetrix_api_facility_locations.bk_hash
   and p_fitmetrix_api_facility_locations.l_fitmetrix_api_facility_locations_id = l_fitmetrix_api_facility_locations.l_fitmetrix_api_facility_locations_id
 where l_fitmetrix_api_facility_locations.l_fitmetrix_api_facility_locations_id is null
    or (l_fitmetrix_api_facility_locations.l_fitmetrix_api_facility_locations_id is not null
        and l_fitmetrix_api_facility_locations.dv_hash <> #l_fitmetrix_api_facility_locations_inserts.source_hash)

--calculate hash and lookup to current s_fitmetrix_api_facility_locations
if object_id('tempdb..#s_fitmetrix_api_facility_locations_inserts') is not null drop table #s_fitmetrix_api_facility_locations_inserts
create table #s_fitmetrix_api_facility_locations_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_facility_locations.bk_hash,
       stage_hash_fitmetrix_api_facility_locations.FACILITYLOCATIONID facility_location_id,
       stage_hash_fitmetrix_api_facility_locations.STREET1 street_1,
       stage_hash_fitmetrix_api_facility_locations.CITY city,
       stage_hash_fitmetrix_api_facility_locations.STATE state,
       stage_hash_fitmetrix_api_facility_locations.ZIP zip,
       stage_hash_fitmetrix_api_facility_locations.COUNTRY country,
       stage_hash_fitmetrix_api_facility_locations.PHONE phone,
       stage_hash_fitmetrix_api_facility_locations.STREET2 street_2,
       stage_hash_fitmetrix_api_facility_locations.HOURS hours,
       stage_hash_fitmetrix_api_facility_locations.EMAIL email,
       stage_hash_fitmetrix_api_facility_locations.MANAGER manager,
       stage_hash_fitmetrix_api_facility_locations.DESCRIPTION description,
       stage_hash_fitmetrix_api_facility_locations.LATITUDE latitude,
       stage_hash_fitmetrix_api_facility_locations.LONGITUDE longitude,
       stage_hash_fitmetrix_api_facility_locations.SERVERTIMEOFFSET server_time_offset,
       stage_hash_fitmetrix_api_facility_locations.NAME name,
       stage_hash_fitmetrix_api_facility_locations.PHONEEXT phone_ext,
       stage_hash_fitmetrix_api_facility_locations.BOOKINGURL booking_url,
       stage_hash_fitmetrix_api_facility_locations.CHECKOUTURL check_out_url,
       stage_hash_fitmetrix_api_facility_locations.RATINGURL rating_url,
       stage_hash_fitmetrix_api_facility_locations.SOCIALURL social_url,
       stage_hash_fitmetrix_api_facility_locations.CLASSDETAILURL class_detail_url,
       stage_hash_fitmetrix_api_facility_locations.LOCATIONURL location_url,
       stage_hash_fitmetrix_api_facility_locations.EMAILFROMNAME email_from_name,
       stage_hash_fitmetrix_api_facility_locations.HIDEINPORTAL hide_in_portal,
       stage_hash_fitmetrix_api_facility_locations.DATEFORMAT date_format,
       stage_hash_fitmetrix_api_facility_locations.LOCATIONBOOKINGWINDOW location_booking_window,
       stage_hash_fitmetrix_api_facility_locations.DISPLAYORDER display_order,
       stage_hash_fitmetrix_api_facility_locations.ICSENABLED ics_enabled,
       stage_hash_fitmetrix_api_facility_locations.BOOKINGCONVERSION booking_conversion,
       stage_hash_fitmetrix_api_facility_locations.PURCHASECONVERSION purchase_conversion,
       stage_hash_fitmetrix_api_facility_locations.MAILCHIMPAPIKEY mail_chimp_api_key,
       stage_hash_fitmetrix_api_facility_locations.MAILCHIMPENABLED mail_chimp_enabled,
       stage_hash_fitmetrix_api_facility_locations.PACKAGEHEADER package_header,
       stage_hash_fitmetrix_api_facility_locations.PACKAGEFOOTER package_footer,
       stage_hash_fitmetrix_api_facility_locations.SUNDAYHOURS sunday_hours,
       stage_hash_fitmetrix_api_facility_locations.MONDAYHOURS monday_hours,
       stage_hash_fitmetrix_api_facility_locations.TUESDAYHOURS tuesday_hours,
       stage_hash_fitmetrix_api_facility_locations.WEDNESDAYHOURS wednesday_hours,
       stage_hash_fitmetrix_api_facility_locations.THURSDAYHOURS thursday_hours,
       stage_hash_fitmetrix_api_facility_locations.FRIDAYHOURS friday_hours,
       stage_hash_fitmetrix_api_facility_locations.SATURDAYHOURS saturday_hours,
       stage_hash_fitmetrix_api_facility_locations.TWITTERURL twitter_url,
       stage_hash_fitmetrix_api_facility_locations.FACEBOOKURL facebook_url,
       stage_hash_fitmetrix_api_facility_locations.CHECKOUTHEADER check_out_header,
       stage_hash_fitmetrix_api_facility_locations.CHECKOUTFOOTER check_out_footer,
       stage_hash_fitmetrix_api_facility_locations.CHECKOUTCALLOUT check_out_call_out,
       stage_hash_fitmetrix_api_facility_locations.PICKASPOTHEADER pick_a_spot_header,
       stage_hash_fitmetrix_api_facility_locations.PICKASPOTFOOTER pick_a_spot_footer,
       stage_hash_fitmetrix_api_facility_locations.PICKASPOTCALLOUT pick_a_spot_call_out,
       stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENTTITLE announcement_title,
       stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENTBODY announcement_body,
       stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENTLINK announcement_link,
       stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENTLINKTEXT announcement_link_text,
       stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENT announcement,
       stage_hash_fitmetrix_api_facility_locations.BOOKINGNOTES booking_notes,
       stage_hash_fitmetrix_api_facility_locations.STRIPEAPIKEY stripe_api_key,
       stage_hash_fitmetrix_api_facility_locations.INSTAGRAMURL instagram_url,
       stage_hash_fitmetrix_api_facility_locations.ISAPPROVAL is_approval,
       stage_hash_fitmetrix_api_facility_locations.SENDSUBTEXTMESSAGES send_sub_text_messages,
       stage_hash_fitmetrix_api_facility_locations.NOTIFYPARTICIPANTS notify_participants,
       stage_hash_fitmetrix_api_facility_locations.NOTIFYMEMBERSOFFAVINSTRUCTORSUB notify_members_of_fav_instructor_sub,
       stage_hash_fitmetrix_api_facility_locations.FacilityLocationActivities facility_location_activities,
       stage_hash_fitmetrix_api_facility_locations.APPSCHEDULEURL app_schedule_url,
       stage_hash_fitmetrix_api_facility_locations.GUESTPASSLIMIT guest_pass_limit,
       stage_hash_fitmetrix_api_facility_locations.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_fitmetrix_api_facility_locations.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_locations.FACILITYLOCATIONID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.STREET1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.CITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.STATE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.COUNTRY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.PHONE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.STREET2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.HOURS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.EMAIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.MANAGER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.DESCRIPTION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.LATITUDE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.LONGITUDE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_locations.SERVERTIMEOFFSET as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.PHONEEXT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.BOOKINGURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.CHECKOUTURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.RATINGURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.SOCIALURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.CLASSDETAILURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.LOCATIONURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.EMAILFROMNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.HIDEINPORTAL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.DATEFORMAT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_locations.LOCATIONBOOKINGWINDOW as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_locations.DISPLAYORDER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.ICSENABLED,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.BOOKINGCONVERSION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.PURCHASECONVERSION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.MAILCHIMPAPIKEY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.MAILCHIMPENABLED,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.PACKAGEHEADER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.PACKAGEFOOTER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.SUNDAYHOURS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.MONDAYHOURS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.TUESDAYHOURS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.WEDNESDAYHOURS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.THURSDAYHOURS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.FRIDAYHOURS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.SATURDAYHOURS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.TWITTERURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.FACEBOOKURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.CHECKOUTHEADER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.CHECKOUTFOOTER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.CHECKOUTCALLOUT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.PICKASPOTHEADER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.PICKASPOTFOOTER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.PICKASPOTCALLOUT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENTTITLE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENTBODY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENTLINK,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENTLINKTEXT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.ANNOUNCEMENT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.BOOKINGNOTES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.STRIPEAPIKEY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.INSTAGRAMURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.ISAPPROVAL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.SENDSUBTEXTMESSAGES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.NOTIFYPARTICIPANTS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.NOTIFYMEMBERSOFFAVINSTRUCTORSUB,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.FacilityLocationActivities,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_facility_locations.APPSCHEDULEURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_facility_locations.GUESTPASSLIMIT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_fitmetrix_api_facility_locations.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_facility_locations
 where stage_hash_fitmetrix_api_facility_locations.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_fitmetrix_api_facility_locations records
set @insert_date_time = getdate()
insert into s_fitmetrix_api_facility_locations (
       bk_hash,
       facility_location_id,
       street_1,
       city,
       state,
       zip,
       country,
       phone,
       street_2,
       hours,
       email,
       manager,
       description,
       latitude,
       longitude,
       server_time_offset,
       name,
       phone_ext,
       booking_url,
       check_out_url,
       rating_url,
       social_url,
       class_detail_url,
       location_url,
       email_from_name,
       hide_in_portal,
       date_format,
       location_booking_window,
       display_order,
       ics_enabled,
       booking_conversion,
       purchase_conversion,
       mail_chimp_api_key,
       mail_chimp_enabled,
       package_header,
       package_footer,
       sunday_hours,
       monday_hours,
       tuesday_hours,
       wednesday_hours,
       thursday_hours,
       friday_hours,
       saturday_hours,
       twitter_url,
       facebook_url,
       check_out_header,
       check_out_footer,
       check_out_call_out,
       pick_a_spot_header,
       pick_a_spot_footer,
       pick_a_spot_call_out,
       announcement_title,
       announcement_body,
       announcement_link,
       announcement_link_text,
       announcement,
       booking_notes,
       stripe_api_key,
       instagram_url,
       is_approval,
       send_sub_text_messages,
       notify_participants,
       notify_members_of_fav_instructor_sub,
       facility_location_activities,
       app_schedule_url,
       guest_pass_limit,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_fitmetrix_api_facility_locations_inserts.bk_hash,
       #s_fitmetrix_api_facility_locations_inserts.facility_location_id,
       #s_fitmetrix_api_facility_locations_inserts.street_1,
       #s_fitmetrix_api_facility_locations_inserts.city,
       #s_fitmetrix_api_facility_locations_inserts.state,
       #s_fitmetrix_api_facility_locations_inserts.zip,
       #s_fitmetrix_api_facility_locations_inserts.country,
       #s_fitmetrix_api_facility_locations_inserts.phone,
       #s_fitmetrix_api_facility_locations_inserts.street_2,
       #s_fitmetrix_api_facility_locations_inserts.hours,
       #s_fitmetrix_api_facility_locations_inserts.email,
       #s_fitmetrix_api_facility_locations_inserts.manager,
       #s_fitmetrix_api_facility_locations_inserts.description,
       #s_fitmetrix_api_facility_locations_inserts.latitude,
       #s_fitmetrix_api_facility_locations_inserts.longitude,
       #s_fitmetrix_api_facility_locations_inserts.server_time_offset,
       #s_fitmetrix_api_facility_locations_inserts.name,
       #s_fitmetrix_api_facility_locations_inserts.phone_ext,
       #s_fitmetrix_api_facility_locations_inserts.booking_url,
       #s_fitmetrix_api_facility_locations_inserts.check_out_url,
       #s_fitmetrix_api_facility_locations_inserts.rating_url,
       #s_fitmetrix_api_facility_locations_inserts.social_url,
       #s_fitmetrix_api_facility_locations_inserts.class_detail_url,
       #s_fitmetrix_api_facility_locations_inserts.location_url,
       #s_fitmetrix_api_facility_locations_inserts.email_from_name,
       #s_fitmetrix_api_facility_locations_inserts.hide_in_portal,
       #s_fitmetrix_api_facility_locations_inserts.date_format,
       #s_fitmetrix_api_facility_locations_inserts.location_booking_window,
       #s_fitmetrix_api_facility_locations_inserts.display_order,
       #s_fitmetrix_api_facility_locations_inserts.ics_enabled,
       #s_fitmetrix_api_facility_locations_inserts.booking_conversion,
       #s_fitmetrix_api_facility_locations_inserts.purchase_conversion,
       #s_fitmetrix_api_facility_locations_inserts.mail_chimp_api_key,
       #s_fitmetrix_api_facility_locations_inserts.mail_chimp_enabled,
       #s_fitmetrix_api_facility_locations_inserts.package_header,
       #s_fitmetrix_api_facility_locations_inserts.package_footer,
       #s_fitmetrix_api_facility_locations_inserts.sunday_hours,
       #s_fitmetrix_api_facility_locations_inserts.monday_hours,
       #s_fitmetrix_api_facility_locations_inserts.tuesday_hours,
       #s_fitmetrix_api_facility_locations_inserts.wednesday_hours,
       #s_fitmetrix_api_facility_locations_inserts.thursday_hours,
       #s_fitmetrix_api_facility_locations_inserts.friday_hours,
       #s_fitmetrix_api_facility_locations_inserts.saturday_hours,
       #s_fitmetrix_api_facility_locations_inserts.twitter_url,
       #s_fitmetrix_api_facility_locations_inserts.facebook_url,
       #s_fitmetrix_api_facility_locations_inserts.check_out_header,
       #s_fitmetrix_api_facility_locations_inserts.check_out_footer,
       #s_fitmetrix_api_facility_locations_inserts.check_out_call_out,
       #s_fitmetrix_api_facility_locations_inserts.pick_a_spot_header,
       #s_fitmetrix_api_facility_locations_inserts.pick_a_spot_footer,
       #s_fitmetrix_api_facility_locations_inserts.pick_a_spot_call_out,
       #s_fitmetrix_api_facility_locations_inserts.announcement_title,
       #s_fitmetrix_api_facility_locations_inserts.announcement_body,
       #s_fitmetrix_api_facility_locations_inserts.announcement_link,
       #s_fitmetrix_api_facility_locations_inserts.announcement_link_text,
       #s_fitmetrix_api_facility_locations_inserts.announcement,
       #s_fitmetrix_api_facility_locations_inserts.booking_notes,
       #s_fitmetrix_api_facility_locations_inserts.stripe_api_key,
       #s_fitmetrix_api_facility_locations_inserts.instagram_url,
       #s_fitmetrix_api_facility_locations_inserts.is_approval,
       #s_fitmetrix_api_facility_locations_inserts.send_sub_text_messages,
       #s_fitmetrix_api_facility_locations_inserts.notify_participants,
       #s_fitmetrix_api_facility_locations_inserts.notify_members_of_fav_instructor_sub,
       #s_fitmetrix_api_facility_locations_inserts.facility_location_activities,
       #s_fitmetrix_api_facility_locations_inserts.app_schedule_url,
       #s_fitmetrix_api_facility_locations_inserts.guest_pass_limit,
       #s_fitmetrix_api_facility_locations_inserts.dummy_modified_date_time,
       case when s_fitmetrix_api_facility_locations.s_fitmetrix_api_facility_locations_id is null then isnull(#s_fitmetrix_api_facility_locations_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #s_fitmetrix_api_facility_locations_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_fitmetrix_api_facility_locations_inserts
  left join p_fitmetrix_api_facility_locations
    on #s_fitmetrix_api_facility_locations_inserts.bk_hash = p_fitmetrix_api_facility_locations.bk_hash
   and p_fitmetrix_api_facility_locations.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_fitmetrix_api_facility_locations
    on p_fitmetrix_api_facility_locations.bk_hash = s_fitmetrix_api_facility_locations.bk_hash
   and p_fitmetrix_api_facility_locations.s_fitmetrix_api_facility_locations_id = s_fitmetrix_api_facility_locations.s_fitmetrix_api_facility_locations_id
 where s_fitmetrix_api_facility_locations.s_fitmetrix_api_facility_locations_id is null
    or (s_fitmetrix_api_facility_locations.s_fitmetrix_api_facility_locations_id is not null
        and s_fitmetrix_api_facility_locations.dv_hash <> #s_fitmetrix_api_facility_locations_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_fitmetrix_api_facility_locations @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_fitmetrix_api_facility_locations @current_dv_batch_id

end
