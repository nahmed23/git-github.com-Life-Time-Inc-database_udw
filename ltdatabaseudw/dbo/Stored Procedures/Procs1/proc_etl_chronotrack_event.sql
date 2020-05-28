CREATE PROC [dbo].[proc_etl_chronotrack_event] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_chronotrack_event

set @insert_date_time = getdate()
insert into dbo.stage_hash_chronotrack_event (
       bk_hash,
       id,
       event_group_id,
       tag,
       name_prefix,
       name,
       description,
       extended_description,
       start_datetime,
       start_time,
       end_datetime,
       end_time,
       date_format,
       location_id,
       event_closed_message,
       organizer_id,
       additional_info,
       timer_id,
       site_uri,
       results_url,
       results_theme,
       facebook_url,
       reader_config,
       messaging_lock,
       live_video,
       mute_video,
       photo_price,
       entry_photo_price,
       is_published,
       is_published_athlinks,
       current_gen_id,
       wants_photo_purchasing,
       photo_purchasing_help,
       photo_service_type,
       photo_service_name,
       photo_service_url,
       show_athlete_names,
       currency_id,
       reg_version,
       auto_recalc,
       ctime,
       mtime,
       exp_photo_count,
       actual_photo_count,
       device_protocol,
       chip_disambiguation_policy,
       bazu_services,
       max_races,
       max_reg_choices,
       wants_cim,
       wants_just_others,
       wants_emergency_group,
       results_age_information,
       results_photo_tab,
       results_video_tab,
       payment_location_id,
       check_payable,
       payee,
       results_timer_name,
       results_timer_contact,
       results_remove_contact,
       enable_kiosk_forms,
       enable_web_forms,
       kiosk_form_settings,
       results_view,
       results_ranking_columns,
       expected_entry,
       penalties_view,
       is_test_event,
       min_interval_duration,
       bib_assignment,
       enable_teams,
       last_yrs_event_id,
       on_site_mode,
       onsite_server_form_redirect,
       org_fee_allocation,
       parent_event_id,
       max_reg_count,
       external_id,
       check_in,
       by_proxy,
       wants_add_address,
       is_membership,
       volt_recalc,
       event_google_tag_manager_key,
       online_payee_id,
       bib_lock,
       ask_for_tag,
       location_info,
       date_and_time_info,
       series_id,
       language_id,
       bib_validation_option,
       hide_old_form_url,
       twitter_handle,
       third_party_reg_link,
       has_hero_image,
       instagram_handle,
       use_email_header,
       hide_from_profile_page,
       clone_inventory,
       hide_results_claim_button,
       online_payee_change_timestamp,
       onsite_payee_id,
       onsite_payee_change_timestamp,
       policy_agree,
       policy_url,
       marketing_email_question,
       marketing_email_question_text,
       marketing_emails_event_name,
       marketing_consent_question,
       marketing_consent_question_text,
       unarchive_time,
       enable_logo_overlay,
       auto_sync_athlinks,
       time_sync_athlinks,
       auto_sync_athlinks_error,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       event_group_id,
       tag,
       name_prefix,
       name,
       description,
       extended_description,
       start_datetime,
       start_time,
       end_datetime,
       end_time,
       date_format,
       location_id,
       event_closed_message,
       organizer_id,
       additional_info,
       timer_id,
       site_uri,
       results_url,
       results_theme,
       facebook_url,
       reader_config,
       messaging_lock,
       live_video,
       mute_video,
       photo_price,
       entry_photo_price,
       is_published,
       is_published_athlinks,
       current_gen_id,
       wants_photo_purchasing,
       photo_purchasing_help,
       photo_service_type,
       photo_service_name,
       photo_service_url,
       show_athlete_names,
       currency_id,
       reg_version,
       auto_recalc,
       ctime,
       mtime,
       exp_photo_count,
       actual_photo_count,
       device_protocol,
       chip_disambiguation_policy,
       bazu_services,
       max_races,
       max_reg_choices,
       wants_cim,
       wants_just_others,
       wants_emergency_group,
       results_age_information,
       results_photo_tab,
       results_video_tab,
       payment_location_id,
       check_payable,
       payee,
       results_timer_name,
       results_timer_contact,
       results_remove_contact,
       enable_kiosk_forms,
       enable_web_forms,
       kiosk_form_settings,
       results_view,
       results_ranking_columns,
       expected_entry,
       penalties_view,
       is_test_event,
       min_interval_duration,
       bib_assignment,
       enable_teams,
       last_yrs_event_id,
       on_site_mode,
       onsite_server_form_redirect,
       org_fee_allocation,
       parent_event_id,
       max_reg_count,
       external_id,
       check_in,
       by_proxy,
       wants_add_address,
       is_membership,
       volt_recalc,
       event_google_tag_manager_key,
       online_payee_id,
       bib_lock,
       ask_for_tag,
       location_info,
       date_and_time_info,
       series_id,
       language_id,
       bib_validation_option,
       hide_old_form_url,
       twitter_handle,
       third_party_reg_link,
       has_hero_image,
       instagram_handle,
       use_email_header,
       hide_from_profile_page,
       clone_inventory,
       hide_results_claim_button,
       online_payee_change_timestamp,
       onsite_payee_id,
       onsite_payee_change_timestamp,
       policy_agree,
       policy_url,
       marketing_email_question,
       marketing_email_question_text,
       marketing_emails_event_name,
       marketing_consent_question,
       marketing_consent_question_text,
       unarchive_time,
       enable_logo_overlay,
       auto_sync_athlinks,
       time_sync_athlinks,
       auto_sync_athlinks_error,
       dummy_modified_date_time,
       isnull(cast(stage_chronotrack_event.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_chronotrack_event
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_chronotrack_event @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_chronotrack_event (
       bk_hash,
       event_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_chronotrack_event.bk_hash,
       stage_hash_chronotrack_event.id event_id,
       isnull(cast(stage_hash_chronotrack_event.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       46,
       @insert_date_time,
       @user
  from stage_hash_chronotrack_event
  left join h_chronotrack_event
    on stage_hash_chronotrack_event.bk_hash = h_chronotrack_event.bk_hash
 where h_chronotrack_event_id is null
   and stage_hash_chronotrack_event.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_chronotrack_event
if object_id('tempdb..#l_chronotrack_event_inserts') is not null drop table #l_chronotrack_event_inserts
create table #l_chronotrack_event_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_chronotrack_event.bk_hash,
       stage_hash_chronotrack_event.id event_id,
       stage_hash_chronotrack_event.event_group_id event_group_id,
       stage_hash_chronotrack_event.location_id location_id,
       stage_hash_chronotrack_event.organizer_id organizer_id,
       stage_hash_chronotrack_event.timer_id timer_id,
       stage_hash_chronotrack_event.currency_id currency_id,
       stage_hash_chronotrack_event.payment_location_id payment_location_id,
       stage_hash_chronotrack_event.last_yrs_event_id last_yrs_event_id,
       stage_hash_chronotrack_event.parent_event_id parent_event_id,
       stage_hash_chronotrack_event.external_id external_id,
       stage_hash_chronotrack_event.online_payee_id online_payee_id,
       stage_hash_chronotrack_event.series_id series_id,
       stage_hash_chronotrack_event.language_id language_id,
       stage_hash_chronotrack_event.onsite_payee_id onsite_payee_id,
       isnull(cast(stage_hash_chronotrack_event.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.event_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.location_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.organizer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.timer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.currency_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.payment_location_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.last_yrs_event_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.parent_event_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.external_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.online_payee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.series_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.language_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.onsite_payee_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_chronotrack_event
 where stage_hash_chronotrack_event.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_chronotrack_event records
set @insert_date_time = getdate()
insert into l_chronotrack_event (
       bk_hash,
       event_id,
       event_group_id,
       location_id,
       organizer_id,
       timer_id,
       currency_id,
       payment_location_id,
       last_yrs_event_id,
       parent_event_id,
       external_id,
       online_payee_id,
       series_id,
       language_id,
       onsite_payee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_chronotrack_event_inserts.bk_hash,
       #l_chronotrack_event_inserts.event_id,
       #l_chronotrack_event_inserts.event_group_id,
       #l_chronotrack_event_inserts.location_id,
       #l_chronotrack_event_inserts.organizer_id,
       #l_chronotrack_event_inserts.timer_id,
       #l_chronotrack_event_inserts.currency_id,
       #l_chronotrack_event_inserts.payment_location_id,
       #l_chronotrack_event_inserts.last_yrs_event_id,
       #l_chronotrack_event_inserts.parent_event_id,
       #l_chronotrack_event_inserts.external_id,
       #l_chronotrack_event_inserts.online_payee_id,
       #l_chronotrack_event_inserts.series_id,
       #l_chronotrack_event_inserts.language_id,
       #l_chronotrack_event_inserts.onsite_payee_id,
       case when l_chronotrack_event.l_chronotrack_event_id is null then isnull(#l_chronotrack_event_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       46,
       #l_chronotrack_event_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_chronotrack_event_inserts
  left join p_chronotrack_event
    on #l_chronotrack_event_inserts.bk_hash = p_chronotrack_event.bk_hash
   and p_chronotrack_event.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_chronotrack_event
    on p_chronotrack_event.bk_hash = l_chronotrack_event.bk_hash
   and p_chronotrack_event.l_chronotrack_event_id = l_chronotrack_event.l_chronotrack_event_id
 where l_chronotrack_event.l_chronotrack_event_id is null
    or (l_chronotrack_event.l_chronotrack_event_id is not null
        and l_chronotrack_event.dv_hash <> #l_chronotrack_event_inserts.source_hash)

--calculate hash and lookup to current s_chronotrack_event
if object_id('tempdb..#s_chronotrack_event_inserts') is not null drop table #s_chronotrack_event_inserts
create table #s_chronotrack_event_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_chronotrack_event.bk_hash,
       stage_hash_chronotrack_event.id event_id,
       stage_hash_chronotrack_event.tag tag,
       stage_hash_chronotrack_event.name_prefix name_prefix,
       stage_hash_chronotrack_event.name name,
       stage_hash_chronotrack_event.description description,
       stage_hash_chronotrack_event.extended_description extended_description,
       stage_hash_chronotrack_event.start_datetime start_datetime,
       stage_hash_chronotrack_event.start_time start_time,
       stage_hash_chronotrack_event.end_datetime end_datetime,
       stage_hash_chronotrack_event.end_time end_time,
       stage_hash_chronotrack_event.date_format date_format,
       stage_hash_chronotrack_event.event_closed_message event_closed_message,
       stage_hash_chronotrack_event.additional_info additional_info,
       stage_hash_chronotrack_event.site_uri site_uri,
       stage_hash_chronotrack_event.results_url results_url,
       stage_hash_chronotrack_event.results_theme results_theme,
       stage_hash_chronotrack_event.facebook_url facebook_url,
       stage_hash_chronotrack_event.reader_config reader_config,
       stage_hash_chronotrack_event.messaging_lock messaging_lock,
       stage_hash_chronotrack_event.live_video live_video,
       stage_hash_chronotrack_event.mute_video mute_video,
       stage_hash_chronotrack_event.photo_price photo_price,
       stage_hash_chronotrack_event.entry_photo_price entry_photo_price,
       stage_hash_chronotrack_event.is_published is_published,
       stage_hash_chronotrack_event.is_published_athlinks is_published_athlinks,
       stage_hash_chronotrack_event.current_gen_id current_gen_id,
       stage_hash_chronotrack_event.wants_photo_purchasing wants_photo_purchasing,
       stage_hash_chronotrack_event.photo_purchasing_help photo_purchasing_help,
       stage_hash_chronotrack_event.photo_service_type photo_service_type,
       stage_hash_chronotrack_event.photo_service_name photo_service_name,
       stage_hash_chronotrack_event.photo_service_url photo_service_url,
       stage_hash_chronotrack_event.show_athlete_names show_athlete_names,
       stage_hash_chronotrack_event.reg_version reg_version,
       stage_hash_chronotrack_event.auto_recalc auto_recalc,
       stage_hash_chronotrack_event.ctime ctime,
       stage_hash_chronotrack_event.mtime mtime,
       stage_hash_chronotrack_event.exp_photo_count exp_photo_count,
       stage_hash_chronotrack_event.actual_photo_count actual_photo_count,
       stage_hash_chronotrack_event.device_protocol device_protocol,
       stage_hash_chronotrack_event.chip_disambiguation_policy chip_disambiguation_policy,
       stage_hash_chronotrack_event.bazu_services bazu_services,
       stage_hash_chronotrack_event.max_races max_races,
       stage_hash_chronotrack_event.max_reg_choices max_reg_choices,
       stage_hash_chronotrack_event.wants_cim wants_cim,
       stage_hash_chronotrack_event.wants_just_others wants_just_others,
       stage_hash_chronotrack_event.wants_emergency_group wants_emergency_group,
       stage_hash_chronotrack_event.results_age_information results_age_information,
       stage_hash_chronotrack_event.results_photo_tab results_photo_tab,
       stage_hash_chronotrack_event.results_video_tab results_video_tab,
       stage_hash_chronotrack_event.check_payable check_payable,
       stage_hash_chronotrack_event.payee payee,
       stage_hash_chronotrack_event.results_timer_name results_timer_name,
       stage_hash_chronotrack_event.results_timer_contact results_timer_contact,
       stage_hash_chronotrack_event.results_remove_contact results_remove_contact,
       stage_hash_chronotrack_event.enable_kiosk_forms enable_kiosk_forms,
       stage_hash_chronotrack_event.enable_web_forms enable_web_forms,
       stage_hash_chronotrack_event.kiosk_form_settings kiosk_form_settings,
       stage_hash_chronotrack_event.results_view results_view,
       stage_hash_chronotrack_event.results_ranking_columns results_ranking_columns,
       stage_hash_chronotrack_event.expected_entry expected_entry,
       stage_hash_chronotrack_event.penalties_view penalties_view,
       stage_hash_chronotrack_event.is_test_event is_test_event,
       stage_hash_chronotrack_event.min_interval_duration min_interval_duration,
       stage_hash_chronotrack_event.bib_assignment bib_assignment,
       stage_hash_chronotrack_event.enable_teams enable_teams,
       stage_hash_chronotrack_event.on_site_mode on_site_mode,
       stage_hash_chronotrack_event.onsite_server_form_redirect onsite_server_form_redirect,
       stage_hash_chronotrack_event.org_fee_allocation org_fee_allocation,
       stage_hash_chronotrack_event.max_reg_count max_reg_count,
       stage_hash_chronotrack_event.check_in check_in,
       stage_hash_chronotrack_event.by_proxy by_proxy,
       stage_hash_chronotrack_event.wants_add_address wants_add_address,
       stage_hash_chronotrack_event.is_membership is_membership,
       stage_hash_chronotrack_event.volt_recalc volt_recalc,
       stage_hash_chronotrack_event.event_google_tag_manager_key event_google_tag_manager_key,
       stage_hash_chronotrack_event.bib_lock bib_lock,
       stage_hash_chronotrack_event.ask_for_tag ask_for_tag,
       stage_hash_chronotrack_event.location_info location_info,
       stage_hash_chronotrack_event.date_and_time_info date_and_time_info,
       stage_hash_chronotrack_event.bib_validation_option bib_validation_option,
       stage_hash_chronotrack_event.hide_old_form_url hide_old_form_url,
       stage_hash_chronotrack_event.twitter_handle twitter_handle,
       stage_hash_chronotrack_event.third_party_reg_link third_party_reg_link,
       stage_hash_chronotrack_event.has_hero_image has_hero_image,
       stage_hash_chronotrack_event.instagram_handle instagram_handle,
       stage_hash_chronotrack_event.use_email_header use_email_header,
       stage_hash_chronotrack_event.hide_from_profile_page hide_from_profile_page,
       stage_hash_chronotrack_event.clone_inventory clone_inventory,
       stage_hash_chronotrack_event.hide_results_claim_button hide_results_claim_button,
       stage_hash_chronotrack_event.online_payee_change_timestamp online_payee_change_timestamp,
       stage_hash_chronotrack_event.onsite_payee_change_timestamp onsite_payee_change_timestamp,
       stage_hash_chronotrack_event.policy_agree policy_agree,
       stage_hash_chronotrack_event.policy_url policy_url,
       stage_hash_chronotrack_event.marketing_email_question marketing_email_question,
       stage_hash_chronotrack_event.marketing_email_question_text marketing_email_question_text,
       stage_hash_chronotrack_event.marketing_emails_event_name marketing_emails_event_name,
       stage_hash_chronotrack_event.marketing_consent_question marketing_consent_question,
       stage_hash_chronotrack_event.marketing_consent_question_text marketing_consent_question_text,
       stage_hash_chronotrack_event.unarchive_time unarchive_time,
       stage_hash_chronotrack_event.enable_logo_overlay enable_logo_overlay,
       stage_hash_chronotrack_event.auto_sync_athlinks auto_sync_athlinks,
       stage_hash_chronotrack_event.time_sync_athlinks time_sync_athlinks,
       stage_hash_chronotrack_event.auto_sync_athlinks_error auto_sync_athlinks_error,
       stage_hash_chronotrack_event.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_chronotrack_event.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.tag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.name_prefix,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.extended_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_chronotrack_event.start_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.start_time as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_chronotrack_event.end_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.end_time as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.date_format,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.event_closed_message,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.additional_info,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.site_uri,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.results_url,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.results_theme,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.facebook_url,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.reader_config,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.messaging_lock as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.live_video,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.mute_video as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.photo_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.entry_photo_price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.is_published as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.is_published_athlinks as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.current_gen_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.wants_photo_purchasing as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.photo_purchasing_help,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.photo_service_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.photo_service_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.photo_service_url,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.show_athlete_names as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.reg_version,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.auto_recalc as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.ctime as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.mtime as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.exp_photo_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.actual_photo_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.device_protocol,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.chip_disambiguation_policy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.bazu_services,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.max_races as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.max_reg_choices as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.wants_cim as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.wants_just_others as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.wants_emergency_group,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.results_age_information,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.results_photo_tab as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.results_video_tab as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.check_payable,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.payee,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.results_timer_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.results_timer_contact,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.results_remove_contact as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.enable_kiosk_forms as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.enable_web_forms as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.kiosk_form_settings,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.results_view,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.results_ranking_columns,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.expected_entry as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.penalties_view,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.is_test_event as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.min_interval_duration as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.bib_assignment as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.enable_teams as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.on_site_mode,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.onsite_server_form_redirect,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.org_fee_allocation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.max_reg_count as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.check_in as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.by_proxy as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.wants_add_address as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.is_membership as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.volt_recalc as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.event_google_tag_manager_key,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.bib_lock as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.ask_for_tag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.location_info as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.date_and_time_info as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.bib_validation_option,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.hide_old_form_url as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.twitter_handle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.third_party_reg_link,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.has_hero_image as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.instagram_handle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.use_email_header as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.hide_from_profile_page as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.clone_inventory as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.hide_results_claim_button as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.online_payee_change_timestamp as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.onsite_payee_change_timestamp as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.policy_agree as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.policy_url,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.marketing_email_question as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.marketing_email_question_text,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.marketing_emails_event_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.marketing_consent_question as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.marketing_consent_question_text,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.unarchive_time as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.enable_logo_overlay as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.auto_sync_athlinks as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_chronotrack_event.time_sync_athlinks as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_chronotrack_event.auto_sync_athlinks_error,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_chronotrack_event
 where stage_hash_chronotrack_event.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_chronotrack_event records
set @insert_date_time = getdate()
insert into s_chronotrack_event (
       bk_hash,
       event_id,
       tag,
       name_prefix,
       name,
       description,
       extended_description,
       start_datetime,
       start_time,
       end_datetime,
       end_time,
       date_format,
       event_closed_message,
       additional_info,
       site_uri,
       results_url,
       results_theme,
       facebook_url,
       reader_config,
       messaging_lock,
       live_video,
       mute_video,
       photo_price,
       entry_photo_price,
       is_published,
       is_published_athlinks,
       current_gen_id,
       wants_photo_purchasing,
       photo_purchasing_help,
       photo_service_type,
       photo_service_name,
       photo_service_url,
       show_athlete_names,
       reg_version,
       auto_recalc,
       ctime,
       mtime,
       exp_photo_count,
       actual_photo_count,
       device_protocol,
       chip_disambiguation_policy,
       bazu_services,
       max_races,
       max_reg_choices,
       wants_cim,
       wants_just_others,
       wants_emergency_group,
       results_age_information,
       results_photo_tab,
       results_video_tab,
       check_payable,
       payee,
       results_timer_name,
       results_timer_contact,
       results_remove_contact,
       enable_kiosk_forms,
       enable_web_forms,
       kiosk_form_settings,
       results_view,
       results_ranking_columns,
       expected_entry,
       penalties_view,
       is_test_event,
       min_interval_duration,
       bib_assignment,
       enable_teams,
       on_site_mode,
       onsite_server_form_redirect,
       org_fee_allocation,
       max_reg_count,
       check_in,
       by_proxy,
       wants_add_address,
       is_membership,
       volt_recalc,
       event_google_tag_manager_key,
       bib_lock,
       ask_for_tag,
       location_info,
       date_and_time_info,
       bib_validation_option,
       hide_old_form_url,
       twitter_handle,
       third_party_reg_link,
       has_hero_image,
       instagram_handle,
       use_email_header,
       hide_from_profile_page,
       clone_inventory,
       hide_results_claim_button,
       online_payee_change_timestamp,
       onsite_payee_change_timestamp,
       policy_agree,
       policy_url,
       marketing_email_question,
       marketing_email_question_text,
       marketing_emails_event_name,
       marketing_consent_question,
       marketing_consent_question_text,
       unarchive_time,
       enable_logo_overlay,
       auto_sync_athlinks,
       time_sync_athlinks,
       auto_sync_athlinks_error,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_chronotrack_event_inserts.bk_hash,
       #s_chronotrack_event_inserts.event_id,
       #s_chronotrack_event_inserts.tag,
       #s_chronotrack_event_inserts.name_prefix,
       #s_chronotrack_event_inserts.name,
       #s_chronotrack_event_inserts.description,
       #s_chronotrack_event_inserts.extended_description,
       #s_chronotrack_event_inserts.start_datetime,
       #s_chronotrack_event_inserts.start_time,
       #s_chronotrack_event_inserts.end_datetime,
       #s_chronotrack_event_inserts.end_time,
       #s_chronotrack_event_inserts.date_format,
       #s_chronotrack_event_inserts.event_closed_message,
       #s_chronotrack_event_inserts.additional_info,
       #s_chronotrack_event_inserts.site_uri,
       #s_chronotrack_event_inserts.results_url,
       #s_chronotrack_event_inserts.results_theme,
       #s_chronotrack_event_inserts.facebook_url,
       #s_chronotrack_event_inserts.reader_config,
       #s_chronotrack_event_inserts.messaging_lock,
       #s_chronotrack_event_inserts.live_video,
       #s_chronotrack_event_inserts.mute_video,
       #s_chronotrack_event_inserts.photo_price,
       #s_chronotrack_event_inserts.entry_photo_price,
       #s_chronotrack_event_inserts.is_published,
       #s_chronotrack_event_inserts.is_published_athlinks,
       #s_chronotrack_event_inserts.current_gen_id,
       #s_chronotrack_event_inserts.wants_photo_purchasing,
       #s_chronotrack_event_inserts.photo_purchasing_help,
       #s_chronotrack_event_inserts.photo_service_type,
       #s_chronotrack_event_inserts.photo_service_name,
       #s_chronotrack_event_inserts.photo_service_url,
       #s_chronotrack_event_inserts.show_athlete_names,
       #s_chronotrack_event_inserts.reg_version,
       #s_chronotrack_event_inserts.auto_recalc,
       #s_chronotrack_event_inserts.ctime,
       #s_chronotrack_event_inserts.mtime,
       #s_chronotrack_event_inserts.exp_photo_count,
       #s_chronotrack_event_inserts.actual_photo_count,
       #s_chronotrack_event_inserts.device_protocol,
       #s_chronotrack_event_inserts.chip_disambiguation_policy,
       #s_chronotrack_event_inserts.bazu_services,
       #s_chronotrack_event_inserts.max_races,
       #s_chronotrack_event_inserts.max_reg_choices,
       #s_chronotrack_event_inserts.wants_cim,
       #s_chronotrack_event_inserts.wants_just_others,
       #s_chronotrack_event_inserts.wants_emergency_group,
       #s_chronotrack_event_inserts.results_age_information,
       #s_chronotrack_event_inserts.results_photo_tab,
       #s_chronotrack_event_inserts.results_video_tab,
       #s_chronotrack_event_inserts.check_payable,
       #s_chronotrack_event_inserts.payee,
       #s_chronotrack_event_inserts.results_timer_name,
       #s_chronotrack_event_inserts.results_timer_contact,
       #s_chronotrack_event_inserts.results_remove_contact,
       #s_chronotrack_event_inserts.enable_kiosk_forms,
       #s_chronotrack_event_inserts.enable_web_forms,
       #s_chronotrack_event_inserts.kiosk_form_settings,
       #s_chronotrack_event_inserts.results_view,
       #s_chronotrack_event_inserts.results_ranking_columns,
       #s_chronotrack_event_inserts.expected_entry,
       #s_chronotrack_event_inserts.penalties_view,
       #s_chronotrack_event_inserts.is_test_event,
       #s_chronotrack_event_inserts.min_interval_duration,
       #s_chronotrack_event_inserts.bib_assignment,
       #s_chronotrack_event_inserts.enable_teams,
       #s_chronotrack_event_inserts.on_site_mode,
       #s_chronotrack_event_inserts.onsite_server_form_redirect,
       #s_chronotrack_event_inserts.org_fee_allocation,
       #s_chronotrack_event_inserts.max_reg_count,
       #s_chronotrack_event_inserts.check_in,
       #s_chronotrack_event_inserts.by_proxy,
       #s_chronotrack_event_inserts.wants_add_address,
       #s_chronotrack_event_inserts.is_membership,
       #s_chronotrack_event_inserts.volt_recalc,
       #s_chronotrack_event_inserts.event_google_tag_manager_key,
       #s_chronotrack_event_inserts.bib_lock,
       #s_chronotrack_event_inserts.ask_for_tag,
       #s_chronotrack_event_inserts.location_info,
       #s_chronotrack_event_inserts.date_and_time_info,
       #s_chronotrack_event_inserts.bib_validation_option,
       #s_chronotrack_event_inserts.hide_old_form_url,
       #s_chronotrack_event_inserts.twitter_handle,
       #s_chronotrack_event_inserts.third_party_reg_link,
       #s_chronotrack_event_inserts.has_hero_image,
       #s_chronotrack_event_inserts.instagram_handle,
       #s_chronotrack_event_inserts.use_email_header,
       #s_chronotrack_event_inserts.hide_from_profile_page,
       #s_chronotrack_event_inserts.clone_inventory,
       #s_chronotrack_event_inserts.hide_results_claim_button,
       #s_chronotrack_event_inserts.online_payee_change_timestamp,
       #s_chronotrack_event_inserts.onsite_payee_change_timestamp,
       #s_chronotrack_event_inserts.policy_agree,
       #s_chronotrack_event_inserts.policy_url,
       #s_chronotrack_event_inserts.marketing_email_question,
       #s_chronotrack_event_inserts.marketing_email_question_text,
       #s_chronotrack_event_inserts.marketing_emails_event_name,
       #s_chronotrack_event_inserts.marketing_consent_question,
       #s_chronotrack_event_inserts.marketing_consent_question_text,
       #s_chronotrack_event_inserts.unarchive_time,
       #s_chronotrack_event_inserts.enable_logo_overlay,
       #s_chronotrack_event_inserts.auto_sync_athlinks,
       #s_chronotrack_event_inserts.time_sync_athlinks,
       #s_chronotrack_event_inserts.auto_sync_athlinks_error,
       #s_chronotrack_event_inserts.dummy_modified_date_time,
       case when s_chronotrack_event.s_chronotrack_event_id is null then isnull(#s_chronotrack_event_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       46,
       #s_chronotrack_event_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_chronotrack_event_inserts
  left join p_chronotrack_event
    on #s_chronotrack_event_inserts.bk_hash = p_chronotrack_event.bk_hash
   and p_chronotrack_event.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_chronotrack_event
    on p_chronotrack_event.bk_hash = s_chronotrack_event.bk_hash
   and p_chronotrack_event.s_chronotrack_event_id = s_chronotrack_event.s_chronotrack_event_id
 where s_chronotrack_event.s_chronotrack_event_id is null
    or (s_chronotrack_event.s_chronotrack_event_id is not null
        and s_chronotrack_event.dv_hash <> #s_chronotrack_event_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_chronotrack_event @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_chronotrack_event @current_dv_batch_id

end
