CREATE PROC [dbo].[proc_etl_sfmc_content_details_log] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_sfmc_content_details_log

set @insert_date_time = getdate()
insert into dbo.stage_hash_sfmc_content_details_log (
       bk_hash,
       ContentID,
       Section,
       Priority,
       SubjectLine,
       Headline,
       Body,
       ImageURL,
       CTA_Text,
       CTA_URL,
       MembershipType,
       ClubIDs,
       Interests,
       LTBucks,
       WLChallenge,
       KidsChoice,
       AdditionalLogic,
       ContentType,
       ET_InsertedDateTime,
       ContentGUID,
       EventDate,
       CompanyInside,
       Subhead,
       interest_id,
       NotificationType,
       NotificationTypeID,
       Channel,
       Gender,
       CreatedBy,
       UpdatedBy,
       InternalNotes,
       CreatedDateTime,
       LastUpdatedDateTime,
       PublishDateTime,
       SwipeStartDate,
       ActiveJuniorFlag,
       ChildCenterSwipedFlag,
       MessageActiveFlag,
       ExpirationDateTime,
       Category,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ContentGUID,'z#@$k%&P'))),2) bk_hash,
       ContentID,
       Section,
       Priority,
       SubjectLine,
       Headline,
       Body,
       ImageURL,
       CTA_Text,
       CTA_URL,
       MembershipType,
       ClubIDs,
       Interests,
       LTBucks,
       WLChallenge,
       KidsChoice,
       AdditionalLogic,
       ContentType,
       ET_InsertedDateTime,
       ContentGUID,
       EventDate,
       CompanyInside,
       Subhead,
       interest_id,
       NotificationType,
       NotificationTypeID,
       Channel,
       Gender,
       CreatedBy,
       UpdatedBy,
       InternalNotes,
       CreatedDateTime,
       LastUpdatedDateTime,
       PublishDateTime,
       SwipeStartDate,
       ActiveJuniorFlag,
       ChildCenterSwipedFlag,
       MessageActiveFlag,
       ExpirationDateTime,
       Category,
       isnull(cast(stage_sfmc_content_details_log.CreatedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_sfmc_content_details_log
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_sfmc_content_details_log @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_sfmc_content_details_log (
       bk_hash,
       content_guid,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_sfmc_content_details_log.bk_hash,
       stage_hash_sfmc_content_details_log.ContentGUID content_guid,
       isnull(cast(stage_hash_sfmc_content_details_log.CreatedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       19,
       @insert_date_time,
       @user
  from stage_hash_sfmc_content_details_log
  left join h_sfmc_content_details_log
    on stage_hash_sfmc_content_details_log.bk_hash = h_sfmc_content_details_log.bk_hash
 where h_sfmc_content_details_log_id is null
   and stage_hash_sfmc_content_details_log.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_sfmc_content_details_log
if object_id('tempdb..#l_sfmc_content_details_log_inserts') is not null drop table #l_sfmc_content_details_log_inserts
create table #l_sfmc_content_details_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_sfmc_content_details_log.bk_hash,
       stage_hash_sfmc_content_details_log.ContentID content_id,
       stage_hash_sfmc_content_details_log.ClubIDs club_ids,
       stage_hash_sfmc_content_details_log.ContentGUID content_guid,
       stage_hash_sfmc_content_details_log.interest_id interest_id,
       stage_hash_sfmc_content_details_log.NotificationTypeID notification_type_id,
       isnull(cast(stage_hash_sfmc_content_details_log.CreatedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.ContentID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.ClubIDs,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.ContentGUID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.interest_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.NotificationTypeID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_sfmc_content_details_log
 where stage_hash_sfmc_content_details_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_sfmc_content_details_log records
set @insert_date_time = getdate()
insert into l_sfmc_content_details_log (
       bk_hash,
       content_id,
       club_ids,
       content_guid,
       interest_id,
       notification_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_sfmc_content_details_log_inserts.bk_hash,
       #l_sfmc_content_details_log_inserts.content_id,
       #l_sfmc_content_details_log_inserts.club_ids,
       #l_sfmc_content_details_log_inserts.content_guid,
       #l_sfmc_content_details_log_inserts.interest_id,
       #l_sfmc_content_details_log_inserts.notification_type_id,
       case when l_sfmc_content_details_log.l_sfmc_content_details_log_id is null then isnull(#l_sfmc_content_details_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #l_sfmc_content_details_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_sfmc_content_details_log_inserts
  left join p_sfmc_content_details_log
    on #l_sfmc_content_details_log_inserts.bk_hash = p_sfmc_content_details_log.bk_hash
   and p_sfmc_content_details_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_sfmc_content_details_log
    on p_sfmc_content_details_log.bk_hash = l_sfmc_content_details_log.bk_hash
   and p_sfmc_content_details_log.l_sfmc_content_details_log_id = l_sfmc_content_details_log.l_sfmc_content_details_log_id
 where l_sfmc_content_details_log.l_sfmc_content_details_log_id is null
    or (l_sfmc_content_details_log.l_sfmc_content_details_log_id is not null
        and l_sfmc_content_details_log.dv_hash <> #l_sfmc_content_details_log_inserts.source_hash)

--calculate hash and lookup to current s_sfmc_content_details_log
if object_id('tempdb..#s_sfmc_content_details_log_inserts') is not null drop table #s_sfmc_content_details_log_inserts
create table #s_sfmc_content_details_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_sfmc_content_details_log.bk_hash,
       stage_hash_sfmc_content_details_log.Section section,
       stage_hash_sfmc_content_details_log.Priority priority,
       stage_hash_sfmc_content_details_log.SubjectLine subject_line,
       stage_hash_sfmc_content_details_log.Headline headline,
       stage_hash_sfmc_content_details_log.Body body,
       stage_hash_sfmc_content_details_log.ImageURL image_url,
       stage_hash_sfmc_content_details_log.CTA_Text cta_text,
       stage_hash_sfmc_content_details_log.CTA_URL cta_url,
       stage_hash_sfmc_content_details_log.MembershipType membership_type,
       stage_hash_sfmc_content_details_log.Interests interests,
       stage_hash_sfmc_content_details_log.LTBucks lt_bucks,
       stage_hash_sfmc_content_details_log.WLChallenge wl_challenge,
       stage_hash_sfmc_content_details_log.KidsChoice kids_choice,
       stage_hash_sfmc_content_details_log.AdditionalLogic additional_logic,
       stage_hash_sfmc_content_details_log.ContentType content_type,
       stage_hash_sfmc_content_details_log.ET_InsertedDateTime et_inserted_date_time,
       stage_hash_sfmc_content_details_log.ContentGUID content_guid,
       stage_hash_sfmc_content_details_log.EventDate event_date,
       stage_hash_sfmc_content_details_log.CompanyInside company_inside,
       stage_hash_sfmc_content_details_log.Subhead subhead,
       stage_hash_sfmc_content_details_log.NotificationType notification_type,
       stage_hash_sfmc_content_details_log.Channel channel,
       stage_hash_sfmc_content_details_log.Gender gender,
       stage_hash_sfmc_content_details_log.CreatedBy created_by,
       stage_hash_sfmc_content_details_log.UpdatedBy updated_by,
       stage_hash_sfmc_content_details_log.InternalNotes internal_notes,
       stage_hash_sfmc_content_details_log.CreatedDateTime created_date_time,
       stage_hash_sfmc_content_details_log.LastUpdatedDateTime last_updated_date_time,
       stage_hash_sfmc_content_details_log.PublishDateTime publish_date_time,
       stage_hash_sfmc_content_details_log.SwipeStartDate swipe_start_date,
       stage_hash_sfmc_content_details_log.ActiveJuniorFlag active_junior_flag,
       stage_hash_sfmc_content_details_log.ChildCenterSwipedFlag child_center_swiped_flag,
       stage_hash_sfmc_content_details_log.MessageActiveFlag message_active_flag,
       stage_hash_sfmc_content_details_log.ExpirationDateTime expiration_date_time,
       stage_hash_sfmc_content_details_log.Category category,
       isnull(cast(stage_hash_sfmc_content_details_log.CreatedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Section,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Priority,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.SubjectLine,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Headline,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Body,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.ImageURL,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.CTA_Text,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.CTA_URL,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.MembershipType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Interests,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.LTBucks,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.WLChallenge,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.KidsChoice,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.AdditionalLogic,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.ContentType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_sfmc_content_details_log.ET_InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.ContentGUID,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_sfmc_content_details_log.EventDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.CompanyInside,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Subhead,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.NotificationType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Channel,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Gender,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.CreatedBy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.UpdatedBy,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.InternalNotes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_sfmc_content_details_log.CreatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_sfmc_content_details_log.LastUpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_sfmc_content_details_log.PublishDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_sfmc_content_details_log.SwipeStartDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.ActiveJuniorFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.ChildCenterSwipedFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.MessageActiveFlag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_sfmc_content_details_log.ExpirationDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_sfmc_content_details_log.Category,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_sfmc_content_details_log
 where stage_hash_sfmc_content_details_log.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_sfmc_content_details_log records
set @insert_date_time = getdate()
insert into s_sfmc_content_details_log (
       bk_hash,
       section,
       priority,
       subject_line,
       headline,
       body,
       image_url,
       cta_text,
       cta_url,
       membership_type,
       interests,
       lt_bucks,
       wl_challenge,
       kids_choice,
       additional_logic,
       content_type,
       et_inserted_date_time,
       content_guid,
       event_date,
       company_inside,
       subhead,
       notification_type,
       channel,
       gender,
       created_by,
       updated_by,
       internal_notes,
       created_date_time,
       last_updated_date_time,
       publish_date_time,
       swipe_start_date,
       active_junior_flag,
       child_center_swiped_flag,
       message_active_flag,
       expiration_date_time,
       category,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_sfmc_content_details_log_inserts.bk_hash,
       #s_sfmc_content_details_log_inserts.section,
       #s_sfmc_content_details_log_inserts.priority,
       #s_sfmc_content_details_log_inserts.subject_line,
       #s_sfmc_content_details_log_inserts.headline,
       #s_sfmc_content_details_log_inserts.body,
       #s_sfmc_content_details_log_inserts.image_url,
       #s_sfmc_content_details_log_inserts.cta_text,
       #s_sfmc_content_details_log_inserts.cta_url,
       #s_sfmc_content_details_log_inserts.membership_type,
       #s_sfmc_content_details_log_inserts.interests,
       #s_sfmc_content_details_log_inserts.lt_bucks,
       #s_sfmc_content_details_log_inserts.wl_challenge,
       #s_sfmc_content_details_log_inserts.kids_choice,
       #s_sfmc_content_details_log_inserts.additional_logic,
       #s_sfmc_content_details_log_inserts.content_type,
       #s_sfmc_content_details_log_inserts.et_inserted_date_time,
       #s_sfmc_content_details_log_inserts.content_guid,
       #s_sfmc_content_details_log_inserts.event_date,
       #s_sfmc_content_details_log_inserts.company_inside,
       #s_sfmc_content_details_log_inserts.subhead,
       #s_sfmc_content_details_log_inserts.notification_type,
       #s_sfmc_content_details_log_inserts.channel,
       #s_sfmc_content_details_log_inserts.gender,
       #s_sfmc_content_details_log_inserts.created_by,
       #s_sfmc_content_details_log_inserts.updated_by,
       #s_sfmc_content_details_log_inserts.internal_notes,
       #s_sfmc_content_details_log_inserts.created_date_time,
       #s_sfmc_content_details_log_inserts.last_updated_date_time,
       #s_sfmc_content_details_log_inserts.publish_date_time,
       #s_sfmc_content_details_log_inserts.swipe_start_date,
       #s_sfmc_content_details_log_inserts.active_junior_flag,
       #s_sfmc_content_details_log_inserts.child_center_swiped_flag,
       #s_sfmc_content_details_log_inserts.message_active_flag,
       #s_sfmc_content_details_log_inserts.expiration_date_time,
       #s_sfmc_content_details_log_inserts.category,
       case when s_sfmc_content_details_log.s_sfmc_content_details_log_id is null then isnull(#s_sfmc_content_details_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       19,
       #s_sfmc_content_details_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_sfmc_content_details_log_inserts
  left join p_sfmc_content_details_log
    on #s_sfmc_content_details_log_inserts.bk_hash = p_sfmc_content_details_log.bk_hash
   and p_sfmc_content_details_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_sfmc_content_details_log
    on p_sfmc_content_details_log.bk_hash = s_sfmc_content_details_log.bk_hash
   and p_sfmc_content_details_log.s_sfmc_content_details_log_id = s_sfmc_content_details_log.s_sfmc_content_details_log_id
 where s_sfmc_content_details_log.s_sfmc_content_details_log_id is null
    or (s_sfmc_content_details_log.s_sfmc_content_details_log_id is not null
        and s_sfmc_content_details_log.dv_hash <> #s_sfmc_content_details_log_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_sfmc_content_details_log @current_dv_batch_id

end
