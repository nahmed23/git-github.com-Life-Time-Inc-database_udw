CREATE PROC [dbo].[proc_etl_fitmetrix_api_instructor] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_fitmetrix_api_instructor

set @insert_date_time = getdate()
insert into dbo.stage_hash_fitmetrix_api_instructor (
       bk_hash,
       INSTRUCTORID,
       FACILITYID,
       FIRSTNAME,
       LASTNAME,
       DESCRIPTION,
       IMAGE,
       EMAIL,
       STREET1,
       STREET2,
       CITY,
       STATE,
       ZIP,
       COUNTRY,
       BIO,
       HOMEPHONE,
       WORKPHONE,
       EXTERNALID,
       GENDER,
       QUOTE,
       SHOWONLINE,
       FACILITYLOCATIONID,
       ACTIVE,
       DISPLAYORDER,
       FACEBOOKURL,
       TWITTERURL,
       INSTAGRAMURL,
       SOUNDCLOUDURL,
       SPOTIFYURL,
       DELETED,
       EXTRAFIELD1,
       EXTRAFIELD2,
       EXTRAFIELD3,
       RECEIVECANCELANDREGISTEREMAIL,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(INSTRUCTORID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       INSTRUCTORID,
       FACILITYID,
       FIRSTNAME,
       LASTNAME,
       DESCRIPTION,
       IMAGE,
       EMAIL,
       STREET1,
       STREET2,
       CITY,
       STATE,
       ZIP,
       COUNTRY,
       BIO,
       HOMEPHONE,
       WORKPHONE,
       EXTERNALID,
       GENDER,
       QUOTE,
       SHOWONLINE,
       FACILITYLOCATIONID,
       ACTIVE,
       DISPLAYORDER,
       FACEBOOKURL,
       TWITTERURL,
       INSTAGRAMURL,
       SOUNDCLOUDURL,
       SPOTIFYURL,
       DELETED,
       EXTRAFIELD1,
       EXTRAFIELD2,
       EXTRAFIELD3,
       RECEIVECANCELANDREGISTEREMAIL,
       dummy_modified_date_time,
       isnull(cast(stage_fitmetrix_api_instructor.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_fitmetrix_api_instructor
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_fitmetrix_api_instructor @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_fitmetrix_api_instructor (
       bk_hash,
       instructor_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_fitmetrix_api_instructor.bk_hash,
       stage_hash_fitmetrix_api_instructor.INSTRUCTORID instructor_id,
       isnull(cast(stage_hash_fitmetrix_api_instructor.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       29,
       @insert_date_time,
       @user
  from stage_hash_fitmetrix_api_instructor
  left join h_fitmetrix_api_instructor
    on stage_hash_fitmetrix_api_instructor.bk_hash = h_fitmetrix_api_instructor.bk_hash
 where h_fitmetrix_api_instructor_id is null
   and stage_hash_fitmetrix_api_instructor.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_fitmetrix_api_instructor
if object_id('tempdb..#l_fitmetrix_api_instructor_inserts') is not null drop table #l_fitmetrix_api_instructor_inserts
create table #l_fitmetrix_api_instructor_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_instructor.bk_hash,
       stage_hash_fitmetrix_api_instructor.INSTRUCTORID instructor_id,
       stage_hash_fitmetrix_api_instructor.FACILITYID facility_id,
       stage_hash_fitmetrix_api_instructor.EXTERNALID external_id,
       stage_hash_fitmetrix_api_instructor.FACILITYLOCATIONID facility_location_id,
       isnull(cast(stage_hash_fitmetrix_api_instructor.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_instructor.INSTRUCTORID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_instructor.FACILITYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.EXTERNALID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_instructor.FACILITYLOCATIONID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_instructor
 where stage_hash_fitmetrix_api_instructor.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_fitmetrix_api_instructor records
set @insert_date_time = getdate()
insert into l_fitmetrix_api_instructor (
       bk_hash,
       instructor_id,
       facility_id,
       external_id,
       facility_location_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_fitmetrix_api_instructor_inserts.bk_hash,
       #l_fitmetrix_api_instructor_inserts.instructor_id,
       #l_fitmetrix_api_instructor_inserts.facility_id,
       #l_fitmetrix_api_instructor_inserts.external_id,
       #l_fitmetrix_api_instructor_inserts.facility_location_id,
       case when l_fitmetrix_api_instructor.l_fitmetrix_api_instructor_id is null then isnull(#l_fitmetrix_api_instructor_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #l_fitmetrix_api_instructor_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_fitmetrix_api_instructor_inserts
  left join p_fitmetrix_api_instructor
    on #l_fitmetrix_api_instructor_inserts.bk_hash = p_fitmetrix_api_instructor.bk_hash
   and p_fitmetrix_api_instructor.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_fitmetrix_api_instructor
    on p_fitmetrix_api_instructor.bk_hash = l_fitmetrix_api_instructor.bk_hash
   and p_fitmetrix_api_instructor.l_fitmetrix_api_instructor_id = l_fitmetrix_api_instructor.l_fitmetrix_api_instructor_id
 where l_fitmetrix_api_instructor.l_fitmetrix_api_instructor_id is null
    or (l_fitmetrix_api_instructor.l_fitmetrix_api_instructor_id is not null
        and l_fitmetrix_api_instructor.dv_hash <> #l_fitmetrix_api_instructor_inserts.source_hash)

--calculate hash and lookup to current s_fitmetrix_api_instructor
if object_id('tempdb..#s_fitmetrix_api_instructor_inserts') is not null drop table #s_fitmetrix_api_instructor_inserts
create table #s_fitmetrix_api_instructor_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_instructor.bk_hash,
       stage_hash_fitmetrix_api_instructor.INSTRUCTORID instructor_id,
       stage_hash_fitmetrix_api_instructor.FIRSTNAME first_name,
       stage_hash_fitmetrix_api_instructor.LASTNAME last_name,
       stage_hash_fitmetrix_api_instructor.DESCRIPTION description,
       stage_hash_fitmetrix_api_instructor.IMAGE image,
       stage_hash_fitmetrix_api_instructor.EMAIL email,
       stage_hash_fitmetrix_api_instructor.STREET1 street_1,
       stage_hash_fitmetrix_api_instructor.STREET2 street_2,
       stage_hash_fitmetrix_api_instructor.CITY city,
       stage_hash_fitmetrix_api_instructor.STATE state,
       stage_hash_fitmetrix_api_instructor.ZIP zip,
       stage_hash_fitmetrix_api_instructor.COUNTRY country,
       stage_hash_fitmetrix_api_instructor.BIO bio,
       stage_hash_fitmetrix_api_instructor.HOMEPHONE home_phone,
       stage_hash_fitmetrix_api_instructor.WORKPHONE work_phone,
       stage_hash_fitmetrix_api_instructor.GENDER gender,
       stage_hash_fitmetrix_api_instructor.QUOTE quote,
       stage_hash_fitmetrix_api_instructor.SHOWONLINE show_online,
       stage_hash_fitmetrix_api_instructor.ACTIVE active,
       stage_hash_fitmetrix_api_instructor.DISPLAYORDER display_order,
       stage_hash_fitmetrix_api_instructor.FACEBOOKURL facebook_url,
       stage_hash_fitmetrix_api_instructor.TWITTERURL twitter_url,
       stage_hash_fitmetrix_api_instructor.INSTAGRAMURL instagram_url,
       stage_hash_fitmetrix_api_instructor.SOUNDCLOUDURL sound_cloud_url,
       stage_hash_fitmetrix_api_instructor.SPOTIFYURL spotify_url,
       stage_hash_fitmetrix_api_instructor.DELETED deleted,
       stage_hash_fitmetrix_api_instructor.EXTRAFIELD1 extra_field_1,
       stage_hash_fitmetrix_api_instructor.EXTRAFIELD2 extra_field_2,
       stage_hash_fitmetrix_api_instructor.EXTRAFIELD3 extra_field_3,
       stage_hash_fitmetrix_api_instructor.RECEIVECANCELANDREGISTEREMAIL receive_cancel_and_register_email,
       stage_hash_fitmetrix_api_instructor.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_fitmetrix_api_instructor.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_instructor.INSTRUCTORID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.FIRSTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.LASTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.DESCRIPTION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.IMAGE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.EMAIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.STREET1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.STREET2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.CITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.STATE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.COUNTRY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.BIO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.HOMEPHONE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.WORKPHONE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.GENDER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.QUOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.SHOWONLINE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.ACTIVE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_instructor.DISPLAYORDER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.FACEBOOKURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.TWITTERURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.INSTAGRAMURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.SOUNDCLOUDURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.SPOTIFYURL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.DELETED,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.EXTRAFIELD1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.EXTRAFIELD2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.EXTRAFIELD3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_instructor.RECEIVECANCELANDREGISTEREMAIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_fitmetrix_api_instructor.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_instructor
 where stage_hash_fitmetrix_api_instructor.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_fitmetrix_api_instructor records
set @insert_date_time = getdate()
insert into s_fitmetrix_api_instructor (
       bk_hash,
       instructor_id,
       first_name,
       last_name,
       description,
       image,
       email,
       street_1,
       street_2,
       city,
       state,
       zip,
       country,
       bio,
       home_phone,
       work_phone,
       gender,
       quote,
       show_online,
       active,
       display_order,
       facebook_url,
       twitter_url,
       instagram_url,
       sound_cloud_url,
       spotify_url,
       deleted,
       extra_field_1,
       extra_field_2,
       extra_field_3,
       receive_cancel_and_register_email,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_fitmetrix_api_instructor_inserts.bk_hash,
       #s_fitmetrix_api_instructor_inserts.instructor_id,
       #s_fitmetrix_api_instructor_inserts.first_name,
       #s_fitmetrix_api_instructor_inserts.last_name,
       #s_fitmetrix_api_instructor_inserts.description,
       #s_fitmetrix_api_instructor_inserts.image,
       #s_fitmetrix_api_instructor_inserts.email,
       #s_fitmetrix_api_instructor_inserts.street_1,
       #s_fitmetrix_api_instructor_inserts.street_2,
       #s_fitmetrix_api_instructor_inserts.city,
       #s_fitmetrix_api_instructor_inserts.state,
       #s_fitmetrix_api_instructor_inserts.zip,
       #s_fitmetrix_api_instructor_inserts.country,
       #s_fitmetrix_api_instructor_inserts.bio,
       #s_fitmetrix_api_instructor_inserts.home_phone,
       #s_fitmetrix_api_instructor_inserts.work_phone,
       #s_fitmetrix_api_instructor_inserts.gender,
       #s_fitmetrix_api_instructor_inserts.quote,
       #s_fitmetrix_api_instructor_inserts.show_online,
       #s_fitmetrix_api_instructor_inserts.active,
       #s_fitmetrix_api_instructor_inserts.display_order,
       #s_fitmetrix_api_instructor_inserts.facebook_url,
       #s_fitmetrix_api_instructor_inserts.twitter_url,
       #s_fitmetrix_api_instructor_inserts.instagram_url,
       #s_fitmetrix_api_instructor_inserts.sound_cloud_url,
       #s_fitmetrix_api_instructor_inserts.spotify_url,
       #s_fitmetrix_api_instructor_inserts.deleted,
       #s_fitmetrix_api_instructor_inserts.extra_field_1,
       #s_fitmetrix_api_instructor_inserts.extra_field_2,
       #s_fitmetrix_api_instructor_inserts.extra_field_3,
       #s_fitmetrix_api_instructor_inserts.receive_cancel_and_register_email,
       #s_fitmetrix_api_instructor_inserts.dummy_modified_date_time,
       case when s_fitmetrix_api_instructor.s_fitmetrix_api_instructor_id is null then isnull(#s_fitmetrix_api_instructor_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #s_fitmetrix_api_instructor_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_fitmetrix_api_instructor_inserts
  left join p_fitmetrix_api_instructor
    on #s_fitmetrix_api_instructor_inserts.bk_hash = p_fitmetrix_api_instructor.bk_hash
   and p_fitmetrix_api_instructor.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_fitmetrix_api_instructor
    on p_fitmetrix_api_instructor.bk_hash = s_fitmetrix_api_instructor.bk_hash
   and p_fitmetrix_api_instructor.s_fitmetrix_api_instructor_id = s_fitmetrix_api_instructor.s_fitmetrix_api_instructor_id
 where s_fitmetrix_api_instructor.s_fitmetrix_api_instructor_id is null
    or (s_fitmetrix_api_instructor.s_fitmetrix_api_instructor_id is not null
        and s_fitmetrix_api_instructor.dv_hash <> #s_fitmetrix_api_instructor_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_fitmetrix_api_instructor @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_fitmetrix_api_instructor @current_dv_batch_id

end
