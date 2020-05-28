CREATE PROC [dbo].[proc_etl_fitmetrix_api_appointment_id_statistics] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_fitmetrix_api_appointment_id_statistics

set @insert_date_time = getdate()
insert into dbo.stage_hash_fitmetrix_api_appointment_id_statistics (
       bk_hash,
       PROFILEAPPOINTMENTID,
       PROFILEID,
       EXTERNALID,
       APPOINTMENTID,
       ZONE1TIME,
       ZONE1CALORIES,
       ZONE2TIME,
       ZONE2CALORIES,
       ZONE3TIME,
       ZONE3CALORIES,
       ZONE4TIME,
       ZONE4CALORIES,
       ZONE0TIME,
       ZONE0CALORIES,
       DISTANCE,
       MAXSPEED,
       AVERAGESPEED,
       MAXPOWER,
       AVERAGEPOWER,
       AVERAGEWATTS,
       MAXWATTS,
       MAXHEARTRATE,
       MINHEARTRATE,
       AVGHEARTRATE,
       DEVICEID,
       TOTALCALORIES,
       TOTALPOINTS,
       APPOINTMENTNAME,
       STARTDATETIME,
       FIRSTNAME,
       LASTNAME,
       HEARTRATEBREAKDOWN,
       DESCRIPTION,
       RANK,
       TOTALRANK,
       CLASSDURATION,
       TOTALMINUTES,
       EMAIL,
       NAME,
       INSTRUCTORFIRSTNAME,
       INSTRUCTORLASTNAME,
       RPMBREAKDOWN,
       WATTSBREAKDOWN,
       SPEEDBREAKDOWN,
       WEIGHT,
       GENDER,
       PTP,
       ZONE1PTPTIME,
       ZONE2PTPTIME,
       ZONE3PTPTIME,
       ZONE4PTPTIME,
       ZONE0PTPTIME,
       ZONE1RPMTIME,
       ZONE1RPMCALORIES,
       ZONE2RPMTIME,
       ZONE2RPMCALORIES,
       ZONE3RPMTIME,
       ZONE3RPMCALORIES,
       ZONE4RPMTIME,
       ZONE4RPMCALORIES,
       ZONE0RPMTIME,
       ZONE0RPMCALORIES,
       LOANERDEVICEID,
       SPOTNUMBER,
       SPOTDEVICEID,
       BOOKINGPRIORITY,
       PTPSTORED,
       BIRTHDATE,
       CREATEDATE,
       WAITLIST,
       CHECKEDIN,
       WAITLISTPOSITION,
       WAITLISTDATETIME,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PROFILEAPPOINTMENTID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       PROFILEAPPOINTMENTID,
       PROFILEID,
       EXTERNALID,
       APPOINTMENTID,
       ZONE1TIME,
       ZONE1CALORIES,
       ZONE2TIME,
       ZONE2CALORIES,
       ZONE3TIME,
       ZONE3CALORIES,
       ZONE4TIME,
       ZONE4CALORIES,
       ZONE0TIME,
       ZONE0CALORIES,
       DISTANCE,
       MAXSPEED,
       AVERAGESPEED,
       MAXPOWER,
       AVERAGEPOWER,
       AVERAGEWATTS,
       MAXWATTS,
       MAXHEARTRATE,
       MINHEARTRATE,
       AVGHEARTRATE,
       DEVICEID,
       TOTALCALORIES,
       TOTALPOINTS,
       APPOINTMENTNAME,
       STARTDATETIME,
       FIRSTNAME,
       LASTNAME,
       HEARTRATEBREAKDOWN,
       DESCRIPTION,
       RANK,
       TOTALRANK,
       CLASSDURATION,
       TOTALMINUTES,
       EMAIL,
       NAME,
       INSTRUCTORFIRSTNAME,
       INSTRUCTORLASTNAME,
       RPMBREAKDOWN,
       WATTSBREAKDOWN,
       SPEEDBREAKDOWN,
       WEIGHT,
       GENDER,
       PTP,
       ZONE1PTPTIME,
       ZONE2PTPTIME,
       ZONE3PTPTIME,
       ZONE4PTPTIME,
       ZONE0PTPTIME,
       ZONE1RPMTIME,
       ZONE1RPMCALORIES,
       ZONE2RPMTIME,
       ZONE2RPMCALORIES,
       ZONE3RPMTIME,
       ZONE3RPMCALORIES,
       ZONE4RPMTIME,
       ZONE4RPMCALORIES,
       ZONE0RPMTIME,
       ZONE0RPMCALORIES,
       LOANERDEVICEID,
       SPOTNUMBER,
       SPOTDEVICEID,
       BOOKINGPRIORITY,
       PTPSTORED,
       BIRTHDATE,
       CREATEDATE,
       WAITLIST,
       CHECKEDIN,
       WAITLISTPOSITION,
       WAITLISTDATETIME,
       dummy_modified_date_time,
       isnull(cast(stage_fitmetrix_api_appointment_id_statistics.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_fitmetrix_api_appointment_id_statistics
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_fitmetrix_api_appointment_id_statistics @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_fitmetrix_api_appointment_id_statistics (
       bk_hash,
       profile_appointment_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_fitmetrix_api_appointment_id_statistics.bk_hash,
       stage_hash_fitmetrix_api_appointment_id_statistics.PROFILEAPPOINTMENTID profile_appointment_id,
       isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       29,
       @insert_date_time,
       @user
  from stage_hash_fitmetrix_api_appointment_id_statistics
  left join h_fitmetrix_api_appointment_id_statistics
    on stage_hash_fitmetrix_api_appointment_id_statistics.bk_hash = h_fitmetrix_api_appointment_id_statistics.bk_hash
 where h_fitmetrix_api_appointment_id_statistics_id is null
   and stage_hash_fitmetrix_api_appointment_id_statistics.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_fitmetrix_api_appointment_id_statistics
if object_id('tempdb..#l_fitmetrix_api_appointment_id_statistics_inserts') is not null drop table #l_fitmetrix_api_appointment_id_statistics_inserts
create table #l_fitmetrix_api_appointment_id_statistics_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_appointment_id_statistics.bk_hash,
       stage_hash_fitmetrix_api_appointment_id_statistics.PROFILEAPPOINTMENTID profile_appointment_id,
       stage_hash_fitmetrix_api_appointment_id_statistics.PROFILEID profile_id,
       stage_hash_fitmetrix_api_appointment_id_statistics.EXTERNALID external_id,
       stage_hash_fitmetrix_api_appointment_id_statistics.APPOINTMENTID appointment_id,
       stage_hash_fitmetrix_api_appointment_id_statistics.DEVICEID device_id,
       stage_hash_fitmetrix_api_appointment_id_statistics.LOANERDEVICEID loaner_device_id,
       stage_hash_fitmetrix_api_appointment_id_statistics.SPOTDEVICEID spot_device_id,
       isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.PROFILEAPPOINTMENTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.PROFILEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.EXTERNALID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.APPOINTMENTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.DEVICEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.LOANERDEVICEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.SPOTDEVICEID,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_appointment_id_statistics
 where stage_hash_fitmetrix_api_appointment_id_statistics.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_fitmetrix_api_appointment_id_statistics records
set @insert_date_time = getdate()
insert into l_fitmetrix_api_appointment_id_statistics (
       bk_hash,
       profile_appointment_id,
       profile_id,
       external_id,
       appointment_id,
       device_id,
       loaner_device_id,
       spot_device_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_fitmetrix_api_appointment_id_statistics_inserts.bk_hash,
       #l_fitmetrix_api_appointment_id_statistics_inserts.profile_appointment_id,
       #l_fitmetrix_api_appointment_id_statistics_inserts.profile_id,
       #l_fitmetrix_api_appointment_id_statistics_inserts.external_id,
       #l_fitmetrix_api_appointment_id_statistics_inserts.appointment_id,
       #l_fitmetrix_api_appointment_id_statistics_inserts.device_id,
       #l_fitmetrix_api_appointment_id_statistics_inserts.loaner_device_id,
       #l_fitmetrix_api_appointment_id_statistics_inserts.spot_device_id,
       case when l_fitmetrix_api_appointment_id_statistics.l_fitmetrix_api_appointment_id_statistics_id is null then isnull(#l_fitmetrix_api_appointment_id_statistics_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #l_fitmetrix_api_appointment_id_statistics_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_fitmetrix_api_appointment_id_statistics_inserts
  left join p_fitmetrix_api_appointment_id_statistics
    on #l_fitmetrix_api_appointment_id_statistics_inserts.bk_hash = p_fitmetrix_api_appointment_id_statistics.bk_hash
   and p_fitmetrix_api_appointment_id_statistics.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_fitmetrix_api_appointment_id_statistics
    on p_fitmetrix_api_appointment_id_statistics.bk_hash = l_fitmetrix_api_appointment_id_statistics.bk_hash
   and p_fitmetrix_api_appointment_id_statistics.l_fitmetrix_api_appointment_id_statistics_id = l_fitmetrix_api_appointment_id_statistics.l_fitmetrix_api_appointment_id_statistics_id
 where l_fitmetrix_api_appointment_id_statistics.l_fitmetrix_api_appointment_id_statistics_id is null
    or (l_fitmetrix_api_appointment_id_statistics.l_fitmetrix_api_appointment_id_statistics_id is not null
        and l_fitmetrix_api_appointment_id_statistics.dv_hash <> #l_fitmetrix_api_appointment_id_statistics_inserts.source_hash)

--calculate hash and lookup to current s_fitmetrix_api_appointment_id_statistics
if object_id('tempdb..#s_fitmetrix_api_appointment_id_statistics_inserts') is not null drop table #s_fitmetrix_api_appointment_id_statistics_inserts
create table #s_fitmetrix_api_appointment_id_statistics_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_fitmetrix_api_appointment_id_statistics.bk_hash,
       stage_hash_fitmetrix_api_appointment_id_statistics.PROFILEAPPOINTMENTID profile_appointment_id,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1TIME zone_1_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1CALORIES zone_1_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2TIME zone_2_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2CALORIES zone_2_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3TIME zone_3_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3CALORIES zone_3_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4TIME zone_4_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4CALORIES zone_4_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0TIME zone_0_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0CALORIES zone_0_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.DISTANCE distance,
       stage_hash_fitmetrix_api_appointment_id_statistics.MAXSPEED max_speed,
       stage_hash_fitmetrix_api_appointment_id_statistics.AVERAGESPEED average_speed,
       stage_hash_fitmetrix_api_appointment_id_statistics.MAXPOWER max_power,
       stage_hash_fitmetrix_api_appointment_id_statistics.AVERAGEPOWER average_power,
       stage_hash_fitmetrix_api_appointment_id_statistics.AVERAGEWATTS average_waits,
       stage_hash_fitmetrix_api_appointment_id_statistics.MAXWATTS max_waits,
       stage_hash_fitmetrix_api_appointment_id_statistics.MAXHEARTRATE max_heart_rate,
       stage_hash_fitmetrix_api_appointment_id_statistics.MINHEARTRATE min_heart_rate,
       stage_hash_fitmetrix_api_appointment_id_statistics.AVGHEARTRATE average_heart_rate,
       stage_hash_fitmetrix_api_appointment_id_statistics.TOTALCALORIES total_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.TOTALPOINTS total_points,
       stage_hash_fitmetrix_api_appointment_id_statistics.APPOINTMENTNAME appointment_name,
       stage_hash_fitmetrix_api_appointment_id_statistics.STARTDATETIME start_date_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.FIRSTNAME first_name,
       stage_hash_fitmetrix_api_appointment_id_statistics.LASTNAME last_name,
       stage_hash_fitmetrix_api_appointment_id_statistics.HEARTRATEBREAKDOWN heart_rate_breakdown,
       stage_hash_fitmetrix_api_appointment_id_statistics.DESCRIPTION description,
       stage_hash_fitmetrix_api_appointment_id_statistics.RANK rank,
       stage_hash_fitmetrix_api_appointment_id_statistics.TOTALRANK total_rank,
       stage_hash_fitmetrix_api_appointment_id_statistics.CLASSDURATION class_duration,
       stage_hash_fitmetrix_api_appointment_id_statistics.TOTALMINUTES total_minutes,
       stage_hash_fitmetrix_api_appointment_id_statistics.EMAIL email,
       stage_hash_fitmetrix_api_appointment_id_statistics.NAME name,
       stage_hash_fitmetrix_api_appointment_id_statistics.INSTRUCTORFIRSTNAME instructor_first_name,
       stage_hash_fitmetrix_api_appointment_id_statistics.INSTRUCTORLASTNAME instructor_last_name,
       stage_hash_fitmetrix_api_appointment_id_statistics.RPMBREAKDOWN rpm_breakdown,
       stage_hash_fitmetrix_api_appointment_id_statistics.WATTSBREAKDOWN watts_breakdown,
       stage_hash_fitmetrix_api_appointment_id_statistics.SPEEDBREAKDOWN speed_breakdown,
       stage_hash_fitmetrix_api_appointment_id_statistics.WEIGHT weight,
       stage_hash_fitmetrix_api_appointment_id_statistics.GENDER gender,
       stage_hash_fitmetrix_api_appointment_id_statistics.PTP ptp,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1PTPTIME zone_1_ptp_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2PTPTIME zone_2_ptp_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3PTPTIME zone_3_ptp_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4PTPTIME zone_4_ptp_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0PTPTIME zone_0_ptp_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1RPMTIME zone_1_rpm_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1RPMCALORIES zone_1_rpm_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2RPMTIME zone_2_rpm_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2RPMCALORIES zone_2_rpm_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3RPMTIME zone_3_rpm_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3RPMCALORIES zone_3_rpm_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4RPMTIME zone_4_rpm_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4RPMCALORIES zone_4_rpm_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0RPMTIME zone_0_rpm_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0RPMCALORIES zone_0_rpm_calories,
       stage_hash_fitmetrix_api_appointment_id_statistics.SPOTNUMBER spot_number,
       stage_hash_fitmetrix_api_appointment_id_statistics.BOOKINGPRIORITY booking_priority,
       stage_hash_fitmetrix_api_appointment_id_statistics.PTPSTORED ptp_stored,
       stage_hash_fitmetrix_api_appointment_id_statistics.BIRTHDATE birthdate,
       stage_hash_fitmetrix_api_appointment_id_statistics.CREATEDATE create_date,
       stage_hash_fitmetrix_api_appointment_id_statistics.WAITLIST waitlist,
       stage_hash_fitmetrix_api_appointment_id_statistics.CHECKEDIN checked_in,
       stage_hash_fitmetrix_api_appointment_id_statistics.WAITLISTPOSITION waitlist_position,
       stage_hash_fitmetrix_api_appointment_id_statistics.WAITLISTDATETIME waitlist_date_time,
       stage_hash_fitmetrix_api_appointment_id_statistics.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.PROFILEAPPOINTMENTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1TIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1CALORIES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2TIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2CALORIES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3TIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3CALORIES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4TIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4CALORIES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0TIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0CALORIES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.DISTANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.MAXSPEED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.AVERAGESPEED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.MAXPOWER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.AVERAGEPOWER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.AVERAGEWATTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.MAXWATTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.MAXHEARTRATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.MINHEARTRATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.AVGHEARTRATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.TOTALCALORIES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.TOTALPOINTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.APPOINTMENTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.STARTDATETIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.FIRSTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.LASTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.HEARTRATEBREAKDOWN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.DESCRIPTION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.RANK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.TOTALRANK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.CLASSDURATION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.TOTALMINUTES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.EMAIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.INSTRUCTORFIRSTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.INSTRUCTORLASTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.RPMBREAKDOWN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.WATTSBREAKDOWN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.SPEEDBREAKDOWN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.WEIGHT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.GENDER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.PTP as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1PTPTIME as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2PTPTIME as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3PTPTIME as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4PTPTIME as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0PTPTIME as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1RPMTIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE1RPMCALORIES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2RPMTIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE2RPMCALORIES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3RPMTIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE3RPMCALORIES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4RPMTIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE4RPMCALORIES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0RPMTIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.ZONE0RPMCALORIES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.SPOTNUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.BOOKINGPRIORITY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.PTPSTORED,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.BIRTHDATE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.CREATEDATE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.WAITLIST,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.CHECKEDIN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_fitmetrix_api_appointment_id_statistics.WAITLISTPOSITION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_fitmetrix_api_appointment_id_statistics.WAITLISTDATETIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_fitmetrix_api_appointment_id_statistics.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_fitmetrix_api_appointment_id_statistics
 where stage_hash_fitmetrix_api_appointment_id_statistics.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_fitmetrix_api_appointment_id_statistics records
set @insert_date_time = getdate()
insert into s_fitmetrix_api_appointment_id_statistics (
       bk_hash,
       profile_appointment_id,
       zone_1_time,
       zone_1_calories,
       zone_2_time,
       zone_2_calories,
       zone_3_time,
       zone_3_calories,
       zone_4_time,
       zone_4_calories,
       zone_0_time,
       zone_0_calories,
       distance,
       max_speed,
       average_speed,
       max_power,
       average_power,
       average_waits,
       max_waits,
       max_heart_rate,
       min_heart_rate,
       average_heart_rate,
       total_calories,
       total_points,
       appointment_name,
       start_date_time,
       first_name,
       last_name,
       heart_rate_breakdown,
       description,
       rank,
       total_rank,
       class_duration,
       total_minutes,
       email,
       name,
       instructor_first_name,
       instructor_last_name,
       rpm_breakdown,
       watts_breakdown,
       speed_breakdown,
       weight,
       gender,
       ptp,
       zone_1_ptp_time,
       zone_2_ptp_time,
       zone_3_ptp_time,
       zone_4_ptp_time,
       zone_0_ptp_time,
       zone_1_rpm_time,
       zone_1_rpm_calories,
       zone_2_rpm_time,
       zone_2_rpm_calories,
       zone_3_rpm_time,
       zone_3_rpm_calories,
       zone_4_rpm_time,
       zone_4_rpm_calories,
       zone_0_rpm_time,
       zone_0_rpm_calories,
       spot_number,
       booking_priority,
       ptp_stored,
       birthdate,
       create_date,
       waitlist,
       checked_in,
       waitlist_position,
       waitlist_date_time,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_fitmetrix_api_appointment_id_statistics_inserts.bk_hash,
       #s_fitmetrix_api_appointment_id_statistics_inserts.profile_appointment_id,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_1_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_1_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_2_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_2_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_3_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_3_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_4_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_4_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_0_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_0_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.distance,
       #s_fitmetrix_api_appointment_id_statistics_inserts.max_speed,
       #s_fitmetrix_api_appointment_id_statistics_inserts.average_speed,
       #s_fitmetrix_api_appointment_id_statistics_inserts.max_power,
       #s_fitmetrix_api_appointment_id_statistics_inserts.average_power,
       #s_fitmetrix_api_appointment_id_statistics_inserts.average_waits,
       #s_fitmetrix_api_appointment_id_statistics_inserts.max_waits,
       #s_fitmetrix_api_appointment_id_statistics_inserts.max_heart_rate,
       #s_fitmetrix_api_appointment_id_statistics_inserts.min_heart_rate,
       #s_fitmetrix_api_appointment_id_statistics_inserts.average_heart_rate,
       #s_fitmetrix_api_appointment_id_statistics_inserts.total_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.total_points,
       #s_fitmetrix_api_appointment_id_statistics_inserts.appointment_name,
       #s_fitmetrix_api_appointment_id_statistics_inserts.start_date_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.first_name,
       #s_fitmetrix_api_appointment_id_statistics_inserts.last_name,
       #s_fitmetrix_api_appointment_id_statistics_inserts.heart_rate_breakdown,
       #s_fitmetrix_api_appointment_id_statistics_inserts.description,
       #s_fitmetrix_api_appointment_id_statistics_inserts.rank,
       #s_fitmetrix_api_appointment_id_statistics_inserts.total_rank,
       #s_fitmetrix_api_appointment_id_statistics_inserts.class_duration,
       #s_fitmetrix_api_appointment_id_statistics_inserts.total_minutes,
       #s_fitmetrix_api_appointment_id_statistics_inserts.email,
       #s_fitmetrix_api_appointment_id_statistics_inserts.name,
       #s_fitmetrix_api_appointment_id_statistics_inserts.instructor_first_name,
       #s_fitmetrix_api_appointment_id_statistics_inserts.instructor_last_name,
       #s_fitmetrix_api_appointment_id_statistics_inserts.rpm_breakdown,
       #s_fitmetrix_api_appointment_id_statistics_inserts.watts_breakdown,
       #s_fitmetrix_api_appointment_id_statistics_inserts.speed_breakdown,
       #s_fitmetrix_api_appointment_id_statistics_inserts.weight,
       #s_fitmetrix_api_appointment_id_statistics_inserts.gender,
       #s_fitmetrix_api_appointment_id_statistics_inserts.ptp,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_1_ptp_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_2_ptp_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_3_ptp_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_4_ptp_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_0_ptp_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_1_rpm_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_1_rpm_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_2_rpm_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_2_rpm_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_3_rpm_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_3_rpm_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_4_rpm_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_4_rpm_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_0_rpm_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.zone_0_rpm_calories,
       #s_fitmetrix_api_appointment_id_statistics_inserts.spot_number,
       #s_fitmetrix_api_appointment_id_statistics_inserts.booking_priority,
       #s_fitmetrix_api_appointment_id_statistics_inserts.ptp_stored,
       #s_fitmetrix_api_appointment_id_statistics_inserts.birthdate,
       #s_fitmetrix_api_appointment_id_statistics_inserts.create_date,
       #s_fitmetrix_api_appointment_id_statistics_inserts.waitlist,
       #s_fitmetrix_api_appointment_id_statistics_inserts.checked_in,
       #s_fitmetrix_api_appointment_id_statistics_inserts.waitlist_position,
       #s_fitmetrix_api_appointment_id_statistics_inserts.waitlist_date_time,
       #s_fitmetrix_api_appointment_id_statistics_inserts.dummy_modified_date_time,
       case when s_fitmetrix_api_appointment_id_statistics.s_fitmetrix_api_appointment_id_statistics_id is null then isnull(#s_fitmetrix_api_appointment_id_statistics_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       29,
       #s_fitmetrix_api_appointment_id_statistics_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_fitmetrix_api_appointment_id_statistics_inserts
  left join p_fitmetrix_api_appointment_id_statistics
    on #s_fitmetrix_api_appointment_id_statistics_inserts.bk_hash = p_fitmetrix_api_appointment_id_statistics.bk_hash
   and p_fitmetrix_api_appointment_id_statistics.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_fitmetrix_api_appointment_id_statistics
    on p_fitmetrix_api_appointment_id_statistics.bk_hash = s_fitmetrix_api_appointment_id_statistics.bk_hash
   and p_fitmetrix_api_appointment_id_statistics.s_fitmetrix_api_appointment_id_statistics_id = s_fitmetrix_api_appointment_id_statistics.s_fitmetrix_api_appointment_id_statistics_id
 where s_fitmetrix_api_appointment_id_statistics.s_fitmetrix_api_appointment_id_statistics_id is null
    or (s_fitmetrix_api_appointment_id_statistics.s_fitmetrix_api_appointment_id_statistics_id is not null
        and s_fitmetrix_api_appointment_id_statistics.dv_hash <> #s_fitmetrix_api_appointment_id_statistics_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_fitmetrix_api_appointment_id_statistics @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_fitmetrix_api_appointment_id_statistics @current_dv_batch_id

end
