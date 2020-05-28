CREATE PROC [dbo].[proc_etl_spabiz_ap] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_AP

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_AP (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       CUSTID,
       Date,
       DATECUST,
       STATUS,
       DATESTATUS,
       CHECKINTIME,
       STAFFID,
       STARTTIME,
       LATE,
       TICKETID,
       STATUSOLD,
       BOOKSTAFFID,
       BOOKTIME,
       MEMO,
       [Delete],
       TIMEID,
       STANDING,
       ALTCUSTID,
       ALTSERVICEID,
       STORE_NUMBER,
       CONFIRMID,
       STARTBOOKING,
       STOPBOOKING,
       UPSELL,
       STOREID2,
       ACTIVITYID,
       NOSHOW,
       APPOINTMENTTYPE,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       CUSTID,
       Date,
       DATECUST,
       STATUS,
       DATESTATUS,
       CHECKINTIME,
       STAFFID,
       STARTTIME,
       LATE,
       TICKETID,
       STATUSOLD,
       BOOKSTAFFID,
       BOOKTIME,
       MEMO,
       [Delete],
       TIMEID,
       STANDING,
       ALTCUSTID,
       ALTSERVICEID,
       STORE_NUMBER,
       CONFIRMID,
       STARTBOOKING,
       STOPBOOKING,
       UPSELL,
       STOREID2,
       ACTIVITYID,
       NOSHOW,
       APPOINTMENTTYPE,
       isnull(cast(stage_spabiz_AP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_AP
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ap @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ap (
       bk_hash,
       ap_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_AP.bk_hash,
       stage_hash_spabiz_AP.ID ap_id,
       stage_hash_spabiz_AP.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_AP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_AP
  left join h_spabiz_ap
    on stage_hash_spabiz_AP.bk_hash = h_spabiz_ap.bk_hash
 where h_spabiz_ap_id is null
   and stage_hash_spabiz_AP.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ap
if object_id('tempdb..#l_spabiz_ap_inserts') is not null drop table #l_spabiz_ap_inserts
create table #l_spabiz_ap_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_AP.bk_hash,
       stage_hash_spabiz_AP.ID ap_id,
       stage_hash_spabiz_AP.STOREID store_id,
       stage_hash_spabiz_AP.CUSTID cust_id,
       stage_hash_spabiz_AP.STAFFID staff_id,
       stage_hash_spabiz_AP.TICKETID ticket_id,
       stage_hash_spabiz_AP.BOOKSTAFFID book_staff_id,
       stage_hash_spabiz_AP.STORE_NUMBER store_number,
       stage_hash_spabiz_AP.CONFIRMID confirm_id,
       stage_hash_spabiz_AP.STOREID2 store_id_2,
       isnull(cast(stage_hash_spabiz_AP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.TICKETID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.BOOKSTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.CONFIRMID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.STOREID2 as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_AP
 where stage_hash_spabiz_AP.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ap records
set @insert_date_time = getdate()
insert into l_spabiz_ap (
       bk_hash,
       ap_id,
       store_id,
       cust_id,
       staff_id,
       ticket_id,
       book_staff_id,
       store_number,
       confirm_id,
       store_id_2,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ap_inserts.bk_hash,
       #l_spabiz_ap_inserts.ap_id,
       #l_spabiz_ap_inserts.store_id,
       #l_spabiz_ap_inserts.cust_id,
       #l_spabiz_ap_inserts.staff_id,
       #l_spabiz_ap_inserts.ticket_id,
       #l_spabiz_ap_inserts.book_staff_id,
       #l_spabiz_ap_inserts.store_number,
       #l_spabiz_ap_inserts.confirm_id,
       #l_spabiz_ap_inserts.store_id_2,
       case when l_spabiz_ap.l_spabiz_ap_id is null then isnull(#l_spabiz_ap_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ap_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ap_inserts
  left join p_spabiz_ap
    on #l_spabiz_ap_inserts.bk_hash = p_spabiz_ap.bk_hash
   and p_spabiz_ap.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ap
    on p_spabiz_ap.bk_hash = l_spabiz_ap.bk_hash
   and p_spabiz_ap.l_spabiz_ap_id = l_spabiz_ap.l_spabiz_ap_id
 where l_spabiz_ap.l_spabiz_ap_id is null
    or (l_spabiz_ap.l_spabiz_ap_id is not null
        and l_spabiz_ap.dv_hash <> #l_spabiz_ap_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ap
if object_id('tempdb..#s_spabiz_ap_inserts') is not null drop table #s_spabiz_ap_inserts
create table #s_spabiz_ap_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_AP.bk_hash,
       stage_hash_spabiz_AP.ID ap_id,
       stage_hash_spabiz_AP.COUNTERID counter_id,
       stage_hash_spabiz_AP.EDITTIME edit_time,
       stage_hash_spabiz_AP.Date date,
       stage_hash_spabiz_AP.DATECUST date_cust,
       stage_hash_spabiz_AP.STATUS status,
       stage_hash_spabiz_AP.DATESTATUS date_status,
       stage_hash_spabiz_AP.CHECKINTIME checkin_time,
       stage_hash_spabiz_AP.STARTTIME start_time,
       stage_hash_spabiz_AP.LATE late,
       stage_hash_spabiz_AP.STATUSOLD status_old,
       stage_hash_spabiz_AP.BOOKTIME book_time,
       stage_hash_spabiz_AP.MEMO memo,
       stage_hash_spabiz_AP.[Delete] ap_delete,
       stage_hash_spabiz_AP.TIMEID time_id,
       stage_hash_spabiz_AP.STANDING standing,
       stage_hash_spabiz_AP.ALTCUSTID alt_cust_id,
       stage_hash_spabiz_AP.ALTSERVICEID alt_service_id,
       stage_hash_spabiz_AP.STORE_NUMBER store_number,
       stage_hash_spabiz_AP.STARTBOOKING start_booking,
       stage_hash_spabiz_AP.STOPBOOKING stop_booking,
       stage_hash_spabiz_AP.UPSELL upsell,
       stage_hash_spabiz_AP.ACTIVITYID activity_id,
       stage_hash_spabiz_AP.NOSHOW no_show,
       stage_hash_spabiz_AP.APPOINTMENTTYPE appointment_type,
       isnull(cast(stage_hash_spabiz_AP.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_AP.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_AP.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_AP.DATECUST,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_AP.DATESTATUS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_AP.CHECKINTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_AP.STARTTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.LATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.STATUSOLD as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_AP.BOOKTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_AP.MEMO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.TIMEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.STANDING as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_AP.ALTCUSTID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_AP.ALTSERVICEID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_AP.STARTBOOKING,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_AP.STOPBOOKING,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.UPSELL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.ACTIVITYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.NOSHOW as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_AP.APPOINTMENTTYPE as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_AP
 where stage_hash_spabiz_AP.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ap records
set @insert_date_time = getdate()
insert into s_spabiz_ap (
       bk_hash,
       ap_id,
       counter_id,
       edit_time,
       date,
       date_cust,
       status,
       date_status,
       checkin_time,
       start_time,
       late,
       status_old,
       book_time,
       memo,
       ap_delete,
       time_id,
       standing,
       alt_cust_id,
       alt_service_id,
       store_number,
       start_booking,
       stop_booking,
       upsell,
       activity_id,
       no_show,
       appointment_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ap_inserts.bk_hash,
       #s_spabiz_ap_inserts.ap_id,
       #s_spabiz_ap_inserts.counter_id,
       #s_spabiz_ap_inserts.edit_time,
       #s_spabiz_ap_inserts.date,
       #s_spabiz_ap_inserts.date_cust,
       #s_spabiz_ap_inserts.status,
       #s_spabiz_ap_inserts.date_status,
       #s_spabiz_ap_inserts.checkin_time,
       #s_spabiz_ap_inserts.start_time,
       #s_spabiz_ap_inserts.late,
       #s_spabiz_ap_inserts.status_old,
       #s_spabiz_ap_inserts.book_time,
       #s_spabiz_ap_inserts.memo,
       #s_spabiz_ap_inserts.ap_delete,
       #s_spabiz_ap_inserts.time_id,
       #s_spabiz_ap_inserts.standing,
       #s_spabiz_ap_inserts.alt_cust_id,
       #s_spabiz_ap_inserts.alt_service_id,
       #s_spabiz_ap_inserts.store_number,
       #s_spabiz_ap_inserts.start_booking,
       #s_spabiz_ap_inserts.stop_booking,
       #s_spabiz_ap_inserts.upsell,
       #s_spabiz_ap_inserts.activity_id,
       #s_spabiz_ap_inserts.no_show,
       #s_spabiz_ap_inserts.appointment_type,
       case when s_spabiz_ap.s_spabiz_ap_id is null then isnull(#s_spabiz_ap_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ap_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ap_inserts
  left join p_spabiz_ap
    on #s_spabiz_ap_inserts.bk_hash = p_spabiz_ap.bk_hash
   and p_spabiz_ap.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ap
    on p_spabiz_ap.bk_hash = s_spabiz_ap.bk_hash
   and p_spabiz_ap.s_spabiz_ap_id = s_spabiz_ap.s_spabiz_ap_id
 where s_spabiz_ap.s_spabiz_ap_id is null
    or (s_spabiz_ap.s_spabiz_ap_id is not null
        and s_spabiz_ap.dv_hash <> #s_spabiz_ap_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ap @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ap @current_dv_batch_id

end
