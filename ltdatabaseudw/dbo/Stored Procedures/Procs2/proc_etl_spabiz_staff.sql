CREATE PROC [dbo].[proc_etl_spabiz_staff] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_STAFF

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_STAFF (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       FIRSTNAME,
       MI,
       LASTNAME,
       FLNAME,
       FNAME,
       QUICKID,
       BOOKNAME,
       ADDRESS1,
       ADDRESS2,
       CITY,
       STATE,
       ZIP,
       TEL_HOME,
       TEL_WORK,
       TEL_MOBIL,
       TEL_PAGER,
       BIRTHDAY,
       SEX,
       EMPLOYER,
       EMPSTARTDATE,
       EMPENDDATE,
       CANUSESYSTEM,
       BALANCE,
       SERVICECOMMISHID,
       ASSCOMMISHID,
       PRODUCTCOMMISHID,
       PRINTTRAVELER,
       POPUPINFO,
       NOTE,
       PRINT1,
       PRINT2,
       PRINT3,
       PRINT4,
       PRINT5,
       PRINT6,
       PRINT7,
       STARTAPCYCLE,
       APCYCLECOUNT,
       DEPTCAT,
       SEARCHCAT,
       STATUS,
       BDAY,
       ANNIVERSARY,
       CLOCKINREQ,
       WAGETYPE,
       WAGE,
       WAGEPERMIN,
       TGLEVEL,
       PRINTPOPUP,
       BIO,
       TYPEOF,
       NAME,
       PAGERNUM,
       PAGERTYPE,
       SALES_TOTAL,
       STORE_NUMBER,
       SERVICETEMPLATEID,
       STAFFTEMPLATEID,
       WEBBOOK,
       NEILLID,
       SCATID,
       LOUIS,
       DONOTPRINTPROD,
       PRINTTRAVLER,
       FOREIGNID,
       NEWID,
       STAFFBACKUPID,
       PRIMARYLOCATION,
       LEVELID,
       USERNAME,
       ISADMIN,
       HEADMAPSTAFF,
       SERVICECOMMISSIONTYPEID,
       ALLOWPOWERBOOKING,
       EMAIL,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       FIRSTNAME,
       MI,
       LASTNAME,
       FLNAME,
       FNAME,
       QUICKID,
       BOOKNAME,
       ADDRESS1,
       ADDRESS2,
       CITY,
       STATE,
       ZIP,
       TEL_HOME,
       TEL_WORK,
       TEL_MOBIL,
       TEL_PAGER,
       BIRTHDAY,
       SEX,
       EMPLOYER,
       EMPSTARTDATE,
       EMPENDDATE,
       CANUSESYSTEM,
       BALANCE,
       SERVICECOMMISHID,
       ASSCOMMISHID,
       PRODUCTCOMMISHID,
       PRINTTRAVELER,
       POPUPINFO,
       NOTE,
       PRINT1,
       PRINT2,
       PRINT3,
       PRINT4,
       PRINT5,
       PRINT6,
       PRINT7,
       STARTAPCYCLE,
       APCYCLECOUNT,
       DEPTCAT,
       SEARCHCAT,
       STATUS,
       BDAY,
       ANNIVERSARY,
       CLOCKINREQ,
       WAGETYPE,
       WAGE,
       WAGEPERMIN,
       TGLEVEL,
       PRINTPOPUP,
       BIO,
       TYPEOF,
       NAME,
       PAGERNUM,
       PAGERTYPE,
       SALES_TOTAL,
       STORE_NUMBER,
       SERVICETEMPLATEID,
       STAFFTEMPLATEID,
       WEBBOOK,
       NEILLID,
       SCATID,
       LOUIS,
       DONOTPRINTPROD,
       PRINTTRAVLER,
       FOREIGNID,
       NEWID,
       STAFFBACKUPID,
       PRIMARYLOCATION,
       LEVELID,
       USERNAME,
       ISADMIN,
       HEADMAPSTAFF,
       SERVICECOMMISSIONTYPEID,
       ALLOWPOWERBOOKING,
       EMAIL,
       isnull(cast(stage_spabiz_STAFF.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_STAFF
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_staff @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_staff (
       bk_hash,
       staff_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_STAFF.bk_hash,
       stage_hash_spabiz_STAFF.ID staff_id,
       stage_hash_spabiz_STAFF.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_STAFF.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_STAFF
  left join h_spabiz_staff
    on stage_hash_spabiz_STAFF.bk_hash = h_spabiz_staff.bk_hash
 where h_spabiz_staff_id is null
   and stage_hash_spabiz_STAFF.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_staff
if object_id('tempdb..#l_spabiz_staff_inserts') is not null drop table #l_spabiz_staff_inserts
create table #l_spabiz_staff_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_STAFF.bk_hash,
       stage_hash_spabiz_STAFF.ID staff_id,
       stage_hash_spabiz_STAFF.STOREID store_id,
       stage_hash_spabiz_STAFF.SERVICECOMMISHID service_commish_id,
       stage_hash_spabiz_STAFF.ASSCOMMISHID ass_commish_id,
       stage_hash_spabiz_STAFF.PRODUCTCOMMISHID product_commish_id,
       stage_hash_spabiz_STAFF.DEPTCAT dept_cat,
       stage_hash_spabiz_STAFF.SEARCHCAT search_cat,
       stage_hash_spabiz_STAFF.STORE_NUMBER store_number,
       stage_hash_spabiz_STAFF.SCATID scat_id,
       stage_hash_spabiz_STAFF.PRIMARYLOCATION primary_location,
       stage_hash_spabiz_STAFF.LEVELID level_id,
       stage_hash_spabiz_STAFF.SERVICECOMMISSIONTYPEID service_commission_type_id,
       stage_hash_spabiz_STAFF.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.SERVICECOMMISHID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.ASSCOMMISHID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRODUCTCOMMISHID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.DEPTCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.SEARCHCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.SCATID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRIMARYLOCATION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.LEVELID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.SERVICECOMMISSIONTYPEID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_STAFF
 where stage_hash_spabiz_STAFF.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_staff records
set @insert_date_time = getdate()
insert into l_spabiz_staff (
       bk_hash,
       staff_id,
       store_id,
       service_commish_id,
       ass_commish_id,
       product_commish_id,
       dept_cat,
       search_cat,
       store_number,
       scat_id,
       primary_location,
       level_id,
       service_commission_type_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_staff_inserts.bk_hash,
       #l_spabiz_staff_inserts.staff_id,
       #l_spabiz_staff_inserts.store_id,
       #l_spabiz_staff_inserts.service_commish_id,
       #l_spabiz_staff_inserts.ass_commish_id,
       #l_spabiz_staff_inserts.product_commish_id,
       #l_spabiz_staff_inserts.dept_cat,
       #l_spabiz_staff_inserts.search_cat,
       #l_spabiz_staff_inserts.store_number,
       #l_spabiz_staff_inserts.scat_id,
       #l_spabiz_staff_inserts.primary_location,
       #l_spabiz_staff_inserts.level_id,
       #l_spabiz_staff_inserts.service_commission_type_id,
       case when l_spabiz_staff.l_spabiz_staff_id is null then isnull(#l_spabiz_staff_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_staff_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_staff_inserts
  left join p_spabiz_staff
    on #l_spabiz_staff_inserts.bk_hash = p_spabiz_staff.bk_hash
   and p_spabiz_staff.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_staff
    on p_spabiz_staff.bk_hash = l_spabiz_staff.bk_hash
   and p_spabiz_staff.l_spabiz_staff_id = l_spabiz_staff.l_spabiz_staff_id
 where l_spabiz_staff.l_spabiz_staff_id is null
    or (l_spabiz_staff.l_spabiz_staff_id is not null
        and l_spabiz_staff.dv_hash <> #l_spabiz_staff_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_staff
if object_id('tempdb..#s_spabiz_staff_inserts') is not null drop table #s_spabiz_staff_inserts
create table #s_spabiz_staff_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_STAFF.bk_hash,
       stage_hash_spabiz_STAFF.ID staff_id,
       stage_hash_spabiz_STAFF.COUNTERID counter_id,
       stage_hash_spabiz_STAFF.EDITTIME edit_time,
       stage_hash_spabiz_STAFF.[Delete] staff_delete,
       stage_hash_spabiz_STAFF.DELETEDATE delete_date,
       stage_hash_spabiz_STAFF.FIRSTNAME first_name,
       stage_hash_spabiz_STAFF.MI mi,
       stage_hash_spabiz_STAFF.LASTNAME last_name,
       stage_hash_spabiz_STAFF.FLNAME f_l_name,
       stage_hash_spabiz_STAFF.FNAME f_name,
       stage_hash_spabiz_STAFF.QUICKID quick_id,
       stage_hash_spabiz_STAFF.BOOKNAME book_name,
       stage_hash_spabiz_STAFF.ADDRESS1 address_1,
       stage_hash_spabiz_STAFF.ADDRESS2 address_2,
       stage_hash_spabiz_STAFF.CITY city,
       stage_hash_spabiz_STAFF.STATE state,
       stage_hash_spabiz_STAFF.ZIP zip,
       stage_hash_spabiz_STAFF.TEL_HOME tel_home,
       stage_hash_spabiz_STAFF.TEL_WORK tel_work,
       stage_hash_spabiz_STAFF.TEL_MOBIL tel_mobil,
       stage_hash_spabiz_STAFF.TEL_PAGER tel_pager,
       stage_hash_spabiz_STAFF.BIRTHDAY birthday,
       stage_hash_spabiz_STAFF.SEX sex,
       stage_hash_spabiz_STAFF.EMPLOYER employer,
       stage_hash_spabiz_STAFF.EMPSTARTDATE emp_start_date,
       stage_hash_spabiz_STAFF.EMPENDDATE emp_end_date,
       stage_hash_spabiz_STAFF.CANUSESYSTEM can_use_system,
       stage_hash_spabiz_STAFF.BALANCE balance,
       stage_hash_spabiz_STAFF.PRINTTRAVELER print_traveler,
       stage_hash_spabiz_STAFF.POPUPINFO pop_up_info,
       stage_hash_spabiz_STAFF.NOTE note,
       stage_hash_spabiz_STAFF.PRINT1 print_1,
       stage_hash_spabiz_STAFF.PRINT2 print_2,
       stage_hash_spabiz_STAFF.PRINT3 print_3,
       stage_hash_spabiz_STAFF.PRINT4 print_4,
       stage_hash_spabiz_STAFF.PRINT5 print_5,
       stage_hash_spabiz_STAFF.PRINT6 print_6,
       stage_hash_spabiz_STAFF.PRINT7 print_7,
       stage_hash_spabiz_STAFF.STARTAPCYCLE start_ap_cycle,
       stage_hash_spabiz_STAFF.APCYCLECOUNT ap_cycle_count,
       stage_hash_spabiz_STAFF.STATUS status,
       stage_hash_spabiz_STAFF.BDAY b_day,
       stage_hash_spabiz_STAFF.ANNIVERSARY anniversary,
       stage_hash_spabiz_STAFF.CLOCKINREQ clock_in_req,
       stage_hash_spabiz_STAFF.WAGETYPE wage_type,
       stage_hash_spabiz_STAFF.WAGE wage,
       stage_hash_spabiz_STAFF.WAGEPERMIN wage_per_min,
       stage_hash_spabiz_STAFF.TGLEVEL tg_level,
       stage_hash_spabiz_STAFF.PRINTPOPUP print_pop_up,
       stage_hash_spabiz_STAFF.BIO bio,
       stage_hash_spabiz_STAFF.TYPEOF type_of,
       stage_hash_spabiz_STAFF.NAME name,
       stage_hash_spabiz_STAFF.PAGERNUM pager_num,
       stage_hash_spabiz_STAFF.PAGERTYPE pager_type,
       stage_hash_spabiz_STAFF.SALES_TOTAL sales_total,
       stage_hash_spabiz_STAFF.STORE_NUMBER store_number,
       stage_hash_spabiz_STAFF.SERVICETEMPLATEID service_template_id,
       stage_hash_spabiz_STAFF.STAFFTEMPLATEID staff_template_id,
       stage_hash_spabiz_STAFF.WEBBOOK web_book,
       stage_hash_spabiz_STAFF.NEILLID neill_id,
       stage_hash_spabiz_STAFF.LOUIS louis,
       stage_hash_spabiz_STAFF.DONOTPRINTPROD do_not_print_prod,
       stage_hash_spabiz_STAFF.PRINTTRAVLER print_travler,
       stage_hash_spabiz_STAFF.FOREIGNID foreign_id,
       stage_hash_spabiz_STAFF.NEWID new_id,
       stage_hash_spabiz_STAFF.STAFFBACKUPID staff_backup_id,
       stage_hash_spabiz_STAFF.USERNAME user_name,
       stage_hash_spabiz_STAFF.ISADMIN is_admin,
       stage_hash_spabiz_STAFF.HEADMAPSTAFF head_map_staff,
       stage_hash_spabiz_STAFF.ALLOWPOWERBOOKING allow_power_booking,
       stage_hash_spabiz_STAFF.EMAIL email,
       stage_hash_spabiz_STAFF.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STAFF.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STAFF.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.FIRSTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.MI,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.LASTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.FLNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.FNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.BOOKNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.ADDRESS1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.ADDRESS2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.CITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.STATE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.TEL_HOME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.TEL_WORK,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.TEL_MOBIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.TEL_PAGER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STAFF.BIRTHDAY,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.SEX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.EMPLOYER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STAFF.EMPSTARTDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STAFF.EMPENDDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.CANUSESYSTEM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.BALANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINTTRAVELER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.POPUPINFO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.NOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINT1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINT2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINT3 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINT4 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINT5 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINT6 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINT7 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STAFF.STARTAPCYCLE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.APCYCLECOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.BDAY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.ANNIVERSARY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.CLOCKINREQ as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.WAGETYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.WAGE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.WAGEPERMIN as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.TGLEVEL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINTPOPUP as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.BIO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.TYPEOF as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PAGERNUM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PAGERTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.SALES_TOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.SERVICETEMPLATEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.STAFFTEMPLATEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.WEBBOOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.NEILLID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.LOUIS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.DONOTPRINTPROD as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.PRINTTRAVLER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.FOREIGNID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.NEWID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.STAFFBACKUPID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.USERNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.ISADMIN as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.HEADMAPSTAFF as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STAFF.ALLOWPOWERBOOKING as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STAFF.EMAIL,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_STAFF
 where stage_hash_spabiz_STAFF.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_staff records
set @insert_date_time = getdate()
insert into s_spabiz_staff (
       bk_hash,
       staff_id,
       counter_id,
       edit_time,
       staff_delete,
       delete_date,
       first_name,
       mi,
       last_name,
       f_l_name,
       f_name,
       quick_id,
       book_name,
       address_1,
       address_2,
       city,
       state,
       zip,
       tel_home,
       tel_work,
       tel_mobil,
       tel_pager,
       birthday,
       sex,
       employer,
       emp_start_date,
       emp_end_date,
       can_use_system,
       balance,
       print_traveler,
       pop_up_info,
       note,
       print_1,
       print_2,
       print_3,
       print_4,
       print_5,
       print_6,
       print_7,
       start_ap_cycle,
       ap_cycle_count,
       status,
       b_day,
       anniversary,
       clock_in_req,
       wage_type,
       wage,
       wage_per_min,
       tg_level,
       print_pop_up,
       bio,
       type_of,
       name,
       pager_num,
       pager_type,
       sales_total,
       store_number,
       service_template_id,
       staff_template_id,
       web_book,
       neill_id,
       louis,
       do_not_print_prod,
       print_travler,
       foreign_id,
       new_id,
       staff_backup_id,
       user_name,
       is_admin,
       head_map_staff,
       allow_power_booking,
       email,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_staff_inserts.bk_hash,
       #s_spabiz_staff_inserts.staff_id,
       #s_spabiz_staff_inserts.counter_id,
       #s_spabiz_staff_inserts.edit_time,
       #s_spabiz_staff_inserts.staff_delete,
       #s_spabiz_staff_inserts.delete_date,
       #s_spabiz_staff_inserts.first_name,
       #s_spabiz_staff_inserts.mi,
       #s_spabiz_staff_inserts.last_name,
       #s_spabiz_staff_inserts.f_l_name,
       #s_spabiz_staff_inserts.f_name,
       #s_spabiz_staff_inserts.quick_id,
       #s_spabiz_staff_inserts.book_name,
       #s_spabiz_staff_inserts.address_1,
       #s_spabiz_staff_inserts.address_2,
       #s_spabiz_staff_inserts.city,
       #s_spabiz_staff_inserts.state,
       #s_spabiz_staff_inserts.zip,
       #s_spabiz_staff_inserts.tel_home,
       #s_spabiz_staff_inserts.tel_work,
       #s_spabiz_staff_inserts.tel_mobil,
       #s_spabiz_staff_inserts.tel_pager,
       #s_spabiz_staff_inserts.birthday,
       #s_spabiz_staff_inserts.sex,
       #s_spabiz_staff_inserts.employer,
       #s_spabiz_staff_inserts.emp_start_date,
       #s_spabiz_staff_inserts.emp_end_date,
       #s_spabiz_staff_inserts.can_use_system,
       #s_spabiz_staff_inserts.balance,
       #s_spabiz_staff_inserts.print_traveler,
       #s_spabiz_staff_inserts.pop_up_info,
       #s_spabiz_staff_inserts.note,
       #s_spabiz_staff_inserts.print_1,
       #s_spabiz_staff_inserts.print_2,
       #s_spabiz_staff_inserts.print_3,
       #s_spabiz_staff_inserts.print_4,
       #s_spabiz_staff_inserts.print_5,
       #s_spabiz_staff_inserts.print_6,
       #s_spabiz_staff_inserts.print_7,
       #s_spabiz_staff_inserts.start_ap_cycle,
       #s_spabiz_staff_inserts.ap_cycle_count,
       #s_spabiz_staff_inserts.status,
       #s_spabiz_staff_inserts.b_day,
       #s_spabiz_staff_inserts.anniversary,
       #s_spabiz_staff_inserts.clock_in_req,
       #s_spabiz_staff_inserts.wage_type,
       #s_spabiz_staff_inserts.wage,
       #s_spabiz_staff_inserts.wage_per_min,
       #s_spabiz_staff_inserts.tg_level,
       #s_spabiz_staff_inserts.print_pop_up,
       #s_spabiz_staff_inserts.bio,
       #s_spabiz_staff_inserts.type_of,
       #s_spabiz_staff_inserts.name,
       #s_spabiz_staff_inserts.pager_num,
       #s_spabiz_staff_inserts.pager_type,
       #s_spabiz_staff_inserts.sales_total,
       #s_spabiz_staff_inserts.store_number,
       #s_spabiz_staff_inserts.service_template_id,
       #s_spabiz_staff_inserts.staff_template_id,
       #s_spabiz_staff_inserts.web_book,
       #s_spabiz_staff_inserts.neill_id,
       #s_spabiz_staff_inserts.louis,
       #s_spabiz_staff_inserts.do_not_print_prod,
       #s_spabiz_staff_inserts.print_travler,
       #s_spabiz_staff_inserts.foreign_id,
       #s_spabiz_staff_inserts.new_id,
       #s_spabiz_staff_inserts.staff_backup_id,
       #s_spabiz_staff_inserts.user_name,
       #s_spabiz_staff_inserts.is_admin,
       #s_spabiz_staff_inserts.head_map_staff,
       #s_spabiz_staff_inserts.allow_power_booking,
       #s_spabiz_staff_inserts.email,
       case when s_spabiz_staff.s_spabiz_staff_id is null then isnull(#s_spabiz_staff_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_staff_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_staff_inserts
  left join p_spabiz_staff
    on #s_spabiz_staff_inserts.bk_hash = p_spabiz_staff.bk_hash
   and p_spabiz_staff.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_staff
    on p_spabiz_staff.bk_hash = s_spabiz_staff.bk_hash
   and p_spabiz_staff.s_spabiz_staff_id = s_spabiz_staff.s_spabiz_staff_id
 where s_spabiz_staff.s_spabiz_staff_id is null
    or (s_spabiz_staff.s_spabiz_staff_id is not null
        and s_spabiz_staff.dv_hash <> #s_spabiz_staff_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_staff @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_staff @current_dv_batch_id

end
