CREATE PROC [dbo].[proc_etl_spabiz_customer] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_CUSTOMER

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_CUSTOMER (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       FIRSTNAME,
       LASTNAME,
       FLNAME,
       FNAME,
       LNAME,
       QUICKID,
       ADDRESS1,
       ADDRESS2,
       CITY,
       STATE,
       ZIP,
       COUNTRY,
       TEL_HOME,
       TEL_WORK,
       TEL_WORKEXT,
       TEL_WORKFAX,
       TEL_MOBIL,
       TEL_PAGER,
       TEL_WHICH,
       EMAIL,
       BDAY,
       SEX,
       PAYERID,
       ACTIVESTATUS,
       CREATEDDATE,
       FIRSTVISIT,
       LASTAPDATE,
       LASTDATE,
       TOTALVISITS,
       SERVICEVISITS,
       RETAINED,
       DRIVERSLICENSE,
       REFERRALID,
       APPCONFIRM,
       APPCONFIRMONDAY,
       TOTALLATE,
       TOTALNOSHOW,
       BALANCE,
       CREDITLIMIT,
       ALLERGIES,
       MEDICATION,
       OCCUPATION,
       EMPLOYER,
       CALLDAYS,
       NOTE,
       SHOWNOTE,
       DONOTCHARGETAX,
       CHARGECOST,
       TAXNUM,
       PRIMARYSTAFFID,
       MAILOK,
       TOTALSERVICE,
       TOTALPRODUCT,
       YTDSERVICE,
       YTDPRODUCT,
       LASTCALLED,
       MARITAL,
       ALTID,
       RID,
       USERNAME,
       store_number,
       CUSTOMERID,
       FN,
       LN,
       ISURGENT,
       DONOTPRINTNOTE,
       URGENT,
       FOREIGNID,
       MEMBERID,
       NOTE1,
       CUSTOMERVID,
       MEMBERVID,
       MEMBERSTATUS,
       MEMBERCATEGORY,
       STATUS,
       APTHOLDINFO,
       UDFIELD1,
       UDFIELD2,
       UDFIELD3,
       UDFIELD4,
       ACCOUNTNUMBER,
       EMAILOK,
       MIDDLENAME,
       MEMBERSHIPID,
       MEMBERACTIVE,
       TEXTMSGOK,
       EMAIL_RECURRING,
       MERCURY1,
       MERCURY2,
       MERCURY3,
       MERCURY4,
       TITLE,
       GENDERPREF,
       PARENTID,
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
       LASTNAME,
       FLNAME,
       FNAME,
       LNAME,
       QUICKID,
       ADDRESS1,
       ADDRESS2,
       CITY,
       STATE,
       ZIP,
       COUNTRY,
       TEL_HOME,
       TEL_WORK,
       TEL_WORKEXT,
       TEL_WORKFAX,
       TEL_MOBIL,
       TEL_PAGER,
       TEL_WHICH,
       EMAIL,
       BDAY,
       SEX,
       PAYERID,
       ACTIVESTATUS,
       CREATEDDATE,
       FIRSTVISIT,
       LASTAPDATE,
       LASTDATE,
       TOTALVISITS,
       SERVICEVISITS,
       RETAINED,
       DRIVERSLICENSE,
       REFERRALID,
       APPCONFIRM,
       APPCONFIRMONDAY,
       TOTALLATE,
       TOTALNOSHOW,
       BALANCE,
       CREDITLIMIT,
       ALLERGIES,
       MEDICATION,
       OCCUPATION,
       EMPLOYER,
       CALLDAYS,
       NOTE,
       SHOWNOTE,
       DONOTCHARGETAX,
       CHARGECOST,
       TAXNUM,
       PRIMARYSTAFFID,
       MAILOK,
       TOTALSERVICE,
       TOTALPRODUCT,
       YTDSERVICE,
       YTDPRODUCT,
       LASTCALLED,
       MARITAL,
       ALTID,
       RID,
       USERNAME,
       store_number,
       CUSTOMERID,
       FN,
       LN,
       ISURGENT,
       DONOTPRINTNOTE,
       URGENT,
       FOREIGNID,
       MEMBERID,
       NOTE1,
       CUSTOMERVID,
       MEMBERVID,
       MEMBERSTATUS,
       MEMBERCATEGORY,
       STATUS,
       APTHOLDINFO,
       UDFIELD1,
       UDFIELD2,
       UDFIELD3,
       UDFIELD4,
       ACCOUNTNUMBER,
       EMAILOK,
       MIDDLENAME,
       MEMBERSHIPID,
       MEMBERACTIVE,
       TEXTMSGOK,
       EMAIL_RECURRING,
       MERCURY1,
       MERCURY2,
       MERCURY3,
       MERCURY4,
       TITLE,
       GENDERPREF,
       PARENTID,
       isnull(cast(stage_spabiz_CUSTOMER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_CUSTOMER
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_customer @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_customer (
       bk_hash,
       customer_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_CUSTOMER.bk_hash,
       stage_hash_spabiz_CUSTOMER.ID customer_id,
       stage_hash_spabiz_CUSTOMER.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_CUSTOMER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_CUSTOMER
  left join h_spabiz_customer
    on stage_hash_spabiz_CUSTOMER.bk_hash = h_spabiz_customer.bk_hash
 where h_spabiz_customer_id is null
   and stage_hash_spabiz_CUSTOMER.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_customer
if object_id('tempdb..#l_spabiz_customer_inserts') is not null drop table #l_spabiz_customer_inserts
create table #l_spabiz_customer_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CUSTOMER.bk_hash,
       stage_hash_spabiz_CUSTOMER.ID customer_id,
       stage_hash_spabiz_CUSTOMER.STOREID store_id,
       stage_hash_spabiz_CUSTOMER.PAYERID payer_id,
       stage_hash_spabiz_CUSTOMER.PRIMARYSTAFFID primary_staff_id,
       stage_hash_spabiz_CUSTOMER.STORE_NUMBER store_number,
       stage_hash_spabiz_CUSTOMER.MEMBERID member_id,
       stage_hash_spabiz_CUSTOMER.MEMBERSHIPID membership_id,
       stage_hash_spabiz_CUSTOMER.PARENTID parent_id,
       isnull(cast(stage_hash_spabiz_CUSTOMER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.PAYERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.PRIMARYSTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MEMBERID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.MEMBERSHIPID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.PARENTID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CUSTOMER
 where stage_hash_spabiz_CUSTOMER.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_customer records
set @insert_date_time = getdate()
insert into l_spabiz_customer (
       bk_hash,
       customer_id,
       store_id,
       payer_id,
       primary_staff_id,
       store_number,
       member_id,
       membership_id,
       parent_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_customer_inserts.bk_hash,
       #l_spabiz_customer_inserts.customer_id,
       #l_spabiz_customer_inserts.store_id,
       #l_spabiz_customer_inserts.payer_id,
       #l_spabiz_customer_inserts.primary_staff_id,
       #l_spabiz_customer_inserts.store_number,
       #l_spabiz_customer_inserts.member_id,
       #l_spabiz_customer_inserts.membership_id,
       #l_spabiz_customer_inserts.parent_id,
       case when l_spabiz_customer.l_spabiz_customer_id is null then isnull(#l_spabiz_customer_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_customer_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_customer_inserts
  left join p_spabiz_customer
    on #l_spabiz_customer_inserts.bk_hash = p_spabiz_customer.bk_hash
   and p_spabiz_customer.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_customer
    on p_spabiz_customer.bk_hash = l_spabiz_customer.bk_hash
   and p_spabiz_customer.l_spabiz_customer_id = l_spabiz_customer.l_spabiz_customer_id
 where l_spabiz_customer.l_spabiz_customer_id is null
    or (l_spabiz_customer.l_spabiz_customer_id is not null
        and l_spabiz_customer.dv_hash <> #l_spabiz_customer_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_customer
if object_id('tempdb..#s_spabiz_customer_inserts') is not null drop table #s_spabiz_customer_inserts
create table #s_spabiz_customer_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CUSTOMER.bk_hash,
       stage_hash_spabiz_CUSTOMER.ID customer_id,
       stage_hash_spabiz_CUSTOMER.COUNTERID counter_id,
       stage_hash_spabiz_CUSTOMER.EDITTIME edit_time,
       stage_hash_spabiz_CUSTOMER.[Delete] customer_delete,
       stage_hash_spabiz_CUSTOMER.DELETEDATE delete_date,
       stage_hash_spabiz_CUSTOMER.FIRSTNAME first_name,
       stage_hash_spabiz_CUSTOMER.LASTNAME last_name,
       stage_hash_spabiz_CUSTOMER.FLNAME f_l_name,
       stage_hash_spabiz_CUSTOMER.FNAME f_name,
       stage_hash_spabiz_CUSTOMER.LNAME l_name,
       stage_hash_spabiz_CUSTOMER.QUICKID quick_id,
       stage_hash_spabiz_CUSTOMER.ADDRESS1 address_1,
       stage_hash_spabiz_CUSTOMER.ADDRESS2 address_2,
       stage_hash_spabiz_CUSTOMER.CITY city,
       stage_hash_spabiz_CUSTOMER.STATE state,
       stage_hash_spabiz_CUSTOMER.ZIP zip,
       stage_hash_spabiz_CUSTOMER.COUNTRY country,
       stage_hash_spabiz_CUSTOMER.TEL_HOME tel_home,
       stage_hash_spabiz_CUSTOMER.TEL_WORK tel_work,
       stage_hash_spabiz_CUSTOMER.TEL_WORKEXT tel_work_ext,
       stage_hash_spabiz_CUSTOMER.TEL_WORKFAX tel_work_fax,
       stage_hash_spabiz_CUSTOMER.TEL_MOBIL tel_mobil,
       stage_hash_spabiz_CUSTOMER.TEL_PAGER tel_pager,
       stage_hash_spabiz_CUSTOMER.TEL_WHICH tel_which,
       stage_hash_spabiz_CUSTOMER.EMAIL email,
       stage_hash_spabiz_CUSTOMER.BDAY b_day,
       stage_hash_spabiz_CUSTOMER.SEX sex,
       stage_hash_spabiz_CUSTOMER.ACTIVESTATUS active_status,
       stage_hash_spabiz_CUSTOMER.CREATEDDATE created_date,
       stage_hash_spabiz_CUSTOMER.FIRSTVISIT first_visit,
       stage_hash_spabiz_CUSTOMER.LASTAPDATE last_ap_date,
       stage_hash_spabiz_CUSTOMER.LASTDATE last_date,
       stage_hash_spabiz_CUSTOMER.TOTALVISITS total_visits,
       stage_hash_spabiz_CUSTOMER.SERVICEVISITS service_visits,
       stage_hash_spabiz_CUSTOMER.RETAINED retained,
       stage_hash_spabiz_CUSTOMER.DRIVERSLICENSE drivers_license,
       stage_hash_spabiz_CUSTOMER.REFERRALID referral_id,
       stage_hash_spabiz_CUSTOMER.APPCONFIRM app_confirm,
       stage_hash_spabiz_CUSTOMER.APPCONFIRMONDAY app_confirm_on_day,
       stage_hash_spabiz_CUSTOMER.TOTALLATE total_late,
       stage_hash_spabiz_CUSTOMER.TOTALNOSHOW total_no_show,
       stage_hash_spabiz_CUSTOMER.BALANCE balance,
       stage_hash_spabiz_CUSTOMER.CREDITLIMIT credit_limit,
       stage_hash_spabiz_CUSTOMER.ALLERGIES allergies,
       stage_hash_spabiz_CUSTOMER.MEDICATION medication,
       stage_hash_spabiz_CUSTOMER.OCCUPATION occupation,
       stage_hash_spabiz_CUSTOMER.EMPLOYER employer,
       stage_hash_spabiz_CUSTOMER.CALLDAYS call_days,
       stage_hash_spabiz_CUSTOMER.NOTE note,
       stage_hash_spabiz_CUSTOMER.SHOWNOTE show_note,
       stage_hash_spabiz_CUSTOMER.DONOTCHARGETAX do_not_charge_tax,
       stage_hash_spabiz_CUSTOMER.CHARGECOST charge_cost,
       stage_hash_spabiz_CUSTOMER.TAXNUM tax_num,
       stage_hash_spabiz_CUSTOMER.MAILOK mail_ok,
       stage_hash_spabiz_CUSTOMER.TOTALSERVICE total_service,
       stage_hash_spabiz_CUSTOMER.TOTALPRODUCT total_product,
       stage_hash_spabiz_CUSTOMER.YTDSERVICE ytd_service,
       stage_hash_spabiz_CUSTOMER.YTDPRODUCT ytd_product,
       stage_hash_spabiz_CUSTOMER.LASTCALLED last_called,
       stage_hash_spabiz_CUSTOMER.MARITAL marital,
       stage_hash_spabiz_CUSTOMER.ALTID alt_id,
       stage_hash_spabiz_CUSTOMER.RID r_id,
       stage_hash_spabiz_CUSTOMER.USERNAME user_name,
       stage_hash_spabiz_CUSTOMER.STORE_NUMBER store_number,
       stage_hash_spabiz_CUSTOMER.CUSTOMERID customer_customer_id,
       stage_hash_spabiz_CUSTOMER.FN fn,
       stage_hash_spabiz_CUSTOMER.LN ln,
       stage_hash_spabiz_CUSTOMER.ISURGENT is_urgent,
       stage_hash_spabiz_CUSTOMER.DONOTPRINTNOTE do_not_print_note,
       stage_hash_spabiz_CUSTOMER.URGENT urgent,
       stage_hash_spabiz_CUSTOMER.FOREIGNID foreign_id,
       stage_hash_spabiz_CUSTOMER.NOTE1 note_1,
       stage_hash_spabiz_CUSTOMER.CUSTOMERVID customer_v_id,
       stage_hash_spabiz_CUSTOMER.MEMBERVID member_v_id,
       stage_hash_spabiz_CUSTOMER.MEMBERSTATUS member_status,
       stage_hash_spabiz_CUSTOMER.MEMBERCATEGORY member_category,
       stage_hash_spabiz_CUSTOMER.STATUS status,
       stage_hash_spabiz_CUSTOMER.APTHOLDINFO apt_hold_info,
       stage_hash_spabiz_CUSTOMER.UDFIELD1 ud_field_1,
       stage_hash_spabiz_CUSTOMER.UDFIELD2 ud_field_2,
       stage_hash_spabiz_CUSTOMER.UDFIELD3 ud_field_3,
       stage_hash_spabiz_CUSTOMER.UDFIELD4 ud_field_4,
       stage_hash_spabiz_CUSTOMER.ACCOUNTNUMBER account_number,
       stage_hash_spabiz_CUSTOMER.EMAILOK email_ok,
       stage_hash_spabiz_CUSTOMER.MIDDLENAME middle_name,
       stage_hash_spabiz_CUSTOMER.MEMBERACTIVE member_active,
       stage_hash_spabiz_CUSTOMER.TEXTMSGOK text_msg_ok,
       stage_hash_spabiz_CUSTOMER.EMAIL_RECURRING email_recurring,
       stage_hash_spabiz_CUSTOMER.MERCURY1 mercury_1,
       stage_hash_spabiz_CUSTOMER.MERCURY2 mercury_2,
       stage_hash_spabiz_CUSTOMER.MERCURY3 mercury_3,
       stage_hash_spabiz_CUSTOMER.MERCURY4 mercury_4,
       stage_hash_spabiz_CUSTOMER.TITLE title,
       stage_hash_spabiz_CUSTOMER.GENDERPREF gender_pref,
       isnull(cast(stage_hash_spabiz_CUSTOMER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTOMER.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTOMER.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.FIRSTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.LASTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.FLNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.FNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.LNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.ADDRESS1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.ADDRESS2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.CITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.STATE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.COUNTRY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.TEL_HOME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.TEL_WORK,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.TEL_WORKEXT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.TEL_WORKFAX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.TEL_MOBIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.TEL_PAGER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.TEL_WHICH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.EMAIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.BDAY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.SEX as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.ACTIVESTATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTOMER.CREATEDDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTOMER.FIRSTVISIT,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTOMER.LASTAPDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTOMER.LASTDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.TOTALVISITS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.SERVICEVISITS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.RETAINED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.DRIVERSLICENSE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.REFERRALID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.APPCONFIRM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.APPCONFIRMONDAY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.TOTALLATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.TOTALNOSHOW as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.BALANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.CREDITLIMIT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.ALLERGIES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MEDICATION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.OCCUPATION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.EMPLOYER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.CALLDAYS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.NOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.SHOWNOTE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.DONOTCHARGETAX as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.CHARGECOST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.TAXNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.MAILOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.TOTALSERVICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.TOTALPRODUCT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.YTDSERVICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.YTDPRODUCT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CUSTOMER.LASTCALLED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MARITAL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.ALTID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.RID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.USERNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.CUSTOMERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.FN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.LN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.ISURGENT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.DONOTPRINTNOTE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.URGENT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.FOREIGNID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.NOTE1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.CUSTOMERVID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MEMBERVID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MEMBERSTATUS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MEMBERCATEGORY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.STATUS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.APTHOLDINFO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.UDFIELD1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.UDFIELD2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.UDFIELD3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.UDFIELD4,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.ACCOUNTNUMBER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.EMAILOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MIDDLENAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.MEMBERACTIVE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.TEXTMSGOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.EMAIL_RECURRING as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MERCURY1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MERCURY2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MERCURY3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.MERCURY4,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CUSTOMER.TITLE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_CUSTOMER.GENDERPREF as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CUSTOMER
 where stage_hash_spabiz_CUSTOMER.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_customer records
set @insert_date_time = getdate()
insert into s_spabiz_customer (
       bk_hash,
       customer_id,
       counter_id,
       edit_time,
       customer_delete,
       delete_date,
       first_name,
       last_name,
       f_l_name,
       f_name,
       l_name,
       quick_id,
       address_1,
       address_2,
       city,
       state,
       zip,
       country,
       tel_home,
       tel_work,
       tel_work_ext,
       tel_work_fax,
       tel_mobil,
       tel_pager,
       tel_which,
       email,
       b_day,
       sex,
       active_status,
       created_date,
       first_visit,
       last_ap_date,
       last_date,
       total_visits,
       service_visits,
       retained,
       drivers_license,
       referral_id,
       app_confirm,
       app_confirm_on_day,
       total_late,
       total_no_show,
       balance,
       credit_limit,
       allergies,
       medication,
       occupation,
       employer,
       call_days,
       note,
       show_note,
       do_not_charge_tax,
       charge_cost,
       tax_num,
       mail_ok,
       total_service,
       total_product,
       ytd_service,
       ytd_product,
       last_called,
       marital,
       alt_id,
       r_id,
       user_name,
       store_number,
       customer_customer_id,
       fn,
       ln,
       is_urgent,
       do_not_print_note,
       urgent,
       foreign_id,
       note_1,
       customer_v_id,
       member_v_id,
       member_status,
       member_category,
       status,
       apt_hold_info,
       ud_field_1,
       ud_field_2,
       ud_field_3,
       ud_field_4,
       account_number,
       email_ok,
       middle_name,
       member_active,
       text_msg_ok,
       email_recurring,
       mercury_1,
       mercury_2,
       mercury_3,
       mercury_4,
       title,
       gender_pref,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_customer_inserts.bk_hash,
       #s_spabiz_customer_inserts.customer_id,
       #s_spabiz_customer_inserts.counter_id,
       #s_spabiz_customer_inserts.edit_time,
       #s_spabiz_customer_inserts.customer_delete,
       #s_spabiz_customer_inserts.delete_date,
       #s_spabiz_customer_inserts.first_name,
       #s_spabiz_customer_inserts.last_name,
       #s_spabiz_customer_inserts.f_l_name,
       #s_spabiz_customer_inserts.f_name,
       #s_spabiz_customer_inserts.l_name,
       #s_spabiz_customer_inserts.quick_id,
       #s_spabiz_customer_inserts.address_1,
       #s_spabiz_customer_inserts.address_2,
       #s_spabiz_customer_inserts.city,
       #s_spabiz_customer_inserts.state,
       #s_spabiz_customer_inserts.zip,
       #s_spabiz_customer_inserts.country,
       #s_spabiz_customer_inserts.tel_home,
       #s_spabiz_customer_inserts.tel_work,
       #s_spabiz_customer_inserts.tel_work_ext,
       #s_spabiz_customer_inserts.tel_work_fax,
       #s_spabiz_customer_inserts.tel_mobil,
       #s_spabiz_customer_inserts.tel_pager,
       #s_spabiz_customer_inserts.tel_which,
       #s_spabiz_customer_inserts.email,
       #s_spabiz_customer_inserts.b_day,
       #s_spabiz_customer_inserts.sex,
       #s_spabiz_customer_inserts.active_status,
       #s_spabiz_customer_inserts.created_date,
       #s_spabiz_customer_inserts.first_visit,
       #s_spabiz_customer_inserts.last_ap_date,
       #s_spabiz_customer_inserts.last_date,
       #s_spabiz_customer_inserts.total_visits,
       #s_spabiz_customer_inserts.service_visits,
       #s_spabiz_customer_inserts.retained,
       #s_spabiz_customer_inserts.drivers_license,
       #s_spabiz_customer_inserts.referral_id,
       #s_spabiz_customer_inserts.app_confirm,
       #s_spabiz_customer_inserts.app_confirm_on_day,
       #s_spabiz_customer_inserts.total_late,
       #s_spabiz_customer_inserts.total_no_show,
       #s_spabiz_customer_inserts.balance,
       #s_spabiz_customer_inserts.credit_limit,
       #s_spabiz_customer_inserts.allergies,
       #s_spabiz_customer_inserts.medication,
       #s_spabiz_customer_inserts.occupation,
       #s_spabiz_customer_inserts.employer,
       #s_spabiz_customer_inserts.call_days,
       #s_spabiz_customer_inserts.note,
       #s_spabiz_customer_inserts.show_note,
       #s_spabiz_customer_inserts.do_not_charge_tax,
       #s_spabiz_customer_inserts.charge_cost,
       #s_spabiz_customer_inserts.tax_num,
       #s_spabiz_customer_inserts.mail_ok,
       #s_spabiz_customer_inserts.total_service,
       #s_spabiz_customer_inserts.total_product,
       #s_spabiz_customer_inserts.ytd_service,
       #s_spabiz_customer_inserts.ytd_product,
       #s_spabiz_customer_inserts.last_called,
       #s_spabiz_customer_inserts.marital,
       #s_spabiz_customer_inserts.alt_id,
       #s_spabiz_customer_inserts.r_id,
       #s_spabiz_customer_inserts.user_name,
       #s_spabiz_customer_inserts.store_number,
       #s_spabiz_customer_inserts.customer_customer_id,
       #s_spabiz_customer_inserts.fn,
       #s_spabiz_customer_inserts.ln,
       #s_spabiz_customer_inserts.is_urgent,
       #s_spabiz_customer_inserts.do_not_print_note,
       #s_spabiz_customer_inserts.urgent,
       #s_spabiz_customer_inserts.foreign_id,
       #s_spabiz_customer_inserts.note_1,
       #s_spabiz_customer_inserts.customer_v_id,
       #s_spabiz_customer_inserts.member_v_id,
       #s_spabiz_customer_inserts.member_status,
       #s_spabiz_customer_inserts.member_category,
       #s_spabiz_customer_inserts.status,
       #s_spabiz_customer_inserts.apt_hold_info,
       #s_spabiz_customer_inserts.ud_field_1,
       #s_spabiz_customer_inserts.ud_field_2,
       #s_spabiz_customer_inserts.ud_field_3,
       #s_spabiz_customer_inserts.ud_field_4,
       #s_spabiz_customer_inserts.account_number,
       #s_spabiz_customer_inserts.email_ok,
       #s_spabiz_customer_inserts.middle_name,
       #s_spabiz_customer_inserts.member_active,
       #s_spabiz_customer_inserts.text_msg_ok,
       #s_spabiz_customer_inserts.email_recurring,
       #s_spabiz_customer_inserts.mercury_1,
       #s_spabiz_customer_inserts.mercury_2,
       #s_spabiz_customer_inserts.mercury_3,
       #s_spabiz_customer_inserts.mercury_4,
       #s_spabiz_customer_inserts.title,
       #s_spabiz_customer_inserts.gender_pref,
       case when s_spabiz_customer.s_spabiz_customer_id is null then isnull(#s_spabiz_customer_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_customer_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_customer_inserts
  left join p_spabiz_customer
    on #s_spabiz_customer_inserts.bk_hash = p_spabiz_customer.bk_hash
   and p_spabiz_customer.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_customer
    on p_spabiz_customer.bk_hash = s_spabiz_customer.bk_hash
   and p_spabiz_customer.s_spabiz_customer_id = s_spabiz_customer.s_spabiz_customer_id
 where s_spabiz_customer.s_spabiz_customer_id is null
    or (s_spabiz_customer.s_spabiz_customer_id is not null
        and s_spabiz_customer.dv_hash <> #s_spabiz_customer_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_customer @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_customer @current_dv_batch_id

end
