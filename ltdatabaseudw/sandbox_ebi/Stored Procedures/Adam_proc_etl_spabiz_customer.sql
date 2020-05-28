CREATE PROC [sandbox_ebi].[Adam_proc_etl_spabiz_customer] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)
declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_customer)

--truncate table sandbox_ebi.stage_hash_spabiz_CUSTOMER

set @insert_date_time = getdate()

create table #stage_hash_spabiz_CUSTOMER WITH (distribution=hash(bk_hash), location=user_db) as
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID as customer_id,
       COUNTERID as counter_id,
       STOREID,   --only exists here
       EDITTIME as edit_time,
       [Delete] as customer_delete,
       DELETEDATE as delete_date,
       FIRSTNAME as first_name,
       LASTNAME as last_name,
       FLNAME as f_l_name,
       FNAME as f_name,
       LNAME as l_name,
       QUICKID as quick_id,
       ADDRESS1 as address_1,
       ADDRESS2 as address_2,
       CITY as city,
       STATE as state,
       ZIP as zip,
       COUNTRY as country,
       TEL_HOME as tel_home,
       TEL_WORK as tel_work,
       TEL_WORKEXT as tel_work_ext,
       TEL_WORKFAX as tel_work_fax,
       TEL_MOBIL as tel_mobil,
       TEL_PAGER as tel_pager,
       TEL_WHICH as tel_which,
       EMAIL  as email,
       BDAY as b_day,
       SEX  as sex,
       PAYERID,--only here
       ACTIVESTATUS as active_status,
       CREATEDDATE as created_date,
       FIRSTVISIT as first_visit,
       LASTAPDATE as last_ap_date,
       LASTDATE as last_date,
       TOTALVISITS  as total_visits,
       SERVICEVISITS as service_visits,
       RETAINED  as retained,
       DRIVERSLICENSE as drivers_license,
       REFERRALID as referral_id,
       APPCONFIRM as app_confirm,
       APPCONFIRMONDAY as app_confirm_on_day,
       TOTALLATE as total_late,
       TOTALNOSHOW as total_no_show,
       BALANCE as balance,
       CREDITLIMIT as credit_limit,
       ALLERGIES as allergies,
       MEDICATION as medication,
       OCCUPATION  as occupation,
       EMPLOYER as employer,
       CALLDAYS as call_days,
       NOTE as note,
       SHOWNOTE as show_note,
       DONOTCHARGETAX as do_not_charge_tax,
       CHARGECOST as charge_cost,
       TAXNUM as tax_num,
       PRIMARYSTAFFID as primary_staff_id,  --only here
       MAILOK as mail_ok,
       TOTALSERVICE as total_service,
       TOTALPRODUCT as total_product,
       YTDSERVICE as ytd_service,
       YTDPRODUCT as ytd_product,
       LASTCALLED as last_called,
       MARITAL as marital,
       ALTID  as alt_id,
       RID as r_id,
       USERNAME as user_name,
       store_number as store_number,
       CUSTOMERID  as customer_customer_id,
       FN as fn,
       LN as ln,
       ISURGENT as is_urgent,
       DONOTPRINTNOTE as do_not_print_note,
       URGENT  as urgent,
       FOREIGNID as foreign_id,
       MEMBERID,  --
       NOTE1 as note_1,
       CUSTOMERVID as customer_v_id,
       MEMBERVID member_v_id,
       MEMBERSTATUS as member_status,
       MEMBERCATEGORY as member_category,
       STATUS as status,
       APTHOLDINFO  as apt_hold_info,
       UDFIELD1 as ud_field_1,
       UDFIELD2 as ud_field_2,
       UDFIELD3 as ud_field_3,
       UDFIELD4 as ud_field_4,
       ACCOUNTNUMBER as account_number,
       EMAILOK as email_ok,
       MIDDLENAME as middle_name,
       MEMBERSHIPID, --
       MEMBERACTIVE as member_active,
       TEXTMSGOK as text_msg_ok,
       EMAIL_RECURRING as email_recurring,
       MERCURY1 as mercury_1,
       MERCURY2 as mercury_2,
       MERCURY3 as mercury_3 ,
       MERCURY4 as mercury_4,
       TITLE as title,
       GENDERPREF as gender_pref,
       PARENTID, --
	   isnull(cast(sandbox_ebi.stage_spabiz_CUSTOMER.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
	   convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_spabiz_customer.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_spabiz_customer.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.[delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_spabiz_customer.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.FIRSTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.LASTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.FLNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.FNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.LNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.ADDRESS1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.ADDRESS2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.CITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.STATE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.COUNTRY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.TEL_HOME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.TEL_WORK,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.TEL_WORKEXT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.TEL_WORKFAX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.TEL_MOBIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.TEL_PAGER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.TEL_WHICH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.EMAIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.BDAY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.SEX as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.ACTIVESTATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_spabiz_customer.CREATEDDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_spabiz_customer.FIRSTVISIT,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_spabiz_customer.LASTAPDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_spabiz_customer.LASTDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.TOTALVISITS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.SERVICEVISITS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.RETAINED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.DRIVERSLICENSE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.REFERRALID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.APPCONFIRM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.APPCONFIRMONDAY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.TOTALLATE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.TOTALNOSHOW as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.BALANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.CREDITLIMIT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.ALLERGIES,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MEDICATION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.OCCUPATION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.EMPLOYER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.CALLDAYS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.NOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.SHOWNOTE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.DONOTCHARGETAX as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.CHARGECOST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.TAXNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.MAILOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.TOTALSERVICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.TOTALPRODUCT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.YTDSERVICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.YTDPRODUCT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_spabiz_customer.LASTCALLED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MARITAL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.ALTID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.RID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.USERNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.CUSTOMERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.FN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.LN,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.ISURGENT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.DONOTPRINTNOTE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.URGENT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.FOREIGNID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.NOTE1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.CUSTOMERVID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MEMBERVID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MEMBERSTATUS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MEMBERCATEGORY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.STATUS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.APTHOLDINFO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.UDFIELD1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.UDFIELD2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.UDFIELD3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.UDFIELD4,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.ACCOUNTNUMBER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.EMAILOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MIDDLENAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.MEMBERACTIVE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.TEXTMSGOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.EMAIL_RECURRING as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MERCURY1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MERCURY2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MERCURY3,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.MERCURY4,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_spabiz_customer.TITLE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_spabiz_customer.GENDERPREF as varchar(500)),'z#@$k%&P'))),2) as source_hash,
       @insert_date_time as dv_inserted_date_time,
       @user as dv_insert_user,
       dv_batch_id
  from sandbox_ebi.stage_spabiz_CUSTOMER
 where dv_batch_id = @current_dv_batch_id

create table #insert with(distribution=hash(bk_hash), location=user_db) as
  select --d_spabiz_customer.bk_hash as bk_hash, --
	   #stage_hash_spabiz_CUSTOMER.bk_hash as bk_hash,
       d_spabiz_customer.bk_hash as dim_spabiz_customer_key,--
       d_spabiz_customer.customer_id as customer_id, --
       #stage_hash_spabiz_customer.store_number as store_number,
	 --  isnull(#h_spabiz_customer.dv_deleted,0) as dv_deleted  --DONT NEED?
	    case when #stage_hash_spabiz_customer.city is null then ''
            else #stage_hash_spabiz_customer.city
        end address_city,
       case when #stage_hash_spabiz_customer.country is null then ''
            else #stage_hash_spabiz_customer.country
        end address_country,
       case when #stage_hash_spabiz_customer.address_1 is null then ''
            else #stage_hash_spabiz_customer.address_1
        end address_line_1,
       case when #stage_hash_spabiz_customer.address_2 is null then ''
            else #stage_hash_spabiz_customer.address_2
        end address_line_2,
       case when #stage_hash_spabiz_customer.zip is null then ''
            else #stage_hash_spabiz_customer.zip
        end address_postal_code,
       case when #stage_hash_spabiz_customer.state is null then ''
            else #stage_hash_spabiz_customer.state
        end address_state_or_province,
       case when #stage_hash_spabiz_customer.allergies is null then ''
            else #stage_hash_spabiz_customer.allergies
        end allergies,
       #stage_hash_spabiz_customer.balance balance,
       #stage_hash_spabiz_customer.call_days call_days,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'  --
            when #stage_hash_spabiz_customer.email_ok = 1 then 'Y'
            else 'N'
        end communicate_via_email_flag,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N' --
            when #stage_hash_spabiz_customer.mail_ok = 0 then 'Y'
            else 'N'
        end communicate_via_mail_flag,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then null  --
            when #stage_hash_spabiz_customer.created_date = convert(date, '18991230', 112) then null
            else #stage_hash_spabiz_customer.created_date
        end created_date_time,
       #stage_hash_spabiz_customer.credit_limit credit_limit,
       '#stage_hash_spabiz_customer.sex_' + convert(varchar,convert(int,#stage_hash_spabiz_customer.sex)) customer_type_dim_description_key,
       convert(int,#stage_hash_spabiz_customer.sex) customer_type_id
       ,case when #stage_hash_spabiz_customer.b_day is null then ''
            else #stage_hash_spabiz_customer.b_day
        end date_of_birth,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then null  --
            when #stage_hash_spabiz_customer.delete_date = convert(date, '18991230', 112) then null
            else #stage_hash_spabiz_customer.delete_date
        end deleted_date_time,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N' --
            when #stage_hash_spabiz_customer.customer_delete = -1 then 'Y'
            else 'N'
        end deleted_flag,    
	   case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then d_spabiz_customer.bk_hash  --
            when d_spabiz_customer.member_id is null then '-998'
            when d_spabiz_customer.member_id = '0' then '-998'
       	 when d_spabiz_customer.member_id = '' then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(d_spabiz_customer.member_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_member_key,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then d_spabiz_customer.bk_hash
            when d_spabiz_customer.membership_id is null then '-998'
            when d_spabiz_customer.membership_id = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(d_spabiz_customer.membership_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_membership_key,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then d_spabiz_customer.bk_hash
            when #stage_hash_spabiz_customer.store_number is null then '-998'
            when #stage_hash_spabiz_customer.store_number = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(#stage_hash_spabiz_customer.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_store_key,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when #stage_hash_spabiz_customer.do_not_charge_tax = 1 then 'Y'
            else 'N'
        end do_not_charge_tax_flag,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when #stage_hash_spabiz_customer.do_not_print_note = 1 then 'Y'
            else 'N'
        end do_not_print_note_flag,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then null
            when #stage_hash_spabiz_customer.edit_time = convert(date, '18991230', 112) then null
            else #stage_hash_spabiz_customer.edit_time
        end edit_date_time,
       case when #stage_hash_spabiz_customer.email is null then ''
            else #stage_hash_spabiz_customer.email
        end email,
       case when #stage_hash_spabiz_customer.employer is null then ''
            else #stage_hash_spabiz_customer.employer
        end employer,
       case when #stage_hash_spabiz_customer.f_l_name is null then ''
            else #stage_hash_spabiz_customer.f_l_name
        end first_initial_last_name
       ,isnull(upper(left(#stage_hash_spabiz_customer.first_name,1))+lower(substring(#stage_hash_spabiz_customer.first_name,2,len(#stage_hash_spabiz_customer.first_name))),'') first_name,
       #stage_hash_spabiz_customer.first_visit first_visit_date_time,
       case when #stage_hash_spabiz_customer.sex = 1 then 'F'
            when #stage_hash_spabiz_customer.sex = 2 then 'M'
            else 'U' end gender_abbreviation,
       case when #stage_hash_spabiz_customer.tel_home is null then ''
            else #stage_hash_spabiz_customer.tel_home
        end home_phone_number,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then null
            when #stage_hash_spabiz_customer.last_ap_date = convert(date, '18991230', 112) then null
            else #stage_hash_spabiz_customer.last_ap_date
        end last_appointment_date_time,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then null
            when #stage_hash_spabiz_customer.last_called = convert(date, '18991230', 112) then null
            else #stage_hash_spabiz_customer.last_called
        end last_called_date_time,
       case when #stage_hash_spabiz_customer.last_name is null then ''
            else #stage_hash_spabiz_customer.last_name
        end last_name	
       ,#stage_hash_spabiz_customer.last_date last_ticket_processed_date_time,
       '#stage_hash_spabiz_customer.marital_' + convert(varchar,#stage_hash_spabiz_customer.marital) marital_status_dim_description_key,
       #stage_hash_spabiz_customer.marital marital_status_id,
       case when #stage_hash_spabiz_customer.medication is null then ''
            else #stage_hash_spabiz_customer.medication
        end medication,
       case when d_spabiz_customer.member_id is null or d_spabiz_customer.member_id = 0 then '' else d_spabiz_customer.member_id end member_id,
       case when d_spabiz_customer.membership_id is null then '0'
            else d_spabiz_customer.membership_id
        end membership_id,
       case when #stage_hash_spabiz_customer.middle_name is null then ''
            else #stage_hash_spabiz_customer.middle_name
        end middle_name,
       case when #stage_hash_spabiz_customer.tel_mobil is null then ''
            else #stage_hash_spabiz_customer.tel_mobil
        end mobile_phone_number,
       case when #stage_hash_spabiz_customer.note is null then ''
            else #stage_hash_spabiz_customer.note
        end note,
       case when #stage_hash_spabiz_customer.note_1 is null then ''
            else #stage_hash_spabiz_customer.note_1
        end note_1,
       case when #stage_hash_spabiz_customer.occupation is null then ''
            else #stage_hash_spabiz_customer.occupation
        end occupation,
       case when #stage_hash_spabiz_customer.tel_pager is null then ''
            else #stage_hash_spabiz_customer.tel_pager
        end pager_number
		,'#stage_hash_spabiz_customer.tel_which_' + convert(varchar,convert(int,#stage_hash_spabiz_customer.tel_which)) preferred_contact_type_dim_description_key,
       convert(int,#stage_hash_spabiz_customer.tel_which) preferred_contact_type_id,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then d_spabiz_customer.bk_hash
            when #stage_hash_spabiz_customer.PRIMARY_STAFF_ID is null then '-998'
            when #stage_hash_spabiz_customer.PRIMARY_STAFF_ID = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(#stage_hash_spabiz_customer.PRIMARY_STAFF_ID as decimal(26,6)) as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(cast(#stage_hash_spabiz_customer.store_number as decimal(26,6)) as varchar(500)),'z#@$k%&P'))),2)
        end primary_dim_spabiz_staff_key,
       case when #stage_hash_spabiz_customer.quick_id is null then ''
            else #stage_hash_spabiz_customer.quick_id
        end quick_id,
       #stage_hash_spabiz_customer.service_visits service_visits,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when #stage_hash_spabiz_customer.show_note = 1 then 'Y'
            else 'N'
        end show_note_flag,
       case when #stage_hash_spabiz_customer.title is null then ''
            else #stage_hash_spabiz_customer.title
        end title,
       case when #stage_hash_spabiz_customer.total_late is null then 0
            else #stage_hash_spabiz_customer.total_late
        end total_late_show,
       case when #stage_hash_spabiz_customer.total_no_show is null then 0
            else #stage_hash_spabiz_customer.total_no_show
        end total_no_show,
       case when #stage_hash_spabiz_customer.total_product is null then 0
            else #stage_hash_spabiz_customer.total_product
        end total_products_purchased,
       case when #stage_hash_spabiz_customer.total_service is null then 0
            else #stage_hash_spabiz_customer.total_service
        end total_services_purchased,
       case when #stage_hash_spabiz_customer.total_visits is null then 0
            else #stage_hash_spabiz_customer.total_visits
        end total_visits,
       case when d_spabiz_customer.bk_hash in ('-997','-998','-999') then 'N'
            when #stage_hash_spabiz_customer.urgent = 1 then 'Y'
            else 'N'
        end urgent_meesage_flag,
       case when #stage_hash_spabiz_customer.tel_work_fax is null then ''
            else #stage_hash_spabiz_customer.tel_work_fax
        end work_fax,
       case when #stage_hash_spabiz_customer.tel_work_ext is null then ''
            else #stage_hash_spabiz_customer.tel_work_ext
        end work_phone_extension,
       case when #stage_hash_spabiz_customer.tel_work is null then ''
            else #stage_hash_spabiz_customer.tel_work
        end work_phone_number,
       case when #stage_hash_spabiz_customer.ytd_product is null then 0
            else #stage_hash_spabiz_customer.ytd_product
        end ytd_spent_on_products,
       case when #stage_hash_spabiz_customer.ytd_service is null then 0
            else #stage_hash_spabiz_customer.ytd_service
        end ytd_spent_on_services,
    --   isnull(#h_spabiz_customer.dv_deleted,0) dv_deleted     
	  
	   d_spabiz_customer.dv_load_end_date_time,
	   #stage_hash_spabiz_customer.dv_batch_id,
	--   d_spabiz_customer.p_spabiz_customer_id,
	   #stage_hash_spabiz_customer.dv_inserted_date_time,
	   d_spabiz_customer.dv_load_date_time,
	   #stage_hash_spabiz_customer.dv_insert_user
	   from dbo.#stage_hash_spabiz_customer
		left join sandbox_ebi.d_spabiz_customer
		on #stage_hash_spabiz_customer.bk_hash = d_spabiz_customer.bk_hash
		--join #l_spabiz_customer_inserts
  --       on d_spabiz_customer.bk_hash = #l_spabiz_customer_inserts.bk_hash
		--join dbo.#stage_hash_spabiz_customer
		--on d_spabiz_customer.bk_hash = #stage_hash_spabiz_customer.bk_hash
	 --   and d_spabiz_customer.customer_id = #stage_hash_spabiz_customer.customer_id
        
		--without WHERE CLAUSE ALL RECORDS COME BY
		--where #stage_hash_spabiz_customer.store_number not in (1,100,999) OR d_spabiz_customer.bk_hash in ('-999','-998','-997')   
		--AND d_spabiz_customer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
  -- and (d_spabiz_customer.dv_batch_id > @max_dv_batch_id
  --    or d_spabiz_customer.dv_batch_id = @current_dv_batch_id)
  
begin tran
  delete sandbox_ebi.d_spabiz_customer
   where d_spabiz_customer.bk_hash in (select bk_hash from #insert)

  insert sandbox_ebi.d_spabiz_customer(
             bk_hash,
             dim_spabiz_customer_key,
             customer_id,
             store_number,
             address_city,
             address_country,
             address_line_1,
             address_line_2,
             address_postal_code,
             address_state_or_province,
             allergies,
             balance,
             call_days
             ,communicate_via_email_flag,
             communicate_via_mail_flag,
             created_date_time,
             credit_limit,
             customer_type_dim_description_key,
             customer_type_id,
             date_of_birth,
             deleted_date_time,
             deleted_flag,
             dim_mms_member_key,
             dim_mms_membership_key,
             dim_spabiz_store_key,
             do_not_charge_tax_flag,
             do_not_print_note_flag,
             edit_date_time,
             email,
             employer,
             first_initial_last_name,
             first_name,
             first_visit_date_time,
             gender_abbreviation,
             home_phone_number,
             last_appointment_date_time,
             last_called_date_time,
             last_name
             ,last_ticket_processed_date_time
            -- ,marital_status_dim_description_key   ---problem!!!!
		     ,marital_status_id,
             medication 
			 ,member_id,
             membership_id
             ,middle_name,
             mobile_phone_number
			 ,note,
             note_1,
             occupation,
             pager_number,
             preferred_contact_type_dim_description_key,
             preferred_contact_type_id,
             primary_dim_spabiz_staff_key
             ,quick_id,
             service_visits,
             show_note_flag,
             title,
             total_late_show,
             total_no_show,
             total_products_purchased,
             total_services_purchased,
             total_visits,
             urgent_meesage_flag,
             work_fax,
             work_phone_extension,
             work_phone_number,
             ytd_spent_on_products,
             ytd_spent_on_services
           --  ,p_spabiz_customer_id
             ,dv_load_date_time,
             dv_load_end_date_time
             ,dv_batch_id
             ,dv_inserted_date_time,
             dv_insert_user
			 )
  select bk_hash,
         dim_spabiz_customer_key,
         customer_id,
         store_number,
         address_city,
         address_country,
         address_line_1,
         address_line_2,
         address_postal_code,
         address_state_or_province,
         allergies,
         balance,
         call_days
         ,communicate_via_email_flag,
         communicate_via_mail_flag,
         created_date_time,
         credit_limit,
         customer_type_dim_description_key,
         customer_type_id,
         date_of_birth,
         deleted_date_time,
         deleted_flag,
         dim_mms_member_key,
         dim_mms_membership_key,
         dim_spabiz_store_key,
         do_not_charge_tax_flag,
         do_not_print_note_flag,
         edit_date_time,
         email,
         employer,
         first_initial_last_name,
         first_name,
         first_visit_date_time,
         gender_abbreviation,
         home_phone_number,
         last_appointment_date_time,
         last_called_date_time,
         last_name
         ,last_ticket_processed_date_time
         --,marital_status_dim_description_key   --PROBLEM
	     ,marital_status_id
         ,medication
         ,member_id,
         membership_id
         ,middle_name,
         mobile_phone_number
         ,note,
         note_1,
         occupation,
         pager_number,
         preferred_contact_type_dim_description_key,
         preferred_contact_type_id,
         primary_dim_spabiz_staff_key
         ,quick_id,
         service_visits,
         show_note_flag,
         title,
         total_late_show,
         total_no_show,
         total_products_purchased,
         total_services_purchased,
         total_visits,
         urgent_meesage_flag,
         work_fax,
         work_phone_extension,
         work_phone_number,
         ytd_spent_on_products,
         ytd_spent_on_services
        -- ,p_spabiz_customer_id
		--,99999
         ,dv_load_date_time,
         dv_load_end_date_time
         ,@current_dv_batch_id
         ,getdate(),
         suser_sname()
    from #insert
commit tran


--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from sandbox_ebi.d_spabiz_customer)
--Done!
end
