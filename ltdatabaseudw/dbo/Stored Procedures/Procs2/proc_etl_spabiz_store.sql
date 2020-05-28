CREATE PROC [dbo].[proc_etl_spabiz_store] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_STORE

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_STORE (
       bk_hash,
       [ID],
       EDITTIME,
       QUICKID,
       LOCKED,
       [Delete],
       DELETEDATE,
       NAME,
       ADDRESS1,
       ADDRESS2,
       CITY,
       [STATE],
       ZIP,
       COUNTRY,
       [BACKUP],
       TELEPHONE,
       RETAILTAXID,
       STARTPERIOD,
       store_1Name,
       store_1Start,
       store_1End,
       store_2Name,
       store_2Start,
       store_2End,
       store_3Name,
       store_3Start,
       store_3End,
       store_4Name,
       store_4Start,
       store_4End,
       store_5Name,
       store_5Start,
       store_5End,
       store_6Name,
       store_6Start,
       store_6End,
       store_7Name,
       store_7Start,
       store_7End,
       store_8Name,
       store_8Start,
       store_8End,
       store_9Name,
       store_9Start,
       store_9End,
       store_10Name,
       store_10Start,
       store_10End,
       store_11Name,
       store_11Start,
       store_11End,
       store_12Name,
       store_12Start,
       store_12End,
       store_13Name,
       store_13Start,
       store_13End,
       OPEN1,
       OPEN2,
       OPEN3,
       OPEN4,
       OPEN5,
       OPEN6,
       OPEN7,
       AVAILABLE,
       DDAY,
       RECCNT,
       DBVERSION,
       HQPATH,
       CASHCHANGE,
       STORE_NUMBER,
       PAYMENTTECH,
       PAGERPORT,
       STOREINFO,
       MERCHANTNUMBER,
       CLIENTNUMBER,
       POWERBOOKING,
       SBLOGONPATH,
       SCAT_STORENUM,
       WEBBOOK,
       COUNTERID,
       STOREID,
       SQRFOOTAGE,
       WEBVIEW,
       ISMASTER,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       [ID],
       EDITTIME,
       QUICKID,
       LOCKED,
       [Delete],
       DELETEDATE,
       NAME,
       ADDRESS1,
       ADDRESS2,
       CITY,
       [STATE],
       ZIP,
       COUNTRY,
       [BACKUP],
       TELEPHONE,
       RETAILTAXID,
       STARTPERIOD,
       store_1Name,
       store_1Start,
       store_1End,
       store_2Name,
       store_2Start,
       store_2End,
       store_3Name,
       store_3Start,
       store_3End,
       store_4Name,
       store_4Start,
       store_4End,
       store_5Name,
       store_5Start,
       store_5End,
       store_6Name,
       store_6Start,
       store_6End,
       store_7Name,
       store_7Start,
       store_7End,
       store_8Name,
       store_8Start,
       store_8End,
       store_9Name,
       store_9Start,
       store_9End,
       store_10Name,
       store_10Start,
       store_10End,
       store_11Name,
       store_11Start,
       store_11End,
       store_12Name,
       store_12Start,
       store_12End,
       store_13Name,
       store_13Start,
       store_13End,
       OPEN1,
       OPEN2,
       OPEN3,
       OPEN4,
       OPEN5,
       OPEN6,
       OPEN7,
       AVAILABLE,
       DDAY,
       RECCNT,
       DBVERSION,
       HQPATH,
       CASHCHANGE,
       STORE_NUMBER,
       PAYMENTTECH,
       PAGERPORT,
       STOREINFO,
       MERCHANTNUMBER,
       CLIENTNUMBER,
       POWERBOOKING,
       SBLOGONPATH,
       SCAT_STORENUM,
       WEBBOOK,
       COUNTERID,
       STOREID,
       SQRFOOTAGE,
       WEBVIEW,
       ISMASTER,
       isnull(cast(stage_spabiz_STORE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_STORE
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_store @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_store (
       bk_hash,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_STORE.bk_hash,
       stage_hash_spabiz_STORE.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_STORE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_STORE
  left join h_spabiz_store
    on stage_hash_spabiz_STORE.bk_hash = h_spabiz_store.bk_hash
 where h_spabiz_store_id is null
   and stage_hash_spabiz_STORE.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_spabiz_store
if object_id('tempdb..#s_spabiz_store_inserts') is not null drop table #s_spabiz_store_inserts
create table #s_spabiz_store_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_STORE.bk_hash,
       stage_hash_spabiz_STORE.ID store_id,
       stage_hash_spabiz_STORE.EDITTIME edit_time,
       stage_hash_spabiz_STORE.QUICKID quick_id,
       stage_hash_spabiz_STORE.LOCKED locked,
       stage_hash_spabiz_STORE.[Delete] store_delete,
       stage_hash_spabiz_STORE.DELETEDATE delete_date,
       stage_hash_spabiz_STORE.NAME name,
       stage_hash_spabiz_STORE.ADDRESS1 address_1,
       stage_hash_spabiz_STORE.ADDRESS2 address_2,
       stage_hash_spabiz_STORE.CITY city,
       stage_hash_spabiz_STORE.[STATE] store_state,
       stage_hash_spabiz_STORE.ZIP zip,
       stage_hash_spabiz_STORE.COUNTRY country,
       stage_hash_spabiz_STORE.[BACKUP] store_backup,
       stage_hash_spabiz_STORE.TELEPHONE telephone,
       stage_hash_spabiz_STORE.RETAILTAXID retail_tax_id,
       stage_hash_spabiz_STORE.STARTPERIOD start_period,
       stage_hash_spabiz_STORE.store_1Name store_1_name,
       stage_hash_spabiz_STORE.store_1Start store_1_start,
       stage_hash_spabiz_STORE.store_1End store_1_end,
       stage_hash_spabiz_STORE.store_2Name store_2_name,
       stage_hash_spabiz_STORE.store_2Start store_2_start,
       stage_hash_spabiz_STORE.store_2End store_2_end,
       stage_hash_spabiz_STORE.store_3Name store_3_name,
       stage_hash_spabiz_STORE.store_3Start store_3_start,
       stage_hash_spabiz_STORE.store_3End store_3_end,
       stage_hash_spabiz_STORE.store_4Name store_4_name,
       stage_hash_spabiz_STORE.store_4Start store_4_start,
       stage_hash_spabiz_STORE.store_4End store_4_end,
       stage_hash_spabiz_STORE.store_5Name store_5_name,
       stage_hash_spabiz_STORE.store_5Start store_5_start,
       stage_hash_spabiz_STORE.store_5End store_5_end,
       stage_hash_spabiz_STORE.store_6Name store_6_name,
       stage_hash_spabiz_STORE.store_6Start store_6_start,
       stage_hash_spabiz_STORE.store_6End store_6_end,
       stage_hash_spabiz_STORE.store_7Name store_7_name,
       stage_hash_spabiz_STORE.store_7Start store_7_start,
       stage_hash_spabiz_STORE.store_7End store_7_end,
       stage_hash_spabiz_STORE.store_8Name store_8_name,
       stage_hash_spabiz_STORE.store_8Start store_8_start,
       stage_hash_spabiz_STORE.store_8End store_8_end,
       stage_hash_spabiz_STORE.store_9Name store_9_name,
       stage_hash_spabiz_STORE.store_9Start store_9_start,
       stage_hash_spabiz_STORE.store_9End store_9_end,
       stage_hash_spabiz_STORE.store_10Name store_10_name,
       stage_hash_spabiz_STORE.store_10Start store_10_start,
       stage_hash_spabiz_STORE.store_10End store_10_end,
       stage_hash_spabiz_STORE.store_11Name store_11_name,
       stage_hash_spabiz_STORE.store_11Start store_11_start,
       stage_hash_spabiz_STORE.store_11End store_11_end,
       stage_hash_spabiz_STORE.store_12Name store_12_name,
       stage_hash_spabiz_STORE.store_12Start store_12_start,
       stage_hash_spabiz_STORE.store_12End store_12_end,
       stage_hash_spabiz_STORE.store_13Name store_13_name,
       stage_hash_spabiz_STORE.store_13Start store_13_start,
       stage_hash_spabiz_STORE.store_13End store_13_end,
       stage_hash_spabiz_STORE.OPEN1 open_1,
       stage_hash_spabiz_STORE.OPEN2 open_2,
       stage_hash_spabiz_STORE.OPEN3 open_3,
       stage_hash_spabiz_STORE.OPEN4 open_4,
       stage_hash_spabiz_STORE.OPEN5 open_5,
       stage_hash_spabiz_STORE.OPEN6 open_6,
       stage_hash_spabiz_STORE.OPEN7 open_7,
       stage_hash_spabiz_STORE.AVAILABLE available,
       stage_hash_spabiz_STORE.DDAY d_day,
       stage_hash_spabiz_STORE.RECCNT rec_cnt,
       stage_hash_spabiz_STORE.DBVERSION db_version,
       stage_hash_spabiz_STORE.HQPATH hq_path,
       stage_hash_spabiz_STORE.CASHCHANGE cash_change,
       stage_hash_spabiz_STORE.STORE_NUMBER store_number,
       stage_hash_spabiz_STORE.PAYMENTTECH payment_tech,
       stage_hash_spabiz_STORE.PAGERPORT pager_port,
       stage_hash_spabiz_STORE.STOREINFO store_info,
       stage_hash_spabiz_STORE.MERCHANTNUMBER merchant_number,
       stage_hash_spabiz_STORE.CLIENTNUMBER client_number,
       stage_hash_spabiz_STORE.POWERBOOKING power_booking,
       stage_hash_spabiz_STORE.SBLOGONPATH s_blog_on_path,
       stage_hash_spabiz_STORE.SCAT_STORENUM s_cat_storenum,
       stage_hash_spabiz_STORE.WEBBOOK web_book,
       stage_hash_spabiz_STORE.COUNTERID counter_id,
       stage_hash_spabiz_STORE.STOREID store_store_id,
       stage_hash_spabiz_STORE.SQRFOOTAGE sqr_footage,
       stage_hash_spabiz_STORE.WEBVIEW web_view,
       stage_hash_spabiz_STORE.ISMASTER is_master,
       stage_hash_spabiz_STORE.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STORE.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.LOCKED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STORE.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.ADDRESS1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.ADDRESS2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.CITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.[STATE],'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.COUNTRY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.[BACKUP],'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.TELEPHONE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.RETAILTAXID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.STARTPERIOD as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_1Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_1Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_1End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_2Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_2Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_2End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_3Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_3Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_3End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_4Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_4Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_4End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_5Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_5Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_5End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_6Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_6Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_6End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_7Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_7Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_7End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_8Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_8Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_8End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_9Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_9Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_9End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_10Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_10Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_10End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_11Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_11Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_11End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_12Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_12Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_12End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_13Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_13Start,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.store_13End,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.OPEN1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.OPEN2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.OPEN3 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.OPEN4 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.OPEN5 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.OPEN6 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.OPEN7 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.AVAILABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_STORE.DDAY,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.RECCNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.DBVERSION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.HQPATH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.CASHCHANGE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.PAYMENTTECH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.PAGERPORT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.STOREINFO,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.MERCHANTNUMBER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.CLIENTNUMBER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.POWERBOOKING as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_STORE.SBLOGONPATH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.SCAT_STORENUM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.WEBBOOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.SQRFOOTAGE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.WEBVIEW as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_STORE.ISMASTER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_STORE
 where stage_hash_spabiz_STORE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_store records
set @insert_date_time = getdate()
insert into s_spabiz_store (
       bk_hash,
       store_id,
       edit_time,
       quick_id,
       locked,
       store_delete,
       delete_date,
       name,
       address_1,
       address_2,
       city,
       store_state,
       zip,
       country,
       store_backup,
       telephone,
       retail_tax_id,
       start_period,
       store_1_name,
       store_1_start,
       store_1_end,
       store_2_name,
       store_2_start,
       store_2_end,
       store_3_name,
       store_3_start,
       store_3_end,
       store_4_name,
       store_4_start,
       store_4_end,
       store_5_name,
       store_5_start,
       store_5_end,
       store_6_name,
       store_6_start,
       store_6_end,
       store_7_name,
       store_7_start,
       store_7_end,
       store_8_name,
       store_8_start,
       store_8_end,
       store_9_name,
       store_9_start,
       store_9_end,
       store_10_name,
       store_10_start,
       store_10_end,
       store_11_name,
       store_11_start,
       store_11_end,
       store_12_name,
       store_12_start,
       store_12_end,
       store_13_name,
       store_13_start,
       store_13_end,
       open_1,
       open_2,
       open_3,
       open_4,
       open_5,
       open_6,
       open_7,
       available,
       d_day,
       rec_cnt,
       db_version,
       hq_path,
       cash_change,
       store_number,
       payment_tech,
       pager_port,
       store_info,
       merchant_number,
       client_number,
       power_booking,
       s_blog_on_path,
       s_cat_storenum,
       web_book,
       counter_id,
       store_store_id,
       sqr_footage,
       web_view,
       is_master,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_store_inserts.bk_hash,
       #s_spabiz_store_inserts.store_id,
       #s_spabiz_store_inserts.edit_time,
       #s_spabiz_store_inserts.quick_id,
       #s_spabiz_store_inserts.locked,
       #s_spabiz_store_inserts.store_delete,
       #s_spabiz_store_inserts.delete_date,
       #s_spabiz_store_inserts.name,
       #s_spabiz_store_inserts.address_1,
       #s_spabiz_store_inserts.address_2,
       #s_spabiz_store_inserts.city,
       #s_spabiz_store_inserts.store_state,
       #s_spabiz_store_inserts.zip,
       #s_spabiz_store_inserts.country,
       #s_spabiz_store_inserts.store_backup,
       #s_spabiz_store_inserts.telephone,
       #s_spabiz_store_inserts.retail_tax_id,
       #s_spabiz_store_inserts.start_period,
       #s_spabiz_store_inserts.store_1_name,
       #s_spabiz_store_inserts.store_1_start,
       #s_spabiz_store_inserts.store_1_end,
       #s_spabiz_store_inserts.store_2_name,
       #s_spabiz_store_inserts.store_2_start,
       #s_spabiz_store_inserts.store_2_end,
       #s_spabiz_store_inserts.store_3_name,
       #s_spabiz_store_inserts.store_3_start,
       #s_spabiz_store_inserts.store_3_end,
       #s_spabiz_store_inserts.store_4_name,
       #s_spabiz_store_inserts.store_4_start,
       #s_spabiz_store_inserts.store_4_end,
       #s_spabiz_store_inserts.store_5_name,
       #s_spabiz_store_inserts.store_5_start,
       #s_spabiz_store_inserts.store_5_end,
       #s_spabiz_store_inserts.store_6_name,
       #s_spabiz_store_inserts.store_6_start,
       #s_spabiz_store_inserts.store_6_end,
       #s_spabiz_store_inserts.store_7_name,
       #s_spabiz_store_inserts.store_7_start,
       #s_spabiz_store_inserts.store_7_end,
       #s_spabiz_store_inserts.store_8_name,
       #s_spabiz_store_inserts.store_8_start,
       #s_spabiz_store_inserts.store_8_end,
       #s_spabiz_store_inserts.store_9_name,
       #s_spabiz_store_inserts.store_9_start,
       #s_spabiz_store_inserts.store_9_end,
       #s_spabiz_store_inserts.store_10_name,
       #s_spabiz_store_inserts.store_10_start,
       #s_spabiz_store_inserts.store_10_end,
       #s_spabiz_store_inserts.store_11_name,
       #s_spabiz_store_inserts.store_11_start,
       #s_spabiz_store_inserts.store_11_end,
       #s_spabiz_store_inserts.store_12_name,
       #s_spabiz_store_inserts.store_12_start,
       #s_spabiz_store_inserts.store_12_end,
       #s_spabiz_store_inserts.store_13_name,
       #s_spabiz_store_inserts.store_13_start,
       #s_spabiz_store_inserts.store_13_end,
       #s_spabiz_store_inserts.open_1,
       #s_spabiz_store_inserts.open_2,
       #s_spabiz_store_inserts.open_3,
       #s_spabiz_store_inserts.open_4,
       #s_spabiz_store_inserts.open_5,
       #s_spabiz_store_inserts.open_6,
       #s_spabiz_store_inserts.open_7,
       #s_spabiz_store_inserts.available,
       #s_spabiz_store_inserts.d_day,
       #s_spabiz_store_inserts.rec_cnt,
       #s_spabiz_store_inserts.db_version,
       #s_spabiz_store_inserts.hq_path,
       #s_spabiz_store_inserts.cash_change,
       #s_spabiz_store_inserts.store_number,
       #s_spabiz_store_inserts.payment_tech,
       #s_spabiz_store_inserts.pager_port,
       #s_spabiz_store_inserts.store_info,
       #s_spabiz_store_inserts.merchant_number,
       #s_spabiz_store_inserts.client_number,
       #s_spabiz_store_inserts.power_booking,
       #s_spabiz_store_inserts.s_blog_on_path,
       #s_spabiz_store_inserts.s_cat_storenum,
       #s_spabiz_store_inserts.web_book,
       #s_spabiz_store_inserts.counter_id,
       #s_spabiz_store_inserts.store_store_id,
       #s_spabiz_store_inserts.sqr_footage,
       #s_spabiz_store_inserts.web_view,
       #s_spabiz_store_inserts.is_master,
       case when s_spabiz_store.s_spabiz_store_id is null then isnull(#s_spabiz_store_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_store_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_store_inserts
  left join p_spabiz_store
    on #s_spabiz_store_inserts.bk_hash = p_spabiz_store.bk_hash
   and p_spabiz_store.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_store
    on p_spabiz_store.bk_hash = s_spabiz_store.bk_hash
   and p_spabiz_store.s_spabiz_store_id = s_spabiz_store.s_spabiz_store_id
 where s_spabiz_store.s_spabiz_store_id is null
    or (s_spabiz_store.s_spabiz_store_id is not null
        and s_spabiz_store.dv_hash <> #s_spabiz_store_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_store @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_store @current_dv_batch_id

end
