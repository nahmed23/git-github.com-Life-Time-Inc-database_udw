CREATE PROC [dbo].[proc_etl_spabiz_vendor] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_VENDOR

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_VENDOR (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       PHONE,
       FAX,
       CONTACT1,
       CONTACT1TITLE,
       CONTACT1TEL,
       CONTACT1EXT,
       CONTACT2,
       CONTACT2TITLE,
       CONTACT2TEL,
       CONTACT2EXT,
       ORDERFREQ,
       POMETHOD,
       ADDRESS1,
       ADDRESS2,
       CITY,
       ST,
       ZIP,
       EMAIL,
       CUSTOMERNUM,
       DAY1,
       DAY2,
       DAY3,
       DAY4,
       DAY5,
       DAY6,
       DAY7,
       WEEKEND,
       MONTHEND,
       STORE_NUMBER,
       NEWID,
       VENDORBACKUPID,
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
       NAME,
       QUICKID,
       PHONE,
       FAX,
       CONTACT1,
       CONTACT1TITLE,
       CONTACT1TEL,
       CONTACT1EXT,
       CONTACT2,
       CONTACT2TITLE,
       CONTACT2TEL,
       CONTACT2EXT,
       ORDERFREQ,
       POMETHOD,
       ADDRESS1,
       ADDRESS2,
       CITY,
       ST,
       ZIP,
       EMAIL,
       CUSTOMERNUM,
       DAY1,
       DAY2,
       DAY3,
       DAY4,
       DAY5,
       DAY6,
       DAY7,
       WEEKEND,
       MONTHEND,
       STORE_NUMBER,
       NEWID,
       VENDORBACKUPID,
       isnull(cast(stage_spabiz_VENDOR.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_VENDOR
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_vendor @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_vendor (
       bk_hash,
       vendor_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_VENDOR.bk_hash,
       stage_hash_spabiz_VENDOR.ID vendor_id,
       stage_hash_spabiz_VENDOR.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_VENDOR.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_VENDOR
  left join h_spabiz_vendor
    on stage_hash_spabiz_VENDOR.bk_hash = h_spabiz_vendor.bk_hash
 where h_spabiz_vendor_id is null
   and stage_hash_spabiz_VENDOR.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_vendor
if object_id('tempdb..#l_spabiz_vendor_inserts') is not null drop table #l_spabiz_vendor_inserts
create table #l_spabiz_vendor_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_VENDOR.bk_hash,
       stage_hash_spabiz_VENDOR.ID vendor_id,
       stage_hash_spabiz_VENDOR.STOREID store_id,
       stage_hash_spabiz_VENDOR.STORE_NUMBER store_number,
       stage_hash_spabiz_VENDOR.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_VENDOR
 where stage_hash_spabiz_VENDOR.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_vendor records
set @insert_date_time = getdate()
insert into l_spabiz_vendor (
       bk_hash,
       vendor_id,
       store_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_vendor_inserts.bk_hash,
       #l_spabiz_vendor_inserts.vendor_id,
       #l_spabiz_vendor_inserts.store_id,
       #l_spabiz_vendor_inserts.store_number,
       case when l_spabiz_vendor.l_spabiz_vendor_id is null then isnull(#l_spabiz_vendor_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_vendor_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_vendor_inserts
  left join p_spabiz_vendor
    on #l_spabiz_vendor_inserts.bk_hash = p_spabiz_vendor.bk_hash
   and p_spabiz_vendor.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_vendor
    on p_spabiz_vendor.bk_hash = l_spabiz_vendor.bk_hash
   and p_spabiz_vendor.l_spabiz_vendor_id = l_spabiz_vendor.l_spabiz_vendor_id
 where l_spabiz_vendor.l_spabiz_vendor_id is null
    or (l_spabiz_vendor.l_spabiz_vendor_id is not null
        and l_spabiz_vendor.dv_hash <> #l_spabiz_vendor_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_vendor
if object_id('tempdb..#s_spabiz_vendor_inserts') is not null drop table #s_spabiz_vendor_inserts
create table #s_spabiz_vendor_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_VENDOR.bk_hash,
       stage_hash_spabiz_VENDOR.ID vendor_id,
       stage_hash_spabiz_VENDOR.COUNTERID counter_id,
       stage_hash_spabiz_VENDOR.EDITTIME edit_time,
       stage_hash_spabiz_VENDOR.[Delete] vendor_delete,
       stage_hash_spabiz_VENDOR.DELETEDATE delete_date,
       stage_hash_spabiz_VENDOR.NAME name,
       stage_hash_spabiz_VENDOR.QUICKID quick_id,
       stage_hash_spabiz_VENDOR.PHONE phone,
       stage_hash_spabiz_VENDOR.FAX fax,
       stage_hash_spabiz_VENDOR.CONTACT1 contact_1,
       stage_hash_spabiz_VENDOR.CONTACT1TITLE contact_1_title,
       stage_hash_spabiz_VENDOR.CONTACT1TEL contact_1_tel,
       stage_hash_spabiz_VENDOR.CONTACT1EXT contact_1_ext,
       stage_hash_spabiz_VENDOR.CONTACT2 contact_2,
       stage_hash_spabiz_VENDOR.CONTACT2TITLE contact_2_title,
       stage_hash_spabiz_VENDOR.CONTACT2TEL contact_2_tel,
       stage_hash_spabiz_VENDOR.CONTACT2EXT contact_2_ext,
       stage_hash_spabiz_VENDOR.ORDERFREQ order_freq,
       stage_hash_spabiz_VENDOR.POMETHOD po_method,
       stage_hash_spabiz_VENDOR.ADDRESS1 address_1,
       stage_hash_spabiz_VENDOR.ADDRESS2 address_2,
       stage_hash_spabiz_VENDOR.CITY city,
       stage_hash_spabiz_VENDOR.ST st,
       stage_hash_spabiz_VENDOR.ZIP zip,
       stage_hash_spabiz_VENDOR.EMAIL email,
       stage_hash_spabiz_VENDOR.CUSTOMERNUM customer_num,
       stage_hash_spabiz_VENDOR.DAY1 day_1,
       stage_hash_spabiz_VENDOR.DAY2 day_2,
       stage_hash_spabiz_VENDOR.DAY3 day_3,
       stage_hash_spabiz_VENDOR.DAY4 day_4,
       stage_hash_spabiz_VENDOR.DAY5 day_5,
       stage_hash_spabiz_VENDOR.DAY6 day_6,
       stage_hash_spabiz_VENDOR.DAY7 day_7,
       stage_hash_spabiz_VENDOR.WEEKEND week_end,
       stage_hash_spabiz_VENDOR.MONTHEND month_end,
       stage_hash_spabiz_VENDOR.STORE_NUMBER store_number,
       stage_hash_spabiz_VENDOR.NEWID new_id,
       stage_hash_spabiz_VENDOR.VENDORBACKUPID vendor_backup_id,
       stage_hash_spabiz_VENDOR.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_VENDOR.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_VENDOR.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.PHONE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.FAX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CONTACT1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CONTACT1TITLE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CONTACT1TEL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CONTACT1EXT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CONTACT2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CONTACT2TITLE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CONTACT2TEL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CONTACT2EXT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.ORDERFREQ as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.POMETHOD as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.ADDRESS1,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.ADDRESS2,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CITY,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.ST,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.ZIP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.EMAIL,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_VENDOR.CUSTOMERNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.DAY1 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.DAY2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.DAY3 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.DAY4 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.DAY5 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.DAY6 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.DAY7 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.WEEKEND as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.MONTHEND as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.NEWID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_VENDOR.VENDORBACKUPID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_VENDOR
 where stage_hash_spabiz_VENDOR.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_vendor records
set @insert_date_time = getdate()
insert into s_spabiz_vendor (
       bk_hash,
       vendor_id,
       counter_id,
       edit_time,
       vendor_delete,
       delete_date,
       name,
       quick_id,
       phone,
       fax,
       contact_1,
       contact_1_title,
       contact_1_tel,
       contact_1_ext,
       contact_2,
       contact_2_title,
       contact_2_tel,
       contact_2_ext,
       order_freq,
       po_method,
       address_1,
       address_2,
       city,
       st,
       zip,
       email,
       customer_num,
       day_1,
       day_2,
       day_3,
       day_4,
       day_5,
       day_6,
       day_7,
       week_end,
       month_end,
       store_number,
       new_id,
       vendor_backup_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_vendor_inserts.bk_hash,
       #s_spabiz_vendor_inserts.vendor_id,
       #s_spabiz_vendor_inserts.counter_id,
       #s_spabiz_vendor_inserts.edit_time,
       #s_spabiz_vendor_inserts.vendor_delete,
       #s_spabiz_vendor_inserts.delete_date,
       #s_spabiz_vendor_inserts.name,
       #s_spabiz_vendor_inserts.quick_id,
       #s_spabiz_vendor_inserts.phone,
       #s_spabiz_vendor_inserts.fax,
       #s_spabiz_vendor_inserts.contact_1,
       #s_spabiz_vendor_inserts.contact_1_title,
       #s_spabiz_vendor_inserts.contact_1_tel,
       #s_spabiz_vendor_inserts.contact_1_ext,
       #s_spabiz_vendor_inserts.contact_2,
       #s_spabiz_vendor_inserts.contact_2_title,
       #s_spabiz_vendor_inserts.contact_2_tel,
       #s_spabiz_vendor_inserts.contact_2_ext,
       #s_spabiz_vendor_inserts.order_freq,
       #s_spabiz_vendor_inserts.po_method,
       #s_spabiz_vendor_inserts.address_1,
       #s_spabiz_vendor_inserts.address_2,
       #s_spabiz_vendor_inserts.city,
       #s_spabiz_vendor_inserts.st,
       #s_spabiz_vendor_inserts.zip,
       #s_spabiz_vendor_inserts.email,
       #s_spabiz_vendor_inserts.customer_num,
       #s_spabiz_vendor_inserts.day_1,
       #s_spabiz_vendor_inserts.day_2,
       #s_spabiz_vendor_inserts.day_3,
       #s_spabiz_vendor_inserts.day_4,
       #s_spabiz_vendor_inserts.day_5,
       #s_spabiz_vendor_inserts.day_6,
       #s_spabiz_vendor_inserts.day_7,
       #s_spabiz_vendor_inserts.week_end,
       #s_spabiz_vendor_inserts.month_end,
       #s_spabiz_vendor_inserts.store_number,
       #s_spabiz_vendor_inserts.new_id,
       #s_spabiz_vendor_inserts.vendor_backup_id,
       case when s_spabiz_vendor.s_spabiz_vendor_id is null then isnull(#s_spabiz_vendor_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_vendor_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_vendor_inserts
  left join p_spabiz_vendor
    on #s_spabiz_vendor_inserts.bk_hash = p_spabiz_vendor.bk_hash
   and p_spabiz_vendor.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_vendor
    on p_spabiz_vendor.bk_hash = s_spabiz_vendor.bk_hash
   and p_spabiz_vendor.s_spabiz_vendor_id = s_spabiz_vendor.s_spabiz_vendor_id
 where s_spabiz_vendor.s_spabiz_vendor_id is null
    or (s_spabiz_vendor.s_spabiz_vendor_id is not null
        and s_spabiz_vendor.dv_hash <> #s_spabiz_vendor_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_vendor @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_vendor @current_dv_batch_id

end
