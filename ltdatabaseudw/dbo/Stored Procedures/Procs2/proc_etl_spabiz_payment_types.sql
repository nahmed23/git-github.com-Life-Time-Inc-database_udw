CREATE PROC [dbo].[proc_etl_spabiz_payment_types] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_PAYMENTTYPES

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_PAYMENTTYPES (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       PAYTYPE,
       ENABLED,
       DEPOSITABLE,
       SERVICECHARGE,
       PROGRAMMED,
       ICON,
       ORDERNUM,
       QUICKKEY,
       NONREVENUE,
       VERIFY,
       DATETIME,
       STORE_NUMBER,
       GLACCOUNT,
       POPDRAWER,
       MULTICOPY,
       SIGNATURELINE,
       NEWID,
       PAYMENTTYPESBACKUPID,
       DEFAULTROOMNUMBER,
       HOTELPOST,
       HOTELPAYCODE,
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
       PAYTYPE,
       ENABLED,
       DEPOSITABLE,
       SERVICECHARGE,
       PROGRAMMED,
       ICON,
       ORDERNUM,
       QUICKKEY,
       NONREVENUE,
       VERIFY,
       DATETIME,
       STORE_NUMBER,
       GLACCOUNT,
       POPDRAWER,
       MULTICOPY,
       SIGNATURELINE,
       NEWID,
       PAYMENTTYPESBACKUPID,
       DEFAULTROOMNUMBER,
       HOTELPOST,
       HOTELPAYCODE,
       isnull(cast(stage_spabiz_PAYMENTTYPES.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_PAYMENTTYPES
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_payment_types @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_payment_types (
       bk_hash,
       payment_types_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_PAYMENTTYPES.bk_hash,
       stage_hash_spabiz_PAYMENTTYPES.ID payment_types_id,
       stage_hash_spabiz_PAYMENTTYPES.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_PAYMENTTYPES.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_PAYMENTTYPES
  left join h_spabiz_payment_types
    on stage_hash_spabiz_PAYMENTTYPES.bk_hash = h_spabiz_payment_types.bk_hash
 where h_spabiz_payment_types_id is null
   and stage_hash_spabiz_PAYMENTTYPES.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_payment_types
if object_id('tempdb..#l_spabiz_payment_types_inserts') is not null drop table #l_spabiz_payment_types_inserts
create table #l_spabiz_payment_types_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PAYMENTTYPES.bk_hash,
       stage_hash_spabiz_PAYMENTTYPES.ID payment_types_id,
       stage_hash_spabiz_PAYMENTTYPES.COUNTERID counter_id,
       stage_hash_spabiz_PAYMENTTYPES.STOREID store_id,
       stage_hash_spabiz_PAYMENTTYPES.ORDERNUM order_num,
       stage_hash_spabiz_PAYMENTTYPES.STORE_NUMBER store_number,
       stage_hash_spabiz_PAYMENTTYPES.GLACCOUNT gl_account,
       stage_hash_spabiz_PAYMENTTYPES.NEWID new_id,
       stage_hash_spabiz_PAYMENTTYPES.PAYMENTTYPESBACKUPID payment_types_backup_id,
       isnull(cast(stage_hash_spabiz_PAYMENTTYPES.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.ORDERNUM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PAYMENTTYPES.GLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.NEWID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.PAYMENTTYPESBACKUPID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PAYMENTTYPES
 where stage_hash_spabiz_PAYMENTTYPES.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_payment_types records
set @insert_date_time = getdate()
insert into l_spabiz_payment_types (
       bk_hash,
       payment_types_id,
       counter_id,
       store_id,
       order_num,
       store_number,
       gl_account,
       new_id,
       payment_types_backup_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_payment_types_inserts.bk_hash,
       #l_spabiz_payment_types_inserts.payment_types_id,
       #l_spabiz_payment_types_inserts.counter_id,
       #l_spabiz_payment_types_inserts.store_id,
       #l_spabiz_payment_types_inserts.order_num,
       #l_spabiz_payment_types_inserts.store_number,
       #l_spabiz_payment_types_inserts.gl_account,
       #l_spabiz_payment_types_inserts.new_id,
       #l_spabiz_payment_types_inserts.payment_types_backup_id,
       case when l_spabiz_payment_types.l_spabiz_payment_types_id is null then isnull(#l_spabiz_payment_types_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_payment_types_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_payment_types_inserts
  left join p_spabiz_payment_types
    on #l_spabiz_payment_types_inserts.bk_hash = p_spabiz_payment_types.bk_hash
   and p_spabiz_payment_types.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_payment_types
    on p_spabiz_payment_types.bk_hash = l_spabiz_payment_types.bk_hash
   and p_spabiz_payment_types.l_spabiz_payment_types_id = l_spabiz_payment_types.l_spabiz_payment_types_id
 where l_spabiz_payment_types.l_spabiz_payment_types_id is null
    or (l_spabiz_payment_types.l_spabiz_payment_types_id is not null
        and l_spabiz_payment_types.dv_hash <> #l_spabiz_payment_types_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_payment_types
if object_id('tempdb..#s_spabiz_payment_types_inserts') is not null drop table #s_spabiz_payment_types_inserts
create table #s_spabiz_payment_types_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PAYMENTTYPES.bk_hash,
       stage_hash_spabiz_PAYMENTTYPES.ID payment_types_id,
       stage_hash_spabiz_PAYMENTTYPES.EDITTIME edit_time,
       stage_hash_spabiz_PAYMENTTYPES.[Delete] payment_types_delete,
       stage_hash_spabiz_PAYMENTTYPES.DELETEDATE delete_date,
       stage_hash_spabiz_PAYMENTTYPES.NAME name,
       stage_hash_spabiz_PAYMENTTYPES.QUICKID quick_id,
       stage_hash_spabiz_PAYMENTTYPES.PAYTYPE pay_type,
       stage_hash_spabiz_PAYMENTTYPES.ENABLED enabled,
       stage_hash_spabiz_PAYMENTTYPES.DEPOSITABLE depositable,
       stage_hash_spabiz_PAYMENTTYPES.SERVICECHARGE service_charge,
       stage_hash_spabiz_PAYMENTTYPES.PROGRAMMED programmed,
       stage_hash_spabiz_PAYMENTTYPES.ICON icon,
       stage_hash_spabiz_PAYMENTTYPES.QUICKKEY quick_key,
       stage_hash_spabiz_PAYMENTTYPES.NONREVENUE non_revenue,
       stage_hash_spabiz_PAYMENTTYPES.VERIFY verify,
       stage_hash_spabiz_PAYMENTTYPES.DATETIME date_time,
       stage_hash_spabiz_PAYMENTTYPES.STORE_NUMBER store_number,
       stage_hash_spabiz_PAYMENTTYPES.POPDRAWER pop_drawer,
       stage_hash_spabiz_PAYMENTTYPES.MULTICOPY multi_copy,
       stage_hash_spabiz_PAYMENTTYPES.SIGNATURELINE signature_line,
       stage_hash_spabiz_PAYMENTTYPES.DEFAULTROOMNUMBER default_room_number,
       stage_hash_spabiz_PAYMENTTYPES.HOTELPOST hotel_post,
       stage_hash_spabiz_PAYMENTTYPES.HOTELPAYCODE hotel_pay_code,
       isnull(cast(stage_hash_spabiz_PAYMENTTYPES.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PAYMENTTYPES.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PAYMENTTYPES.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PAYMENTTYPES.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PAYMENTTYPES.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.PAYTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.ENABLED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.DEPOSITABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.SERVICECHARGE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.PROGRAMMED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PAYMENTTYPES.ICON,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.QUICKKEY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.NONREVENUE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.VERIFY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PAYMENTTYPES.DATETIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.POPDRAWER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.MULTICOPY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.SIGNATURELINE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PAYMENTTYPES.DEFAULTROOMNUMBER,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PAYMENTTYPES.HOTELPOST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PAYMENTTYPES.HOTELPAYCODE,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PAYMENTTYPES
 where stage_hash_spabiz_PAYMENTTYPES.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_payment_types records
set @insert_date_time = getdate()
insert into s_spabiz_payment_types (
       bk_hash,
       payment_types_id,
       edit_time,
       payment_types_delete,
       delete_date,
       name,
       quick_id,
       pay_type,
       enabled,
       depositable,
       service_charge,
       programmed,
       icon,
       quick_key,
       non_revenue,
       verify,
       date_time,
       store_number,
       pop_drawer,
       multi_copy,
       signature_line,
       default_room_number,
       hotel_post,
       hotel_pay_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_payment_types_inserts.bk_hash,
       #s_spabiz_payment_types_inserts.payment_types_id,
       #s_spabiz_payment_types_inserts.edit_time,
       #s_spabiz_payment_types_inserts.payment_types_delete,
       #s_spabiz_payment_types_inserts.delete_date,
       #s_spabiz_payment_types_inserts.name,
       #s_spabiz_payment_types_inserts.quick_id,
       #s_spabiz_payment_types_inserts.pay_type,
       #s_spabiz_payment_types_inserts.enabled,
       #s_spabiz_payment_types_inserts.depositable,
       #s_spabiz_payment_types_inserts.service_charge,
       #s_spabiz_payment_types_inserts.programmed,
       #s_spabiz_payment_types_inserts.icon,
       #s_spabiz_payment_types_inserts.quick_key,
       #s_spabiz_payment_types_inserts.non_revenue,
       #s_spabiz_payment_types_inserts.verify,
       #s_spabiz_payment_types_inserts.date_time,
       #s_spabiz_payment_types_inserts.store_number,
       #s_spabiz_payment_types_inserts.pop_drawer,
       #s_spabiz_payment_types_inserts.multi_copy,
       #s_spabiz_payment_types_inserts.signature_line,
       #s_spabiz_payment_types_inserts.default_room_number,
       #s_spabiz_payment_types_inserts.hotel_post,
       #s_spabiz_payment_types_inserts.hotel_pay_code,
       case when s_spabiz_payment_types.s_spabiz_payment_types_id is null then isnull(#s_spabiz_payment_types_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_payment_types_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_payment_types_inserts
  left join p_spabiz_payment_types
    on #s_spabiz_payment_types_inserts.bk_hash = p_spabiz_payment_types.bk_hash
   and p_spabiz_payment_types.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_payment_types
    on p_spabiz_payment_types.bk_hash = s_spabiz_payment_types.bk_hash
   and p_spabiz_payment_types.s_spabiz_payment_types_id = s_spabiz_payment_types.s_spabiz_payment_types_id
 where s_spabiz_payment_types.s_spabiz_payment_types_id is null
    or (s_spabiz_payment_types.s_spabiz_payment_types_id is not null
        and s_spabiz_payment_types.dv_hash <> #s_spabiz_payment_types_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_payment_types @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_payment_types @current_dv_batch_id

end
