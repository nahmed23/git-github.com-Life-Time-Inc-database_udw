CREATE PROC [dbo].[proc_etl_spabiz_service] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_SERVICE

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_SERVICE (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       BOOKNAME,
       RETAILPRICE,
       DATECREATED,
       ACTIVE,
       COST,
       TIME,
       PROCESS,
       FINISH,
       DEPTCAT,
       SEARCHCAT,
       COSTTYPE,
       CALLAFTERXDAYS,
       PAYCOMISH,
       DESCRIPTION,
       POPUP,
       TAXABLE,
       RESOURCECOUNT,
       NEWEXTRATIME,
       REQUIRESTAFF,
       Date,
       STORE_NUMBER,
       GLACCOUNT,
       PAYTIP,
       TIP,
       WEBBOOK,
       NEWID,
       SERVICEBACKUPID,
       WEBVIEW,
       HEADMAPSERVICE,
       HAIRLENGTH,
       ISHILITEPROCEDURE,
       ISCOLORBALANCE,
       SERVICELEVEL,
       SERVICE_CLASS,
       ALLOWPOWERBOOKING,
       REQCC,
       EXCLUDEAPPTGAUR,
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
       BOOKNAME,
       RETAILPRICE,
       DATECREATED,
       ACTIVE,
       COST,
       TIME,
       PROCESS,
       FINISH,
       DEPTCAT,
       SEARCHCAT,
       COSTTYPE,
       CALLAFTERXDAYS,
       PAYCOMISH,
       DESCRIPTION,
       POPUP,
       TAXABLE,
       RESOURCECOUNT,
       NEWEXTRATIME,
       REQUIRESTAFF,
       Date,
       STORE_NUMBER,
       GLACCOUNT,
       PAYTIP,
       TIP,
       WEBBOOK,
       NEWID,
       SERVICEBACKUPID,
       WEBVIEW,
       HEADMAPSERVICE,
       HAIRLENGTH,
       ISHILITEPROCEDURE,
       ISCOLORBALANCE,
       SERVICELEVEL,
       SERVICE_CLASS,
       ALLOWPOWERBOOKING,
       REQCC,
       EXCLUDEAPPTGAUR,
       isnull(cast(stage_spabiz_SERVICE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_SERVICE
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_service @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_service (
       bk_hash,
       service_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_SERVICE.bk_hash,
       stage_hash_spabiz_SERVICE.ID service_id,
       stage_hash_spabiz_SERVICE.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_SERVICE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_SERVICE
  left join h_spabiz_service
    on stage_hash_spabiz_SERVICE.bk_hash = h_spabiz_service.bk_hash
 where h_spabiz_service_id is null
   and stage_hash_spabiz_SERVICE.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_service
if object_id('tempdb..#l_spabiz_service_inserts') is not null drop table #l_spabiz_service_inserts
create table #l_spabiz_service_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERVICE.bk_hash,
       stage_hash_spabiz_SERVICE.ID service_id,
       stage_hash_spabiz_SERVICE.STOREID store_id,
       stage_hash_spabiz_SERVICE.DEPTCAT dept_cat,
       stage_hash_spabiz_SERVICE.STORE_NUMBER store_number,
       stage_hash_spabiz_SERVICE.GLACCOUNT gl_account,
       stage_hash_spabiz_SERVICE.HEADMAPSERVICE headmap_service,
       isnull(cast(stage_hash_spabiz_SERVICE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.DEPTCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.GLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.HEADMAPSERVICE as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERVICE
 where stage_hash_spabiz_SERVICE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_service records
set @insert_date_time = getdate()
insert into l_spabiz_service (
       bk_hash,
       service_id,
       store_id,
       dept_cat,
       store_number,
       gl_account,
       headmap_service,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_service_inserts.bk_hash,
       #l_spabiz_service_inserts.service_id,
       #l_spabiz_service_inserts.store_id,
       #l_spabiz_service_inserts.dept_cat,
       #l_spabiz_service_inserts.store_number,
       #l_spabiz_service_inserts.gl_account,
       #l_spabiz_service_inserts.headmap_service,
       case when l_spabiz_service.l_spabiz_service_id is null then isnull(#l_spabiz_service_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_service_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_service_inserts
  left join p_spabiz_service
    on #l_spabiz_service_inserts.bk_hash = p_spabiz_service.bk_hash
   and p_spabiz_service.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_service
    on p_spabiz_service.bk_hash = l_spabiz_service.bk_hash
   and p_spabiz_service.l_spabiz_service_id = l_spabiz_service.l_spabiz_service_id
 where l_spabiz_service.l_spabiz_service_id is null
    or (l_spabiz_service.l_spabiz_service_id is not null
        and l_spabiz_service.dv_hash <> #l_spabiz_service_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_service
if object_id('tempdb..#s_spabiz_service_inserts') is not null drop table #s_spabiz_service_inserts
create table #s_spabiz_service_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERVICE.bk_hash,
       stage_hash_spabiz_SERVICE.ID service_id,
       stage_hash_spabiz_SERVICE.COUNTERID counter_id,
       stage_hash_spabiz_SERVICE.EDITTIME edit_time,
       stage_hash_spabiz_SERVICE.[Delete] service_delete,
       stage_hash_spabiz_SERVICE.DELETEDATE delete_date,
       stage_hash_spabiz_SERVICE.NAME name,
       stage_hash_spabiz_SERVICE.QUICKID quick_id,
       stage_hash_spabiz_SERVICE.BOOKNAME book_name,
       stage_hash_spabiz_SERVICE.RETAILPRICE retail_price,
       stage_hash_spabiz_SERVICE.DATECREATED date_created,
       stage_hash_spabiz_SERVICE.ACTIVE active,
       stage_hash_spabiz_SERVICE.COST cost,
       stage_hash_spabiz_SERVICE.TIME time,
       stage_hash_spabiz_SERVICE.PROCESS process,
       stage_hash_spabiz_SERVICE.FINISH finish,
       stage_hash_spabiz_SERVICE.SEARCHCAT search_cat,
       stage_hash_spabiz_SERVICE.COSTTYPE cost_type,
       stage_hash_spabiz_SERVICE.CALLAFTERXDAYS call_after_x_days,
       stage_hash_spabiz_SERVICE.PAYCOMISH pay_comish,
       stage_hash_spabiz_SERVICE.DESCRIPTION description,
       stage_hash_spabiz_SERVICE.POPUP popup,
       stage_hash_spabiz_SERVICE.TAXABLE taxable,
       stage_hash_spabiz_SERVICE.RESOURCECOUNT resource_count,
       stage_hash_spabiz_SERVICE.NEWEXTRATIME new_extra_time,
       stage_hash_spabiz_SERVICE.REQUIRESTAFF require_staff,
       stage_hash_spabiz_SERVICE.Date date,
       stage_hash_spabiz_SERVICE.STORE_NUMBER store_number,
       stage_hash_spabiz_SERVICE.PAYTIP pay_tip,
       stage_hash_spabiz_SERVICE.TIP tip,
       stage_hash_spabiz_SERVICE.WEBBOOK web_book,
       stage_hash_spabiz_SERVICE.NEWID new_id,
       stage_hash_spabiz_SERVICE.SERVICEBACKUPID service_backup_id,
       stage_hash_spabiz_SERVICE.WEBVIEW web_view,
       stage_hash_spabiz_SERVICE.HAIRLENGTH hair_length,
       stage_hash_spabiz_SERVICE.ISHILITEPROCEDURE is_hilite_procedure,
       stage_hash_spabiz_SERVICE.ISCOLORBALANCE is_color_balance,
       stage_hash_spabiz_SERVICE.SERVICELEVEL service_level,
       stage_hash_spabiz_SERVICE.SERVICE_CLASS service_class,
       stage_hash_spabiz_SERVICE.ALLOWPOWERBOOKING allow_power_booking,
       stage_hash_spabiz_SERVICE.REQCC req_cc,
       stage_hash_spabiz_SERVICE.EXCLUDEAPPTGAUR exclude_appt_gaur,
       isnull(cast(stage_hash_spabiz_SERVICE.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERVICE.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERVICE.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.BOOKNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERVICE.DATECREATED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.ACTIVE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.COST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.TIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.PROCESS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.FINISH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.SEARCHCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.COSTTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.CALLAFTERXDAYS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.PAYCOMISH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.DESCRIPTION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.POPUP,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.TAXABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.RESOURCECOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.NEWEXTRATIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.REQUIRESTAFF as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERVICE.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.PAYTIP as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.TIP as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.WEBBOOK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.NEWID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.SERVICEBACKUPID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.WEBVIEW as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.HAIRLENGTH,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.ISHILITEPROCEDURE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.ISCOLORBALANCE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.SERVICELEVEL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERVICE.SERVICE_CLASS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.ALLOWPOWERBOOKING as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.REQCC as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERVICE.EXCLUDEAPPTGAUR as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERVICE
 where stage_hash_spabiz_SERVICE.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_service records
set @insert_date_time = getdate()
insert into s_spabiz_service (
       bk_hash,
       service_id,
       counter_id,
       edit_time,
       service_delete,
       delete_date,
       name,
       quick_id,
       book_name,
       retail_price,
       date_created,
       active,
       cost,
       time,
       process,
       finish,
       search_cat,
       cost_type,
       call_after_x_days,
       pay_comish,
       description,
       popup,
       taxable,
       resource_count,
       new_extra_time,
       require_staff,
       date,
       store_number,
       pay_tip,
       tip,
       web_book,
       new_id,
       service_backup_id,
       web_view,
       hair_length,
       is_hilite_procedure,
       is_color_balance,
       service_level,
       service_class,
       allow_power_booking,
       req_cc,
       exclude_appt_gaur,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_service_inserts.bk_hash,
       #s_spabiz_service_inserts.service_id,
       #s_spabiz_service_inserts.counter_id,
       #s_spabiz_service_inserts.edit_time,
       #s_spabiz_service_inserts.service_delete,
       #s_spabiz_service_inserts.delete_date,
       #s_spabiz_service_inserts.name,
       #s_spabiz_service_inserts.quick_id,
       #s_spabiz_service_inserts.book_name,
       #s_spabiz_service_inserts.retail_price,
       #s_spabiz_service_inserts.date_created,
       #s_spabiz_service_inserts.active,
       #s_spabiz_service_inserts.cost,
       #s_spabiz_service_inserts.time,
       #s_spabiz_service_inserts.process,
       #s_spabiz_service_inserts.finish,
       #s_spabiz_service_inserts.search_cat,
       #s_spabiz_service_inserts.cost_type,
       #s_spabiz_service_inserts.call_after_x_days,
       #s_spabiz_service_inserts.pay_comish,
       #s_spabiz_service_inserts.description,
       #s_spabiz_service_inserts.popup,
       #s_spabiz_service_inserts.taxable,
       #s_spabiz_service_inserts.resource_count,
       #s_spabiz_service_inserts.new_extra_time,
       #s_spabiz_service_inserts.require_staff,
       #s_spabiz_service_inserts.date,
       #s_spabiz_service_inserts.store_number,
       #s_spabiz_service_inserts.pay_tip,
       #s_spabiz_service_inserts.tip,
       #s_spabiz_service_inserts.web_book,
       #s_spabiz_service_inserts.new_id,
       #s_spabiz_service_inserts.service_backup_id,
       #s_spabiz_service_inserts.web_view,
       #s_spabiz_service_inserts.hair_length,
       #s_spabiz_service_inserts.is_hilite_procedure,
       #s_spabiz_service_inserts.is_color_balance,
       #s_spabiz_service_inserts.service_level,
       #s_spabiz_service_inserts.service_class,
       #s_spabiz_service_inserts.allow_power_booking,
       #s_spabiz_service_inserts.req_cc,
       #s_spabiz_service_inserts.exclude_appt_gaur,
       case when s_spabiz_service.s_spabiz_service_id is null then isnull(#s_spabiz_service_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_service_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_service_inserts
  left join p_spabiz_service
    on #s_spabiz_service_inserts.bk_hash = p_spabiz_service.bk_hash
   and p_spabiz_service.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_service
    on p_spabiz_service.bk_hash = s_spabiz_service.bk_hash
   and p_spabiz_service.s_spabiz_service_id = s_spabiz_service.s_spabiz_service_id
 where s_spabiz_service.s_spabiz_service_id is null
    or (s_spabiz_service.s_spabiz_service_id is not null
        and s_spabiz_service.dv_hash <> #s_spabiz_service_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_service @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_service @current_dv_batch_id

end
