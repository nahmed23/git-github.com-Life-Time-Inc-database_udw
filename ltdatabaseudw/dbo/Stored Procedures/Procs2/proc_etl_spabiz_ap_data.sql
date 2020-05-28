CREATE PROC [dbo].[proc_etl_spabiz_ap_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_APDATA

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_APDATA (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       APID,
       SERVICEID,
       CUSTID,
       STAFFID,
       DATATYPE,
       STIME,
       ETIME,
       STARTTIME,
       ENDTIME,
       TIME,
       PARENTID,
       STAFFADDED,
       APTIMEINDEX,
       STATUS,
       RETENTION,
       NOTE,
       TICKETDATAID,
       PRICE,
       RESOURCEID,
       [Delete],
       RES_STIME,
       RES_ETIME,
       CHECKIN,
       CHECKOUT,
       BLOCKTIMENAME,
       RETENTIONNAME,
       RETENTIONCOLOR,
       CUSTOMERFIRSTNAME,
       CUSTOMERLASTNAME,
       CUSTOMERSERVICEVISITS,
       SERVICEBOOKNAME,
       SERVICENAME,
       STORE_NUMBER,
       STANDING,
       RESBLOCK,
       BOOKEDONWEB,
       BYSTAFFID,
       ACTIVITYID,
       SERVICEAPID,
       HTNGID,
       DEMANDFORCE,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       APID,
       SERVICEID,
       CUSTID,
       STAFFID,
       DATATYPE,
       STIME,
       ETIME,
       STARTTIME,
       ENDTIME,
       TIME,
       PARENTID,
       STAFFADDED,
       APTIMEINDEX,
       STATUS,
       RETENTION,
       NOTE,
       TICKETDATAID,
       PRICE,
       RESOURCEID,
       [Delete],
       RES_STIME,
       RES_ETIME,
       CHECKIN,
       CHECKOUT,
       BLOCKTIMENAME,
       RETENTIONNAME,
       RETENTIONCOLOR,
       CUSTOMERFIRSTNAME,
       CUSTOMERLASTNAME,
       CUSTOMERSERVICEVISITS,
       SERVICEBOOKNAME,
       SERVICENAME,
       STORE_NUMBER,
       STANDING,
       RESBLOCK,
       BOOKEDONWEB,
       BYSTAFFID,
       ACTIVITYID,
       SERVICEAPID,
       HTNGID,
       DEMANDFORCE,
       isnull(cast(stage_spabiz_APDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_APDATA
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ap_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ap_data (
       bk_hash,
       ap_data_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_APDATA.bk_hash,
       stage_hash_spabiz_APDATA.ID ap_data_id,
       stage_hash_spabiz_APDATA.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_APDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_APDATA
  left join h_spabiz_ap_data
    on stage_hash_spabiz_APDATA.bk_hash = h_spabiz_ap_data.bk_hash
 where h_spabiz_ap_data_id is null
   and stage_hash_spabiz_APDATA.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ap_data
if object_id('tempdb..#l_spabiz_ap_data_inserts') is not null drop table #l_spabiz_ap_data_inserts
create table #l_spabiz_ap_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_APDATA.bk_hash,
       stage_hash_spabiz_APDATA.ID ap_data_id,
       stage_hash_spabiz_APDATA.STOREID store_id,
       stage_hash_spabiz_APDATA.APID ap_id,
       stage_hash_spabiz_APDATA.SERVICEID service_id,
       stage_hash_spabiz_APDATA.CUSTID cust_id,
       stage_hash_spabiz_APDATA.STAFFID staff_id,
       stage_hash_spabiz_APDATA.PARENTID parent_id,
       stage_hash_spabiz_APDATA.STAFFADDED staff_added,
       stage_hash_spabiz_APDATA.RETENTION retention,
       stage_hash_spabiz_APDATA.TICKETDATAID ticket_data_id,
       stage_hash_spabiz_APDATA.RESOURCEID resource_id,
       stage_hash_spabiz_APDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_APDATA.BYSTAFFID by_staff_id,
       isnull(cast(stage_hash_spabiz_APDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.APID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.SERVICEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.PARENTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.STAFFADDED as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.RETENTION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.TICKETDATAID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.RESOURCEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.BYSTAFFID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_APDATA
 where stage_hash_spabiz_APDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ap_data records
set @insert_date_time = getdate()
insert into l_spabiz_ap_data (
       bk_hash,
       ap_data_id,
       store_id,
       ap_id,
       service_id,
       cust_id,
       staff_id,
       parent_id,
       staff_added,
       retention,
       ticket_data_id,
       resource_id,
       store_number,
       by_staff_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ap_data_inserts.bk_hash,
       #l_spabiz_ap_data_inserts.ap_data_id,
       #l_spabiz_ap_data_inserts.store_id,
       #l_spabiz_ap_data_inserts.ap_id,
       #l_spabiz_ap_data_inserts.service_id,
       #l_spabiz_ap_data_inserts.cust_id,
       #l_spabiz_ap_data_inserts.staff_id,
       #l_spabiz_ap_data_inserts.parent_id,
       #l_spabiz_ap_data_inserts.staff_added,
       #l_spabiz_ap_data_inserts.retention,
       #l_spabiz_ap_data_inserts.ticket_data_id,
       #l_spabiz_ap_data_inserts.resource_id,
       #l_spabiz_ap_data_inserts.store_number,
       #l_spabiz_ap_data_inserts.by_staff_id,
       case when l_spabiz_ap_data.l_spabiz_ap_data_id is null then isnull(#l_spabiz_ap_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ap_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ap_data_inserts
  left join p_spabiz_ap_data
    on #l_spabiz_ap_data_inserts.bk_hash = p_spabiz_ap_data.bk_hash
   and p_spabiz_ap_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ap_data
    on p_spabiz_ap_data.bk_hash = l_spabiz_ap_data.bk_hash
   and p_spabiz_ap_data.l_spabiz_ap_data_id = l_spabiz_ap_data.l_spabiz_ap_data_id
 where l_spabiz_ap_data.l_spabiz_ap_data_id is null
    or (l_spabiz_ap_data.l_spabiz_ap_data_id is not null
        and l_spabiz_ap_data.dv_hash <> #l_spabiz_ap_data_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ap_data
if object_id('tempdb..#s_spabiz_ap_data_inserts') is not null drop table #s_spabiz_ap_data_inserts
create table #s_spabiz_ap_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_APDATA.bk_hash,
       stage_hash_spabiz_APDATA.ID ap_data_id,
       stage_hash_spabiz_APDATA.COUNTERID counter_id,
       stage_hash_spabiz_APDATA.EDITTIME edit_time,
       stage_hash_spabiz_APDATA.DATATYPE data_type,
       stage_hash_spabiz_APDATA.STIME s_time,
       stage_hash_spabiz_APDATA.ETIME e_time,
       stage_hash_spabiz_APDATA.STARTTIME start_time,
       stage_hash_spabiz_APDATA.ENDTIME end_time,
       stage_hash_spabiz_APDATA.TIME time,
       stage_hash_spabiz_APDATA.APTIMEINDEX ap_time_index,
       stage_hash_spabiz_APDATA.STATUS status,
       stage_hash_spabiz_APDATA.NOTE note,
       stage_hash_spabiz_APDATA.PRICE price,
       stage_hash_spabiz_APDATA.[Delete] ap_data_delete,
       stage_hash_spabiz_APDATA.RES_STIME ress_time,
       stage_hash_spabiz_APDATA.RES_ETIME rese_time,
       stage_hash_spabiz_APDATA.CHECKIN check_in,
       stage_hash_spabiz_APDATA.CHECKOUT check_out,
       stage_hash_spabiz_APDATA.BLOCKTIMENAME block_time_name,
       stage_hash_spabiz_APDATA.RETENTIONNAME retention_name,
       stage_hash_spabiz_APDATA.RETENTIONCOLOR retention_color,
       stage_hash_spabiz_APDATA.CUSTOMERFIRSTNAME customer_first_name,
       stage_hash_spabiz_APDATA.CUSTOMERLASTNAME customer_last_name,
       stage_hash_spabiz_APDATA.CUSTOMERSERVICEVISITS customer_service_visits,
       stage_hash_spabiz_APDATA.SERVICEBOOKNAME service_book_name,
       stage_hash_spabiz_APDATA.SERVICENAME service_name,
       stage_hash_spabiz_APDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_APDATA.STANDING standing,
       stage_hash_spabiz_APDATA.RESBLOCK res_block,
       stage_hash_spabiz_APDATA.BOOKEDONWEB booked_on_web,
       stage_hash_spabiz_APDATA.ACTIVITYID activity_id,
       stage_hash_spabiz_APDATA.SERVICEAPID service_ap_id,
       stage_hash_spabiz_APDATA.HTNGID htng_id,
       stage_hash_spabiz_APDATA.DEMANDFORCE demand_force,
       isnull(cast(stage_hash_spabiz_APDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_APDATA.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.DATATYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.STIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.ETIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_APDATA.STARTTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_APDATA.ENDTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.TIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.APTIMEINDEX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.NOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.PRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.RES_STIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.RES_ETIME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_APDATA.CHECKIN,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_APDATA.CHECKOUT,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.BLOCKTIMENAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.RETENTIONNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.RETENTIONCOLOR as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.CUSTOMERFIRSTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.CUSTOMERLASTNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.CUSTOMERSERVICEVISITS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.SERVICEBOOKNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.SERVICENAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.STANDING as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.RESBLOCK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.BOOKEDONWEB as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.ACTIVITYID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.SERVICEAPID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_APDATA.HTNGID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_APDATA.DEMANDFORCE as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_APDATA
 where stage_hash_spabiz_APDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ap_data records
set @insert_date_time = getdate()
insert into s_spabiz_ap_data (
       bk_hash,
       ap_data_id,
       counter_id,
       edit_time,
       data_type,
       s_time,
       e_time,
       start_time,
       end_time,
       time,
       ap_time_index,
       status,
       note,
       price,
       ap_data_delete,
       ress_time,
       rese_time,
       check_in,
       check_out,
       block_time_name,
       retention_name,
       retention_color,
       customer_first_name,
       customer_last_name,
       customer_service_visits,
       service_book_name,
       service_name,
       store_number,
       standing,
       res_block,
       booked_on_web,
       activity_id,
       service_ap_id,
       htng_id,
       demand_force,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ap_data_inserts.bk_hash,
       #s_spabiz_ap_data_inserts.ap_data_id,
       #s_spabiz_ap_data_inserts.counter_id,
       #s_spabiz_ap_data_inserts.edit_time,
       #s_spabiz_ap_data_inserts.data_type,
       #s_spabiz_ap_data_inserts.s_time,
       #s_spabiz_ap_data_inserts.e_time,
       #s_spabiz_ap_data_inserts.start_time,
       #s_spabiz_ap_data_inserts.end_time,
       #s_spabiz_ap_data_inserts.time,
       #s_spabiz_ap_data_inserts.ap_time_index,
       #s_spabiz_ap_data_inserts.status,
       #s_spabiz_ap_data_inserts.note,
       #s_spabiz_ap_data_inserts.price,
       #s_spabiz_ap_data_inserts.ap_data_delete,
       #s_spabiz_ap_data_inserts.ress_time,
       #s_spabiz_ap_data_inserts.rese_time,
       #s_spabiz_ap_data_inserts.check_in,
       #s_spabiz_ap_data_inserts.check_out,
       #s_spabiz_ap_data_inserts.block_time_name,
       #s_spabiz_ap_data_inserts.retention_name,
       #s_spabiz_ap_data_inserts.retention_color,
       #s_spabiz_ap_data_inserts.customer_first_name,
       #s_spabiz_ap_data_inserts.customer_last_name,
       #s_spabiz_ap_data_inserts.customer_service_visits,
       #s_spabiz_ap_data_inserts.service_book_name,
       #s_spabiz_ap_data_inserts.service_name,
       #s_spabiz_ap_data_inserts.store_number,
       #s_spabiz_ap_data_inserts.standing,
       #s_spabiz_ap_data_inserts.res_block,
       #s_spabiz_ap_data_inserts.booked_on_web,
       #s_spabiz_ap_data_inserts.activity_id,
       #s_spabiz_ap_data_inserts.service_ap_id,
       #s_spabiz_ap_data_inserts.htng_id,
       #s_spabiz_ap_data_inserts.demand_force,
       case when s_spabiz_ap_data.s_spabiz_ap_data_id is null then isnull(#s_spabiz_ap_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ap_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ap_data_inserts
  left join p_spabiz_ap_data
    on #s_spabiz_ap_data_inserts.bk_hash = p_spabiz_ap_data.bk_hash
   and p_spabiz_ap_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ap_data
    on p_spabiz_ap_data.bk_hash = s_spabiz_ap_data.bk_hash
   and p_spabiz_ap_data.s_spabiz_ap_data_id = s_spabiz_ap_data.s_spabiz_ap_data_id
 where s_spabiz_ap_data.s_spabiz_ap_data_id is null
    or (s_spabiz_ap_data.s_spabiz_ap_data_id is not null
        and s_spabiz_ap_data.dv_hash <> #s_spabiz_ap_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ap_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ap_data @current_dv_batch_id

end
