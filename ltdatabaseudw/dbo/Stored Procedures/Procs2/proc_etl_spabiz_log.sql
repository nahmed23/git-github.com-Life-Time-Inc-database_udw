CREATE PROC [dbo].[proc_etl_spabiz_log] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_LOG

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_LOG (
       bk_hash,
       APID,
       APDATAID,
       ID,
       TIMEID,
       ACTION,
       BYSTAFFID,
       TIMESTAMP,
       CUSTID,
       STAFFID,
       SERVICEID,
       STARTTIME,
       ENDTIME,
       STORE_NUMBER,
       COUNTERID,
       STOREID,
       EDITTIME,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(APDATAID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       APID,
       APDATAID,
       ID,
       TIMEID,
       ACTION,
       BYSTAFFID,
       TIMESTAMP,
       CUSTID,
       STAFFID,
       SERVICEID,
       STARTTIME,
       ENDTIME,
       STORE_NUMBER,
       COUNTERID,
       STOREID,
       EDITTIME,
       isnull(cast(stage_spabiz_LOG.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_LOG
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_log @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_log (
       bk_hash,
       ap_data_id,
       log_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_LOG.bk_hash,
       stage_hash_spabiz_LOG.APDATAID ap_data_id,
       stage_hash_spabiz_LOG.ID log_id,
       stage_hash_spabiz_LOG.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_LOG.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_LOG
  left join h_spabiz_log
    on stage_hash_spabiz_LOG.bk_hash = h_spabiz_log.bk_hash
 where h_spabiz_log_id is null
   and stage_hash_spabiz_LOG.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_log
if object_id('tempdb..#l_spabiz_log_inserts') is not null drop table #l_spabiz_log_inserts
create table #l_spabiz_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_LOG.bk_hash,
       stage_hash_spabiz_LOG.APID ap_id,
       stage_hash_spabiz_LOG.APDATAID ap_data_id,
       stage_hash_spabiz_LOG.ID log_id,
       stage_hash_spabiz_LOG.TIMEID time_id,
       stage_hash_spabiz_LOG.BYSTAFFID by_staff_id,
       stage_hash_spabiz_LOG.CUSTID cust_id,
       stage_hash_spabiz_LOG.STAFFID staff_id,
       stage_hash_spabiz_LOG.SERVICEID service_id,
       stage_hash_spabiz_LOG.STORE_NUMBER store_number,
       stage_hash_spabiz_LOG.COUNTERID counter_id,
       stage_hash_spabiz_LOG.STOREID store_id,
       stage_hash_spabiz_LOG.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.APID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.APDATAID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.TIMEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.BYSTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.SERVICEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.STOREID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_LOG
 where stage_hash_spabiz_LOG.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_log records
set @insert_date_time = getdate()
insert into l_spabiz_log (
       bk_hash,
       ap_id,
       ap_data_id,
       log_id,
       time_id,
       by_staff_id,
       cust_id,
       staff_id,
       service_id,
       store_number,
       counter_id,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_log_inserts.bk_hash,
       #l_spabiz_log_inserts.ap_id,
       #l_spabiz_log_inserts.ap_data_id,
       #l_spabiz_log_inserts.log_id,
       #l_spabiz_log_inserts.time_id,
       #l_spabiz_log_inserts.by_staff_id,
       #l_spabiz_log_inserts.cust_id,
       #l_spabiz_log_inserts.staff_id,
       #l_spabiz_log_inserts.service_id,
       #l_spabiz_log_inserts.store_number,
       #l_spabiz_log_inserts.counter_id,
       #l_spabiz_log_inserts.store_id,
       case when l_spabiz_log.l_spabiz_log_id is null then isnull(#l_spabiz_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_log_inserts
  left join p_spabiz_log
    on #l_spabiz_log_inserts.bk_hash = p_spabiz_log.bk_hash
   and p_spabiz_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_log
    on p_spabiz_log.bk_hash = l_spabiz_log.bk_hash
   and p_spabiz_log.l_spabiz_log_id = l_spabiz_log.l_spabiz_log_id
 where l_spabiz_log.l_spabiz_log_id is null
    or (l_spabiz_log.l_spabiz_log_id is not null
        and l_spabiz_log.dv_hash <> #l_spabiz_log_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_log
if object_id('tempdb..#s_spabiz_log_inserts') is not null drop table #s_spabiz_log_inserts
create table #s_spabiz_log_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_LOG.bk_hash,
       stage_hash_spabiz_LOG.APDATAID ap_data_id,
       stage_hash_spabiz_LOG.ID log_id,
       stage_hash_spabiz_LOG.ACTION action,
       stage_hash_spabiz_LOG.TIMESTAMP timestamp,
       stage_hash_spabiz_LOG.STARTTIME start_time,
       stage_hash_spabiz_LOG.ENDTIME end_time,
       stage_hash_spabiz_LOG.STORE_NUMBER store_number,
       stage_hash_spabiz_LOG.EDITTIME edit_time,
       stage_hash_spabiz_LOG.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.APDATAID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.ACTION as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_LOG.TIMESTAMP,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_LOG.STARTTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_LOG.ENDTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_LOG.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_LOG.EDITTIME,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_LOG
 where stage_hash_spabiz_LOG.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_log records
set @insert_date_time = getdate()
insert into s_spabiz_log (
       bk_hash,
       ap_data_id,
       log_id,
       action,
       timestamp,
       start_time,
       end_time,
       store_number,
       edit_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_log_inserts.bk_hash,
       #s_spabiz_log_inserts.ap_data_id,
       #s_spabiz_log_inserts.log_id,
       #s_spabiz_log_inserts.action,
       #s_spabiz_log_inserts.timestamp,
       #s_spabiz_log_inserts.start_time,
       #s_spabiz_log_inserts.end_time,
       #s_spabiz_log_inserts.store_number,
       #s_spabiz_log_inserts.edit_time,
       case when s_spabiz_log.s_spabiz_log_id is null then isnull(#s_spabiz_log_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_log_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_log_inserts
  left join p_spabiz_log
    on #s_spabiz_log_inserts.bk_hash = p_spabiz_log.bk_hash
   and p_spabiz_log.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_log
    on p_spabiz_log.bk_hash = s_spabiz_log.bk_hash
   and p_spabiz_log.s_spabiz_log_id = s_spabiz_log.s_spabiz_log_id
 where s_spabiz_log.s_spabiz_log_id is null
    or (s_spabiz_log.s_spabiz_log_id is not null
        and s_spabiz_log.dv_hash <> #s_spabiz_log_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_log @current_dv_batch_id

end
