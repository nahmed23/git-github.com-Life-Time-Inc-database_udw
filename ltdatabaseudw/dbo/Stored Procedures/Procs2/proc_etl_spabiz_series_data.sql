CREATE PROC [dbo].[proc_etl_spabiz_series_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_SERIESDATA

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_SERIESDATA (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       SERVICEID,
       SERVICEPRICE,
       PRICETYPE,
       SERIESID,
       ORDERINDEX,
       [Order],
       CUSTID,
       STORE_NUMBER,
       MASTERSERIESDATAID,
       TIPAMT,
       TIPTYPE,
       TIPPERCENT,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       SERVICEID,
       SERVICEPRICE,
       PRICETYPE,
       SERIESID,
       ORDERINDEX,
       [Order],
       CUSTID,
       STORE_NUMBER,
       MASTERSERIESDATAID,
       TIPAMT,
       TIPTYPE,
       TIPPERCENT,
       isnull(cast(stage_spabiz_SERIESDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_SERIESDATA
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_series_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_series_data (
       bk_hash,
       series_data_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_SERIESDATA.bk_hash,
       stage_hash_spabiz_SERIESDATA.ID series_data_id,
       stage_hash_spabiz_SERIESDATA.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_SERIESDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_SERIESDATA
  left join h_spabiz_series_data
    on stage_hash_spabiz_SERIESDATA.bk_hash = h_spabiz_series_data.bk_hash
 where h_spabiz_series_data_id is null
   and stage_hash_spabiz_SERIESDATA.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_series_data
if object_id('tempdb..#l_spabiz_series_data_inserts') is not null drop table #l_spabiz_series_data_inserts
create table #l_spabiz_series_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERIESDATA.bk_hash,
       stage_hash_spabiz_SERIESDATA.ID series_data_id,
       stage_hash_spabiz_SERIESDATA.STOREID store_id,
       stage_hash_spabiz_SERIESDATA.SERVICEID service_id,
       stage_hash_spabiz_SERIESDATA.SERIESID series_id,
       stage_hash_spabiz_SERIESDATA.CUSTID cust_id,
       stage_hash_spabiz_SERIESDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_SERIESDATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.SERVICEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.SERIESID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.CUSTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERIESDATA
 where stage_hash_spabiz_SERIESDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_series_data records
set @insert_date_time = getdate()
insert into l_spabiz_series_data (
       bk_hash,
       series_data_id,
       store_id,
       service_id,
       series_id,
       cust_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_series_data_inserts.bk_hash,
       #l_spabiz_series_data_inserts.series_data_id,
       #l_spabiz_series_data_inserts.store_id,
       #l_spabiz_series_data_inserts.service_id,
       #l_spabiz_series_data_inserts.series_id,
       #l_spabiz_series_data_inserts.cust_id,
       #l_spabiz_series_data_inserts.store_number,
       case when l_spabiz_series_data.l_spabiz_series_data_id is null then isnull(#l_spabiz_series_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_series_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_series_data_inserts
  left join p_spabiz_series_data
    on #l_spabiz_series_data_inserts.bk_hash = p_spabiz_series_data.bk_hash
   and p_spabiz_series_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_series_data
    on p_spabiz_series_data.bk_hash = l_spabiz_series_data.bk_hash
   and p_spabiz_series_data.l_spabiz_series_data_id = l_spabiz_series_data.l_spabiz_series_data_id
 where l_spabiz_series_data.l_spabiz_series_data_id is null
    or (l_spabiz_series_data.l_spabiz_series_data_id is not null
        and l_spabiz_series_data.dv_hash <> #l_spabiz_series_data_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_series_data
if object_id('tempdb..#s_spabiz_series_data_inserts') is not null drop table #s_spabiz_series_data_inserts
create table #s_spabiz_series_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERIESDATA.bk_hash,
       stage_hash_spabiz_SERIESDATA.ID series_data_id,
       stage_hash_spabiz_SERIESDATA.COUNTERID counter_id,
       stage_hash_spabiz_SERIESDATA.EDITTIME edit_time,
       stage_hash_spabiz_SERIESDATA.SERVICEPRICE service_price,
       stage_hash_spabiz_SERIESDATA.PRICETYPE price_type,
       stage_hash_spabiz_SERIESDATA.ORDERINDEX order_index,
       stage_hash_spabiz_SERIESDATA.[Order] series_data_order,
       stage_hash_spabiz_SERIESDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_SERIESDATA.MASTERSERIESDATAID master_series_data_id,
       stage_hash_spabiz_SERIESDATA.TIPAMT tip_amt,
       stage_hash_spabiz_SERIESDATA.TIPTYPE tip_type,
       stage_hash_spabiz_SERIESDATA.TIPPERCENT tip_percent,
       stage_hash_spabiz_SERIESDATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERIESDATA.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.SERVICEPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.PRICETYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERIESDATA.ORDERINDEX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.[Order] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.MASTERSERIESDATAID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.TIPAMT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.TIPTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIESDATA.TIPPERCENT as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERIESDATA
 where stage_hash_spabiz_SERIESDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_series_data records
set @insert_date_time = getdate()
insert into s_spabiz_series_data (
       bk_hash,
       series_data_id,
       counter_id,
       edit_time,
       service_price,
       price_type,
       order_index,
       series_data_order,
       store_number,
       master_series_data_id,
       tip_amt,
       tip_type,
       tip_percent,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_series_data_inserts.bk_hash,
       #s_spabiz_series_data_inserts.series_data_id,
       #s_spabiz_series_data_inserts.counter_id,
       #s_spabiz_series_data_inserts.edit_time,
       #s_spabiz_series_data_inserts.service_price,
       #s_spabiz_series_data_inserts.price_type,
       #s_spabiz_series_data_inserts.order_index,
       #s_spabiz_series_data_inserts.series_data_order,
       #s_spabiz_series_data_inserts.store_number,
       #s_spabiz_series_data_inserts.master_series_data_id,
       #s_spabiz_series_data_inserts.tip_amt,
       #s_spabiz_series_data_inserts.tip_type,
       #s_spabiz_series_data_inserts.tip_percent,
       case when s_spabiz_series_data.s_spabiz_series_data_id is null then isnull(#s_spabiz_series_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_series_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_series_data_inserts
  left join p_spabiz_series_data
    on #s_spabiz_series_data_inserts.bk_hash = p_spabiz_series_data.bk_hash
   and p_spabiz_series_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_series_data
    on p_spabiz_series_data.bk_hash = s_spabiz_series_data.bk_hash
   and p_spabiz_series_data.s_spabiz_series_data_id = s_spabiz_series_data.s_spabiz_series_data_id
 where s_spabiz_series_data.s_spabiz_series_data_id is null
    or (s_spabiz_series_data.s_spabiz_series_data_id is not null
        and s_spabiz_series_data.dv_hash <> #s_spabiz_series_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_series_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_series_data @current_dv_batch_id

end
