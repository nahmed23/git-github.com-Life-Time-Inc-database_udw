CREATE PROC [dbo].[proc_etl_spabiz_series] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_SERIES

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_SERIES (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       NAME,
       QUICKID,
       RETAILPRICE,
       TAXABLE,
       DELETEDATE,
       [Order],
       ORDERINDEX,
       STORE_NUMBER,
       MASTERSERIESID,
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
       NAME,
       QUICKID,
       RETAILPRICE,
       TAXABLE,
       DELETEDATE,
       [Order],
       ORDERINDEX,
       STORE_NUMBER,
       MASTERSERIESID,
       isnull(cast(stage_spabiz_SERIES.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_SERIES
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_series @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_series (
       bk_hash,
       series_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_SERIES.bk_hash,
       stage_hash_spabiz_SERIES.ID series_id,
       stage_hash_spabiz_SERIES.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_SERIES.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_SERIES
  left join h_spabiz_series
    on stage_hash_spabiz_SERIES.bk_hash = h_spabiz_series.bk_hash
 where h_spabiz_series_id is null
   and stage_hash_spabiz_SERIES.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_series
if object_id('tempdb..#l_spabiz_series_inserts') is not null drop table #l_spabiz_series_inserts
create table #l_spabiz_series_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERIES.bk_hash,
       stage_hash_spabiz_SERIES.ID series_id,
       stage_hash_spabiz_SERIES.STOREID store_id,
       stage_hash_spabiz_SERIES.STORE_NUMBER store_number,
       stage_hash_spabiz_SERIES.MASTERSERIESID master_series_id,
       stage_hash_spabiz_SERIES.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.MASTERSERIESID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERIES
 where stage_hash_spabiz_SERIES.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_series records
set @insert_date_time = getdate()
insert into l_spabiz_series (
       bk_hash,
       series_id,
       store_id,
       store_number,
       master_series_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_series_inserts.bk_hash,
       #l_spabiz_series_inserts.series_id,
       #l_spabiz_series_inserts.store_id,
       #l_spabiz_series_inserts.store_number,
       #l_spabiz_series_inserts.master_series_id,
       case when l_spabiz_series.l_spabiz_series_id is null then isnull(#l_spabiz_series_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_series_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_series_inserts
  left join p_spabiz_series
    on #l_spabiz_series_inserts.bk_hash = p_spabiz_series.bk_hash
   and p_spabiz_series.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_series
    on p_spabiz_series.bk_hash = l_spabiz_series.bk_hash
   and p_spabiz_series.l_spabiz_series_id = l_spabiz_series.l_spabiz_series_id
 where l_spabiz_series.l_spabiz_series_id is null
    or (l_spabiz_series.l_spabiz_series_id is not null
        and l_spabiz_series.dv_hash <> #l_spabiz_series_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_series
if object_id('tempdb..#s_spabiz_series_inserts') is not null drop table #s_spabiz_series_inserts
create table #s_spabiz_series_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_SERIES.bk_hash,
       stage_hash_spabiz_SERIES.ID series_id,
       stage_hash_spabiz_SERIES.COUNTERID counter_id,
       stage_hash_spabiz_SERIES.EDITTIME edit_time,
       stage_hash_spabiz_SERIES.[Delete] series_delete,
       stage_hash_spabiz_SERIES.NAME name,
       stage_hash_spabiz_SERIES.QUICKID quick_id,
       stage_hash_spabiz_SERIES.RETAILPRICE retail_price,
       stage_hash_spabiz_SERIES.TAXABLE taxable,
       stage_hash_spabiz_SERIES.DELETEDATE delete_date,
       stage_hash_spabiz_SERIES.[Order] [order],
       stage_hash_spabiz_SERIES.ORDERINDEX orderindex,
       stage_hash_spabiz_SERIES.STORE_NUMBER store_number,
       stage_hash_spabiz_SERIES.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERIES.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERIES.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERIES.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.TAXABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_SERIES.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.[Order] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_SERIES.ORDERINDEX,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_SERIES.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_SERIES
 where stage_hash_spabiz_SERIES.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_series records
set @insert_date_time = getdate()
insert into s_spabiz_series (
       bk_hash,
       series_id,
       counter_id,
       edit_time,
       series_delete,
       name,
       quick_id,
       retail_price,
       taxable,
       delete_date,
       [order],
       orderindex,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_series_inserts.bk_hash,
       #s_spabiz_series_inserts.series_id,
       #s_spabiz_series_inserts.counter_id,
       #s_spabiz_series_inserts.edit_time,
       #s_spabiz_series_inserts.series_delete,
       #s_spabiz_series_inserts.name,
       #s_spabiz_series_inserts.quick_id,
       #s_spabiz_series_inserts.retail_price,
       #s_spabiz_series_inserts.taxable,
       #s_spabiz_series_inserts.delete_date,
       #s_spabiz_series_inserts.[order],
       #s_spabiz_series_inserts.orderindex,
       #s_spabiz_series_inserts.store_number,
       case when s_spabiz_series.s_spabiz_series_id is null then isnull(#s_spabiz_series_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_series_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_series_inserts
  left join p_spabiz_series
    on #s_spabiz_series_inserts.bk_hash = p_spabiz_series.bk_hash
   and p_spabiz_series.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_series
    on p_spabiz_series.bk_hash = s_spabiz_series.bk_hash
   and p_spabiz_series.s_spabiz_series_id = s_spabiz_series.s_spabiz_series_id
 where s_spabiz_series.s_spabiz_series_id is null
    or (s_spabiz_series.s_spabiz_series_id is not null
        and s_spabiz_series.dv_hash <> #s_spabiz_series_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_series @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_series @current_dv_batch_id

end
