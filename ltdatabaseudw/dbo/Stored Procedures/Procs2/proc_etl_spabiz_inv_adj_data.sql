CREATE PROC [dbo].[proc_etl_spabiz_inv_adj_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_INVADJDATA

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_INVADJDATA (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       Date,
       ADJID,
       QTY,
       PRODUCTID,
       STAFFID,
       REASONID,
       COST,
       LAYERID,
       SOURCETYPE,
       SOURCEID,
       STATUS,
       CATID,
       STORE_NUMBER,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       Date,
       ADJID,
       QTY,
       PRODUCTID,
       STAFFID,
       REASONID,
       COST,
       LAYERID,
       SOURCETYPE,
       SOURCEID,
       STATUS,
       CATID,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_INVADJDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_INVADJDATA
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_inv_adj_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_inv_adj_data (
       bk_hash,
       inv_adj_data_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_INVADJDATA.bk_hash,
       stage_hash_spabiz_INVADJDATA.ID inv_adj_data_id,
       stage_hash_spabiz_INVADJDATA.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_INVADJDATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_INVADJDATA
  left join h_spabiz_inv_adj_data
    on stage_hash_spabiz_INVADJDATA.bk_hash = h_spabiz_inv_adj_data.bk_hash
 where h_spabiz_inv_adj_data_id is null
   and stage_hash_spabiz_INVADJDATA.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_inv_adj_data
if object_id('tempdb..#l_spabiz_inv_adj_data_inserts') is not null drop table #l_spabiz_inv_adj_data_inserts
create table #l_spabiz_inv_adj_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_INVADJDATA.bk_hash,
       stage_hash_spabiz_INVADJDATA.ID inv_adj_data_id,
       stage_hash_spabiz_INVADJDATA.STOREID store_id,
       stage_hash_spabiz_INVADJDATA.ADJID adj_id,
       stage_hash_spabiz_INVADJDATA.PRODUCTID product_id,
       stage_hash_spabiz_INVADJDATA.STAFFID staff_id,
       stage_hash_spabiz_INVADJDATA.REASONID reason_id,
       stage_hash_spabiz_INVADJDATA.SOURCEID source_id,
       stage_hash_spabiz_INVADJDATA.CATID cat_id,
       stage_hash_spabiz_INVADJDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_INVADJDATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.ADJID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.PRODUCTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.REASONID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.SOURCEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.CATID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_INVADJDATA
 where stage_hash_spabiz_INVADJDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_inv_adj_data records
set @insert_date_time = getdate()
insert into l_spabiz_inv_adj_data (
       bk_hash,
       inv_adj_data_id,
       store_id,
       adj_id,
       product_id,
       staff_id,
       reason_id,
       source_id,
       cat_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_inv_adj_data_inserts.bk_hash,
       #l_spabiz_inv_adj_data_inserts.inv_adj_data_id,
       #l_spabiz_inv_adj_data_inserts.store_id,
       #l_spabiz_inv_adj_data_inserts.adj_id,
       #l_spabiz_inv_adj_data_inserts.product_id,
       #l_spabiz_inv_adj_data_inserts.staff_id,
       #l_spabiz_inv_adj_data_inserts.reason_id,
       #l_spabiz_inv_adj_data_inserts.source_id,
       #l_spabiz_inv_adj_data_inserts.cat_id,
       #l_spabiz_inv_adj_data_inserts.store_number,
       case when l_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id is null then isnull(#l_spabiz_inv_adj_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_inv_adj_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_inv_adj_data_inserts
  left join p_spabiz_inv_adj_data
    on #l_spabiz_inv_adj_data_inserts.bk_hash = p_spabiz_inv_adj_data.bk_hash
   and p_spabiz_inv_adj_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_inv_adj_data
    on p_spabiz_inv_adj_data.bk_hash = l_spabiz_inv_adj_data.bk_hash
   and p_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id = l_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id
 where l_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id is null
    or (l_spabiz_inv_adj_data.l_spabiz_inv_adj_data_id is not null
        and l_spabiz_inv_adj_data.dv_hash <> #l_spabiz_inv_adj_data_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_inv_adj_data
if object_id('tempdb..#s_spabiz_inv_adj_data_inserts') is not null drop table #s_spabiz_inv_adj_data_inserts
create table #s_spabiz_inv_adj_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_INVADJDATA.bk_hash,
       stage_hash_spabiz_INVADJDATA.ID inv_adj_data_id,
       stage_hash_spabiz_INVADJDATA.COUNTERID counter_id,
       stage_hash_spabiz_INVADJDATA.EDITTIME edit_time,
       stage_hash_spabiz_INVADJDATA.Date date,
       stage_hash_spabiz_INVADJDATA.QTY qty,
       stage_hash_spabiz_INVADJDATA.COST cost,
       stage_hash_spabiz_INVADJDATA.LAYERID layer_id,
       stage_hash_spabiz_INVADJDATA.SOURCETYPE source_type,
       stage_hash_spabiz_INVADJDATA.STATUS status,
       stage_hash_spabiz_INVADJDATA.STORE_NUMBER store_number,
       stage_hash_spabiz_INVADJDATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INVADJDATA.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_INVADJDATA.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.QTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.COST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.LAYERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.SOURCETYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_INVADJDATA.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_INVADJDATA
 where stage_hash_spabiz_INVADJDATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_inv_adj_data records
set @insert_date_time = getdate()
insert into s_spabiz_inv_adj_data (
       bk_hash,
       inv_adj_data_id,
       counter_id,
       edit_time,
       date,
       qty,
       cost,
       layer_id,
       source_type,
       status,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_inv_adj_data_inserts.bk_hash,
       #s_spabiz_inv_adj_data_inserts.inv_adj_data_id,
       #s_spabiz_inv_adj_data_inserts.counter_id,
       #s_spabiz_inv_adj_data_inserts.edit_time,
       #s_spabiz_inv_adj_data_inserts.date,
       #s_spabiz_inv_adj_data_inserts.qty,
       #s_spabiz_inv_adj_data_inserts.cost,
       #s_spabiz_inv_adj_data_inserts.layer_id,
       #s_spabiz_inv_adj_data_inserts.source_type,
       #s_spabiz_inv_adj_data_inserts.status,
       #s_spabiz_inv_adj_data_inserts.store_number,
       case when s_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id is null then isnull(#s_spabiz_inv_adj_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_inv_adj_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_inv_adj_data_inserts
  left join p_spabiz_inv_adj_data
    on #s_spabiz_inv_adj_data_inserts.bk_hash = p_spabiz_inv_adj_data.bk_hash
   and p_spabiz_inv_adj_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_inv_adj_data
    on p_spabiz_inv_adj_data.bk_hash = s_spabiz_inv_adj_data.bk_hash
   and p_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id = s_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id
 where s_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id is null
    or (s_spabiz_inv_adj_data.s_spabiz_inv_adj_data_id is not null
        and s_spabiz_inv_adj_data.dv_hash <> #s_spabiz_inv_adj_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_inv_adj_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_inv_adj_data @current_dv_batch_id

end
