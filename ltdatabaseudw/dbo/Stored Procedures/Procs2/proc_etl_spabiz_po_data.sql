CREATE PROC [dbo].[proc_etl_spabiz_po_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_PODATA

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_PODATA (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       Date,
       POID,
       TYPE,
       VENDORID,
       LINENUM,
       PRODUCTID,
       NORMALCOST,
       COST,
       EXTCOST,
       QTYORD,
       QTYREC,
       STATUS,
       CATID,
       MARGIN,
       RETAILPRICE,
       NAME,
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
       POID,
       TYPE,
       VENDORID,
       LINENUM,
       PRODUCTID,
       NORMALCOST,
       COST,
       EXTCOST,
       QTYORD,
       QTYREC,
       STATUS,
       CATID,
       MARGIN,
       RETAILPRICE,
       NAME,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_PODATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_PODATA
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_po_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_po_data (
       bk_hash,
       po_data_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_PODATA.bk_hash,
       stage_hash_spabiz_PODATA.ID po_data_id,
       stage_hash_spabiz_PODATA.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_PODATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_PODATA
  left join h_spabiz_po_data
    on stage_hash_spabiz_PODATA.bk_hash = h_spabiz_po_data.bk_hash
 where h_spabiz_po_data_id is null
   and stage_hash_spabiz_PODATA.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_po_data
if object_id('tempdb..#l_spabiz_po_data_inserts') is not null drop table #l_spabiz_po_data_inserts
create table #l_spabiz_po_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PODATA.bk_hash,
       stage_hash_spabiz_PODATA.ID po_data_id,
       stage_hash_spabiz_PODATA.STOREID store_id,
       stage_hash_spabiz_PODATA.POID po_id,
       stage_hash_spabiz_PODATA.VENDORID vendor_id,
       stage_hash_spabiz_PODATA.PRODUCTID product_id,
       stage_hash_spabiz_PODATA.CATID cat_id,
       stage_hash_spabiz_PODATA.STORE_NUMBER store_number,
       stage_hash_spabiz_PODATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.POID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.VENDORID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.PRODUCTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.CATID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PODATA
 where stage_hash_spabiz_PODATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_po_data records
set @insert_date_time = getdate()
insert into l_spabiz_po_data (
       bk_hash,
       po_data_id,
       store_id,
       po_id,
       vendor_id,
       product_id,
       cat_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_po_data_inserts.bk_hash,
       #l_spabiz_po_data_inserts.po_data_id,
       #l_spabiz_po_data_inserts.store_id,
       #l_spabiz_po_data_inserts.po_id,
       #l_spabiz_po_data_inserts.vendor_id,
       #l_spabiz_po_data_inserts.product_id,
       #l_spabiz_po_data_inserts.cat_id,
       #l_spabiz_po_data_inserts.store_number,
       case when l_spabiz_po_data.l_spabiz_po_data_id is null then isnull(#l_spabiz_po_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_po_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_po_data_inserts
  left join p_spabiz_po_data
    on #l_spabiz_po_data_inserts.bk_hash = p_spabiz_po_data.bk_hash
   and p_spabiz_po_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_po_data
    on p_spabiz_po_data.bk_hash = l_spabiz_po_data.bk_hash
   and p_spabiz_po_data.l_spabiz_po_data_id = l_spabiz_po_data.l_spabiz_po_data_id
 where l_spabiz_po_data.l_spabiz_po_data_id is null
    or (l_spabiz_po_data.l_spabiz_po_data_id is not null
        and l_spabiz_po_data.dv_hash <> #l_spabiz_po_data_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_po_data
if object_id('tempdb..#s_spabiz_po_data_inserts') is not null drop table #s_spabiz_po_data_inserts
create table #s_spabiz_po_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PODATA.bk_hash,
       stage_hash_spabiz_PODATA.ID po_data_id,
       stage_hash_spabiz_PODATA.COUNTERID counter_id,
       stage_hash_spabiz_PODATA.EDITTIME edit_time,
       stage_hash_spabiz_PODATA.Date date,
       stage_hash_spabiz_PODATA.TYPE type,
       stage_hash_spabiz_PODATA.LINENUM line_num,
       stage_hash_spabiz_PODATA.NORMALCOST normal_cost,
       stage_hash_spabiz_PODATA.COST cost,
       stage_hash_spabiz_PODATA.EXTCOST ext_cost,
       stage_hash_spabiz_PODATA.QTYORD qty_ord,
       stage_hash_spabiz_PODATA.QTYREC qty_rec,
       stage_hash_spabiz_PODATA.STATUS status,
       stage_hash_spabiz_PODATA.MARGIN margin,
       stage_hash_spabiz_PODATA.RETAILPRICE retail_price,
       stage_hash_spabiz_PODATA.NAME name,
       stage_hash_spabiz_PODATA.STORE_NUMBER store_number,
       stage_hash_spabiz_PODATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PODATA.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PODATA.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.TYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.LINENUM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.NORMALCOST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.COST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.EXTCOST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.QTYORD as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.QTYREC as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.MARGIN as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PODATA.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PODATA.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PODATA
 where stage_hash_spabiz_PODATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_po_data records
set @insert_date_time = getdate()
insert into s_spabiz_po_data (
       bk_hash,
       po_data_id,
       counter_id,
       edit_time,
       date,
       type,
       line_num,
       normal_cost,
       cost,
       ext_cost,
       qty_ord,
       qty_rec,
       status,
       margin,
       retail_price,
       name,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_po_data_inserts.bk_hash,
       #s_spabiz_po_data_inserts.po_data_id,
       #s_spabiz_po_data_inserts.counter_id,
       #s_spabiz_po_data_inserts.edit_time,
       #s_spabiz_po_data_inserts.date,
       #s_spabiz_po_data_inserts.type,
       #s_spabiz_po_data_inserts.line_num,
       #s_spabiz_po_data_inserts.normal_cost,
       #s_spabiz_po_data_inserts.cost,
       #s_spabiz_po_data_inserts.ext_cost,
       #s_spabiz_po_data_inserts.qty_ord,
       #s_spabiz_po_data_inserts.qty_rec,
       #s_spabiz_po_data_inserts.status,
       #s_spabiz_po_data_inserts.margin,
       #s_spabiz_po_data_inserts.retail_price,
       #s_spabiz_po_data_inserts.name,
       #s_spabiz_po_data_inserts.store_number,
       case when s_spabiz_po_data.s_spabiz_po_data_id is null then isnull(#s_spabiz_po_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_po_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_po_data_inserts
  left join p_spabiz_po_data
    on #s_spabiz_po_data_inserts.bk_hash = p_spabiz_po_data.bk_hash
   and p_spabiz_po_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_po_data
    on p_spabiz_po_data.bk_hash = s_spabiz_po_data.bk_hash
   and p_spabiz_po_data.s_spabiz_po_data_id = s_spabiz_po_data.s_spabiz_po_data_id
 where s_spabiz_po_data.s_spabiz_po_data_id is null
    or (s_spabiz_po_data.s_spabiz_po_data_id is not null
        and s_spabiz_po_data.dv_hash <> #s_spabiz_po_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_po_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_po_data @current_dv_batch_id

end
