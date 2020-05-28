CREATE PROC [dbo].[proc_etl_spabiz_ro_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_RODATA

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_RODATA (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       Date,
       ROID,
       POID,
       POLINEID,
       LINENUM,
       VENDORID,
       PRODUCTID,
       COST,
       EXTCOST,
       QTYREC,
       QTYBO,
       STATUS,
       TYPE,
       CATID,
       MARGIN,
       RETAILPRICE,
       AMOUNT,
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
       [Delete],
       DELETEDATE,
       Date,
       ROID,
       POID,
       POLINEID,
       LINENUM,
       VENDORID,
       PRODUCTID,
       COST,
       EXTCOST,
       QTYREC,
       QTYBO,
       STATUS,
       TYPE,
       CATID,
       MARGIN,
       RETAILPRICE,
       AMOUNT,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_RODATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_RODATA
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ro_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ro_data (
       bk_hash,
       ro_data_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_RODATA.bk_hash,
       stage_hash_spabiz_RODATA.ID ro_data_id,
       stage_hash_spabiz_RODATA.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_RODATA.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_RODATA
  left join h_spabiz_ro_data
    on stage_hash_spabiz_RODATA.bk_hash = h_spabiz_ro_data.bk_hash
 where h_spabiz_ro_data_id is null
   and stage_hash_spabiz_RODATA.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ro_data
if object_id('tempdb..#l_spabiz_ro_data_inserts') is not null drop table #l_spabiz_ro_data_inserts
create table #l_spabiz_ro_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_RODATA.bk_hash,
       stage_hash_spabiz_RODATA.ID ro_data_id,
       stage_hash_spabiz_RODATA.STOREID store_id,
       stage_hash_spabiz_RODATA.ROID ro_id,
       stage_hash_spabiz_RODATA.POID po_id,
       stage_hash_spabiz_RODATA.POLINEID po_line_id,
       stage_hash_spabiz_RODATA.VENDORID vendor_id,
       stage_hash_spabiz_RODATA.PRODUCTID product_id,
       stage_hash_spabiz_RODATA.CATID cat_id,
       stage_hash_spabiz_RODATA.STORE_NUMBER store_number,
       stage_hash_spabiz_RODATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.ROID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.POID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.POLINEID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.VENDORID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.PRODUCTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.CATID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_RODATA
 where stage_hash_spabiz_RODATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ro_data records
set @insert_date_time = getdate()
insert into l_spabiz_ro_data (
       bk_hash,
       ro_data_id,
       store_id,
       ro_id,
       po_id,
       po_line_id,
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
select #l_spabiz_ro_data_inserts.bk_hash,
       #l_spabiz_ro_data_inserts.ro_data_id,
       #l_spabiz_ro_data_inserts.store_id,
       #l_spabiz_ro_data_inserts.ro_id,
       #l_spabiz_ro_data_inserts.po_id,
       #l_spabiz_ro_data_inserts.po_line_id,
       #l_spabiz_ro_data_inserts.vendor_id,
       #l_spabiz_ro_data_inserts.product_id,
       #l_spabiz_ro_data_inserts.cat_id,
       #l_spabiz_ro_data_inserts.store_number,
       case when l_spabiz_ro_data.l_spabiz_ro_data_id is null then isnull(#l_spabiz_ro_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ro_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ro_data_inserts
  left join p_spabiz_ro_data
    on #l_spabiz_ro_data_inserts.bk_hash = p_spabiz_ro_data.bk_hash
   and p_spabiz_ro_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ro_data
    on p_spabiz_ro_data.bk_hash = l_spabiz_ro_data.bk_hash
   and p_spabiz_ro_data.l_spabiz_ro_data_id = l_spabiz_ro_data.l_spabiz_ro_data_id
 where l_spabiz_ro_data.l_spabiz_ro_data_id is null
    or (l_spabiz_ro_data.l_spabiz_ro_data_id is not null
        and l_spabiz_ro_data.dv_hash <> #l_spabiz_ro_data_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ro_data
if object_id('tempdb..#s_spabiz_ro_data_inserts') is not null drop table #s_spabiz_ro_data_inserts
create table #s_spabiz_ro_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_RODATA.bk_hash,
       stage_hash_spabiz_RODATA.ID ro_data_id,
       stage_hash_spabiz_RODATA.COUNTERID counter_id,
       stage_hash_spabiz_RODATA.EDITTIME edit_time,
       stage_hash_spabiz_RODATA.[Delete] ro_data_delete,
       stage_hash_spabiz_RODATA.DELETEDATE delete_date,
       stage_hash_spabiz_RODATA.Date date,
       stage_hash_spabiz_RODATA.LINENUM line_num,
       stage_hash_spabiz_RODATA.COST cost,
       stage_hash_spabiz_RODATA.EXTCOST ext_cost,
       stage_hash_spabiz_RODATA.QTYREC qty_rec,
       stage_hash_spabiz_RODATA.QTYBO qty_bo,
       stage_hash_spabiz_RODATA.STATUS status,
       stage_hash_spabiz_RODATA.TYPE type,
       stage_hash_spabiz_RODATA.MARGIN margin,
       stage_hash_spabiz_RODATA.RETAILPRICE retail_price,
       stage_hash_spabiz_RODATA.AMOUNT amount,
       stage_hash_spabiz_RODATA.STORE_NUMBER store_number,
       stage_hash_spabiz_RODATA.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_RODATA.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_RODATA.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_RODATA.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.LINENUM as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.COST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.EXTCOST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.QTYREC as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.QTYBO as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.TYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.MARGIN as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.AMOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RODATA.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_RODATA
 where stage_hash_spabiz_RODATA.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ro_data records
set @insert_date_time = getdate()
insert into s_spabiz_ro_data (
       bk_hash,
       ro_data_id,
       counter_id,
       edit_time,
       ro_data_delete,
       delete_date,
       date,
       line_num,
       cost,
       ext_cost,
       qty_rec,
       qty_bo,
       status,
       type,
       margin,
       retail_price,
       amount,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ro_data_inserts.bk_hash,
       #s_spabiz_ro_data_inserts.ro_data_id,
       #s_spabiz_ro_data_inserts.counter_id,
       #s_spabiz_ro_data_inserts.edit_time,
       #s_spabiz_ro_data_inserts.ro_data_delete,
       #s_spabiz_ro_data_inserts.delete_date,
       #s_spabiz_ro_data_inserts.date,
       #s_spabiz_ro_data_inserts.line_num,
       #s_spabiz_ro_data_inserts.cost,
       #s_spabiz_ro_data_inserts.ext_cost,
       #s_spabiz_ro_data_inserts.qty_rec,
       #s_spabiz_ro_data_inserts.qty_bo,
       #s_spabiz_ro_data_inserts.status,
       #s_spabiz_ro_data_inserts.type,
       #s_spabiz_ro_data_inserts.margin,
       #s_spabiz_ro_data_inserts.retail_price,
       #s_spabiz_ro_data_inserts.amount,
       #s_spabiz_ro_data_inserts.store_number,
       case when s_spabiz_ro_data.s_spabiz_ro_data_id is null then isnull(#s_spabiz_ro_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ro_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ro_data_inserts
  left join p_spabiz_ro_data
    on #s_spabiz_ro_data_inserts.bk_hash = p_spabiz_ro_data.bk_hash
   and p_spabiz_ro_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ro_data
    on p_spabiz_ro_data.bk_hash = s_spabiz_ro_data.bk_hash
   and p_spabiz_ro_data.s_spabiz_ro_data_id = s_spabiz_ro_data.s_spabiz_ro_data_id
 where s_spabiz_ro_data.s_spabiz_ro_data_id is null
    or (s_spabiz_ro_data.s_spabiz_ro_data_id is not null
        and s_spabiz_ro_data.dv_hash <> #s_spabiz_ro_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ro_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ro_data @current_dv_batch_id

end
