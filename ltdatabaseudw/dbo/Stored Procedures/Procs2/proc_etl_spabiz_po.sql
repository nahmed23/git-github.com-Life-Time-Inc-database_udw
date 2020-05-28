CREATE PROC [dbo].[proc_etl_spabiz_po] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_PO

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_PO (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       NUM,
       VENDORID,
       Date,
       STAFFID,
       STATUS,
       PAYMENT,
       DISCOUNT,
       TAX,
       TOTAL,
       SORTBY,
       RETAILTOTAL,
       SUBTOTAL,
       DELETEDATE,
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
       NUM,
       VENDORID,
       Date,
       STAFFID,
       STATUS,
       PAYMENT,
       DISCOUNT,
       TAX,
       TOTAL,
       SORTBY,
       RETAILTOTAL,
       SUBTOTAL,
       DELETEDATE,
       STORE_NUMBER,
       isnull(cast(stage_spabiz_PO.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_PO
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_po @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_po (
       bk_hash,
       po_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_PO.bk_hash,
       stage_hash_spabiz_PO.ID po_id,
       stage_hash_spabiz_PO.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_PO.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_PO
  left join h_spabiz_po
    on stage_hash_spabiz_PO.bk_hash = h_spabiz_po.bk_hash
 where h_spabiz_po_id is null
   and stage_hash_spabiz_PO.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_po
if object_id('tempdb..#l_spabiz_po_inserts') is not null drop table #l_spabiz_po_inserts
create table #l_spabiz_po_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PO.bk_hash,
       stage_hash_spabiz_PO.ID po_id,
       stage_hash_spabiz_PO.STOREID store_id,
       stage_hash_spabiz_PO.VENDORID vendor_id,
       stage_hash_spabiz_PO.STAFFID staff_id,
       stage_hash_spabiz_PO.STORE_NUMBER store_number,
       stage_hash_spabiz_PO.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.VENDORID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PO
 where stage_hash_spabiz_PO.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_po records
set @insert_date_time = getdate()
insert into l_spabiz_po (
       bk_hash,
       po_id,
       store_id,
       vendor_id,
       staff_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_po_inserts.bk_hash,
       #l_spabiz_po_inserts.po_id,
       #l_spabiz_po_inserts.store_id,
       #l_spabiz_po_inserts.vendor_id,
       #l_spabiz_po_inserts.staff_id,
       #l_spabiz_po_inserts.store_number,
       case when l_spabiz_po.l_spabiz_po_id is null then isnull(#l_spabiz_po_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_po_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_po_inserts
  left join p_spabiz_po
    on #l_spabiz_po_inserts.bk_hash = p_spabiz_po.bk_hash
   and p_spabiz_po.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_po
    on p_spabiz_po.bk_hash = l_spabiz_po.bk_hash
   and p_spabiz_po.l_spabiz_po_id = l_spabiz_po.l_spabiz_po_id
 where l_spabiz_po.l_spabiz_po_id is null
    or (l_spabiz_po.l_spabiz_po_id is not null
        and l_spabiz_po.dv_hash <> #l_spabiz_po_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_po
if object_id('tempdb..#s_spabiz_po_inserts') is not null drop table #s_spabiz_po_inserts
create table #s_spabiz_po_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PO.bk_hash,
       stage_hash_spabiz_PO.ID po_id,
       stage_hash_spabiz_PO.COUNTERID counter_id,
       stage_hash_spabiz_PO.EDITTIME edit_time,
       stage_hash_spabiz_PO.NUM num,
       stage_hash_spabiz_PO.Date date,
       stage_hash_spabiz_PO.STATUS status,
       stage_hash_spabiz_PO.PAYMENT payment,
       stage_hash_spabiz_PO.DISCOUNT discount,
       stage_hash_spabiz_PO.TAX tax,
       stage_hash_spabiz_PO.TOTAL total,
       stage_hash_spabiz_PO.SORTBY sort_by,
       stage_hash_spabiz_PO.RETAILTOTAL retail_total,
       stage_hash_spabiz_PO.SUBTOTAL sub_total,
       stage_hash_spabiz_PO.DELETEDATE delete_date,
       stage_hash_spabiz_PO.STORE_NUMBER store_number,
       stage_hash_spabiz_PO.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PO.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PO.NUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PO.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PO.PAYMENT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.DISCOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.TAX as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.TOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.SORTBY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.RETAILTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.SUBTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PO.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PO.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PO
 where stage_hash_spabiz_PO.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_po records
set @insert_date_time = getdate()
insert into s_spabiz_po (
       bk_hash,
       po_id,
       counter_id,
       edit_time,
       num,
       date,
       status,
       payment,
       discount,
       tax,
       total,
       sort_by,
       retail_total,
       sub_total,
       delete_date,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_po_inserts.bk_hash,
       #s_spabiz_po_inserts.po_id,
       #s_spabiz_po_inserts.counter_id,
       #s_spabiz_po_inserts.edit_time,
       #s_spabiz_po_inserts.num,
       #s_spabiz_po_inserts.date,
       #s_spabiz_po_inserts.status,
       #s_spabiz_po_inserts.payment,
       #s_spabiz_po_inserts.discount,
       #s_spabiz_po_inserts.tax,
       #s_spabiz_po_inserts.total,
       #s_spabiz_po_inserts.sort_by,
       #s_spabiz_po_inserts.retail_total,
       #s_spabiz_po_inserts.sub_total,
       #s_spabiz_po_inserts.delete_date,
       #s_spabiz_po_inserts.store_number,
       case when s_spabiz_po.s_spabiz_po_id is null then isnull(#s_spabiz_po_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_po_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_po_inserts
  left join p_spabiz_po
    on #s_spabiz_po_inserts.bk_hash = p_spabiz_po.bk_hash
   and p_spabiz_po.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_po
    on p_spabiz_po.bk_hash = s_spabiz_po.bk_hash
   and p_spabiz_po.s_spabiz_po_id = s_spabiz_po.s_spabiz_po_id
 where s_spabiz_po.s_spabiz_po_id is null
    or (s_spabiz_po.s_spabiz_po_id is not null
        and s_spabiz_po.dv_hash <> #s_spabiz_po_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_po @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_po @current_dv_batch_id

end
