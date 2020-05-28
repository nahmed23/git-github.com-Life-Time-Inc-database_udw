CREATE PROC [dbo].[proc_etl_spabiz_ro] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_RO

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_RO (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TYPE,
       NUM,
       VENDORID,
       Date,
       STAFFID,
       CHECKSTAFFID,
       STOCKSTAFFID,
       PACKNUM,
       STATUS,
       PAYMENT,
       DISCOUNT,
       TAX,
       TOTAL,
       POID,
       PONUM,
       SUBTOTAL,
       FREIGHT,
       RETAILTOTAL,
       TAXTYPE,
       STORE_NUMBER,
       INVDATE,
       INVNUMBER,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(ID as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       TYPE,
       NUM,
       VENDORID,
       Date,
       STAFFID,
       CHECKSTAFFID,
       STOCKSTAFFID,
       PACKNUM,
       STATUS,
       PAYMENT,
       DISCOUNT,
       TAX,
       TOTAL,
       POID,
       PONUM,
       SUBTOTAL,
       FREIGHT,
       RETAILTOTAL,
       TAXTYPE,
       STORE_NUMBER,
       INVDATE,
       INVNUMBER,
       isnull(cast(stage_spabiz_RO.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_RO
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_ro @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_ro (
       bk_hash,
       ro_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_RO.bk_hash,
       stage_hash_spabiz_RO.ID ro_id,
       stage_hash_spabiz_RO.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_RO.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_RO
  left join h_spabiz_ro
    on stage_hash_spabiz_RO.bk_hash = h_spabiz_ro.bk_hash
 where h_spabiz_ro_id is null
   and stage_hash_spabiz_RO.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_ro
if object_id('tempdb..#l_spabiz_ro_inserts') is not null drop table #l_spabiz_ro_inserts
create table #l_spabiz_ro_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_RO.bk_hash,
       stage_hash_spabiz_RO.ID ro_id,
       stage_hash_spabiz_RO.STOREID store_id,
       stage_hash_spabiz_RO.VENDORID vendor_id,
       stage_hash_spabiz_RO.STAFFID staff_id,
       stage_hash_spabiz_RO.CHECKSTAFFID check_staff_id,
       stage_hash_spabiz_RO.STOCKSTAFFID stock_staff_id,
       stage_hash_spabiz_RO.POID po_id,
       stage_hash_spabiz_RO.PONUM po_num,
       stage_hash_spabiz_RO.STORE_NUMBER store_number,
       stage_hash_spabiz_RO.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.VENDORID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.STAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.CHECKSTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.STOCKSTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.POID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_RO.PONUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.STORE_NUMBER as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_RO
 where stage_hash_spabiz_RO.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_ro records
set @insert_date_time = getdate()
insert into l_spabiz_ro (
       bk_hash,
       ro_id,
       store_id,
       vendor_id,
       staff_id,
       check_staff_id,
       stock_staff_id,
       po_id,
       po_num,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_ro_inserts.bk_hash,
       #l_spabiz_ro_inserts.ro_id,
       #l_spabiz_ro_inserts.store_id,
       #l_spabiz_ro_inserts.vendor_id,
       #l_spabiz_ro_inserts.staff_id,
       #l_spabiz_ro_inserts.check_staff_id,
       #l_spabiz_ro_inserts.stock_staff_id,
       #l_spabiz_ro_inserts.po_id,
       #l_spabiz_ro_inserts.po_num,
       #l_spabiz_ro_inserts.store_number,
       case when l_spabiz_ro.l_spabiz_ro_id is null then isnull(#l_spabiz_ro_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_ro_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_ro_inserts
  left join p_spabiz_ro
    on #l_spabiz_ro_inserts.bk_hash = p_spabiz_ro.bk_hash
   and p_spabiz_ro.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_ro
    on p_spabiz_ro.bk_hash = l_spabiz_ro.bk_hash
   and p_spabiz_ro.l_spabiz_ro_id = l_spabiz_ro.l_spabiz_ro_id
 where l_spabiz_ro.l_spabiz_ro_id is null
    or (l_spabiz_ro.l_spabiz_ro_id is not null
        and l_spabiz_ro.dv_hash <> #l_spabiz_ro_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_ro
if object_id('tempdb..#s_spabiz_ro_inserts') is not null drop table #s_spabiz_ro_inserts
create table #s_spabiz_ro_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_RO.bk_hash,
       stage_hash_spabiz_RO.ID ro_id,
       stage_hash_spabiz_RO.COUNTERID counter_id,
       stage_hash_spabiz_RO.EDITTIME edit_time,
       stage_hash_spabiz_RO.TYPE type,
       stage_hash_spabiz_RO.NUM num,
       stage_hash_spabiz_RO.Date date,
       stage_hash_spabiz_RO.PACKNUM pack_num,
       stage_hash_spabiz_RO.STATUS status,
       stage_hash_spabiz_RO.PAYMENT payment,
       stage_hash_spabiz_RO.DISCOUNT discount,
       stage_hash_spabiz_RO.TAX tax,
       stage_hash_spabiz_RO.TOTAL total,
       stage_hash_spabiz_RO.SUBTOTAL sub_total,
       stage_hash_spabiz_RO.FREIGHT freight,
       stage_hash_spabiz_RO.RETAILTOTAL retail_total,
       stage_hash_spabiz_RO.TAXTYPE tax_type,
       stage_hash_spabiz_RO.STORE_NUMBER store_number,
       stage_hash_spabiz_RO.INVDATE inv_date,
       stage_hash_spabiz_RO.INVNUMBER inv_number,
       stage_hash_spabiz_RO.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_RO.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.TYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_RO.NUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_RO.Date,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_RO.PACKNUM,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.STATUS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_RO.PAYMENT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.DISCOUNT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.TAX as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.TOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.SUBTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.FREIGHT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.RETAILTOTAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.TAXTYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_RO.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_RO.INVDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_RO.INVNUMBER,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_RO
 where stage_hash_spabiz_RO.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_ro records
set @insert_date_time = getdate()
insert into s_spabiz_ro (
       bk_hash,
       ro_id,
       counter_id,
       edit_time,
       type,
       num,
       date,
       pack_num,
       status,
       payment,
       discount,
       tax,
       total,
       sub_total,
       freight,
       retail_total,
       tax_type,
       store_number,
       inv_date,
       inv_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_ro_inserts.bk_hash,
       #s_spabiz_ro_inserts.ro_id,
       #s_spabiz_ro_inserts.counter_id,
       #s_spabiz_ro_inserts.edit_time,
       #s_spabiz_ro_inserts.type,
       #s_spabiz_ro_inserts.num,
       #s_spabiz_ro_inserts.date,
       #s_spabiz_ro_inserts.pack_num,
       #s_spabiz_ro_inserts.status,
       #s_spabiz_ro_inserts.payment,
       #s_spabiz_ro_inserts.discount,
       #s_spabiz_ro_inserts.tax,
       #s_spabiz_ro_inserts.total,
       #s_spabiz_ro_inserts.sub_total,
       #s_spabiz_ro_inserts.freight,
       #s_spabiz_ro_inserts.retail_total,
       #s_spabiz_ro_inserts.tax_type,
       #s_spabiz_ro_inserts.store_number,
       #s_spabiz_ro_inserts.inv_date,
       #s_spabiz_ro_inserts.inv_number,
       case when s_spabiz_ro.s_spabiz_ro_id is null then isnull(#s_spabiz_ro_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_ro_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_ro_inserts
  left join p_spabiz_ro
    on #s_spabiz_ro_inserts.bk_hash = p_spabiz_ro.bk_hash
   and p_spabiz_ro.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_ro
    on p_spabiz_ro.bk_hash = s_spabiz_ro.bk_hash
   and p_spabiz_ro.s_spabiz_ro_id = s_spabiz_ro.s_spabiz_ro_id
 where s_spabiz_ro.s_spabiz_ro_id is null
    or (s_spabiz_ro.s_spabiz_ro_id is not null
        and s_spabiz_ro.dv_hash <> #s_spabiz_ro_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_ro @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_ro @current_dv_batch_id

end
