CREATE PROC [dbo].[proc_etl_spabiz_product] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_PRODUCT

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_PRODUCT (
       bk_hash,
       ID,
       COUNTERID,
       STOREID,
       EDITTIME,
       [Delete],
       DELETEDATE,
       NAME,
       QUICKID,
       VENDORID,
       VENDORCODE,
       MANID,
       MANCODE,
       COST,
       CURRENTCOST,
       CURRENTLAYER,
       RETAILPRICE,
       COMMISSIONID,
       TAXABLE,
       TYPE,
       PURCHASETAX,
       DATECREATED,
       WIDTH,
       HEIGHT,
       DEPTH,
       WEIGHT,
       ORDERFREQ,
       CASEQTY,
       DEPTCAT,
       SEARCHCAT,
       LOCATION,
       EOQ,
       SEASONAL,
       MIN,
       MAX,
       LABELNAME,
       PRINTLABELS,
       COST2,
       COST2QTY,
       COST3,
       COST3QTY,
       CURRENTQTY,
       PRINTONTICKET,
       STATUS,
       LABELS,
       ONORDER,
       NOTE,
       LASTSOLD,
       LASTCOUNT,
       STORERANK,
       MINMAXID,
       STOCKLEVEL,
       QUARTERSALES,
       MINDAYS,
       MAXDAYS,
       LASTPURCHASE,
       AVGCOST,
       DEFAULTSTAFFID,
       ACTIVE,
       STORE_NUMBER,
       GLACCOUNT,
       ENTERID,
       LINKID,
       GUID_LINK,
       GUID_SOURCEID,
       PRODUCTLEVEL,
       PARENTID,
       NEWID,
       PRODUCTBACKUPID,
       BACKUPPRODID,
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
       VENDORID,
       VENDORCODE,
       MANID,
       MANCODE,
       COST,
       CURRENTCOST,
       CURRENTLAYER,
       RETAILPRICE,
       COMMISSIONID,
       TAXABLE,
       TYPE,
       PURCHASETAX,
       DATECREATED,
       WIDTH,
       HEIGHT,
       DEPTH,
       WEIGHT,
       ORDERFREQ,
       CASEQTY,
       DEPTCAT,
       SEARCHCAT,
       LOCATION,
       EOQ,
       SEASONAL,
       MIN,
       MAX,
       LABELNAME,
       PRINTLABELS,
       COST2,
       COST2QTY,
       COST3,
       COST3QTY,
       CURRENTQTY,
       PRINTONTICKET,
       STATUS,
       LABELS,
       ONORDER,
       NOTE,
       LASTSOLD,
       LASTCOUNT,
       STORERANK,
       MINMAXID,
       STOCKLEVEL,
       QUARTERSALES,
       MINDAYS,
       MAXDAYS,
       LASTPURCHASE,
       AVGCOST,
       DEFAULTSTAFFID,
       ACTIVE,
       STORE_NUMBER,
       GLACCOUNT,
       ENTERID,
       LINKID,
       GUID_LINK,
       GUID_SOURCEID,
       PRODUCTLEVEL,
       PARENTID,
       NEWID,
       PRODUCTBACKUPID,
       BACKUPPRODID,
       isnull(cast(stage_spabiz_PRODUCT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_PRODUCT
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_product @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_product (
       bk_hash,
       product_id,
       store_number,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_PRODUCT.bk_hash,
       stage_hash_spabiz_PRODUCT.ID product_id,
       stage_hash_spabiz_PRODUCT.STORE_NUMBER store_number,
       isnull(cast(stage_hash_spabiz_PRODUCT.EDITTIME as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_PRODUCT
  left join h_spabiz_product
    on stage_hash_spabiz_PRODUCT.bk_hash = h_spabiz_product.bk_hash
 where h_spabiz_product_id is null
   and stage_hash_spabiz_PRODUCT.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_spabiz_product
if object_id('tempdb..#l_spabiz_product_inserts') is not null drop table #l_spabiz_product_inserts
create table #l_spabiz_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PRODUCT.bk_hash,
       stage_hash_spabiz_PRODUCT.ID product_id,
       stage_hash_spabiz_PRODUCT.STOREID store_id,
       stage_hash_spabiz_PRODUCT.VENDORID vendor_id,
       stage_hash_spabiz_PRODUCT.MANID man_id,
       stage_hash_spabiz_PRODUCT.COMMISSIONID commission_id,
       stage_hash_spabiz_PRODUCT.DEPTCAT dept_cat,
       stage_hash_spabiz_PRODUCT.SEARCHCAT search_cat,
       stage_hash_spabiz_PRODUCT.MINMAXID min_max_id,
       stage_hash_spabiz_PRODUCT.DEFAULTSTAFFID default_staff_id,
       stage_hash_spabiz_PRODUCT.STORE_NUMBER store_number,
       stage_hash_spabiz_PRODUCT.GLACCOUNT gl_account,
       stage_hash_spabiz_PRODUCT.ENTERID enter_id,
       stage_hash_spabiz_PRODUCT.LINKID link_id,
       stage_hash_spabiz_PRODUCT.PARENTID parent_id,
       stage_hash_spabiz_PRODUCT.PRODUCTBACKUPID product_backup_id,
       stage_hash_spabiz_PRODUCT.BACKUPPRODID backup_prod_id,
       stage_hash_spabiz_PRODUCT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.STOREID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.VENDORID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.MANID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.COMMISSIONID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.DEPTCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.SEARCHCAT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.MINMAXID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.DEFAULTSTAFFID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.GLACCOUNT,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.ENTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.LINKID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.PARENTID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.PRODUCTBACKUPID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.BACKUPPRODID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PRODUCT
 where stage_hash_spabiz_PRODUCT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_spabiz_product records
set @insert_date_time = getdate()
insert into l_spabiz_product (
       bk_hash,
       product_id,
       store_id,
       vendor_id,
       man_id,
       commission_id,
       dept_cat,
       search_cat,
       min_max_id,
       default_staff_id,
       store_number,
       gl_account,
       enter_id,
       link_id,
       parent_id,
       product_backup_id,
       backup_prod_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_spabiz_product_inserts.bk_hash,
       #l_spabiz_product_inserts.product_id,
       #l_spabiz_product_inserts.store_id,
       #l_spabiz_product_inserts.vendor_id,
       #l_spabiz_product_inserts.man_id,
       #l_spabiz_product_inserts.commission_id,
       #l_spabiz_product_inserts.dept_cat,
       #l_spabiz_product_inserts.search_cat,
       #l_spabiz_product_inserts.min_max_id,
       #l_spabiz_product_inserts.default_staff_id,
       #l_spabiz_product_inserts.store_number,
       #l_spabiz_product_inserts.gl_account,
       #l_spabiz_product_inserts.enter_id,
       #l_spabiz_product_inserts.link_id,
       #l_spabiz_product_inserts.parent_id,
       #l_spabiz_product_inserts.product_backup_id,
       #l_spabiz_product_inserts.backup_prod_id,
       case when l_spabiz_product.l_spabiz_product_id is null then isnull(#l_spabiz_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #l_spabiz_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_spabiz_product_inserts
  left join p_spabiz_product
    on #l_spabiz_product_inserts.bk_hash = p_spabiz_product.bk_hash
   and p_spabiz_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_spabiz_product
    on p_spabiz_product.bk_hash = l_spabiz_product.bk_hash
   and p_spabiz_product.l_spabiz_product_id = l_spabiz_product.l_spabiz_product_id
 where l_spabiz_product.l_spabiz_product_id is null
    or (l_spabiz_product.l_spabiz_product_id is not null
        and l_spabiz_product.dv_hash <> #l_spabiz_product_inserts.source_hash)

--calculate hash and lookup to current s_spabiz_product
if object_id('tempdb..#s_spabiz_product_inserts') is not null drop table #s_spabiz_product_inserts
create table #s_spabiz_product_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_PRODUCT.bk_hash,
       stage_hash_spabiz_PRODUCT.ID product_id,
       stage_hash_spabiz_PRODUCT.COUNTERID counter_id,
       stage_hash_spabiz_PRODUCT.EDITTIME edit_time,
       stage_hash_spabiz_PRODUCT.[Delete] product_delete,
       stage_hash_spabiz_PRODUCT.DELETEDATE delete_date,
       stage_hash_spabiz_PRODUCT.NAME name,
       stage_hash_spabiz_PRODUCT.QUICKID quick_id,
       stage_hash_spabiz_PRODUCT.VENDORCODE vendor_code,
       stage_hash_spabiz_PRODUCT.MANCODE man_code,
       stage_hash_spabiz_PRODUCT.COST cost,
       stage_hash_spabiz_PRODUCT.CURRENTCOST current_cost,
       stage_hash_spabiz_PRODUCT.CURRENTLAYER current_layer,
       stage_hash_spabiz_PRODUCT.RETAILPRICE retail_price,
       stage_hash_spabiz_PRODUCT.TAXABLE taxable,
       stage_hash_spabiz_PRODUCT.TYPE type,
       stage_hash_spabiz_PRODUCT.PURCHASETAX purchase_tax,
       stage_hash_spabiz_PRODUCT.DATECREATED date_created,
       stage_hash_spabiz_PRODUCT.WIDTH width,
       stage_hash_spabiz_PRODUCT.HEIGHT height,
       stage_hash_spabiz_PRODUCT.DEPTH depth,
       stage_hash_spabiz_PRODUCT.WEIGHT weight,
       stage_hash_spabiz_PRODUCT.ORDERFREQ order_freq,
       stage_hash_spabiz_PRODUCT.CASEQTY case_qty,
       stage_hash_spabiz_PRODUCT.LOCATION location,
       stage_hash_spabiz_PRODUCT.EOQ eoq,
       stage_hash_spabiz_PRODUCT.SEASONAL seasonal,
       stage_hash_spabiz_PRODUCT.MIN min,
       stage_hash_spabiz_PRODUCT.MAX max,
       stage_hash_spabiz_PRODUCT.LABELNAME label_name,
       stage_hash_spabiz_PRODUCT.PRINTLABELS print_labels,
       stage_hash_spabiz_PRODUCT.COST2 cost2,
       stage_hash_spabiz_PRODUCT.COST2QTY cost2_qty,
       stage_hash_spabiz_PRODUCT.COST3 cost3,
       stage_hash_spabiz_PRODUCT.COST3QTY cost3_qty,
       stage_hash_spabiz_PRODUCT.CURRENTQTY current_qty,
       stage_hash_spabiz_PRODUCT.PRINTONTICKET print_on_ticket,
       stage_hash_spabiz_PRODUCT.STATUS status,
       stage_hash_spabiz_PRODUCT.LABELS labels,
       stage_hash_spabiz_PRODUCT.ONORDER on_order,
       stage_hash_spabiz_PRODUCT.NOTE note,
       stage_hash_spabiz_PRODUCT.LASTSOLD last_sold,
       stage_hash_spabiz_PRODUCT.LASTCOUNT last_count,
       stage_hash_spabiz_PRODUCT.STORERANK store_rank,
       stage_hash_spabiz_PRODUCT.STOCKLEVEL stock_level,
       stage_hash_spabiz_PRODUCT.QUARTERSALES quarter_sales,
       stage_hash_spabiz_PRODUCT.MINDAYS min_days,
       stage_hash_spabiz_PRODUCT.MAXDAYS max_days,
       stage_hash_spabiz_PRODUCT.LASTPURCHASE last_purchase,
       stage_hash_spabiz_PRODUCT.AVGCOST avg_cost,
       stage_hash_spabiz_PRODUCT.ACTIVE active,
       stage_hash_spabiz_PRODUCT.STORE_NUMBER store_number,
       stage_hash_spabiz_PRODUCT.GUID_LINK guid_link,
       stage_hash_spabiz_PRODUCT.GUID_SOURCEID guid_source_id,
       stage_hash_spabiz_PRODUCT.PRODUCTLEVEL product_level,
       stage_hash_spabiz_PRODUCT.NEWID new_id,
       stage_hash_spabiz_PRODUCT.EDITTIME dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.ID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.COUNTERID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PRODUCT.EDITTIME,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.[Delete] as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PRODUCT.DELETEDATE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.NAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.QUICKID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.VENDORCODE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.MANCODE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.COST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.CURRENTCOST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.CURRENTLAYER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.RETAILPRICE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.TAXABLE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.TYPE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.PURCHASETAX as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PRODUCT.DATECREATED,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.WIDTH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.HEIGHT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.DEPTH as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.WEIGHT as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.ORDERFREQ as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.CASEQTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.LOCATION,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.EOQ as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.SEASONAL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.MIN as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.MAX as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.LABELNAME,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.PRINTLABELS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.COST2 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.COST2QTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.COST3 as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.COST3QTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.CURRENTQTY as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.PRINTONTICKET,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.STATUS,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.LABELS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.ONORDER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.NOTE,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PRODUCT.LASTSOLD,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PRODUCT.LASTCOUNT,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.STORERANK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.STOCKLEVEL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.QUARTERSALES as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.MINDAYS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.MAXDAYS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_PRODUCT.LASTPURCHASE,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.AVGCOST as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.ACTIVE as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.STORE_NUMBER as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.GUID_LINK,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_PRODUCT.GUID_SOURCEID,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.PRODUCTLEVEL as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_spabiz_PRODUCT.NEWID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_PRODUCT
 where stage_hash_spabiz_PRODUCT.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_product records
set @insert_date_time = getdate()
insert into s_spabiz_product (
       bk_hash,
       product_id,
       counter_id,
       edit_time,
       product_delete,
       delete_date,
       name,
       quick_id,
       vendor_code,
       man_code,
       cost,
       current_cost,
       current_layer,
       retail_price,
       taxable,
       type,
       purchase_tax,
       date_created,
       width,
       height,
       depth,
       weight,
       order_freq,
       case_qty,
       location,
       eoq,
       seasonal,
       min,
       max,
       label_name,
       print_labels,
       cost2,
       cost2_qty,
       cost3,
       cost3_qty,
       current_qty,
       print_on_ticket,
       status,
       labels,
       on_order,
       note,
       last_sold,
       last_count,
       store_rank,
       stock_level,
       quarter_sales,
       min_days,
       max_days,
       last_purchase,
       avg_cost,
       active,
       store_number,
       guid_link,
       guid_source_id,
       product_level,
       new_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_product_inserts.bk_hash,
       #s_spabiz_product_inserts.product_id,
       #s_spabiz_product_inserts.counter_id,
       #s_spabiz_product_inserts.edit_time,
       #s_spabiz_product_inserts.product_delete,
       #s_spabiz_product_inserts.delete_date,
       #s_spabiz_product_inserts.name,
       #s_spabiz_product_inserts.quick_id,
       #s_spabiz_product_inserts.vendor_code,
       #s_spabiz_product_inserts.man_code,
       #s_spabiz_product_inserts.cost,
       #s_spabiz_product_inserts.current_cost,
       #s_spabiz_product_inserts.current_layer,
       #s_spabiz_product_inserts.retail_price,
       #s_spabiz_product_inserts.taxable,
       #s_spabiz_product_inserts.type,
       #s_spabiz_product_inserts.purchase_tax,
       #s_spabiz_product_inserts.date_created,
       #s_spabiz_product_inserts.width,
       #s_spabiz_product_inserts.height,
       #s_spabiz_product_inserts.depth,
       #s_spabiz_product_inserts.weight,
       #s_spabiz_product_inserts.order_freq,
       #s_spabiz_product_inserts.case_qty,
       #s_spabiz_product_inserts.location,
       #s_spabiz_product_inserts.eoq,
       #s_spabiz_product_inserts.seasonal,
       #s_spabiz_product_inserts.min,
       #s_spabiz_product_inserts.max,
       #s_spabiz_product_inserts.label_name,
       #s_spabiz_product_inserts.print_labels,
       #s_spabiz_product_inserts.cost2,
       #s_spabiz_product_inserts.cost2_qty,
       #s_spabiz_product_inserts.cost3,
       #s_spabiz_product_inserts.cost3_qty,
       #s_spabiz_product_inserts.current_qty,
       #s_spabiz_product_inserts.print_on_ticket,
       #s_spabiz_product_inserts.status,
       #s_spabiz_product_inserts.labels,
       #s_spabiz_product_inserts.on_order,
       #s_spabiz_product_inserts.note,
       #s_spabiz_product_inserts.last_sold,
       #s_spabiz_product_inserts.last_count,
       #s_spabiz_product_inserts.store_rank,
       #s_spabiz_product_inserts.stock_level,
       #s_spabiz_product_inserts.quarter_sales,
       #s_spabiz_product_inserts.min_days,
       #s_spabiz_product_inserts.max_days,
       #s_spabiz_product_inserts.last_purchase,
       #s_spabiz_product_inserts.avg_cost,
       #s_spabiz_product_inserts.active,
       #s_spabiz_product_inserts.store_number,
       #s_spabiz_product_inserts.guid_link,
       #s_spabiz_product_inserts.guid_source_id,
       #s_spabiz_product_inserts.product_level,
       #s_spabiz_product_inserts.new_id,
       case when s_spabiz_product.s_spabiz_product_id is null then isnull(#s_spabiz_product_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_product_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_product_inserts
  left join p_spabiz_product
    on #s_spabiz_product_inserts.bk_hash = p_spabiz_product.bk_hash
   and p_spabiz_product.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_product
    on p_spabiz_product.bk_hash = s_spabiz_product.bk_hash
   and p_spabiz_product.s_spabiz_product_id = s_spabiz_product.s_spabiz_product_id
 where s_spabiz_product.s_spabiz_product_id is null
    or (s_spabiz_product.s_spabiz_product_id is not null
        and s_spabiz_product.dv_hash <> #s_spabiz_product_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_product @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_product @current_dv_batch_id

end
