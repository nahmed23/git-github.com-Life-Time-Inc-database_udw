CREATE PROC [dbo].[proc_etl_magento_catalog_inventory_stock_item] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_cataloginventory_stock_item

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_cataloginventory_stock_item (
       bk_hash,
       item_id,
       product_id,
       stock_id,
       qty,
       min_qty,
       use_config_min_qty,
       is_qty_decimal,
       backorders,
       use_config_backorders,
       min_sale_qty,
       use_config_min_sale_qty,
       max_sale_qty,
       use_config_max_sale_qty,
       is_in_stock,
       low_stock_date,
       notify_stock_qty,
       use_config_notify_stock_qty,
       manage_stock,
       use_config_manage_stock,
       stock_status_changed_auto,
       use_config_qty_increments,
       qty_increments,
       use_config_enable_qty_inc,
       enable_qty_increments,
       is_decimal_divided,
       website_id,
       deferred_stock_update,
       use_config_deferred_stock_update,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(item_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       item_id,
       product_id,
       stock_id,
       qty,
       min_qty,
       use_config_min_qty,
       is_qty_decimal,
       backorders,
       use_config_backorders,
       min_sale_qty,
       use_config_min_sale_qty,
       max_sale_qty,
       use_config_max_sale_qty,
       is_in_stock,
       low_stock_date,
       notify_stock_qty,
       use_config_notify_stock_qty,
       manage_stock,
       use_config_manage_stock,
       stock_status_changed_auto,
       use_config_qty_increments,
       qty_increments,
       use_config_enable_qty_inc,
       enable_qty_increments,
       is_decimal_divided,
       website_id,
       deferred_stock_update,
       use_config_deferred_stock_update,
       dummy_modified_date_time,
       isnull(cast(stage_magento_cataloginventory_stock_item.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_cataloginventory_stock_item
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_catalog_inventory_stock_item @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_catalog_inventory_stock_item (
       bk_hash,
       item_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_cataloginventory_stock_item.bk_hash,
       stage_hash_magento_cataloginventory_stock_item.item_id item_id,
       isnull(cast(stage_hash_magento_cataloginventory_stock_item.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_cataloginventory_stock_item
  left join h_magento_catalog_inventory_stock_item
    on stage_hash_magento_cataloginventory_stock_item.bk_hash = h_magento_catalog_inventory_stock_item.bk_hash
 where h_magento_catalog_inventory_stock_item_id is null
   and stage_hash_magento_cataloginventory_stock_item.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_catalog_inventory_stock_item
if object_id('tempdb..#l_magento_catalog_inventory_stock_item_inserts') is not null drop table #l_magento_catalog_inventory_stock_item_inserts
create table #l_magento_catalog_inventory_stock_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_cataloginventory_stock_item.bk_hash,
       stage_hash_magento_cataloginventory_stock_item.item_id item_id,
       stage_hash_magento_cataloginventory_stock_item.product_id product_id,
       stage_hash_magento_cataloginventory_stock_item.stock_id stock_id,
       stage_hash_magento_cataloginventory_stock_item.website_id website_id,
       isnull(cast(stage_hash_magento_cataloginventory_stock_item.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.product_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.stock_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.website_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_cataloginventory_stock_item
 where stage_hash_magento_cataloginventory_stock_item.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_catalog_inventory_stock_item records
set @insert_date_time = getdate()
insert into l_magento_catalog_inventory_stock_item (
       bk_hash,
       item_id,
       product_id,
       stock_id,
       website_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_catalog_inventory_stock_item_inserts.bk_hash,
       #l_magento_catalog_inventory_stock_item_inserts.item_id,
       #l_magento_catalog_inventory_stock_item_inserts.product_id,
       #l_magento_catalog_inventory_stock_item_inserts.stock_id,
       #l_magento_catalog_inventory_stock_item_inserts.website_id,
       case when l_magento_catalog_inventory_stock_item.l_magento_catalog_inventory_stock_item_id is null then isnull(#l_magento_catalog_inventory_stock_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_catalog_inventory_stock_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_catalog_inventory_stock_item_inserts
  left join p_magento_catalog_inventory_stock_item
    on #l_magento_catalog_inventory_stock_item_inserts.bk_hash = p_magento_catalog_inventory_stock_item.bk_hash
   and p_magento_catalog_inventory_stock_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_catalog_inventory_stock_item
    on p_magento_catalog_inventory_stock_item.bk_hash = l_magento_catalog_inventory_stock_item.bk_hash
   and p_magento_catalog_inventory_stock_item.l_magento_catalog_inventory_stock_item_id = l_magento_catalog_inventory_stock_item.l_magento_catalog_inventory_stock_item_id
 where l_magento_catalog_inventory_stock_item.l_magento_catalog_inventory_stock_item_id is null
    or (l_magento_catalog_inventory_stock_item.l_magento_catalog_inventory_stock_item_id is not null
        and l_magento_catalog_inventory_stock_item.dv_hash <> #l_magento_catalog_inventory_stock_item_inserts.source_hash)

--calculate hash and lookup to current s_magento_catalog_inventory_stock_item
if object_id('tempdb..#s_magento_catalog_inventory_stock_item_inserts') is not null drop table #s_magento_catalog_inventory_stock_item_inserts
create table #s_magento_catalog_inventory_stock_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_cataloginventory_stock_item.bk_hash,
       stage_hash_magento_cataloginventory_stock_item.item_id item_id,
       stage_hash_magento_cataloginventory_stock_item.qty qty,
       stage_hash_magento_cataloginventory_stock_item.min_qty min_qty,
       stage_hash_magento_cataloginventory_stock_item.use_config_min_qty use_config_min_qty,
       stage_hash_magento_cataloginventory_stock_item.is_qty_decimal is_qty_decimal,
       stage_hash_magento_cataloginventory_stock_item.backorders backorders,
       stage_hash_magento_cataloginventory_stock_item.use_config_backorders use_config_backorders,
       stage_hash_magento_cataloginventory_stock_item.min_sale_qty min_sale_qty,
       stage_hash_magento_cataloginventory_stock_item.use_config_min_sale_qty use_config_min_sale_qty,
       stage_hash_magento_cataloginventory_stock_item.max_sale_qty max_sale_qty,
       stage_hash_magento_cataloginventory_stock_item.use_config_max_sale_qty use_config_max_sale_qty,
       stage_hash_magento_cataloginventory_stock_item.is_in_stock is_in_stock,
       stage_hash_magento_cataloginventory_stock_item.low_stock_date low_stock_date,
       stage_hash_magento_cataloginventory_stock_item.notify_stock_qty notify_stock_qty,
       stage_hash_magento_cataloginventory_stock_item.use_config_notify_stock_qty use_config_notify_stock_qty,
       stage_hash_magento_cataloginventory_stock_item.manage_stock manage_stock,
       stage_hash_magento_cataloginventory_stock_item.use_config_manage_stock use_config_manage_stock,
       stage_hash_magento_cataloginventory_stock_item.stock_status_changed_auto stock_status_changed_auto,
       stage_hash_magento_cataloginventory_stock_item.use_config_qty_increments use_config_qty_increments,
       stage_hash_magento_cataloginventory_stock_item.qty_increments qty_increments,
       stage_hash_magento_cataloginventory_stock_item.use_config_enable_qty_inc use_config_enable_qty_inc,
       stage_hash_magento_cataloginventory_stock_item.enable_qty_increments enable_qty_increments,
       stage_hash_magento_cataloginventory_stock_item.is_decimal_divided is_decimal_divided,
       stage_hash_magento_cataloginventory_stock_item.deferred_stock_update deferred_stock_update,
       stage_hash_magento_cataloginventory_stock_item.use_config_deferred_stock_update use_config_deferred_stock_update,
       stage_hash_magento_cataloginventory_stock_item.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_cataloginventory_stock_item.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.min_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_min_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.is_qty_decimal as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.backorders as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_backorders as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.min_sale_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_min_sale_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.max_sale_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_max_sale_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.is_in_stock as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_magento_cataloginventory_stock_item.low_stock_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.notify_stock_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_notify_stock_qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.manage_stock as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_manage_stock as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.stock_status_changed_auto as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_qty_increments as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.qty_increments as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_enable_qty_inc as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.enable_qty_increments as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.is_decimal_divided as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.deferred_stock_update as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_item.use_config_deferred_stock_update as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_cataloginventory_stock_item
 where stage_hash_magento_cataloginventory_stock_item.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_catalog_inventory_stock_item records
set @insert_date_time = getdate()
insert into s_magento_catalog_inventory_stock_item (
       bk_hash,
       item_id,
       qty,
       min_qty,
       use_config_min_qty,
       is_qty_decimal,
       backorders,
       use_config_backorders,
       min_sale_qty,
       use_config_min_sale_qty,
       max_sale_qty,
       use_config_max_sale_qty,
       is_in_stock,
       low_stock_date,
       notify_stock_qty,
       use_config_notify_stock_qty,
       manage_stock,
       use_config_manage_stock,
       stock_status_changed_auto,
       use_config_qty_increments,
       qty_increments,
       use_config_enable_qty_inc,
       enable_qty_increments,
       is_decimal_divided,
       deferred_stock_update,
       use_config_deferred_stock_update,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_catalog_inventory_stock_item_inserts.bk_hash,
       #s_magento_catalog_inventory_stock_item_inserts.item_id,
       #s_magento_catalog_inventory_stock_item_inserts.qty,
       #s_magento_catalog_inventory_stock_item_inserts.min_qty,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_min_qty,
       #s_magento_catalog_inventory_stock_item_inserts.is_qty_decimal,
       #s_magento_catalog_inventory_stock_item_inserts.backorders,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_backorders,
       #s_magento_catalog_inventory_stock_item_inserts.min_sale_qty,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_min_sale_qty,
       #s_magento_catalog_inventory_stock_item_inserts.max_sale_qty,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_max_sale_qty,
       #s_magento_catalog_inventory_stock_item_inserts.is_in_stock,
       #s_magento_catalog_inventory_stock_item_inserts.low_stock_date,
       #s_magento_catalog_inventory_stock_item_inserts.notify_stock_qty,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_notify_stock_qty,
       #s_magento_catalog_inventory_stock_item_inserts.manage_stock,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_manage_stock,
       #s_magento_catalog_inventory_stock_item_inserts.stock_status_changed_auto,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_qty_increments,
       #s_magento_catalog_inventory_stock_item_inserts.qty_increments,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_enable_qty_inc,
       #s_magento_catalog_inventory_stock_item_inserts.enable_qty_increments,
       #s_magento_catalog_inventory_stock_item_inserts.is_decimal_divided,
       #s_magento_catalog_inventory_stock_item_inserts.deferred_stock_update,
       #s_magento_catalog_inventory_stock_item_inserts.use_config_deferred_stock_update,
       #s_magento_catalog_inventory_stock_item_inserts.dummy_modified_date_time,
       case when s_magento_catalog_inventory_stock_item.s_magento_catalog_inventory_stock_item_id is null then isnull(#s_magento_catalog_inventory_stock_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_catalog_inventory_stock_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_catalog_inventory_stock_item_inserts
  left join p_magento_catalog_inventory_stock_item
    on #s_magento_catalog_inventory_stock_item_inserts.bk_hash = p_magento_catalog_inventory_stock_item.bk_hash
   and p_magento_catalog_inventory_stock_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_catalog_inventory_stock_item
    on p_magento_catalog_inventory_stock_item.bk_hash = s_magento_catalog_inventory_stock_item.bk_hash
   and p_magento_catalog_inventory_stock_item.s_magento_catalog_inventory_stock_item_id = s_magento_catalog_inventory_stock_item.s_magento_catalog_inventory_stock_item_id
 where s_magento_catalog_inventory_stock_item.s_magento_catalog_inventory_stock_item_id is null
    or (s_magento_catalog_inventory_stock_item.s_magento_catalog_inventory_stock_item_id is not null
        and s_magento_catalog_inventory_stock_item.dv_hash <> #s_magento_catalog_inventory_stock_item_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_catalog_inventory_stock_item @current_dv_batch_id

end
