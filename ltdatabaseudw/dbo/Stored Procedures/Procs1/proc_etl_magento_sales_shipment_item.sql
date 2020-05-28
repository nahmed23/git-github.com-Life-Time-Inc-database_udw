﻿CREATE PROC [dbo].[proc_etl_magento_sales_shipment_item] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_sales_Shipment_Item

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_sales_Shipment_Item (
       bk_hash,
       entity_id,
       parent_id,
       row_total,
       price,
       weight,
       qty,
       product_id,
       order_item_id,
       additional_data,
       description,
       name,
       sku,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(entity_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       entity_id,
       parent_id,
       row_total,
       price,
       weight,
       qty,
       product_id,
       order_item_id,
       additional_data,
       description,
       name,
       sku,
       dummy_modified_date_time,
       isnull(cast(stage_magento_sales_Shipment_Item.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_sales_Shipment_Item
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_sales_shipment_item @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_sales_shipment_item (
       bk_hash,
       entity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_sales_Shipment_Item.bk_hash,
       stage_hash_magento_sales_Shipment_Item.entity_id entity_id,
       isnull(cast(stage_hash_magento_sales_Shipment_Item.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_sales_Shipment_Item
  left join h_magento_sales_shipment_item
    on stage_hash_magento_sales_Shipment_Item.bk_hash = h_magento_sales_shipment_item.bk_hash
 where h_magento_sales_shipment_item_id is null
   and stage_hash_magento_sales_Shipment_Item.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_sales_shipment_item
if object_id('tempdb..#l_magento_sales_shipment_item_inserts') is not null drop table #l_magento_sales_shipment_item_inserts
create table #l_magento_sales_shipment_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_Shipment_Item.bk_hash,
       stage_hash_magento_sales_Shipment_Item.entity_id entity_id,
       stage_hash_magento_sales_Shipment_Item.parent_id parent_id,
       stage_hash_magento_sales_Shipment_Item.product_id product_id,
       stage_hash_magento_sales_Shipment_Item.order_item_id order_item_id,
       stage_hash_magento_sales_Shipment_Item.sku sku,
       isnull(cast(stage_hash_magento_sales_Shipment_Item.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.parent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.product_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.order_item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_Shipment_Item.sku,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_Shipment_Item
 where stage_hash_magento_sales_Shipment_Item.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_sales_shipment_item records
set @insert_date_time = getdate()
insert into l_magento_sales_shipment_item (
       bk_hash,
       entity_id,
       parent_id,
       product_id,
       order_item_id,
       sku,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_sales_shipment_item_inserts.bk_hash,
       #l_magento_sales_shipment_item_inserts.entity_id,
       #l_magento_sales_shipment_item_inserts.parent_id,
       #l_magento_sales_shipment_item_inserts.product_id,
       #l_magento_sales_shipment_item_inserts.order_item_id,
       #l_magento_sales_shipment_item_inserts.sku,
       case when l_magento_sales_shipment_item.l_magento_sales_shipment_item_id is null then isnull(#l_magento_sales_shipment_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_sales_shipment_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_sales_shipment_item_inserts
  left join p_magento_sales_shipment_item
    on #l_magento_sales_shipment_item_inserts.bk_hash = p_magento_sales_shipment_item.bk_hash
   and p_magento_sales_shipment_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_sales_shipment_item
    on p_magento_sales_shipment_item.bk_hash = l_magento_sales_shipment_item.bk_hash
   and p_magento_sales_shipment_item.l_magento_sales_shipment_item_id = l_magento_sales_shipment_item.l_magento_sales_shipment_item_id
 where l_magento_sales_shipment_item.l_magento_sales_shipment_item_id is null
    or (l_magento_sales_shipment_item.l_magento_sales_shipment_item_id is not null
        and l_magento_sales_shipment_item.dv_hash <> #l_magento_sales_shipment_item_inserts.source_hash)

--calculate hash and lookup to current s_magento_sales_shipment_item
if object_id('tempdb..#s_magento_sales_shipment_item_inserts') is not null drop table #s_magento_sales_shipment_item_inserts
create table #s_magento_sales_shipment_item_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_sales_Shipment_Item.bk_hash,
       stage_hash_magento_sales_Shipment_Item.entity_id entity_id,
       stage_hash_magento_sales_Shipment_Item.row_total row_total,
       stage_hash_magento_sales_Shipment_Item.price price,
       stage_hash_magento_sales_Shipment_Item.weight weight,
       stage_hash_magento_sales_Shipment_Item.qty qty,
       stage_hash_magento_sales_Shipment_Item.additional_data additional_data,
       stage_hash_magento_sales_Shipment_Item.description description,
       stage_hash_magento_sales_Shipment_Item.name name,
       stage_hash_magento_sales_Shipment_Item.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_sales_Shipment_Item.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.entity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.row_total as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.price as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.weight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_sales_Shipment_Item.qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_Shipment_Item.additional_data,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_Shipment_Item.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_sales_Shipment_Item.name,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_sales_Shipment_Item
 where stage_hash_magento_sales_Shipment_Item.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_sales_shipment_item records
set @insert_date_time = getdate()
insert into s_magento_sales_shipment_item (
       bk_hash,
       entity_id,
       row_total,
       price,
       weight,
       qty,
       additional_data,
       description,
       name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_sales_shipment_item_inserts.bk_hash,
       #s_magento_sales_shipment_item_inserts.entity_id,
       #s_magento_sales_shipment_item_inserts.row_total,
       #s_magento_sales_shipment_item_inserts.price,
       #s_magento_sales_shipment_item_inserts.weight,
       #s_magento_sales_shipment_item_inserts.qty,
       #s_magento_sales_shipment_item_inserts.additional_data,
       #s_magento_sales_shipment_item_inserts.description,
       #s_magento_sales_shipment_item_inserts.name,
       #s_magento_sales_shipment_item_inserts.dummy_modified_date_time,
       case when s_magento_sales_shipment_item.s_magento_sales_shipment_item_id is null then isnull(#s_magento_sales_shipment_item_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_sales_shipment_item_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_sales_shipment_item_inserts
  left join p_magento_sales_shipment_item
    on #s_magento_sales_shipment_item_inserts.bk_hash = p_magento_sales_shipment_item.bk_hash
   and p_magento_sales_shipment_item.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_sales_shipment_item
    on p_magento_sales_shipment_item.bk_hash = s_magento_sales_shipment_item.bk_hash
   and p_magento_sales_shipment_item.s_magento_sales_shipment_item_id = s_magento_sales_shipment_item.s_magento_sales_shipment_item_id
 where s_magento_sales_shipment_item.s_magento_sales_shipment_item_id is null
    or (s_magento_sales_shipment_item.s_magento_sales_shipment_item_id is not null
        and s_magento_sales_shipment_item.dv_hash <> #s_magento_sales_shipment_item_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_sales_shipment_item @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_magento_sales_shipment_item @current_dv_batch_id

end
