CREATE PROC [dbo].[proc_etl_magento_catalog_inventory_stock_status] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_cataloginventory_stock_status

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_cataloginventory_stock_status (
       bk_hash,
       product_id,
       website_id,
       stock_id,
       qty,
       stock_status,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(product_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(website_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(stock_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       product_id,
       website_id,
       stock_id,
       qty,
       stock_status,
       dummy_modified_date_time,
       isnull(cast(stage_magento_cataloginventory_stock_status.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_cataloginventory_stock_status
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_catalog_inventory_stock_status @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_catalog_inventory_stock_status (
       bk_hash,
       product_id,
       website_id,
       stock_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_cataloginventory_stock_status.bk_hash,
       stage_hash_magento_cataloginventory_stock_status.product_id product_id,
       stage_hash_magento_cataloginventory_stock_status.website_id website_id,
       stage_hash_magento_cataloginventory_stock_status.stock_id stock_id,
       isnull(cast(stage_hash_magento_cataloginventory_stock_status.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_cataloginventory_stock_status
  left join h_magento_catalog_inventory_stock_status
    on stage_hash_magento_cataloginventory_stock_status.bk_hash = h_magento_catalog_inventory_stock_status.bk_hash
 where h_magento_catalog_inventory_stock_status_id is null
   and stage_hash_magento_cataloginventory_stock_status.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_magento_catalog_inventory_stock_status
if object_id('tempdb..#s_magento_catalog_inventory_stock_status_inserts') is not null drop table #s_magento_catalog_inventory_stock_status_inserts
create table #s_magento_catalog_inventory_stock_status_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_cataloginventory_stock_status.bk_hash,
       stage_hash_magento_cataloginventory_stock_status.product_id product_id,
       stage_hash_magento_cataloginventory_stock_status.website_id website_id,
       stage_hash_magento_cataloginventory_stock_status.stock_id stock_id,
       stage_hash_magento_cataloginventory_stock_status.qty qty,
       stage_hash_magento_cataloginventory_stock_status.stock_status stock_status,
       stage_hash_magento_cataloginventory_stock_status.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_cataloginventory_stock_status.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_status.product_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_status.website_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_status.stock_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_status.qty as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_cataloginventory_stock_status.stock_status as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_cataloginventory_stock_status
 where stage_hash_magento_cataloginventory_stock_status.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_catalog_inventory_stock_status records
set @insert_date_time = getdate()
insert into s_magento_catalog_inventory_stock_status (
       bk_hash,
       product_id,
       website_id,
       stock_id,
       qty,
       stock_status,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_catalog_inventory_stock_status_inserts.bk_hash,
       #s_magento_catalog_inventory_stock_status_inserts.product_id,
       #s_magento_catalog_inventory_stock_status_inserts.website_id,
       #s_magento_catalog_inventory_stock_status_inserts.stock_id,
       #s_magento_catalog_inventory_stock_status_inserts.qty,
       #s_magento_catalog_inventory_stock_status_inserts.stock_status,
       #s_magento_catalog_inventory_stock_status_inserts.dummy_modified_date_time,
       case when s_magento_catalog_inventory_stock_status.s_magento_catalog_inventory_stock_status_id is null then isnull(#s_magento_catalog_inventory_stock_status_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_catalog_inventory_stock_status_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_catalog_inventory_stock_status_inserts
  left join p_magento_catalog_inventory_stock_status
    on #s_magento_catalog_inventory_stock_status_inserts.bk_hash = p_magento_catalog_inventory_stock_status.bk_hash
   and p_magento_catalog_inventory_stock_status.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_catalog_inventory_stock_status
    on p_magento_catalog_inventory_stock_status.bk_hash = s_magento_catalog_inventory_stock_status.bk_hash
   and p_magento_catalog_inventory_stock_status.s_magento_catalog_inventory_stock_status_id = s_magento_catalog_inventory_stock_status.s_magento_catalog_inventory_stock_status_id
 where s_magento_catalog_inventory_stock_status.s_magento_catalog_inventory_stock_status_id is null
    or (s_magento_catalog_inventory_stock_status.s_magento_catalog_inventory_stock_status_id is not null
        and s_magento_catalog_inventory_stock_status.dv_hash <> #s_magento_catalog_inventory_stock_status_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_catalog_inventory_stock_status @current_dv_batch_id

end
