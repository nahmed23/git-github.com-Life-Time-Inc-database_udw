CREATE PROC [dbo].[proc_etl_lt_bucks_products] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_lt_bucks_Products

set @insert_date_time = getdate()
insert into dbo.stage_hash_lt_bucks_Products (
       bk_hash,
       product_id,
       product_sku,
       product_name,
       product_desc,
       product_price,
       product_has_colors,
       product_has_sizes,
       product_order,
       product_vendor,
       product_vendor_id,
       product_vendor_desc,
       product_vendor_cost,
       product_vendor_drop_ship,
       product_vendor_est_frt,
       product_vendor_act_frt,
       product_msrp,
       product_weight,
       product_date_created,
       product_date_updated,
       product_active,
       product_pgroup,
       product_must_obey_inventory,
       product_discontinued,
       product_on_closeout,
       product_asi_customer,
       product_from_asi,
       product_country,
       product_image_filename,
       product_per,
       product_schart,
       product_isFlat,
       product_promotion,
       product_track_inventory,
       product_shipping_point_amount,
       product_last_user,
       product_isDeleted,
       product_fulfillment_eligible,
       product_kit_eligible,
       LastModifiedTimestamp,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(product_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       product_id,
       product_sku,
       product_name,
       product_desc,
       product_price,
       product_has_colors,
       product_has_sizes,
       product_order,
       product_vendor,
       product_vendor_id,
       product_vendor_desc,
       product_vendor_cost,
       product_vendor_drop_ship,
       product_vendor_est_frt,
       product_vendor_act_frt,
       product_msrp,
       product_weight,
       product_date_created,
       product_date_updated,
       product_active,
       product_pgroup,
       product_must_obey_inventory,
       product_discontinued,
       product_on_closeout,
       product_asi_customer,
       product_from_asi,
       product_country,
       product_image_filename,
       product_per,
       product_schart,
       product_isFlat,
       product_promotion,
       product_track_inventory,
       product_shipping_point_amount,
       product_last_user,
       product_isDeleted,
       product_fulfillment_eligible,
       product_kit_eligible,
       LastModifiedTimestamp,
       isnull(cast(stage_lt_bucks_Products.product_date_created as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_lt_bucks_Products
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_lt_bucks_products @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_lt_bucks_products (
       bk_hash,
       product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_lt_bucks_Products.bk_hash,
       stage_hash_lt_bucks_Products.product_id product_id,
       isnull(cast(stage_hash_lt_bucks_Products.product_date_created as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       5,
       @insert_date_time,
       @user
  from stage_hash_lt_bucks_Products
  left join h_lt_bucks_products
    on stage_hash_lt_bucks_Products.bk_hash = h_lt_bucks_products.bk_hash
 where h_lt_bucks_products_id is null
   and stage_hash_lt_bucks_Products.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_lt_bucks_products
if object_id('tempdb..#l_lt_bucks_products_inserts') is not null drop table #l_lt_bucks_products_inserts
create table #l_lt_bucks_products_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_Products.bk_hash,
       stage_hash_lt_bucks_Products.product_id product_id,
       stage_hash_lt_bucks_Products.product_vendor_id vendor_id,
       stage_hash_lt_bucks_Products.product_last_user last_user,
       isnull(cast(stage_hash_lt_bucks_Products.product_date_created as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Products.product_vendor_id,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_last_user as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_Products
 where stage_hash_lt_bucks_Products.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_lt_bucks_products records
set @insert_date_time = getdate()
insert into l_lt_bucks_products (
       bk_hash,
       product_id,
       vendor_id,
       last_user,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_lt_bucks_products_inserts.bk_hash,
       #l_lt_bucks_products_inserts.product_id,
       #l_lt_bucks_products_inserts.vendor_id,
       #l_lt_bucks_products_inserts.last_user,
       case when l_lt_bucks_products.l_lt_bucks_products_id is null then isnull(#l_lt_bucks_products_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #l_lt_bucks_products_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_lt_bucks_products_inserts
  left join p_lt_bucks_products
    on #l_lt_bucks_products_inserts.bk_hash = p_lt_bucks_products.bk_hash
   and p_lt_bucks_products.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_lt_bucks_products
    on p_lt_bucks_products.bk_hash = l_lt_bucks_products.bk_hash
   and p_lt_bucks_products.l_lt_bucks_products_id = l_lt_bucks_products.l_lt_bucks_products_id
 where l_lt_bucks_products.l_lt_bucks_products_id is null
    or (l_lt_bucks_products.l_lt_bucks_products_id is not null
        and l_lt_bucks_products.dv_hash <> #l_lt_bucks_products_inserts.source_hash)

--calculate hash and lookup to current s_lt_bucks_products
if object_id('tempdb..#s_lt_bucks_products_inserts') is not null drop table #s_lt_bucks_products_inserts
create table #s_lt_bucks_products_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_lt_bucks_Products.bk_hash,
       stage_hash_lt_bucks_Products.product_id product_id,
       stage_hash_lt_bucks_Products.product_sku sku,
       stage_hash_lt_bucks_Products.product_name name,
       stage_hash_lt_bucks_Products.product_desc product_desc,
       stage_hash_lt_bucks_Products.product_price price,
       stage_hash_lt_bucks_Products.product_has_colors has_colors,
       stage_hash_lt_bucks_Products.product_has_sizes has_sizes,
       stage_hash_lt_bucks_Products.product_order product_order,
       stage_hash_lt_bucks_Products.product_vendor vendor,
       stage_hash_lt_bucks_Products.product_vendor_desc vendor_desc,
       stage_hash_lt_bucks_Products.product_vendor_cost vendor_cost,
       stage_hash_lt_bucks_Products.product_vendor_drop_ship vendor_drop_ship,
       stage_hash_lt_bucks_Products.product_vendor_est_frt vendor_est_frt,
       stage_hash_lt_bucks_Products.product_vendor_act_frt vendor_act_frt,
       stage_hash_lt_bucks_Products.product_msrp msrp,
       stage_hash_lt_bucks_Products.product_weight weight,
       stage_hash_lt_bucks_Products.product_date_created date_created,
       stage_hash_lt_bucks_Products.product_date_updated date_updated,
       stage_hash_lt_bucks_Products.product_active active,
       stage_hash_lt_bucks_Products.product_pgroup pgroup,
       stage_hash_lt_bucks_Products.product_must_obey_inventory must_obey_inventory,
       stage_hash_lt_bucks_Products.product_discontinued discontinued,
       stage_hash_lt_bucks_Products.product_on_closeout on_closeout,
       stage_hash_lt_bucks_Products.product_asi_customer asi_customer,
       stage_hash_lt_bucks_Products.product_from_asi from_asi,
       stage_hash_lt_bucks_Products.product_country country,
       stage_hash_lt_bucks_Products.product_image_filename image_filename,
       stage_hash_lt_bucks_Products.product_per per,
       stage_hash_lt_bucks_Products.product_schart schart,
       stage_hash_lt_bucks_Products.product_isFlat is_flat,
       stage_hash_lt_bucks_Products.product_promotion promotion,
       stage_hash_lt_bucks_Products.product_track_inventory track_inventory,
       stage_hash_lt_bucks_Products.product_shipping_point_amount shipping_point_amount,
       stage_hash_lt_bucks_Products.product_isDeleted is_deleted,
       stage_hash_lt_bucks_Products.product_fulfillment_eligible fulfillment_eligible,
       stage_hash_lt_bucks_Products.product_kit_eligible kit_eligible,
       stage_hash_lt_bucks_Products.LastModifiedTimestamp last_modified_timestamp,
       isnull(cast(stage_hash_lt_bucks_Products.product_date_created as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_id as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Products.product_sku,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Products.product_name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Products.product_desc,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_price as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_has_colors as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_has_sizes as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_order as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_vendor as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Products.product_vendor_desc,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_vendor_cost as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_vendor_drop_ship as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_vendor_est_frt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_vendor_act_frt as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_msrp as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_weight as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Products.product_date_created,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Products.product_date_updated,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_active as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_pgroup as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_must_obey_inventory as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_discontinued as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_on_closeout as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_asi_customer as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_from_asi as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_country as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Products.product_image_filename,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_lt_bucks_Products.product_per,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_schart as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_isFlat as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_promotion as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_track_inventory as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_shipping_point_amount as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_isDeleted as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_fulfillment_eligible as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_lt_bucks_Products.product_kit_eligible as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_lt_bucks_Products.LastModifiedTimestamp,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_lt_bucks_Products
 where stage_hash_lt_bucks_Products.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_lt_bucks_products records
set @insert_date_time = getdate()
insert into s_lt_bucks_products (
       bk_hash,
       product_id,
       sku,
       name,
       product_desc,
       price,
       has_colors,
       has_sizes,
       product_order,
       vendor,
       vendor_desc,
       vendor_cost,
       vendor_drop_ship,
       vendor_est_frt,
       vendor_act_frt,
       msrp,
       weight,
       date_created,
       date_updated,
       active,
       pgroup,
       must_obey_inventory,
       discontinued,
       on_closeout,
       asi_customer,
       from_asi,
       country,
       image_filename,
       per,
       schart,
       is_flat,
       promotion,
       track_inventory,
       shipping_point_amount,
       is_deleted,
       fulfillment_eligible,
       kit_eligible,
       last_modified_timestamp,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_lt_bucks_products_inserts.bk_hash,
       #s_lt_bucks_products_inserts.product_id,
       #s_lt_bucks_products_inserts.sku,
       #s_lt_bucks_products_inserts.name,
       #s_lt_bucks_products_inserts.product_desc,
       #s_lt_bucks_products_inserts.price,
       #s_lt_bucks_products_inserts.has_colors,
       #s_lt_bucks_products_inserts.has_sizes,
       #s_lt_bucks_products_inserts.product_order,
       #s_lt_bucks_products_inserts.vendor,
       #s_lt_bucks_products_inserts.vendor_desc,
       #s_lt_bucks_products_inserts.vendor_cost,
       #s_lt_bucks_products_inserts.vendor_drop_ship,
       #s_lt_bucks_products_inserts.vendor_est_frt,
       #s_lt_bucks_products_inserts.vendor_act_frt,
       #s_lt_bucks_products_inserts.msrp,
       #s_lt_bucks_products_inserts.weight,
       #s_lt_bucks_products_inserts.date_created,
       #s_lt_bucks_products_inserts.date_updated,
       #s_lt_bucks_products_inserts.active,
       #s_lt_bucks_products_inserts.pgroup,
       #s_lt_bucks_products_inserts.must_obey_inventory,
       #s_lt_bucks_products_inserts.discontinued,
       #s_lt_bucks_products_inserts.on_closeout,
       #s_lt_bucks_products_inserts.asi_customer,
       #s_lt_bucks_products_inserts.from_asi,
       #s_lt_bucks_products_inserts.country,
       #s_lt_bucks_products_inserts.image_filename,
       #s_lt_bucks_products_inserts.per,
       #s_lt_bucks_products_inserts.schart,
       #s_lt_bucks_products_inserts.is_flat,
       #s_lt_bucks_products_inserts.promotion,
       #s_lt_bucks_products_inserts.track_inventory,
       #s_lt_bucks_products_inserts.shipping_point_amount,
       #s_lt_bucks_products_inserts.is_deleted,
       #s_lt_bucks_products_inserts.fulfillment_eligible,
       #s_lt_bucks_products_inserts.kit_eligible,
       #s_lt_bucks_products_inserts.last_modified_timestamp,
       case when s_lt_bucks_products.s_lt_bucks_products_id is null then isnull(#s_lt_bucks_products_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       5,
       #s_lt_bucks_products_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_lt_bucks_products_inserts
  left join p_lt_bucks_products
    on #s_lt_bucks_products_inserts.bk_hash = p_lt_bucks_products.bk_hash
   and p_lt_bucks_products.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_lt_bucks_products
    on p_lt_bucks_products.bk_hash = s_lt_bucks_products.bk_hash
   and p_lt_bucks_products.s_lt_bucks_products_id = s_lt_bucks_products.s_lt_bucks_products_id
 where s_lt_bucks_products.s_lt_bucks_products_id is null
    or (s_lt_bucks_products.s_lt_bucks_products_id is not null
        and s_lt_bucks_products.dv_hash <> #s_lt_bucks_products_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_lt_bucks_products @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_lt_bucks_products @current_dv_batch_id

end
