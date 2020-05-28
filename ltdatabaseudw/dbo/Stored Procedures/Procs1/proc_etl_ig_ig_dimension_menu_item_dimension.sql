CREATE PROC [dbo].[proc_etl_ig_ig_dimension_menu_item_dimension] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ig_ig_dimension_Menu_Item_Dimension

set @insert_date_time = getdate()
insert into dbo.stage_hash_ig_ig_dimension_Menu_Item_Dimension (
       bk_hash,
       menu_item_dim_id,
       profit_center_dim_ent_level_id,
       menu_item_id,
       customer_id,
       ent_id,
       division_id,
       store_created_id,
       menu_item_name,
       sold_by_weight_flag,
       tare_weight,
       sku_number,
       cost_amount,
       product_class_id,
       product_class_name,
       revenue_category_id,
       revenue_category_name,
       tax_group_id,
       tax_group_name,
       tax_included_flag,
       report_category_id,
       report_category_name,
       eff_date_from,
       eff_date_to,
       product_class_default_revenue_category_id,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(menu_item_dim_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       menu_item_dim_id,
       profit_center_dim_ent_level_id,
       menu_item_id,
       customer_id,
       ent_id,
       division_id,
       store_created_id,
       menu_item_name,
       sold_by_weight_flag,
       tare_weight,
       sku_number,
       cost_amount,
       product_class_id,
       product_class_name,
       revenue_category_id,
       revenue_category_name,
       tax_group_id,
       tax_group_name,
       tax_included_flag,
       report_category_id,
       report_category_name,
       eff_date_from,
       eff_date_to,
       product_class_default_revenue_category_id,
       isnull(cast(stage_ig_ig_dimension_Menu_Item_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_ig_ig_dimension_Menu_Item_Dimension
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ig_ig_dimension_menu_item_dimension @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ig_ig_dimension_menu_item_dimension (
       bk_hash,
       menu_item_dim_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_ig_ig_dimension_Menu_Item_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_dim_id menu_item_dim_id,
       isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       13,
       @insert_date_time,
       @user
  from stage_hash_ig_ig_dimension_Menu_Item_Dimension
  left join h_ig_ig_dimension_menu_item_dimension
    on stage_hash_ig_ig_dimension_Menu_Item_Dimension.bk_hash = h_ig_ig_dimension_menu_item_dimension.bk_hash
 where h_ig_ig_dimension_menu_item_dimension_id is null
   and stage_hash_ig_ig_dimension_Menu_Item_Dimension.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ig_ig_dimension_menu_item_dimension
if object_id('tempdb..#l_ig_ig_dimension_menu_item_dimension_inserts') is not null drop table #l_ig_ig_dimension_menu_item_dimension_inserts
create table #l_ig_ig_dimension_menu_item_dimension_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Menu_Item_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_dim_id menu_item_dim_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.profit_center_dim_ent_level_id profit_center_dim_ent_level_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_id menu_item_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.customer_id customer_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.ent_id ent_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.division_id division_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.store_created_id store_created_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.product_class_id product_class_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.revenue_category_id revenue_category_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.tax_group_id tax_group_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.report_category_id report_category_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.product_class_default_revenue_category_id product_class_default_revenue_category_id,
       isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_dim_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.profit_center_dim_ent_level_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.customer_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.ent_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.division_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.store_created_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.product_class_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.revenue_category_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.tax_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.report_category_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.product_class_default_revenue_category_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Menu_Item_Dimension
 where stage_hash_ig_ig_dimension_Menu_Item_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ig_ig_dimension_menu_item_dimension records
set @insert_date_time = getdate()
insert into l_ig_ig_dimension_menu_item_dimension (
       bk_hash,
       menu_item_dim_id,
       profit_center_dim_ent_level_id,
       menu_item_id,
       customer_id,
       ent_id,
       division_id,
       store_created_id,
       product_class_id,
       revenue_category_id,
       tax_group_id,
       report_category_id,
       product_class_default_revenue_category_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ig_ig_dimension_menu_item_dimension_inserts.bk_hash,
       #l_ig_ig_dimension_menu_item_dimension_inserts.menu_item_dim_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.profit_center_dim_ent_level_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.menu_item_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.customer_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.ent_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.division_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.store_created_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.product_class_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.revenue_category_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.tax_group_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.report_category_id,
       #l_ig_ig_dimension_menu_item_dimension_inserts.product_class_default_revenue_category_id,
       case when l_ig_ig_dimension_menu_item_dimension.l_ig_ig_dimension_menu_item_dimension_id is null then isnull(#l_ig_ig_dimension_menu_item_dimension_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #l_ig_ig_dimension_menu_item_dimension_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ig_ig_dimension_menu_item_dimension_inserts
  left join p_ig_ig_dimension_menu_item_dimension
    on #l_ig_ig_dimension_menu_item_dimension_inserts.bk_hash = p_ig_ig_dimension_menu_item_dimension.bk_hash
   and p_ig_ig_dimension_menu_item_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ig_ig_dimension_menu_item_dimension
    on p_ig_ig_dimension_menu_item_dimension.bk_hash = l_ig_ig_dimension_menu_item_dimension.bk_hash
   and p_ig_ig_dimension_menu_item_dimension.l_ig_ig_dimension_menu_item_dimension_id = l_ig_ig_dimension_menu_item_dimension.l_ig_ig_dimension_menu_item_dimension_id
 where l_ig_ig_dimension_menu_item_dimension.l_ig_ig_dimension_menu_item_dimension_id is null
    or (l_ig_ig_dimension_menu_item_dimension.l_ig_ig_dimension_menu_item_dimension_id is not null
        and l_ig_ig_dimension_menu_item_dimension.dv_hash <> #l_ig_ig_dimension_menu_item_dimension_inserts.source_hash)

--calculate hash and lookup to current s_ig_ig_dimension_menu_item_dimension
if object_id('tempdb..#s_ig_ig_dimension_menu_item_dimension_inserts') is not null drop table #s_ig_ig_dimension_menu_item_dimension_inserts
create table #s_ig_ig_dimension_menu_item_dimension_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ig_ig_dimension_Menu_Item_Dimension.bk_hash,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_dim_id menu_item_dim_id,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_name menu_item_name,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.sold_by_weight_flag sold_by_weight_flag,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.tare_weight tare_weight,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.sku_number sku_number,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.cost_amount cost_amount,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.product_class_name product_class_name,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.revenue_category_name revenue_category_name,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.tax_group_name tax_group_name,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.tax_included_flag tax_included_flag,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.report_category_name report_category_name,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.eff_date_from eff_date_from,
       stage_hash_ig_ig_dimension_Menu_Item_Dimension.eff_date_to eff_date_to,
       isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.eff_date_from as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_dim_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Menu_Item_Dimension.menu_item_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Menu_Item_Dimension.sold_by_weight_flag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.tare_weight as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Menu_Item_Dimension.sku_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ig_ig_dimension_Menu_Item_Dimension.cost_amount as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Menu_Item_Dimension.product_class_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Menu_Item_Dimension.revenue_category_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Menu_Item_Dimension.tax_group_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Menu_Item_Dimension.tax_included_flag,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ig_ig_dimension_Menu_Item_Dimension.report_category_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_dimension_Menu_Item_Dimension.eff_date_from,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ig_ig_dimension_Menu_Item_Dimension.eff_date_to,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ig_ig_dimension_Menu_Item_Dimension
 where stage_hash_ig_ig_dimension_Menu_Item_Dimension.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ig_ig_dimension_menu_item_dimension records
set @insert_date_time = getdate()
insert into s_ig_ig_dimension_menu_item_dimension (
       bk_hash,
       menu_item_dim_id,
       menu_item_name,
       sold_by_weight_flag,
       tare_weight,
       sku_number,
       cost_amount,
       product_class_name,
       revenue_category_name,
       tax_group_name,
       tax_included_flag,
       report_category_name,
       eff_date_from,
       eff_date_to,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ig_ig_dimension_menu_item_dimension_inserts.bk_hash,
       #s_ig_ig_dimension_menu_item_dimension_inserts.menu_item_dim_id,
       #s_ig_ig_dimension_menu_item_dimension_inserts.menu_item_name,
       #s_ig_ig_dimension_menu_item_dimension_inserts.sold_by_weight_flag,
       #s_ig_ig_dimension_menu_item_dimension_inserts.tare_weight,
       #s_ig_ig_dimension_menu_item_dimension_inserts.sku_number,
       #s_ig_ig_dimension_menu_item_dimension_inserts.cost_amount,
       #s_ig_ig_dimension_menu_item_dimension_inserts.product_class_name,
       #s_ig_ig_dimension_menu_item_dimension_inserts.revenue_category_name,
       #s_ig_ig_dimension_menu_item_dimension_inserts.tax_group_name,
       #s_ig_ig_dimension_menu_item_dimension_inserts.tax_included_flag,
       #s_ig_ig_dimension_menu_item_dimension_inserts.report_category_name,
       #s_ig_ig_dimension_menu_item_dimension_inserts.eff_date_from,
       #s_ig_ig_dimension_menu_item_dimension_inserts.eff_date_to,
       case when s_ig_ig_dimension_menu_item_dimension.s_ig_ig_dimension_menu_item_dimension_id is null then isnull(#s_ig_ig_dimension_menu_item_dimension_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       13,
       #s_ig_ig_dimension_menu_item_dimension_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ig_ig_dimension_menu_item_dimension_inserts
  left join p_ig_ig_dimension_menu_item_dimension
    on #s_ig_ig_dimension_menu_item_dimension_inserts.bk_hash = p_ig_ig_dimension_menu_item_dimension.bk_hash
   and p_ig_ig_dimension_menu_item_dimension.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ig_ig_dimension_menu_item_dimension
    on p_ig_ig_dimension_menu_item_dimension.bk_hash = s_ig_ig_dimension_menu_item_dimension.bk_hash
   and p_ig_ig_dimension_menu_item_dimension.s_ig_ig_dimension_menu_item_dimension_id = s_ig_ig_dimension_menu_item_dimension.s_ig_ig_dimension_menu_item_dimension_id
 where s_ig_ig_dimension_menu_item_dimension.s_ig_ig_dimension_menu_item_dimension_id is null
    or (s_ig_ig_dimension_menu_item_dimension.s_ig_ig_dimension_menu_item_dimension_id is not null
        and s_ig_ig_dimension_menu_item_dimension.dv_hash <> #s_ig_ig_dimension_menu_item_dimension_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ig_ig_dimension_menu_item_dimension @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ig_ig_dimension_menu_item_dimension @current_dv_batch_id

end
