CREATE PROC [dbo].[proc_dim_cafe_product] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


-------rank to take the record with the latest effective_dim_date_key and max menu_item_dim_id
if object_id('tempdb..#rank_d_ig_ig_dimension_menu_item_dimension') is not null drop table #rank_d_ig_ig_dimension_menu_item_dimension
create table dbo.#rank_d_ig_ig_dimension_menu_item_dimension with(distribution=round_robin, location=user_db) as
select row_number() over (partition by d_ig_ig_dimension_menu_item_dimension.menu_item_id
                           order by d_ig_ig_dimension_menu_item_dimension.effective_dim_date_key desc,
                                    d_ig_ig_dimension_menu_item_dimension.menu_item_dim_id desc) r,
       d_ig_ig_dimension_menu_item_dimension.bk_hash
  from d_ig_ig_dimension_menu_item_dimension
 where d_ig_ig_dimension_menu_item_dimension.expiration_dim_date_key = '99991231'
    or d_ig_ig_dimension_menu_item_dimension.bk_hash in ('-997','-998','-999')

------updating the default records
update #rank_d_ig_ig_dimension_menu_item_dimension
   set r = 1
 where bk_hash in ('-997','-998','-999')

-----#etl_step_1
if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(dim_cafe_product_key), location=user_db) as
select d_ig_ig_dimension_menu_item_dimension.dim_cafe_product_key,
       d_ig_ig_dimension_menu_item_dimension.menu_item_id,
       case when d_ig_it_cfg_menu_item_master.bk_hash in ('-997','-998','-999') then d_ig_it_cfg_menu_item_master.bk_hash
       when d_ig_it_cfg_menu_item_master.master_menu_item_name is null then d_ig_ig_dimension_menu_item_dimension.menu_item_name
       else d_ig_it_cfg_menu_item_master.master_menu_item_name end as menu_item_name,
       d_ig_ig_dimension_menu_item_dimension.sku_number,
       d_ig_ig_dimension_menu_item_dimension.product_class_id,
       d_ig_ig_dimension_menu_item_dimension.product_class_name,
       case when d_ig_it_cfg_menu_item_master.mi_not_active_flag = 0 then 'Y'
       else 'N' end as menu_item_active_flag,
       isnull(dim_kronos_labor_category_map.kronos_labor_category,'Retail') kronos_labor_category,
       case when d_ig_ig_dimension_menu_item_dimension.dv_load_date_time < d_ig_it_cfg_menu_item_master.dv_load_date_time 
                 or d_ig_ig_dimension_menu_item_dimension.dv_load_date_time is null 
            then d_ig_it_cfg_menu_item_master.dv_load_date_time
            else d_ig_ig_dimension_menu_item_dimension.dv_load_date_time end as dv_load_date_time,
            '9999-12-31' as dv_load_end_date_time,
       case when d_ig_ig_dimension_menu_item_dimension.dv_batch_id < d_ig_it_cfg_menu_item_master.dv_batch_id
                 or d_ig_ig_dimension_menu_item_dimension.dv_batch_id is null
            then d_ig_it_cfg_menu_item_master.dv_batch_id 
            else d_ig_ig_dimension_menu_item_dimension.dv_batch_id end as dv_batch_id,
       d_ig_ig_dimension_menu_item_dimension.default_dim_reporting_hierarchy_key
  from d_ig_ig_dimension_menu_item_dimension
  join #rank_d_ig_ig_dimension_menu_item_dimension
    on d_ig_ig_dimension_menu_item_dimension.bk_hash = #rank_d_ig_ig_dimension_menu_item_dimension.bk_hash
   and #rank_d_ig_ig_dimension_menu_item_dimension.r = 1
  left join d_ig_it_cfg_menu_item_master
    on d_ig_ig_dimension_menu_item_dimension.menu_item_id = d_ig_it_cfg_menu_item_master.menu_item_id
  left join dim_kronos_labor_category_map
    on d_ig_ig_dimension_menu_item_dimension.product_class_id = dim_kronos_labor_category_map.ig_it_cfg_product_class_master_product_class_id


begin tran

  delete dbo.dim_cafe_product
   where dim_cafe_product_key in (select dim_cafe_product_key from dbo.#etl_step_1) 

  insert into dim_cafe_product
           (dim_cafe_product_key,
            menu_item_id,
            menu_item_name,
            sku_number,
            product_class_id,
            product_class_name,
            menu_item_active_flag,
            kronos_labor_category,
            dv_load_date_time,
            dv_load_end_date_time,
            dv_batch_id,
            dv_inserted_date_time,
            dv_insert_user,
            default_dim_reporting_hierarchy_key)
  select dim_cafe_product_key,
            menu_item_id,
            menu_item_name,
            sku_number,
            product_class_id,
            product_class_name,
            menu_item_active_flag,
            kronos_labor_category,
			dv_load_date_time,
            dv_load_end_date_time,
            dv_batch_id,
            getdate(),
            suser_sname(),
            default_dim_reporting_hierarchy_key
			from #etl_step_1

commit tran

end
