CREATE PROC [dbo].[proc_dim_magento_product] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-1)  from dim_magento_product)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#d_magento_eav_attribute') is not null drop table #d_magento_eav_attribute
create table dbo.#d_magento_eav_attribute with (distribution = hash (bk_hash),location = user_db) as
select bk_hash,attribute_code
from d_magento_eav_attribute
where attribute_code in (
		'color'
		,'description'
		,'free_shipping'
		,'ltbucks_eligible'
		,'mms_club_id'
		,'mms_id'
		,'name'
		,'quantity_and_stock_status'
		,'shipment_type'
		,'short_description'
		,'sku_type'
		,'status'
		,'ts_dimensions_height'
		,'ts_dimensions_length'
		,'ts_dimensions_width'
		,'vendor'
		,'wd_costcenter_id'
		,'wd_offering_id'
		,'wd_region_id'
		,'wd_revenue_id'
        ,'wd_spending_id'
		,'weight'
		,'brand'
		)

if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with (distribution = hash (product_id),location = user_db) as
/* get the product data in the current batch*/
select d_magento_catalog_product_entity.entity_id product_id
	,d_magento_catalog_product_entity.created_dim_date_key
	,d_magento_catalog_product_entity.created_dim_time_key
	,d_magento_catalog_product_entity.has_options_flag
	,d_magento_catalog_product_entity.required_options_flag
	,d_magento_catalog_product_entity.sku
	,d_magento_catalog_product_entity.type_id
	,d_magento_catalog_product_entity.updated_dim_date_key
	,d_magento_catalog_product_entity.updated_dim_time_key
	,d_magento_catalog_product_entity_int.d_magento_catalog_eav_attribute_bk_hash
	,d_magento_eav_attribute.attribute_code
	,cast(d_magento_catalog_product_entity_int.value as varchar(255)) value /*column should be renamed to  [value]*/
	,d_magento_catalog_product_entity.dv_batch_id
	,d_magento_catalog_product_entity.dv_load_date_time
	,d_magento_catalog_product_entity.default_dim_reporting_hierarchy_key
from d_magento_catalog_product_entity
join d_magento_catalog_product_entity_int on d_magento_catalog_product_entity.bk_hash = d_magento_catalog_product_entity_int.d_magento_catalog_product_entity_bk_hash
join #d_magento_eav_attribute d_magento_eav_attribute on d_magento_eav_attribute.bk_hash = d_magento_catalog_product_entity_int.d_magento_catalog_eav_attribute_bk_hash
where d_magento_catalog_product_entity.dv_batch_id >= @load_dv_batch_id

union

select d_magento_catalog_product_entity.entity_id product_id
	,d_magento_catalog_product_entity.created_dim_date_key
	,d_magento_catalog_product_entity.created_dim_time_key
	,d_magento_catalog_product_entity.has_options_flag
	,d_magento_catalog_product_entity.required_options_flag
	,d_magento_catalog_product_entity.sku
	,d_magento_catalog_product_entity.type_id
	,d_magento_catalog_product_entity.updated_dim_date_key
	,d_magento_catalog_product_entity.updated_dim_time_key
	,d_magento_catalog_product_entity_varchar.d_magento_catalog_eav_attribute_bk_hash
	,d_magento_eav_attribute.attribute_code
	,d_magento_catalog_product_entity_varchar.value /*column should be renamed to  [value]*/
	,d_magento_catalog_product_entity.dv_batch_id
	,d_magento_catalog_product_entity.dv_load_date_time
	,d_magento_catalog_product_entity.default_dim_reporting_hierarchy_key
from d_magento_catalog_product_entity
join d_magento_catalog_product_entity_varchar on d_magento_catalog_product_entity.bk_hash = d_magento_catalog_product_entity_varchar.d_magento_catalog_product_entity_bk_hash
join #d_magento_eav_attribute d_magento_eav_attribute on d_magento_eav_attribute.bk_hash = d_magento_catalog_product_entity_varchar.d_magento_catalog_eav_attribute_bk_hash
where d_magento_catalog_product_entity.dv_batch_id >= @load_dv_batch_id

union

select d_magento_catalog_product_entity.entity_id product_id
	,d_magento_catalog_product_entity.created_dim_date_key
	,d_magento_catalog_product_entity.created_dim_time_key
	,d_magento_catalog_product_entity.has_options_flag
	,d_magento_catalog_product_entity.required_options_flag
	,d_magento_catalog_product_entity.sku
	,d_magento_catalog_product_entity.type_id
	,d_magento_catalog_product_entity.updated_dim_date_key
	,d_magento_catalog_product_entity.updated_dim_time_key
	,d_magento_catalog_product_entity_text.d_magento_catalog_eav_attribute_bk_hash
	,d_magento_eav_attribute.attribute_code
	,d_magento_catalog_product_entity_text.value /*column should be renamed to  [value]*/
	,d_magento_catalog_product_entity.dv_batch_id
	,d_magento_catalog_product_entity.dv_load_date_time
	,d_magento_catalog_product_entity.default_dim_reporting_hierarchy_key
from d_magento_catalog_product_entity
join d_magento_catalog_product_entity_text on d_magento_catalog_product_entity.bk_hash = d_magento_catalog_product_entity_text.d_magento_catalog_product_entity_bk_hash
join #d_magento_eav_attribute d_magento_eav_attribute on d_magento_eav_attribute.bk_hash = d_magento_catalog_product_entity_text.d_magento_catalog_eav_attribute_bk_hash
where d_magento_catalog_product_entity.dv_batch_id >= @load_dv_batch_id

union

select d_magento_catalog_product_entity.entity_id product_id
	,d_magento_catalog_product_entity.created_dim_date_key
	,d_magento_catalog_product_entity.created_dim_time_key
	,d_magento_catalog_product_entity.has_options_flag
	,d_magento_catalog_product_entity.required_options_flag
	,d_magento_catalog_product_entity.sku
	,d_magento_catalog_product_entity.type_id
	,d_magento_catalog_product_entity.updated_dim_date_key
	,d_magento_catalog_product_entity.updated_dim_time_key
	,d_magento_catalog_product_entity_decimal.d_magento_catalog_eav_attribute_bk_hash
	,d_magento_eav_attribute.attribute_code
	,cast(d_magento_catalog_product_entity_decimal.value as varchar(255)) value
	,d_magento_catalog_product_entity.dv_batch_id
	,d_magento_catalog_product_entity.dv_load_date_time
	,d_magento_catalog_product_entity.default_dim_reporting_hierarchy_key
from d_magento_catalog_product_entity
join d_magento_catalog_product_entity_decimal on d_magento_catalog_product_entity.bk_hash = d_magento_catalog_product_entity_decimal.d_magento_catalog_product_entity_bk_hash
join #d_magento_eav_attribute d_magento_eav_attribute on d_magento_eav_attribute.bk_hash = d_magento_catalog_product_entity_decimal.d_magento_catalog_eav_attribute_bk_hash
where d_magento_catalog_product_entity.dv_batch_id >= @load_dv_batch_id



if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with (distribution = hash (dim_magento_product_key),location = user_db) as
select case when #etl_step_1.product_id is null then '-998'
       	when ltrim(rtrim(#etl_step_1.product_id))='' then '-998'
          when isnumeric(#etl_step_1.product_id)=0 then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_1.product_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_magento_product_key
     ,product_id
	,created_dim_date_key
	,created_dim_time_key
	,has_options_flag
	,required_options_flag
	,sku
	,type_id
	,updated_dim_date_key
	,updated_dim_time_key
	,dv_batch_id
	,dv_load_date_time
	,default_dim_reporting_hierarchy_key
	,max(case when attribute_code = 'mms_id' then value else null end) mms_product_id
	,max(case when attribute_code = 'name' then value else null end) product_name
	,max(case when attribute_code = 'description' then value else null end) description
	,max(case when attribute_code = 'short_description' then value else null end) short_description
	,max(case when attribute_code = 'status' then value else null end) status
	,max(case when attribute_code = 'weight' then value else null end) weight
	,max(case when attribute_code = 'ts_dimensions_length' then value else null end) length
	,max(case when attribute_code = 'ts_dimensions_width' then value else null end) width
	,max(case when attribute_code = 'ts_dimensions_height' then value else null end) height
	,max(case when attribute_code = 'color' then value else null end) color
	,max(case when attribute_code = 'free_shipping' then value else null end) free_shipping
	,max(case when attribute_code = 'vendor' then value else null end) vendor
	,max(case when attribute_code = 'ltbucks_eligible' then value else null end) lt_bucks_eligible
	,max(case when attribute_code = 'mms_club_id' then value else null end) mms_club_id
	,max(case when attribute_code = 'wd_offering_id' then value else null end) workday_offering_id
	,max(case when attribute_code = 'wd_costcenter_id' then value else null end) workday_costcenter_id
	,max(case when attribute_code = 'wd_region_id' then value else null end) workday_region_id
	,max(case when attribute_code = 'wd_revenue_id' then value else null end) workday_revenue_id
	,max(case when attribute_code = 'wd_spending_id' then value else null end) workday_spending_id
	,max(case when attribute_code = 'brand' then value else null end) manufacturer
from #etl_step_1
group by case when #etl_step_1.product_id is null then '-998'
       	when ltrim(rtrim(#etl_step_1.product_id))='' then '-998'
          when isnumeric(#etl_step_1.product_id)=0 then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_1.product_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end
     ,product_id
	,created_dim_date_key
	,created_dim_time_key
	,has_options_flag
	,required_options_flag
	,sku
	,type_id
	,updated_dim_date_key
     ,updated_dim_time_key
	,dv_batch_id
	,dv_load_date_time
	,default_dim_reporting_hierarchy_key


if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with (distribution = hash (dim_magento_product_key),location = user_db) as
select dim_magento_product_key
     ,product_id
	,created_dim_date_key
	,created_dim_time_key
	,has_options_flag
	,required_options_flag
	,sku
	,type_id
	,updated_dim_date_key
	,updated_dim_time_key
	,case when #etl_step_2.mms_product_id is null then '-998'
       	when ltrim(rtrim(#etl_step_2.mms_product_id))='' then '-998'
          when isnumeric(#etl_step_2.mms_product_id)=0 then '-998'
		   when charindex(',', #etl_step_2.mms_product_id) > 0 then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_2.mms_product_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_product_key
	,product_name
	,description
	,short_description
	,status
	,weight
	,length
	,width
	,height
	,color
	,free_shipping
	,vendor
	,lt_bucks_eligible
	,case when #etl_step_2.mms_club_id is null then '-998'
       	when ltrim(rtrim(#etl_step_2.mms_club_id))='' then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(ltrim(rtrim(#etl_step_2.mms_club_id)) as int) as varchar(500)),'z#@$k%&P'))),2) end dim_club_key
	,workday_offering.option_value workday_offering_id
	,workday_costcenter.option_value workday_costcenter_id
	,workday_region.option_value workday_region_id
	,workday_revenue.option_value workday_revenue_id
	,workday_spending.option_value workday_spending_id
	,manufacturer
	,default_dim_reporting_hierarchy_key
	,#etl_step_2.dv_batch_id dv_batch_id
	,#etl_step_2.dv_load_date_time dv_load_date_time
from #etl_step_2
left join d_magento_eav_attribute_option_value workday_offering on #etl_step_2.workday_offering_id=workday_offering.option_id
left join d_magento_eav_attribute_option_value workday_costcenter on #etl_step_2.workday_costcenter_id=workday_costcenter.option_id
left join d_magento_eav_attribute_option_value workday_region on #etl_step_2.workday_region_id=workday_region.option_id
left join d_magento_eav_attribute_option_value workday_revenue on #etl_step_2.workday_revenue_id=workday_revenue.option_id
left join d_magento_eav_attribute_option_value workday_spending on #etl_step_2.workday_spending_id=workday_spending.option_id

/* delete and re-insert as a single transaction*/
/*   delete records from the table that exist*/
/*   insert records from records from current and missing batches*/

begin tran

  delete dbo.dim_magento_product
   where dim_magento_product_key in (select dim_magento_product_key from #etl_step_3)

  insert into dim_magento_product
        (dim_magento_product_key
	    ,product_id
	    ,created_dim_date_key
   	    ,created_dim_time_key
         ,has_options_flag
	    ,required_options_flag
	    ,sku
	    ,type_id
	    ,updated_dim_date_key
	    ,updated_dim_time_key
	    ,dim_mms_product_key
	    ,product_name
	    ,description
	    ,short_description
	    ,status
	    ,weight
	    ,length
	    ,width
	    ,height
	    ,color
	    ,free_shipping
	    ,vendor
	    ,lt_bucks_eligible
	    ,dim_club_key
	    ,workday_offering_id
	    ,workday_costcenter_id
	    ,workday_region_id
	    ,workday_revenue_id
	    ,workday_spending_id
		,manufacturer
		,default_dim_reporting_hierarchy_key
         ,dv_load_date_time
         ,dv_load_end_date_time
         ,dv_batch_id
         ,dv_inserted_date_time
         ,dv_insert_user)
  select dim_magento_product_key
        ,product_id
	    ,created_dim_date_key
   	    ,created_dim_time_key
         ,has_options_flag
	    ,required_options_flag
	    ,sku
	    ,type_id
	    ,updated_dim_date_key
	    ,updated_dim_time_key
	    ,dim_mms_product_key
	    ,product_name
	    ,description
	    ,short_description
	    ,status
	    ,weight
	    ,length
	    ,width
	    ,height
	    ,color
	    ,free_shipping
	    ,vendor
	    ,lt_bucks_eligible
	    ,dim_club_key
	    ,workday_offering_id
	    ,workday_costcenter_id
	    ,workday_region_id
	    ,workday_revenue_id
		,workday_spending_id
		,manufacturer
		,default_dim_reporting_hierarchy_key
        ,dv_load_date_time
	    ,convert(datetime, '99991231', 112)
        ,dv_batch_id
        ,getdate()
        ,suser_sname()
    from #etl_step_3

commit tran

end



