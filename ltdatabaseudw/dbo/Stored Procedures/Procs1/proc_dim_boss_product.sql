CREATE PROC [dbo].[proc_dim_boss_product] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on


declare @max_dv_batch_id bigint = (select ISNULL(MAX(dv_batch_id),-1)  from dim_boss_product)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#etl_step1') is not null drop table #etl_step1
create table dbo.#etl_step1 with(distribution=hash(dim_boss_product_key), location=user_db) as
	select d_boss_asi_invtr.dim_boss_product_key,
           d_boss_asi_invtr.color,
           d_boss_asi_invtr.dim_mms_product_key,
           d_boss_asi_invtr.display_flag,
		   d_boss_asi_class_r.product_line product_hierarchy_level_1,
		   d_boss_asi_size_r.product_hierarchy_level_2,
		   d_boss_asi_color_r.product_hierarchy_level_3,
           d_boss_asi_invtr.product_description,
           d_boss_asi_invtr.size,
           d_boss_asi_invtr.sku,
           d_boss_asi_invtr.style,
           d_boss_asi_invtr.invtr_upc_code,
		   d_boss_asi_invtr.invtr_category_id,
           d_boss_asi_dept_m.department_code,
           d_boss_asi_dept_m.department_description,
           d_boss_asi_class_r.product_line,
           d_boss_asi_class_r.class_r_interest_id as product_interest_id,
		   d_boss_interest.interest_short_desc as product_interest_short_desc,
		   d_boss_interest.interest_long_desc as product_interest_long_desc,
		   d_boss_asi_class_r.class_r_format_id as product_format_id,
		   d_boss_product_format.product_format_short_desc,
		   d_boss_product_format.product_format_long_desc,
		   d_boss_asi_class_r.class_r_updated_at as product_updated_timestamp,
           case when isnull(d_boss_asi_invtr.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_class_r.dv_load_date_time,'Jan 1, 1753') 
                and isnull(d_boss_asi_invtr.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_dept_m.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_invtr.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_color_r.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_invtr.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_size_r.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_invtr.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_interest.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_invtr.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_product_format.dv_load_date_time,'Jan 1, 1753')
                then isnull(d_boss_asi_invtr.dv_load_date_time,'Jan 1, 1753')
                when isnull(d_boss_asi_class_r.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_dept_m.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_class_r.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_color_r.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_class_r.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_size_r.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_class_r.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_interest.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_class_r.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_product_format.dv_load_date_time,'Jan 1, 1753')
                then isnull(d_boss_asi_class_r.dv_load_date_time,'Jan 1, 1753')				
				when isnull(d_boss_asi_dept_m.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_color_r.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_dept_m.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_size_r.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_dept_m.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_interest.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_dept_m.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_product_format.dv_load_date_time,'Jan 1, 1753')
                then isnull(d_boss_asi_dept_m.dv_load_date_time,'Jan 1, 1753')				
				when isnull(d_boss_asi_color_r.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_asi_size_r.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_color_r.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_interest.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_color_r.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_product_format.dv_load_date_time,'Jan 1, 1753')
                then isnull(d_boss_asi_color_r.dv_load_date_time,'Jan 1, 1753')	
				when isnull(d_boss_asi_size_r.dv_load_date_time,'Jan 1, 1753') > = isnull(d_boss_interest.dv_load_date_time,'Jan 1, 1753')
				and isnull(d_boss_asi_size_r.dv_load_date_time,'Jan 1, 1753') > = isnull(d_boss_product_format.dv_load_date_time,'Jan 1, 1753') 
				then isnull(d_boss_asi_size_r.dv_load_date_time,'Jan 1, 1753')
				when isnull(d_boss_interest.dv_load_date_time,'Jan 1, 1753') >= isnull(d_boss_product_format.dv_load_date_time,'Jan 1, 1753') 
				then isnull(d_boss_interest.dv_load_date_time,'Jan 1, 1753')
           else isnull(d_boss_product_format.dv_load_date_time,'Jan 1, 1753')  end dv_load_date_time,
           convert(datetime, '99991231', 112) dv_load_end_date_time,
           case when isnull(d_boss_asi_invtr.dv_batch_id,'-1') >= isnull(d_boss_asi_class_r.dv_batch_id,'-1') 
                and isnull(d_boss_asi_invtr.dv_batch_id,'-1') >= isnull(d_boss_asi_dept_m.dv_batch_id,'-1')
				and isnull(d_boss_asi_invtr.dv_batch_id,'-1') >= isnull(d_boss_asi_color_r.dv_batch_id,'-1')
				and isnull(d_boss_asi_invtr.dv_batch_id,'-1') >= isnull(d_boss_asi_size_r.dv_batch_id,'-1')
				and isnull(d_boss_asi_invtr.dv_batch_id,'-1') >= isnull(d_boss_interest.dv_batch_id,'-1')
				and isnull(d_boss_asi_invtr.dv_batch_id,'-1') >= isnull(d_boss_product_format.dv_batch_id,'-1')
                then isnull(d_boss_asi_invtr.dv_batch_id,'-1')
                when isnull(d_boss_asi_class_r.dv_batch_id,'-1') >= isnull(d_boss_asi_dept_m.dv_batch_id,'-1')
				and isnull(d_boss_asi_class_r.dv_batch_id,'-1') >= isnull(d_boss_asi_color_r.dv_batch_id,'-1')
				and isnull(d_boss_asi_class_r.dv_batch_id,'-1') >= isnull(d_boss_asi_size_r.dv_batch_id,'-1')
				and isnull(d_boss_asi_class_r.dv_batch_id,'-1') >= isnull(d_boss_interest.dv_batch_id,'-1')
				and isnull(d_boss_asi_class_r.dv_batch_id,'-1') >= isnull(d_boss_product_format.dv_batch_id,'-1')
                then isnull(d_boss_asi_class_r.dv_batch_id,'-1')				
				when isnull(d_boss_asi_dept_m.dv_batch_id,'-1') >= isnull(d_boss_asi_color_r.dv_batch_id,'-1')
				and isnull(d_boss_asi_dept_m.dv_batch_id,'-1') >= isnull(d_boss_asi_size_r.dv_batch_id,'-1')
				and isnull(d_boss_asi_dept_m.dv_batch_id,'-1') >= isnull(d_boss_interest.dv_batch_id,'-1')
				and isnull(d_boss_asi_dept_m.dv_batch_id,'-1') >= isnull(d_boss_product_format.dv_batch_id,'-1')
                then isnull(d_boss_asi_dept_m.dv_batch_id,'-1')				
				when isnull(d_boss_asi_color_r.dv_batch_id,'-1') >= isnull(d_boss_asi_size_r.dv_batch_id,'-1')
				and isnull(d_boss_asi_color_r.dv_batch_id,'-1') >= isnull(d_boss_interest.dv_batch_id,'-1')
				and isnull(d_boss_asi_color_r.dv_batch_id,'-1') >= isnull(d_boss_product_format.dv_batch_id,'-1')
                then isnull(d_boss_asi_color_r.dv_batch_id,'-1')	
				when isnull(d_boss_asi_size_r.dv_batch_id,'-1') > = isnull(d_boss_interest.dv_batch_id,'-1')
				and isnull(d_boss_asi_size_r.dv_batch_id,'-1') > = isnull(d_boss_product_format.dv_batch_id,'-1') 
				then isnull(d_boss_asi_size_r.dv_batch_id,'-1')
				when isnull(d_boss_interest.dv_batch_id,'-1') >= isnull(d_boss_product_format.dv_batch_id,'-1') 
				then isnull(d_boss_interest.dv_batch_id,'-1')
           else isnull(d_boss_product_format.dv_batch_id,'-1')  end dv_batch_id
  from d_boss_asi_invtr
  join d_boss_asi_class_r
    on d_boss_asi_invtr.d_boss_asi_class_r_bk_hash = d_boss_asi_class_r.bk_hash
  join d_boss_asi_dept_m
    on d_boss_asi_invtr.d_boss_asi_dept_m_bk_hash = d_boss_asi_dept_m.bk_hash
  left join d_boss_asi_color_r
    on d_boss_asi_invtr.d_boss_asi_color_r_bk_hash = d_boss_asi_color_r.bk_hash
  left join d_boss_asi_size_r
    on d_boss_asi_invtr.d_boss_asi_size_r_bk_hash = d_boss_asi_size_r.bk_hash
  left join d_boss_interest
	on d_boss_asi_class_r.d_boss_interest_bk_hash = d_boss_interest.bk_hash
  left join d_boss_product_format
	on d_boss_asi_class_r.d_boss_product_format_bk_hash = d_boss_product_format.bk_hash
 where d_boss_asi_invtr.dv_batch_id >= @load_dv_batch_id
    or d_boss_asi_class_r.dv_batch_id >= @load_dv_batch_id
    or d_boss_asi_dept_m.dv_batch_id >= @load_dv_batch_id
	or d_boss_asi_color_r.dv_batch_id >= @load_dv_batch_id
    or d_boss_asi_size_r.dv_batch_id >= @load_dv_batch_id
	or d_boss_interest.dv_batch_id >= @load_dv_batch_id
	or d_boss_product_format.dv_batch_id >= @load_dv_batch_id


/* Delete and re-insert as a single transaction*/
/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/

begin tran

  delete dbo.dim_boss_product
   where dim_boss_product_key in (select dim_boss_product_key from dbo.#etl_step1) 

insert into dim_boss_product
        (dim_boss_product_key,
         color,
         dim_mms_product_key,
         display_flag,
	    product_hierarchy_level_1,
	    product_hierarchy_level_2,
	    product_hierarchy_level_3,
         product_description,
         size,
         sku,
         style,
         upc_code,
		 category_id,
         department_code,
         department_description,
         product_line,
		 product_interest_id,
		 product_interest_short_desc,
		 product_interest_long_desc,
		 product_format_id,
		 product_format_short_desc,
		 product_format_long_desc,
		 product_updated_timestamp,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         dv_inserted_date_time,
         dv_insert_user)
select dim_boss_product_key,
         color,
         dim_mms_product_key,
         display_flag,
	    product_hierarchy_level_1,
	    product_hierarchy_level_2,
	    product_hierarchy_level_3,
         product_description,
         size,
         sku,
         style,
         invtr_upc_code,
		 invtr_category_id,
         department_code,
         department_description,
         product_line,
		 product_interest_id,
		 product_interest_short_desc,
		 product_interest_long_desc,
		 product_format_id,
		 product_format_short_desc,
		 product_format_long_desc,
		 product_updated_timestamp,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate() ,
         suser_sname()
    from #etl_step1
 
commit tran
end
