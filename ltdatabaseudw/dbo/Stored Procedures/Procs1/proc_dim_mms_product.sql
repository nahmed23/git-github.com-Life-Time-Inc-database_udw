CREATE PROC [dbo].[proc_dim_mms_product] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_mms_product)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end

if object_id('tempdb..#dim_mms_product') is not null drop table #dim_mms_product
create table dbo.#dim_mms_product with(distribution=hash(dim_mms_product_key), location=user_db, heap) as
select d_mms_product.dim_mms_product_key,
       d_mms_product.product_id,
       d_mms_product.assess_as_dues_flag,
       d_mms_product.deferred_revenue_flag,
       isnull(d_mms_gl_account.discount_gl_account, '') discount_gl_account,
       d_mms_product.display_ui_flag,
       d_mms_product.gl_account_number,
	   d_mms_product.gl_department_code,
       d_mms_product.gl_over_ride_club_id,
       d_mms_product.gl_product_code,
       d_mms_product.junior_member_dues_flag,
       d_mms_product.package_product_flag,
       d_mms_product.pay_component,
       d_mms_product.price_locked_flag,
       d_mms_product.product_description,
       d_mms_product.product_name,
       r_mms_val_product_status.description product_status,
       r_mms_val_recurrent_product_type.description recurrent_product_type_description,
       isnull(d_mms_gl_account.refund_gl_account_number, '') refund_gl_account_number,
       d_mms_product.revenue_category,
       d_mms_product.sales_quantity_factor,
       d_mms_product.sku,
       d_mms_product.spend_category,
       d_mms_product.tip_allowed_flag,
       d_mms_product.workday_account,
       d_mms_product.workday_cost_center,
       isnull(gl_account_workday_account.discount_gl_account, '') workday_discount_gl_account,
       d_mms_product.workday_offering,
       d_mms_product.workday_over_ride_region,
       isnull(gl_account_workday_account.refund_gl_account_number, '') workday_refund_gl_account,
       d_mms_product.workday_revenue_product_group_account,
       isnull(gl_account_workday_revenue_account.discount_gl_account, '') workday_revenue_product_group_discount_gl_account,
       isnull(gl_account_workday_revenue_account.refund_gl_account_number, '') workday_revenue_product_group_refund_gl_account,
       case when d_mms_product.dv_load_date_time >= isnull(d_mms_gl_account.dv_load_date_time,'jan 1, 1753') then d_mms_product.dv_load_date_time
            else d_mms_gl_account.dv_load_date_time
        end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when d_mms_product.dv_batch_id >= isnull(d_mms_gl_account.dv_batch_id,-1) then d_mms_product.dv_batch_id
            else d_mms_gl_account.dv_batch_id
        end dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user,
       isnull(d_mms_department.description,'') department_description,
	   d_mms_product.default_dim_reporting_hierarchy_key,
	   d_mms_product.lt_buck_eligible lt_buck_eligible,
	   d_mms_product.lt_buck_cost_percent lt_buck_cost_percent,
	   d_mms_product.department_id department_id,
	   d_mms_product.access_by_price_paid_flag
  from d_mms_product
  left join d_mms_department
    on d_mms_product.mms_department_bk_hash = d_mms_department.dim_mms_department_key
  left join d_mms_gl_account 
    on d_mms_product.gl_account_number  = d_mms_gl_account.revenue_gl_account_number
  left join d_mms_gl_account gl_account_workday_account
    on d_mms_product.workday_account = gl_account_workday_account.revenue_gl_account_number
  left join d_mms_gl_account gl_account_workday_revenue_account
    on d_mms_product.workday_revenue_product_group_account = gl_account_workday_revenue_account.revenue_gl_account_number
  left join r_mms_val_recurrent_product_type
    on d_mms_product.r_mms_val_recurrent_product_type_bk_hash = r_mms_val_recurrent_product_type.bk_hash
   and r_mms_val_recurrent_product_type.dv_load_end_date_time = '9999-12-31'
  left join r_mms_val_product_status
    on d_mms_product.r_mms_val_product_status_bk_hash = r_mms_val_product_status.bk_hash
   and r_mms_val_product_status.dv_load_end_date_time = '9999-12-31'
 where (d_mms_product.dv_batch_id >= @load_dv_batch_id
        or d_mms_gl_account.dv_batch_id >= @load_dv_batch_id)

   
/* Delete and re-insert as a single transaction*/
/*   Delete records from the table that exist*/
/*   Insert records from records from current and missing batches*/

begin tran

  delete dbo.dim_mms_product
   where dim_mms_product_key in (select dim_mms_product_key from dbo.#dim_mms_product) 

  insert into dim_mms_product
   (dim_mms_product_key,
    product_id,
    assess_as_dues_flag,
    deferred_revenue_flag,
    department_description,
    discount_gl_account,
    display_ui_flag,
    gl_account_number,
    gl_department_code,
    gl_over_ride_club_id,
    gl_product_code,
    junior_member_dues_flag,
    package_product_flag,
    pay_component,
    price_locked_flag,
    product_description,
    product_name,
    product_status,
    recurrent_product_type_description,
    refund_gl_account_number,
    revenue_category,
    sales_quantity_factor,
    sku,
    spend_category,
    tip_allowed_flag,
    workday_account,
    workday_cost_center,
    workday_discount_gl_account,
    workday_offering,
    workday_over_ride_region,
    workday_refund_gl_account,
    workday_revenue_product_group_account,
    workday_revenue_product_group_discount_gl_account,
    workday_revenue_product_group_refund_gl_account,
	lt_buck_eligible, 
	lt_buck_cost_percent,
	department_id,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user,
	default_dim_reporting_hierarchy_key,
	access_by_price_paid_flag
     )
 select dim_mms_product_key,
        product_id,
        assess_as_dues_flag,
        deferred_revenue_flag,
        department_description,
        discount_gl_account,
        display_ui_flag,
        gl_account_number,
	    gl_department_code,
        gl_over_ride_club_id,
        gl_product_code,
        junior_member_dues_flag,
        package_product_flag,
        pay_component,
        price_locked_flag,
        product_description,
        product_name,
        product_status,
        recurrent_product_type_description,
        refund_gl_account_number,
        revenue_category,
        sales_quantity_factor,
        sku,
        spend_category,
        tip_allowed_flag,
        workday_account,
        workday_cost_center,
        workday_discount_gl_account,
        workday_offering,
        workday_over_ride_region,
        workday_refund_gl_account,
        workday_revenue_product_group_account,
        workday_revenue_product_group_discount_gl_account,
        workday_revenue_product_group_refund_gl_account,
		lt_buck_eligible, 
	    lt_buck_cost_percent,
		department_id,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id,
        dv_inserted_date_time,
        dv_insert_user,
		default_dim_reporting_hierarchy_key,
		access_by_price_paid_flag
    from #dim_mms_product

commit tran

end
