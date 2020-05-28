CREATE PROC [dbo].[proc_payroll_icm_daily] AS
BEGIN

SET XACT_ABORT ON
SET NOCOUNT ON

if object_id('tempdb..#DimDate') is not null drop table #DimDate
create table dbo.#DimDate with(distribution=round_robin, location=user_db) as 
select dd.dim_date_key today_dim_date_key,
       week_dim_date.dim_date_key week_dim_date_key
from dim_date dd
join v_get_date gd on dd.calendar_date = gd.get_date --today, central
join dim_date week_dim_date on dd.week_number_in_year = week_dim_date.week_number_in_year and dd.year = week_dim_date.year

if object_id('tempdb..#magento_sales') is not null drop table #magento_sales
create table dbo.#magento_sales with(distribution=round_robin, location=user_db) as 
SELECT '6'+case when refund_flag = 'Y' then 'Y'+transaction_item.fact_magento_refund_item_key
            else 'N'+transaction_item.fact_magento_invoice_item_key end unique_key,
       transaction_item.payroll_dim_club_key,
       transaction_item.dim_employee_key,
       transaction_item.dim_mms_member_key,
       cast(product.product_id as varchar(100)) product_id,
       product.product_name product_description,
       product.payroll_description payroll_extract_name,
       product.payroll_standard_group_description payroll_group,
       case when product.payroll_description = 'PT Commissionable Sales and Service' then transaction_item.allocated_amount
            else transaction_item.transaction_item_amount end amount,
       transaction_item.allocated_amount,
       transaction_item.transaction_quantity quantity,
       'Sale' record_type,
       transaction_item.transaction_dim_date_key,
       isnull(invoice_idk.udw_inserted_dim_date_key,refund_idk.udw_inserted_dim_date_key) udw_inserted_dim_date_key,
       product.payroll_description,
       transaction_item.order_item_id
  FROM marketing.v_fact_magento_transaction_item transaction_item
  JOIN marketing.v_dim_magento_product product
    ON transaction_item.dim_magento_product_key = product.dim_magento_product_key
  left join idk_fact_magento_transaction_item invoice_idk
    on transaction_item.fact_magento_invoice_item_key = invoice_idk.fact_magento_invoice_item_key
   and transaction_item.fact_magento_invoice_item_key not in ('-999','-998','-997')
  left join idk_fact_magento_transaction_item refund_idk
    on transaction_item.fact_magento_refund_item_key = invoice_idk.fact_magento_refund_item_key
   and transaction_item.fact_magento_refund_item_key not in ('-999','-998','-997')
 where transaction_item.dim_employee_key not in ('-999','-998','-997')
   and (product.payroll_lt_bucks_sales_amount_flag = 'Y'
        or product.payroll_standard_sales_amount_flag = 'Y')


select r.unique_key,
       dim_employee.employee_id EmployeeID,
	   dim_club.workday_region WorkdayRegion,
       r.product_id ProductID,
       r.product_description ProductDescription,
       dim_member.member_id MemberID,
       dim_member.customer_name CustomerName,
       convert(varchar,dd.calendar_date,101) TranDate,
       r.amount,
       r.quantity,
       r.payroll_extract_name PayrollExtractName,
       r.payroll_group PayrollGroup,
       r.record_type RecordType,
       cast(r.udw_inserted_dim_date_key as datetime) InsertedDateTime
  from #magento_sales r
  join marketing.v_dim_club dim_club on r.payroll_dim_club_key= dim_club.dim_club_key
  join marketing.v_dim_employee dim_employee on  r.dim_employee_key = dim_employee.dim_employee_key
  join marketing.v_dim_mms_member dim_member on r.dim_mms_member_key = dim_member.dim_mms_member_key
  left join marketing.v_dim_employee customer_dim_employee on dim_member.member_id = customer_dim_employee.member_id
  join marketing.v_dim_date dd on r.transaction_dim_date_key = dd.dim_date_key
  join #DimDate on r.udw_inserted_dim_date_key = #DimDate.week_dim_date_key
  left join (select d.fact_magento_order_item_key, oi.order_item_id 
               from d_magento_lifetime_order_item_change_log d 
               join fact_magento_order_item oi 
                 on d.fact_magento_order_item_key = oi.fact_magento_order_item_key 
              where d.transaction_type = 'MMS Transaction' 
                and d.mms_tran_id is not null 
              group by d.fact_magento_order_item_key,
                      oi.order_item_id) package
    on r.order_item_id = package.order_item_id
 where (dim_member.member_id <> dim_employee.member_id or dim_member.member_id is null) --no self sales
   and (r.payroll_description <> 'PT Commissionable Sales and Service' --Include all things not PT
        or (r.payroll_description = 'PT Commissionable Sales and Service' and package.fact_magento_order_item_key is not null) --Include all things PT service
        or (r.payroll_description = 'PT Commissionable Sales and Service' and package.fact_magento_order_item_key is null and customer_dim_employee.dim_employee_key is null ) --Include PT sales where customer is not an employee
       )
       
drop table #magento_sales
drop table #dimdate

end
