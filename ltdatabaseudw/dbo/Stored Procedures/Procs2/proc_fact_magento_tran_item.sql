CREATE PROC [dbo].[proc_fact_magento_tran_item] @dv_batch_id [bigint] AS
begin

set xact_abort on
set nocount on

/*d_magento_eav_attribute_option_value(option_value--fulfillment_partner). One to One relationship option_id-->option_value(join won't return Dups)*/
if object_id('tempdb..#eaov') is not null drop table #eaov
create table dbo.#eaov with(distribution=hash(option_id), location=user_db) as 
	select distinct option_id,option_value from d_magento_eav_attribute_option_value
	
/*sales*/
if object_id('tempdb..#s') is not null drop table #s
create table dbo.#s with(distribution=hash(unique_key), location=user_db) as 
select case when soi.bk_hash in ('-999','-998','-997') then soi.bk_hash
            else 's'+soi.bk_hash end unique_key,
       's'+so.bk_hash unique_parent_key,
       so.bk_hash order_key, /*order/refund key*/
       so.order_id,
       so.order_number,
       soi.bk_hash order_item_key,
       soi.order_item_id,
       isnull(so.sales_order_base_shipping_amount,0) base_shipping_amount,
       isnull(so.sales_order_base_shipping_tax_amount,0) base_shipping_tax_amount,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.qty_ordered
            else soi.qty_ordered end qty,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_cost
            else soi.base_cost end base_cost,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_price
            else soi.base_price end base_price,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_row_invoiced
            else soi.base_row_invoiced end base_row_invoiced,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_row_total
            else soi.base_row_total end base_row_total,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_tax_amount
            else soi.base_tax_amount end base_tax_amount,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_discount_amount
            else soi.base_discount_amount end base_discount_amount,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.sales_order_item_lt_bucks_redeemed
            else soi.sales_order_item_lt_bucks_redeemed end base_bucks_amount,
       soi.dim_club_key,
       so.dim_magento_customer_key,
       so.dim_mms_member_key,
       so.d_workday_employee_trainer_bk_hash dim_employee_key,
       soi.dim_magento_product_key,
       so.sales_order_base_currency_code currency_code,
       p.cc_type payment_type,
       p.cc_last_4 cc_last_4,
       p.batch_number batch_number,
       p.credit_tran_id credit_tran_id,
       soi.sales_order_item_product_type product_type, /*product_type*/
       so.d_magento_sales_order_address_bk_hash, /*req'd for shipping_state*/
       'N' refund_flag,
       case when psoi.bk_hash is not null then psoi.bk_hash else soi.bk_hash end order_item_shipment_bk_hash, /*parents get shipped*/
       soi.is_virtual_flag,
       so.status order_status,
       so.created_at order_datetime,
       convert(varchar(8),so.created_at,112) order_dim_date_key,
       so.created_at,
       so.updated_at,
/*UDW_11477 adding  missing columns from magento*/
	   so.coupon_code,
	   dmp.manufacturer,
	   (case when (psoi.sales_order_item_product_type = 'configurable') then (case when (psoi.qty_refunded = psoi.qty_ordered) then 'REFUNDED'
			when (psoi.qty_refunded > 0) then 'PARTIALLY_REFUNDED'
			when (psoi.qty_shipped = psoi.qty_ordered) then 'SHIPPED'
			when (psoi.qty_shipped > 0) then 'PARTIALLY_SHIPPED'
			when (psoi.qty_invoiced = psoi.qty_ordered) then 'INVOICED'
			when (psoi.qty_invoiced > 0) then 'PARTIALLY_INVOICED'
			when (psoi.sales_order_item_qty_canceled > 0) then 'CANCELED'
			when (so.status = 'holded') then 'ON_HOLD'
			else 'NEW' end)
			when (soi.qty_refunded = soi.qty_ordered) then 'REFUNDED'
			when (soi.qty_refunded > 0) then 'PARTIALLY_REFUNDED'
			when (soi.qty_shipped = soi.qty_ordered) then 'SHIPPED'
			when (soi.qty_shipped > 0) then 'PARTIALLY_SHIPPED'
			when (soi.qty_invoiced = soi.qty_ordered) then 'INVOICED'
			when (soi.qty_invoiced > 0) then 'PARTIALLY_INVOICED'
			when (soi.sales_order_item_qty_canceled > 0) then 'CANCELED'
			when (so.status = 'holded') then 'ON_HOLD'
			else 'NEW' end) AS item_status,
		(case when (psoi.sales_order_item_product_type = 'configurable') then psoi.qty_invoiced else soi.qty_invoiced end) AS items_invoiced,
		eaov.option_value as fulfillment_partner
  from d_magento_sales_order so
  join d_magento_sales_order_item soi 
    on so.bk_hash = soi.d_magento_sales_order_bk_hash
  join #eaov eaov /*UDW_11477 adding  missing columns from magento*/
    on eaov.option_id = soi.vendor
  join d_magento_sales_order_payment p 
    on so.bk_hash = p.fact_magento_sales_order_key
	/*UDW_11477 adding  missing columns from magento*/
  left join dim_magento_product dmp
	on dmp.dim_magento_product_key=soi.dim_magento_product_key
  left join d_magento_sales_order_item psoi /*attach parent*/
    on soi.parent_fact_magento_order_item_key = psoi.bk_hash 
   and psoi.bk_hash not in ('-999','-998','-997')
 where soi.sales_order_item_product_type not in ('configurable') /*remove configurable */

/*more sale transformations, namely ship date*/
if object_id('tempdb..#s2') is not null drop table #s2
create table dbo.#s2 with(distribution=hash(unique_key), location=user_db) as 
   select #s.unique_key,
       #s.order_key,
       #s.order_id,
       #s.order_number,
       #s.order_item_key,
       #s.order_item_id,
       #s.base_shipping_amount,
       #s.base_shipping_tax_amount,
       #s.qty,
       isnull(#s.base_cost,0) base_cost,
       isnull(#s.base_price,0) base_price,
       isnull(#s.base_row_invoiced,0) base_row_total,
       isnull(#s.base_tax_amount,0) base_tax_amount,
       isnull(#s.base_discount_amount,0) base_discount_amount,
       isnull(#s.base_bucks_amount,0) base_bucks_amount,
       #s.dim_club_key,
       #s.dim_magento_customer_key,
       #s.dim_mms_member_key,
       #s.dim_employee_key,
       #s.dim_magento_product_key,
       #s.currency_code,
       #s.payment_type,
       #s.cc_last_4,
       #s.batch_number,
       #s.credit_tran_id,
       #s.refund_flag,
       case when isnull(#s.base_row_invoiced,0) = 0 then 0
            else isnull(#s.base_row_invoiced,0) - isnull(#s.base_discount_amount,0) + isnull(#s.base_tax_amount,0) - isnull(#s.base_bucks_amount,0) 
        end transaction_amount,
       isnull(#s.base_row_total,0) - isnull(#s.base_discount_amount,0) payroll_amount,
       #s.order_datetime,
       #s.order_dim_date_key,
       case when ship_date.fact_magento_order_item_key is not null then ship_date.shipment_date /*item was shipped*/
            when #s.is_virtual_flag = 'Y' then #s.created_at /*virtual products are not shipped, consider them shipped immediately*/
            when #s.order_status = 'complete' then #s.updated_at /*if the order is complete, assume everything without a shipment record has shipped, this needs to be saved in IDK*/
            else NULL
        end ship_date,
       #s.order_status,
	   case when #s.order_status = 'canceled' then #s.updated_at else null end canceled_datetime,
      row_number() over (partition by #s.order_key order by #s.order_item_id) r,
	  #s.d_magento_sales_order_address_bk_hash,
	  #s.coupon_code,/*UDW_11477 adding  missing columns from magento*/
	  #s.manufacturer,
	  #s.item_status,
	  #s.items_invoiced,
	  #s.fulfillment_partner
from #s
left join (select si.fact_magento_order_item_key, min(s.created_at) shipment_date, count(*) c /*duplicate shipment records exist, assume an order item is fully shipped at once*/
             from d_magento_sales_shipment_item si
             join d_magento_sales_shipment s 
               on si.d_magento_sales_shipment_bk_hash = s.bk_hash
            where si.bk_hash not in ('-998')
            group by si.fact_magento_order_item_key) ship_date /*virtual products are not shipped, just use the order date*/
  on #s.order_item_shipment_bk_hash = ship_date.fact_magento_order_item_key
  
/*refunds*/
if object_id('tempdb..#r') is not null drop table #r
create table dbo.#r with(distribution=hash(unique_key), location=user_db) as 
select case when soi.bk_hash in ('-999','-998','-997') then soi.bk_hash
            else 'r'+soi.bk_hash end unique_key,
       'r'+so.bk_hash unique_parent_key,
       so.bk_hash order_key, /*order/refund key*/
       so.order_id,
       so.order_number,
       soi.bk_hash order_item_key,
       soi.order_item_id,
       isnull(so.sales_order_base_shipping_refunded,0) base_shipping_refunded,
       isnull(so.sales_order_base_shipping_tax_refunded,0) base_shipping_tax_refunded,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.qty_refunded
            else soi.qty_refunded end qty,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_cost
            else soi.base_cost end base_cost,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_price
            else soi.base_price end base_price,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_amount_refunded
            else soi.base_amount_refunded end base_amount_refunded,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.sales_order_item_base_tax_refunded
            else soi.sales_order_item_base_tax_refunded end base_tax_refunded,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.base_discount_refunded
            else soi.base_discount_refunded end base_discount_refunded,
       case when psoi.sales_order_item_product_type = 'configurable' then psoi.sales_order_item_lt_bucks_refunded
            else soi.sales_order_item_lt_bucks_refunded end base_bucks_refunded,
       soi.dim_club_key,
       so.dim_magento_customer_key,
       so.dim_mms_member_key,
       so.d_workday_employee_trainer_bk_hash dim_employee_key,
       soi.dim_magento_product_key,
       so.sales_order_base_currency_code currency_code,
       p.cc_type payment_type,
       p.cc_last_4 cc_last_4,
       p.batch_number batch_number,
       p.credit_tran_id credit_tran_id,
       soi.sales_order_item_product_type product_type, /*product_type*/
       so.d_magento_sales_order_address_bk_hash, /*shipping_state*/
       'Y' refund_flag,
       case when psoi.bk_hash is not null then psoi.bk_hash else soi.bk_hash end order_item_shipment_bk_hash,
       soi.is_virtual_flag,
       so.status order_status,
       so.created_at order_datetime,
       convert(varchar(8),so.created_at,112) order_dim_date_key,
	   so.coupon_code,/*UDW_11477 adding  missing columns from magento*/
	   dmp.manufacturer,
	   	   (case when (psoi.sales_order_item_product_type = 'configurable') then (case when (psoi.qty_refunded = psoi.qty_ordered) then 'REFUNDED'
			when (psoi.qty_refunded > 0) then 'PARTIALLY_REFUNDED'
			when (psoi.qty_shipped = psoi.qty_ordered) then 'SHIPPED'
			when (psoi.qty_shipped > 0) then 'PARTIALLY_SHIPPED'
			when (psoi.qty_invoiced = psoi.qty_ordered) then 'INVOICED'
			when (psoi.qty_invoiced > 0) then 'PARTIALLY_INVOICED'
			when (psoi.sales_order_item_qty_canceled > 0) then 'CANCELED'
			when (so.status = 'holded') then 'ON_HOLD'
			else 'NEW' end)
			when (soi.qty_refunded = soi.qty_ordered) then 'REFUNDED'
			when (soi.qty_refunded > 0) then 'PARTIALLY_REFUNDED'
			when (soi.qty_shipped = soi.qty_ordered) then 'SHIPPED'
			when (soi.qty_shipped > 0) then 'PARTIALLY_SHIPPED'
			when (soi.qty_invoiced = soi.qty_ordered) then 'INVOICED'
			when (soi.qty_invoiced > 0) then 'PARTIALLY_INVOICED'
			when (soi.sales_order_item_qty_canceled > 0) then 'CANCELED'
			when (so.status = 'holded') then 'ON_HOLD'
			else 'NEW' end) AS item_status,
		(case when (psoi.sales_order_item_product_type = 'configurable') then psoi.qty_invoiced else soi.qty_invoiced end) AS items_invoiced,
 		eaov.option_value as fulfillment_partner		
  from d_magento_sales_order so
  join d_magento_sales_order_item soi 
    on so.bk_hash = soi.d_magento_sales_order_bk_hash
  join #eaov eaov
    on eaov.option_id = soi.vendor
  join d_magento_sales_order_payment p 
    on so.bk_hash = p.fact_magento_sales_order_key
		/*UDW_11477 adding  missing columns from magento*/
  left join dim_magento_product dmp
	on dmp.dim_magento_product_key=soi.dim_magento_product_key
  left join d_magento_sales_order_item psoi /*attach parent*/
    on soi.parent_fact_magento_order_item_key = psoi.bk_hash 
   and psoi.bk_hash not in ('-999','-998','-997')
 where soi.sales_order_item_product_type not in ('configurable') /*remove configurable */
   and soi.qty_refunded > 0 /*refunds*/
   and so.bk_hash not in ('-999','-998','-997')

/*more refund transformations, add credit memo*/
if object_id('tempdb..#r2') is not null drop table #r2
create table dbo.#r2 with(distribution=hash(unique_key), location=user_db) as 
select distinct  /*ew, gross*/
       #r.unique_key,
       #r.order_key,
       #r.order_id,
       #r.order_number,
       #r.order_item_key,
       #r.order_item_id,
       -1 * abs(#r.base_shipping_refunded) base_shipping_refunded,
       -1 * abs(#r.base_shipping_tax_refunded) base_shipping_tax_refunded,
       -1 * abs(#r.qty) qty,
       -1 * abs(isnull(#r.base_cost,0)) base_cost,
       -1 * abs(isnull(#r.base_price,0)) base_price,
       -1 * abs(isnull(#r.base_amount_refunded,0)) base_amount_refunded,
       -1 * abs(isnull(#r.base_tax_refunded,0)) base_tax_refunded,
       -1 * abs(isnull(#r.base_discount_refunded,0)) base_discount_refunded,
       -1 * abs(isnull(#r.base_bucks_refunded,0)) base_bucks_refunded,
       #r.dim_club_key,
       #r.dim_magento_customer_key,
       #r.dim_mms_member_key,
       #r.dim_employee_key,
       #r.dim_magento_product_key,
       #r.currency_code,
       #r.payment_type,
       #r.cc_last_4,
       #r.batch_number,
       #r.credit_tran_id,
       #r.refund_flag,
       -1 * abs (case when isnull(#r.base_amount_refunded,0) = 0 then 0
                      else isnull(#r.base_amount_refunded,0) - isnull(#r.base_discount_refunded,0) + isnull(#r.base_tax_refunded,0) - isnull(#r.base_bucks_refunded,0) 
                 end) transaction_refund_amount,
       -1 * abs (isnull(#r.base_amount_refunded,0) - isnull(#r.base_discount_refunded,0)) payroll_refund_amount,
       #r.order_datetime,
       #r.order_dim_date_key,
       cm.created_at refund_datetime,
       cm.increment_id refund_number,
       #r.order_status,
       row_number() over (partition by #r.order_key order by #r.order_item_id) r,
	   #r.d_magento_sales_order_address_bk_hash,
	   #r.coupon_code,/*UDW_11477 adding  missing columns from magento*/
	   #r.manufacturer,
	   #r.item_status,
	   #r.items_invoiced,
	   #r.fulfillment_partner
from #r
join d_magento_sales_credit_memo_item cmi
  on #r.order_item_key = cmi.fact_magento_order_item_key
join d_magento_sales_credit_memo cm
  on cm.bk_hash = cmi.d_magento_sales_credit_memo_bk_hash

delete from #r2 where transaction_refund_amount = 0 and base_bucks_refunded = 0 /*valid??*/

if object_id('tempdb..#t') is not null drop table #t
create table dbo.#t with(distribution=hash(unique_key), location=user_db) as  
select unique_key,
       order_number,
       order_id,
       null refund_number,
       order_item_id,
       order_key,
       order_item_key,
       case when r =1 then base_shipping_amount else 0 end base_shipping_amount,
       case when r = 1 then base_shipping_tax_amount else 0 end base_shipping_tax_amount,
       qty,
       base_cost,
       base_price,
       base_row_total,
       base_tax_amount,
       base_discount_amount,
       base_bucks_amount,
       dim_club_key,
       dim_magento_customer_key,
       dim_mms_member_key,
       dim_employee_key,
       dim_magento_product_key,
       currency_code,
       payment_type,
       cc_last_4,
       batch_number,
       credit_tran_id,
       refund_flag,
       transaction_amount,
	   case when payroll_amount - base_bucks_amount < 0 then 0 else payroll_amount - base_bucks_amount end payroll_amount,
       order_datetime,
       order_dim_date_key,
       order_status,
       ship_date allocated_datetime,
	   canceled_datetime,
	   d_magento_sales_order_address_bk_hash,
	   coupon_code,
	   manufacturer,
	   item_status,
	   items_invoiced,
	   fulfillment_partner
from #s2
union
select unique_key,
       order_number,
       order_id,
       refund_number,
       order_item_id,
       order_key,
       order_item_key,
       case when r = 1 then base_shipping_refunded else 0 end base_shipping_refunded,
       case when r =1 then base_shipping_tax_refunded else 0 end base_shipping_tax_refunded,
       qty,
       base_cost,
       base_price,
       base_amount_refunded,
       base_tax_refunded,
       base_discount_refunded,
       base_bucks_refunded,
       dim_club_key,
       dim_magento_customer_key,
       dim_mms_member_key,
       dim_employee_key,
       dim_magento_product_key,
       currency_code,
       payment_type,
       cc_last_4,
       batch_number,
       credit_tran_id,
       refund_flag,
       transaction_refund_amount,
	   case when payroll_refund_amount - base_bucks_refunded > 0 then 0 else payroll_refund_amount - base_bucks_refunded end payroll_refund_amount,
       order_datetime,
       order_dim_date_key,
       order_status,
       refund_datetime allocated_datetime,
	   NULL canceled_datetime,
	   d_magento_sales_order_address_bk_hash,
	   coupon_code,
	   manufacturer,
	   item_status,
	   items_invoiced,
	   fulfillment_partner
from #r2


/*shipping_state*/
if object_id('tempdb..#shipping_state') is not null drop table #shipping_state
create table dbo.#shipping_state with(distribution=hash(unique_key), location=user_db) as  
select #t.unique_key,
       isnull(isnull(isnull(r1.abbreviation,r2.abbreviation),r3.abbreviation),r4.abbreviation) shipping_state
from #t
left join d_magento_sales_order_address sa on #t.d_magento_sales_order_address_bk_hash = sa.bk_hash
left join r_mms_val_state r1 on sa.region = r1.description and r1.dv_load_end_date_time = 'dec 31, 9999'
left join r_mms_val_state r2 on sa.region = r2.abbreviation and r2.dv_load_end_date_time = 'dec 31, 9999'
left join d_magento_sales_order_address ba on #t.d_magento_sales_order_address_bk_hash= ba.bk_hash
left join r_mms_val_state r3 on ba.region = r3.description and r3.dv_load_end_date_time = 'dec 31, 9999'
left join r_mms_val_state r4 on ba.region = r4.abbreviation and r4.dv_load_end_date_time = 'dec 31, 9999'

/*denormalized mms_tran_ids, deprecated*/
/*
if object_id('tempdb..#mms_tran_id') is not null drop table #mms_tran_id
create table dbo.#mms_tran_id with(distribution=hash(fact_magento_order_item_key), location=user_db) as
with mms_tran_id (fact_magento_order_item_key,mms_tran_id,r) 
as
(
        select fact_magento_order_item_key,
               cast(mms_tran_id as varchar(255)) mms_tran_id,
               row_number() over (partition by fact_magento_order_item_key order by mms_tran_id) r
        from d_magento_lifetime_order_item_change_log
        /*where fact_magento_order_item_key in (select fact_magento_order_item_key from #invoice)*/
)
select m1.fact_magento_order_item_key,
       m1.mms_tran_id + isnull(','+m2.mms_tran_id,'')+ isnull(','+m3.mms_tran_id,'')+ isnull(','+m4.mms_tran_id,'')+ isnull(','+m5.mms_tran_id,'') as mms_tran_id
  from mms_tran_id m1
  left join mms_tran_id m2 on m1.fact_magento_order_item_key = m2.fact_magento_order_item_key and m1.r+1 = m2.r
  left join mms_tran_id m3 on m1.fact_magento_order_item_key = m3.fact_magento_order_item_key and m1.r+2 = m3.r
  left join mms_tran_id m4 on m1.fact_magento_order_item_key = m4.fact_magento_order_item_key and m1.r+3 = m4.r
  left join mms_tran_id m5 on m1.fact_magento_order_item_key = m5.fact_magento_order_item_key and m1.r+4 = m5.r
where m1.r = 1
*/


if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(unique_key)) as 
select #t.unique_key,
       #t.order_dim_date_key,
       #t.dim_employee_key,
       #t.dim_mms_member_key,
       d_mms_member.dim_mms_membership_key dim_mms_membership_key,
       dim_mms_membership_history.home_dim_club_key membership_dim_club_key,
       d_mms_employee_history.dim_club_key employee_dim_club_key,
       corporate_club.dim_club_key corporate_dim_club_key,
       #t.dim_club_key,
       dim_date.month_ending_dim_date_key transaction_month_ending_dim_date_key,
       d_mms_member.member_id,
       d_mms_employee_history.employee_id,
       #t.currency_code
  from #t
  left join dim_date
    on #t.order_dim_date_key = dim_date.dim_date_key
  left join d_mms_member
    on #t.dim_mms_member_key = d_mms_member.dim_mms_member_key
  left join dim_mms_membership_history
    on d_mms_member.dim_mms_membership_key = dim_mms_membership_history.dim_mms_membership_key
   and dim_mms_membership_history.dim_mms_membership_key not in ('-998','-997','-998')
   and dim_mms_membership_history.effective_date_time <= dim_date.next_month_starting_date/*jan 1, 12:00:00.000*/
   and dim_mms_membership_history.expiration_date_time > dim_date.next_month_starting_date/*jan 1, 12:00:00.000*/
  left join d_mms_employee_history
    on #t.dim_employee_key = d_mms_employee_history.dim_employee_key
   and d_mms_employee_history.bk_hash not in ('-998','-997','-998')
   and d_mms_employee_history.effective_date_time <= dim_date.next_month_starting_date/*#etl_step_1.invoice_datetime*/
   and d_mms_employee_history.expiration_date_time > dim_date.next_month_starting_date/*#etl_step_1.invoice_datetime*/
  join dim_club corporate_club
    on corporate_club.club_id = 13
  
if object_id('tempdb..#etl_step_2') is not null drop table #etl_step_2
create table dbo.#etl_step_2 with(distribution=hash(unique_key)) as 
select #etl_step_1.unique_key,
       #etl_step_1.dim_mms_member_key,
       #etl_step_1.dim_mms_membership_key,
       #etl_step_1.dim_club_key,
       #etl_step_1.transaction_month_ending_dim_date_key,
       #etl_step_1.order_dim_date_key,
       #etl_step_1.dim_employee_key,
       dim_club.club_id,
       isnull(dim_club.local_currency_code,'USD') original_currency_code,

       case when #etl_step_1.dim_club_key not in ('-999','-998','-997') and dim_club.club_id <> 13 and (dim_club.club_close_dim_date_key = '-998' or dim_club.club_close_dim_date_key >= #etl_step_1.order_dim_date_key)
                 then dim_club.dim_club_key
            when #etl_step_1.dim_employee_key not in ('-999','-998','-997') and employee_club.club_id <> 13 and (employee_club.club_close_dim_date_key = '-998' or employee_club.club_close_dim_date_key >= #etl_step_1.order_dim_date_key)
                 then employee_club.dim_club_key
            else #etl_step_1.corporate_dim_club_key
        end payroll_dim_club_key,
       
       case when #etl_step_1.dim_club_key not in ('-999','-998','-997') and dim_club.club_id <> 13 and (dim_club.club_close_dim_date_key = '-998' or dim_club.club_close_dim_date_key >= #etl_step_1.order_dim_date_key)
                 then dim_club.dim_club_key
            when #etl_step_1.dim_employee_key not in ('-999','-998','-997') and employee_club.club_id <> 13 and (employee_club.club_close_dim_date_key = '-998' or employee_club.club_close_dim_date_key >= #etl_step_1.order_dim_date_key)
                 then employee_club.dim_club_key
            when #etl_step_1.dim_mms_member_key not in ('-999','-998','-997') and membership_club.club_id <> 13 and (membership_club.club_close_dim_date_key = '-998' or membership_club.club_close_dim_date_key >= #etl_step_1.order_dim_date_key)
                 then membership_club.dim_club_key
            else #etl_step_1.corporate_dim_club_key
        end transaction_reporting_dim_club_key,
       #etl_step_1.membership_dim_club_key,
       #etl_step_1.employee_dim_club_key,
       #etl_step_1.member_id,
       #etl_step_1.employee_id,
       #etl_step_1.currency_code
from #etl_step_1
left join dim_club
  on #etl_step_1.dim_club_key = dim_club.dim_club_key
left join dim_club membership_club
  on #etl_step_1.membership_dim_club_key = membership_club.dim_club_key
left join dim_club employee_club
  on #etl_step_1.employee_dim_club_key = employee_club.dim_club_key

if object_id('tempdb..#etl_step_3') is not null drop table #etl_step_3
create table dbo.#etl_step_3 with(distribution=hash(unique_key), location=user_db) as
select #etl_step_2.unique_key,
       #etl_step_2.dim_mms_member_key,
       #etl_step_2.dim_mms_membership_key,
       #etl_step_2.dim_club_key,
       #etl_step_2.club_id,
       #etl_step_2.currency_code,
       #etl_step_2.payroll_dim_club_key,
       #etl_step_2.transaction_reporting_dim_club_key,
       case when #etl_step_2.unique_key in ('-997', '-998', '-999') and #etl_step_2.unique_key in ('-997', '-998', '-999')
                 then #etl_step_2.unique_key
            when #etl_step_2.order_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_2.transaction_month_ending_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(#etl_step_2.original_currency_code,'USD'),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
        end usd_monthly_average_dim_exchange_rate_key,
       case when #etl_step_2.unique_key in ('-997', '-998', '-999') and #etl_step_2.unique_key in ('-997', '-998', '-999')
                 then #etl_step_2.unique_key
            when #etl_step_2.order_dim_date_key is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_2.order_dim_date_key),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull(isnull(#etl_step_2.original_currency_code,'USD'),'z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('USD','z#@$k%&P')+
                                                   'P%#&z$@k'+isnull('Daily Exchange Rate','z#@$k%&P'))),2)
        end usd_daily_dim_exchange_rate_key,
      case when #etl_step_2.unique_key in ('-997', '-998', '-999') and #etl_step_2.unique_key in ('-997', '-998', '-999')
                then #etl_step_2.unique_key
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_2.original_currency_code,'z#@$k%&P')+
                                               'P%#&z$@k'+isnull('USD','z#@$k%&P'))),2)
       end usd_dim_plan_exchange_rate_key,
      case when #etl_step_2.unique_key in ('-997', '-998', '-999') and #etl_step_2.unique_key in ('-997', '-998', '-999')
                then #etl_step_2.unique_key
           when #etl_step_2.order_dim_date_key is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(convert(varchar,#etl_step_2.transaction_month_ending_dim_date_key),'z#@$k%&P')+
                                                  'P%#&z$@k'+isnull(isnull(#etl_step_2.original_currency_code,'USD'),'z#@$k%&P')+
                                                  'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P')+
                                                  'P%#&z$@k'+isnull('Monthly Average Exchange Rate','z#@$k%&P'))),2)
       end transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
      case when #etl_step_2.unique_key in ('-997', '-998', '-999') and #etl_step_2.unique_key in ('-997', '-998', '-999')
                then #etl_step_2.unique_key
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(#etl_step_2.original_currency_code,'z#@$k%&P')+
                                               'P%#&z$@k'+isnull(dim_club.local_currency_code,'z#@$k%&P'))),2)
       end transaction_reporting_local_currency_dim_plan_exchange_rate_key,
       #etl_step_2.member_id,
       #etl_step_2.employee_id
from #etl_step_2
left join dim_club
  on #etl_step_2.transaction_reporting_dim_club_key = dim_club.dim_club_key
left join dim_club membership_club
  on #etl_step_2.membership_dim_club_key = membership_club.dim_club_key
left join dim_club employee_club
  on #etl_step_2.employee_dim_club_key = employee_club.dim_club_key

declare @get_date_time datetime = (select get_datetime from v_get_date)
declare @get_date_dim_date_key int = (select convert(varchar(8),@get_date_time,112))

if object_id('tempdb..#final_step') is not null drop table #final_step
create table dbo.#final_step with(distribution=hash(unique_key), location=user_db) as
select #t.transaction_amount allocated_amount,
       transaction_reporting_dim_club_key allocated_dim_club_key,
       case when idk_fact_magento_tran_item.allocated_datetime is null then #t.allocated_datetime
            else idk_fact_magento_tran_item.allocated_datetime
        end allocated_datetime,
       case when idk_fact_magento_tran_item.allocated_datetime is null then convert(varchar(8),#t.allocated_datetime,112)
            else convert(varchar(8),idk_fact_magento_tran_item.allocated_datetime,112)
        end allocated_dim_date_key,
       case when idk_fact_magento_tran_item.allocated_datetime is null then dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,#t.allocated_datetime)),0)) 
            else dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,idk_fact_magento_tran_item.allocated_datetime)),0))
        end allocated_recalculate_through_datetime,
       case when idk_fact_magento_tran_item.allocated_datetime is null then convert(varchar(8),dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,#t.allocated_datetime)),0)),112)
            else convert(varchar(8),dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,idk_fact_magento_tran_item.allocated_datetime)),0)),112)
        end allocated_recalculate_through_dim_date_key,
       case when idk_fact_magento_tran_item.allocated_datetime is null then convert(varchar(8),DATEADD(mm,DATEDIFF(mm,0,#t.allocated_datetime),0),112)
            else convert(varchar(8),DATEADD(mm,DATEDIFF(mm,0,idk_fact_magento_tran_item.allocated_datetime),0),112)
        end allocated_month_starting_dim_date_key,
       #t.batch_number,
       case when idk_fact_magento_tran_item.canceled_datetime is null then #t.canceled_datetime
            else idk_fact_magento_tran_item.canceled_datetime
        end canceled_datetime,
       case when idk_fact_magento_tran_item.canceled_datetime is null then convert(varchar(8),#t.canceled_datetime,112)
            else convert(varchar(8),idk_fact_magento_tran_item.canceled_datetime,112)
        end canceled_dim_date_key,
       #t.cc_last_4,
       #t.credit_tran_id,
       #t.dim_club_key,
       #t.dim_employee_key,
       #t.dim_magento_customer_key,
       #t.dim_magento_product_key,
       #t.dim_mms_member_key,
       dim_mms_membership_key,
       employee_id,
       member_id,
       #t.order_datetime,
       #t.order_dim_date_key,
       #t.order_id,
       #t.order_item_id,
       #t.order_number,
       #t.order_status,
       #t.currency_code original_currency_code,
       #t.payment_type,
	   #t.payroll_amount,
       payroll_dim_club_key,
       #t.base_cost product_cost,
       #t.base_price product_price,
       #t.refund_flag,
       #t.refund_number,
       #t.base_shipping_amount shipping_amount,
       ss.shipping_state,
       #t.base_shipping_tax_amount shipping_tax_amount,
       #t.transaction_amount,
       #t.base_bucks_amount transaction_bucks_amount,
       #t.base_discount_amount transaction_discount_amount,
       #t.base_row_total  transaction_item_amount,
       #t.qty transaction_quantity,
       transaction_reporting_dim_club_key,
       transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
       #t.base_tax_amount transaction_tax_amount,
       case when idk_fact_magento_tran_item.unique_key is null then @get_date_time
            else idk_fact_magento_tran_item.udw_inserted_datetime
        end udw_inserted_datetime,
       case when idk_fact_magento_tran_item.unique_key is null then convert(varchar(8),@get_date_time,112)
            else convert(varchar(8),idk_fact_magento_tran_item.udw_inserted_datetime,112)
        end udw_inserted_dim_date_key,
       #t.unique_key,
       usd_daily_dim_exchange_rate_key,
       usd_dim_plan_exchange_rate_key,
       usd_monthly_average_dim_exchange_rate_key,
       getdate() dv_load_date_time,
       'dec 31, 9999' dv_load_end_date_time,
       @dv_batch_id dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user,
	   #t.coupon_code,
	   #t.manufacturer,
	   #t.item_status,
	   #t.items_invoiced,
	   #t.fulfillment_partner
from #t
join #etl_step_3 on #t.unique_key = #etl_step_3.unique_key
left join #shipping_state ss on #t.unique_key = ss.unique_key
left join idk_fact_magento_tran_item on #t.unique_key = idk_fact_magento_tran_item.unique_key

truncate table dbo.idk_fact_magento_tran_item

insert into idk_fact_magento_tran_item (
    unique_key,
    udw_inserted_datetime,
    allocated_datetime,
	canceled_datetime,
    dv_batch_id
)
select unique_key,
       udw_inserted_datetime,
       allocated_datetime,
	   canceled_datetime,
       dv_batch_id
  from #final_step

truncate table dbo.fact_magento_tran_item

insert into fact_magento_tran_item (
    allocated_amount,
    allocated_datetime,
	allocated_dim_date_key,
    allocated_dim_club_key,
    allocated_month_starting_dim_date_key,
    allocated_recalculate_through_datetime,
    allocated_recalculate_through_dim_date_key,
    batch_number,
	canceled_datetime,
	canceled_dim_date_key,
    cc_last_4,
    credit_tran_id,
    dim_club_key,
    dim_employee_key,
    dim_magento_customer_key,
    dim_magento_product_key,
    dim_mms_member_key,
    dim_mms_membership_key,
    employee_id,
    member_id,
    order_datetime,
    order_dim_date_key,
    order_id,
    order_item_id,
    order_number,
    order_status,
    original_currency_code,
    payment_type,
	payroll_amount,
    payroll_dim_club_key,
    product_cost,
    product_price,
    refund_flag,
    refund_number,
    shipment_datetime,
	shipment_dim_date_key,
    shipping_amount,
    shipping_state,
    shipping_tax_amount,
    transaction_amount,
    transaction_bucks_amount,
    transaction_discount_amount,
    transaction_item_amount,
    transaction_quantity,
    transaction_reporting_dim_club_key,
    transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
    transaction_tax_amount,
    udw_inserted_datetime,
    udw_inserted_dim_date_key,
    unique_key,
    usd_daily_dim_exchange_rate_key,
    usd_dim_plan_exchange_rate_key,
    usd_monthly_average_dim_exchange_rate_key,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user,
	voucher_code,/*coupon_code,*/
	manufacturer,
	is_autoship,
	item_status,
	items_invoiced,
	fulfillment_partner)
select allocated_amount,
       allocated_datetime,
	   allocated_dim_date_key,
       allocated_dim_club_key,
       allocated_month_starting_dim_date_key,
       allocated_recalculate_through_datetime,
       allocated_recalculate_through_dim_date_key,
       batch_number,
	   canceled_datetime,
	   canceled_dim_date_key,
       cc_last_4,
       credit_tran_id,
       dim_club_key,
       dim_employee_key,
       dim_magento_customer_key,
       dim_magento_product_key,
       dim_mms_member_key,
       dim_mms_membership_key,
       employee_id,
       member_id,
       order_datetime,
       order_dim_date_key,
       order_id,
       order_item_id,
       order_number,
       order_status,
       original_currency_code,
       payment_type,
	   payroll_amount,
       payroll_dim_club_key,
       product_cost,
       product_price,
       refund_flag,
       refund_number,
       case when refund_flag = 'N' then allocated_datetime 
            else null end shipment_datetime,
       case when refund_flag = 'N' then allocated_dim_date_key
            else '-998' end shipment_dim_date_key,
       shipping_amount,
       shipping_state,
       shipping_tax_amount,
       transaction_amount,
       transaction_bucks_amount,
       transaction_discount_amount,
       transaction_item_amount,
       transaction_quantity,
       transaction_reporting_dim_club_key,
       transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
       transaction_tax_amount,
       udw_inserted_datetime,
       udw_inserted_dim_date_key,
       unique_key,
       usd_daily_dim_exchange_rate_key,
       usd_dim_plan_exchange_rate_key,
       usd_monthly_average_dim_exchange_rate_key,
       dv_load_date_time,
       dv_load_end_date_time,
       dv_batch_id,
       dv_inserted_date_time,
       dv_insert_user,
	   coupon_code,
	   manufacturer,
	   0 AS is_autoship,
	   item_status,
	   items_invoiced,
	   fulfillment_partner
  from #final_step
end
