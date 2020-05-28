CREATE PROC [dbo].[proc_d_hybris_seven_day_orders] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_hybris_seven_day_orders)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_hybris_seven_day_orders_insert') is not null drop table #p_hybris_seven_day_orders_insert
create table dbo.#p_hybris_seven_day_orders_insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_seven_day_orders.p_hybris_seven_day_orders_id,
       p_hybris_seven_day_orders.bk_hash
  from dbo.p_hybris_seven_day_orders
 where p_hybris_seven_day_orders.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_hybris_seven_day_orders.dv_batch_id > @max_dv_batch_id
        or p_hybris_seven_day_orders.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_seven_day_orders.bk_hash,
       p_hybris_seven_day_orders.order_code order_code,
       p_hybris_seven_day_orders.order_entry_entry_number entry_number,
       l_hybris_seven_day_orders.affiliate_id affiliate_id,
       case when s_hybris_seven_day_orders.order_auto_ship_flag = 1 then 'Y' else 'N' end auto_ship_flag,
       s_hybris_seven_day_orders.order_entry_refund_amount base_refund_amount,
       isnull(s_hybris_seven_day_orders.capture_amex,0) capture_amex,
       isnull(s_hybris_seven_day_orders.capture_discover,0) capture_discover,
       isnull(s_hybris_seven_day_orders.capture_lt_bucks,0) capture_lt_bucks,
       isnull(s_hybris_seven_day_orders.capture_master,0) capture_master,
       isnull(s_hybris_seven_day_orders.capture_paypal,0) capture_paypal,
       isnull(s_hybris_seven_day_orders.capture_visa,0) capture_visa,
       l_hybris_seven_day_orders.order_commision_employee_id commission_employee_id,
       s_hybris_seven_day_orders.customer_email customer_email,
       s_hybris_seven_day_orders.customer_group customer_group,
       s_hybris_seven_day_orders.customer_name customer_name,
       case when p_hybris_seven_day_orders.bk_hash in ('-997','-998','-999') then p_hybris_seven_day_orders.bk_hash
            when s_hybris_seven_day_orders.order_entry_product_code is not null then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(s_hybris_seven_day_orders.order_entry_product_code as nvarchar(258)),'z#@$k%&P'))),2)
            else '-998' 
        end dim_hybris_product_key,
       isnull(abs(s_hybris_seven_day_orders.order_entry_total_discounts),0) discount_amount,
       case when p_hybris_seven_day_orders.bk_hash in ('-997','-998','-999') then p_hybris_seven_day_orders.bk_hash
            when l_hybris_seven_day_orders.mms_transaction_id is not null then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_hybris_seven_day_orders.mms_transaction_id as int) as varchar(500)),'z#@$k%&P'))),2)
            else '-998' 
        end fact_mms_sales_transaction_key,
       s_hybris_seven_day_orders.fulfillment_partner fulfillment_partner,
       s_hybris_seven_day_orders.lt_bucks_earned lt_bucks_earned,
       l_hybris_seven_day_orders.order_ltf_party_id ltf_party_id,
       s_hybris_seven_day_orders.method_of_pay method_of_pay,
       l_hybris_seven_day_orders.mms_transaction_id mms_transaction_id,
       s_hybris_seven_day_orders.order_datetime order_datetime,
       convert(varchar(8), s_hybris_seven_day_orders.order_datetime, 112) order_dim_date_key,
       isnull(abs(s_hybris_seven_day_orders.order_entry_base_price),0) original_unit_price,
       s_hybris_seven_day_orders.order_entry_product_code product_code,
       isnull(abs(s_hybris_seven_day_orders.order_entry_total_price),0) purchase_unit_price,
       convert(varchar(8),DATEADD(mm,DATEDIFF(mm,0,s_hybris_seven_day_orders.order_entry_refund_datetime),0),112) refund_allocated_month_starting_dim_date_key,
       convert(varchar(8),dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_hybris_seven_day_orders.order_entry_refund_datetime)),0)),112) refund_allocated_recalculate_through_datetime,
       dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_hybris_seven_day_orders.order_entry_refund_datetime)),0)) refund_allocated_recalculate_through_dim_date_key,
       isnull(s_hybris_seven_day_orders.refund_amex,0) refund_amex,
       case when (isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) > s_hybris_seven_day_orders.order_entry_total_price) 
                 and (isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) <= (isnull(s_hybris_seven_day_orders.order_entry_total_price,0) + isnull(s_hybris_seven_day_orders.order_entry_total_tax,0) + isnull(s_hybris_seven_day_orders.order_entry_shipping_cost,0)))
                 then s_hybris_seven_day_orders.order_entry_total_price - isnull(s_hybris_seven_day_orders.capture_lt_bucks,0)
            when isnull(s_hybris_seven_day_orders.order_entry_refund_amount, 0) > (isnull(s_hybris_seven_day_orders.order_entry_total_price,0) + isnull(s_hybris_seven_day_orders.order_entry_total_tax, 0) + isnull(s_hybris_seven_day_orders.order_entry_shipping_cost,0)) 
                 then (s_hybris_seven_day_orders.order_entry_total_price + (isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) - isnull(s_hybris_seven_day_orders.order_entry_total_price,0) - isnull(s_hybris_seven_day_orders.order_entry_total_tax, 0) - isnull(s_hybris_seven_day_orders.order_entry_shipping_cost, 0))) - isnull(s_hybris_seven_day_orders.capture_lt_bucks, 0)
            else isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) - isnull(s_hybris_seven_day_orders.capture_lt_bucks,0)
       end refund_amount,
       case when isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) > isnull(s_hybris_seven_day_orders.order_entry_total_price,0) and isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) <= (isnull(s_hybris_seven_day_orders.order_entry_total_price,0) + isnull(s_hybris_seven_day_orders.order_entry_total_tax, 0) + isnull(s_hybris_seven_day_orders.order_entry_shipping_cost, 0))
                 then isnull(s_hybris_seven_day_orders.order_entry_total_price,0)
            when isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) > (isnull(s_hybris_seven_day_orders.order_entry_total_price,0) + isnull(s_hybris_seven_day_orders.order_entry_total_tax, 0) + isnull(s_hybris_seven_day_orders.order_entry_shipping_cost, 0))
                 then (isnull(s_hybris_seven_day_orders.order_entry_total_price,0) + (isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) - isnull(s_hybris_seven_day_orders.order_entry_total_price,0) - isnull(s_hybris_seven_day_orders.order_entry_total_tax, 0) - isnull(s_hybris_seven_day_orders.order_entry_shipping_cost, 0))) * isnull(s_hybris_seven_day_orders.order_entry_quantity,0)
            else isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0)
       end refund_amount_gross,
       s_hybris_seven_day_orders.order_entry_refund_datetime refund_datetime,
       isnull(s_hybris_seven_day_orders.refund_discover,0) refund_discover,
       case when p_hybris_seven_day_orders.bk_hash in ('-997', '-998', '-999') then p_hybris_seven_day_orders.bk_hash
                   else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(p_hybris_seven_day_orders.order_code,'z#@$k%&P')+
                                                          'P%#&z$@k'+isnull(convert(varchar(500),p_hybris_seven_day_orders.order_entry_entry_number),'z#@$k%&P')+
                                                          'P%#&z$@k'+isnull('Y','z#@$k%&P'))),2)
               end refund_fact_hybris_transaction_item_key,
       case when s_hybris_seven_day_orders.order_entry_refund_datetime is not null then 'Y' else 'N' end refund_flag,
       isnull(s_hybris_seven_day_orders.refund_lt_bucks,0) refund_lt_bucks,
       isnull(s_hybris_seven_day_orders.refund_master,0) refund_master,
       isnull(s_hybris_seven_day_orders.refund_paypal,0) refund_paypal,
       isnull(abs(s_hybris_seven_day_orders.order_entry_quantity),0) refund_quantity,
       s_hybris_seven_day_orders.refund_reason refund_reason,
       case when isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) < isnull(s_hybris_seven_day_orders.order_entry_total_price,0) then 0
            when isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) - (isnull(s_hybris_seven_day_orders.order_entry_total_price,0) + isnull(s_hybris_seven_day_orders.order_entry_total_tax,0)) >= s_hybris_seven_day_orders.order_entry_shipping_cost
                 then abs(s_hybris_seven_day_orders.order_entry_shipping_cost)
            else abs(isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) - (isnull(s_hybris_seven_day_orders.order_entry_total_price,0) + isnull(s_hybris_seven_day_orders.order_entry_total_tax,0)))
        end refund_shipping_and_handling_amount,
       s_hybris_seven_day_orders.order_entry_refund_status refund_status,
       case when isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) < isnull(s_hybris_seven_day_orders.order_entry_total_price,0) then 0
            when isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) - isnull(s_hybris_seven_day_orders.order_entry_total_price,0) >= s_hybris_seven_day_orders.order_entry_total_tax
                 then abs(s_hybris_seven_day_orders.order_entry_total_tax)
            else abs(isnull(s_hybris_seven_day_orders.order_entry_refund_amount,0) - isnull(s_hybris_seven_day_orders.order_entry_total_price,0))
       end refund_tax_amount,
       isnull(s_hybris_seven_day_orders.refund_visa,0) refund_visa,
       convert(varchar(8),DATEADD(mm,DATEDIFF(mm,0,s_hybris_seven_day_orders.order_entry_settlement_datetime),0),112) sale_allocated_month_starting_dim_date_key,
       convert(varchar(8),dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_hybris_seven_day_orders.order_entry_settlement_datetime)),0)),112) sale_allocated_recalculate_through_datetime,
       dateadd(dd,15,DATEADD(mm,DATEDIFF(mm,0,DATEADD(mm,1,s_hybris_seven_day_orders.order_entry_settlement_datetime)),0)) sale_allocated_recalculate_through_dim_date_key,
       case when p_hybris_seven_day_orders.bk_hash in ('-997', '-998', '-999') then p_hybris_seven_day_orders.bk_hash
                   else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(p_hybris_seven_day_orders.order_code,'z#@$k%&P')+
                                                          'P%#&z$@k'+isnull(convert(varchar(500),p_hybris_seven_day_orders.order_entry_entry_number),'z#@$k%&P')+
                                                          'P%#&z$@k'+isnull('N','z#@$k%&P'))),2)
               end sale_fact_hybris_transaction_item_key,
       isnull(s_hybris_seven_day_orders.order_entry_total_price,0) - isnull(s_hybris_seven_day_orders.capture_lt_bucks,0) sales_amount,
       isnull(s_hybris_seven_day_orders.order_entry_total_price,0) sales_amount_gross,
       case when p_hybris_seven_day_orders.bk_hash in ('-997','-998','-999') then p_hybris_seven_day_orders.bk_hash
            when l_hybris_seven_day_orders.order_commision_employee_id is not null then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_hybris_seven_day_orders.order_commision_employee_id as int) as varchar(500)),'z#@$k%&P'))),2)
            else '-998' 
        end sales_dim_employee_key,
       isnull(s_hybris_seven_day_orders.order_entry_quantity,0) sales_quantity,
       isnull(abs(s_hybris_seven_day_orders.order_entry_shipping_cost),0) sales_shipping_and_handling_amount,
       isnull(abs(s_hybris_seven_day_orders.order_entry_total_tax),0) sales_tax_amount,
       s_hybris_seven_day_orders.selected_club selected_club_id,
       case when p_hybris_seven_day_orders.bk_hash in ('-997','-998','-999') then p_hybris_seven_day_orders.bk_hash
            when s_hybris_seven_day_orders.selected_club is not null then convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_hybris_seven_day_orders.selected_club as int) as varchar(500)),'z#@$k%&P'))),2)
            else '-998' 
        end selected_dim_club_key,
       s_hybris_seven_day_orders.order_entry_settlement_datetime settlement_datetime,
       convert(varchar(8), s_hybris_seven_day_orders.order_entry_settlement_datetime, 112) settlement_dim_date_key,
       case when s_hybris_seven_day_orders.order_entry_settlement_datetime is null then  '-998'
       else '1'+ replace(substring(convert(varchar,convert(datetime,s_hybris_seven_day_orders.order_entry_settlement_datetime,126),114), 1, 5),':','')  end settlement_dim_time_key,
       s_hybris_seven_day_orders.order_entry_tracking_number tracking_number,
       isnull(h_hybris_seven_day_orders.dv_deleted,0) dv_deleted,
       p_hybris_seven_day_orders.p_hybris_seven_day_orders_id,
       p_hybris_seven_day_orders.dv_batch_id,
       p_hybris_seven_day_orders.dv_load_date_time,
       p_hybris_seven_day_orders.dv_load_end_date_time
  from dbo.h_hybris_seven_day_orders
  join dbo.p_hybris_seven_day_orders
    on h_hybris_seven_day_orders.bk_hash = p_hybris_seven_day_orders.bk_hash
  join #p_hybris_seven_day_orders_insert
    on p_hybris_seven_day_orders.bk_hash = #p_hybris_seven_day_orders_insert.bk_hash
   and p_hybris_seven_day_orders.p_hybris_seven_day_orders_id = #p_hybris_seven_day_orders_insert.p_hybris_seven_day_orders_id
  join dbo.l_hybris_seven_day_orders
    on p_hybris_seven_day_orders.bk_hash = l_hybris_seven_day_orders.bk_hash
   and p_hybris_seven_day_orders.l_hybris_seven_day_orders_id = l_hybris_seven_day_orders.l_hybris_seven_day_orders_id
  join dbo.s_hybris_seven_day_orders
    on p_hybris_seven_day_orders.bk_hash = s_hybris_seven_day_orders.bk_hash
   and p_hybris_seven_day_orders.s_hybris_seven_day_orders_id = s_hybris_seven_day_orders.s_hybris_seven_day_orders_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_hybris_seven_day_orders
   where d_hybris_seven_day_orders.bk_hash in (select bk_hash from #p_hybris_seven_day_orders_insert)

  insert dbo.d_hybris_seven_day_orders(
             bk_hash,
             order_code,
             entry_number,
             affiliate_id,
             auto_ship_flag,
             base_refund_amount,
             capture_amex,
             capture_discover,
             capture_lt_bucks,
             capture_master,
             capture_paypal,
             capture_visa,
             commission_employee_id,
             customer_email,
             customer_group,
             customer_name,
             dim_hybris_product_key,
             discount_amount,
             fact_mms_sales_transaction_key,
             fulfillment_partner,
             lt_bucks_earned,
             ltf_party_id,
             method_of_pay,
             mms_transaction_id,
             order_datetime,
             order_dim_date_key,
             original_unit_price,
             product_code,
             purchase_unit_price,
             refund_allocated_month_starting_dim_date_key,
             refund_allocated_recalculate_through_datetime,
             refund_allocated_recalculate_through_dim_date_key,
             refund_amex,
             refund_amount,
             refund_amount_gross,
             refund_datetime,
             refund_discover,
             refund_fact_hybris_transaction_item_key,
             refund_flag,
             refund_lt_bucks,
             refund_master,
             refund_paypal,
             refund_quantity,
             refund_reason,
             refund_shipping_and_handling_amount,
             refund_status,
             refund_tax_amount,
             refund_visa,
             sale_allocated_month_starting_dim_date_key,
             sale_allocated_recalculate_through_datetime,
             sale_allocated_recalculate_through_dim_date_key,
             sale_fact_hybris_transaction_item_key,
             sales_amount,
             sales_amount_gross,
             sales_dim_employee_key,
             sales_quantity,
             sales_shipping_and_handling_amount,
             sales_tax_amount,
             selected_club_id,
             selected_dim_club_key,
             settlement_datetime,
             settlement_dim_date_key,
             settlement_dim_time_key,
             tracking_number,
             deleted_flag,
             p_hybris_seven_day_orders_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         order_code,
         entry_number,
         affiliate_id,
         auto_ship_flag,
         base_refund_amount,
         capture_amex,
         capture_discover,
         capture_lt_bucks,
         capture_master,
         capture_paypal,
         capture_visa,
         commission_employee_id,
         customer_email,
         customer_group,
         customer_name,
         dim_hybris_product_key,
         discount_amount,
         fact_mms_sales_transaction_key,
         fulfillment_partner,
         lt_bucks_earned,
         ltf_party_id,
         method_of_pay,
         mms_transaction_id,
         order_datetime,
         order_dim_date_key,
         original_unit_price,
         product_code,
         purchase_unit_price,
         refund_allocated_month_starting_dim_date_key,
         refund_allocated_recalculate_through_datetime,
         refund_allocated_recalculate_through_dim_date_key,
         refund_amex,
         refund_amount,
         refund_amount_gross,
         refund_datetime,
         refund_discover,
         refund_fact_hybris_transaction_item_key,
         refund_flag,
         refund_lt_bucks,
         refund_master,
         refund_paypal,
         refund_quantity,
         refund_reason,
         refund_shipping_and_handling_amount,
         refund_status,
         refund_tax_amount,
         refund_visa,
         sale_allocated_month_starting_dim_date_key,
         sale_allocated_recalculate_through_datetime,
         sale_allocated_recalculate_through_dim_date_key,
         sale_fact_hybris_transaction_item_key,
         sales_amount,
         sales_amount_gross,
         sales_dim_employee_key,
         sales_quantity,
         sales_shipping_and_handling_amount,
         sales_tax_amount,
         selected_club_id,
         selected_dim_club_key,
         settlement_datetime,
         settlement_dim_date_key,
         settlement_dim_time_key,
         tracking_number,
         dv_deleted,
         p_hybris_seven_day_orders_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_hybris_seven_day_orders)
--Done!
end
