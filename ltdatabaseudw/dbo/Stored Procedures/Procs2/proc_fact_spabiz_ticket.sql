CREATE PROC [dbo].[proc_fact_spabiz_ticket] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
if object_id('tempdb..#dv_batch_id') is not null drop table #dv_batch_id
create table dbo.#dv_batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @dv_batch_id as current_dv_batch_id
  from dbo.fact_spabiz_ticket

if object_id('tempdb..#fact_spabiz_ticket_key') is not null drop table #fact_spabiz_ticket_key
create table dbo.#fact_spabiz_ticket_key with(distribution=hash(fact_spabiz_ticket_key), location=user_db, heap) as
select fact_spabiz_ticket_key,
       rank() over (order by fact_spabiz_ticket_key) r
from (select fact_spabiz_ticket_key
      from d_spabiz_ticket_data
      join #dv_batch_id
        on d_spabiz_ticket_data.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or d_spabiz_ticket_data.dv_batch_id = #dv_batch_id.current_dv_batch_id
      union
      select bk_hash
      from d_spabiz_ticket 
      join #dv_batch_id
        on d_spabiz_ticket.dv_batch_id > #dv_batch_id.max_dv_batch_id
        or d_spabiz_ticket.dv_batch_id = #dv_batch_id.current_dv_batch_id
      where d_spabiz_ticket.dv_load_end_date_time = 'Dec 31, 9999'
) x

if object_id('tempdb..#p_spabiz_ticket_data') is not null drop table #p_spabiz_ticket_data
create table dbo.#p_spabiz_ticket_data with(distribution=hash(fact_spabiz_ticket_key), location=user_db, heap) as
select d_spabiz_ticket_data.fact_spabiz_ticket_key,
       count(distinct d_spabiz_ticket_data.dim_spabiz_customer_key) unique_customer_count,
       count(distinct case when d_spabiz_ticket_data.dim_spabiz_service_key not in ('-999','-998','-997') then d_spabiz_ticket_data.dim_spabiz_customer_key else null end) unique_service_customer_count,
       count(distinct case when d_spabiz_ticket_data.dim_spabiz_product_key not in ('-999','-998','-997') then d_spabiz_ticket_data.dim_spabiz_customer_key else null end) unique_product_customer_count,
       max(d_spabiz_ticket_data.p_spabiz_ticket_data_id) p_spabiz_ticket_data_id,
       max(d_spabiz_ticket_data.dv_batch_id) dv_batch_id,
       max(d_spabiz_ticket_data.dv_load_date_time) dv_load_date_time,
       max(d_spabiz_ticket_data.dv_load_end_date_time) dv_load_end_date_time,
       max(#fact_spabiz_ticket_key.r) r
  from dbo.d_spabiz_ticket_data
  join #fact_spabiz_ticket_key
    on d_spabiz_ticket_data.fact_spabiz_ticket_key = #fact_spabiz_ticket_key.fact_spabiz_ticket_key
 group by d_spabiz_ticket_data.fact_spabiz_ticket_key

if object_id('tempdb..#p_spabiz_ticket') is not null drop table #p_spabiz_ticket
create table dbo.#p_spabiz_ticket with(distribution=hash(fact_spabiz_ticket_key), location=user_db, heap) as
select #fact_spabiz_ticket_key.r,
       d_spabiz_ticket.fact_spabiz_ticket_key,
       d_spabiz_ticket.store_number,
       d_spabiz_ticket.ticket_id,
       d_spabiz_ticket.cash_change,
       d_spabiz_ticket.check_in_date_time,
       d_spabiz_ticket.created_date_time,
       d_spabiz_ticket.dim_spabiz_customer_key,
       d_spabiz_ticket.dim_spabiz_payment_type_key,
       d_spabiz_ticket.dim_spabiz_shift_key,
       d_spabiz_ticket.dim_spabiz_staff_key,
       d_spabiz_ticket.dim_spabiz_store_key,
       d_spabiz_ticket.discount_product,
       d_spabiz_ticket.discount_service,
       d_spabiz_ticket.discount_total,
       d_spabiz_ticket.edit_date_time,
       d_spabiz_ticket.fact_spabiz_appointment_key,
       d_spabiz_ticket.is_master_ticket_flag,
       d_spabiz_ticket.late,
       d_spabiz_ticket.note,
       d_spabiz_ticket.sales_gift_total sales_gift_total,
       d_spabiz_ticket.sales_package_total sales_package_total,
       d_spabiz_ticket.sales_product_total sales_product_total,
       d_spabiz_ticket.sales_series_total sales_series_total,
       d_spabiz_ticket.sales_service_total sales_service_total,
       d_spabiz_ticket.sales_subtotal sales_subtotal,
       d_spabiz_ticket.sales_total,
       d_spabiz_ticket.status_dim_description_key,
       d_spabiz_ticket.status_id,
       d_spabiz_ticket.tax_total tax_total,
       d_spabiz_ticket.ticket_id_for_day ticket_id_for_day,
       d_spabiz_ticket.ticket_number,
       d_spabiz_ticket.tip_amount,
       d_spabiz_ticket.voider_dim_spabiz_staff_key,
       d_spabiz_ticket.l_spabiz_ticket_ap_id,
       d_spabiz_ticket.l_spabiz_ticket_cust_id,
       d_spabiz_ticket.l_spabiz_ticket_pay_type_id,
       d_spabiz_ticket.l_spabiz_ticket_shift_id,
       d_spabiz_ticket.l_spabiz_ticket_staff_id,
       d_spabiz_ticket.l_spabiz_ticket_voider_id,
       d_spabiz_ticket.p_spabiz_ticket_id,
       d_spabiz_ticket.dv_batch_id,
       d_spabiz_ticket.dv_load_date_time,
       d_spabiz_ticket.dv_load_end_date_time
  from dbo.d_spabiz_ticket
  join #fact_spabiz_ticket_key
    on d_spabiz_ticket.bk_hash = #fact_spabiz_ticket_key.fact_spabiz_ticket_key


-- do as a single transaction
--   delete records from the fact table that exist
--   insert records from records from current and missing batches
    begin tran
      delete dbo.fact_spabiz_ticket
       where fact_spabiz_ticket_key in (select fact_spabiz_ticket_key from #fact_spabiz_ticket_key)

      insert dbo.fact_spabiz_ticket(
                 fact_spabiz_ticket_key,
                 store_number,
                 ticket_id,
                 cash_change,
                 check_in_date_time,
                 created_date_time,
                 dim_spabiz_customer_key,
                 dim_spabiz_payment_type_key,
                 dim_spabiz_shift_key,
                 dim_spabiz_staff_key,
                 dim_spabiz_store_key,
                 discount_product,
                 discount_service,
                 discount_total,
                 edit_date_time,
                 fact_spabiz_appointment_key,
                 is_master_ticket_flag,
                 late,
                 note,
                 sales_gift_total,
                 sales_package_total,
                 sales_product_total,
                 sales_series_total,
                 sales_service_total,
                 sales_subtotal,
                 sales_total,
                 status_dim_description_key,
                 status_id,
                 tax_total,
                 ticket_id_for_day,
                 ticket_number,
                 tip_amount,
                 unique_customer_count,
                 unique_product_customer_count,
                 unique_service_customer_count,
                 voider_dim_spabiz_staff_key,
                 p_spabiz_ticket_id,
                 p_spabiz_ticket_data_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select #p_spabiz_ticket.fact_spabiz_ticket_key,
             store_number,
             ticket_id,
             cash_change,
             check_in_date_time,
             created_date_time,
             dim_spabiz_customer_key,
             dim_spabiz_payment_type_key,
             dim_spabiz_shift_key,
             dim_spabiz_staff_key,
             dim_spabiz_store_key,
             discount_product,
             discount_service,
             discount_total,
             edit_date_time,
             fact_spabiz_appointment_key,
             is_master_ticket_flag,
             late,
             note,
             sales_gift_total,
             sales_package_total,
             sales_product_total,
             sales_series_total,
             sales_service_total,
             sales_subtotal,
             sales_total,
             status_dim_description_key,
             status_id,
             tax_total,
             ticket_id_for_day,
             ticket_number,
             tip_amount,
             isnull(#p_spabiz_ticket_data.unique_customer_count,0),
             isnull(#p_spabiz_ticket_data.unique_product_customer_count,0),
             isnull(#p_spabiz_ticket_data.unique_service_customer_count,0),
             voider_dim_spabiz_staff_key,
             p_spabiz_ticket_data_id,
             p_spabiz_ticket_id,
             case when #p_spabiz_ticket.dv_load_date_time >= isnull(#p_spabiz_ticket_data.dv_load_date_time,'Jan 1, 1753') then #p_spabiz_ticket.dv_load_date_time
                  else isnull(#p_spabiz_ticket_data.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
             case when #p_spabiz_ticket.dv_load_end_date_time >= isnull(#p_spabiz_ticket_data.dv_load_end_date_time,'Jan 1, 1753') then #p_spabiz_ticket.dv_load_end_date_time
                  else isnull(#p_spabiz_ticket_data.dv_load_end_date_time,'Jan 1, 1753') end dv_load_end_date_time,
             case when #p_spabiz_ticket.dv_batch_id >= isnull(#p_spabiz_ticket_data.dv_batch_id,-1) then #p_spabiz_ticket.dv_batch_id
                  else isnull(#p_spabiz_ticket_data.dv_batch_id,-1) end dv_batch_id,
             getdate(),
             suser_sname()
        from #p_spabiz_ticket
        left join #p_spabiz_ticket_data
          on #p_spabiz_ticket_data.fact_spabiz_ticket_key = #p_spabiz_ticket.fact_spabiz_ticket_key

    commit tran


end
