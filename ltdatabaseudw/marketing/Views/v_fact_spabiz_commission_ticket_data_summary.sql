CREATE VIEW [marketing].[v_fact_spabiz_commission_ticket_data_summary] AS select dim_spabiz_staff_key,
       ticket_item_date_time,
       store_number,
       ticket_status,
       sum(unique_customer_count) unique_customer_count,
       sum(unique_product_customer_count) unique_product_customer_count,
       sum(unique_service_customer_count) unique_service_customer_count,
       sum(retail_amount) retail_amount,
       sum(service_amount) service_amount,
       sum(discount_amount) discount_amount,
       sum(product_commission_amount) product_commission_amount,
       sum(product_commission_discount_amount) product_commission_discount_amount,
       sum(service_commission_amount) service_commission_amount,---service
       sum(service_commission_discount_amount) service_commission_discount_amount
from (select d_fact_spabiz_ticket_item.first_dim_spabiz_staff_key dim_spabiz_staff_key,
             d_fact_spabiz_ticket_item.ticket_item_date_time,
             d_fact_spabiz_ticket_item.store_number,
             d_fact_spabiz_ticket_item.status ticket_status,
             sum(fact_spabiz_ticket.unique_customer_count) unique_customer_count,
             sum(fact_spabiz_ticket.unique_product_customer_count) unique_product_customer_count,
             sum(fact_spabiz_ticket.unique_service_customer_count) unique_service_customer_count,
             sum(product_amount) retail_amount,
             sum(service_amount) service_amount,
             sum(ticket_total_discount_amount) discount_amount,
             sum(case when d_fact_spabiz_ticket_item.dim_spabiz_product_key not in ('-999','-998','-997') then d_fact_spabiz_ticket_item.employee_commission_amount 
                      else 0 end) product_commission_amount,
             sum(case when d_dim_spabiz_discount.all_service_commission_flag = 'Y' and d_fact_spabiz_ticket_item.dim_spabiz_product_key not in ('-999','-998','-997') then d_fact_spabiz_ticket_item.employee_commission_discount_amount 
                      else 0 end) product_commission_discount_amount,
             sum(case when dim_spabiz_service.pay_commission_flag = 'Y' then d_fact_spabiz_ticket_item.employee_commission_amount else 0 end) service_commission_amount,---service
             sum(case when d_dim_spabiz_discount.all_service_commission_flag = 'Y' and d_fact_spabiz_ticket_item.dim_spabiz_service_key not in ('-999','-998','-997') then d_fact_spabiz_ticket_item.employee_commission_discount_amount 
                      else 0 end) service_commission_discount_amount
        from d_fact_spabiz_ticket_item
        join fact_spabiz_ticket --for customer counts
          on d_fact_spabiz_ticket_item.fact_spabiz_ticket_key = fact_spabiz_ticket.fact_spabiz_ticket_key
        join d_dim_spabiz_discount
          on d_fact_spabiz_ticket_item.dim_spabiz_discount_key = d_dim_spabiz_discount.dim_spabiz_discount_key
        join dim_spabiz_service
          on d_fact_spabiz_ticket_item.dim_spabiz_service_key = dim_spabiz_service.dim_spabiz_service_key
       group by d_fact_spabiz_ticket_item.first_dim_spabiz_staff_key,
                d_fact_spabiz_ticket_item.ticket_item_date_time,
                d_fact_spabiz_ticket_item.store_number,
                d_fact_spabiz_ticket_item.status
       union all
      select d_fact_spabiz_ticket_item.second_dim_spabiz_staff_key dim_spabiz_staff_key,
             d_fact_spabiz_ticket_item.ticket_item_date_time,
             d_fact_spabiz_ticket_item.store_number,
             d_fact_spabiz_ticket_item.status ticket_status,
             sum(fact_spabiz_ticket.unique_customer_count) unique_customer_count,
             sum(fact_spabiz_ticket.unique_product_customer_count) unique_product_customer_count,
             sum(fact_spabiz_ticket.unique_service_customer_count) unique_service_customer_count,
             sum(product_amount) retail_amount,
             sum(service_amount) service_amount,
             sum(ticket_total_discount_amount) discount_amount,
             sum(case when d_fact_spabiz_ticket_item.dim_spabiz_product_key not in ('-999','-998','-997') then d_fact_spabiz_ticket_item.employee_commission_amount 
                      else 0 end) product_commission_amount,
             sum(case when d_dim_spabiz_discount.all_service_commission_flag = 'Y' and d_fact_spabiz_ticket_item.dim_spabiz_product_key not in ('-999','-998','-997') then d_fact_spabiz_ticket_item.employee_commission_discount_amount 
                      else 0 end) product_commission_discount_amount,
             sum(case when dim_spabiz_service.pay_commission_flag = 'Y' then d_fact_spabiz_ticket_item.employee_commission_amount else 0 end) service_commission_amount,---service
             sum(case when d_dim_spabiz_discount.all_service_commission_flag = 'Y' and d_fact_spabiz_ticket_item.dim_spabiz_service_key not in ('-999','-998','-997') then d_fact_spabiz_ticket_item.employee_commission_discount_amount 
                      else 0 end) service_commission_discount_amount
      from d_fact_spabiz_ticket_item
      join fact_spabiz_ticket --for customer counts
        on d_fact_spabiz_ticket_item.fact_spabiz_ticket_key = fact_spabiz_ticket.fact_spabiz_ticket_key
      join d_dim_spabiz_discount
        on d_fact_spabiz_ticket_item.dim_spabiz_discount_key = d_dim_spabiz_discount.dim_spabiz_discount_key
      join dim_spabiz_service
        on d_fact_spabiz_ticket_item.dim_spabiz_service_key = dim_spabiz_service.dim_spabiz_service_key
     where d_fact_spabiz_ticket_item.second_dim_spabiz_staff_key not in ('-999','998','-997')
       and d_fact_spabiz_ticket_item.first_dim_spabiz_staff_key <> d_fact_spabiz_ticket_item.second_dim_spabiz_staff_key
      group by d_fact_spabiz_ticket_item.second_dim_spabiz_staff_key,
               d_fact_spabiz_ticket_item.ticket_item_date_time,
               d_fact_spabiz_ticket_item.store_number,
               d_fact_spabiz_ticket_item.status
) x
group by dim_spabiz_staff_key,
         ticket_item_date_time,
         store_number,
         ticket_status;