CREATE VIEW [marketing].[v_fact_magento_payment]
AS select fact_magento_payment.base_amount_authorized base_amount_authorized,
       fact_magento_payment.base_amount_ordered base_amount_ordered,
       fact_magento_payment.base_amount_paid base_amount_paid,
       fact_magento_payment.cc_type cc_type,
       fact_magento_payment.created_dim_date_key created_dim_date_key,
       fact_magento_payment.created_dim_time_key created_dim_time_key,
       fact_magento_payment.fact_magento_payment_key fact_magento_payment_key,
       fact_magento_payment.fact_magento_sales_order_key fact_magento_sales_order_key,
       fact_magento_payment.fact_magento_sales_order_payment_key fact_magento_sales_order_payment_key,
       fact_magento_payment.sales_order_payment_id sales_order_payment_id,
       fact_magento_payment.transaction_id transaction_id
  from dbo.fact_magento_payment;