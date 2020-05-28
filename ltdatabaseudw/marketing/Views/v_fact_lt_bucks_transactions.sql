CREATE VIEW [marketing].[v_fact_lt_bucks_transactions]
AS select d_lt_bucks_transactions.fact_lt_bucks_transactions_key fact_lt_bucks_transactions_key,
       d_lt_bucks_transactions.transaction_id transaction_id,
       d_lt_bucks_transactions.award_reason award_reason,
       d_lt_bucks_transactions.bucks_amount bucks_amount,
       d_lt_bucks_transactions.bucks_expiration_date_time bucks_expiration_date_time,
       d_lt_bucks_transactions.cancelled_order_original_fact_mylt_bucks_transaction_id cancelled_order_original_fact_mylt_bucks_transaction_id,
       d_lt_bucks_transactions.cancelled_order_original_fact_mylt_bucks_transaction_item_id cancelled_order_original_fact_mylt_bucks_transaction_item_id,
       d_lt_bucks_transactions.dim_lt_bucks_user_key dim_lt_bucks_user_key,
       d_lt_bucks_transactions.pended_date_time pended_date_time,
       d_lt_bucks_transactions.transaction_date_time transaction_date_time,
       d_lt_bucks_transactions.transaction_type_id transaction_type_id
  from dbo.d_lt_bucks_transactions;