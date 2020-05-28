CREATE VIEW [marketing].[v_fact_lt_bucks_award_spend] AS select d_lt_bucks_transaction_fifo.fact_lt_bucks_award_spend_key fact_lt_bucks_award_spend_key,
       d_lt_bucks_transaction_fifo.tfifo_id tfifo_id,
       d_lt_bucks_transaction_fifo.award_fact_lt_bucks_transaction_key award_fact_lt_bucks_transaction_key,
       d_lt_bucks_transaction_fifo.bucks_amount bucks_amount,
       d_lt_bucks_transaction_fifo.spend_fact_lt_bucks_transaction_key spend_fact_lt_bucks_transaction_key,
       d_lt_bucks_transaction_fifo.transaction_date_time transaction_date_time
  from dbo.d_lt_bucks_transaction_fifo;