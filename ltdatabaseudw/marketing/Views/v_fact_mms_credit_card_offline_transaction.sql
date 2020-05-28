﻿CREATE VIEW [marketing].[v_fact_mms_credit_card_offline_transaction] AS select fact_mms_credit_card_offline_transaction.fact_mms_credit_card_offline_transaction_key fact_mms_credit_card_offline_transaction_key,
       fact_mms_credit_card_offline_transaction.third_party_pos_payment_id third_party_pos_payment_id,
       fact_mms_credit_card_offline_transaction.card_on_file_flag card_on_file_flag,
       fact_mms_credit_card_offline_transaction.credit_card_type_dim_mms_description_key credit_card_type_dim_mms_description_key,
       fact_mms_credit_card_offline_transaction.declined_or_error_flag declined_or_error_flag,
       fact_mms_credit_card_offline_transaction.dim_mms_customer_key dim_mms_customer_key,
       fact_mms_credit_card_offline_transaction.dim_mms_location_key dim_mms_location_key,
       fact_mms_credit_card_offline_transaction.dim_mms_member_key dim_mms_member_key,
       fact_mms_credit_card_offline_transaction.local_currency_dim_plan_exchange_rate_key local_currency_dim_plan_exchange_rate_key,
       fact_mms_credit_card_offline_transaction.local_currency_monthly_average_dim_mms_exchange_rate_key local_currency_monthly_average_dim_mms_exchange_rate_key,
       fact_mms_credit_card_offline_transaction.masked_credit_card_number masked_credit_card_number,
       fact_mms_credit_card_offline_transaction.original_currency_code original_currency_code,
       fact_mms_credit_card_offline_transaction.payment_status_dim_mms_description_key payment_status_dim_mms_description_key,
       fact_mms_credit_card_offline_transaction.pos_tran_date_time pos_tran_date_time,
       fact_mms_credit_card_offline_transaction.pos_unique_transaction_id pos_unique_transaction_id,
       fact_mms_credit_card_offline_transaction.pt_credit_card_rejected_transaction_id pt_credit_card_rejected_transaction_id,
       fact_mms_credit_card_offline_transaction.rejected_transaction_date_time rejected_transaction_date_time,
       fact_mms_credit_card_offline_transaction.rejected_transaction_error_message rejected_transaction_error_message,
       fact_mms_credit_card_offline_transaction.timed_out_flag timed_out_flag,
       fact_mms_credit_card_offline_transaction.transaction_amount transaction_amount,
       fact_mms_credit_card_offline_transaction.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
       fact_mms_credit_card_offline_transaction.usd_monthy_average_dim_exchange_rate_key usd_monthy_average_dim_exchange_rate_key
  from dbo.fact_mms_credit_card_offline_transaction;