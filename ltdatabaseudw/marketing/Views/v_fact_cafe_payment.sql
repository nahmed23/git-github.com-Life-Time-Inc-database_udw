﻿CREATE VIEW [marketing].[v_fact_cafe_payment] AS select fact_cafe_payment.fact_cafe_payment_key fact_cafe_payment_key,
       fact_cafe_payment.change_amount change_amount,
       fact_cafe_payment.charges_to_date_amount charges_to_date_amount,
       fact_cafe_payment.dim_cafe_payment_type_key dim_cafe_payment_type_key,
       fact_cafe_payment.dim_mms_member_key dim_mms_member_key,
       fact_cafe_payment.order_hdr_id order_hdr_id,
       fact_cafe_payment.pro_rata_discount_amount pro_rata_discount_amount,
       fact_cafe_payment.pro_rata_gratuity_amount pro_rata_gratuity_amount,
       fact_cafe_payment.pro_rata_sales_amount_gross pro_rata_sales_amount_gross,
       fact_cafe_payment.pro_rata_service_charge_amount pro_rata_service_charge_amount,
       fact_cafe_payment.pro_rata_tax_amount pro_rata_tax_amount,
       fact_cafe_payment.remaining_balance_amount remaining_balance_amount,
       fact_cafe_payment.tender_amount tender_amount,
       fact_cafe_payment.tender_seq tender_seq,
       fact_cafe_payment.tender_type_id tender_type_id,
       fact_cafe_payment.tip_amount tip_amount
  from dbo.fact_Cafe_payment;