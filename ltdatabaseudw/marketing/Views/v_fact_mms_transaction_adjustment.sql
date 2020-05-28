CREATE VIEW [marketing].[v_fact_mms_transaction_adjustment]
AS select fact_mms_sales_transaction_adjustment.dim_club_key dim_club_key,
       fact_mms_sales_transaction_adjustment.dim_mms_drawer_activity_key dim_mms_drawer_activity_key,
       fact_mms_sales_transaction_adjustment.dim_mms_member_key dim_mms_member_key,
       fact_mms_sales_transaction_adjustment.dim_mms_membership_key dim_mms_membership_key,
       fact_mms_sales_transaction_adjustment.dim_mms_transaction_reason_key dim_mms_transaction_reason_key,
       fact_mms_sales_transaction_adjustment.fact_mms_sales_transaction_adjustment_key fact_mms_sales_transaction_adjustment_key,
       fact_mms_sales_transaction_adjustment.mms_tran_id mms_tran_id,
       fact_mms_sales_transaction_adjustment.pos_amount pos_amount,
       fact_mms_sales_transaction_adjustment.post_dim_date_key post_dim_date_key,
       fact_mms_sales_transaction_adjustment.tran_amount tran_amount,
       fact_mms_sales_transaction_adjustment.tran_dim_date_key tran_dim_date_key,
       fact_mms_sales_transaction_adjustment.tran_item_exists_flag tran_item_exists_flag,
       fact_mms_sales_transaction_adjustment.transaction_entered_dim_employee_key transaction_entered_dim_employee_key,
       fact_mms_sales_transaction_adjustment.transaction_reporting_dim_club_key transaction_reporting_dim_club_key,
       fact_mms_sales_transaction_adjustment.udw_inserted_dim_date_key udw_inserted_dim_date_key,
       fact_mms_sales_transaction_adjustment.voided_flag voided_flag
  from dbo.fact_mms_sales_transaction_adjustment;