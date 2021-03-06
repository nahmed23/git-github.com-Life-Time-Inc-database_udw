﻿CREATE VIEW [marketing].[v_dim_mms_eft]
AS select d_mms_eft.eft_id eft_id,
       d_mms_eft.account_number account_number,
       d_mms_eft.account_owner account_owner,
       d_mms_eft.d_mms_eft_return_code_bk_hash d_mms_eft_return_code_bk_hash,
       d_mms_eft.dim_mms_member_key dim_mms_member_key,
       d_mms_eft.dim_mms_membership_key dim_mms_membership_key,
       d_mms_eft.dues_amount_used_for_products dues_amount_used_for_products,
       d_mms_eft.eft_amount eft_amount,
       d_mms_eft.eft_amount_products eft_amount_products,
       d_mms_eft.eft_date eft_date,
       d_mms_eft.eft_dim_date_key eft_dim_date_key,
       d_mms_eft.eft_dim_time_key eft_dim_time_key,
       d_mms_eft.eft_return_code_id eft_return_code_id,
       d_mms_eft.expiration_date expiration_date,
       d_mms_eft.expiration_dim_date_key expiration_dim_date_key,
       d_mms_eft.expiration_dim_time_key expiration_dim_time_key,
       d_mms_eft.fact_mms_payment_key fact_mms_payment_key,
       d_mms_eft.inserted_date_time inserted_date_time,
       d_mms_eft.inserted_dim_date_key inserted_dim_date_key,
       d_mms_eft.inserted_dim_time_key inserted_dim_time_key,
       d_mms_eft.job_task_id job_task_id,
       d_mms_eft.masked_account_number masked_account_number,
       d_mms_eft.masked_account_number64 masked_account_number64,
       d_mms_eft.member_id member_id,
       d_mms_eft.membership_id membership_id,
       d_mms_eft.order_number order_number,
       d_mms_eft.payment_id payment_id,
       d_mms_eft.return_code return_code,
       d_mms_eft.routing_number routing_number,
       d_mms_eft.token token,
       d_mms_eft.updated_date_time updated_date_time,
       d_mms_eft.updated_dim_date_key updated_dim_date_key,
       d_mms_eft.updated_dim_time_key updated_dim_time_key,
       d_mms_eft.val_eft_account_type_id val_eft_account_type_id,
       d_mms_eft.val_eft_status_id val_eft_status_id,
       d_mms_eft.val_eft_type_id val_eft_type_id,
       d_mms_eft.val_payment_type_id val_payment_type_id
  from dbo.d_mms_eft;