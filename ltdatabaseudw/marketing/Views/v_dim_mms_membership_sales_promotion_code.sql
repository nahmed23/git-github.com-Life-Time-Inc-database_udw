CREATE VIEW [marketing].[v_dim_mms_membership_sales_promotion_code]
AS select d_mms_membership_sales_promotion_code.membership_sales_promotion_code_id membership_sales_promotion_code_id,
       d_mms_membership_sales_promotion_code.dim_mms_member_key dim_mms_member_key,
       d_mms_membership_sales_promotion_code.dim_mms_membership_key dim_mms_membership_key,
       d_mms_membership_sales_promotion_code.dim_mms_sales_promotion_code_key dim_mms_sales_promotion_code_key,
       d_mms_membership_sales_promotion_code.inserted_dim_date_key inserted_dim_date_key,
       d_mms_membership_sales_promotion_code.inserted_dim_time_key inserted_dim_time_key,
       d_mms_membership_sales_promotion_code.member_id member_id,
       d_mms_membership_sales_promotion_code.membership_id membership_id,
       d_mms_membership_sales_promotion_code.sales_advisor_dim_employee_key sales_advisor_dim_employee_key,
       d_mms_membership_sales_promotion_code.sales_advisor_employee_id sales_advisor_employee_id,
       d_mms_membership_sales_promotion_code.sales_promotion_code_id sales_promotion_code_id,
       d_mms_membership_sales_promotion_code.updated_dim_date_key updated_dim_date_key,
       d_mms_membership_sales_promotion_code.updated_dim_time_key updated_dim_time_key
  from dbo.d_mms_membership_sales_promotion_code;