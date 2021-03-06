﻿CREATE VIEW [marketing].[v_dim_mms_membership]
AS select dim_mms_membership.dim_mms_membership_key dim_mms_membership_key,
       dim_mms_membership.membership_id membership_id,
       dim_mms_membership.advisor_employee_id advisor_employee_id,
       dim_mms_membership.attrition_date attrition_date,
       dim_mms_membership.created_date_time created_date_time,
       dim_mms_membership.created_date_time_key created_date_time_key,
       dim_mms_membership.crm_opportunity_id crm_opportunity_id,
       dim_mms_membership.current_price current_price,
       dim_mms_membership.dim_crm_opportunity_key dim_crm_opportunity_key,
       dim_mms_membership.dim_mms_company_key dim_mms_company_key,
       dim_mms_membership.dim_mms_membership_type_key dim_mms_membership_type_key,
       dim_mms_membership.eft_option eft_option,
       dim_mms_membership.home_dim_club_key home_dim_club_key,
       dim_mms_membership.membership_activation_date membership_activation_date,
       dim_mms_membership.membership_address_city membership_address_city,
       dim_mms_membership.membership_address_country membership_address_country,
       dim_mms_membership.membership_address_line_1 membership_address_line_1,
       dim_mms_membership.membership_address_line_2 membership_address_line_2,
       dim_mms_membership.membership_address_postal_code membership_address_postal_code,
       dim_mms_membership.membership_address_state_abbreviation membership_address_state_abbreviation,
       dim_mms_membership.membership_cancellation_request_date membership_cancellation_request_date,
       dim_mms_membership.membership_expiration_date membership_expiration_date,
       dim_mms_membership.membership_sales_channel_dim_description_key membership_sales_channel_dim_description_key,
       dim_mms_membership.membership_source membership_source,
       dim_mms_membership.membership_status membership_status,
       dim_mms_membership.membership_type membership_type,
       dim_mms_membership.membership_type_id membership_type_id,
       dim_mms_membership.money_back_cancellation_flag money_back_cancellation_flag,
       dim_mms_membership.non_payment_termination_flag non_payment_termination_flag,
       dim_mms_membership.original_sales_dim_team_member_key original_sales_dim_team_member_key,
       dim_mms_membership.p_mms_membership_id p_mms_membership_id,
       dim_mms_membership.prior_plus_membership_type prior_plus_membership_type,
       dim_mms_membership.prior_plus_membership_type_key prior_plus_membership_type_key,
       dim_mms_membership.prior_plus_price prior_plus_price,
       dim_mms_membership.prior_plus_undiscounted_price prior_plus_undiscounted_price,
       dim_mms_membership.revenue_reporting_category_description revenue_reporting_category_description,
       dim_mms_membership.sales_reporting_category_description sales_reporting_category_description,
       dim_mms_membership.termination_reason termination_reason,
       dim_mms_membership.undiscounted_price undiscounted_price,
       dim_mms_membership.val_country_id val_country_id,
       dim_mms_membership.val_eft_option_id val_eft_option_id,
       dim_mms_membership.val_membership_source_id val_membership_source_id,
       dim_mms_membership.val_membership_status_id val_membership_status_id,
       dim_mms_membership.val_revenue_reporting_category_id val_revenue_reporting_category_id,
       dim_mms_membership.val_sales_reporting_category_id val_sales_reporting_category_id,
       dim_mms_membership.val_state_id val_state_id,
       dim_mms_membership.val_termination_reason_id val_termination_reason_id
  from dbo.dim_mms_membership;