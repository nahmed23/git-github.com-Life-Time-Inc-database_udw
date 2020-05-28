CREATE VIEW [marketing].[v_dim_revenue_allocation_rule]
AS select dim_revenue_allocation_rule.dim_club_key dim_club_key,
       dim_revenue_allocation_rule.accumulated_ratio accumulated_ratio,
       dim_revenue_allocation_rule.earliest_transaction_dim_date_key earliest_transaction_dim_date_key,
       dim_revenue_allocation_rule.effective_date effective_date,
       dim_revenue_allocation_rule.expiration_date expiration_date,
       dim_revenue_allocation_rule.latest_transaction_dim_date_key latest_transaction_dim_date_key,
       dim_revenue_allocation_rule.one_off_rule_flag one_off_rule_flag,
       dim_revenue_allocation_rule.ratio ratio,
       dim_revenue_allocation_rule.revenue_allocation_rule_name revenue_allocation_rule_name,
       dim_revenue_allocation_rule.revenue_allocation_rule_set revenue_allocation_rule_set,
       dim_revenue_allocation_rule.revenue_from_late_transaction_flag revenue_from_late_transaction_flag,
       dim_revenue_allocation_rule.revenue_posting_month_ending_dim_date_key revenue_posting_month_ending_dim_date_key,
       dim_revenue_allocation_rule.revenue_posting_month_four_digit_year_dash_two_digit_month revenue_posting_month_four_digit_year_dash_two_digit_month,
       dim_revenue_allocation_rule.revenue_posting_month_starting_dim_date_key revenue_posting_month_starting_dim_date_key
 from dim_revenue_allocation_rule    left outer join dim_club on dim_revenue_allocation_rule.club_id = dim_club.club_id;