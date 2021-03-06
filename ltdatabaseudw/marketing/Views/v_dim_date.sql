﻿CREATE VIEW [marketing].[v_dim_date] AS select dim_date_key as dim_date_key,
       bi_weekly_pay_period_code,
       calendar_date,
       full_date_description,
       full_date_numeric_description,
       year_month_name,
       month_name_year,
       four_digit_year_dash_two_digit_month,
	   four_digit_year_two_digit_month_two_digit_day,
       standard_date_name,
       day_of_week_name,
       day_of_week_abbreviation,
       day_number_in_week,
       day_number_in_month,
       day_number_in_quarter,
       day_number_in_year,
       week_ending_date,
       week_number_in_year,
       month_name,
       month_abbreviation,
       month_number_in_year,
       quarter_name,
       quarter_number,
       year,
       number_of_days_in_month,
       weekday_flag,
       last_day_in_week_flag,
       last_day_in_month_flag,
       month_starting_dim_date_key as month_starting_dim_date_key,
       month_starting_date,
       month_ending_dim_date_key as month_ending_dim_date_key,
       month_ending_date,
       prior_year_date,
       prior_year_day_number_in_week,
       prior_year_day_number_in_month,
       prior_year_day_number_in_quarter,
       prior_year_day_number_in_year,
       prior_year_week_number_in_year,
       prior_year,
       prior_month_starting_dim_date_key as prior_month_starting_dim_date_key,
       prior_month_starting_date,
       prior_month_ending_dim_date_key as prior_month_ending_dim_date_key,
       prior_month_ending_date,
       next_month_starting_dim_date_key as next_month_starting_dim_date_key,
       next_month_starting_date,
       next_month_ending_dim_date_key as next_month_ending_dim_date_key,
       next_month_ending_date,
       next_month_name,
       prior_day_dim_date_key as prior_day_dim_date_key,
       next_day_dim_date_key as next_day_dim_date_key,
       number_of_days_in_month_for_dssr,
       day_number_in_month_for_dssr,
       accounting_period_code,
       pay_period_code,
       pay_period_number_in_month,
       pay_period_full_description,
       pay_period_first_day_flag,
       pay_period_last_day_flag
  from dim_date;