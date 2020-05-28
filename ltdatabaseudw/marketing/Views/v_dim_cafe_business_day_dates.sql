CREATE VIEW [marketing].[v_dim_cafe_business_day_dates]
AS select d_ig_it_trn_business_day_dates.dim_cafe_business_day_dates_key dim_cafe_business_day_dates_key,
       d_ig_it_trn_business_day_dates.bus_day_id bus_day_id,
       d_ig_it_trn_business_day_dates.business_day_end_dim_date_key business_day_end_dim_date_key,
       d_ig_it_trn_business_day_dates.business_day_end_dim_time_key business_day_end_dim_time_key,
       d_ig_it_trn_business_day_dates.business_day_start_dim_date_key business_day_start_dim_date_key,
       d_ig_it_trn_business_day_dates.business_day_start_dim_time_key business_day_start_dim_time_key
  from dbo.d_ig_it_trn_business_day_dates;