CREATE VIEW [marketing].[v_dim_cafe_meal_period]
AS select d_ig_it_cfg_meal_period_master.ent_id ent_id,
       d_ig_it_cfg_meal_period_master.meal_period_id meal_period_id,
       d_ig_it_cfg_meal_period_master.default_check_type_id default_check_type_id,
       d_ig_it_cfg_meal_period_master.default_dim_cafe_check_type_key default_dim_cafe_check_type_key,
       d_ig_it_cfg_meal_period_master.default_price_level_id default_price_level_id,
       d_ig_it_cfg_meal_period_master.dim_cafe_meal_period_key dim_cafe_meal_period_key,
       d_ig_it_cfg_meal_period_master.enterprise_created_id enterprise_created_id,
       d_ig_it_cfg_meal_period_master.entertainment_flag entertainment_flag,
       d_ig_it_cfg_meal_period_master.meal_period_abbr_1 meal_period_abbr_1,
       d_ig_it_cfg_meal_period_master.meal_period_abbr_2 meal_period_abbr_2,
       d_ig_it_cfg_meal_period_master.meal_period_name meal_period_name,
       d_ig_it_cfg_meal_period_master.meal_period_sec_id meal_period_sec_id,
       d_ig_it_cfg_meal_period_master.receipt_code receipt_code,
       d_ig_it_cfg_meal_period_master.row_version row_version,
       d_ig_it_cfg_meal_period_master.secondary_dim_cafe_meal_period_key secondary_dim_cafe_meal_period_key,
       d_ig_it_cfg_meal_period_master.store_id store_id
  from dbo.d_ig_it_cfg_meal_period_master;