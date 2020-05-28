CREATE VIEW [marketing].[v_dim_ig_it_cfg_check_type_master]
AS select d_ig_it_cfg_check_type_master.dim_ig_it_cfg_check_type_master_key dim_ig_it_cfg_check_type_master_key,
       d_ig_it_cfg_check_type_master.ent_id ent_id,
       d_ig_it_cfg_check_type_master.check_type_id check_type_id,
       d_ig_it_cfg_check_type_master.check_type_abbr_1 check_type_abbr_1,
       d_ig_it_cfg_check_type_master.check_type_abbr_2 check_type_abbr_2,
       d_ig_it_cfg_check_type_master.check_type_name check_type_name,
       d_ig_it_cfg_check_type_master.default_price_level_id default_price_level_id,
       d_ig_it_cfg_check_type_master.default_secondary_id default_secondary_id,
       d_ig_it_cfg_check_type_master.discount_id discount_id,
       d_ig_it_cfg_check_type_master.round_basis round_basis,
       d_ig_it_cfg_check_type_master.round_type_id round_type_id,
       d_ig_it_cfg_check_type_master.row_version row_version,
       d_ig_it_cfg_check_type_master.sales_tippable_flag sales_tippable_flag,
       d_ig_it_cfg_check_type_master.store_id store_id
  from dbo.d_ig_it_cfg_check_type_master;