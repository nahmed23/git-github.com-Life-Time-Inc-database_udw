CREATE VIEW [marketing].[v_dim_cafe_discount_coupon] AS select d_ig_it_cfg_discoup_master.ent_id ent_id,
       d_ig_it_cfg_discoup_master.discoup_id discoup_id,
       d_ig_it_cfg_discoup_master.amount amount,
       d_ig_it_cfg_discoup_master.amount_discount_flag  amount_discount_flag ,
       d_ig_it_cfg_discoup_master.amount_maximum amount_maximum,
       d_ig_it_cfg_discoup_master.dim_cafe_discount_coupon_key dim_cafe_discount_coupon_key,
       d_ig_it_cfg_discoup_master.discount_coupon_abbreviation_1 discount_coupon_abbreviation_1,
       d_ig_it_cfg_discoup_master.discount_coupon_abbreviation_2 discount_coupon_abbreviation_2,
       d_ig_it_cfg_discoup_master.discount_coupon_name discount_coupon_name,
       d_ig_it_cfg_discoup_master.discount_coupon_type discount_coupon_type,
       d_ig_it_cfg_discoup_master.discount_percent discount_percent,
       d_ig_it_cfg_discoup_master.discount_percent_maximum discount_percent_maximum,
       d_ig_it_cfg_discoup_master.percent_discount_flag  percent_discount_flag 
  from dbo.d_ig_it_cfg_discoup_master;