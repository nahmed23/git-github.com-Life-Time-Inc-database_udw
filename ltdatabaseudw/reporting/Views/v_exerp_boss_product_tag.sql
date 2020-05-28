CREATE VIEW [reporting].[v_exerp_boss_product_tag] AS select 
  'EXERP' data_source,
  a.dim_exerp_activity_key as dim_product_key, b.tag_name, b.tag_type from [marketing].[v_dim_exerp_activity] a
  LEFT JOIN [marketing].[v_dim_exerp_activity_tagging] b 
  on a.dim_exerp_activity_key = b.dim_exerp_activity_key 
  UNION
  select 
  'BOSS' data_source,
  a.dim_boss_product_key as dim_product_key, b.tag_name, b.tag_type from [marketing].[v_dim_boss_product] a
  LEFT JOIN [marketing].[v_dim_boss_product_tagging] b
  on a.dim_boss_product_key = b.dim_boss_product_key;