CREATE VIEW [marketing].[v_dim_boss_product_bridge_dim_boss_prod_kit] AS select d_boss_asi_prod_kit.parent_upc parent_upc,
       d_boss_asi_prod_kit.child_upc child_upc,
       d_boss_asi_prod_kit.child_dim_boss_product_key child_dim_boss_product_key,
       d_boss_asi_prod_kit.parent_dim_boss_product_key parent_dim_boss_product_key
  from dbo.d_boss_asi_prod_kit;