CREATE VIEW [marketing].[v_dim_boss_product_tagging] AS select dim_boss_product_tagging.dim_boss_product_key dim_boss_product_key,
       dim_boss_product_tagging.dim_boss_product_tagging_key dim_boss_product_tagging_key,
       dim_boss_product_tagging.tag_name tag_name,
       dim_boss_product_tagging.tag_type tag_type,
       dim_boss_product_tagging.taggable_id taggable_id,
       dim_boss_product_tagging.taggings_id taggings_id,
       dim_boss_product_tagging.tags_id tags_id
  from dbo.dim_boss_product_tagging;