CREATE VIEW [marketing].[v_dim_exerp_activity_tagging] AS select dim_exerp_activity_tagging.dim_boss_product_key dim_boss_product_key,
       dim_exerp_activity_tagging.dim_exerp_activity_key dim_exerp_activity_key,
       dim_exerp_activity_tagging.dim_exerp_activity_tagging_key dim_exerp_activity_tagging_key,
       dim_exerp_activity_tagging.tag_name tag_name,
       dim_exerp_activity_tagging.tag_type tag_type,
       dim_exerp_activity_tagging.taggable_id taggable_id,
       dim_exerp_activity_tagging.taggings_id taggings_id,
       dim_exerp_activity_tagging.tags_id tags_id
  from dbo.dim_exerp_activity_tagging;