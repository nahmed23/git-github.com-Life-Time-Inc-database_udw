CREATE VIEW [marketing].[v_dim_exerp_product_product_group]
AS select dim_exerp_product_product_group.dim_exerp_product_key dim_exerp_product_key,
       dim_exerp_product_product_group.dim_exerp_product_product_group_key dim_exerp_product_product_group_key,
       dim_exerp_product_product_group.dimension_product_group_id dimension_product_group_id,
       dim_exerp_product_product_group.dimension_product_group_name dimension_product_group_name,
       dim_exerp_product_product_group.parent_product_group_id parent_product_group_id,
       dim_exerp_product_product_group.parent_product_group_name parent_product_group_name,
       dim_exerp_product_product_group.primary_product_group_flag primary_product_group_flag,
       dim_exerp_product_product_group.product_group_external_id product_group_external_id,
       dim_exerp_product_product_group.product_group_id product_group_id,
       dim_exerp_product_product_group.product_group_name product_group_name,
       dim_exerp_product_product_group.product_id product_id
  from dbo.dim_exerp_product_product_group;